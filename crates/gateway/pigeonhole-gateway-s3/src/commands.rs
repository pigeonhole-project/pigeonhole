//! The S3 namespace of the metadata state machine.
//!
//! [`S3Namespace::apply`] is the only code that writes buckets, objects and
//! multipart uploads. It must stay deterministic: timestamps and upload ids
//! come in the command (chosen by [`crate::index::Index`]).

use crate::index::SCHEMA;
use anyhow::{Context, Result};
use async_trait::async_trait;
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{Applied, Namespace};
use prost::Message;
use sqlx::SqliteConnection;

pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/pigeonhole.s3.v1.rs"));
}

use pb::rejection::Reason;

pub(crate) const NAMESPACE: &str = "s3";

pub(crate) struct S3Namespace;

#[async_trait]
impl Namespace for S3Namespace {
    fn name(&self) -> &'static str {
        NAMESPACE
    }

    async fn init(&self, conn: &mut SqliteConnection) -> Result<()> {
        for ddl in SCHEMA {
            sqlx::query(ddl).execute(&mut *conn).await?;
        }
        Ok(())
    }

    async fn apply(&self, conn: &mut SqliteConnection, command: &[u8]) -> Result<Applied> {
        use pb::command::Op;
        let cmd = pb::Command::decode(command).context("decode s3 command")?;
        match cmd.op.context("empty s3 command")? {
            Op::CreateBucket(c) => create_bucket(conn, c).await,
            Op::DeleteBucket(c) => delete_bucket(conn, &c.name).await,
            Op::PutObject(c) => put_object(conn, c).await,
            Op::DeleteObject(c) => delete_object(conn, &c.bucket, &c.key).await,
            Op::PutObjectTags(c) => put_tags(conn, &c.bucket, &c.key, &c.tags).await,
            Op::DeleteObjectTags(c) => put_tags(conn, &c.bucket, &c.key, &[]).await,
            Op::CreateUpload(c) => create_upload(conn, c).await,
            Op::PutPart(c) => put_part(conn, c).await,
            Op::CompleteUpload(c) => complete_upload(conn, c).await,
            Op::AbortUpload(c) => abort_upload(conn, &c.upload_id).await,
        }
    }
}

fn ok() -> Result<Applied> {
    Ok(Applied::Commit(Vec::new()))
}

fn released(mut chunk_ids: Vec<ChunkId>) -> Result<Applied> {
    chunk_ids.sort_unstable();
    chunk_ids.dedup();
    Ok(Applied::Commit(pb::Released { chunk_ids }.encode_to_vec()))
}

fn reject(reason: Reason) -> Result<Applied> {
    Ok(Applied::Reject(
        pb::Rejection {
            reason: Some(reason),
        }
        .encode_to_vec(),
    ))
}

pub(crate) fn chunk_ids_from_json(json: &str) -> Result<Vec<ChunkId>> {
    let extents: Vec<Extent> = serde_json::from_str(json).context("parse extents_json")?;
    Ok(extents.into_iter().map(|e| e.chunk).collect())
}

pub(crate) fn extents_json(extents: &[pb::Extent]) -> Result<String> {
    let extents: Vec<Extent> = extents
        .iter()
        .map(|e| Extent {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        })
        .collect();
    Ok(serde_json::to_string(&extents)?)
}

async fn create_bucket(conn: &mut SqliteConnection, c: pb::CreateBucket) -> Result<Applied> {
    let res = sqlx::query(
        "INSERT OR IGNORE INTO buckets (name, created_at, scope_id) VALUES (?, ?, ?)",
    )
    .bind(&c.name)
    .bind(&c.created_at)
    .bind(&c.scope_id)
    .execute(&mut *conn)
    .await?;
    if res.rows_affected() == 0 {
        return reject(Reason::BucketExists(true));
    }
    ok()
}

async fn delete_bucket(conn: &mut SqliteConnection, name: &str) -> Result<Applied> {
    let (objects,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM objects WHERE bucket = ?")
        .bind(name)
        .fetch_one(&mut *conn)
        .await?;
    if objects > 0 {
        return reject(Reason::BucketNotEmpty(true));
    }
    let res = sqlx::query("DELETE FROM buckets WHERE name = ?")
        .bind(name)
        .execute(&mut *conn)
        .await?;
    if res.rows_affected() == 0 {
        return reject(Reason::BucketNotFound(true));
    }
    ok()
}

/// Write the object row + user metadata + tags, returning the replaced version's chunks.
#[allow(clippy::too_many_arguments)]
async fn write_object(
    conn: &mut SqliteConnection,
    bucket: &str,
    key: &str,
    etag: &str,
    size: i64,
    content_type: Option<&str>,
    extents_json: &str,
    user_meta: &[pb::Pair],
    tags: &[pb::Pair],
    checksums_json: &str,
    mtime: &str,
) -> Result<Vec<ChunkId>> {
    let old: Option<(String,)> =
        sqlx::query_as("SELECT extents_json FROM objects WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .fetch_optional(&mut *conn)
            .await?;
    let old_ids = old
        .map(|(j,)| chunk_ids_from_json(&j))
        .transpose()?
        .unwrap_or_default();

    for sql in [
        "DELETE FROM object_metadata WHERE bucket = ? AND key = ?",
        "DELETE FROM object_tags WHERE bucket = ? AND key = ?",
    ] {
        sqlx::query(sql).bind(bucket).bind(key).execute(&mut *conn).await?;
    }
    sqlx::query(
        r#"
        INSERT INTO objects
            (bucket, key, etag, size, content_type, mtime, checksums_json, extents_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(bucket, key) DO UPDATE SET
            etag = excluded.etag,
            size = excluded.size,
            content_type = excluded.content_type,
            mtime = excluded.mtime,
            checksums_json = excluded.checksums_json,
            extents_json = excluded.extents_json
        "#,
    )
    .bind(bucket)
    .bind(key)
    .bind(etag)
    .bind(size)
    .bind(content_type)
    .bind(mtime)
    .bind(checksums_json)
    .bind(extents_json)
    .execute(&mut *conn)
    .await?;
    for m in user_meta {
        sqlx::query("INSERT INTO object_metadata (bucket, key, name, value) VALUES (?, ?, ?, ?)")
            .bind(bucket)
            .bind(key)
            .bind(&m.name)
            .bind(&m.value)
            .execute(&mut *conn)
            .await?;
    }
    insert_tags(conn, bucket, key, tags).await?;
    Ok(old_ids)
}

async fn insert_tags(
    conn: &mut SqliteConnection,
    bucket: &str,
    key: &str,
    tags: &[pb::Pair],
) -> Result<()> {
    for t in tags {
        sqlx::query(
            "INSERT INTO object_tags (bucket, key, tag_key, tag_value) VALUES (?, ?, ?, ?)",
        )
        .bind(bucket)
        .bind(key)
        .bind(&t.name)
        .bind(&t.value)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

async fn put_object(conn: &mut SqliteConnection, c: pb::PutObject) -> Result<Applied> {
    let old = write_object(
        conn,
        &c.bucket,
        &c.key,
        &c.etag,
        c.size,
        c.content_type.as_deref(),
        &extents_json(&c.extents)?,
        &c.user_meta,
        &[],
        &c.checksums_json,
        &c.mtime,
    )
    .await?;
    released(old)
}

async fn delete_object(conn: &mut SqliteConnection, bucket: &str, key: &str) -> Result<Applied> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT extents_json FROM objects WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((json,)) = row else {
        return reject(Reason::NoSuchKey(true));
    };
    sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
        .bind(bucket)
        .bind(key)
        .execute(&mut *conn)
        .await?;
    released(chunk_ids_from_json(&json)?)
}

async fn put_tags(
    conn: &mut SqliteConnection,
    bucket: &str,
    key: &str,
    tags: &[pb::Pair],
) -> Result<Applied> {
    sqlx::query("DELETE FROM object_tags WHERE bucket = ? AND key = ?")
        .bind(bucket)
        .bind(key)
        .execute(&mut *conn)
        .await?;
    insert_tags(conn, bucket, key, tags).await?;
    ok()
}

fn pairs_json(pairs: &[pb::Pair]) -> Result<String> {
    let v: Vec<(&str, &str)> = pairs.iter().map(|p| (p.name.as_str(), p.value.as_str())).collect();
    Ok(serde_json::to_string(&v)?)
}

fn pairs_from_json(json: &str) -> Vec<pb::Pair> {
    serde_json::from_str::<Vec<(String, String)>>(json)
        .unwrap_or_default()
        .into_iter()
        .map(|(name, value)| pb::Pair { name, value })
        .collect()
}

async fn create_upload(conn: &mut SqliteConnection, c: pb::CreateUpload) -> Result<Applied> {
    sqlx::query(
        r#"
        INSERT INTO multipart_uploads
            (upload_id, bucket, key, content_type, user_meta_json, tagging_json,
             checksum_algorithm, initiated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&c.upload_id)
    .bind(&c.bucket)
    .bind(&c.key)
    .bind(&c.content_type)
    .bind(pairs_json(&c.user_meta)?)
    .bind(pairs_json(&c.tags)?)
    .bind(&c.checksum_algorithm)
    .bind(&c.initiated_at)
    .execute(&mut *conn)
    .await?;
    ok()
}

async fn put_part(conn: &mut SqliteConnection, c: pb::PutPart) -> Result<Applied> {
    let old: Option<(String,)> = sqlx::query_as(
        "SELECT extents_json FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
    )
    .bind(&c.upload_id)
    .bind(c.part_number)
    .fetch_optional(&mut *conn)
    .await?;
    let old_ids = old
        .map(|(j,)| chunk_ids_from_json(&j))
        .transpose()?
        .unwrap_or_default();
    sqlx::query(
        r#"
        INSERT INTO multipart_parts (upload_id, part_number, etag, size, extents_json)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(upload_id, part_number) DO UPDATE SET
            etag = excluded.etag,
            size = excluded.size,
            extents_json = excluded.extents_json
        "#,
    )
    .bind(&c.upload_id)
    .bind(c.part_number)
    .bind(&c.etag)
    .bind(c.size)
    .bind(extents_json(&c.extents)?)
    .execute(&mut *conn)
    .await?;
    released(old_ids)
}

async fn complete_upload(conn: &mut SqliteConnection, c: pb::CompleteUpload) -> Result<Applied> {
    let upload: Option<(String, String, Option<String>, String, String)> = sqlx::query_as(
        r#"
        SELECT bucket, key, content_type, user_meta_json, tagging_json
        FROM multipart_uploads WHERE upload_id = ?
        "#,
    )
    .bind(&c.upload_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((bucket, key, content_type, user_meta_json, tagging_json)) = upload else {
        return reject(Reason::NoSuchUpload(true));
    };

    let mut assembled: Vec<Extent> = Vec::new();
    for &pn in &c.part_numbers {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
        )
        .bind(&c.upload_id)
        .bind(pn)
        .fetch_optional(&mut *conn)
        .await?;
        let Some((json,)) = row else {
            return reject(Reason::MissingPart(pn));
        };
        let mut part: Vec<Extent> = serde_json::from_str(&json)?;
        assembled.append(&mut part);
    }

    let old = write_object(
        conn,
        &bucket,
        &key,
        &c.etag,
        c.total_size,
        content_type.as_deref(),
        &serde_json::to_string(&assembled)?,
        &pairs_from_json(&user_meta_json),
        &pairs_from_json(&tagging_json),
        "{}",
        &c.mtime,
    )
    .await?;
    // Part extents now belong to the object: drop the upload without releasing them.
    sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
        .bind(&c.upload_id)
        .execute(&mut *conn)
        .await?;
    released(old)
}

async fn abort_upload(conn: &mut SqliteConnection, upload_id: &str) -> Result<Applied> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT extents_json FROM multipart_parts WHERE upload_id = ?")
            .bind(upload_id)
            .fetch_all(&mut *conn)
            .await?;
    let res = sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
        .bind(upload_id)
        .execute(&mut *conn)
        .await?;
    if res.rows_affected() == 0 {
        return reject(Reason::NoSuchUpload(true));
    }
    let mut ids = Vec::new();
    for (json,) in rows {
        ids.extend(chunk_ids_from_json(&json)?);
    }
    released(ids)
}
