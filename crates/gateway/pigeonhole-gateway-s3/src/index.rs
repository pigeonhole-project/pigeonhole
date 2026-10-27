//! S3 gateway index: buckets/objects/multipart with extent lists.
//!
//! Queries read SQLite directly; every change goes through an `s3` command
//! (see [`crate::commands`]). Chunk refcounts live in the chunk store
//! ([`ChunkStore::retain`] / [`release`]).

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use serde::{Deserialize, Serialize};
use crate::commands::{pb, S3Namespace, NAMESPACE};
use pigeonhole_meta::{Applied, MetaStore, Namespace};
use prost::Message;
use sqlx::{FromRow, SqlitePool};
use std::sync::Arc;

#[derive(Clone)]
pub struct Index {
    pool: SqlitePool,
    meta: Arc<MetaStore>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Bucket {
    pub name: String,
    pub created_at: String,
    pub scope_id: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub mtime: String,
}

/// `(bucket, key, size, content_type, etag, extents_json, checksums_json)`.
type SourceRow = (String, String, i64, Option<String>, String, String, String);

/// Snapshot of a copy source, see [`Index::get_copy_source`].
#[derive(Debug, Clone)]
pub struct SourceSnapshot {
    pub etag: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub extents: Vec<Extent>,
    pub user_meta: Vec<(String, String)>,
    pub checksums_json: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct UserMeta {
    pub bucket: String,
    pub key: String,
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
    pub user_meta_json: String,
    pub tagging_json: String,
    pub checksum_algorithm: Option<String>,
    pub initiated_at: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct MultipartPart {
    pub upload_id: String,
    pub part_number: i64,
    pub etag: String,
    pub size: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexSnapshot {
    pub buckets: Vec<Bucket>,
    pub objects: Vec<ObjectRow>,
    #[serde(default)]
    pub metadata: Vec<UserMeta>,
    #[serde(default)]
    pub tags: Vec<ObjectTagRow>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ObjectRow {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub mtime: String,
    #[serde(default)]
    pub checksums_json: String,
    pub extents_json: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ObjectTagRow {
    pub bucket: String,
    pub key: String,
    pub tag_key: String,
    pub tag_value: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeleteBucketResult {
    Deleted,
    NotFound,
    NotEmpty,
}

/// Current schema of the `s3` namespace. No upgrades.
pub(crate) const SCHEMA: &[&str] = &[
    r#"
            CREATE TABLE IF NOT EXISTS s3_buckets (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                scope_id TEXT NOT NULL DEFAULT ''
            )
    "#,
    r#"
            CREATE TABLE IF NOT EXISTS s3_objects (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                etag TEXT NOT NULL,
                size INTEGER NOT NULL,
                content_type TEXT,
                mtime TEXT NOT NULL,
                checksums_json TEXT NOT NULL DEFAULT '{}',
                extents_json TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (bucket, key),
                FOREIGN KEY (bucket) REFERENCES s3_buckets(name) ON DELETE CASCADE
            )
    "#,
    r#"
            CREATE TABLE IF NOT EXISTS s3_object_metadata (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                name TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (bucket, key, name),
                FOREIGN KEY (bucket, key) REFERENCES s3_objects(bucket, key) ON DELETE CASCADE
            )
    "#,
    r#"
            CREATE TABLE IF NOT EXISTS s3_object_tags (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                tag_key TEXT NOT NULL,
                tag_value TEXT NOT NULL,
                PRIMARY KEY (bucket, key, tag_key),
                FOREIGN KEY (bucket, key) REFERENCES s3_objects(bucket, key) ON DELETE CASCADE
            )
    "#,
    r#"
            CREATE TABLE IF NOT EXISTS s3_uploads (
                upload_id TEXT PRIMARY KEY,
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                content_type TEXT,
                user_meta_json TEXT NOT NULL DEFAULT '[]',
                tagging_json TEXT NOT NULL DEFAULT '[]',
                checksum_algorithm TEXT,
                initiated_at TEXT NOT NULL,
                FOREIGN KEY (bucket) REFERENCES s3_buckets(name) ON DELETE CASCADE
            )
    "#,
    r#"
            CREATE TABLE IF NOT EXISTS s3_parts (
                upload_id TEXT NOT NULL,
                part_number INTEGER NOT NULL,
                etag TEXT NOT NULL,
                size INTEGER NOT NULL,
                extents_json TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (upload_id, part_number),
                FOREIGN KEY (upload_id) REFERENCES s3_uploads(upload_id) ON DELETE CASCADE
            )
    "#,
    r#"
            CREATE TABLE IF NOT EXISTS s3_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
    "#,
];

impl Index {
    /// Open a standalone tablet at `database_url` (local log next to it) with
    /// only this namespace.
    pub async fn connect(database_url: &str) -> Result<Self> {
        Self::open(MetaStore::open_local(database_url, vec![Self::namespace()]).await?).await
    }

    /// The `s3` namespace, to pass to [`MetaStore::open`].
    pub fn namespace() -> Arc<dyn Namespace> {
        Arc::new(S3Namespace)
    }

    /// Use `meta`, which must have been opened with [`Self::namespace`].
    pub async fn open(meta: Arc<MetaStore>) -> Result<Self> {
        if !meta.has_namespace(NAMESPACE) {
            anyhow::bail!("metadata tablet opened without the s3 namespace");
        }
        Ok(Self {
            pool: meta.pool().clone(),
            meta,
        })
    }

    /// Propose one s3 command; `Ok(Err(reason))` is a deterministic rejection.
    async fn propose(
        &self,
        op: pb::command::Op,
    ) -> Result<std::result::Result<Vec<ChunkId>, pb::rejection::Reason>> {
        let cmd = pb::Command { op: Some(op) }.encode_to_vec();
        match self.meta.propose(NAMESPACE, &cmd).await?.applied {
            Applied::Commit(bytes) => Ok(Ok(pb::Released::decode(bytes.as_slice())?.chunk_ids)),
            Applied::Reject(bytes) => Ok(Err(pb::Rejection::decode(bytes.as_slice())?
                .reason
                .context("rejection without reason")?)),
        }
    }

    async fn propose_infallible(&self, op: pb::command::Op) -> Result<Vec<ChunkId>> {
        match self.propose(op).await? {
            Ok(ids) => Ok(ids),
            Err(reason) => anyhow::bail!("unexpected s3 rejection {reason:?}"),
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Returns false when the bucket already exists.
    pub async fn create_bucket(&self, name: &str, scope_id: &str) -> Result<bool> {
        let op = pb::command::Op::CreateBucket(pb::CreateBucket {
            name: name.into(),
            scope_id: scope_id.into(),
            created_at: Utc::now().to_rfc3339(),
        });
        match self.propose(op).await? {
            Ok(_) => Ok(true),
            Err(pb::rejection::Reason::BucketExists(_)) => Ok(false),
            Err(other) => anyhow::bail!("unexpected create_bucket rejection {other:?}"),
        }
    }

    pub async fn bucket_scope_id(&self, name: &str) -> Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT scope_id FROM s3_buckets WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<DeleteBucketResult> {
        use pb::rejection::Reason;
        let op = pb::command::Op::DeleteBucket(pb::DeleteBucket { name: name.into() });
        Ok(match self.propose(op).await? {
            Ok(_) => DeleteBucketResult::Deleted,
            Err(Reason::BucketNotFound(_)) => DeleteBucketResult::NotFound,
            Err(Reason::BucketNotEmpty(_)) => DeleteBucketResult::NotEmpty,
            Err(other) => anyhow::bail!("unexpected delete_bucket rejection {other:?}"),
        })
    }

    pub async fn list_buckets(&self) -> Result<Vec<Bucket>> {
        let rows = sqlx::query_as::<_, Bucket>(
            "SELECT name, created_at, scope_id FROM s3_buckets ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool> {
        let row: Option<(String,)> = sqlx::query_as("SELECT name FROM s3_buckets WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// Insert/replace object. Returns previous extent chunk ids (caller should `release`).
    #[allow(clippy::too_many_arguments)]
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        etag: &str,
        size: i64,
        content_type: Option<&str>,
        extents: &[Extent],
        user_meta: &[(String, String)],
        checksums_json: &str,
    ) -> Result<Vec<ChunkId>> {
        let mtime = Utc::now().to_rfc3339();
        self.put_object_at(bucket, key, etag, size, content_type, extents, user_meta, checksums_json, &mtime)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_object_at(
        &self,
        bucket: &str,
        key: &str,
        etag: &str,
        size: i64,
        content_type: Option<&str>,
        extents: &[Extent],
        user_meta: &[(String, String)],
        checksums_json: &str,
        mtime: &str,
    ) -> Result<Vec<ChunkId>> {
        self.propose_infallible(pb::command::Op::PutObject(pb::PutObject {
            bucket: bucket.into(),
            key: key.into(),
            etag: etag.into(),
            size,
            content_type: content_type.map(Into::into),
            extents: pb_extents(extents),
            user_meta: pb_pairs(user_meta),
            checksums_json: checksums_json.into(),
            mtime: mtime.into(),
        }))
        .await
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let row = sqlx::query_as::<_, ObjectMeta>(
            "SELECT bucket, key, etag, size, content_type, mtime FROM s3_objects WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_extents(&self, bucket: &str, key: &str) -> Result<Option<Vec<Extent>>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM s3_objects WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((j,)) => Ok(Some(serde_json::from_str(&j)?)),
        }
    }

    pub async fn get_object_checksums_json(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT checksums_json FROM s3_objects WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(j,)| j))
    }

    pub async fn get_user_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT name, value FROM s3_object_metadata WHERE bucket = ? AND key = ? ORDER BY name",
        )
        .bind(bucket)
        .bind(key)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT tag_key, tag_value FROM s3_object_tags WHERE bucket = ? AND key = ? ORDER BY tag_key",
        )
        .bind(bucket)
        .bind(key)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        tags: &[(String, String)],
    ) -> Result<()> {
        self.propose_infallible(pb::command::Op::PutObjectTags(pb::PutObjectTags {
            bucket: bucket.into(),
            key: key.into(),
            tags: pb_pairs(tags),
        }))
        .await?;
        Ok(())
    }

    pub async fn delete_object_tags(&self, bucket: &str, key: &str) -> Result<()> {
        self.propose_infallible(pb::command::Op::DeleteObjectTags(pb::DeleteObjectTags {
            bucket: bucket.into(),
            key: key.into(),
        }))
        .await?;
        Ok(())
    }

    /// Delete object; returns extents' chunk ids to `release`, or `None` if missing.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Vec<ChunkId>>> {
        let op = pb::command::Op::DeleteObject(pb::DeleteObject {
            bucket: bucket.into(),
            key: key.into(),
        });
        match self.propose(op).await? {
            Ok(ids) => Ok(Some(ids)),
            Err(pb::rejection::Reason::NoSuchKey(_)) => Ok(None),
            Err(other) => anyhow::bail!("unexpected delete_object rejection {other:?}"),
        }
    }

    /// Consistent view of a copy source: object row, extents, user metadata, and
    /// checksums from one read transaction (no mix of two versions).
    pub async fn get_copy_source(&self, bucket: &str, key: &str) -> Result<Option<SourceSnapshot>> {
        let mut tx = self.pool.begin().await?;
        let row: Option<SourceRow> =
            sqlx::query_as(
                r#"
                SELECT bucket, key, size, content_type, etag, extents_json, checksums_json
                FROM s3_objects WHERE bucket = ? AND key = ?
                "#,
            )
            .bind(bucket)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
        let Some((bucket, key, size, content_type, etag, extents_json, checksums_json)) = row
        else {
            return Ok(None);
        };
        let user_meta = sqlx::query_as::<_, (String, String)>(
            "SELECT name, value FROM s3_object_metadata WHERE bucket = ? AND key = ? ORDER BY name",
        )
        .bind(&bucket)
        .bind(&key)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(SourceSnapshot {
            etag,
            size,
            content_type,
            extents: serde_json::from_str(&extents_json)?,
            user_meta,
            checksums_json,
        }))
    }

    /// Shallow copy of `src` (from [`Self::get_copy_source`]) to `dst`. The caller must
    /// `retain` the source extents' chunks **before** calling this, so the new object
    /// never points at chunks it does not hold a ref on. Returns the destination row as
    /// written and the previous destination chunk ids to `release`.
    pub async fn copy_object(
        &self,
        src: &SourceSnapshot,
        dst_bucket: &str,
        dst_key: &str,
        content_type: Option<&str>,
        user_meta: &[(String, String)],
        copy_source_meta: bool,
    ) -> Result<(ObjectMeta, Vec<ChunkId>)> {
        let ct = content_type.or(src.content_type.as_deref());
        let meta = if copy_source_meta {
            &src.user_meta[..]
        } else {
            user_meta
        };
        let mtime = Utc::now().to_rfc3339();
        let old = self
            .put_object_at(
                dst_bucket,
                dst_key,
                &src.etag,
                src.size,
                ct,
                &src.extents,
                meta,
                &src.checksums_json,
                &mtime,
            )
            .await?;

        // The row as written, not re-read: a concurrent delete or overwrite of
        // `dst` may already be applied after this put.
        let dst = ObjectMeta {
            bucket: dst_bucket.into(),
            key: dst_key.into(),
            etag: src.etag.clone(),
            size: src.size,
            content_type: ct.map(Into::into),
            mtime,
        };
        Ok((dst, old))
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: i64,
        start_after: Option<&str>,
    ) -> Result<(Vec<ObjectMeta>, Vec<String>, bool, Option<String>)> {
        let max_keys = max_keys.clamp(1, 1000) as usize;
        let range_end = exclusive_prefix_end(prefix);

        if let Some(delim) = delimiter {
            let batch = (max_keys * 32).max(64).min(10_000) as i64;
            let mut cursor: Option<(bool, String)> = match start_after {
                Some(after) if after.ends_with(delim) => {
                    exclusive_prefix_end(after).map(|e| (true, e))
                }
                Some(after) => Some((false, after.to_string())),
                None => None,
            };
            let mut collected: Vec<(String, DelimEntry)> = Vec::new();
            let mut exhausted = false;

            loop {
                let mut q = String::from(
                    r#"
                    SELECT bucket, key, etag, size, content_type, mtime
                    FROM s3_objects
                    WHERE bucket = ?
                    "#,
                );
                let mut binds: Vec<String> = Vec::new();
                if !prefix.is_empty() {
                    q.push_str(" AND key >= ? COLLATE BINARY");
                    binds.push(prefix.to_string());
                }
                if let Some(ref hi) = range_end {
                    q.push_str(" AND key < ? COLLATE BINARY");
                    binds.push(hi.clone());
                }
                match &cursor {
                    Some((true, c)) => {
                        q.push_str(" AND key >= ? COLLATE BINARY");
                        binds.push(c.clone());
                    }
                    Some((false, c)) => {
                        q.push_str(" AND key > ? COLLATE BINARY");
                        binds.push(c.clone());
                    }
                    None => {}
                }
                q.push_str(" ORDER BY key COLLATE BINARY LIMIT ?");

                let mut query = sqlx::query_as::<_, ObjectMeta>(&q).bind(bucket);
                for b in &binds {
                    query = query.bind(b);
                }
                query = query.bind(batch);

                let objs = query.fetch_all(&self.pool).await?;
                if (objs.len() as i64) < batch {
                    exhausted = true;
                }

                let batch_entries = group_delimiter_entries(prefix, delim, objs);
                let mut advanced = false;
                for e in batch_entries {
                    if let Some(after) = start_after {
                        if e.0.as_str() <= after {
                            continue;
                        }
                    }
                    if collected.last().map(|(k, _)| k) == Some(&e.0) {
                        continue;
                    }
                    let is_prefix = matches!(e.1, DelimEntry::Prefix(_));
                    let sort_key = e.0.clone();
                    collected.push(e);
                    advanced = true;
                    if is_prefix {
                        if let Some(end) = exclusive_prefix_end(&sort_key) {
                            cursor = Some((true, end));
                        } else {
                            cursor = Some((false, sort_key));
                        }
                    } else {
                        cursor = Some((false, sort_key));
                    }
                    if collected.len() > max_keys {
                        break;
                    }
                }

                if collected.len() > max_keys || exhausted || !advanced {
                    break;
                }
            }

            let truncated = collected.len() > max_keys;
            collected.truncate(max_keys);
            let next = if truncated {
                collected.last().map(|(k, _)| k.clone())
            } else {
                None
            };
            let mut objects = Vec::new();
            let mut common = Vec::new();
            for (_, e) in collected {
                match e {
                    DelimEntry::Object(o) => objects.push(o),
                    DelimEntry::Prefix(p) => common.push(p),
                }
            }
            return Ok((objects, common, truncated, next));
        }

        let fetch_limit = (max_keys + 1) as i64;
        let mut q = String::from(
            r#"
            SELECT bucket, key, etag, size, content_type, mtime
            FROM s3_objects
            WHERE bucket = ?
            "#,
        );
        if !prefix.is_empty() {
            q.push_str(" AND key >= ? COLLATE BINARY");
        }
        if range_end.is_some() {
            q.push_str(" AND key < ? COLLATE BINARY");
        }
        if start_after.is_some() {
            q.push_str(" AND key > ? COLLATE BINARY");
        }
        q.push_str(" ORDER BY key COLLATE BINARY LIMIT ?");

        let mut query = sqlx::query_as::<_, ObjectMeta>(&q).bind(bucket);
        if !prefix.is_empty() {
            query = query.bind(prefix);
        }
        if let Some(ref hi) = range_end {
            query = query.bind(hi);
        }
        if let Some(after) = start_after {
            query = query.bind(after);
        }
        query = query.bind(fetch_limit);

        let objs = query.fetch_all(&self.pool).await?;
        let truncated = objs.len() > max_keys;
        let mut objs = objs;
        objs.truncate(max_keys);
        let next = if truncated {
            objs.last().map(|o| o.key.clone())
        } else {
            None
        };
        Ok((objs, Vec::new(), truncated, next))
    }

    pub async fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as("SELECT value FROM s3_meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO s3_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn count_objects(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM s3_objects")
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }

    pub async fn count_buckets(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM s3_buckets")
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }

    pub async fn has_data(&self) -> Result<bool> {
        Ok(self.count_buckets().await? > 0 || self.count_objects().await? > 0)
    }

    pub async fn wipe_all(&self) -> Result<()> {
        sqlx::query("DELETE FROM s3_uploads")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM s3_objects")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM s3_buckets")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM s3_meta").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn export_snapshot(&self) -> Result<IndexSnapshot> {
        let buckets = self.list_buckets().await?;
        let objects = sqlx::query_as::<_, ObjectRow>(
            r#"
            SELECT bucket, key, etag, size, content_type, mtime, checksums_json, extents_json
            FROM s3_objects ORDER BY bucket, key
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        let metadata = sqlx::query_as::<_, UserMeta>(
            "SELECT bucket, key, name, value FROM s3_object_metadata ORDER BY bucket, key, name",
        )
        .fetch_all(&self.pool)
        .await?;
        let tags = sqlx::query_as::<_, ObjectTagRow>(
            "SELECT bucket, key, tag_key, tag_value FROM s3_object_tags ORDER BY bucket, key, tag_key",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(IndexSnapshot {
            buckets,
            objects,
            metadata,
            tags,
        })
    }

    pub async fn import_snapshot(&self, snap: &IndexSnapshot) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM s3_uploads")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM s3_objects")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM s3_buckets")
            .execute(&mut *tx)
            .await?;

        for b in &snap.buckets {
            sqlx::query(
                "INSERT INTO s3_buckets (name, created_at, scope_id) VALUES (?, ?, ?)",
            )
            .bind(&b.name)
            .bind(&b.created_at)
            .bind(&b.scope_id)
            .execute(&mut *tx)
            .await?;
        }
        for o in &snap.objects {
            sqlx::query(
                r#"
                INSERT INTO s3_objects
                    (bucket, key, etag, size, content_type, mtime, checksums_json, extents_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&o.bucket)
            .bind(&o.key)
            .bind(&o.etag)
            .bind(o.size)
            .bind(&o.content_type)
            .bind(&o.mtime)
            .bind(&o.checksums_json)
            .bind(&o.extents_json)
            .execute(&mut *tx)
            .await?;
        }
        for m in &snap.metadata {
            sqlx::query(
                "INSERT INTO s3_object_metadata (bucket, key, name, value) VALUES (?, ?, ?, ?)",
            )
            .bind(&m.bucket)
            .bind(&m.key)
            .bind(&m.name)
            .bind(&m.value)
            .execute(&mut *tx)
            .await?;
        }
        for t in &snap.tags {
            sqlx::query(
                "INSERT INTO s3_object_tags (bucket, key, tag_key, tag_value) VALUES (?, ?, ?, ?)",
            )
            .bind(&t.bucket)
            .bind(&t.key)
            .bind(&t.tag_key)
            .bind(&t.tag_value)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_multipart_upload(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: &[(String, String)],
        tags: &[(String, String)],
        checksum_algorithm: Option<&str>,
    ) -> Result<()> {
        self.propose_infallible(pb::command::Op::CreateUpload(pb::CreateUpload {
            upload_id: upload_id.into(),
            bucket: bucket.into(),
            key: key.into(),
            content_type: content_type.map(Into::into),
            user_meta: pb_pairs(user_meta),
            tags: pb_pairs(tags),
            checksum_algorithm: checksum_algorithm.map(Into::into),
            initiated_at: Utc::now().to_rfc3339(),
        }))
        .await?;
        Ok(())
    }

    pub async fn get_multipart_upload(&self, upload_id: &str) -> Result<Option<MultipartUpload>> {
        let row = sqlx::query_as::<_, MultipartUpload>(
            r#"
            SELECT upload_id, bucket, key, content_type, user_meta_json, tagging_json,
                   checksum_algorithm, initiated_at
            FROM s3_uploads WHERE upload_id = ?
            "#,
        )
        .bind(upload_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Store part extents. Returns previous part chunk ids to `release`.
    pub async fn put_multipart_part(
        &self,
        upload_id: &str,
        part_number: i64,
        etag: &str,
        size: i64,
        extents: &[Extent],
    ) -> Result<Vec<ChunkId>> {
        self.propose_infallible(pb::command::Op::PutPart(pb::PutPart {
            upload_id: upload_id.into(),
            part_number,
            etag: etag.into(),
            size,
            extents: pb_extents(extents),
        }))
        .await
    }

    pub async fn get_multipart_part(
        &self,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultipartPart>> {
        let row = sqlx::query_as::<_, MultipartPart>(
            "SELECT upload_id, part_number, etag, size FROM s3_parts WHERE upload_id = ? AND part_number = ?",
        )
        .bind(upload_id)
        .bind(part_number)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_multipart_part_extents(
        &self,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<Vec<Extent>>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM s3_parts WHERE upload_id = ? AND part_number = ?",
        )
        .bind(upload_id)
        .bind(part_number)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((j,)) => Ok(Some(serde_json::from_str(&j)?)),
        }
    }

    /// Assemble object from parts (ownership transfer). Returns old object chunk ids to release.
    pub async fn complete_multipart_upload(
        &self,
        upload: &MultipartUpload,
        part_numbers: &[i64],
        etag: &str,
        total_size: i64,
    ) -> Result<Vec<ChunkId>> {
        let op = pb::command::Op::CompleteUpload(pb::CompleteUpload {
            upload_id: upload.upload_id.clone(),
            part_numbers: part_numbers.to_vec(),
            etag: etag.into(),
            total_size,
            mtime: Utc::now().to_rfc3339(),
        });
        match self.propose(op).await? {
            Ok(ids) => Ok(ids),
            Err(pb::rejection::Reason::MissingPart(pn)) => anyhow::bail!("missing part {pn}"),
            Err(other) => anyhow::bail!("complete_multipart_upload rejected: {other:?}"),
        }
    }

    /// Abort upload; returns all part chunk ids to `release`, or `None` if missing.
    pub async fn abort_multipart_upload(&self, upload_id: &str) -> Result<Option<Vec<ChunkId>>> {
        let op = pb::command::Op::AbortUpload(pb::AbortUpload {
            upload_id: upload_id.into(),
        });
        match self.propose(op).await? {
            Ok(ids) => Ok(Some(ids)),
            Err(pb::rejection::Reason::NoSuchUpload(_)) => Ok(None),
            Err(other) => anyhow::bail!("unexpected abort rejection {other:?}"),
        }
    }

    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: i64,
    ) -> Result<(Vec<MultipartUpload>, bool, Option<(String, String)>)> {
        let max_uploads = max_uploads.clamp(1, 1000) as usize;
        let mut sql = String::from(
            r#"
            SELECT upload_id, bucket, key, content_type, user_meta_json, tagging_json,
                   checksum_algorithm, initiated_at
            FROM s3_uploads
            WHERE bucket = ?
            "#,
        );
        let mut binds: Vec<String> = vec![bucket.to_string()];
        if !prefix.is_empty() {
            sql.push_str(" AND key >= ?");
            binds.push(prefix.to_string());
            if let Some(end) = exclusive_prefix_end(prefix) {
                sql.push_str(" AND key < ?");
                binds.push(end);
            }
        }
        if let Some(km) = key_marker.filter(|s| !s.is_empty()) {
            if let Some(um) = upload_id_marker.filter(|s| !s.is_empty()) {
                sql.push_str(" AND (key > ? OR (key = ? AND upload_id > ?))");
                binds.push(km.to_string());
                binds.push(km.to_string());
                binds.push(um.to_string());
            } else {
                sql.push_str(" AND key > ?");
                binds.push(km.to_string());
            }
        }
        sql.push_str(" ORDER BY key, upload_id LIMIT ?");

        let mut query = sqlx::query_as::<_, MultipartUpload>(&sql);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind((max_uploads + 1) as i64);
        let mut rows = query.fetch_all(&self.pool).await?;
        let truncated = rows.len() > max_uploads;
        rows.truncate(max_uploads);
        let next = if truncated {
            rows.last().map(|u| (u.key.clone(), u.upload_id.clone()))
        } else {
            None
        };
        Ok((rows, truncated, next))
    }

    pub async fn list_parts(
        &self,
        upload_id: &str,
        part_number_marker: Option<i64>,
        max_parts: i64,
    ) -> Result<(Vec<MultipartPart>, bool, Option<i64>)> {
        let max_parts = max_parts.clamp(1, 1000) as usize;
        let marker = part_number_marker.unwrap_or(0);
        let mut rows = sqlx::query_as::<_, MultipartPart>(
            r#"
            SELECT upload_id, part_number, etag, size
            FROM s3_parts
            WHERE upload_id = ? AND part_number > ?
            ORDER BY part_number
            LIMIT ?
            "#,
        )
        .bind(upload_id)
        .bind(marker)
        .bind((max_parts + 1) as i64)
        .fetch_all(&self.pool)
        .await?;
        let truncated = rows.len() > max_parts;
        rows.truncate(max_parts);
        let next = if truncated {
            rows.last().map(|p| p.part_number)
        } else {
            None
        };
        Ok((rows, truncated, next))
    }
}

/// Unique chunk ids referenced by an extent list (sorted).
pub fn unique_chunk_ids(extents: &[Extent]) -> Vec<ChunkId> {
    let mut ids: Vec<_> = extents.iter().map(|e| e.chunk).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Slice an extent list to cover `[start, start+length)` logical bytes.
pub fn slice_extents(extents: &[Extent], start: u64, length: u64) -> Vec<Extent> {
    if length == 0 {
        return Vec::new();
    }
    let end = start.saturating_add(length);
    let mut cursor = 0u64;
    let mut out = Vec::new();
    for ext in extents {
        let elen = if ext.len < 0 { 0u64 } else { ext.len as u64 };
        let estart = cursor;
        let eend = cursor + elen;
        cursor = eend;
        if eend <= start || estart >= end {
            continue;
        }
        let from = start.saturating_sub(estart);
        let to = end.min(eend).saturating_sub(estart);
        if to > from {
            out.push(Extent {
                chunk: ext.chunk,
                offset: ext.offset + from as i64,
                len: (to - from) as i64,
            });
        }
    }
    out
}


enum DelimEntry {
    Object(ObjectMeta),
    Prefix(String),
}

fn exclusive_prefix_end(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(b) = bytes.last_mut() {
        if *b != 0xff {
            *b += 1;
            return Some(String::from_utf8_lossy(&bytes).into_owned());
        }
        bytes.pop();
    }
    None
}

fn group_delimiter_entries(
    prefix: &str,
    delim: &str,
    objs: Vec<ObjectMeta>,
) -> Vec<(String, DelimEntry)> {
    let mut entries: Vec<(String, DelimEntry)> = Vec::new();
    let mut seen_cp = std::collections::BTreeSet::new();

    for obj in objs {
        let Some(rest) = obj.key.strip_prefix(prefix) else {
            continue;
        };
        if let Some(idx) = rest.find(delim) {
            let cp = format!("{prefix}{}", &rest[..=idx]);
            if seen_cp.insert(cp.clone()) {
                entries.push((cp.clone(), DelimEntry::Prefix(cp)));
            }
        } else {
            entries.push((obj.key.clone(), DelimEntry::Object(obj)));
        }
    }
    entries
}

pub fn parse_rfc3339(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn pb_extents(extents: &[Extent]) -> Vec<pb::Extent> {
    extents
        .iter()
        .map(|e| pb::Extent {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        })
        .collect()
}

fn pb_pairs(pairs: &[(String, String)]) -> Vec<pb::Pair> {
    pairs
        .iter()
        .map(|(name, value)| pb::Pair {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}
