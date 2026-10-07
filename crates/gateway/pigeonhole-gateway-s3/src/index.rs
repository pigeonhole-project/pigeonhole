//! S3 gateway index: buckets/objects/multipart with extent lists (stage F).
//!
//! Chunk refcounts live in `blob.db` via [`ChunkStore::retain`] / [`release`];
//! this SQLite DB only stores gateway metadata and serialized extent lists.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, FromRow, SqlitePool};

#[derive(Clone)]
pub struct Index {
    pool: SqlitePool,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Bucket {
    pub name: String,
    pub created_at: String,
    #[serde(default)]
    pub chat_id: String,
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

impl Index {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let url = if database_url == "sqlite:s3gram.db" {
            "sqlite:s3gram.db?mode=rwc".to_string()
        } else if database_url.starts_with("sqlite:") && !database_url.contains('?') {
            format!("{database_url}?mode=rwc")
        } else {
            database_url.to_string()
        };

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .with_context(|| format!("connect sqlite {url}"))?;

        let idx = Self { pool };
        idx.migrate().await?;
        Ok(idx)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA busy_timeout = 5000")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                chat_id TEXT NOT NULL DEFAULT ''
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS objects (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                etag TEXT NOT NULL,
                size INTEGER NOT NULL,
                content_type TEXT,
                mtime TEXT NOT NULL,
                checksums_json TEXT NOT NULL DEFAULT '{}',
                extents_json TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (bucket, key),
                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self.column_exists("objects", "extents_json").await? {
            sqlx::query(
                "ALTER TABLE objects ADD COLUMN extents_json TEXT NOT NULL DEFAULT '[]'",
            )
            .execute(&self.pool)
            .await?;
        }
        if !self.column_exists("objects", "checksums_json").await? {
            sqlx::query(
                "ALTER TABLE objects ADD COLUMN checksums_json TEXT NOT NULL DEFAULT '{}'",
            )
            .execute(&self.pool)
            .await?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS object_metadata (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                name TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (bucket, key, name),
                FOREIGN KEY (bucket, key) REFERENCES objects(bucket, key) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS object_tags (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                tag_key TEXT NOT NULL,
                tag_value TEXT NOT NULL,
                PRIMARY KEY (bucket, key, tag_key),
                FOREIGN KEY (bucket, key) REFERENCES objects(bucket, key) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS multipart_uploads (
                upload_id TEXT PRIMARY KEY,
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                content_type TEXT,
                user_meta_json TEXT NOT NULL DEFAULT '[]',
                tagging_json TEXT NOT NULL DEFAULT '[]',
                checksum_algorithm TEXT,
                initiated_at TEXT NOT NULL,
                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS multipart_parts (
                upload_id TEXT NOT NULL,
                part_number INTEGER NOT NULL,
                etag TEXT NOT NULL,
                size INTEGER NOT NULL,
                extents_json TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (upload_id, part_number),
                FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self.column_exists("multipart_parts", "extents_json").await? {
            sqlx::query(
                "ALTER TABLE multipart_parts ADD COLUMN extents_json TEXT NOT NULL DEFAULT '[]'",
            )
            .execute(&self.pool)
            .await?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn column_exists(&self, table: &str, column: &str) -> Result<bool> {
        let (n,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?"
        ))
        .bind(column)
        .fetch_one(&self.pool)
        .await?;
        Ok(n > 0)
    }

    pub async fn create_bucket(&self, name: &str, chat_id: &str) -> Result<bool> {
        let created_at = Utc::now().to_rfc3339();
        let res = sqlx::query(
            "INSERT OR IGNORE INTO buckets (name, created_at, chat_id) VALUES (?, ?, ?)",
        )
        .bind(name)
        .bind(created_at)
        .bind(chat_id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn upsert_bucket(&self, name: &str, chat_id: &str) -> Result<()> {
        let created_at = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO buckets (name, created_at, chat_id) VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET chat_id = excluded.chat_id
            "#,
        )
        .bind(name)
        .bind(created_at)
        .bind(chat_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn bucket_chat_id(&self, name: &str) -> Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT chat_id FROM buckets WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<DeleteBucketResult> {
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM objects WHERE bucket = ?")
                .bind(name)
                .fetch_one(&self.pool)
                .await?;
        if count.0 > 0 {
            return Ok(DeleteBucketResult::NotEmpty);
        }
        let res = sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await?;
        if res.rows_affected() == 0 {
            Ok(DeleteBucketResult::NotFound)
        } else {
            Ok(DeleteBucketResult::Deleted)
        }
    }

    pub async fn list_buckets(&self) -> Result<Vec<Bucket>> {
        let rows = sqlx::query_as::<_, Bucket>(
            "SELECT name, created_at, chat_id FROM buckets ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool> {
        let row: Option<(String,)> = sqlx::query_as("SELECT name FROM buckets WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// Insert/replace object. Returns previous extent chunk ids (caller should `release`).
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
        let mut tx = self.pool.begin().await?;
        let old_json: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM objects WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;
        let old_ids = old_json
            .map(|(j,)| chunk_ids_from_json(&j))
            .transpose()?
            .unwrap_or_default();

        sqlx::query("DELETE FROM object_metadata WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM object_tags WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;

        let mtime = Utc::now().to_rfc3339();
        let extents_json = serde_json::to_string(extents)?;
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
        .bind(&mtime)
        .bind(checksums_json)
        .bind(&extents_json)
        .execute(&mut *tx)
        .await?;

        for (name, value) in user_meta {
            sqlx::query(
                "INSERT INTO object_metadata (bucket, key, name, value) VALUES (?, ?, ?, ?)",
            )
            .bind(bucket)
            .bind(key)
            .bind(name)
            .bind(value)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(old_ids)
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let row = sqlx::query_as::<_, ObjectMeta>(
            "SELECT bucket, key, etag, size, content_type, mtime FROM objects WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_extents(&self, bucket: &str, key: &str) -> Result<Option<Vec<Extent>>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM objects WHERE bucket = ? AND key = ?",
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
            "SELECT checksums_json FROM objects WHERE bucket = ? AND key = ?",
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
            "SELECT name, value FROM object_metadata WHERE bucket = ? AND key = ? ORDER BY name",
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
            "SELECT tag_key, tag_value FROM object_tags WHERE bucket = ? AND key = ? ORDER BY tag_key",
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
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM object_tags WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;
        for (k, v) in tags {
            sqlx::query(
                "INSERT INTO object_tags (bucket, key, tag_key, tag_value) VALUES (?, ?, ?, ?)",
            )
            .bind(bucket)
            .bind(key)
            .bind(k)
            .bind(v)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_object_tags(&self, bucket: &str, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM object_tags WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete object; returns extents' chunk ids to `release`, or `None` if missing.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Vec<ChunkId>>> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM objects WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((extents_json,)) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let ids = chunk_ids_from_json(&extents_json)?;
        sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(ids))
    }

    /// Shallow copy: same extents, caller must `retain` returned chunk ids.
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        content_type: Option<&str>,
        user_meta: &[(String, String)],
        copy_source_meta: bool,
    ) -> Result<(ObjectMeta, Vec<Extent>, Vec<ChunkId>)> {
        let src = self
            .get_object(src_bucket, src_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source object not found"))?;
        let extents = self
            .get_extents(src_bucket, src_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source extents missing"))?;
        let ct = content_type.or(src.content_type.as_deref());
        let meta: Vec<(String, String)> = if copy_source_meta {
            self.get_user_metadata(src_bucket, src_key).await?
        } else {
            user_meta.to_vec()
        };
        let checksums_json = self
            .get_object_checksums_json(src_bucket, src_key)
            .await?
            .unwrap_or_else(|| "{}".to_string());

        let old = self
            .put_object(
                dst_bucket,
                dst_key,
                &src.etag,
                src.size,
                ct,
                &extents,
                &meta,
                &checksums_json,
            )
            .await?;

        let dst = self
            .get_object(dst_bucket, dst_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("destination missing after copy"))?;
        Ok((dst, extents, old))
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
                    FROM objects
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
            FROM objects
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
        let row: Option<(String,)> = sqlx::query_as("SELECT value FROM meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn count_objects(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM objects")
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }

    pub async fn count_buckets(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM buckets")
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }

    pub async fn has_data(&self) -> Result<bool> {
        Ok(self.count_buckets().await? > 0 || self.count_objects().await? > 0)
    }

    pub async fn wipe_all(&self) -> Result<()> {
        sqlx::query("DELETE FROM multipart_uploads")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM objects")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM buckets")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM meta").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn export_snapshot(&self) -> Result<IndexSnapshot> {
        let buckets = self.list_buckets().await?;
        let objects = sqlx::query_as::<_, ObjectRow>(
            r#"
            SELECT bucket, key, etag, size, content_type, mtime, checksums_json, extents_json
            FROM objects ORDER BY bucket, key
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        let metadata = sqlx::query_as::<_, UserMeta>(
            "SELECT bucket, key, name, value FROM object_metadata ORDER BY bucket, key, name",
        )
        .fetch_all(&self.pool)
        .await?;
        let tags = sqlx::query_as::<_, ObjectTagRow>(
            "SELECT bucket, key, tag_key, tag_value FROM object_tags ORDER BY bucket, key, tag_key",
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
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM multipart_uploads")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM objects")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM buckets")
            .execute(&mut *tx)
            .await?;

        for b in &snap.buckets {
            sqlx::query(
                "INSERT INTO buckets (name, created_at, chat_id) VALUES (?, ?, ?)",
            )
            .bind(&b.name)
            .bind(&b.created_at)
            .bind(&b.chat_id)
            .execute(&mut *tx)
            .await?;
        }
        for o in &snap.objects {
            sqlx::query(
                r#"
                INSERT INTO objects
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
                "INSERT INTO object_metadata (bucket, key, name, value) VALUES (?, ?, ?, ?)",
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
                "INSERT INTO object_tags (bucket, key, tag_key, tag_value) VALUES (?, ?, ?, ?)",
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
        let initiated_at = Utc::now().to_rfc3339();
        let user_meta_json = serde_json::to_string(user_meta)?;
        let tagging_json = serde_json::to_string(tags)?;
        sqlx::query(
            r#"
            INSERT INTO multipart_uploads
                (upload_id, bucket, key, content_type, user_meta_json, tagging_json,
                 checksum_algorithm, initiated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(upload_id)
        .bind(bucket)
        .bind(key)
        .bind(content_type)
        .bind(user_meta_json)
        .bind(tagging_json)
        .bind(checksum_algorithm)
        .bind(initiated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_multipart_upload(&self, upload_id: &str) -> Result<Option<MultipartUpload>> {
        let row = sqlx::query_as::<_, MultipartUpload>(
            r#"
            SELECT upload_id, bucket, key, content_type, user_meta_json, tagging_json,
                   checksum_algorithm, initiated_at
            FROM multipart_uploads WHERE upload_id = ?
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
        let mut tx = self.pool.begin().await?;
        let old: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
        )
        .bind(upload_id)
        .bind(part_number)
        .fetch_optional(&mut *tx)
        .await?;
        let old_ids = old
            .map(|(j,)| chunk_ids_from_json(&j))
            .transpose()?
            .unwrap_or_default();

        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ? AND part_number = ?")
            .bind(upload_id)
            .bind(part_number)
            .execute(&mut *tx)
            .await?;

        let extents_json = serde_json::to_string(extents)?;
        sqlx::query(
            r#"
            INSERT INTO multipart_parts (upload_id, part_number, etag, size, extents_json)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(upload_id)
        .bind(part_number)
        .bind(etag)
        .bind(size)
        .bind(&extents_json)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(old_ids)
    }

    pub async fn get_multipart_part(
        &self,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultipartPart>> {
        let row = sqlx::query_as::<_, MultipartPart>(
            "SELECT upload_id, part_number, etag, size FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
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
            "SELECT extents_json FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
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
        let user_meta: Vec<(String, String)> =
            serde_json::from_str(&upload.user_meta_json).unwrap_or_default();
        let tags: Vec<(String, String)> =
            serde_json::from_str(&upload.tagging_json).unwrap_or_default();

        let mut tx = self.pool.begin().await?;
        let mut assembled: Vec<Extent> = Vec::new();
        for pn in part_numbers {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT extents_json FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
            )
            .bind(&upload.upload_id)
            .bind(pn)
            .fetch_optional(&mut *tx)
            .await?;
            let Some((j,)) = row else {
                anyhow::bail!("missing part {pn}");
            };
            let mut part_ext: Vec<Extent> = serde_json::from_str(&j)?;
            assembled.append(&mut part_ext);
        }

        let old_json: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM objects WHERE bucket = ? AND key = ?",
        )
        .bind(&upload.bucket)
        .bind(&upload.key)
        .fetch_optional(&mut *tx)
        .await?;
        let old_ids = old_json
            .map(|(j,)| chunk_ids_from_json(&j))
            .transpose()?
            .unwrap_or_default();

        sqlx::query("DELETE FROM object_metadata WHERE bucket = ? AND key = ?")
            .bind(&upload.bucket)
            .bind(&upload.key)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM object_tags WHERE bucket = ? AND key = ?")
            .bind(&upload.bucket)
            .bind(&upload.key)
            .execute(&mut *tx)
            .await?;

        let mtime = Utc::now().to_rfc3339();
        let extents_json = serde_json::to_string(&assembled)?;
        sqlx::query(
            r#"
            INSERT INTO objects
                (bucket, key, etag, size, content_type, mtime, checksums_json, extents_json)
            VALUES (?, ?, ?, ?, ?, ?, '{}', ?)
            ON CONFLICT(bucket, key) DO UPDATE SET
                etag = excluded.etag,
                size = excluded.size,
                content_type = excluded.content_type,
                mtime = excluded.mtime,
                checksums_json = excluded.checksums_json,
                extents_json = excluded.extents_json
            "#,
        )
        .bind(&upload.bucket)
        .bind(&upload.key)
        .bind(etag)
        .bind(total_size)
        .bind(&upload.content_type)
        .bind(&mtime)
        .bind(&extents_json)
        .execute(&mut *tx)
        .await?;

        for (name, value) in &user_meta {
            sqlx::query(
                "INSERT INTO object_metadata (bucket, key, name, value) VALUES (?, ?, ?, ?)",
            )
            .bind(&upload.bucket)
            .bind(&upload.key)
            .bind(name)
            .bind(value)
            .execute(&mut *tx)
            .await?;
        }
        for (tag_key, tag_value) in &tags {
            sqlx::query(
                "INSERT INTO object_tags (bucket, key, tag_key, tag_value) VALUES (?, ?, ?, ?)",
            )
            .bind(&upload.bucket)
            .bind(&upload.key)
            .bind(tag_key)
            .bind(tag_value)
            .execute(&mut *tx)
            .await?;
        }

        // Drop multipart rows without releasing part extents (ownership transferred).
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(&upload.upload_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(old_ids)
    }

    /// Abort upload; returns all part chunk ids to `release`, or `None` if missing.
    pub async fn abort_multipart_upload(&self, upload_id: &str) -> Result<Option<Vec<ChunkId>>> {
        if self.get_multipart_upload(upload_id).await?.is_none() {
            return Ok(None);
        }
        let mut tx = self.pool.begin().await?;
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM multipart_parts WHERE upload_id = ?",
        )
        .bind(upload_id)
        .fetch_all(&mut *tx)
        .await?;
        let mut ids = Vec::new();
        for (j,) in rows {
            ids.extend(chunk_ids_from_json(&j)?);
        }
        ids.sort_unstable();
        ids.dedup();
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(ids))
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
            FROM multipart_uploads
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
            FROM multipart_parts
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

fn chunk_ids_from_json(json: &str) -> Result<Vec<ChunkId>> {
    let extents: Vec<Extent> = serde_json::from_str(json).unwrap_or_default();
    Ok(unique_chunk_ids(&extents))
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
