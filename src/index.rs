use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
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
pub struct Chunk {
    pub bucket: String,
    pub key: String,
    pub part_no: i64,
    pub file_id: String,
    pub message_id: i64,
    pub size: i64,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Blob {
    pub file_id: String,
    pub message_id: i64,
    pub size: i64,
    pub refcount: i64,
    #[serde(default)]
    pub chat_id: String,
}

/// Telegram message that became unreferenced and should be deleted: (chat_id, message_id).
pub type OrphanMsg = (String, i64);

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
    pub initiated_at: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct MultipartPart {
    pub upload_id: String,
    pub part_number: i64,
    pub etag: String,
    pub size: i64,
}

#[derive(Debug, Clone, FromRow)]
pub struct MultipartPartChunk {
    pub upload_id: String,
    pub part_number: i64,
    pub chunk_no: i64,
    pub file_id: String,
    pub message_id: i64,
    pub size: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexSnapshot {
    pub buckets: Vec<Bucket>,
    pub objects: Vec<ObjectMeta>,
    pub chunks: Vec<Chunk>,
    #[serde(default)]
    pub blobs: Vec<Blob>,
    #[serde(default)]
    pub metadata: Vec<UserMeta>,
}

impl Index {
    pub async fn connect(database_url: &str) -> Result<Self> {
        // sqlx sqlite URLs: sqlite:path or sqlite://path
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

    async fn column_exists(&self, table: &str, column: &str) -> Result<bool> {
        let (n,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?"
        ))
        .bind(column)
        .fetch_one(&self.pool)
        .await?;
        Ok(n > 0)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&self.pool)
            .await?;
        // WAL lets readers (snapshot export) proceed while writers run.
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
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self.column_exists("buckets", "chat_id").await? {
            sqlx::query("ALTER TABLE buckets ADD COLUMN chat_id TEXT NOT NULL DEFAULT ''")
                .execute(&self.pool)
                .await?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS objects (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                etag TEXT NOT NULL,
                size INTEGER NOT NULL,
                content_type TEXT,
                mtime TEXT NOT NULL,
                PRIMARY KEY (bucket, key),
                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chunks (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                part_no INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                PRIMARY KEY (bucket, key, part_no),
                FOREIGN KEY (bucket, key) REFERENCES objects(bucket, key) ON DELETE CASCADE
            );
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
                initiated_at TEXT NOT NULL,
                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self
            .column_exists("multipart_uploads", "user_meta_json")
            .await?
        {
            sqlx::query(
                "ALTER TABLE multipart_uploads ADD COLUMN user_meta_json TEXT NOT NULL DEFAULT '[]'",
            )
            .execute(&self.pool)
            .await?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS multipart_parts (
                upload_id TEXT NOT NULL,
                part_number INTEGER NOT NULL,
                etag TEXT NOT NULL,
                size INTEGER NOT NULL,
                PRIMARY KEY (upload_id, part_number),
                FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS multipart_part_chunks (
                upload_id TEXT NOT NULL,
                part_number INTEGER NOT NULL,
                chunk_no INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                PRIMARY KEY (upload_id, part_number, chunk_no),
                FOREIGN KEY (upload_id, part_number)
                    REFERENCES multipart_parts(upload_id, part_number) ON DELETE CASCADE
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blobs (
                file_id TEXT PRIMARY KEY,
                message_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                refcount INTEGER NOT NULL,
                chat_id TEXT NOT NULL DEFAULT ''
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self.column_exists("blobs", "chat_id").await? {
            sqlx::query("ALTER TABLE blobs ADD COLUMN chat_id TEXT NOT NULL DEFAULT ''")
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
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pending_tg_deletes (
                chat_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                queued_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                PRIMARY KEY (chat_id, message_id)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self
            .column_exists("pending_tg_deletes", "attempts")
            .await?
        {
            sqlx::query(
                "ALTER TABLE pending_tg_deletes ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
            )
            .execute(&self.pool)
            .await?;
        }
        if !self
            .column_exists("pending_tg_deletes", "last_error")
            .await?
        {
            sqlx::query("ALTER TABLE pending_tg_deletes ADD COLUMN last_error TEXT")
                .execute(&self.pool)
                .await?;
        }

        // Backfill blobs from existing chunk references (idempotent for empty blobs table).
        let (blob_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blobs")
            .fetch_one(&self.pool)
            .await?;
        if blob_count == 0 {
            sqlx::query(
                r#"
                INSERT INTO blobs (file_id, message_id, size, refcount, chat_id)
                SELECT file_id, MIN(message_id), MAX(size), COUNT(*), ''
                FROM (
                    SELECT file_id, message_id, size FROM chunks
                    UNION ALL
                    SELECT file_id, message_id, size FROM multipart_part_chunks
                )
                GROUP BY file_id
                "#,
            )
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    /// Backfill empty `chat_id` on buckets/blobs with `CHAT_ID`.
    /// Safe to call on every startup.
    pub async fn migrate_legacy_chat_ids(&self, legacy_chat_id: &str) -> Result<u64> {
        if legacy_chat_id.is_empty() {
            return Ok(0);
        }
        let r1 = sqlx::query(
            "UPDATE buckets SET chat_id = ? WHERE chat_id IS NULL OR chat_id = ''",
        )
        .bind(legacy_chat_id)
        .execute(&self.pool)
        .await?;
        let r2 = sqlx::query(
            "UPDATE blobs SET chat_id = ? WHERE chat_id IS NULL OR chat_id = ''",
        )
        .bind(legacy_chat_id)
        .execute(&self.pool)
        .await?;
        Ok(r1.rows_affected() + r2.rows_affected())
    }

    /// Ensure the reserved service bucket exists and maps to the service Telegram chat.
    pub async fn ensure_service_bucket(&self, name: &str, chat_id: &str) -> Result<()> {
        let created_at = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO buckets (name, created_at, chat_id) VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET chat_id = excluded.chat_id
            "#,
        )
        .bind(name)
        .bind(&created_at)
        .bind(chat_id)
        .execute(&self.pool)
        .await?;
        Ok(())
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

    pub async fn bucket_using_chat(&self, chat_id: &str) -> Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT name FROM buckets WHERE chat_id = ? LIMIT 1")
                .bind(chat_id)
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

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        etag: &str,
        size: i64,
        content_type: Option<&str>,
        chunks: &[(i64, String, i64, i64)],
        chat_id: &str,
        user_meta: &[(String, String)],
    ) -> Result<Vec<OrphanMsg>> {
        let mut tx = self.pool.begin().await?;
        let mut orphans = Vec::new();

        let old_chunks = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size FROM chunks WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_all(&mut *tx)
        .await?;

        for c in &old_chunks {
            if let Some(o) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(o);
            }
        }

        sqlx::query("DELETE FROM chunks WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM object_metadata WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;

        let mtime = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO objects (bucket, key, etag, size, content_type, mtime)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket, key) DO UPDATE SET
                etag = excluded.etag,
                size = excluded.size,
                content_type = excluded.content_type,
                mtime = excluded.mtime
            "#,
        )
        .bind(bucket)
        .bind(key)
        .bind(etag)
        .bind(size)
        .bind(content_type)
        .bind(&mtime)
        .execute(&mut *tx)
        .await?;

        for (part_no, file_id, message_id, chunk_size) in chunks {
            bump_blob(&mut tx, file_id, *message_id, *chunk_size, chat_id).await?;
            sqlx::query(
                r#"
                INSERT INTO chunks (bucket, key, part_no, file_id, message_id, size)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(bucket)
            .bind(key)
            .bind(part_no)
            .bind(file_id)
            .bind(message_id)
            .bind(chunk_size)
            .execute(&mut *tx)
            .await?;
        }

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
        Ok(orphans)
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

    pub async fn get_chunks(&self, bucket: &str, key: &str) -> Result<Vec<Chunk>> {
        let rows = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size FROM chunks WHERE bucket = ? AND key = ? ORDER BY part_no",
        )
        .bind(bucket)
        .bind(key)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Vec<OrphanMsg>>> {
        let mut tx = self.pool.begin().await?;
        let chunks = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size FROM chunks WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_all(&mut *tx)
        .await?;

        let res = sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;

        if res.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(None);
        }

        // chunks/metadata cascade-deleted; release blob refs
        let mut orphans = Vec::new();
        for c in chunks {
            if let Some(o) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(o);
            }
        }
        tx.commit().await?;
        Ok(Some(orphans))
    }

    /// Shallow copy: destination reuses the same Telegram file_ids (refcount++).
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        content_type: Option<&str>,
        user_meta: &[(String, String)],
        copy_source_meta: bool,
    ) -> Result<(ObjectMeta, Vec<OrphanMsg>)> {
        let src = self
            .get_object(src_bucket, src_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source object not found"))?;
        let src_chunks = self.get_chunks(src_bucket, src_key).await?;
        let src_chat = self
            .bucket_chat_id(src_bucket)
            .await?
            .unwrap_or_default();

        let ct = content_type.or(src.content_type.as_deref());
        let chunk_tuples: Vec<(i64, String, i64, i64)> = src_chunks
            .iter()
            .map(|c| (c.part_no, c.file_id.clone(), c.message_id, c.size))
            .collect();

        let meta: Vec<(String, String)> = if copy_source_meta {
            self.get_user_metadata(src_bucket, src_key).await?
        } else {
            user_meta.to_vec()
        };

        // Shallow copy keeps blobs in the source chat (refcount++).
        let orphans = self
            .put_object(
                dst_bucket,
                dst_key,
                &src.etag,
                src.size,
                ct,
                &chunk_tuples,
                &src_chat,
                &meta,
            )
            .await?;

        let dst = self
            .get_object(dst_bucket, dst_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("destination missing after copy"))?;
        Ok((dst, orphans))
    }

    /// List objects with prefix range scan (case-sensitive) and S3-correct
    /// delimiter pagination. Returns (objects, common_prefixes, truncated, next_token).
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
            // After an object key: key > cursor. After a common prefix: key >= succ(cp).
            let mut cursor: Option<(bool /*inclusive*/, String)> = match start_after {
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
        // Prefix as range so SQLite can use an index on (bucket, key) / key.
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

    pub async fn queue_tg_delete(&self, chat_id: &str, message_id: i64) -> Result<()> {
        let queued_at = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO pending_tg_deletes (chat_id, message_id, queued_at, attempts, last_error)
            VALUES (?, ?, ?, 0, NULL)
            ON CONFLICT(chat_id, message_id) DO NOTHING
            "#,
        )
        .bind(chat_id)
        .bind(message_id)
        .bind(queued_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Prefer entries with fewer attempts so permanent failures cannot block the queue.
    pub async fn list_pending_tg_deletes(
        &self,
        limit: i64,
        max_attempts: i64,
    ) -> Result<Vec<(String, i64, i64)>> {
        sqlx::query_as(
            r#"
            SELECT chat_id, message_id, attempts
            FROM pending_tg_deletes
            WHERE attempts < ?
            ORDER BY attempts ASC, queued_at ASC
            LIMIT ?
            "#,
        )
        .bind(max_attempts)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn bump_pending_tg_delete(
        &self,
        chat_id: &str,
        message_id: i64,
        error: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE pending_tg_deletes
            SET attempts = attempts + 1, last_error = ?
            WHERE chat_id = ? AND message_id = ?
            "#,
        )
        .bind(error)
        .bind(chat_id)
        .bind(message_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn clear_pending_tg_delete(&self, chat_id: &str, message_id: i64) -> Result<()> {
        sqlx::query(
            "DELETE FROM pending_tg_deletes WHERE chat_id = ? AND message_id = ?",
        )
        .bind(chat_id)
        .bind(message_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Drop permanently failed deletes so they stop occupying the queue.
    pub async fn drop_exhausted_pending_tg_deletes(&self, max_attempts: i64) -> Result<u64> {
        let r = sqlx::query("DELETE FROM pending_tg_deletes WHERE attempts >= ?")
            .bind(max_attempts)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected())
    }

    pub async fn export_snapshot(&self) -> Result<IndexSnapshot> {
        let mut tx = self.pool.begin().await?;
        let buckets = sqlx::query_as::<_, Bucket>(
            "SELECT name, created_at, chat_id FROM buckets ORDER BY name",
        )
        .fetch_all(&mut *tx)
        .await?;
        let objects = sqlx::query_as::<_, ObjectMeta>(
            "SELECT bucket, key, etag, size, content_type, mtime FROM objects ORDER BY bucket, key",
        )
        .fetch_all(&mut *tx)
        .await?;
        let chunks = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size FROM chunks ORDER BY bucket, key, part_no",
        )
        .fetch_all(&mut *tx)
        .await?;
        let blobs = sqlx::query_as::<_, Blob>(
            "SELECT file_id, message_id, size, refcount, chat_id FROM blobs ORDER BY file_id",
        )
        .fetch_all(&mut *tx)
        .await?;
        let metadata = sqlx::query_as::<_, UserMeta>(
            "SELECT bucket, key, name, value FROM object_metadata ORDER BY bucket, key, name",
        )
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(IndexSnapshot {
            buckets,
            objects,
            chunks,
            blobs,
            metadata,
        })
    }

    pub async fn import_snapshot(&self, snap: &IndexSnapshot) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM multipart_part_chunks")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM multipart_parts")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM multipart_uploads")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM object_metadata")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chunks").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM blobs").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM objects").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM buckets").execute(&mut *tx).await?;

        for b in &snap.buckets {
            sqlx::query("INSERT INTO buckets (name, created_at, chat_id) VALUES (?, ?, ?)")
                .bind(&b.name)
                .bind(&b.created_at)
                .bind(&b.chat_id)
                .execute(&mut *tx)
                .await?;
        }
        for o in &snap.objects {
            sqlx::query(
                "INSERT INTO objects (bucket, key, etag, size, content_type, mtime) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&o.bucket)
            .bind(&o.key)
            .bind(&o.etag)
            .bind(o.size)
            .bind(&o.content_type)
            .bind(&o.mtime)
            .execute(&mut *tx)
            .await?;
        }
        for c in &snap.chunks {
            sqlx::query(
                "INSERT INTO chunks (bucket, key, part_no, file_id, message_id, size) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&c.bucket)
            .bind(&c.key)
            .bind(c.part_no)
            .bind(&c.file_id)
            .bind(c.message_id)
            .bind(c.size)
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
        if snap.blobs.is_empty() {
            sqlx::query(
                r#"
                INSERT INTO blobs (file_id, message_id, size, refcount, chat_id)
                SELECT file_id, MIN(message_id), MAX(size), COUNT(*), ''
                FROM chunks
                GROUP BY file_id
                "#,
            )
            .execute(&mut *tx)
            .await?;
        } else {
            for b in &snap.blobs {
                sqlx::query(
                    "INSERT INTO blobs (file_id, message_id, size, refcount, chat_id) VALUES (?, ?, ?, ?, ?)",
                )
                .bind(&b.file_id)
                .bind(b.message_id)
                .bind(b.size)
                .bind(b.refcount)
                .bind(&b.chat_id)
                .execute(&mut *tx)
                .await?;
            }
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
    ) -> Result<()> {
        let initiated_at = Utc::now().to_rfc3339();
        let user_meta_json = serde_json::to_string(user_meta)?;
        sqlx::query(
            r#"
            INSERT INTO multipart_uploads
                (upload_id, bucket, key, content_type, user_meta_json, initiated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(upload_id)
        .bind(bucket)
        .bind(key)
        .bind(content_type)
        .bind(user_meta_json)
        .bind(initiated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_multipart_upload(&self, upload_id: &str) -> Result<Option<MultipartUpload>> {
        let row = sqlx::query_as::<_, MultipartUpload>(
            "SELECT upload_id, bucket, key, content_type, user_meta_json, initiated_at FROM multipart_uploads WHERE upload_id = ?",
        )
        .bind(upload_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn put_multipart_part(
        &self,
        upload_id: &str,
        part_number: i64,
        etag: &str,
        size: i64,
        chunks: &[(i64, String, i64, i64)],
        chat_id: &str,
    ) -> Result<Vec<OrphanMsg>> {
        let mut tx = self.pool.begin().await?;
        let mut orphans = Vec::new();

        let old = sqlx::query_as::<_, MultipartPartChunk>(
            r#"
            SELECT upload_id, part_number, chunk_no, file_id, message_id, size
            FROM multipart_part_chunks
            WHERE upload_id = ? AND part_number = ?
            "#,
        )
        .bind(upload_id)
        .bind(part_number)
        .fetch_all(&mut *tx)
        .await?;

        for c in &old {
            if let Some(o) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(o);
            }
        }

        sqlx::query("DELETE FROM multipart_part_chunks WHERE upload_id = ? AND part_number = ?")
            .bind(upload_id)
            .bind(part_number)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ? AND part_number = ?")
            .bind(upload_id)
            .bind(part_number)
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            "INSERT INTO multipart_parts (upload_id, part_number, etag, size) VALUES (?, ?, ?, ?)",
        )
        .bind(upload_id)
        .bind(part_number)
        .bind(etag)
        .bind(size)
        .execute(&mut *tx)
        .await?;

        for (chunk_no, file_id, message_id, chunk_size) in chunks {
            bump_blob(&mut tx, file_id, *message_id, *chunk_size, chat_id).await?;
            sqlx::query(
                r#"
                INSERT INTO multipart_part_chunks
                    (upload_id, part_number, chunk_no, file_id, message_id, size)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(upload_id)
            .bind(part_number)
            .bind(chunk_no)
            .bind(file_id)
            .bind(message_id)
            .bind(chunk_size)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(orphans)
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

    pub async fn list_multipart_part_chunks(
        &self,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Vec<MultipartPartChunk>> {
        let rows = sqlx::query_as::<_, MultipartPartChunk>(
            r#"
            SELECT upload_id, part_number, chunk_no, file_id, message_id, size
            FROM multipart_part_chunks
            WHERE upload_id = ? AND part_number = ?
            ORDER BY chunk_no
            "#,
        )
        .bind(upload_id)
        .bind(part_number)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Finalize multipart atomically: object ownership + release part refs + delete upload.
    pub async fn complete_multipart_upload(
        &self,
        upload: &MultipartUpload,
        part_numbers: &[i64],
        etag: &str,
        total_size: i64,
    ) -> Result<Vec<OrphanMsg>> {
        let chat_id = self
            .bucket_chat_id(&upload.bucket)
            .await?
            .unwrap_or_default();
        let user_meta: Vec<(String, String)> =
            serde_json::from_str(&upload.user_meta_json).unwrap_or_default();

        let mut tx = self.pool.begin().await?;
        let mut assembled: Vec<(i64, String, i64, i64)> = Vec::new();
        let mut part_chunks: Vec<MultipartPartChunk> = Vec::new();
        let mut part_no: i64 = 0;

        for pn in part_numbers {
            let chunks = sqlx::query_as::<_, MultipartPartChunk>(
                r#"
                SELECT upload_id, part_number, chunk_no, file_id, message_id, size
                FROM multipart_part_chunks
                WHERE upload_id = ? AND part_number = ?
                ORDER BY chunk_no
                "#,
            )
            .bind(&upload.upload_id)
            .bind(pn)
            .fetch_all(&mut *tx)
            .await?;
            for c in chunks {
                assembled.push((part_no, c.file_id.clone(), c.message_id, c.size));
                part_chunks.push(c);
                part_no += 1;
            }
        }

        // Replace destination object inside this transaction.
        let old_chunks = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size FROM chunks WHERE bucket = ? AND key = ?",
        )
        .bind(&upload.bucket)
        .bind(&upload.key)
        .fetch_all(&mut *tx)
        .await?;

        let mut orphans = Vec::new();
        for c in &old_chunks {
            if let Some(o) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(o);
            }
        }
        sqlx::query("DELETE FROM chunks WHERE bucket = ? AND key = ?")
            .bind(&upload.bucket)
            .bind(&upload.key)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM object_metadata WHERE bucket = ? AND key = ?")
            .bind(&upload.bucket)
            .bind(&upload.key)
            .execute(&mut *tx)
            .await?;

        let mtime = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO objects (bucket, key, etag, size, content_type, mtime)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket, key) DO UPDATE SET
                etag = excluded.etag,
                size = excluded.size,
                content_type = excluded.content_type,
                mtime = excluded.mtime
            "#,
        )
        .bind(&upload.bucket)
        .bind(&upload.key)
        .bind(etag)
        .bind(total_size)
        .bind(&upload.content_type)
        .bind(&mtime)
        .execute(&mut *tx)
        .await?;

        for (pno, file_id, message_id, chunk_size) in &assembled {
            bump_blob(&mut tx, file_id, *message_id, *chunk_size, &chat_id).await?;
            sqlx::query(
                r#"
                INSERT INTO chunks (bucket, key, part_no, file_id, message_id, size)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&upload.bucket)
            .bind(&upload.key)
            .bind(pno)
            .bind(file_id)
            .bind(message_id)
            .bind(chunk_size)
            .execute(&mut *tx)
            .await?;
        }

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

        // Release multipart refs (net-zero with bumps above for shared file_ids).
        for c in &part_chunks {
            if let Some(o) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(o);
            }
        }
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(&upload.upload_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(orphans)
    }

    pub async fn abort_multipart_upload(&self, upload_id: &str) -> Result<Option<Vec<OrphanMsg>>> {
        let upload = match self.get_multipart_upload(upload_id).await? {
            Some(u) => u,
            None => return Ok(None),
        };
        let _ = upload;

        let mut tx = self.pool.begin().await?;
        let chunks = sqlx::query_as::<_, MultipartPartChunk>(
            r#"
            SELECT upload_id, part_number, chunk_no, file_id, message_id, size
            FROM multipart_part_chunks
            WHERE upload_id = ?
            "#,
        )
        .bind(upload_id)
        .fetch_all(&mut *tx)
        .await?;

        let mut orphans = Vec::new();
        for c in &chunks {
            if let Some(o) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(o);
            }
        }

        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(Some(orphans))
    }
}

async fn bump_blob(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    file_id: &str,
    message_id: i64,
    size: i64,
    chat_id: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO blobs (file_id, message_id, size, refcount, chat_id)
        VALUES (?, ?, ?, 1, ?)
        ON CONFLICT(file_id) DO UPDATE SET
            refcount = refcount + 1
        "#,
    )
    .bind(file_id)
    .bind(message_id)
    .bind(size)
    .bind(chat_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Decrement refcount; if it hits zero, delete blob row and return (chat_id, message_id).
async fn release_blob(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    file_id: &str,
) -> Result<Option<OrphanMsg>> {
    let row: Option<(String, i64, i64)> =
        sqlx::query_as("SELECT chat_id, message_id, refcount FROM blobs WHERE file_id = ?")
            .bind(file_id)
            .fetch_optional(&mut **tx)
            .await?;

    let Some((chat_id, message_id, refcount)) = row else {
        return Ok(None);
    };

    if refcount <= 1 {
        sqlx::query("DELETE FROM blobs WHERE file_id = ?")
            .bind(file_id)
            .execute(&mut **tx)
            .await?;
        Ok(Some((chat_id, message_id)))
    } else {
        sqlx::query("UPDATE blobs SET refcount = refcount - 1 WHERE file_id = ?")
            .bind(file_id)
            .execute(&mut **tx)
            .await?;
        Ok(None)
    }
}

enum DelimEntry {
    Object(ObjectMeta),
    Prefix(String),
}

/// Smallest string strictly greater than all keys with the given prefix (byte successor).
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

/// Group object keys into interleaved (sort_key, key|common-prefix) entries.
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

#[derive(Debug, PartialEq, Eq)]
pub enum DeleteBucketResult {
    Deleted,
    NotFound,
    NotEmpty,
}

pub fn parse_rfc3339(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}
