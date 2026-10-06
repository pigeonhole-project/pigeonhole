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

    async fn migrate(&self) -> Result<()> {
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            );
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
                initiated_at TEXT NOT NULL,
                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            );
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
                refcount INTEGER NOT NULL
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

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

        // Backfill blobs from existing chunk references (idempotent for empty blobs table).
        let (blob_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blobs")
            .fetch_one(&self.pool)
            .await?;
        if blob_count == 0 {
            sqlx::query(
                r#"
                INSERT INTO blobs (file_id, message_id, size, refcount)
                SELECT file_id, MIN(message_id), MAX(size), COUNT(*)
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

    pub async fn create_bucket(&self, name: &str) -> Result<bool> {
        let created_at = Utc::now().to_rfc3339();
        let res = sqlx::query("INSERT OR IGNORE INTO buckets (name, created_at) VALUES (?, ?)")
            .bind(name)
            .bind(created_at)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
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
            "SELECT name, created_at FROM buckets ORDER BY name",
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
        user_meta: &[(String, String)],
    ) -> Result<Vec<i64>> {
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
            if let Some(msg) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(msg);
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
            bump_blob(&mut tx, file_id, *message_id, *chunk_size).await?;
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

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Vec<i64>>> {
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
            if let Some(msg) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(msg);
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
    ) -> Result<(ObjectMeta, Vec<i64>)> {
        let src = self
            .get_object(src_bucket, src_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source object not found"))?;
        let src_chunks = self.get_chunks(src_bucket, src_key).await?;

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

        let orphans = self
            .put_object(
                dst_bucket,
                dst_key,
                &src.etag,
                src.size,
                ct,
                &chunk_tuples,
                &meta,
            )
            .await?;

        let dst = self
            .get_object(dst_bucket, dst_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("destination missing after copy"))?;
        Ok((dst, orphans))
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: i64,
        start_after: Option<&str>,
    ) -> Result<(Vec<ObjectMeta>, Vec<String>, bool)> {
        let mut objs = sqlx::query_as::<_, ObjectMeta>(
            r#"
            SELECT bucket, key, etag, size, content_type, mtime
            FROM objects
            WHERE bucket = ? AND key LIKE ?
            ORDER BY key
            "#,
        )
        .bind(bucket)
        .bind(format!("{prefix}%"))
        .fetch_all(&self.pool)
        .await?;

        if let Some(after) = start_after {
            objs.retain(|o| o.key.as_str() > after);
        }

        if let Some(delim) = delimiter {
            let mut keys = Vec::new();
            let mut common = Vec::new();
            let mut seen_prefixes = std::collections::BTreeSet::new();

            for obj in objs {
                let rest = &obj.key[prefix.len()..];
                if let Some(idx) = rest.find(delim) {
                    let cp = format!("{prefix}{}", &rest[..=idx]);
                    if seen_prefixes.insert(cp.clone()) {
                        common.push(cp);
                    }
                } else {
                    keys.push(obj);
                }
            }

            let truncated = (keys.len() + common.len()) as i64 > max_keys;
            keys.truncate(max_keys as usize);
            // Prefer keys first then common prefixes within max_keys budget
            let remaining = max_keys as usize - keys.len();
            common.truncate(remaining);
            return Ok((keys, common, truncated));
        }

        let truncated = objs.len() as i64 > max_keys;
        objs.truncate(max_keys as usize);
        Ok((objs, Vec::new(), truncated))
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

    pub async fn export_snapshot(&self) -> Result<IndexSnapshot> {
        let buckets = self.list_buckets().await?;
        let objects = sqlx::query_as::<_, ObjectMeta>(
            "SELECT bucket, key, etag, size, content_type, mtime FROM objects ORDER BY bucket, key",
        )
        .fetch_all(&self.pool)
        .await?;
        let chunks = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size FROM chunks ORDER BY bucket, key, part_no",
        )
        .fetch_all(&self.pool)
        .await?;
        let blobs = sqlx::query_as::<_, Blob>(
            "SELECT file_id, message_id, size, refcount FROM blobs ORDER BY file_id",
        )
        .fetch_all(&self.pool)
        .await?;
        let metadata = sqlx::query_as::<_, UserMeta>(
            "SELECT bucket, key, name, value FROM object_metadata ORDER BY bucket, key, name",
        )
        .fetch_all(&self.pool)
        .await?;
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
            sqlx::query("INSERT INTO buckets (name, created_at) VALUES (?, ?)")
                .bind(&b.name)
                .bind(&b.created_at)
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
                INSERT INTO blobs (file_id, message_id, size, refcount)
                SELECT file_id, MIN(message_id), MAX(size), COUNT(*)
                FROM chunks
                GROUP BY file_id
                "#,
            )
            .execute(&mut *tx)
            .await?;
        } else {
            for b in &snap.blobs {
                sqlx::query(
                    "INSERT INTO blobs (file_id, message_id, size, refcount) VALUES (?, ?, ?, ?)",
                )
                .bind(&b.file_id)
                .bind(b.message_id)
                .bind(b.size)
                .bind(b.refcount)
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
    ) -> Result<()> {
        let initiated_at = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO multipart_uploads (upload_id, bucket, key, content_type, initiated_at)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(upload_id)
        .bind(bucket)
        .bind(key)
        .bind(content_type)
        .bind(initiated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_multipart_upload(&self, upload_id: &str) -> Result<Option<MultipartUpload>> {
        let row = sqlx::query_as::<_, MultipartUpload>(
            "SELECT upload_id, bucket, key, content_type, initiated_at FROM multipart_uploads WHERE upload_id = ?",
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
    ) -> Result<Vec<i64>> {
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
            if let Some(msg) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(msg);
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
            bump_blob(&mut tx, file_id, *message_id, *chunk_size).await?;
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

    /// Finalize multipart: object takes ownership of part blobs (refcount net-zero transfer).
    pub async fn complete_multipart_upload(
        &self,
        upload: &MultipartUpload,
        part_numbers: &[i64],
        etag: &str,
        total_size: i64,
    ) -> Result<Vec<i64>> {
        let mut assembled: Vec<(i64, String, i64, i64)> = Vec::new();
        let mut part_chunks: Vec<MultipartPartChunk> = Vec::new();
        let mut part_no: i64 = 0;
        for pn in part_numbers {
            let chunks = self.list_multipart_part_chunks(&upload.upload_id, *pn).await?;
            for c in chunks {
                assembled.push((part_no, c.file_id.clone(), c.message_id, c.size));
                part_chunks.push(c);
                part_no += 1;
            }
        }

        // Bump via put_object (object refs), then release multipart refs.
        let mut orphans = self
            .put_object(
                &upload.bucket,
                &upload.key,
                etag,
                total_size,
                upload.content_type.as_deref(),
                &assembled,
                &[],
            )
            .await?;

        let mut tx = self.pool.begin().await?;
        for c in &part_chunks {
            if let Some(msg) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(msg);
            }
        }
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(&upload.upload_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(orphans)
    }

    pub async fn abort_multipart_upload(&self, upload_id: &str) -> Result<Option<Vec<i64>>> {
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
            if let Some(msg) = release_blob(&mut tx, &c.file_id).await? {
                orphans.push(msg);
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
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO blobs (file_id, message_id, size, refcount)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(file_id) DO UPDATE SET
            refcount = refcount + 1,
            message_id = excluded.message_id,
            size = excluded.size
        "#,
    )
    .bind(file_id)
    .bind(message_id)
    .bind(size)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Decrement refcount; if it hits zero, delete blob row and return message_id for TG cleanup.
async fn release_blob(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    file_id: &str,
) -> Result<Option<i64>> {
    let row: Option<(i64, i64)> =
        sqlx::query_as("SELECT message_id, refcount FROM blobs WHERE file_id = ?")
            .bind(file_id)
            .fetch_optional(&mut **tx)
            .await?;

    let Some((message_id, refcount)) = row else {
        return Ok(None);
    };

    if refcount <= 1 {
        sqlx::query("DELETE FROM blobs WHERE file_id = ?")
            .bind(file_id)
            .execute(&mut **tx)
            .await?;
        Ok(Some(message_id))
    } else {
        sqlx::query("UPDATE blobs SET refcount = refcount - 1 WHERE file_id = ?")
            .bind(file_id)
            .execute(&mut **tx)
            .await?;
        Ok(None)
    }
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
