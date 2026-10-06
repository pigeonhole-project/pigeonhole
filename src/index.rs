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
    ) -> Result<Vec<Chunk>> {
        let mut tx = self.pool.begin().await?;

        let old_chunks = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size FROM chunks WHERE bucket = ? AND key = ?",
        )
        .bind(bucket)
        .bind(key)
        .fetch_all(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM chunks WHERE bucket = ? AND key = ?")
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

        tx.commit().await?;
        Ok(old_chunks)
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

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Vec<Chunk>>> {
        let chunks = self.get_chunks(bucket, key).await?;
        let res = sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&self.pool)
            .await?;
        // chunks cascade-deleted
        if res.rows_affected() == 0 {
            Ok(None)
        } else {
            Ok(Some(chunks))
        }
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
        Ok(IndexSnapshot {
            buckets,
            objects,
            chunks,
        })
    }

    pub async fn import_snapshot(&self, snap: &IndexSnapshot) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM chunks").execute(&mut *tx).await?;
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
    ) -> Result<Vec<MultipartPartChunk>> {
        let mut tx = self.pool.begin().await?;

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
        Ok(old)
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

    /// Finalize multipart: assemble object chunks and drop upload metadata (TG blobs stay).
    pub async fn complete_multipart_upload(
        &self,
        upload: &MultipartUpload,
        part_numbers: &[i64],
        etag: &str,
        total_size: i64,
    ) -> Result<Vec<Chunk>> {
        let mut assembled: Vec<(i64, String, i64, i64)> = Vec::new();
        let mut part_no: i64 = 0;
        for pn in part_numbers {
            let chunks = self.list_multipart_part_chunks(&upload.upload_id, *pn).await?;
            for c in chunks {
                assembled.push((part_no, c.file_id, c.message_id, c.size));
                part_no += 1;
            }
        }

        let old = self
            .put_object(
                &upload.bucket,
                &upload.key,
                etag,
                total_size,
                upload.content_type.as_deref(),
                &assembled,
            )
            .await?;

        // Remove multipart rows only (do not touch TG messages — they are now object chunks)
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(&upload.upload_id)
            .execute(&self.pool)
            .await?;

        Ok(old)
    }

    pub async fn abort_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Result<Option<(MultipartUpload, Vec<MultipartPartChunk>)>> {
        let upload = match self.get_multipart_upload(upload_id).await? {
            Some(u) => u,
            None => return Ok(None),
        };
        let chunks = sqlx::query_as::<_, MultipartPartChunk>(
            r#"
            SELECT upload_id, part_number, chunk_no, file_id, message_id, size
            FROM multipart_part_chunks
            WHERE upload_id = ?
            "#,
        )
        .bind(upload_id)
        .fetch_all(&self.pool)
        .await?;

        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(upload_id)
            .execute(&self.pool)
            .await?;

        Ok(Some((upload, chunks)))
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
