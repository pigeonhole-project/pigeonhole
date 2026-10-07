use pigeonhole_codec::ChunkCodec;
use pigeonhole_codec::FrameRecord;
use pigeonhole_codec::{codec_from_sql, codec_to_sql, UploadedChunk};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, FromRow, SqlitePool};

#[derive(Clone)]
pub struct Index {
    pub(crate) pool: SqlitePool,
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

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Chunk {
    pub bucket: String,
    pub key: String,
    pub part_no: i64,
    pub file_id: String,
    pub message_id: i64,
    /// Logical (uncompressed) byte length of this slice.
    pub size: i64,
    /// On-wire encoding: `raw`, `gzip`, `zstd`, or `frames`.
    pub codec: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ChunkFrameRow {
    pub file_id: String,
    pub frame_no: i64,
    pub stored_off: i64,
    pub stored_len: i64,
    pub logical_off: i64,
    pub logical_len: i64,
    pub codec: String,
}

impl ChunkFrameRow {
    pub fn to_record(&self) -> FrameRecord {
        FrameRecord {
            frame_no: self.frame_no,
            stored_off: self.stored_off,
            stored_len: self.stored_len,
            logical_off: self.logical_off,
            logical_len: self.logical_len,
            codec: self.codec.clone(),
        }
    }
}

impl Chunk {
    pub fn stored_codec(&self) -> ChunkCodec {
        codec_from_sql(&self.codec).unwrap_or(ChunkCodec::Raw)
    }
}

impl<'de> Deserialize<'de> for Chunk {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct RawChunk {
            bucket: String,
            key: String,
            part_no: i64,
            file_id: String,
            message_id: i64,
            size: i64,
            #[serde(default)]
            codec: Option<String>,
            /// Legacy pin/index field from early compression builds.
            #[serde(default)]
            compressed: Option<bool>,
        }
        let r = RawChunk::deserialize(deserializer)?;
        let codec = match r.codec.filter(|s| !s.is_empty()) {
            Some(c) => c,
            None => match r.compressed {
                Some(true) => ChunkCodec::Gzip.as_str().to_string(),
                _ => ChunkCodec::Raw.as_str().to_string(),
            },
        };
        Ok(Self {
            bucket: r.bucket,
            key: r.key,
            part_no: r.part_no,
            file_id: r.file_id,
            message_id: r.message_id,
            size: r.size,
            codec,
        })
    }
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

/// Placement of a blob on a concrete backend (`tg:<chat_id>`, `memory:local`, …).
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct BlobReplica {
    pub file_id: String,
    pub backend_id: String,
    /// JSON [`pigeonhole_types::Locator`].
    pub locator: String,
    /// `ready` | `pending` | `failed` (policies later).
    pub state: String,
}

/// Telegram message that became unreferenced and should be deleted: (chat_id, message_id).
/// `(chat_id, message_id, file_id)` for GC delete + cache invalidation.
pub type OrphanMsg = (String, i64, String);

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

#[derive(Debug, Clone, FromRow)]
pub struct MultipartPartChunk {
    pub upload_id: String,
    pub part_number: i64,
    pub chunk_no: i64,
    pub file_id: String,
    pub message_id: i64,
    pub size: i64,
    pub codec: String,
}

impl MultipartPartChunk {
    pub fn stored_codec(&self) -> ChunkCodec {
        codec_from_sql(&self.codec).unwrap_or(ChunkCodec::Raw)
    }
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
    /// Optional: present in snapshots after frame packing landed.
    #[serde(default)]
    pub chunk_frames: Vec<ChunkFrameRow>,
    /// Optional: backend placement rows (Stage 3+).
    #[serde(default)]
    pub blob_replicas: Vec<BlobReplica>,
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
                checksums_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (bucket, key),
                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self.column_exists("objects", "checksums_json").await? {
            sqlx::query(
                "ALTER TABLE objects ADD COLUMN checksums_json TEXT NOT NULL DEFAULT '{}'",
            )
            .execute(&self.pool)
            .await?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chunks (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                part_no INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                codec TEXT NOT NULL DEFAULT 'raw',
                PRIMARY KEY (bucket, key, part_no),
                FOREIGN KEY (bucket, key) REFERENCES objects(bucket, key) ON DELETE CASCADE
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self.column_exists("chunks", "codec").await? {
            sqlx::query("ALTER TABLE chunks ADD COLUMN codec TEXT NOT NULL DEFAULT 'raw'")
                .execute(&self.pool)
                .await?;
            // Migrate briefly-lived `compressed` INTEGER column if present.
            if self.column_exists("chunks", "compressed").await? {
                sqlx::query("UPDATE chunks SET codec = 'gzip' WHERE compressed = 1")
                    .execute(&self.pool)
                    .await?;
            }
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chunk_frames (
                file_id TEXT NOT NULL,
                frame_no INTEGER NOT NULL,
                stored_off INTEGER NOT NULL,
                stored_len INTEGER NOT NULL,
                logical_off INTEGER NOT NULL,
                logical_len INTEGER NOT NULL,
                codec TEXT NOT NULL,
                PRIMARY KEY (file_id, frame_no)
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
                tagging_json TEXT NOT NULL DEFAULT '[]',
                checksum_algorithm TEXT,
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

        if !self
            .column_exists("multipart_uploads", "tagging_json")
            .await?
        {
            sqlx::query(
                "ALTER TABLE multipart_uploads ADD COLUMN tagging_json TEXT NOT NULL DEFAULT '[]'",
            )
            .execute(&self.pool)
            .await?;
        }

        if !self
            .column_exists("multipart_uploads", "checksum_algorithm")
            .await?
        {
            sqlx::query("ALTER TABLE multipart_uploads ADD COLUMN checksum_algorithm TEXT")
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
                codec TEXT NOT NULL DEFAULT 'raw',
                PRIMARY KEY (upload_id, part_number, chunk_no),
                FOREIGN KEY (upload_id, part_number)
                    REFERENCES multipart_parts(upload_id, part_number) ON DELETE CASCADE
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        if !self
            .column_exists("multipart_part_chunks", "codec")
            .await?
        {
            sqlx::query(
                "ALTER TABLE multipart_part_chunks ADD COLUMN codec TEXT NOT NULL DEFAULT 'raw'",
            )
            .execute(&self.pool)
            .await?;
            if self
                .column_exists("multipart_part_chunks", "compressed")
                .await?
            {
                sqlx::query(
                    "UPDATE multipart_part_chunks SET codec = 'gzip' WHERE compressed = 1",
                )
                .execute(&self.pool)
                .await?;
            }
        }

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
        if !self.column_exists("blobs", "stored_crc32").await? {
            sqlx::query("ALTER TABLE blobs ADD COLUMN stored_crc32 INTEGER")
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
            CREATE TABLE IF NOT EXISTS object_tags (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                tag_key TEXT NOT NULL,
                tag_value TEXT NOT NULL,
                PRIMARY KEY (bucket, key, tag_key),
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

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blob_replicas (
                file_id TEXT NOT NULL,
                backend_id TEXT NOT NULL,
                locator TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'ready',
                PRIMARY KEY (file_id, backend_id)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Backfill one replica per blob from legacy file_id/message_id/chat_id.
        let (replica_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM blob_replicas")
                .fetch_one(&self.pool)
                .await?;
        if replica_count == 0 {
            let blobs = sqlx::query_as::<_, Blob>(
                "SELECT file_id, message_id, size, refcount, chat_id FROM blobs",
            )
            .fetch_all(&self.pool)
            .await?;
            for b in blobs {
                let backend_id = if b.chat_id.is_empty() {
                    pigeonhole_types::BackendId::memory().0
                } else {
                    pigeonhole_types::BackendId::telegram(&b.chat_id).0
                };
                let locator = if b.chat_id.is_empty() {
                    pigeonhole_types::Locator::memory(&b.file_id, b.message_id)
                } else {
                    pigeonhole_types::Locator::telegram(&b.file_id, b.message_id)
                };
                let locator_json = locator.to_json()?;
                sqlx::query(
                    r#"
                    INSERT INTO blob_replicas (file_id, backend_id, locator, state)
                    VALUES (?, ?, ?, 'ready')
                    ON CONFLICT(file_id, backend_id) DO NOTHING
                    "#,
                )
                .bind(&b.file_id)
                .bind(&backend_id)
                .bind(&locator_json)
                .execute(&self.pool)
                .await?;
            }
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cas_blobs (
                hash TEXT NOT NULL,
                size INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                last_access TEXT NOT NULL,
                PRIMARY KEY (hash, size)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Chunked CAS manifests (JSON). Legacy single-blob rows keep file_id and NULL manifest.
        let _ = sqlx::query(
            "ALTER TABLE cas_blobs ADD COLUMN manifest TEXT",
        )
        .execute(&self.pool)
        .await;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pending_cas_deletes (
                hash TEXT NOT NULL,
                size INTEGER NOT NULL,
                queued_at TEXT NOT NULL,
                PRIMARY KEY (hash, size)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

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
        chunks: &[UploadedChunk],
        chat_id: &str,
        user_meta: &[(String, String)],
        checksums_json: &str,
    ) -> Result<Vec<OrphanMsg>> {
        let mut tx = self.pool.begin().await?;
        let mut orphans = Vec::new();

        let old_chunks = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size, codec FROM chunks WHERE bucket = ? AND key = ?",
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
        sqlx::query("DELETE FROM object_tags WHERE bucket = ? AND key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;

        let mtime = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO objects (bucket, key, etag, size, content_type, mtime, checksums_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket, key) DO UPDATE SET
                etag = excluded.etag,
                size = excluded.size,
                content_type = excluded.content_type,
                mtime = excluded.mtime,
                checksums_json = excluded.checksums_json
            "#,
        )
        .bind(bucket)
        .bind(key)
        .bind(etag)
        .bind(size)
        .bind(content_type)
        .bind(&mtime)
        .bind(checksums_json)
        .execute(&mut *tx)
        .await?;

        for c in chunks {
            bump_blob(
                &mut tx,
                &c.file_id,
                c.message_id,
                c.logical_size,
                chat_id,
                c.stored_crc32,
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO chunks (bucket, key, part_no, file_id, message_id, size, codec)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(bucket)
            .bind(key)
            .bind(c.part_no)
            .bind(&c.file_id)
            .bind(c.message_id)
            .bind(c.logical_size)
            .bind(codec_to_sql(c.codec))
            .execute(&mut *tx)
            .await?;
            // Shallow copies pass empty frames and must not wipe existing rows.
            if !c.frames.is_empty() {
                replace_chunk_frames(&mut tx, &c.file_id, &c.frames).await?;
            }
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

    pub async fn get_chunks(&self, bucket: &str, key: &str) -> Result<Vec<Chunk>> {
        let rows = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size, codec FROM chunks WHERE bucket = ? AND key = ? ORDER BY part_no",
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
            "SELECT bucket, key, part_no, file_id, message_id, size, codec FROM chunks WHERE bucket = ? AND key = ?",
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
        let chunk_tuples: Vec<UploadedChunk> = src_chunks
            .iter()
            .map(|c| UploadedChunk {
                part_no: c.part_no,
                file_id: c.file_id.clone(),
                message_id: c.message_id,
                logical_size: c.size,
                codec: c.stored_codec(),
                // Frames stay keyed by file_id; shallow copy reuses them.
                frames: Vec::new(),
                stored_crc32: None,
            })
            .collect();

        let meta: Vec<(String, String)> = if copy_source_meta {
            self.get_user_metadata(src_bucket, src_key).await?
        } else {
            user_meta.to_vec()
        };
        let checksums_json = self
            .get_object_checksums_json(src_bucket, src_key)
            .await?
            .unwrap_or_else(|| "{}".to_string());

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
                &checksums_json,
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

    /// All Telegram `message_id`s known to the index (blobs, chunks, multipart, pending deletes).
    pub async fn list_tracked_message_ids(&self) -> Result<Vec<i64>> {
        let mut ids: Vec<i64> = Vec::new();
        let mut push_rows = |rows: Vec<(i64,)>| {
            for (id,) in rows {
                ids.push(id);
            }
        };
        push_rows(
            sqlx::query_as("SELECT message_id FROM blobs")
                .fetch_all(&self.pool)
                .await?,
        );
        push_rows(
            sqlx::query_as("SELECT message_id FROM chunks")
                .fetch_all(&self.pool)
                .await?,
        );
        push_rows(
            sqlx::query_as("SELECT message_id FROM multipart_part_chunks")
                .fetch_all(&self.pool)
                .await?,
        );
        push_rows(
            sqlx::query_as("SELECT message_id FROM pending_tg_deletes")
                .fetch_all(&self.pool)
                .await?,
        );
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
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

    /// True when the index has any buckets or objects (unsafe to silent-overwrite).
    pub async fn has_data(&self) -> Result<bool> {
        Ok(self.count_buckets().await? > 0 || self.count_objects().await? > 0)
    }

    /// Wipe all S3/index state. Does not touch Telegram; call after deleting tracked messages.
    pub async fn wipe_all(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM pending_tg_deletes")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM multipart_part_chunks")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM multipart_parts")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM multipart_uploads")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM object_tags")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM object_metadata")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chunk_frames")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chunks")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM objects")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM blob_replicas")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pending_cas_deletes")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM cas_blobs")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM blobs")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM buckets")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM meta").execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_chunk_frames(&self, file_id: &str) -> Result<Vec<FrameRecord>> {
        let rows = sqlx::query_as::<_, ChunkFrameRow>(
            r#"
            SELECT file_id, frame_no, stored_off, stored_len, logical_off, logical_len, codec
            FROM chunk_frames WHERE file_id = ? ORDER BY frame_no
            "#,
        )
        .bind(file_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.to_record()).collect())
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
            "SELECT bucket, key, part_no, file_id, message_id, size, codec FROM chunks ORDER BY bucket, key, part_no",
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
        let chunk_frames = sqlx::query_as::<_, ChunkFrameRow>(
            r#"
            SELECT file_id, frame_no, stored_off, stored_len, logical_off, logical_len, codec
            FROM chunk_frames ORDER BY file_id, frame_no
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;
        let blob_replicas = sqlx::query_as::<_, BlobReplica>(
            r#"
            SELECT file_id, backend_id, locator, state
            FROM blob_replicas ORDER BY file_id, backend_id
            "#,
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
            chunk_frames,
            blob_replicas,
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
        sqlx::query("DELETE FROM chunk_frames")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chunks").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM blob_replicas")
            .execute(&mut *tx)
            .await?;
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
                "INSERT INTO chunks (bucket, key, part_no, file_id, message_id, size, codec) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&c.bucket)
            .bind(&c.key)
            .bind(c.part_no)
            .bind(&c.file_id)
            .bind(c.message_id)
            .bind(c.size)
            .bind(codec_to_sql(c.stored_codec()))
            .execute(&mut *tx)
            .await?;
        }
        for f in &snap.chunk_frames {
            sqlx::query(
                r#"
                INSERT INTO chunk_frames
                    (file_id, frame_no, stored_off, stored_len, logical_off, logical_len, codec)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&f.file_id)
            .bind(f.frame_no)
            .bind(f.stored_off)
            .bind(f.stored_len)
            .bind(f.logical_off)
            .bind(f.logical_len)
            .bind(&f.codec)
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
        if snap.blob_replicas.is_empty() {
            // Derive replicas from imported blobs (legacy snapshots).
            let blobs = sqlx::query_as::<_, Blob>(
                "SELECT file_id, message_id, size, refcount, chat_id FROM blobs",
            )
            .fetch_all(&mut *tx)
            .await?;
            for b in blobs {
                upsert_replica_tx(&mut tx, &b.file_id, b.message_id, &b.chat_id).await?;
            }
        } else {
            for r in &snap.blob_replicas {
                sqlx::query(
                    r#"
                    INSERT INTO blob_replicas (file_id, backend_id, locator, state)
                    VALUES (?, ?, ?, ?)
                    "#,
                )
                .bind(&r.file_id)
                .bind(&r.backend_id)
                .bind(&r.locator)
                .bind(&r.state)
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

    pub async fn put_multipart_part(
        &self,
        upload_id: &str,
        part_number: i64,
        etag: &str,
        size: i64,
        chunks: &[UploadedChunk],
        chat_id: &str,
    ) -> Result<Vec<OrphanMsg>> {
        let mut tx = self.pool.begin().await?;
        let mut orphans = Vec::new();

        let old = sqlx::query_as::<_, MultipartPartChunk>(
            r#"
            SELECT upload_id, part_number, chunk_no, file_id, message_id, size, codec
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

        for c in chunks {
            bump_blob(
                &mut tx,
                &c.file_id,
                c.message_id,
                c.logical_size,
                chat_id,
                c.stored_crc32,
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO multipart_part_chunks
                    (upload_id, part_number, chunk_no, file_id, message_id, size, codec)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(upload_id)
            .bind(part_number)
            .bind(c.part_no)
            .bind(&c.file_id)
            .bind(c.message_id)
            .bind(c.logical_size)
            .bind(codec_to_sql(c.codec))
            .execute(&mut *tx)
            .await?;
            if !c.frames.is_empty() {
                replace_chunk_frames(&mut tx, &c.file_id, &c.frames).await?;
            }
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
            SELECT upload_id, part_number, chunk_no, file_id, message_id, size, codec
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
        let tags: Vec<(String, String)> =
            serde_json::from_str(&upload.tagging_json).unwrap_or_default();

        let mut tx = self.pool.begin().await?;
        let mut assembled: Vec<UploadedChunk> = Vec::new();
        let mut part_chunks: Vec<MultipartPartChunk> = Vec::new();
        let mut part_no: i64 = 0;

        for pn in part_numbers {
            let chunks = sqlx::query_as::<_, MultipartPartChunk>(
                r#"
                SELECT upload_id, part_number, chunk_no, file_id, message_id, size, codec
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
                assembled.push(UploadedChunk {
                    part_no,
                    file_id: c.file_id.clone(),
                    message_id: c.message_id,
                    logical_size: c.size,
                    codec: c.stored_codec(),
                    frames: Vec::new(),
                    stored_crc32: None,
                });
                part_chunks.push(c);
                part_no += 1;
            }
        }

        // Replace destination object inside this transaction.
        let old_chunks = sqlx::query_as::<_, Chunk>(
            "SELECT bucket, key, part_no, file_id, message_id, size, codec FROM chunks WHERE bucket = ? AND key = ?",
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
        sqlx::query("DELETE FROM object_tags WHERE bucket = ? AND key = ?")
            .bind(&upload.bucket)
            .bind(&upload.key)
            .execute(&mut *tx)
            .await?;

        let mtime = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO objects (bucket, key, etag, size, content_type, mtime, checksums_json)
            VALUES (?, ?, ?, ?, ?, ?, '{}')
            ON CONFLICT(bucket, key) DO UPDATE SET
                etag = excluded.etag,
                size = excluded.size,
                content_type = excluded.content_type,
                mtime = excluded.mtime,
                checksums_json = excluded.checksums_json
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

        for c in &assembled {
            bump_blob(
                &mut tx,
                &c.file_id,
                c.message_id,
                c.logical_size,
                &chat_id,
                c.stored_crc32,
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO chunks (bucket, key, part_no, file_id, message_id, size, codec)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&upload.bucket)
            .bind(&upload.key)
            .bind(c.part_no)
            .bind(&c.file_id)
            .bind(c.message_id)
            .bind(c.logical_size)
            .bind(codec_to_sql(c.codec))
            .execute(&mut *tx)
            .await?;
            // Frames already written at UploadPart time for these file_ids.
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
            SELECT upload_id, part_number, chunk_no, file_id, message_id, size, codec
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

    /// List in-progress multipart uploads for a bucket (lexicographic by key, upload_id).
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

    /// List uploaded parts for a multipart upload (after `part_number_marker`).
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

pub(crate) async fn bump_blob(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    file_id: &str,
    message_id: i64,
    size: i64,
    chat_id: &str,
    stored_crc32: Option<u32>,
) -> Result<()> {
    let crc = stored_crc32.map(|c| c as i64);
    sqlx::query(
        r#"
        INSERT INTO blobs (file_id, message_id, size, refcount, chat_id, stored_crc32)
        VALUES (?, ?, ?, 1, ?, ?)
        ON CONFLICT(file_id) DO UPDATE SET
            refcount = refcount + 1,
            stored_crc32 = COALESCE(excluded.stored_crc32, blobs.stored_crc32)
        "#,
    )
    .bind(file_id)
    .bind(message_id)
    .bind(size)
    .bind(chat_id)
    .bind(crc)
    .execute(&mut **tx)
    .await?;
    upsert_replica_tx(tx, file_id, message_id, chat_id).await?;
    Ok(())
}

async fn upsert_replica_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    file_id: &str,
    message_id: i64,
    chat_id: &str,
) -> Result<()> {
    let (backend_id, locator) = if chat_id.is_empty() {
        (
            pigeonhole_types::BackendId::memory(),
            pigeonhole_types::Locator::memory(file_id, message_id),
        )
    } else if let Some((mid, aid)) = pigeonhole_types::Locator::parse_discord_store_file_id(file_id) {
        (
            pigeonhole_types::BackendId::discord(chat_id),
            pigeonhole_types::Locator::discord(chat_id, mid, aid, ""),
        )
    } else {
        (
            pigeonhole_types::BackendId::telegram(chat_id),
            pigeonhole_types::Locator::telegram(file_id, message_id),
        )
    };
    let backend_id = backend_id.0;
    let locator_json = locator.to_json()?;
    sqlx::query(
        r#"
        INSERT INTO blob_replicas (file_id, backend_id, locator, state)
        VALUES (?, ?, ?, 'ready')
        ON CONFLICT(file_id, backend_id) DO UPDATE SET
            locator = excluded.locator,
            state = 'ready'
        "#,
    )
    .bind(file_id)
    .bind(&backend_id)
    .bind(&locator_json)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Decrement refcount; if it hits zero, delete blob row and return (chat_id, message_id, file_id).
pub(crate) async fn release_blob(
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
        sqlx::query("DELETE FROM chunk_frames WHERE file_id = ?")
            .bind(file_id)
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM blob_replicas WHERE file_id = ?")
            .bind(file_id)
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM blobs WHERE file_id = ?")
            .bind(file_id)
            .execute(&mut **tx)
            .await?;
        Ok(Some((chat_id, message_id, file_id.to_string())))
    } else {
        sqlx::query("UPDATE blobs SET refcount = refcount - 1 WHERE file_id = ?")
            .bind(file_id)
            .execute(&mut **tx)
            .await?;
        Ok(None)
    }
}

async fn replace_chunk_frames(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    file_id: &str,
    frames: &[FrameRecord],
) -> Result<()> {
    sqlx::query("DELETE FROM chunk_frames WHERE file_id = ?")
        .bind(file_id)
        .execute(&mut **tx)
        .await?;
    for f in frames {
        sqlx::query(
            r#"
            INSERT INTO chunk_frames
                (file_id, frame_no, stored_off, stored_len, logical_off, logical_len, codec)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(file_id)
        .bind(f.frame_no)
        .bind(f.stored_off)
        .bind(f.stored_len)
        .bind(f.logical_off)
        .bind(f.logical_len)
        .bind(&f.codec)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
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
