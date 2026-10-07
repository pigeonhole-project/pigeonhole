//! CAS mapping index owned by the bytestream gateway (stage 1.5).
//!
//! Today this shares the SQLite pool with the S3 [`Index`] for blob refcounts
//! (`bump_blob` / `release_blob`). Stage 1.6 moves refcounts into `blob.db`.

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use pigeonhole_blob_store::Index;
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct CasBlobRow {
    pub hash: String,
    pub size: i64,
    pub file_id: String,
    pub last_access: String,
    pub manifest: Option<String>,
}

/// Resolved CAS object: legacy single blob or chunked manifest JSON.
#[derive(Debug, Clone)]
pub struct CasEntry {
    pub file_id: String,
    pub manifest: Option<String>,
}

/// Gateway-owned CAS index (wrapper over the shared pool until blob.db owns refs).
#[derive(Clone)]
pub struct CasIndex {
    index: Index,
}

impl CasIndex {
    pub fn new(index: Index) -> Self {
        Self { index }
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub async fn find_missing(&self, digests: &[(String, i64)]) -> Result<Vec<(String, i64)>> {
        let mut missing = Vec::new();
        for (hash, size) in digests {
            let row: Option<(i64,)> = sqlx::query_as(
                "SELECT 1 FROM cas_blobs WHERE hash = ? AND size = ?",
            )
            .bind(hash)
            .bind(size)
            .fetch_optional(self.index.pool())
            .await?;
            if row.is_none() {
                missing.push((hash.clone(), *size));
            }
        }
        Ok(missing)
    }

    pub async fn get(&self, hash: &str, size: i64) -> Result<Option<CasEntry>> {
        let row: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT file_id, manifest FROM cas_blobs WHERE hash = ? AND size = ?",
        )
        .bind(hash)
        .bind(size)
        .fetch_optional(self.index.pool())
        .await?;
        Ok(row.map(|(file_id, manifest)| CasEntry { file_id, manifest }))
    }

    pub async fn touch(&self, hash: &str, size: i64) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE cas_blobs SET last_access = ? WHERE hash = ? AND size = ?")
            .bind(&now)
            .bind(hash)
            .bind(size)
            .execute(self.index.pool())
            .await?;
        Ok(())
    }

    pub async fn release(&self, hash: &str, size: i64) -> Result<Vec<pigeonhole_blob_store::OrphanMsg>> {
        self.index.cas_release(hash, size).await
    }

    pub async fn stale_before(&self, cutoff: DateTime<Utc>) -> Result<Vec<CasBlobRow>> {
        let cutoff = cutoff.to_rfc3339();
        let rows = sqlx::query_as::<_, CasBlobRow>(
            "SELECT hash, size, file_id, last_access, manifest FROM cas_blobs WHERE last_access < ?",
        )
        .bind(cutoff)
        .fetch_all(self.index.pool())
        .await?;
        Ok(rows)
    }

    pub async fn queue_delete(&self, hash: &str, size: i64) -> Result<()> {
        let queued_at = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO pending_cas_deletes (hash, size, queued_at)
            VALUES (?, ?, ?)
            ON CONFLICT(hash, size) DO NOTHING
            "#,
        )
        .bind(hash)
        .bind(size)
        .bind(queued_at)
        .execute(self.index.pool())
        .await?;
        Ok(())
    }

    pub async fn list_pending_deletes(&self, limit: i64) -> Result<Vec<(String, i64)>> {
        sqlx::query_as(
            r#"
            SELECT hash, size FROM pending_cas_deletes
            ORDER BY queued_at ASC
            LIMIT ?
            "#,
        )
        .bind(limit)
        .fetch_all(self.index.pool())
        .await
        .map_err(Into::into)
    }

    pub async fn clear_pending_delete(&self, hash: &str, size: i64) -> Result<()> {
        sqlx::query("DELETE FROM pending_cas_deletes WHERE hash = ? AND size = ?")
            .bind(hash)
            .bind(size)
            .execute(self.index.pool())
            .await?;
        Ok(())
    }

    pub async fn store_manifest(
        &self,
        hash: &str,
        size: i64,
        chat_id: &str,
        chunks: &[(String, i64, i64, Option<u32>)],
        manifest_json: &str,
    ) -> Result<()> {
        self.index
            .cas_store_manifest(hash, size, chat_id, chunks, manifest_json)
            .await
    }

    pub async fn store_entry(
        &self,
        hash: &str,
        size: i64,
        file_id: &str,
        message_id: i64,
        blob_size: i64,
        chat_id: &str,
        stored_crc32: Option<u32>,
        manifest_json: Option<&str>,
    ) -> Result<()> {
        if size != blob_size && manifest_json.is_none() {
            bail!("CAS digest size {size} != payload length {blob_size}");
        }
        self.index
            .cas_store_entry(
                hash,
                size,
                file_id,
                message_id,
                blob_size,
                chat_id,
                stored_crc32,
                manifest_json,
            )
            .await
    }
}
