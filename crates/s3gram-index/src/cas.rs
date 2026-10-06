use crate::Index;
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct CasBlobRow {
    pub hash: String,
    pub size: i64,
    pub file_id: String,
    pub last_access: String,
}

impl Index {
    pub async fn cas_find_missing(
        &self,
        digests: &[(String, i64)],
    ) -> Result<Vec<(String, i64)>> {
        let mut missing = Vec::new();
        for (hash, size) in digests {
            let row: Option<(i64,)> = sqlx::query_as(
                "SELECT 1 FROM cas_blobs WHERE hash = ? AND size = ?",
            )
            .bind(hash)
            .bind(size)
            .fetch_optional(&self.pool)
            .await?;
            if row.is_none() {
                missing.push((hash.clone(), *size));
            }
        }
        Ok(missing)
    }

    pub async fn cas_lookup(&self, hash: &str, size: i64) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT file_id FROM cas_blobs WHERE hash = ? AND size = ?",
        )
        .bind(hash)
        .bind(size)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn cas_touch(&self, hash: &str, size: i64) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE cas_blobs SET last_access = ? WHERE hash = ? AND size = ?",
        )
        .bind(&now)
        .bind(hash)
        .bind(size)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Register a CAS blob after payload is stored in `blobs` via [`crate::index::bump_blob`].
    pub async fn cas_insert(
        &self,
        hash: &str,
        size: i64,
        file_id: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO cas_blobs (hash, size, file_id, last_access)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(hash, size) DO UPDATE SET
                last_access = excluded.last_access
            "#,
        )
        .bind(hash)
        .bind(size)
        .bind(file_id)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn cas_release(&self, hash: &str, size: i64) -> Result<Option<crate::OrphanMsg>> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT file_id FROM cas_blobs WHERE hash = ? AND size = ?",
        )
        .bind(hash)
        .bind(size)
        .fetch_optional(&mut *tx)
        .await?;

        let Some((file_id,)) = row else {
            tx.commit().await?;
            return Ok(None);
        };

        sqlx::query("DELETE FROM cas_blobs WHERE hash = ? AND size = ?")
            .bind(hash)
            .bind(size)
            .execute(&mut *tx)
            .await?;

        let orphan = super::index::release_blob(&mut tx, &file_id).await?;
        tx.commit().await?;
        Ok(orphan)
    }

    /// Rows not accessed since `cutoff` (for TTL GC).
    pub async fn cas_stale_before(&self, cutoff: DateTime<Utc>) -> Result<Vec<CasBlobRow>> {
        let cutoff = cutoff.to_rfc3339();
        let rows = sqlx::query_as::<_, CasBlobRow>(
            "SELECT hash, size, file_id, last_access FROM cas_blobs WHERE last_access < ?",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn cas_queue_delete(&self, hash: &str, size: i64) -> Result<()> {
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
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn cas_list_pending_deletes(&self, limit: i64) -> Result<Vec<(String, i64)>> {
        sqlx::query_as(
            r#"
            SELECT hash, size FROM pending_cas_deletes
            ORDER BY queued_at ASC
            LIMIT ?
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn cas_clear_pending_delete(&self, hash: &str, size: i64) -> Result<()> {
        sqlx::query("DELETE FROM pending_cas_deletes WHERE hash = ? AND size = ?")
            .bind(hash)
            .bind(size)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Bump blob refcount and insert CAS mapping (idempotent on hash+size).
    pub async fn cas_store_new_blob(
        &self,
        hash: &str,
        size: i64,
        file_id: &str,
        message_id: i64,
        blob_size: i64,
        chat_id: &str,
        stored_crc32: Option<u32>,
    ) -> Result<()> {
        if size != blob_size {
            bail!("CAS digest size {size} != payload length {blob_size}");
        }
        let mut tx = self.pool.begin().await?;
        let exists: Option<(String,)> = sqlx::query_as(
            "SELECT file_id FROM cas_blobs WHERE hash = ? AND size = ?",
        )
        .bind(hash)
        .bind(size)
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_some() {
            let now = Utc::now().to_rfc3339();
            sqlx::query(
                "UPDATE cas_blobs SET last_access = ? WHERE hash = ? AND size = ?",
            )
            .bind(&now)
            .bind(hash)
            .bind(size)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(());
        }

        super::index::bump_blob(
            &mut tx,
            file_id,
            message_id,
            blob_size,
            chat_id,
            stored_crc32,
        )
        .await?;

        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO cas_blobs (hash, size, file_id, last_access)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(hash)
        .bind(size)
        .bind(file_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
