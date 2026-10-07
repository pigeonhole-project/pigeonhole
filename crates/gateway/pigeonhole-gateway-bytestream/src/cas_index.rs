//! CAS mapping index owned by the bytestream gateway (stage F).
//!
//! Stores `CasEntry { hash, size, extents }` rows; chunk refs live in blob.db.

use anyhow::Result;
use chrono::{DateTime, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, FromRow, SqlitePool};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasEntry {
    pub hash: String,
    pub size: i64,
    pub extents: Vec<Extent>,
}

#[derive(Debug, Clone, FromRow)]
pub struct CasBlobRow {
    pub hash: String,
    pub size: i64,
    pub extents_json: String,
    pub last_access: String,
}

#[derive(Clone)]
pub struct CasIndex {
    pool: SqlitePool,
}

impl CasIndex {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let url = if database_url.starts_with("sqlite:") && !database_url.contains('?') {
            format!("{database_url}?mode=rwc")
        } else {
            database_url.to_string()
        };
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;
        let idx = Self { pool };
        idx.migrate().await?;
        Ok(idx)
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
            CREATE TABLE IF NOT EXISTS cas_blobs (
                hash TEXT NOT NULL,
                size INTEGER NOT NULL,
                extents_json TEXT NOT NULL,
                last_access TEXT NOT NULL,
                PRIMARY KEY (hash, size)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Drop legacy pending-delete queue (stage G: GC releases directly).
        let _ = sqlx::query("DROP TABLE IF EXISTS pending_cas_deletes")
            .execute(&self.pool)
            .await;

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

    pub async fn find_missing(&self, digests: &[(String, i64)]) -> Result<Vec<(String, i64)>> {
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

    pub async fn get(&self, hash: &str, size: i64) -> Result<Option<CasEntry>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM cas_blobs WHERE hash = ? AND size = ?",
        )
        .bind(hash)
        .bind(size)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((json,)) => {
                let extents: Vec<Extent> = serde_json::from_str(&json)?;
                Ok(Some(CasEntry {
                    hash: hash.to_string(),
                    size,
                    extents,
                }))
            }
        }
    }

    pub async fn touch(&self, hash: &str, size: i64) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE cas_blobs SET last_access = ? WHERE hash = ? AND size = ?")
            .bind(&now)
            .bind(hash)
            .bind(size)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Insert entry. Returns `true` if newly inserted (caller owns ingest refs).
    pub async fn insert(&self, entry: &CasEntry) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let json = serde_json::to_string(&entry.extents)?;
        let res = sqlx::query(
            r#"
            INSERT INTO cas_blobs (hash, size, extents_json, last_access)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(hash, size) DO UPDATE SET
                last_access = excluded.last_access
            "#,
        )
        .bind(&entry.hash)
        .bind(entry.size)
        .bind(&json)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        // rows_affected == 1 for insert; SQLite ON CONFLICT UPDATE also reports 1.
        // Detect prior existence separately.
        Ok(res.rows_affected() > 0)
    }

    /// Store a new CAS entry. If the digest already exists, returns existing extents
    /// so the caller can `release` the newly ingested chunks and `retain` existing ones.
    pub async fn store_or_get_existing(
        &self,
        entry: &CasEntry,
    ) -> Result<Option<Vec<Extent>>> {
        if let Some(existing) = self.get(&entry.hash, entry.size).await? {
            let _ = self.touch(&entry.hash, entry.size).await;
            return Ok(Some(existing.extents));
        }
        let now = Utc::now().to_rfc3339();
        let json = serde_json::to_string(&entry.extents)?;
        let res = sqlx::query(
            r#"
            INSERT OR IGNORE INTO cas_blobs (hash, size, extents_json, last_access)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(&entry.hash)
        .bind(entry.size)
        .bind(&json)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            // Lost race — return the winner's extents.
            let existing = self
                .get(&entry.hash, entry.size)
                .await?
                .expect("cas row after conflict");
            return Ok(Some(existing.extents));
        }
        Ok(None)
    }

    /// Remove CAS row; returns chunk ids to `release`.
    pub async fn release(&self, hash: &str, size: i64) -> Result<Vec<ChunkId>> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT extents_json FROM cas_blobs WHERE hash = ? AND size = ?",
        )
        .bind(hash)
        .bind(size)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((json,)) = row else {
            tx.commit().await?;
            return Ok(Vec::new());
        };
        let extents: Vec<Extent> = serde_json::from_str(&json).unwrap_or_default();
        let mut ids: Vec<_> = extents.iter().map(|e| e.chunk).collect();
        ids.sort_unstable();
        ids.dedup();
        sqlx::query("DELETE FROM cas_blobs WHERE hash = ? AND size = ?")
            .bind(hash)
            .bind(size)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(ids)
    }

    pub async fn stale_before(&self, cutoff: DateTime<Utc>) -> Result<Vec<CasBlobRow>> {
        let cutoff = cutoff.to_rfc3339();
        let rows = sqlx::query_as::<_, CasBlobRow>(
            "SELECT hash, size, extents_json, last_access FROM cas_blobs WHERE last_access < ?",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
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

    pub async fn export_entries(&self) -> Result<Vec<CasEntry>> {
        let rows = sqlx::query_as::<_, CasBlobRow>(
            "SELECT hash, size, extents_json, last_access FROM cas_blobs ORDER BY hash, size",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let extents: Vec<Extent> = serde_json::from_str(&r.extents_json)?;
            out.push(CasEntry {
                hash: r.hash,
                size: r.size,
                extents,
            });
        }
        Ok(out)
    }

    pub async fn import_entries(&self, entries: &[CasEntry]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM cas_blobs")
            .execute(&mut *tx)
            .await?;
        let now = Utc::now().to_rfc3339();
        for e in entries {
            let json = serde_json::to_string(&e.extents)?;
            sqlx::query(
                "INSERT INTO cas_blobs (hash, size, extents_json, last_access) VALUES (?, ?, ?, ?)",
            )
            .bind(&e.hash)
            .bind(e.size)
            .bind(&json)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
