//! CAS mapping index owned by the bytestream gateway.
//!
//! Stores `CasEntry { hash, size, extents }` rows; chunk refs live in the chunk
//! store. Queries read SQLite directly; changes go through `cas` commands.

use crate::commands::{from_pb, pb, to_pb, CasNamespace, NAMESPACE};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{Applied, MetaStore};
use prost::Message;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use std::sync::Arc;

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
    meta: Arc<MetaStore>,
}

/// Current schema of the `cas` namespace. No upgrades.
pub(crate) const SCHEMA: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS cas_blobs (
        hash TEXT NOT NULL,
        size INTEGER NOT NULL,
        extents_json TEXT NOT NULL,
        last_access TEXT NOT NULL,
        PRIMARY KEY (hash, size)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS cas_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    "#,
];

impl CasIndex {
    /// Open a standalone metadata store at `database_url` with only this namespace.
    pub async fn connect(database_url: &str) -> Result<Self> {
        Self::open(MetaStore::open(database_url).await?).await
    }

    /// Register the `cas` namespace on `meta`.
    pub async fn open(meta: Arc<MetaStore>) -> Result<Self> {
        meta.register(Arc::new(CasNamespace)).await?;
        Ok(Self {
            pool: meta.pool().clone(),
            meta,
        })
    }

    async fn propose(&self, op: pb::command::Op) -> Result<Vec<u8>> {
        let cmd = pb::Command { op: Some(op) }.encode_to_vec();
        match self.meta.propose(NAMESPACE, &cmd).await?.applied {
            Applied::Commit(reply) => Ok(reply),
            Applied::Reject(_) => anyhow::bail!("cas commands are never rejected"),
        }
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
        self.propose(pb::command::Op::TouchEntry(pb::TouchEntry {
            hash: hash.into(),
            size,
            at: Utc::now().to_rfc3339(),
        }))
        .await?;
        Ok(())
    }

    /// Store a new CAS entry. If the digest already exists, returns existing extents
    /// so the caller can `release` the newly ingested chunks (the existing row keeps its ref).
    pub async fn store_or_get_existing(&self, entry: &CasEntry) -> Result<Option<Vec<Extent>>> {
        let reply = self
            .propose(pb::command::Op::PutEntry(pb::PutEntry {
                hash: entry.hash.clone(),
                size: entry.size,
                extents: to_pb(&entry.extents),
                at: Utc::now().to_rfc3339(),
            }))
            .await?;
        let r = pb::PutResult::decode(reply.as_slice()).context("decode PutResult")?;
        Ok(r.existed.then(|| from_pb(r.existing)))
    }

    /// Remove CAS row; returns chunk ids to `release`.
    pub async fn release(&self, hash: &str, size: i64) -> Result<Vec<ChunkId>> {
        let reply = self
            .propose(pb::command::Op::DropEntry(pb::DropEntry {
                hash: hash.into(),
                size,
            }))
            .await?;
        Ok(pb::Released::decode(reply.as_slice())?.chunk_ids)
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
        let row: Option<(String,)> = sqlx::query_as("SELECT value FROM cas_meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO cas_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
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
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
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
