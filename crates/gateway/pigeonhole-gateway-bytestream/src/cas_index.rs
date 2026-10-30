//! CAS mapping index owned by the bytestream gateway.
//!
//! Maps `(hash, size)` to extents; chunk refs live in the chunk store. Reads
//! look at the tablet's in-memory [`CasState`]; changes go through `cas`
//! commands.

use crate::commands::{CasCommand, CasReply, CasState, NAMESPACE};
use anyhow::Result;
use chrono::{DateTime, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{MetaStore, Namespaces, Prepared};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasEntry {
    pub hash: String,
    pub size: i64,
    pub extents: Vec<Extent>,
}

/// A stored entry with its last access time (RFC 3339).
#[derive(Debug, Clone)]
pub struct CasBlobRow {
    pub hash: String,
    pub size: i64,
    pub extents: Vec<Extent>,
    pub last_access: String,
}

#[derive(Clone)]
pub struct CasIndex {
    meta: Arc<MetaStore>,
}

impl CasIndex {
    /// Add the `cas` namespace to the set passed to [`MetaStore::open`].
    pub fn register(namespaces: Namespaces) -> Namespaces {
        namespaces.with::<CasState>()
    }

    /// Use `meta`, which must have been opened with [`Self::register`].
    pub async fn open(meta: Arc<MetaStore>) -> Result<Self> {
        if !meta.has_namespace(NAMESPACE) {
            anyhow::bail!("metadata tablet opened without the cas namespace");
        }
        Ok(Self { meta })
    }

    async fn propose(&self, cmd: CasCommand) -> Result<CasReply> {
        let Ok(reply) = self.meta.propose::<CasState>(cmd).await?;
        Ok(reply)
    }

    pub async fn find_missing(&self, digests: &[(String, i64)]) -> Result<Vec<(String, i64)>> {
        Ok(self.meta.read::<CasState, _>(|s| {
            digests.iter().filter(|(hash, size)| !s.blobs.contains_key(&(hash.clone(), *size))).cloned().collect()
        }))
    }

    pub async fn get(&self, hash: &str, size: i64) -> Result<Option<CasEntry>> {
        Ok(self.meta.read::<CasState, _>(|s| {
            s.blobs.get(&(hash.to_string(), size)).map(|r| CasEntry {
                hash: hash.to_string(),
                size,
                extents: r.extents.clone(),
            })
        }))
    }

    pub async fn touch(&self, hash: &str, size: i64) -> Result<()> {
        let at = Utc::now().to_rfc3339();
        self.propose(CasCommand::TouchEntry { hash: hash.into(), size, at }).await?;
        Ok(())
    }

    /// Store a new CAS entry. If the digest already exists, returns existing extents
    /// so the caller can `release` the newly ingested chunks (the existing row keeps its ref).
    pub async fn store_or_get_existing(&self, entry: &CasEntry) -> Result<Option<Vec<Extent>>> {
        let cmd = CasCommand::PutEntry {
            hash: entry.hash.clone(),
            size: entry.size,
            extents: entry.extents.clone(),
            at: Utc::now().to_rfc3339(),
        };
        match self.propose(cmd).await? {
            CasReply::Stored => Ok(None),
            CasReply::Existing(extents) => Ok(Some(extents)),
            other => anyhow::bail!("unexpected cas reply {other:?}"),
        }
    }

    /// The `PutEntry` of [`Self::store_or_get_existing`], to commit with the
    /// entry's chunks in one entry. Outcome `Existing` = another writer stored
    /// the digest first; release ours.
    pub fn prepare_store(&self, entry: &CasEntry) -> Prepared<CasState> {
        Prepared::new(&CasCommand::PutEntry {
            hash: entry.hash.clone(),
            size: entry.size,
            extents: entry.extents.clone(),
            at: Utc::now().to_rfc3339(),
        })
    }

    /// The tablet the CAS index lives in.
    pub fn meta(&self) -> &Arc<MetaStore> {
        &self.meta
    }

    /// Remove CAS row; returns chunk ids to `release`.
    pub async fn release(&self, hash: &str, size: i64) -> Result<Vec<ChunkId>> {
        match self.propose(CasCommand::DropEntry { hash: hash.into(), size }).await? {
            CasReply::Dropped(ids) => Ok(ids),
            other => anyhow::bail!("unexpected cas reply {other:?}"),
        }
    }

    /// Entries last accessed before `cutoff`.
    pub async fn stale_before(&self, cutoff: DateTime<Utc>) -> Result<Vec<CasBlobRow>> {
        let cutoff = cutoff.to_rfc3339();
        Ok(self.meta.read::<CasState, _>(|s| {
            s.blobs
                .iter()
                .filter(|(_, r)| r.last_access < cutoff)
                .map(|((hash, size), r)| CasBlobRow {
                    hash: hash.clone(),
                    size: *size,
                    extents: r.extents.clone(),
                    last_access: r.last_access.clone(),
                })
                .collect()
        }))
    }
}
