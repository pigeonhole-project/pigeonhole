//! CAS mapping index owned by the bytestream gateway.
//!
//! Maps `(hash, size)` to extents; chunk refs live in the chunk store. Reads
//! look at the tablet's in-memory [`CasState`]; changes go through `cas`
//! commands.

use crate::commands::{from_pb, pb, to_pb, CasState, NAMESPACE};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{Applied, MetaStore, Namespaces};
use prost::Message;
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
    /// Open a standalone tablet at `database_url` (local log next to it) with
    /// only this namespace.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let namespaces = Self::register(Namespaces::new());
        Self::open(MetaStore::open_local(database_url, namespaces).await?).await
    }

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

    async fn propose(&self, op: pb::command::Op) -> Result<Vec<u8>> {
        let cmd = pb::Command { op: Some(op) }.encode_to_vec();
        match self.meta.propose(NAMESPACE, &cmd).await?.applied {
            Applied::Commit(reply) => Ok(reply),
            Applied::Reject(_) => anyhow::bail!("cas commands are never rejected"),
        }
    }

    pub async fn find_missing(&self, digests: &[(String, i64)]) -> Result<Vec<(String, i64)>> {
        Ok(self.meta.read::<CasState, _>(|s| {
            digests
                .iter()
                .filter(|(hash, size)| !s.blobs.contains_key(&(hash.clone(), *size)))
                .cloned()
                .collect()
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
