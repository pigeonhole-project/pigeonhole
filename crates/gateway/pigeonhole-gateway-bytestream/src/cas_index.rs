//! CAS mapping index owned by the bytestream gateway.
//!
//! Maps `(hash, size)` to extents (and an action digest to its
//! `ActionResult`); chunk refs live in the chunk store. Reads look at the
//! tablet's in-memory [`CasState`]; changes go through `cas` commands. A
//! read appends nothing to the log: its time is noted in this process and
//! written as one `TouchMany` per GC pass ([`CasIndex::flush_touches`]).

use crate::commands::{Access, CasCommand, CasReply, CasState, DigestKey, NAMESPACE};
use anyhow::Result;
use chrono::{DateTime, SecondsFormat, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{MetaStore, Namespaces, Prepared};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasEntry {
    pub hash: String,
    pub size: i64,
    pub extents: Vec<Extent>,
}

/// Reads not yet written to the log: digest → when.
#[derive(Default)]
struct Touches {
    blobs: BTreeMap<DigestKey, String>,
    actions: BTreeMap<DigestKey, String>,
}

#[derive(Clone)]
pub struct CasIndex {
    meta: Arc<MetaStore>,
    touches: Arc<Mutex<Touches>>,
}

/// Now as the commands carry it: RFC 3339, fixed width (compared as text).
fn now() -> String {
    rfc3339(Utc::now())
}

fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl CasIndex {
    /// Add the `cas` namespace to the set passed to [`MetaStore::open`].
    pub fn register(namespaces: Namespaces) -> Namespaces {
        namespaces.with::<CasState>()
    }

    /// Whether this node leads the tablet's cluster with a quorum (only
    /// such a leader GCs).
    pub fn can_write(&self) -> bool {
        self.meta.has_quorum()
    }

    /// Use `meta`, which must have been opened with [`Self::register`].
    pub async fn open(meta: Arc<MetaStore>) -> Result<Self> {
        if !meta.has_namespace(NAMESPACE) {
            anyhow::bail!("metadata tablet opened without the cas namespace");
        }
        Ok(Self { meta, touches: Arc::default() })
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

    /// Note a read of a CAS blob (in memory, until [`Self::flush_touches`]).
    pub fn touch(&self, hash: &str, size: i64) {
        self.touches.lock().unwrap().blobs.insert((hash.to_string(), size), now());
    }

    /// Write the reads noted since the last call to the log, as one
    /// command; nothing when there were none.
    pub async fn flush_touches(&self) -> Result<()> {
        let taken = std::mem::take(&mut *self.touches.lock().unwrap());
        if taken.blobs.is_empty() && taken.actions.is_empty() {
            return Ok(());
        }
        let access = |m: &BTreeMap<DigestKey, String>| -> Vec<Access> {
            m.iter().map(|((hash, size), at)| Access { hash: hash.clone(), size: *size, at: at.clone() }).collect()
        };
        let cmd = CasCommand::TouchMany { blobs: access(&taken.blobs), actions: access(&taken.actions) };
        if let Err(e) = self.propose(cmd).await {
            // Keep them for the next pass (a read noted meanwhile is newer).
            let mut touches = self.touches.lock().unwrap();
            for (k, at) in taken.blobs {
                touches.blobs.entry(k).or_insert(at);
            }
            for (k, at) in taken.actions {
                touches.actions.entry(k).or_insert(at);
            }
            return Err(e);
        }
        Ok(())
    }

    /// The `PutEntry` of a new digest, to commit with the entry's chunks in
    /// one entry. Outcome `Existing` = another writer stored the digest
    /// first; release ours.
    pub fn prepare_store(&self, entry: &CasEntry) -> Prepared<CasState> {
        Prepared::new(&CasCommand::PutEntry {
            hash: entry.hash.clone(),
            size: entry.size,
            extents: entry.extents.clone(),
            at: now(),
        })
    }

    /// The tablet the CAS index lives in.
    pub fn meta(&self) -> &Arc<MetaStore> {
        &self.meta
    }

    /// Remove a CAS row; returns chunk ids to `release`.
    pub async fn release(&self, hash: &str, size: i64) -> Result<Vec<ChunkId>> {
        match self.propose(CasCommand::DropEntry { hash: hash.into(), size }).await? {
            CasReply::Dropped(ids) => Ok(ids),
            other => anyhow::bail!("unexpected cas reply {other:?}"),
        }
    }

    /// Drop the CAS entries and action results not read since `cutoff` (one
    /// command; reads noted in memory and not flushed yet count as reads);
    /// returns the chunk ids the dropped entries held, to `release`.
    pub async fn drop_stale(&self, cutoff: DateTime<Utc>) -> Result<Vec<ChunkId>> {
        let before = rfc3339(cutoff);
        let (blobs, actions) = {
            let touches = self.touches.lock().unwrap();
            self.meta.read::<CasState, _>(|s| {
                let blobs: Vec<DigestKey> = s
                    .blobs
                    .iter()
                    .filter(|(k, r)| r.last_access < before && !touches.blobs.contains_key(*k))
                    .map(|(k, _)| k.clone())
                    .collect();
                let actions: Vec<DigestKey> = s
                    .actions
                    .iter()
                    .filter(|(k, r)| r.last_access < before && !touches.actions.contains_key(*k))
                    .map(|(k, _)| k.clone())
                    .collect();
                (blobs, actions)
            })
        };
        if blobs.is_empty() && actions.is_empty() {
            return Ok(Vec::new());
        }
        match self.propose(CasCommand::DropStale { blobs, actions, before }).await? {
            CasReply::Dropped(ids) => Ok(ids),
            other => anyhow::bail!("unexpected cas reply {other:?}"),
        }
    }

    /// The `ActionResult` (encoded) cached for the action with this digest;
    /// the read is noted like a CAS read.
    pub fn get_action(&self, hash: &str, size: i64) -> Option<Vec<u8>> {
        let key = (hash.to_string(), size);
        let result = self.meta.read::<CasState, _>(|s| s.actions.get(&key).map(|r| r.result.clone()))?;
        self.touches.lock().unwrap().actions.insert(key, now());
        Some(result)
    }

    /// Cache `result` (an encoded `ActionResult`) for the action with this
    /// digest, replacing what was there.
    pub async fn put_action(&self, hash: &str, size: i64, result: Vec<u8>) -> Result<()> {
        self.propose(CasCommand::PutAction { hash: hash.into(), size, result, at: now() }).await?;
        Ok(())
    }
}
