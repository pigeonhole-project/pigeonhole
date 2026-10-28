//! The chunk-store namespace of the metadata tablet.
//!
//! [`ChunkState`] holds instances, chunks (blocks, replicas, refs, lifecycle)
//! and the chunk id high-water mark in memory; [`ChunkState::apply`] is the only
//! code that changes them. It must stay deterministic: no clocks, no id
//! generation, no metrics — those belong to the proposer in [`crate::BlobDb`].
//! Every check runs before the first mutation, so a rejected command leaves
//! the state untouched.

use crate::blob_db::{ChunkId, StoredBlock};
use anyhow::{bail, Result};
use pigeonhole_blob::{BlobId, BlobLocator, PartLayout, ReplicaLayout};
use pigeonhole_meta::{Outcome, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

pub(crate) const NAMESPACE: &str = "chunk";

/// Commands of the `chunk` namespace. Changing the shape of a variant or the
/// order of variants needs a new [`ChunkState::VERSION`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkCommand {
    /// Upsert configured instances; instances not listed become retired.
    SyncInstances { instances: Vec<(String, InstanceRec)> },
    /// New chunk with refs = 1 after a successful quorum write.
    CommitChunk {
        chunk_id: ChunkId,
        logical_size: i64,
        crc32: u32,
        blocks: Vec<StoredBlock>,
        replicas: Vec<Replica>,
        created_at: String,
    },
    /// All-or-nothing refs + 1.
    Retain { chunk_ids: Vec<ChunkId> },
    /// All-or-nothing refs - 1.
    Release { chunk_ids: Vec<ChunkId> },
    /// live with refs = 0 → reclaiming (or confirm an already reclaiming chunk).
    BeginReclaim { chunk_id: ChunkId },
    /// Forget a reclaiming chunk after its backend parts are deleted.
    DropChunk { chunk_id: ChunkId },
    /// Replace one instance's replica of a chunk (repair / backfill).
    AddReplica { chunk_id: ChunkId, replica: Replica },
}

/// Deterministic failure of a [`ChunkCommand`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkReject {
    /// Retain: chunk missing, refs = 0, or reclaiming.
    ChunkGone(ChunkId),
    /// Release: refs already 0, chunk missing, or reclaiming.
    DoubleRelease(ChunkId),
    /// BeginReclaim: chunk is referenced again or already gone.
    NotReclaimable(ChunkId),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceRec {
    pub kind: String,
    pub fingerprint: String,
    pub location: String,
    /// read-write | read-only | retired
    pub state: String,
}

/// One instance's copy of a chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replica {
    pub instance: String,
    pub parts: Vec<PartRec>,
}

/// One part of a replica: a blob holding `block_count` blocks from `first_block`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRec {
    pub first_block: u32,
    pub block_count: u32,
    pub sort_key: Vec<u8>,
    pub locator: Vec<u8>,
    /// Our id of the blob (its uploaded file name).
    pub blob_id: BlobId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChunkRec {
    pub logical_size: i64,
    pub crc32: u32,
    pub refs: i64,
    /// Parts are being deleted; `retain` fails, the sweeper finishes the job.
    pub reclaiming: bool,
    pub created_at: String,
    /// Ordered by `block_no`.
    pub blocks: Vec<StoredBlock>,
    /// Instance id → parts in order.
    pub replicas: BTreeMap<String, Vec<PartRec>>,
}

impl ChunkRec {
    /// Referenced and not being reclaimed: its parts must stay.
    pub fn is_live(&self) -> bool {
        self.refs > 0 && !self.reclaiming
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ChunkState {
    pub(crate) instances: BTreeMap<String, InstanceRec>,
    pub(crate) chunks: BTreeMap<ChunkId, ChunkRec>,
    /// `(instance, sort_key)` → owning chunk; a backend key belongs to one part.
    #[serde(skip)]
    pub(crate) by_key: HashMap<(String, Vec<u8>), ChunkId>,
    /// Highest chunk id ever committed: ids are never reused (block cache and
    /// extents are keyed by chunk id).
    pub(crate) last_chunk_id: ChunkId,
}

type Applied = std::result::Result<(), ChunkReject>;

impl StateMachine for ChunkState {
    const NAME: &'static str = NAMESPACE;
    const VERSION: u32 = 2;
    type Command = ChunkCommand;
    type Reply = ();
    type Reject = ChunkReject;

    fn apply(&mut self, command: ChunkCommand) -> Result<Outcome<Self>> {
        match command {
            ChunkCommand::SyncInstances { instances } => self.sync_instances(instances),
            ChunkCommand::CommitChunk { chunk_id, logical_size, crc32, blocks, replicas, created_at } => {
                self.commit_chunk(chunk_id, logical_size, crc32, blocks, replicas, created_at)
            }
            ChunkCommand::Retain { chunk_ids } => Ok(self.retain(&chunk_ids)),
            ChunkCommand::Release { chunk_ids } => Ok(self.release(&chunk_ids)),
            ChunkCommand::BeginReclaim { chunk_id } => Ok(self.begin_reclaim(chunk_id)),
            ChunkCommand::DropChunk { chunk_id } => {
                self.drop_chunk(chunk_id);
                Ok(Ok(()))
            }
            ChunkCommand::AddReplica { chunk_id, replica } => self.add_replica(chunk_id, replica),
        }
    }

    fn rebuild(&mut self) {
        self.by_key = self
            .chunks
            .iter()
            .flat_map(|(&id, c)| {
                c.replicas.iter().flat_map(move |(instance, parts)| {
                    parts.iter().map(move |p| ((instance.clone(), p.sort_key.clone()), id))
                })
            })
            .collect();
    }
}

impl ChunkState {
    fn sync_instances(&mut self, configured: Vec<(String, InstanceRec)>) -> Result<Applied> {
        let mut instances = self.instances.clone();
        let ids: HashSet<&str> = configured.iter().map(|(id, _)| id.as_str()).collect();
        for (id, rec) in instances.iter_mut() {
            if !ids.contains(id.as_str()) {
                rec.state = "retired".into();
            }
        }
        instances.extend(configured);
        let mut locations = HashSet::new();
        for (id, rec) in &instances {
            if rec.state == "read-write" && !locations.insert(rec.location.as_str()) {
                bail!("instance {id}: location {} already has a read-write instance", rec.location);
            }
        }
        self.instances = instances;
        Ok(Ok(()))
    }

    /// A replica's parts must name a known instance and keys no other part owns.
    fn check_replica(&self, chunk_id: ChunkId, rep: &Replica) -> Result<()> {
        if !self.instances.contains_key(&rep.instance) {
            bail!("chunk {chunk_id}: unknown instance {}", rep.instance);
        }
        let mut seen = HashSet::new();
        for part in &rep.parts {
            if !seen.insert(part.sort_key.as_slice()) {
                bail!("chunk {chunk_id}: duplicate sort_key on instance {}", rep.instance);
            }
            let owner = self.by_key.get(&(rep.instance.clone(), part.sort_key.clone()));
            if let Some(&owner) = owner.filter(|&&o| o != chunk_id) {
                bail!(
                    "chunk {chunk_id}: key {} on {} belongs to chunk {owner}",
                    hex::encode(&part.sort_key),
                    rep.instance
                );
            }
        }
        Ok(())
    }

    fn put_replica(&mut self, chunk_id: ChunkId, rep: Replica) {
        for p in &rep.parts {
            self.by_key.insert((rep.instance.clone(), p.sort_key.clone()), chunk_id);
        }
        let chunk = self.chunks.get_mut(&chunk_id).expect("caller checked the chunk");
        chunk.replicas.insert(rep.instance, rep.parts);
    }

    fn remove_replica(&mut self, chunk_id: ChunkId, instance: &str) -> Option<Replica> {
        let parts = self.chunks.get_mut(&chunk_id)?.replicas.remove(instance)?;
        for p in &parts {
            self.by_key.remove(&(instance.to_string(), p.sort_key.clone()));
        }
        Some(Replica { instance: instance.to_string(), parts })
    }

    fn commit_chunk(
        &mut self,
        chunk_id: ChunkId,
        logical_size: i64,
        crc32: u32,
        mut blocks: Vec<StoredBlock>,
        replicas: Vec<Replica>,
        created_at: String,
    ) -> Result<Applied> {
        if self.chunks.contains_key(&chunk_id) {
            bail!("chunk {chunk_id} committed twice");
        }
        let mut instances = HashSet::new();
        for rep in &replicas {
            if !instances.insert(rep.instance.as_str()) {
                bail!("chunk {chunk_id}: two replicas on {}", rep.instance);
            }
            self.check_replica(chunk_id, rep)?;
        }
        blocks.sort_by_key(|b| b.block_no);
        let rec = ChunkRec {
            logical_size,
            crc32,
            refs: 1,
            reclaiming: false,
            created_at,
            blocks,
            replicas: BTreeMap::new(),
        };
        self.chunks.insert(chunk_id, rec);
        self.last_chunk_id = self.last_chunk_id.max(chunk_id);
        for rep in replicas {
            self.put_replica(chunk_id, rep);
        }
        Ok(Ok(()))
    }

    /// All-or-nothing `refs + 1` (an id listed twice gets two).
    fn retain(&mut self, ids: &[ChunkId]) -> Applied {
        for &id in ids {
            if !self.chunks.get(&id).is_some_and(ChunkRec::is_live) {
                return Err(ChunkReject::ChunkGone(id));
            }
        }
        for id in ids {
            self.chunks.get_mut(id).expect("checked above").refs += 1;
        }
        Ok(())
    }

    /// All-or-nothing `refs - 1`; going below zero is a double release.
    fn release(&mut self, ids: &[ChunkId]) -> Applied {
        let mut wanted: BTreeMap<ChunkId, i64> = BTreeMap::new();
        for &id in ids {
            let n = wanted.entry(id).or_default();
            *n += 1;
            match self.chunks.get(&id) {
                Some(c) if !c.reclaiming && c.refs >= *n => {}
                _ => return Err(ChunkReject::DoubleRelease(id)),
            }
        }
        for (id, n) in wanted {
            self.chunks.get_mut(&id).expect("checked above").refs -= n;
        }
        Ok(())
    }

    /// An already reclaiming chunk (a pass interrupted by a crash) counts as started.
    fn begin_reclaim(&mut self, id: ChunkId) -> Applied {
        match self.chunks.get_mut(&id) {
            Some(c) if c.reclaiming => Ok(()),
            Some(c) if c.refs == 0 => {
                c.reclaiming = true;
                Ok(())
            }
            _ => Err(ChunkReject::NotReclaimable(id)),
        }
    }

    /// Missing is fine.
    fn drop_chunk(&mut self, id: ChunkId) {
        let instances: Vec<String> = self
            .chunks
            .get(&id)
            .map(|c| c.replicas.keys().cloned().collect())
            .unwrap_or_default();
        for instance in instances {
            self.remove_replica(id, &instance);
        }
        self.chunks.remove(&id);
    }

    fn add_replica(&mut self, chunk_id: ChunkId, rep: Replica) -> Result<Applied> {
        if !self.chunks.contains_key(&chunk_id) {
            bail!("add_replica: chunk {chunk_id} not found");
        }
        // Keys of the replica being replaced may be reused by the new one.
        let old = self.remove_replica(chunk_id, &rep.instance);
        if let Err(e) = self.check_replica(chunk_id, &rep) {
            if let Some(old) = old {
                self.put_replica(chunk_id, old);
            }
            return Err(e);
        }
        self.put_replica(chunk_id, rep);
        Ok(Ok(()))
    }
}

// --- conversions -----------------------------------------------------------

impl From<&ReplicaLayout> for Replica {
    fn from(r: &ReplicaLayout) -> Self {
        Self {
            instance: r.instance.clone(),
            parts: r
                .parts
                .iter()
                .map(|p: &PartLayout| PartRec {
                    first_block: p.first_block,
                    block_count: p.block_count,
                    sort_key: p.locator.key.clone(),
                    locator: p.locator.locator.clone(),
                    blob_id: p.blob_id,
                })
                .collect(),
        }
    }
}

impl ChunkRec {
    /// Replica layouts for [`pigeonhole_blob::Replicated::read`], with each
    /// part's `block_stored_lens` taken from the chunk's blocks.
    pub(crate) fn replica_layouts(&self) -> Vec<ReplicaLayout> {
        let stored_lens: Vec<u32> = self.blocks.iter().map(|b| b.stored_len.max(0) as u32).collect();
        self.replicas
            .iter()
            .map(|(instance, parts)| ReplicaLayout {
                instance: instance.clone(),
                parts: parts
                    .iter()
                    .map(|p| {
                        let end = (p.first_block as usize).saturating_add(p.block_count as usize);
                        PartLayout {
                            first_block: p.first_block,
                            block_count: p.block_count,
                            locator: BlobLocator {
                                key: p.sort_key.clone(),
                                locator: p.locator.clone(),
                            },
                            blob_id: p.blob_id,
                            block_stored_lens: stored_lens
                                .get(p.first_block as usize..end)
                                .map(<[u32]>::to_vec)
                                .unwrap_or_default(),
                        }
                    })
                    .collect(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails when the encoding of a command changes: bump `VERSION` and update
    /// the expected bytes (old logs then refuse to replay, as they should).
    #[test]
    fn command_encoding_is_pinned() {
        let cmd = ChunkCommand::Release { chunk_ids: vec![1, 2] };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [3, 2, 2, 4]);
        assert_eq!(ChunkState::VERSION, 2);
    }
}
