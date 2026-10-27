//! The chunk-store namespace of the metadata tablet.
//!
//! [`ChunkState`] holds instances, chunks (blocks, replicas, refs, lifecycle)
//! and the chunk id high-water mark in memory; [`ChunkState::apply`] is the only
//! code that changes them. It must stay deterministic: no clocks, no id
//! generation, no metrics — those belong to the proposer in [`crate::BlobDb`].
//! Every check runs before the first mutation, so a rejected command leaves
//! the state untouched.

use crate::blob_db::{ChunkId, Extent, StoredBlock};
use anyhow::{bail, Context, Result};
use pigeonhole_blob::{BlobLocator, PartLayout, ReplicaLayout};
use pigeonhole_meta::{Applied, StateMachine};
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};

pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/pigeonhole.chunk.v1.rs"));
}

pub(crate) const NAMESPACE: &str = "chunk";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstanceRec {
    pub kind: String,
    pub fingerprint: String,
    pub location: String,
    /// read-write | read-only | retired
    pub state: String,
}

/// One part of a replica: a blob holding `block_count` blocks from `first_block`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartRec {
    pub first_block: u32,
    pub block_count: u32,
    pub sort_key: Vec<u8>,
    pub locator: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Default)]
pub struct ChunkState {
    pub(crate) instances: BTreeMap<String, InstanceRec>,
    pub(crate) chunks: BTreeMap<ChunkId, ChunkRec>,
    /// `(instance, sort_key)` → owning chunk; a backend key belongs to one part.
    pub(crate) by_key: HashMap<(String, Vec<u8>), ChunkId>,
    /// Highest chunk id ever committed: ids are never reused (block cache and
    /// extents are keyed by chunk id).
    pub(crate) last_chunk_id: ChunkId,
}

impl StateMachine for ChunkState {
    const NAME: &'static str = NAMESPACE;

    fn apply(&mut self, command: &[u8]) -> Result<Applied> {
        use pb::command::Op;
        let cmd = pb::Command::decode(command).context("decode chunk command")?;
        match cmd.op.context("empty chunk command")? {
            Op::SyncInstances(c) => self.sync_instances(c),
            Op::CommitChunk(c) => self.commit_chunk(c),
            Op::Retain(c) => Ok(self.retain(&c.chunk_ids)),
            Op::Release(c) => Ok(self.release(&c.chunk_ids)),
            Op::BeginReclaim(c) => Ok(self.begin_reclaim(c.chunk_id)),
            Op::DropChunk(c) => Ok(self.drop_chunk(c.chunk_id)),
            Op::AddReplica(c) => self.add_replica(c),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let instances = self
            .instances
            .iter()
            .map(|(id, i)| pb::Instance {
                id: id.clone(),
                kind: i.kind.clone(),
                fingerprint: i.fingerprint.clone(),
                location: i.location.clone(),
                state: i.state.clone(),
            })
            .collect();
        let chunks = self
            .chunks
            .iter()
            .map(|(&chunk_id, c)| pb::ChunkState {
                chunk_id,
                logical_size: c.logical_size,
                crc32: c.crc32,
                refs: c.refs,
                reclaiming: c.reclaiming,
                created_at: c.created_at.clone(),
                blocks: c.blocks.iter().map(pb::Block::from).collect(),
                replicas: c
                    .replicas
                    .iter()
                    .map(|(instance, parts)| pb::Replica {
                        instance: instance.clone(),
                        parts: parts.iter().map(pb::Part::from).collect(),
                    })
                    .collect(),
            })
            .collect();
        pb::Snapshot { instances, chunks, last_chunk_id: self.last_chunk_id }.encode_to_vec()
    }

    fn restore(data: &[u8]) -> Result<Self> {
        let snap = pb::Snapshot::decode(data).context("decode chunk snapshot")?;
        let mut state = ChunkState { last_chunk_id: snap.last_chunk_id, ..Default::default() };
        for i in snap.instances {
            state.instances.insert(i.id.clone(), InstanceRec::from(i));
        }
        for c in snap.chunks {
            let rec = ChunkRec {
                logical_size: c.logical_size,
                crc32: c.crc32,
                refs: c.refs,
                reclaiming: c.reclaiming,
                created_at: c.created_at,
                blocks: c.blocks.into_iter().map(StoredBlock::from).collect(),
                replicas: BTreeMap::new(),
            };
            state.chunks.insert(c.chunk_id, rec);
            for rep in &c.replicas {
                state.check_replica(c.chunk_id, rep)?;
                state.put_replica(c.chunk_id, rep);
            }
        }
        Ok(state)
    }
}

impl ChunkState {
    /// Upsert configured instances; instances not listed become retired.
    fn sync_instances(&mut self, c: pb::SyncInstances) -> Result<Applied> {
        let mut instances = self.instances.clone();
        let configured: HashSet<&str> = c.instances.iter().map(|i| i.id.as_str()).collect();
        for (id, rec) in instances.iter_mut() {
            if !configured.contains(id.as_str()) {
                rec.state = "retired".into();
            }
        }
        for i in c.instances {
            instances.insert(i.id.clone(), InstanceRec::from(i));
        }
        let mut locations = HashSet::new();
        for (id, rec) in &instances {
            if rec.state == "read-write" && !locations.insert(rec.location.as_str()) {
                bail!("instance {id}: location {} already has a read-write instance", rec.location);
            }
        }
        self.instances = instances;
        Ok(ok())
    }

    /// A replica's parts must name a known instance and keys no other part owns.
    fn check_replica(&self, chunk_id: ChunkId, rep: &pb::Replica) -> Result<()> {
        if !self.instances.contains_key(&rep.instance) {
            bail!("chunk {chunk_id}: unknown instance {}", rep.instance);
        }
        let mut seen = HashSet::new();
        for part in &rep.parts {
            if !seen.insert(part.sort_key.as_slice()) {
                bail!("chunk {chunk_id}: duplicate sort_key on instance {}", rep.instance);
            }
            let owner = self.by_key.get(&(rep.instance.clone(), part.sort_key.clone()));
            if owner.is_some_and(|&o| o != chunk_id) {
                bail!(
                    "chunk {chunk_id}: key {} on {} belongs to chunk {}",
                    hex::encode(&part.sort_key),
                    rep.instance,
                    owner.unwrap()
                );
            }
        }
        Ok(())
    }

    fn put_replica(&mut self, chunk_id: ChunkId, rep: &pb::Replica) {
        let parts: Vec<PartRec> = rep.parts.iter().map(PartRec::from).collect();
        for p in &parts {
            self.by_key.insert((rep.instance.clone(), p.sort_key.clone()), chunk_id);
        }
        let chunk = self.chunks.get_mut(&chunk_id).expect("caller checked the chunk");
        chunk.replicas.insert(rep.instance.clone(), parts);
    }

    fn remove_replica(&mut self, chunk_id: ChunkId, instance: &str) {
        let Some(chunk) = self.chunks.get_mut(&chunk_id) else {
            return;
        };
        for p in chunk.replicas.remove(instance).unwrap_or_default() {
            self.by_key.remove(&(instance.to_string(), p.sort_key));
        }
    }

    /// New chunk with refs = 1 after a successful quorum write.
    fn commit_chunk(&mut self, c: pb::CommitChunk) -> Result<Applied> {
        if self.chunks.contains_key(&c.chunk_id) {
            bail!("chunk {} committed twice", c.chunk_id);
        }
        let mut instances = HashSet::new();
        for rep in &c.replicas {
            if !instances.insert(rep.instance.as_str()) {
                bail!("chunk {}: two replicas on {}", c.chunk_id, rep.instance);
            }
            self.check_replica(c.chunk_id, rep)?;
        }
        let mut blocks: Vec<StoredBlock> = c.blocks.into_iter().map(StoredBlock::from).collect();
        blocks.sort_by_key(|b| b.block_no);
        let rec = ChunkRec {
            logical_size: c.logical_size,
            crc32: c.crc32,
            refs: 1,
            reclaiming: false,
            created_at: c.created_at,
            blocks,
            replicas: BTreeMap::new(),
        };
        self.chunks.insert(c.chunk_id, rec);
        self.last_chunk_id = self.last_chunk_id.max(c.chunk_id);
        for rep in &c.replicas {
            self.put_replica(c.chunk_id, rep);
        }
        Ok(ok())
    }

    /// All-or-nothing `refs + 1` (an id listed twice gets two).
    fn retain(&mut self, ids: &[ChunkId]) -> Applied {
        for &id in ids {
            if !self.chunks.get(&id).is_some_and(ChunkRec::is_live) {
                return reject(pb::rejection::Reason::ChunkGone(id));
            }
        }
        for id in ids {
            self.chunks.get_mut(id).expect("checked above").refs += 1;
        }
        ok()
    }

    /// All-or-nothing `refs - 1`; going below zero is a double release.
    fn release(&mut self, ids: &[ChunkId]) -> Applied {
        let mut wanted: BTreeMap<ChunkId, i64> = BTreeMap::new();
        for &id in ids {
            let n = wanted.entry(id).or_default();
            *n += 1;
            match self.chunks.get(&id) {
                Some(c) if !c.reclaiming && c.refs >= *n => {}
                _ => return reject(pb::rejection::Reason::DoubleRelease(id)),
            }
        }
        for (id, n) in wanted {
            self.chunks.get_mut(&id).expect("checked above").refs -= n;
        }
        ok()
    }

    /// live with refs = 0 → reclaiming; an already reclaiming chunk (a pass
    /// interrupted by a crash) counts as started.
    fn begin_reclaim(&mut self, id: ChunkId) -> Applied {
        match self.chunks.get_mut(&id) {
            Some(c) if c.reclaiming => ok(),
            Some(c) if c.refs == 0 => {
                c.reclaiming = true;
                ok()
            }
            _ => reject(pb::rejection::Reason::NotReclaimable(id)),
        }
    }

    /// Forget a chunk whose backend parts are deleted (missing is fine).
    fn drop_chunk(&mut self, id: ChunkId) -> Applied {
        let instances: Vec<String> = self
            .chunks
            .get(&id)
            .map(|c| c.replicas.keys().cloned().collect())
            .unwrap_or_default();
        for instance in instances {
            self.remove_replica(id, &instance);
        }
        self.chunks.remove(&id);
        ok()
    }

    /// Replace one instance's replica of a chunk (repair / backfill).
    fn add_replica(&mut self, c: pb::AddReplica) -> Result<Applied> {
        let rep = c.replica.context("add_replica without replica")?;
        if !self.chunks.contains_key(&c.chunk_id) {
            bail!("add_replica: chunk {} not found", c.chunk_id);
        }
        // Keys of the replica being replaced may be reused by the new one.
        let old = self.chunks[&c.chunk_id].replicas.get(&rep.instance).cloned();
        self.remove_replica(c.chunk_id, &rep.instance);
        if let Err(e) = self.check_replica(c.chunk_id, &rep) {
            if let Some(parts) = old {
                let old = pb::Replica {
                    instance: rep.instance.clone(),
                    parts: parts.iter().map(pb::Part::from).collect(),
                };
                self.put_replica(c.chunk_id, &old);
            }
            return Err(e);
        }
        self.put_replica(c.chunk_id, &rep);
        Ok(ok())
    }
}

fn ok() -> Applied {
    Applied::Commit(Vec::new())
}

fn reject(reason: pb::rejection::Reason) -> Applied {
    Applied::Reject(pb::Rejection { reason: Some(reason) }.encode_to_vec())
}

// --- conversions -----------------------------------------------------------

impl From<pb::Instance> for InstanceRec {
    fn from(i: pb::Instance) -> Self {
        Self { kind: i.kind, fingerprint: i.fingerprint, location: i.location, state: i.state }
    }
}

impl From<pb::Extent> for Extent {
    fn from(e: pb::Extent) -> Self {
        Self {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        }
    }
}

impl From<&Extent> for pb::Extent {
    fn from(e: &Extent) -> Self {
        Self {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        }
    }
}

impl From<&StoredBlock> for pb::Block {
    fn from(b: &StoredBlock) -> Self {
        Self {
            block_no: b.block_no,
            logical_off: b.logical_off,
            logical_len: b.logical_len,
            stored_len: b.stored_len,
            codec: b.codec.clone(),
        }
    }
}

impl From<pb::Block> for StoredBlock {
    fn from(b: pb::Block) -> Self {
        Self {
            block_no: b.block_no,
            logical_off: b.logical_off,
            logical_len: b.logical_len,
            stored_len: b.stored_len,
            codec: b.codec,
        }
    }
}

impl From<&pb::Part> for PartRec {
    fn from(p: &pb::Part) -> Self {
        Self {
            first_block: p.first_block,
            block_count: p.block_count,
            sort_key: p.sort_key.clone(),
            locator: p.locator.clone(),
        }
    }
}

impl From<&PartRec> for pb::Part {
    fn from(p: &PartRec) -> Self {
        Self {
            first_block: p.first_block,
            block_count: p.block_count,
            sort_key: p.sort_key.clone(),
            locator: p.locator.clone(),
        }
    }
}

impl From<&ReplicaLayout> for pb::Replica {
    fn from(r: &ReplicaLayout) -> Self {
        Self {
            instance: r.instance.clone(),
            parts: r
                .parts
                .iter()
                .map(|p: &PartLayout| pb::Part {
                    first_block: p.first_block,
                    block_count: p.block_count,
                    sort_key: p.locator.key.clone(),
                    locator: p.locator.locator.clone(),
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
