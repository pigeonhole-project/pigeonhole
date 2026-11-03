//! Chunk-store metadata: queries plus the proposer side of the `chunk` namespace.
//!
//! Chunk state (instances, chunks, blocks, parts) lives in the tablet's
//! [`ChunkState`] and changes only through `chunk` commands. Node-local
//! progress (sweep cursor, repair queue, read pins) is kept in this
//! process: it is not replicated and starts empty on every boot.

use crate::commands::{ChunkCommand, ChunkRec, ChunkReject, ChunkState, InstanceRec, Replica, NAMESPACE};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use pigeonhole_blob::{InstanceInfo, Owner, ReplicaLayout};
use pigeonhole_meta::{MetaStore, Namespaces, Prepared, Proposal};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// Failed copies after which a repair job is parked as [`Unrepairable`].
pub const REPAIR_MAX_ATTEMPTS: u32 = 6;
/// Wait after a repair job's first failure; doubled after each next one, up
/// to [`REPAIR_BACKOFF_MAX`]. A deferred job (its stores busy) waits this
/// long too, without counting as a failure.
pub const REPAIR_BACKOFF: Duration = Duration::from_secs(30);
pub const REPAIR_BACKOFF_MAX: Duration = Duration::from_secs(30 * 60);

/// Chunk id, chosen by the proposer and never reused.
pub type ChunkId = i64;

/// `retain` failed because the chunk is missing, `refs = 0`, or already reclaiming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkGone {
    pub chunk_id: ChunkId,
}

impl std::fmt::Display for ChunkGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chunk {} gone", self.chunk_id)
    }
}

impl std::error::Error for ChunkGone {}

/// True when `err` (or a cause) is [`ChunkGone`].
pub fn is_chunk_gone(err: &anyhow::Error) -> bool {
    err.downcast_ref::<ChunkGone>().is_some() || err.chain().any(|c| c.downcast_ref::<ChunkGone>().is_some())
}

/// Logical byte range within a chunk (`offset`/`len` are logical bytes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extent {
    pub chunk: ChunkId,
    pub offset: i64,
    pub len: i64,
}

/// The chunks `extents` point into, each once, sorted.
pub fn unique_chunk_ids<'a>(extents: impl IntoIterator<Item = &'a Extent>) -> Vec<ChunkId> {
    let mut ids: Vec<ChunkId> = extents.into_iter().map(|e| e.chunk).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// One stored block (offsets inside a part are derived from `stored_len`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredBlock {
    pub block_no: u32,
    /// Where it starts in the chunk's logical bytes.
    pub logical_off: u64,
    pub logical_len: u32,
    pub stored_len: u32,
    /// What its stored bytes are in: raw, gzip or zstd.
    pub codec: pigeonhole_codec::ChunkCodec,
    /// crc32 of the stored bytes: a damaged replica is detected and read
    /// around.
    pub crc32: u32,
}

/// What [`BlobDb::stats`] counts.
#[derive(Debug, Default, Clone)]
pub struct ChunkStats {
    pub live: u64,
    /// No refs, not yet reclaimed (the sweeper's next work).
    pub unreferenced: u64,
    pub reclaiming: u64,
    pub logical_bytes: u64,
    /// Instance → stored bytes of the replicas it holds.
    pub stored_bytes: BTreeMap<String, u64>,
}

/// A repair job waiting in this process.
#[derive(Debug, Clone, Copy)]
struct RepairJob {
    /// Enqueue order (a deferred job goes to the back).
    seq: u64,
    /// Failed copies so far.
    attempts: u32,
    /// Not taken before then (after a failure or a deferral).
    not_before: Option<Instant>,
}

/// A repair job given up on after [`REPAIR_MAX_ATTEMPTS`] failed copies (no
/// source can be read, the target refuses every upload): parked, so that
/// it no longer takes a pass's slots from the jobs that can succeed. Kept
/// until the chunk goes or [`BlobDb::retry_unrepairable`] queues it again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unrepairable {
    pub chunk_id: ChunkId,
    pub instance: String,
    pub attempts: u32,
    /// The last failure.
    pub error: String,
}

/// Progress of this process; lost on restart by design.
#[derive(Default)]
struct NodeLocal {
    sweep_cursor: HashMap<String, Vec<u8>>,
    /// `(chunk, instance)` → the job.
    repair_queue: BTreeMap<(ChunkId, String), RepairJob>,
    repair_seq: u64,
    /// Jobs given up on.
    unrepairable: BTreeMap<(ChunkId, String), Unrepairable>,
    /// Reclaiming chunk → a log position at or after its `BeginReclaim`.
    reclaim_gates: HashMap<ChunkId, u64>,
}

/// Chunks being read right now (chunk → readers). Reclaim leaves a pinned
/// chunk's parts alone until the readers are done.
type ReadPins = Arc<Mutex<HashMap<ChunkId, usize>>>;

/// Pin on the chunks of one read; [`Drop`] releases it.
pub struct ReadPin {
    pins: ReadPins,
    ids: Vec<ChunkId>,
}

impl Drop for ReadPin {
    fn drop(&mut self) {
        let mut pins = self.pins.lock().unwrap();
        for id in &self.ids {
            if let Some(n) = pins.get_mut(id) {
                *n -= 1;
                if *n == 0 {
                    pins.remove(id);
                }
            }
        }
    }
}

/// Chunk-store view of the metadata tablet.
#[derive(Clone)]
pub struct BlobDb {
    meta: Arc<MetaStore>,
    read_pins: ReadPins,
    /// Next chunk id to hand out; ids are chosen by the proposer (see commands.rs).
    next_chunk_id: Arc<AtomicI64>,
    local: Arc<Mutex<NodeLocal>>,
}

impl std::fmt::Debug for BlobDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobDb").finish_non_exhaustive()
    }
}

impl BlobDb {
    /// Add the `chunk` namespace to the set passed to [`MetaStore::open`].
    pub fn register(namespaces: Namespaces) -> Namespaces {
        namespaces.with::<ChunkState>()
    }

    /// Use `meta`, which must have been opened with [`Self::register`].
    pub async fn open(meta: Arc<MetaStore>) -> Result<Self> {
        if !meta.has_namespace(NAMESPACE) {
            anyhow::bail!("metadata tablet opened without the chunk namespace");
        }
        let last = meta.read::<ChunkState, _>(|s| s.last_chunk_id);
        Ok(Self {
            meta,
            read_pins: Arc::default(),
            next_chunk_id: Arc::new(AtomicI64::new(last + 1)),
            local: Arc::default(),
        })
    }

    fn read<R>(&self, f: impl FnOnce(&ChunkState) -> R) -> R {
        self.meta.read::<ChunkState, _>(f)
    }

    /// Chunk counts and sizes, for gauges (a pass over every chunk).
    pub fn stats(&self) -> ChunkStats {
        self.read(|s| {
            let mut st = ChunkStats::default();
            for c in s.chunks.values() {
                match (c.reclaiming, c.refs > 0) {
                    (true, _) => st.reclaiming += 1,
                    (false, true) => st.live += 1,
                    (false, false) => st.unreferenced += 1,
                }
                st.logical_bytes += c.logical_size.max(0) as u64;
                let stored = if c.blocks.is_empty() {
                    c.logical_size.max(0) as u64
                } else {
                    c.blocks.iter().map(|b| u64::from(b.stored_len)).sum()
                };
                for instance in c.replicas.keys() {
                    *st.stored_bytes.entry(instance.clone()).or_default() += stored;
                }
            }
            st
        })
    }

    fn chunk<R>(&self, chunk_id: ChunkId, f: impl FnOnce(&ChunkRec) -> R) -> Option<R> {
        self.read(|s| s.chunks.get(&chunk_id).map(f))
    }

    /// Prove the tablet still holds its generation (a log round trip); call
    /// right before deleting backend blobs. `Err` = fenced or stopped.
    pub async fn confirm_active(&self) -> Result<()> {
        self.meta.confirm_active().await
    }

    /// Whether this node leads its tablet's cluster: only the leader does
    /// background work (a follower's writes would fail).
    pub fn is_leader(&self) -> bool {
        self.meta.is_leader()
    }

    /// Whether this node leads with a quorum: background passes, which
    /// write, run only then (a read-only leader's writes are refused).
    pub fn can_write(&self) -> bool {
        self.meta.has_quorum()
    }

    /// Whether the tablet has stopped (fenced or failed): background work ends.
    pub fn tablet_stopped(&self) -> bool {
        self.meta.is_stopped()
    }

    /// Propose one chunk command; `Ok(Err(_))` is a deterministic rejection.
    async fn propose(&self, cmd: ChunkCommand) -> Result<Result<(), ChunkReject>> {
        self.meta.propose::<ChunkState>(cmd).await
    }

    /// Propose a command that has no state-dependent failure.
    async fn propose_infallible(&self, cmd: ChunkCommand) -> Result<()> {
        match self.propose(cmd).await? {
            Ok(()) => Ok(()),
            Err(reason) => bail!("unexpected rejection {reason:?}"),
        }
    }

    /// The deployment's owner, chosen at random and committed on first use.
    pub async fn owner(&self) -> Result<Owner> {
        if let Some(owner) = self.read(|s| s.owner) {
            return Ok(owner);
        }
        self.propose_infallible(ChunkCommand::SetOwner { owner: Owner::random() }).await?;
        self.read(|s| s.owner).context("the owner was just set")
    }

    pub async fn sync_instances(&self, configured: &[InstanceInfo]) -> Result<()> {
        let instances = configured
            .iter()
            .map(|i| {
                let rec = InstanceRec {
                    kind: i.kind.into(),
                    fingerprint: i.fingerprint.clone(),
                    location: i.location.clone(),
                    state: i.role.as_str().into(),
                };
                (i.id.clone(), rec)
            })
            .collect();
        self.propose_infallible(ChunkCommand::SyncInstances { instances }).await
    }

    pub async fn list_instance_fingerprints(&self) -> Result<Vec<(String, String)>> {
        Ok(self.read(|s| s.instances.iter().map(|(id, i)| (id.clone(), i.fingerprint.clone())).collect()))
    }

    /// `read-write` / `read-only` / `retired`, or `None` if never configured.
    pub async fn instance_state(&self, instance_id: &str) -> Result<Option<String>> {
        Ok(self.read(|s| s.instances.get(instance_id).map(|i| i.state.clone())))
    }

    /// Chunk with refs = 1 and no blocks or parts (tests).
    #[cfg(test)]
    pub(crate) async fn insert_chunk(&self, logical_size: i64, crc32: u32) -> Result<ChunkId> {
        self.commit_chunk(logical_size, crc32, &[], &[]).await
    }

    /// Atomically record chunk + blocks + parts after a successful quorum write.
    pub async fn commit_chunk(
        &self,
        logical_size: i64,
        crc32: u32,
        blocks: &[StoredBlock],
        replicas: &[ReplicaLayout],
    ) -> Result<ChunkId> {
        let shard: Vec<String> = replicas.iter().map(|r| r.instance.clone()).collect();
        let (chunk_id, prepared) = self.prepare_commit(logical_size, crc32, blocks, replicas, &shard)?;
        let mut proposal = Proposal::new();
        let slot = proposal.push(prepared);
        match self.meta.propose_all(proposal).await?.take(slot)? {
            Ok(()) => Ok(chunk_id),
            Err(reason) => bail!("chunk commit rejected: {reason:?}"),
        }
    }

    /// A chunk id no chunk has had: past every one handed out here and past
    /// the newest the tablet applied. That one moves with replication: a
    /// node that took over from another leader goes on after its ids, as it
    /// serves only once it applied all they committed
    /// ([`pigeonhole_meta::MetaStore::is_leader`]).
    fn next_chunk_id(&self) -> ChunkId {
        let floor = self.read(|s| s.last_chunk_id) + 1;
        let mut next = self.next_chunk_id.load(Ordering::SeqCst);
        loop {
            let id = next.max(floor);
            match self.next_chunk_id.compare_exchange(next, id + 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return id,
                Err(now) => next = now,
            }
        }
    }

    /// The `CommitChunk` of a new chunk (its id chosen now), to be proposed
    /// with other commands in one entry; `shard`: the instances of the shard
    /// it was written to. Fenced to the leader's term: the id
    /// is unique only while this node goes on leading, so a commit written
    /// after the lead changed (even back to this node) is not applied
    /// ([`pigeonhole_meta::NotApplied::Fenced`]). `Err` off the lead.
    pub fn prepare_commit(
        &self,
        logical_size: i64,
        crc32: u32,
        blocks: &[StoredBlock],
        replicas: &[ReplicaLayout],
        shard: &[String],
    ) -> Result<(ChunkId, Prepared<ChunkState>)> {
        let term = self.meta.serving_term()?;
        let chunk_id = self.next_chunk_id();
        let command = ChunkCommand::CommitChunk {
            chunk_id,
            logical_size,
            crc32,
            blocks: blocks.to_vec(),
            replicas: replicas.iter().map(Replica::from).collect(),
            shard: shard.to_vec(),
            created_at: Utc::now().to_rfc3339(),
        };
        Ok((chunk_id, Prepared::new(&command).fenced(term)))
    }

    /// The tablet this layer's state lives in.
    pub fn meta(&self) -> &Arc<MetaStore> {
        &self.meta
    }

    /// Atomically bump refs for every id. Fails with [`ChunkGone`] (nothing retained)
    /// if any chunk is missing, has `refs = 0`, or is `reclaiming`.
    pub async fn retain(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        match self.propose(ChunkCommand::Retain { chunk_ids: ids.to_vec() }).await? {
            Ok(()) => Ok(()),
            Err(ChunkReject::ChunkGone(chunk_id)) => {
                pigeonhole_blob::record_chunk_gone();
                Err(anyhow::Error::new(ChunkGone { chunk_id }))
            }
            Err(other) => bail!("unexpected retain rejection {other:?}"),
        }
    }

    /// Decrement refs; errors on double-release (`refs` already 0 / missing / reclaiming).
    pub async fn release(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        match self.propose(ChunkCommand::Release { chunk_ids: ids.to_vec() }).await? {
            Ok(()) => Ok(()),
            Err(ChunkReject::DoubleRelease(id)) => {
                pigeonhole_blob::record_double_release();
                bail!("double release or missing chunk {id}")
            }
            Err(other) => bail!("unexpected release rejection {other:?}"),
        }
    }

    /// `(logical_size, crc32, refs)`.
    pub async fn chunk_meta(&self, chunk_id: i64) -> Result<Option<(i64, u32, i64)>> {
        Ok(self.chunk(chunk_id, |c| (c.logical_size, c.crc32, c.refs)))
    }

    pub async fn chunk_block_count(&self, chunk_id: i64) -> Result<Option<i64>> {
        Ok(self.chunk(chunk_id, |c| c.blocks.len() as i64))
    }

    /// Blocks in `block_no` order; empty for a missing chunk.
    pub async fn get_blocks(&self, chunk_id: i64) -> Result<Vec<StoredBlock>> {
        Ok(self.chunk(chunk_id, |c| c.blocks.clone()).unwrap_or_default())
    }

    /// The chunk's logical size; `None` for a missing chunk.
    pub(crate) fn chunk_meta_size(&self, chunk_id: ChunkId) -> Option<i64> {
        self.chunk(chunk_id, |c| c.logical_size)
    }

    /// The chunk's blocks that hold any of its logical bytes `from..to`.
    pub(crate) fn blocks_overlapping(&self, chunk_id: ChunkId, from: usize, to: usize) -> Vec<StoredBlock> {
        self.chunk(chunk_id, |c| {
            c.blocks
                .iter()
                .filter(|b| {
                    let start = b.logical_off as usize;
                    start < to && start + b.logical_len as usize > from
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
    }

    /// [`Self::get_replica_layouts`], without the `async`.
    pub(crate) fn get_replica_layouts_now(&self, chunk_id: ChunkId) -> Vec<ReplicaLayout> {
        self.chunk(chunk_id, ChunkRec::replica_layouts).unwrap_or_default()
    }

    /// The instances the chunk belongs on (see `ChunkRec::shard`); `None`
    /// for a missing chunk.
    pub fn chunk_shard(&self, chunk_id: ChunkId) -> Option<Vec<String>> {
        self.chunk(chunk_id, |c| c.shard.clone())
    }

    /// Replica layouts for a chunk (for [`pigeonhole_blob::Replicated::read`]),
    /// ordered by instance; empty for a missing chunk.
    pub async fn get_replica_layouts(&self, chunk_id: i64) -> Result<Vec<ReplicaLayout>> {
        Ok(self.chunk(chunk_id, ChunkRec::replica_layouts).unwrap_or_default())
    }

    pub async fn get_sweep_cursor(&self, instance_id: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.local.lock().unwrap().sweep_cursor.get(instance_id).cloned())
    }

    pub async fn set_sweep_cursor(&self, instance_id: &str, after: Option<&[u8]>) -> Result<()> {
        let mut local = self.local.lock().unwrap();
        match after {
            Some(k) => local.sweep_cursor.insert(instance_id.to_string(), k.to_vec()),
            None => local.sweep_cursor.remove(instance_id),
        };
        Ok(())
    }

    /// Pin `ids` for a read. Pin first, then check [`Self::chunk_readable`]:
    /// reclaim commits `BeginReclaim` first, then checks pins, so one of the
    /// two always sees the other.
    pub fn pin_reads(&self, ids: &[ChunkId]) -> ReadPin {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        let mut pins = self.read_pins.lock().unwrap();
        for id in &ids {
            *pins.entry(*id).or_default() += 1;
        }
        ReadPin { pins: self.read_pins.clone(), ids }
    }

    pub fn is_read_pinned(&self, chunk_id: ChunkId) -> bool {
        self.read_pins.lock().unwrap().contains_key(&chunk_id)
    }

    /// Every chunk of `ids` is referenced and not being reclaimed (a read
    /// of the state: no log entry, unlike a `retain` + `release`).
    pub fn are_live(&self, ids: &[ChunkId]) -> bool {
        self.read(|s| ids.iter().all(|id| s.chunks.get(id).is_some_and(ChunkRec::is_live)))
    }

    /// The chunk exists and is not being reclaimed.
    pub fn chunk_readable(&self, chunk_id: ChunkId) -> bool {
        self.chunk(chunk_id, |c| !c.reclaiming).unwrap_or(false)
    }

    /// Highest sort key of any committed part on `instance_id`.
    pub fn max_part_key(&self, instance_id: &str) -> Option<Vec<u8>> {
        self.read(|s| {
            s.chunks
                .values()
                .flat_map(|c| c.replicas.get(instance_id).into_iter().flatten())
                .map(|p| p.sort_key.clone())
                .max()
        })
    }

    /// Sort keys of parts belonging to live chunks on this instance.
    pub async fn live_part_keys(&self, instance_id: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self.read(|s| {
            s.chunks
                .values()
                .filter(|c| c.is_live())
                .flat_map(|c| c.replicas.get(instance_id).into_iter().flatten())
                .map(|p| p.sort_key.clone())
                .collect()
        }))
    }

    /// Which of `keys` are parts of a chunk in the state on this instance, live
    /// or not: those are reclaim's to delete (it respects read pins), never the
    /// orphan sweep's. Used by the sweeper per batch (never cache across batches).
    pub async fn owned_part_keys_among(&self, instance_id: &str, keys: &[Vec<u8>]) -> Result<HashSet<Vec<u8>>> {
        Ok(self.read(|s| {
            keys.iter().filter(|k| s.by_key.contains_key(&(instance_id.to_string(), (*k).clone()))).cloned().collect()
        }))
    }

    /// Chunk ids ready for reclaim: `refs = 0` and still live, plus crash leftovers
    /// already marked reclaiming.
    pub async fn list_zero_ref_chunks(&self) -> Result<Vec<ChunkId>> {
        Ok(self.read(|s| s.chunks.iter().filter(|(_, c)| c.refs == 0 || c.reclaiming).map(|(&id, _)| id).collect()))
    }

    /// Atomically mark a zero-ref live chunk as reclaiming. Returns false if another
    /// worker won the race or `retain` revived the chunk.
    pub async fn try_begin_reclaim(&self, chunk_id: ChunkId) -> Result<bool> {
        if self.local.lock().unwrap().reclaim_gates.contains_key(&chunk_id) {
            return Ok(true);
        }
        match self.propose(ChunkCommand::BeginReclaim { chunk_id }).await? {
            Ok(()) => {
                let at = self.meta.applied_position();
                self.local.lock().unwrap().reclaim_gates.insert(chunk_id, at);
                Ok(true)
            }
            Err(ChunkReject::NotReclaimable(_)) => Ok(false),
            Err(other) => bail!("unexpected begin_reclaim rejection {other:?}"),
        }
    }

    /// Whether the stored snapshot the log starts from already has
    /// `chunk_id` reclaiming (after [`Self::try_begin_reclaim`]), so its parts
    /// may go: a lossy restore from it cannot bring back an object that points
    /// at them. Always true when snapshots do not outlive the log.
    pub fn reclaim_in_snapshot(&self, chunk_id: ChunkId) -> bool {
        let Some(restorable) = self.meta.restorable_position() else {
            return true;
        };
        let gate = self.local.lock().unwrap().reclaim_gates.get(&chunk_id).copied();
        gate.is_some_and(|at| restorable >= at)
    }

    /// Forget the chunk (after its backend parts are deleted).
    pub async fn delete_chunk_metadata(&self, chunk_id: ChunkId) -> Result<()> {
        self.propose_infallible(ChunkCommand::DropChunk { chunk_id }).await?;
        let mut local = self.local.lock().unwrap();
        local.repair_queue.retain(|(id, _), _| *id != chunk_id);
        local.unrepairable.retain(|(id, _), _| *id != chunk_id);
        local.reclaim_gates.remove(&chunk_id);
        Ok(())
    }

    /// Enqueue `repair(chunk_id, instance_id)` (idempotent; a job parked
    /// as [`Unrepairable`] stays parked).
    pub async fn enqueue_repair(&self, chunk_id: ChunkId, instance_id: &str) -> Result<()> {
        let mut local = self.local.lock().unwrap();
        let key = (chunk_id, instance_id.to_string());
        if local.unrepairable.contains_key(&key) {
            return Ok(());
        }
        let seq = local.repair_seq;
        if let std::collections::btree_map::Entry::Vacant(e) = local.repair_queue.entry(key) {
            e.insert(RepairJob { seq, attempts: 0, not_before: None });
            local.repair_seq += 1;
        }
        Ok(())
    }

    /// Remove a completed / cancelled repair job.
    pub async fn dequeue_repair(&self, chunk_id: ChunkId, instance_id: &str) -> Result<()> {
        self.local.lock().unwrap().repair_queue.remove(&(chunk_id, instance_id.to_string()));
        Ok(())
    }

    /// A job's copy failed: it waits [`REPAIR_BACKOFF`] (doubling per
    /// failure), and after [`REPAIR_MAX_ATTEMPTS`] it is parked as
    /// [`Unrepairable`]. Whether it was parked.
    pub fn repair_failed(&self, chunk_id: ChunkId, instance_id: &str, error: &str) -> bool {
        let key = (chunk_id, instance_id.to_string());
        let mut local = self.local.lock().unwrap();
        let Some(job) = local.repair_queue.get_mut(&key) else { return false };
        job.attempts += 1;
        let attempts = job.attempts;
        if attempts < REPAIR_MAX_ATTEMPTS {
            let wait = REPAIR_BACKOFF.saturating_mul(1 << (attempts - 1).min(16)).min(REPAIR_BACKOFF_MAX);
            job.not_before = Some(Instant::now() + wait);
            return false;
        }
        local.repair_queue.remove(&key);
        let parked = Unrepairable { chunk_id, instance: key.1.clone(), attempts, error: error.to_string() };
        local.unrepairable.insert(key, parked);
        pigeonhole_blob::record_repair("parked", 1);
        true
    }

    /// A job put off (its stores are busy): to the back of the queue, not
    /// taken for [`REPAIR_BACKOFF`]; not a failure.
    pub fn repair_deferred(&self, chunk_id: ChunkId, instance_id: &str) {
        let mut local = self.local.lock().unwrap();
        let seq = local.repair_seq;
        if let Some(job) = local.repair_queue.get_mut(&(chunk_id, instance_id.to_string())) {
            job.seq = seq;
            job.not_before = Some(Instant::now() + REPAIR_BACKOFF);
            local.repair_seq += 1;
        }
    }

    /// Up to `limit` jobs due now, chunks with the fewest replicas first,
    /// then oldest. A job that failed or was deferred is not due until its
    /// wait is over, so jobs that cannot succeed never take every slot.
    pub async fn take_repair_jobs(&self, limit: usize) -> Result<Vec<(ChunkId, String)>> {
        Ok(self.repair_jobs_due(limit, Instant::now()))
    }

    pub(crate) fn repair_jobs_due(&self, limit: usize, now: Instant) -> Vec<(ChunkId, String)> {
        let queue: Vec<((ChunkId, String), u64)> = {
            let local = self.local.lock().unwrap();
            local
                .repair_queue
                .iter()
                .filter(|(_, job)| job.not_before.is_none_or(|t| t <= now))
                .map(|(k, job)| (k.clone(), job.seq))
                .collect()
        };
        let mut jobs: Vec<(usize, u64, ChunkId, String)> = self.read(|s| {
            queue
                .into_iter()
                .map(|((id, instance), seq)| {
                    let replicas = s.chunks.get(&id).map_or(0, |c| c.replicas.len());
                    (replicas, seq, id, instance)
                })
                .collect()
        });
        jobs.sort();
        jobs.truncate(limit.max(1));
        jobs.into_iter().map(|(_, _, id, instance)| (id, instance)).collect()
    }

    /// The jobs parked as unrepairable whose chunk is still there.
    pub fn unrepairable(&self) -> Vec<Unrepairable> {
        let parked: Vec<Unrepairable> = self.local.lock().unwrap().unrepairable.values().cloned().collect();
        self.read(|s| parked.into_iter().filter(|u| s.chunks.contains_key(&u.chunk_id)).collect())
    }

    /// Queue every parked job again, from no attempts (after the operator
    /// fixed a store); how many.
    pub fn retry_unrepairable(&self) -> usize {
        let mut local = self.local.lock().unwrap();
        let parked = std::mem::take(&mut local.unrepairable);
        for key in parked.keys() {
            let seq = local.repair_seq;
            local.repair_seq += 1;
            local.repair_queue.insert(key.clone(), RepairJob { seq, attempts: 0, not_before: None });
        }
        parked.len()
    }

    /// Live chunks of `instance_id`'s shard (one that belongs on one of
    /// `peers`) that have no replica on `instance_id`.
    pub async fn chunks_missing_instance(&self, instance_id: &str, peers: &[&str]) -> Result<Vec<ChunkId>> {
        Ok(self.read(|s| {
            s.chunks
                .iter()
                .filter(|(_, c)| {
                    c.refs > 0
                        && !c.replicas.contains_key(instance_id)
                        && c.shard.iter().any(|i| peers.contains(&i.as_str()))
                })
                .map(|(&id, _)| id)
                .collect()
        }))
    }

    /// Referenced chunks (`refs > 0`) with a replica on `instance_id`.
    pub async fn chunks_on_instance(&self, instance_id: &str) -> Result<Vec<ChunkId>> {
        Ok(self.read(|s| {
            s.chunks
                .iter()
                .filter(|(_, c)| c.refs > 0 && c.replicas.contains_key(instance_id))
                .map(|(&id, _)| id)
                .collect()
        }))
    }

    /// Replace each chunk's replica on its layout's instance, all in one
    /// entry. Per replica: `false` when its chunk is gone or reclaiming (that
    /// one is not added; the others are). `pins` (the copies' upload pins)
    /// go with the entry and are dropped once it is applied, even if the
    /// caller is gone by then ([`Proposal::hold`]).
    pub async fn commit_instance_replicas(
        &self,
        replicas: &[(ChunkId, &ReplicaLayout)],
        pins: Vec<pigeonhole_blob::UploadPin>,
    ) -> Result<Vec<bool>> {
        let mut proposal = Proposal::new();
        let slots = proposal.extend(replicas.iter().map(|&(chunk_id, layout)| {
            Prepared::<ChunkState>::new(&ChunkCommand::AddReplica { chunk_id, replica: Replica::from(layout) })
        }));
        proposal.hold(pins);
        let mut outcomes = self.meta.propose_all(proposal).await?;
        let mut added = Vec::with_capacity(slots.len());
        for slot in slots {
            match outcomes.take(slot)? {
                Ok(()) => added.push(true),
                Err(ChunkReject::ChunkGone(_)) => added.push(false),
                Err(reason) => bail!("unexpected rejection {reason:?}"),
            }
        }
        Ok(added)
    }

    /// The repair jobs waiting, (chunk, instance), in queue order.
    pub async fn repair_jobs(&self) -> Vec<(ChunkId, String)> {
        let local = self.local.lock().unwrap();
        let mut jobs: Vec<_> = local.repair_queue.iter().map(|(k, job)| (job.seq, k.clone())).collect();
        jobs.sort();
        jobs.into_iter().map(|(_, k)| k).collect()
    }

    /// Pending repair queue depth (parked jobs not counted).
    pub async fn repair_queue_len(&self) -> Result<u64> {
        Ok(self.local.lock().unwrap().repair_queue.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_blob::{BlobId, BlobLocator, InstanceRole, PartLayout};

    #[tokio::test]
    async fn instances_and_refs_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
        let info = InstanceInfo {
            id: "tg-main".into(),
            kind: "telegram",
            fingerprint: "tg:1:-100".into(),
            location: "tg:chat:-100".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(&[info]).await.unwrap();
        let fps = db.list_instance_fingerprints().await.unwrap();
        assert_eq!(fps, vec![("tg-main".into(), "tg:1:-100".into())]);

        let chunk_id = db.insert_chunk(32, 0xdeadbeef).await.unwrap();
        let layouts = vec![ReplicaLayout {
            instance: "tg-main".into(),
            parts: vec![PartLayout {
                first_block: 0,
                block_count: 0,
                locator: BlobLocator { key: vec![0, 0, 0, 1], locator: b"loc".to_vec() },
                blob_id: BlobId(0),
                block_stored_lens: vec![],
            }],
        }];
        // Direct part insert via commit of empty blocks + one part.
        let id2 = db.commit_chunk(32, 1, &[], &layouts).await.unwrap();
        let _ = chunk_id;
        db.retain(&[id2]).await.unwrap();
        db.release(&[id2]).await.unwrap();
        assert_eq!(db.chunk_meta(id2).await.unwrap().unwrap().2, 1);
    }

    #[tokio::test]
    async fn missing_from_config_becomes_retired() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
        let a = InstanceInfo {
            id: "a".into(),
            kind: "memory",
            fingerprint: "memory:local".into(),
            location: "memory:local".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(std::slice::from_ref(&a)).await.unwrap();
        db.sync_instances(&[]).await.unwrap();
        assert_eq!(db.instance_state("a").await.unwrap().as_deref(), Some("retired"));
    }

    #[tokio::test]
    async fn reclaim_then_retain_returns_chunk_gone() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
        let info = InstanceInfo {
            id: "mem".into(),
            kind: "memory",
            fingerprint: "memory:mem".into(),
            location: "memory:mem".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(&[info]).await.unwrap();
        let id = db
            .commit_chunk(
                8,
                1,
                &[],
                &[ReplicaLayout {
                    instance: "mem".into(),
                    parts: vec![PartLayout {
                        first_block: 0,
                        block_count: 1,
                        locator: BlobLocator { key: vec![0, 0, 0, 0, 0, 0, 0, 1], locator: b"{}".to_vec() },
                        blob_id: BlobId(0),
                        block_stored_lens: vec![8],
                    }],
                }],
            )
            .await
            .unwrap();
        db.release(&[id]).await.unwrap();
        assert!(db.try_begin_reclaim(id).await.unwrap());
        let err = db.retain(&[id]).await.unwrap_err();
        assert!(is_chunk_gone(&err), "{err}");
        // Multi-id: one reclaiming → none retained.
        let id2 = db
            .commit_chunk(
                4,
                2,
                &[],
                &[ReplicaLayout {
                    instance: "mem".into(),
                    parts: vec![PartLayout {
                        first_block: 0,
                        block_count: 1,
                        locator: BlobLocator { key: vec![0, 0, 0, 0, 0, 0, 0, 2], locator: b"{}".to_vec() },
                        blob_id: BlobId(0),
                        block_stored_lens: vec![4],
                    }],
                }],
            )
            .await
            .unwrap();
        let err = db.retain(&[id2, id]).await.unwrap_err();
        assert!(is_chunk_gone(&err), "{err}");
        let meta = db.chunk_meta(id2).await.unwrap().unwrap();
        assert_eq!(meta.2, 1, "id2 refs must be unchanged after rolled-back retain");
    }

    #[tokio::test]
    async fn chunk_ids_go_on_after_ones_another_node_committed() {
        let dir = tempfile::tempdir().unwrap();
        let meta = pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await;
        // Two views opened at once: one is this node, the other stands for
        // a leader before it whose commits reach it by replication.
        let (this, before) = (BlobDb::open(meta.clone()).await.unwrap(), BlobDb::open(meta).await.unwrap());
        let first = before.insert_chunk(8, 1).await.unwrap();
        let next = this.insert_chunk(8, 1).await.unwrap();
        assert!(next > first, "{next} after {first}");
    }

    #[tokio::test]
    async fn reclaimed_chunk_ids_are_never_reused() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
        db.sync_instances(&[InstanceInfo {
            id: "mem".into(),
            kind: "memory",
            fingerprint: "memory:mem".into(),
            location: "memory:mem".into(),
            role: InstanceRole::ReadWrite,
        }])
        .await
        .unwrap();
        let commit = |key: u8| {
            let db = db.clone();
            async move {
                db.commit_chunk(
                    8,
                    1,
                    &[],
                    &[ReplicaLayout {
                        instance: "mem".into(),
                        parts: vec![PartLayout {
                            first_block: 0,
                            block_count: 1,
                            locator: BlobLocator { key: vec![0, 0, 0, 0, 0, 0, 0, key], locator: b"{}".to_vec() },
                            blob_id: BlobId(0),
                            block_stored_lens: vec![8],
                        }],
                    }],
                )
                .await
                .unwrap()
            }
        };
        // Reclaim the highest id: plain INTEGER PRIMARY KEY would hand it out again,
        // and anything keyed by chunk id (block cache, stale extents) would alias.
        let old = commit(1).await;
        db.release(&[old]).await.unwrap();
        assert!(db.try_begin_reclaim(old).await.unwrap());
        db.delete_chunk_metadata(old).await.unwrap();
        let new = commit(2).await;
        assert!(new > old, "chunk id {old} reused as {new}");

        // Same after the state is rebuilt from the log: the high-water mark is state.
        db.release(&[new]).await.unwrap();
        assert!(db.try_begin_reclaim(new).await.unwrap());
        db.delete_chunk_metadata(new).await.unwrap();
        let replayed = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let after = replayed.insert_chunk(8, 1).await.unwrap();
        assert!(after > new, "chunk id {new} reused as {after} after replay");
    }

    #[tokio::test]
    async fn decoded_state_rebuilds_the_key_index() {
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        db.sync_instances(&[InstanceInfo {
            id: "mem".into(),
            kind: "memory",
            fingerprint: "memory:mem".into(),
            location: "memory:mem".into(),
            role: InstanceRole::ReadWrite,
        }])
        .await
        .unwrap();
        for key in 1..=3u8 {
            let part = PartLayout {
                first_block: 0,
                block_count: 0,
                locator: BlobLocator { key: vec![key], locator: b"{}".to_vec() },
                blob_id: BlobId(0),
                block_stored_lens: vec![],
            };
            let replica = ReplicaLayout { instance: "mem".into(), parts: vec![part] };
            db.commit_chunk(1, 1, &[], &[replica]).await.unwrap();
        }
        let (bytes, index) = db.read(|s| (pigeonhole_meta::encode_state(s), s.by_key.clone()));
        let decoded = pigeonhole_meta::decode_state::<ChunkState>(&bytes).unwrap();
        assert_eq!(index.len(), 3);
        assert_eq!(decoded.by_key, index);
    }

    /// A failing job backs off (doubling) so the others get the slots, and
    /// is parked once it failed too often; a parked job is not queued again
    /// until retried.
    #[tokio::test]
    async fn a_failing_repair_job_backs_off_then_is_parked() {
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let (bad, good) = (db.insert_chunk(1, 0).await.unwrap(), db.insert_chunk(1, 0).await.unwrap());
        db.enqueue_repair(bad, "b").await.unwrap();
        db.enqueue_repair(good, "b").await.unwrap();
        assert_eq!(db.take_repair_jobs(1).await.unwrap(), vec![(bad, "b".to_string())], "oldest first");

        assert!(!db.repair_failed(bad, "b", "no source"));
        assert_eq!(db.take_repair_jobs(1).await.unwrap(), vec![(good, "b".to_string())], "the failed one waits");
        let now = Instant::now();
        assert_eq!(db.repair_jobs_due(2, now + REPAIR_BACKOFF).len(), 2, "due again after the backoff");
        assert!(!db.repair_failed(bad, "b", "no source"));
        assert_eq!(db.repair_jobs_due(2, now + REPAIR_BACKOFF).len(), 1, "the second wait is longer");
        assert_eq!(db.repair_jobs_due(2, now + REPAIR_BACKOFF * 2 + Duration::from_secs(1)).len(), 2);

        for _ in 2..REPAIR_MAX_ATTEMPTS - 1 {
            assert!(!db.repair_failed(bad, "b", "no source"));
        }
        assert!(db.repair_failed(bad, "b", "no source"), "parked");
        let parked = db.unrepairable();
        assert_eq!(parked.len(), 1);
        assert_eq!(
            (parked[0].chunk_id, parked[0].attempts, parked[0].error.as_str()),
            (bad, REPAIR_MAX_ATTEMPTS, "no source")
        );
        assert_eq!(db.repair_jobs().await, vec![(good, "b".to_string())]);
        db.enqueue_repair(bad, "b").await.unwrap();
        assert_eq!(db.repair_queue_len().await.unwrap(), 1, "a parked job is not queued again");

        assert_eq!(db.retry_unrepairable(), 1);
        assert!(db.unrepairable().is_empty());
        assert_eq!(db.repair_queue_len().await.unwrap(), 2);
        db.repair_deferred(bad, "b");
        assert_eq!(db.take_repair_jobs(2).await.unwrap(), vec![(good, "b".to_string())], "deferred: waits too");
    }

    #[tokio::test]
    async fn double_release_errors() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
        let info = InstanceInfo {
            id: "mem".into(),
            kind: "memory",
            fingerprint: "memory:mem".into(),
            location: "memory:mem".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(&[info]).await.unwrap();
        let id = db
            .commit_chunk(
                1,
                1,
                &[],
                &[ReplicaLayout {
                    instance: "mem".into(),
                    parts: vec![PartLayout {
                        first_block: 0,
                        block_count: 0,
                        locator: BlobLocator { key: vec![1], locator: b"{}".to_vec() },
                        blob_id: BlobId(0),
                        block_stored_lens: vec![],
                    }],
                }],
            )
            .await
            .unwrap();
        db.release(&[id]).await.unwrap();
        let err = db.release(&[id]).await.unwrap_err();
        assert!(err.to_string().contains("double release"), "{err}");
    }
}
