//! Chunk-store metadata: queries plus the proposer side of the `chunk` namespace.
//!
//! Chunk state (instances, chunks, blocks, parts) lives in the tablet's
//! [`ChunkState`] and changes only through `chunk` commands. Node-local
//! progress (put watermarks, sweep cursor, repair queue) is kept in this
//! process: it is not replicated and starts empty on every boot.

use crate::commands::{pb, ChunkRec, ChunkState, NAMESPACE};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use pigeonhole_blob::{InstanceInfo, ReplicaLayout};
use pigeonhole_meta::{Applied, MetaStore, Namespaces};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

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
    err.downcast_ref::<ChunkGone>().is_some()
        || err.chain().any(|c| c.downcast_ref::<ChunkGone>().is_some())
}

/// Logical byte range within a chunk (`offset`/`len` are logical bytes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extent {
    pub chunk: ChunkId,
    pub offset: i64,
    pub len: i64,
}

/// One stored block (offsets inside a part are derived from `stored_len`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredBlock {
    pub block_no: i64,
    pub logical_off: i64,
    pub logical_len: i64,
    pub stored_len: i64,
    pub codec: String,
}

/// Progress of this process; lost on restart by design.
#[derive(Default)]
struct NodeLocal {
    /// Per instance: `(at, max_key)` of successful puts, oldest first.
    watermarks: HashMap<String, Vec<(DateTime<Utc>, Vec<u8>)>>,
    sweep_cursor: HashMap<String, Vec<u8>>,
    /// `(chunk, instance)` → enqueue order.
    repair_queue: BTreeMap<(ChunkId, String), u64>,
    repair_seq: u64,
}

/// Chunk-store view of the metadata tablet.
#[derive(Clone)]
pub struct BlobDb {
    meta: Arc<MetaStore>,
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
    /// Open a standalone tablet at `database_url` (local log next to it) with
    /// only this namespace.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let namespaces = Self::register(Namespaces::new());
        Self::open(MetaStore::open_local(database_url, namespaces).await?).await
    }

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
            next_chunk_id: Arc::new(AtomicI64::new(last + 1)),
            local: Arc::default(),
        })
    }

    fn read<R>(&self, f: impl FnOnce(&ChunkState) -> R) -> R {
        self.meta.read::<ChunkState, _>(f)
    }

    fn chunk<R>(&self, chunk_id: ChunkId, f: impl FnOnce(&ChunkRec) -> R) -> Option<R> {
        self.read(|s| s.chunks.get(&chunk_id).map(f))
    }

    /// Propose one chunk command; `Err(reason)` is a deterministic rejection.
    async fn propose(&self, op: pb::command::Op) -> Result<Result<(), pb::rejection::Reason>> {
        let cmd = pb::Command { op: Some(op) }.encode_to_vec();
        match self.meta.propose(NAMESPACE, &cmd).await?.applied {
            Applied::Commit(_) => Ok(Ok(())),
            Applied::Reject(bytes) => Ok(Err(pb::Rejection::decode(bytes.as_slice())?
                .reason
                .context("rejection without reason")?)),
        }
    }

    /// Propose a command that has no state-dependent failure.
    async fn propose_infallible(&self, op: pb::command::Op) -> Result<()> {
        match self.propose(op).await? {
            Ok(()) => Ok(()),
            Err(reason) => bail!("unexpected rejection {reason:?}"),
        }
    }

    pub async fn sync_instances(&self, configured: &[InstanceInfo]) -> Result<()> {
        let instances = configured
            .iter()
            .map(|i| pb::Instance {
                id: i.id.clone(),
                kind: i.kind.as_str().into(),
                fingerprint: i.fingerprint.clone(),
                location: i.location.clone(),
                state: i.role.as_str().into(),
            })
            .collect();
        self.propose_infallible(pb::command::Op::SyncInstances(pb::SyncInstances { instances }))
            .await
    }

    pub async fn list_instance_fingerprints(&self) -> Result<Vec<(String, String)>> {
        Ok(self.read(|s| {
            s.instances
                .iter()
                .map(|(id, i)| (id.clone(), i.fingerprint.clone()))
                .collect()
        }))
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
        let chunk_id = self.next_chunk_id.fetch_add(1, Ordering::SeqCst);
        self.propose_infallible(pb::command::Op::CommitChunk(pb::CommitChunk {
            chunk_id,
            logical_size,
            crc32,
            blocks: blocks.iter().map(pb::Block::from).collect(),
            replicas: replicas.iter().map(pb::Replica::from).collect(),
            created_at: Utc::now().to_rfc3339(),
        }))
        .await?;
        Ok(chunk_id)
    }

    /// Atomically bump refs for every id. Fails with [`ChunkGone`] (nothing retained)
    /// if any chunk is missing, has `refs = 0`, or is `reclaiming`.
    pub async fn retain(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        match self
            .propose(pb::command::Op::Retain(pb::Retain { chunk_ids: ids.to_vec() }))
            .await?
        {
            Ok(()) => Ok(()),
            Err(pb::rejection::Reason::ChunkGone(chunk_id)) => {
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
        match self
            .propose(pb::command::Op::Release(pb::Release { chunk_ids: ids.to_vec() }))
            .await?
        {
            Ok(()) => Ok(()),
            Err(pb::rejection::Reason::DoubleRelease(id)) => {
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

    /// Replica layouts for a chunk (for [`pigeonhole_blob::Replicated::read`]),
    /// ordered by instance; empty for a missing chunk.
    pub async fn get_replica_layouts(&self, chunk_id: i64) -> Result<Vec<ReplicaLayout>> {
        Ok(self.chunk(chunk_id, ChunkRec::replica_layouts).unwrap_or_default())
    }

    /// Record the highest sort key observed from a successful backend put.
    pub async fn record_put_watermark(&self, instance_id: &str, max_key: &[u8]) -> Result<()> {
        self.record_put_watermark_at(instance_id, max_key, Utc::now())
            .await
    }

    /// Test helper: record a watermark at an explicit timestamp.
    pub async fn record_put_watermark_at(
        &self,
        instance_id: &str,
        max_key: &[u8],
        at: DateTime<Utc>,
    ) -> Result<()> {
        let mut local = self.local.lock().unwrap();
        local
            .watermarks
            .entry(instance_id.to_string())
            .or_default()
            .push((at, max_key.to_vec()));
        Ok(())
    }

    /// Max sort key among watermarks with `at <= now - grace` (sweep upper bound).
    pub async fn sweep_watermark(
        &self,
        instance_id: &str,
        grace: std::time::Duration,
    ) -> Result<Option<Vec<u8>>> {
        let cutoff = Utc::now() - chrono::Duration::from_std(grace).unwrap_or(chrono::Duration::zero());
        let mut local = self.local.lock().unwrap();
        let Some(marks) = local.watermarks.get_mut(instance_id) else {
            return Ok(None);
        };
        // Everything at or before the cutoff collapses into one entry: later
        // cutoffs only move forward.
        let (old, mut fresh): (Vec<_>, Vec<_>) = marks.drain(..).partition(|(at, _)| *at <= cutoff);
        let best = old.into_iter().max_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        let upto = best.as_ref().map(|(_, k)| k.clone());
        if let Some(b) = best {
            fresh.insert(0, b);
        }
        *marks = fresh;
        Ok(upto)
    }

    pub async fn get_sweep_cursor(&self, instance_id: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.local.lock().unwrap().sweep_cursor.get(instance_id).cloned())
    }

    pub async fn set_sweep_cursor(
        &self,
        instance_id: &str,
        after: Option<&[u8]>,
    ) -> Result<()> {
        let mut local = self.local.lock().unwrap();
        match after {
            Some(k) => local.sweep_cursor.insert(instance_id.to_string(), k.to_vec()),
            None => local.sweep_cursor.remove(instance_id),
        };
        Ok(())
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

    /// Which of `keys` belong to live (`refs > 0`, not reclaiming) chunks on this
    /// instance. Used by the sweeper per batch (never cache across batches).
    pub async fn live_part_keys_among(
        &self,
        instance_id: &str,
        keys: &[Vec<u8>],
    ) -> Result<HashSet<Vec<u8>>> {
        Ok(self.read(|s| {
            keys.iter()
                .filter(|k| {
                    s.by_key
                        .get(&(instance_id.to_string(), (*k).clone()))
                        .and_then(|id| s.chunks.get(id))
                        .is_some_and(ChunkRec::is_live)
                })
                .cloned()
                .collect()
        }))
    }

    /// Chunk ids ready for reclaim: `refs = 0` and still live, plus crash leftovers
    /// already marked reclaiming.
    pub async fn list_zero_ref_chunks(&self) -> Result<Vec<ChunkId>> {
        Ok(self.read(|s| {
            s.chunks
                .iter()
                .filter(|(_, c)| c.refs == 0 || c.reclaiming)
                .map(|(&id, _)| id)
                .collect()
        }))
    }

    /// Atomically mark a zero-ref live chunk as reclaiming. Returns false if another
    /// worker won the race or `retain` revived the chunk.
    pub async fn try_begin_reclaim(&self, chunk_id: ChunkId) -> Result<bool> {
        match self
            .propose(pb::command::Op::BeginReclaim(pb::BeginReclaim { chunk_id }))
            .await?
        {
            Ok(()) => Ok(true),
            Err(pb::rejection::Reason::NotReclaimable(_)) => Ok(false),
            Err(other) => bail!("unexpected begin_reclaim rejection {other:?}"),
        }
    }

    /// Forget the chunk (after its backend parts are deleted).
    pub async fn delete_chunk_metadata(&self, chunk_id: ChunkId) -> Result<()> {
        self.propose_infallible(pb::command::Op::DropChunk(pb::DropChunk { chunk_id }))
            .await?;
        self.local
            .lock()
            .unwrap()
            .repair_queue
            .retain(|(id, _), _| *id != chunk_id);
        Ok(())
    }

    /// Enqueue `repair(chunk_id, instance_id)` (idempotent).
    pub async fn enqueue_repair(&self, chunk_id: ChunkId, instance_id: &str) -> Result<()> {
        let mut local = self.local.lock().unwrap();
        let seq = local.repair_seq;
        if let std::collections::btree_map::Entry::Vacant(e) =
            local.repair_queue.entry((chunk_id, instance_id.to_string()))
        {
            e.insert(seq);
            local.repair_seq += 1;
        }
        Ok(())
    }

    /// Remove a completed / cancelled repair job.
    pub async fn dequeue_repair(&self, chunk_id: ChunkId, instance_id: &str) -> Result<()> {
        self.local
            .lock()
            .unwrap()
            .repair_queue
            .remove(&(chunk_id, instance_id.to_string()));
        Ok(())
    }

    /// Up to `limit` jobs, chunks with the fewest replicas first, then oldest.
    pub async fn take_repair_jobs(&self, limit: usize) -> Result<Vec<(ChunkId, String)>> {
        let queue: Vec<((ChunkId, String), u64)> = {
            let local = self.local.lock().unwrap();
            local.repair_queue.iter().map(|(k, &seq)| (k.clone(), seq)).collect()
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
        Ok(jobs.into_iter().map(|(_, _, id, instance)| (id, instance)).collect())
    }

    /// Referenced chunks (`refs > 0`) missing a replica on `instance_id`.
    pub async fn chunks_missing_instance(&self, instance_id: &str) -> Result<Vec<ChunkId>> {
        Ok(self.read(|s| {
            s.chunks
                .iter()
                .filter(|(_, c)| c.refs > 0 && !c.replicas.contains_key(instance_id))
                .map(|(&id, _)| id)
                .collect()
        }))
    }

    /// Replace all parts for one instance replica.
    pub async fn commit_instance_replica(
        &self,
        chunk_id: ChunkId,
        layout: &ReplicaLayout,
    ) -> Result<()> {
        self.propose_infallible(pb::command::Op::AddReplica(pb::AddReplica {
            chunk_id,
            replica: Some(pb::Replica::from(layout)),
        }))
        .await
    }

    /// Pending repair queue depth (tests / metrics).
    pub async fn repair_queue_len(&self) -> Result<u64> {
        Ok(self.local.lock().unwrap().repair_queue.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_blob::{BlobLocator, InstanceKind, InstanceRole, PartLayout};

    #[tokio::test]
    async fn instances_and_refs_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let info = InstanceInfo {
            id: "tg-main".into(),
            kind: InstanceKind::Telegram,
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
                locator: BlobLocator {
                    key: vec![0, 0, 0, 1],
                    locator: b"loc".to_vec(),
                },
                block_stored_lens: vec![],
            }],
        }];
        // Direct part insert via commit of empty blocks + one part.
        let id2 = db
            .commit_chunk(
                32,
                1,
                &[],
                &layouts,
            )
            .await
            .unwrap();
        let _ = chunk_id;
        db.retain(&[id2]).await.unwrap();
        db.release(&[id2]).await.unwrap();
        assert_eq!(db.chunk_meta(id2).await.unwrap().unwrap().2, 1);
    }

    #[tokio::test]
    async fn missing_from_config_becomes_retired() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let a = InstanceInfo {
            id: "a".into(),
            kind: InstanceKind::Memory,
            fingerprint: "memory:local".into(),
            location: "memory:local".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(&[a.clone()]).await.unwrap();
        db.sync_instances(&[]).await.unwrap();
        assert_eq!(db.instance_state("a").await.unwrap().as_deref(), Some("retired"));
    }

    #[tokio::test]
    async fn reclaim_then_retain_returns_chunk_gone() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let info = InstanceInfo {
            id: "mem".into(),
            kind: InstanceKind::Memory,
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
                        locator: BlobLocator {
                            key: vec![0, 0, 0, 0, 0, 0, 0, 1],
                            locator: b"{}".to_vec(),
                        },
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
                        locator: BlobLocator {
                            key: vec![0, 0, 0, 0, 0, 0, 0, 2],
                            locator: b"{}".to_vec(),
                        },
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
    async fn reclaimed_chunk_ids_are_never_reused() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        db.sync_instances(&[InstanceInfo {
            id: "mem".into(),
            kind: InstanceKind::Memory,
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
                            locator: BlobLocator {
                                key: vec![0, 0, 0, 0, 0, 0, 0, key],
                                locator: b"{}".to_vec(),
                            },
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
        let url2 = format!("sqlite:{}?mode=rwc", dir.path().join("replayed.db").display());
        std::fs::rename(dir.path().join("blob.db.log"), dir.path().join("replayed.db.log")).unwrap();
        let replayed = BlobDb::connect(&url2).await.unwrap();
        let after = replayed.insert_chunk(8, 1).await.unwrap();
        assert!(after > new, "chunk id {new} reused as {after} after replay");
    }

    #[tokio::test]
    async fn double_release_errors() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let info = InstanceInfo {
            id: "mem".into(),
            kind: InstanceKind::Memory,
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
                        locator: BlobLocator {
                            key: vec![1],
                            locator: b"{}".to_vec(),
                        },
                        block_stored_lens: vec![],
                    }],
                }],
            )
            .await
            .unwrap();
        db.release(&[id]).await.unwrap();
        let err = db.release(&[id]).await.unwrap_err();
        assert!(
            err.to_string().contains("double release"),
            "{err}"
        );
    }
}
