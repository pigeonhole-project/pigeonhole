//! Stage H: reclaim orphan backend blobs and zero-ref chunk parts.

use crate::blob_db::BlobDb;
use crate::durability::Durability;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    BlobLocator, BoxByteStream, CostHint, DynBlobBackend, DynSweep, InflightParts, InstanceInfo,
    OpKind, SharedBackend,
};
use pigeonhole_types::{BackendLimits, ByteRange};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default grace: keys from puts newer than this are not swept.
pub const DEFAULT_SWEEP_GRACE: Duration = Duration::from_secs(15 * 60);
/// Candidates fetched / deleted per batch.
pub const SWEEP_BATCH_SIZE: usize = 100;
/// Default pause between full sweep passes in the binary.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Runtime knobs for the sweeper.
#[derive(Clone, Debug)]
pub struct SweepConfig {
    pub grace: Duration,
    pub batch_size: usize,
    pub interval: Duration,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            grace: DEFAULT_SWEEP_GRACE,
            batch_size: SWEEP_BATCH_SIZE,
            interval: DEFAULT_SWEEP_INTERVAL,
        }
    }
}

/// Counts from one [`Sweeper::sweep_once`] pass.
#[derive(Clone, Debug, Default)]
pub struct SweepStats {
    pub zero_ref_chunks: u64,
    pub keys_deleted: u64,
    pub keys_skipped_live: u64,
}

/// Wraps a [`DynBlobBackend`] so every successful put records a watermark and
/// individual puts older than `grace` are interrupted (when grace is non-zero).
///
/// The per-put timeout does **not** cover the window from the first part of a
/// chunk until `commit_chunk` / superblock publish — that protection is
/// [`InflightParts`], consulted by [`Sweeper`] on every delete batch.
pub struct WatermarkBackend {
    inner: SharedBackend,
    db: BlobDb,
    grace: Duration,
}

impl WatermarkBackend {
    pub fn new(inner: SharedBackend, db: BlobDb, grace: Duration) -> Self {
        Self { inner, db, grace }
    }

    pub fn wrap(inner: SharedBackend, db: BlobDb, grace: Duration) -> SharedBackend {
        Arc::new(Self::new(inner, db, grace))
    }
}

#[async_trait]
impl DynBlobBackend for WatermarkBackend {
    fn instance(&self) -> &InstanceInfo {
        self.inner.instance()
    }

    fn limits(&self) -> &BackendLimits {
        self.inner.limits()
    }

    fn cost(&self, op: OpKind, id: Option<&BlobLocator>) -> CostHint {
        self.inner.cost(op, id)
    }

    async fn put(&self, data: Bytes) -> Result<BlobLocator> {
        let put = self.inner.put(data);
        let loc = if self.grace.is_zero() {
            put.await?
        } else {
            match tokio::time::timeout(self.grace, put).await {
                Ok(r) => r?,
                Err(_) => bail!(
                    "put on instance {} exceeded sweep grace {:?}",
                    self.inner.instance().id,
                    self.grace
                ),
            }
        };
        self.db
            .record_put_watermark(&self.inner.instance().id, &loc.key)
            .await
            .context("record put watermark")?;
        Ok(loc)
    }

    async fn get(&self, id: &BlobLocator, range: Option<ByteRange>) -> Result<BoxByteStream> {
        self.inner.get(id, range).await
    }

    async fn delete(&self, keys: &[Vec<u8>]) -> Result<()> {
        self.inner.delete(keys).await
    }

    fn sweeper(&self) -> Option<&dyn DynSweep> {
        self.inner.sweeper()
    }
}

/// One sweeper per process; iterates every Sweepable member of the placement group.
pub struct Sweeper {
    db: BlobDb,
    durability: Arc<Durability>,
    members: Vec<SharedBackend>,
    config: SweepConfig,
    inflight: Arc<InflightParts>,
}

impl Sweeper {
    pub fn new(
        db: BlobDb,
        durability: Arc<Durability>,
        members: Vec<SharedBackend>,
        config: SweepConfig,
    ) -> Self {
        Self::with_inflight(db, durability, members, config, InflightParts::shared())
    }

    pub fn with_inflight(
        db: BlobDb,
        durability: Arc<Durability>,
        members: Vec<SharedBackend>,
        config: SweepConfig,
        inflight: Arc<InflightParts>,
    ) -> Self {
        Self {
            db,
            durability,
            members,
            config,
            inflight,
        }
    }

    pub fn inflight(&self) -> &Arc<InflightParts> {
        &self.inflight
    }

    pub fn config(&self) -> &SweepConfig {
        &self.config
    }

    /// Background loop: sleep `interval`, then [`Self::sweep_once`].
    pub async fn run_loop(self) {
        let period = self.config.interval.max(Duration::from_secs(1));
        loop {
            tokio::time::sleep(period).await;
            match self.sweep_once().await {
                Ok(stats) => {
                    pigeonhole_blob::record_sweep("deleted", stats.keys_deleted);
                    pigeonhole_blob::record_sweep("skipped_live", stats.keys_skipped_live);
                    pigeonhole_blob::record_sweep("zero_ref", stats.zero_ref_chunks);
                    if stats.keys_deleted > 0 || stats.zero_ref_chunks > 0 {
                        info!(
                            zero_ref_chunks = stats.zero_ref_chunks,
                            keys_deleted = stats.keys_deleted,
                            keys_skipped_live = stats.keys_skipped_live,
                            "sweep pass complete"
                        );
                    } else {
                        debug!(
                            keys_skipped_live = stats.keys_skipped_live,
                            "sweep pass idle"
                        );
                    }
                }
                Err(e) => warn!(error = %e, "sweep pass failed"),
            }
        }
    }

    /// Single pass: reclaim `refs = 0` chunks, then range-sweep each instance.
    pub async fn sweep_once(&self) -> Result<SweepStats> {
        let mut stats = SweepStats::default();
        stats.zero_ref_chunks = self.reclaim_zero_refs().await?;
        for member in &self.members {
            let (deleted, skipped) = self.sweep_instance(member).await?;
            stats.keys_deleted += deleted;
            stats.keys_skipped_live += skipped;
        }
        Ok(stats)
    }

    async fn reclaim_zero_refs(&self) -> Result<u64> {
        let chunks = self.db.list_zero_ref_chunks().await?;
        let mut n = 0u64;
        for chunk_id in chunks {
            let layouts = self.db.get_replica_layouts(chunk_id).await?;
            for layout in &layouts {
                let Some(backend) = self
                    .members
                    .iter()
                    .find(|m| m.instance().id == layout.instance)
                else {
                    warn!(
                        chunk_id,
                        instance = %layout.instance,
                        "zero-ref reclaim: instance not in placement; leaving parts"
                    );
                    continue;
                };
                let keys: Vec<Vec<u8>> = layout
                    .parts
                    .iter()
                    .map(|p| p.locator.key.clone())
                    .collect();
                if !keys.is_empty() {
                    backend
                        .delete(&keys)
                        .await
                        .with_context(|| {
                            format!(
                                "delete zero-ref parts chunk={chunk_id} instance={}",
                                layout.instance
                            )
                        })?;
                }
            }
            self.db.delete_chunk_metadata(chunk_id).await?;
            n += 1;
        }
        Ok(n)
    }

    async fn sweep_instance(&self, backend: &SharedBackend) -> Result<(u64, u64)> {
        let Some(sweep) = backend.sweeper() else {
            return Ok((0, 0));
        };
        let instance_id = backend.instance().id.as_str();
        let Some(upto) = self
            .db
            .sweep_watermark(instance_id, self.config.grace)
            .await?
        else {
            return Ok((0, 0));
        };

        let mut after = self.db.get_sweep_cursor(instance_id).await?;
        let mut deleted = 0u64;
        let mut skipped = 0u64;
        let batch = self.config.batch_size.max(1);

        loop {
            let keys = sweep
                .candidates(after.as_deref(), &upto, batch)
                .await
                .with_context(|| format!("candidates for {instance_id}"))?;
            if keys.is_empty() {
                self.db.set_sweep_cursor(instance_id, None).await?;
                break;
            }

            // Re-check liveness immediately before delete (never cache across batches):
            // committed chunk_parts, durability system keys, and in-flight uploads.
            let (to_delete, skipped_batch, protected) =
                self.filter_deletable(instance_id, &keys).await?;
            skipped += skipped_batch;
            if protected > 0 {
                pigeonhole_blob::record_sweep("inflight_protected", protected);
            }
            if !to_delete.is_empty() {
                backend
                    .delete(&to_delete)
                    .await
                    .with_context(|| format!("batch delete on {instance_id}"))?;
                deleted += to_delete.len() as u64;
            }

            let last = keys.last().cloned();
            after = last.clone();
            self.db
                .set_sweep_cursor(instance_id, last.as_deref())
                .await?;

            if keys.len() < batch {
                self.db.set_sweep_cursor(instance_id, None).await?;
                break;
            }
        }
        Ok((deleted, skipped))
    }

    /// Classify a candidate batch: `(to_delete, skipped_live, inflight_protected)`.
    async fn filter_deletable(
        &self,
        instance_id: &str,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Vec<u8>>, u64, u64)> {
        let committed = self.db.live_part_keys_among(instance_id, keys).await?;
        let system: HashSet<Vec<u8>> = self
            .durability
            .system_keys(instance_id)
            .await?
            .into_iter()
            .collect();

        let mut to_delete = Vec::new();
        let mut skipped = 0u64;
        let mut protected = 0u64;
        for k in keys {
            if committed.contains(k) || system.contains(k) {
                skipped += 1;
            } else if self.inflight.contains(instance_id, k) {
                protected += 1;
                skipped += 1;
            } else {
                to_delete.push(k.clone());
            }
        }
        Ok((to_delete, skipped, protected))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::Superblock;
    use crate::ingest::IngestOptions;
    use crate::instances::{validate_instances, InstanceConfig};
    use crate::layer::ChunkStore;
    use chrono::{Duration as ChronoDuration, Utc};
    use pigeonhole_blob::{
        erase_sweep, InflightParts, InstanceInfo, InstanceKind, InstanceRole, OrderedKey,
        TypedBootstrapPointer,
    };
    use pigeonhole_codec::ChunkCodec;
    use pigeonhole_storage_memory::MemoryBlobStore;
    use std::sync::Mutex as StdMutex;

    struct MemPin {
        data: StdMutex<Option<Bytes>>,
    }

    #[async_trait]
    impl TypedBootstrapPointer for MemPin {
        async fn read(&self) -> Result<Option<Bytes>> {
            Ok(self.data.lock().unwrap().clone())
        }
        async fn swap(&self, new: Bytes) -> Result<()> {
            *self.data.lock().unwrap() = Some(new);
            Ok(())
        }
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        db: BlobDb,
        mem: Arc<MemoryBlobStore>,
        backend: SharedBackend,
        dur: Arc<Durability>,
        layer: ChunkStore,
        inflight: Arc<InflightParts>,
        replicated: Arc<pigeonhole_blob::Replicated>,
    }

    async fn setup() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let mem = Arc::new(MemoryBlobStore::new().with_instance_id("default"));
        let backend = WatermarkBackend::wrap(
            Arc::new(erase_sweep(mem.clone())),
            db.clone(),
            Duration::ZERO,
        );
        let inflight = InflightParts::shared();
        let replicated = Arc::new(
            pigeonhole_blob::Replicated::new(
                vec![backend.clone()],
                1,
                Arc::new(pigeonhole_blob::CheapestFirst::new()),
            )
            .unwrap()
            .with_inflight(inflight.clone()),
        );
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open_replicated(db.clone(), replicated.clone(), opts)
            .await
            .unwrap();
        let info = backend.instance().clone();
        let pin = Arc::new(MemPin {
            data: StdMutex::new(None),
        });
        let genesis = Superblock::new(0, info.id.clone(), info.fingerprint.clone());
        let dur = Arc::new(Durability::new_replicated(
            replicated.clone(),
            vec![crate::durability::PinTarget {
                instance_id: info.id,
                fingerprint: info.fingerprint,
                pin,
                backend: backend.clone(),
            }],
            genesis,
        ));
        Fixture {
            _dir: dir,
            db,
            mem,
            backend,
            dur,
            layer,
            inflight,
            replicated,
        }
    }

    fn sweeper(
        db: BlobDb,
        dur: Arc<Durability>,
        backend: SharedBackend,
        grace: Duration,
        inflight: Arc<InflightParts>,
    ) -> Sweeper {
        Sweeper::with_inflight(
            db,
            dur,
            vec![backend],
            SweepConfig {
                grace,
                batch_size: SWEEP_BATCH_SIZE,
                interval: Duration::from_secs(1),
            },
            inflight,
        )
    }

    #[tokio::test]
    async fn orphans_from_partial_writes_are_deleted() {
        let f = setup().await;
        let orphan = f
            .backend
            .put(Bytes::from_static(b"orphan-partial"))
            .await
            .unwrap();
        assert!(f
            .mem
            .message_keys()
            .contains(&u64::from_bytes(&orphan.key).unwrap()));

        let stats = sweeper(f.db, f.dur, f.backend, Duration::ZERO, f.inflight)
            .sweep_once()
            .await
            .unwrap();
        assert!(stats.keys_deleted >= 1);
        assert!(!f
            .mem
            .message_keys()
            .contains(&u64::from_bytes(&orphan.key).unwrap()));
    }

    #[tokio::test]
    async fn keys_newer_than_watermark_untouched() {
        let f = setup().await;
        let old = f.backend.put(Bytes::from_static(b"old-orphan")).await.unwrap();
        let new = f.backend.put(Bytes::from_static(b"new-orphan")).await.unwrap();

        // Backdate only the old key's watermark; keep grace so the fresh watermark
        // for `new` does not advance the sweep upper bound.
        let past = Utc::now() - ChronoDuration::minutes(30);
        f.db
            .record_put_watermark_at("default", &old.key, past)
            .await
            .unwrap();

        let grace = Duration::from_secs(15 * 60);
        let stats = sweeper(f.db, f.dur, f.backend, grace, f.inflight)
            .sweep_once()
            .await
            .unwrap();
        assert!(stats.keys_deleted >= 1);
        let keys = f.mem.message_keys();
        assert!(!keys.contains(&u64::from_bytes(&old.key).unwrap()));
        assert!(keys.contains(&u64::from_bytes(&new.key).unwrap()));
    }

    #[tokio::test]
    async fn system_blobs_untouched() {
        let f = setup().await;
        f.dur.checkpoint(f.layer.db()).await.unwrap();
        let system = f.dur.system_keys("default").await.unwrap();
        assert!(!system.is_empty());

        let orphan = f.backend.put(Bytes::from_static(b"junk")).await.unwrap();
        let stats = sweeper(f.db, f.dur.clone(), f.backend, Duration::ZERO, f.inflight)
            .sweep_once()
            .await
            .unwrap();
        assert!(stats.keys_deleted >= 1);
        assert!(!f
            .mem
            .message_keys()
            .contains(&u64::from_bytes(&orphan.key).unwrap()));

        for k in system {
            assert!(
                f.mem
                    .message_keys()
                    .contains(&u64::from_bytes(&k).unwrap()),
                "system blob deleted"
            );
        }
    }

    #[tokio::test]
    async fn repeat_pass_idempotent() {
        let f = setup().await;
        let _ = f.backend.put(Bytes::from_static(b"o1")).await.unwrap();
        let _ = f.backend.put(Bytes::from_static(b"o2")).await.unwrap();
        let s = sweeper(
            f.db.clone(),
            f.dur.clone(),
            f.backend.clone(),
            Duration::ZERO,
            f.inflight.clone(),
        );
        let a = s.sweep_once().await.unwrap();
        assert!(a.keys_deleted >= 2);
        let mid = f.mem.message_keys();
        let b = s.sweep_once().await.unwrap();
        assert_eq!(b.keys_deleted, 0);
        assert_eq!(f.mem.message_keys(), mid);
    }

    #[tokio::test]
    async fn zero_ref_chunk_parts_reclaimed() {
        let f = setup().await;
        let id = f
            .layer
            .put_small(Bytes::from_static(b"live-then-release"))
            .await
            .unwrap();
        let keys_before = f.mem.message_keys();
        assert!(!keys_before.is_empty());
        f.layer.release(&[id]).await.unwrap();
        let stats = sweeper(f.db, f.dur, f.backend, Duration::ZERO, f.inflight)
            .sweep_once()
            .await
            .unwrap();
        assert_eq!(stats.zero_ref_chunks, 1);
        assert!(f.layer.db().chunk_meta(id).await.unwrap().is_none());
        for k in keys_before {
            assert!(!f.mem.message_keys().contains(&k));
        }
    }

    #[tokio::test]
    async fn inflight_parts_survive_sweep_past_grace() {
        let f = setup().await;
        // Simulate first part of a slow chunk: uploaded, watermark aged past grace,
        // not yet in chunk_parts — but protected by InflightParts.
        let part = f
            .backend
            .put(Bytes::from(vec![0xABu8; 64]))
            .await
            .unwrap();
        let guard = f.inflight.guard("default", part.key.clone());
        let past = Utc::now() - ChronoDuration::minutes(30);
        f.db
            .record_put_watermark_at("default", &part.key, past)
            .await
            .unwrap();

        let stats = sweeper(
            f.db.clone(),
            f.dur.clone(),
            f.backend.clone(),
            Duration::from_secs(15 * 60),
            f.inflight.clone(),
        )
        .sweep_once()
        .await
        .unwrap();
        assert_eq!(stats.keys_deleted, 0);
        assert!(f
            .mem
            .message_keys()
            .contains(&u64::from_bytes(&part.key).unwrap()));

        // Writer crashed / aborted without commit → drop guard → next sweep reclaims.
        drop(guard);
        let stats = sweeper(
            f.db,
            f.dur,
            f.backend,
            Duration::from_secs(15 * 60),
            f.inflight,
        )
        .sweep_once()
        .await
        .unwrap();
        assert!(stats.keys_deleted >= 1);
        assert!(!f
            .mem
            .message_keys()
            .contains(&u64::from_bytes(&part.key).unwrap()));
    }

    #[tokio::test]
    async fn commit_during_sweep_batch_protects_new_parts() {
        let f = setup().await;
        // Orphan candidate key that will be committed mid-pass via filter_deletable.
        let loc = f
            .backend
            .put(Bytes::from_static(b"about-to-commit"))
            .await
            .unwrap();
        let past = Utc::now() - ChronoDuration::minutes(30);
        f.db
            .record_put_watermark_at("default", &loc.key, past)
            .await
            .unwrap();

        // Commit into chunk_parts before the delete decision (simulates commit
        // between candidate listing and delete — we re-query per batch).
        let chunks = f
            .db
            .commit_chunk(
                16,
                1,
                &[],
                &[pigeonhole_blob::ReplicaLayout {
                    instance: "default".into(),
                    parts: vec![pigeonhole_blob::PartLayout {
                        first_block: 0,
                        block_count: 1,
                        locator: loc.clone(),
                        block_stored_lens: vec![16],
                    }],
                }],
            )
            .await
            .unwrap();
        let _ = chunks;

        let stats = sweeper(
            f.db,
            f.dur,
            f.backend,
            Duration::from_secs(15 * 60),
            f.inflight,
        )
        .sweep_once()
        .await
        .unwrap();
        assert_eq!(stats.keys_deleted, 0);
        assert!(f
            .mem
            .message_keys()
            .contains(&u64::from_bytes(&loc.key).unwrap()));
    }

    #[tokio::test]
    async fn unpublished_journal_segment_survives_sweep() {
        let f = setup().await;
        // Put a journal-like part and hold inflight as flush_journal does before publish.
        let loc = f
            .backend
            .put(Bytes::from_static(b"journal-seg"))
            .await
            .unwrap();
        let guard = f.inflight.guard("default", loc.key.clone());
        let past = Utc::now() - ChronoDuration::minutes(30);
        f.db
            .record_put_watermark_at("default", &loc.key, past)
            .await
            .unwrap();

        let stats = sweeper(
            f.db.clone(),
            f.dur.clone(),
            f.backend.clone(),
            Duration::from_secs(15 * 60),
            f.inflight.clone(),
        )
        .sweep_once()
        .await
        .unwrap();
        assert_eq!(stats.keys_deleted, 0);
        drop(guard);
        let _ = stats;
    }

    #[tokio::test]
    async fn repair_inflight_parts_survive_sweep() {
        let f = setup().await;
        let loc = f
            .backend
            .put(Bytes::from_static(b"repair-part"))
            .await
            .unwrap();
        let guard = f.replicated.inflight().guard("default", loc.key.clone());
        let past = Utc::now() - ChronoDuration::minutes(30);
        f.db
            .record_put_watermark_at("default", &loc.key, past)
            .await
            .unwrap();
        let stats = sweeper(
            f.db,
            f.dur,
            f.backend,
            Duration::from_secs(15 * 60),
            f.inflight,
        )
        .sweep_once()
        .await
        .unwrap();
        assert_eq!(stats.keys_deleted, 0);
        assert!(f
            .mem
            .message_keys()
            .contains(&u64::from_bytes(&loc.key).unwrap()));
        drop(guard);
    }

    #[test]
    fn two_writers_same_location_refused() {
        let a = InstanceConfig {
            info: InstanceInfo {
                id: "a".into(),
                kind: InstanceKind::Memory,
                fingerprint: "memory:a".into(),
                location: "memory:same".into(),
                role: InstanceRole::ReadWrite,
            },
            bot_token_env: String::new(),
            bot_token: String::new(),
            scope_id: "a".into(),
        };
        let mut b = a.clone();
        b.info.id = "b".into();
        b.info.fingerprint = "memory:b".into();
        assert!(validate_instances(&[a, b]).is_err());
    }
}
