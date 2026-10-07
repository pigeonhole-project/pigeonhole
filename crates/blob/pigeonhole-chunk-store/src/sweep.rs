//! Stage H: reclaim orphan backend blobs and zero-ref chunk parts.

use crate::blob_db::BlobDb;
use crate::durability::Durability;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    BoxByteStream, CostHint, DynBlobBackend, DynSweep, InstanceInfo, OpKind, SharedBackend,
    BlobLocator,
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
/// puts older than `grace` are interrupted (when grace is non-zero).
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
}

impl Sweeper {
    pub fn new(
        db: BlobDb,
        durability: Arc<Durability>,
        members: Vec<SharedBackend>,
        config: SweepConfig,
    ) -> Self {
        Self {
            db,
            durability,
            members,
            config,
        }
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

        let live = self.live_keys(instance_id).await?;
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

            let mut to_delete = Vec::new();
            for k in &keys {
                if live.contains(k) {
                    skipped += 1;
                } else {
                    to_delete.push(k.clone());
                }
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

    async fn live_keys(&self, instance_id: &str) -> Result<HashSet<Vec<u8>>> {
        let mut live: HashSet<Vec<u8>> = self
            .db
            .live_part_keys(instance_id)
            .await?
            .into_iter()
            .collect();
        for k in self.durability.system_keys(instance_id).await? {
            live.insert(k);
        }
        Ok(live)
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
        erase_sweep, InstanceInfo, InstanceKind, InstanceRole, OrderedKey, TypedBootstrapPointer,
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
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open_replicated(
            db.clone(),
            Arc::new(
                pigeonhole_blob::Replicated::new(
                    vec![backend.clone()],
                    1,
                    Arc::new(pigeonhole_blob::CheapestFirst::new()),
                )
                .unwrap(),
            ),
            opts,
        )
        .await
        .unwrap();
        let info = backend.instance().clone();
        let pin = Arc::new(MemPin {
            data: StdMutex::new(None),
        });
        let genesis = Superblock::new(0, info.id.clone(), info.fingerprint.clone());
        let dur = Arc::new(Durability::new(backend.clone(), pin, genesis));
        Fixture {
            _dir: dir,
            db,
            mem,
            backend,
            dur,
            layer,
        }
    }

    fn sweeper(db: BlobDb, dur: Arc<Durability>, backend: SharedBackend, grace: Duration) -> Sweeper {
        Sweeper::new(
            db,
            dur,
            vec![backend],
            SweepConfig {
                grace,
                batch_size: SWEEP_BATCH_SIZE,
                interval: Duration::from_secs(1),
            },
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

        let stats = sweeper(f.db, f.dur, f.backend, Duration::ZERO)
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
        let stats = sweeper(f.db, f.dur, f.backend, grace)
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
        let stats = sweeper(f.db, f.dur.clone(), f.backend, Duration::ZERO)
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
        let s = sweeper(f.db.clone(), f.dur.clone(), f.backend.clone(), Duration::ZERO);
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
        let stats = sweeper(f.db, f.dur, f.backend, Duration::ZERO)
            .sweep_once()
            .await
            .unwrap();
        assert_eq!(stats.zero_ref_chunks, 1);
        assert!(f.layer.db().chunk_meta(id).await.unwrap().is_none());
        for k in keys_before {
            assert!(!f.mem.message_keys().contains(&k));
        }
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
