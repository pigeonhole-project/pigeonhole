//! Reclaim zero-ref chunks and orphan backend blobs.
//!
//! Orphans are swept only up to the instance's upload horizon
//! ([`UploadPins::horizon`]): every key at or below it is either a committed
//! part (kept) or garbage, because uploads are pinned before they start and
//! unpinned only after their commit is applied.

use crate::blob_db::BlobDb;
use anyhow::{Context, Result};
use pigeonhole_blob::{SharedBackend, UploadPins};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Candidates fetched / deleted per batch.
pub const SWEEP_BATCH_SIZE: usize = 100;
/// Default pause between full sweep passes in the binary.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Runtime knobs for the sweeper.
#[derive(Clone, Debug)]
pub struct SweepConfig {
    pub batch_size: usize,
    pub interval: Duration,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
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

/// One sweeper per process; iterates every Sweepable member of the placement group.
pub struct Sweeper {
    db: BlobDb,
    members: Vec<SharedBackend>,
    config: SweepConfig,
    /// The upload pins of this process (`Replicated::pins`).
    pins: Arc<UploadPins>,
}

impl Sweeper {
    pub fn new(
        db: BlobDb,
        members: Vec<SharedBackend>,
        config: SweepConfig,
        pins: Arc<UploadPins>,
    ) -> Self {
        Self { db, members, config, pins }
    }

    pub fn config(&self) -> &SweepConfig {
        &self.config
    }

    /// Background loop: sleep `interval`, then [`Self::sweep_once`]; ends once
    /// the tablet stops (a fenced incarnation must not delete anything).
    pub async fn run_loop(self) {
        let period = self.config.interval.max(Duration::from_secs(1));
        loop {
            tokio::time::sleep(period).await;
            if self.db.tablet_stopped() {
                info!("metadata tablet stopped; sweeper exits");
                return;
            }
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
            if !self.db.try_begin_reclaim(chunk_id).await? {
                continue;
            }
            // Committed as reclaiming first, then the pins: a reader that pins
            // later sees `reclaiming` and backs off; one already reading
            // keeps the parts until the next pass.
            if self.db.is_read_pinned(chunk_id) {
                debug!(chunk_id, "reclaim deferred: chunk is being read");
                continue;
            }
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
        let Some(upto) = self.pins.horizon(instance_id) else {
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

            // Re-check liveness immediately before delete (never cache across batches).
            let (to_delete, skipped_batch) = self.filter_deletable(instance_id, &keys).await?;
            skipped += skipped_batch;
            if !to_delete.is_empty() {
                // Orphans are judged by this incarnation's state: delete only
                // while it still holds the generation. (Reclaim needs no check:
                // its deletes follow a committed BeginReclaim.)
                self.db
                    .confirm_active()
                    .await
                    .context("confirm the tablet's generation before deleting orphans")?;
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

    /// Split a candidate batch (all at or below the horizon) into
    /// `(to_delete, skipped)`: parts of any chunk in the state stay (live ones,
    /// and released ones that reclaim will delete once nobody reads them).
    async fn filter_deletable(
        &self,
        instance_id: &str,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let owned = self.db.owned_part_keys_among(instance_id, keys).await?;
        let to_delete: Vec<Vec<u8>> = keys.iter().filter(|k| !owned.contains(*k)).cloned().collect();
        let skipped = (keys.len() - to_delete.len()) as u64;
        Ok((to_delete, skipped))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::IngestOptions;
    use crate::instances::{validate_instances, InstanceConfig};
    use crate::layer::ChunkStore;
    use bytes::Bytes;
    use pigeonhole_blob::{
        erase_sweep, BlobLocator, InstanceInfo, InstanceKind, InstanceRole, OrderedKey, Replicated,
    };
    use pigeonhole_codec::ChunkCodec;
    use pigeonhole_storage_memory::MemoryBlobStore;

    struct Fixture {
        _dir: tempfile::TempDir,
        db: BlobDb,
        mem: Arc<MemoryBlobStore>,
        backend: SharedBackend,
        layer: ChunkStore,
        pins: Arc<UploadPins>,
    }

    async fn open_layer(db: BlobDb, backend: SharedBackend) -> (ChunkStore, Arc<UploadPins>) {
        let replicated = Arc::new(
            Replicated::new(vec![backend], 1, Arc::new(pigeonhole_blob::CheapestFirst::new())).unwrap(),
        );
        let pins = replicated.pins().clone();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open_replicated(db, replicated, opts).await.unwrap();
        (layer, pins)
    }

    async fn setup() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::connect(dir.path().join("blob.log")).await.unwrap();
        let mem = Arc::new(MemoryBlobStore::new().with_instance_id("default"));
        let backend: SharedBackend = Arc::new(erase_sweep(mem.clone()));
        let (layer, pins) = open_layer(db.clone(), backend.clone()).await;
        Fixture { _dir: dir, db, mem, backend, layer, pins }
    }

    fn sweeper(f: &Fixture) -> Sweeper {
        let config = SweepConfig { batch_size: SWEEP_BATCH_SIZE, interval: Duration::from_secs(1) };
        Sweeper::new(f.db.clone(), vec![f.backend.clone()], config, f.pins.clone())
    }

    /// An upload this process saw finish and never committed (a failed chunk).
    async fn orphan(f: &Fixture, data: &'static [u8]) -> BlobLocator {
        let loc = f.backend.put(Bytes::from_static(data), "orphan.bin").await.unwrap();
        f.pins.returned("default", &loc.key);
        loc
    }

    fn on_backend(f: &Fixture, loc: &BlobLocator) -> bool {
        f.mem.message_keys().contains(&u64::from_bytes(&loc.key).unwrap())
    }

    #[tokio::test]
    async fn orphans_below_the_horizon_are_deleted() {
        let f = setup().await;
        let loc = orphan(&f, b"orphan-partial").await;
        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!(stats.keys_deleted, 1);
        assert!(!on_backend(&f, &loc));
    }

    #[tokio::test]
    async fn fenced_sweeper_deletes_nothing() {
        let f = setup().await;
        let loc = orphan(&f, b"orphan").await;
        // A newer incarnation claims the same log; this one has not written since.
        let _newer = BlobDb::connect(f._dir.path().join("blob.log")).await.unwrap();

        let err = sweeper(&f).sweep_once().await.unwrap_err();
        assert!(format!("{err:#}").contains("fenced"), "{err:#}");
        assert!(f.db.tablet_stopped());
        assert!(on_backend(&f, &loc), "orphan kept");
    }

    #[tokio::test]
    async fn keys_above_the_horizon_untouched() {
        let f = setup().await;
        let old = orphan(&f, b"old-orphan").await;
        // An upload this process never saw return (another writer's, say).
        let unseen = f.backend.put(Bytes::from_static(b"unseen"), "orphan.bin").await.unwrap();
        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!(stats.keys_deleted, 1);
        assert!(!on_backend(&f, &old));
        assert!(on_backend(&f, &unseen));
    }

    #[tokio::test]
    async fn pinned_upload_survives_until_unpinned() {
        let f = setup().await;
        let before = orphan(&f, b"before").await;
        // A slow chunk: pinned, uploaded, not committed; later uploads finish.
        let pin = f.pins.pin("default");
        let slow = f.backend.put(Bytes::from_static(b"slow-part"), "orphan.bin").await.unwrap();
        f.pins.returned("default", &slow.key);
        let after = orphan(&f, b"after").await;

        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!(stats.keys_deleted, 1, "only what is below the pin's floor");
        assert!(!on_backend(&f, &before));
        assert!(on_backend(&f, &slow) && on_backend(&f, &after));

        // The writer gave up without committing: its part is garbage now.
        drop(pin);
        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!(stats.keys_deleted, 2);
        assert!(!on_backend(&f, &slow) && !on_backend(&f, &after));
    }

    #[tokio::test]
    async fn committed_parts_below_the_horizon_survive() {
        let f = setup().await;
        let id = f.layer.put_small(Bytes::from_static(b"committed")).await.unwrap();
        let layouts = f.db.get_replica_layouts(id).await.unwrap();
        let loc = layouts[0].parts[0].locator.clone();
        let later = orphan(&f, b"later").await;
        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!((stats.keys_deleted, stats.keys_skipped_live), (1, 1));
        assert!(on_backend(&f, &loc));
        assert!(!on_backend(&f, &later));
    }

    #[tokio::test]
    async fn committed_parts_carry_the_name_they_were_uploaded_under() {
        let f = setup().await;
        let id = f.layer.put_small(Bytes::from_static(b"named")).await.unwrap();
        let part = f.db.get_replica_layouts(id).await.unwrap()[0].parts[0].clone();
        let key = u64::from_bytes(&part.locator.key).unwrap();
        assert_eq!(f.mem.name_of(key), Some(part.blob_id.name()));
        assert_eq!(pigeonhole_blob::BlobId::from_name(&part.blob_id.name()), Some(part.blob_id));
    }

    #[tokio::test]
    async fn repeat_pass_idempotent() {
        let f = setup().await;
        orphan(&f, b"o1").await;
        orphan(&f, b"o2").await;
        let s = sweeper(&f);
        assert_eq!(s.sweep_once().await.unwrap().keys_deleted, 2);
        let mid = f.mem.message_keys();
        assert_eq!(s.sweep_once().await.unwrap().keys_deleted, 0);
        assert_eq!(f.mem.message_keys(), mid);
    }

    #[tokio::test]
    async fn zero_ref_chunk_parts_reclaimed() {
        let f = setup().await;
        let id = f.layer.put_small(Bytes::from_static(b"live-then-release")).await.unwrap();
        let keys_before = f.mem.message_keys();
        assert!(!keys_before.is_empty());
        f.layer.release(&[id]).await.unwrap();
        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!(stats.zero_ref_chunks, 1);
        assert!(f.layer.db().chunk_meta(id).await.unwrap().is_none());
        for k in keys_before {
            assert!(!f.mem.message_keys().contains(&k));
        }
    }

    #[tokio::test]
    async fn reclaim_waits_for_readers_and_late_readers_back_off() {
        let f = setup().await;
        let id = f.layer.put_small(Bytes::from_static(b"being-read")).await.unwrap();
        let keys = f.mem.message_keys();
        let reading = f.db.pin_reads(&[id]);
        f.layer.release(&[id]).await.unwrap();

        // Committed as reclaiming, but a read holds it: parts stay.
        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!(stats.zero_ref_chunks, 0);
        assert_eq!(f.mem.message_keys(), keys);

        // A read starting now sees `reclaiming` and gets ChunkGone.
        let extents = [crate::Extent { chunk: id, offset: 0, len: 10 }];
        let err = f.layer.read(&extents, None).await.unwrap_err();
        assert!(crate::is_chunk_gone(&err), "{err:#}");

        drop(reading);
        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!(stats.zero_ref_chunks, 1);
        assert!(f.db.chunk_meta(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn restart_seeds_the_horizon_from_committed_parts() {
        let f = setup().await;
        // A dead writer's upload, then a committed chunk above it.
        let dead = f.backend.put(Bytes::from_static(b"dead-writer"), "orphan.bin").await.unwrap();
        let id = f.layer.put_small(Bytes::from_static(b"committed")).await.unwrap();

        // Restart: fresh pins, nothing returned in this process yet.
        let (_layer, pins) = open_layer(f.db.clone(), f.backend.clone()).await;
        let config = SweepConfig { batch_size: SWEEP_BATCH_SIZE, interval: Duration::from_secs(1) };
        let stats = Sweeper::new(f.db.clone(), vec![f.backend.clone()], config, pins)
            .sweep_once()
            .await
            .unwrap();
        assert_eq!(stats.keys_deleted, 1);
        assert!(!on_backend(&f, &dead));
        assert!(f.db.chunk_meta(id).await.unwrap().is_some());
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
            max_blob_size: None,
        };
        let mut b = a.clone();
        b.info.id = "b".into();
        b.info.fingerprint = "memory:b".into();
        assert!(validate_instances(&[a, b]).is_err());
    }
}
