//! Reclaim zero-ref chunks and orphan backend blobs.
//!
//! Orphans are swept only up to the instance's upload horizon
//! ([`UploadPins::horizon`]): every key at or below it is either a committed
//! part (kept) or garbage, because uploads are pinned before they start and
//! unpinned only after their commit is applied.

use crate::blob_db::BlobDb;
use anyhow::{Context, Result};
use pigeonhole_blob::{ReplicaLayout, SharedBackend, UploadPins};
use pigeonhole_types::schedule::Schedule;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Candidates fetched / deleted per batch.
pub const SWEEP_BATCH_SIZE: usize = 100;
/// Default pause between full sweep passes in the binary.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Runtime knobs for the sweeper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SweepConfig {
    pub batch_size: usize,
    /// Pause between passes; `None`: no passes but those asked for.
    pub interval: Option<Duration>,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self { batch_size: SWEEP_BATCH_SIZE, interval: Some(DEFAULT_SWEEP_INTERVAL) }
    }
}

/// Counts from one [`Sweeper::sweep_once`] pass.
#[derive(Clone, Debug, Default)]
pub struct SweepStats {
    pub zero_ref_chunks: u64,
    pub keys_deleted: u64,
    pub keys_skipped_live: u64,
    /// Instances the pass could not sweep (or delete a reclaimed chunk's
    /// parts on): down, rate-limited. The others are swept all the same.
    pub failed: Vec<String>,
}

/// One sweeper per process; iterates every member of the placement group.
pub struct Sweeper {
    db: BlobDb,
    members: Vec<SharedBackend>,
    config: SweepConfig,
    /// The upload pins of this process (`Replicated::pins`).
    pins: Arc<UploadPins>,
}

impl Sweeper {
    pub fn new(db: BlobDb, members: Vec<SharedBackend>, config: SweepConfig, pins: Arc<UploadPins>) -> Self {
        Self { db, members, config, pins }
    }

    pub fn config(&self) -> &SweepConfig {
        &self.config
    }

    /// Background loop: [`Self::sweep_once`] whenever `schedule` says, with
    /// its knobs, while this node leads; ends once the tablet stops (a fenced
    /// incarnation must not delete anything).
    pub async fn run_loop(mut self, mut schedule: Schedule<SweepConfig>) {
        loop {
            self.config = schedule.next(|c| c.interval).await;
            if self.db.tablet_stopped() {
                info!("metadata tablet stopped; sweeper exits");
                return;
            }
            if self.db.can_write() {
                self.background_pass().await;
            } else {
                debug!("not the leader, or no quorum: no sweep pass");
            }
            schedule.done();
        }
    }

    /// One pass of the background loop, recorded and logged.
    async fn background_pass(&self) {
        let t0 = std::time::Instant::now();
        let pass = self.sweep_once().await;
        let ok = pass.as_ref().is_ok_and(|s| s.failed.is_empty());
        pigeonhole_blob::record_background_pass("sweep", ok, t0.elapsed());
        match pass {
            Ok(stats) => {
                if !stats.failed.is_empty() {
                    warn!(failed = ?stats.failed, "sweep pass left instances out");
                }
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
                    debug!(keys_skipped_live = stats.keys_skipped_live, "sweep pass idle");
                }
            }
            Err(e) => warn!(error = %e, "sweep pass failed"),
        }
    }

    /// Single pass: reclaim `refs = 0` chunks, then range-sweep each instance.
    /// An instance that fails (down, rate-limited) is left for the next pass
    /// ([`SweepStats::failed`]); the others are swept all the same.
    pub async fn sweep_once(&self) -> Result<SweepStats> {
        let mut stats = SweepStats::default();
        self.reclaim_zero_refs(&mut stats).await?;
        for member in &self.members {
            self.sweep_instance(member, &mut stats).await?;
        }
        Ok(stats)
    }

    async fn reclaim_zero_refs(&self, stats: &mut SweepStats) -> Result<()> {
        let chunks = self.db.list_zero_ref_chunks().await?;
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
            // Until a stored snapshot shows the chunk reclaiming, restoring from the
            // newest one would resurrect objects that point at these parts.
            if !self.db.reclaim_in_snapshot(chunk_id) {
                debug!(chunk_id, "reclaim deferred: waiting for a snapshot");
                continue;
            }
            let layouts = self.db.get_replica_layouts(chunk_id).await?;
            // A chunk whose parts could not all go keeps its metadata: the
            // next pass deletes them again.
            if self.delete_parts(chunk_id, &layouts, stats).await {
                self.db.delete_chunk_metadata(chunk_id).await?;
                stats.zero_ref_chunks += 1;
            }
        }
        Ok(())
    }

    /// Delete a reclaimed chunk's parts on every instance; whether all went.
    async fn delete_parts(&self, chunk_id: i64, layouts: &[ReplicaLayout], stats: &mut SweepStats) -> bool {
        let mut all_gone = true;
        for layout in layouts {
            let Some(backend) = self.members.iter().find(|m| m.instance().id == layout.instance) else {
                warn!(chunk_id, instance = %layout.instance, "zero-ref reclaim: instance not in placement; leaving parts");
                continue;
            };
            let keys: Vec<Vec<u8>> = layout.parts.iter().map(|p| p.locator.key.clone()).collect();
            if keys.is_empty() {
                continue;
            }
            if let Err(e) = backend.delete(&keys).await {
                warn!(chunk_id, instance = %layout.instance, error = %format!("{e:#}"), "zero-ref reclaim: parts not deleted; next pass");
                fail(stats, &layout.instance);
                all_gone = false;
            }
        }
        all_gone
    }

    /// Range-sweep one instance up to its horizon. The instance failing (a
    /// list or a delete) leaves it for the next pass, from where it got to;
    /// the metadata failing (or the tablet fenced) fails the pass.
    async fn sweep_instance(&self, backend: &SharedBackend, stats: &mut SweepStats) -> Result<()> {
        let instance_id = backend.instance().id.as_str();
        let Some(upto) = self.pins.horizon(instance_id) else {
            return Ok(());
        };
        let failed = |stats: &mut SweepStats, what: &str, e: anyhow::Error| {
            warn!(instance = instance_id, error = %format!("{e:#}"), "sweep: {what} failed; the instance waits for the next pass");
            fail(stats, instance_id);
        };

        let mut after = self.db.get_sweep_cursor(instance_id).await?;
        let batch = self.config.batch_size.max(1);

        loop {
            let keys = match backend.candidates(self.pins.owner(), after.as_deref(), &upto, batch).await {
                Ok(keys) => keys,
                Err(e) => {
                    failed(stats, "listing", e);
                    return Ok(());
                }
            };
            if keys.is_empty() {
                self.db.set_sweep_cursor(instance_id, None).await?;
                break;
            }

            // Re-check liveness immediately before delete (never cache across batches).
            let (to_delete, skipped_batch) = self.filter_deletable(instance_id, &keys).await?;
            stats.keys_skipped_live += skipped_batch;
            if !to_delete.is_empty() {
                // Orphans are judged by this incarnation's state: delete only
                // while it still holds the generation. (Reclaim needs no check:
                // its deletes follow a committed BeginReclaim.)
                self.db.confirm_active().await.context("confirm the tablet's generation before deleting orphans")?;
                if let Err(e) = backend.delete(&to_delete).await {
                    failed(stats, "delete", e);
                    return Ok(());
                }
                stats.keys_deleted += to_delete.len() as u64;
            }

            let last = keys.last().cloned();
            after = last.clone();
            self.db.set_sweep_cursor(instance_id, last.as_deref()).await?;

            if keys.len() < batch {
                self.db.set_sweep_cursor(instance_id, None).await?;
                break;
            }
        }
        Ok(())
    }

    /// Split a candidate batch (all at or below the horizon) into
    /// `(to_delete, skipped)`: parts of any chunk in the state stay (live ones,
    /// and released ones that reclaim will delete once nobody reads them).
    async fn filter_deletable(&self, instance_id: &str, keys: &[Vec<u8>]) -> Result<(Vec<Vec<u8>>, u64)> {
        let owned = self.db.owned_part_keys_among(instance_id, keys).await?;
        let to_delete: Vec<Vec<u8>> = keys.iter().filter(|k| !owned.contains(*k)).cloned().collect();
        let skipped = (keys.len() - to_delete.len()) as u64;
        Ok((to_delete, skipped))
    }
}

/// Instance `instance` failed this pass (once in the list).
fn fail(stats: &mut SweepStats, instance: &str) {
    if !stats.failed.iter().any(|i| i == instance) {
        stats.failed.push(instance.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::IngestOptions;
    use crate::layer::ChunkStore;
    use bytes::Bytes;
    use pigeonhole_blob::{erase, BlobLocator, OrderedKey, Replicated};
    use pigeonhole_codec::ChunkCodec;
    use pigeonhole_storage_memory::MemoryBlobStore;

    struct Fixture {
        dir: tempfile::TempDir,
        db: BlobDb,
        mem: Arc<MemoryBlobStore>,
        backend: SharedBackend,
        layer: ChunkStore,
        pins: Arc<UploadPins>,
    }

    async fn open_layer(db: BlobDb, backend: SharedBackend) -> (ChunkStore, Arc<UploadPins>) {
        let replicated =
            Arc::new(Replicated::new(vec![backend], 1, Arc::new(pigeonhole_blob::CheapestFirst::new())).unwrap());
        let pins = replicated.pins().clone();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open_replicated(db, replicated, opts).await.unwrap();
        (layer, pins)
    }

    async fn setup() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let mem = Arc::new(MemoryBlobStore::new().with_instance_id("default"));
        let backend: SharedBackend = Arc::new(erase(mem.clone()));
        let (layer, pins) = open_layer(db.clone(), backend.clone()).await;
        Fixture { dir, db, mem, backend, layer, pins }
    }

    fn sweeper(f: &Fixture) -> Sweeper {
        let config = SweepConfig { batch_size: SWEEP_BATCH_SIZE, interval: Some(Duration::from_secs(1)) };
        Sweeper::new(f.db.clone(), vec![f.backend.clone()], config, f.pins.clone())
    }

    /// A name of this deployment's blobs.
    fn ours(f: &Fixture) -> String {
        pigeonhole_blob::BlobId::random_of(f.pins.owner()).name()
    }

    /// An upload this process saw finish and never committed (a failed chunk).
    async fn orphan(f: &Fixture, data: &'static [u8]) -> BlobLocator {
        let loc = f.backend.put(Bytes::from_static(data), &ours(f)).await.unwrap();
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
        let _newer = BlobDb::open(pigeonhole_testkit::tablet(f.dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();

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
        let unseen = f.backend.put(Bytes::from_static(b"unseen"), &ours(&f)).await.unwrap();
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
        let slow = f.backend.put(Bytes::from_static(b"slow-part"), &ours(&f)).await.unwrap();
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
    async fn reclaim_waits_for_a_snapshot_that_shows_it() {
        let dir = tempfile::tempdir().unwrap();
        let opts = pigeonhole_meta::MetaOptions {
            snapshot_every: None,
            snapshot_interval: Some(Duration::from_millis(50)),
            snapshots_outlive_log: true,
            ..Default::default()
        };
        let (log, snapshots) = pigeonhole_testkit::disk_meta(dir.path());
        let namespaces = BlobDb::register(Default::default());
        let meta = pigeonhole_meta::MetaStore::open(log, snapshots, namespaces, opts).await.unwrap();
        let db = BlobDb::open(meta.clone()).await.unwrap();
        let mem = Arc::new(MemoryBlobStore::new().with_instance_id("default"));
        let backend: SharedBackend = Arc::new(erase(mem.clone()));
        let (layer, pins) = open_layer(db.clone(), backend.clone()).await;
        let f = Fixture { dir, db, mem, backend, layer, pins };

        let id = f.layer.put_small(Bytes::from_static(b"released")).await.unwrap();
        let keys = f.mem.message_keys();
        f.layer.release(&[id]).await.unwrap();
        let stats = sweeper(&f).sweep_once().await.unwrap();
        assert_eq!(stats.zero_ref_chunks, 0, "no snapshot shows the release yet");
        assert_eq!(f.mem.message_keys(), keys);
        let position = meta.applied_position();
        assert_eq!(sweeper(&f).sweep_once().await.unwrap().zero_ref_chunks, 0);
        assert_eq!(meta.applied_position(), position, "a deferred reclaim proposes nothing again");

        while meta.restorable_position() < Some(position) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(sweeper(&f).sweep_once().await.unwrap().zero_ref_chunks, 1);
        assert!(f.mem.message_keys().is_empty());
    }

    #[tokio::test]
    async fn restart_seeds_the_horizon_from_committed_parts() {
        let f = setup().await;
        // A dead writer's upload, then a committed chunk above it.
        let dead = f.backend.put(Bytes::from_static(b"dead-writer"), &ours(&f)).await.unwrap();
        let id = f.layer.put_small(Bytes::from_static(b"committed")).await.unwrap();

        // Restart: fresh pins, nothing returned in this process yet.
        let (_layer, pins) = open_layer(f.db.clone(), f.backend.clone()).await;
        let config = SweepConfig { batch_size: SWEEP_BATCH_SIZE, interval: Some(Duration::from_secs(1)) };
        let stats = Sweeper::new(f.db.clone(), vec![f.backend.clone()], config, pins).sweep_once().await.unwrap();
        assert_eq!(stats.keys_deleted, 1);
        assert!(!on_backend(&f, &dead));
        assert!(f.db.chunk_meta(id).await.unwrap().is_some());
    }

    /// Two deployments on one store by mistake: neither one's sweep takes
    /// the other's blobs for orphans, though each knows only its own.
    #[tokio::test]
    async fn another_deployments_blobs_are_never_orphans() {
        let a = setup().await;
        let dir_b = tempfile::tempdir().unwrap();
        let db_b = BlobDb::open(pigeonhole_testkit::tablet(dir_b.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let (layer_b, pins_b) = open_layer(db_b.clone(), a.backend.clone()).await;
        assert_ne!(a.pins.owner(), pins_b.owner());

        let id_a = a.layer.put_small(Bytes::from_static(b"a's object")).await.unwrap();
        let id_b = layer_b.put_small(Bytes::from_static(b"b's object")).await.unwrap();
        let both = a.mem.message_keys();
        assert_eq!(both.len(), 2);

        let config = SweepConfig { batch_size: SWEEP_BATCH_SIZE, interval: Some(Duration::from_secs(1)) };
        let by_b = Sweeper::new(db_b.clone(), vec![a.backend.clone()], config.clone(), pins_b).sweep_once().await;
        assert_eq!(by_b.unwrap().keys_deleted, 0, "b's sweep left a's part");
        assert_eq!(sweeper(&a).sweep_once().await.unwrap().keys_deleted, 0, "a's sweep left b's part");
        assert_eq!(a.mem.message_keys(), both);
        assert!(a.db.chunk_meta(id_a).await.unwrap().is_some());
        assert!(db_b.chunk_meta(id_b).await.unwrap().is_some());
    }

    /// A blob store with a down switch: every call fails while it is down.
    struct Switch {
        inner: MemoryBlobStore,
        down: std::sync::atomic::AtomicBool,
    }

    impl Switch {
        fn up(&self) -> Result<()> {
            match self.down.load(std::sync::atomic::Ordering::SeqCst) {
                true => anyhow::bail!("{} is down", pigeonhole_blob::BlobBackend::instance(&self.inner).id),
                false => Ok(()),
            }
        }
    }

    #[async_trait::async_trait]
    impl pigeonhole_blob::BlobBackend for Switch {
        type Id = <MemoryBlobStore as pigeonhole_blob::BlobBackend>::Id;
        type Key = <MemoryBlobStore as pigeonhole_blob::BlobBackend>::Key;
        fn instance(&self) -> &pigeonhole_blob::InstanceInfo {
            self.inner.instance()
        }
        fn limits(&self) -> &pigeonhole_blob::BackendLimits {
            self.inner.limits()
        }
        fn key(id: &Self::Id) -> Self::Key {
            MemoryBlobStore::key(id)
        }
        fn cost(&self, op: pigeonhole_blob::OpKind, id: Option<&Self::Id>) -> pigeonhole_blob::CostHint {
            self.inner.cost(op, id)
        }
        async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
            self.up()?;
            self.inner.put(data, name).await
        }
        async fn get(
            &self,
            id: &Self::Id,
            range: Option<pigeonhole_blob::ByteRange>,
        ) -> Result<pigeonhole_blob::BoxByteStream> {
            self.up()?;
            self.inner.get(id, range).await
        }
        async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
            self.up()?;
            self.inner.delete(keys).await
        }
        async fn candidates(
            &self,
            owner: pigeonhole_blob::Owner,
            after: Option<Self::Key>,
            upto: Self::Key,
            limit: usize,
        ) -> Result<Vec<Self::Key>> {
            self.up()?;
            self.inner.candidates(owner, after, upto, limit).await
        }
    }

    /// Two copies of every chunk: on `first` (with a down switch), then on
    /// `second` (memory); the sweeper goes through them in that order.
    struct Pair {
        _dir: tempfile::TempDir,
        db: BlobDb,
        first: Arc<Switch>,
        second: Arc<MemoryBlobStore>,
        layer: ChunkStore,
        members: Vec<SharedBackend>,
        pins: Arc<UploadPins>,
    }

    async fn pair() -> Pair {
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let first =
            Arc::new(Switch { inner: MemoryBlobStore::new().with_instance_id("first"), down: Default::default() });
        let second = Arc::new(MemoryBlobStore::new().with_instance_id("second"));
        let members: Vec<SharedBackend> = vec![Arc::new(erase(first.clone())), Arc::new(erase(second.clone()))];
        let replicated =
            Arc::new(Replicated::new(members.clone(), 2, Arc::new(pigeonhole_blob::CheapestFirst::new())).unwrap());
        let pins = replicated.pins().clone();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open_replicated(db.clone(), replicated, opts).await.unwrap();
        Pair { _dir: dir, db, first, second, layer, members, pins }
    }

    impl Pair {
        fn sweeper(&self) -> Sweeper {
            let config = SweepConfig { batch_size: SWEEP_BATCH_SIZE, interval: Some(Duration::from_secs(1)) };
            Sweeper::new(self.db.clone(), self.members.clone(), config, self.pins.clone())
        }

        fn set_down(&self, down: bool) {
            self.first.down.store(down, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// The first copy down, a write of two fails, its part on the second is
    /// an orphan: the sweep takes it, past the first's failure, and says so.
    #[tokio::test]
    async fn an_instance_down_does_not_keep_the_others_from_being_swept() {
        let p = pair().await;
        p.layer.put_small(Bytes::from_static(b"on both")).await.unwrap();
        let live = p.second.message_keys();
        p.set_down(true);
        p.layer.put_small(Bytes::from_static(b"one copy of two")).await.unwrap_err();
        assert_eq!(p.second.message_keys().len(), live.len() + 1, "the orphan");

        let stats = p.sweeper().sweep_once().await.unwrap();
        assert_eq!((stats.keys_deleted, stats.failed.clone()), (1, vec!["first".to_string()]));
        assert_eq!(p.second.message_keys(), live);

        p.set_down(false);
        let stats = p.sweeper().sweep_once().await.unwrap();
        assert!(stats.failed.is_empty(), "{:?}", stats.failed);
    }

    /// A released chunk whose first copy cannot be deleted keeps its
    /// metadata; its second copy goes, and the first once it is back.
    #[tokio::test]
    async fn a_reclaim_that_cannot_reach_a_copy_waits_for_it() {
        let p = pair().await;
        let id = p.layer.put_small(Bytes::from_static(b"released")).await.unwrap();
        p.layer.release(&[id]).await.unwrap();
        p.set_down(true);

        let stats = p.sweeper().sweep_once().await.unwrap();
        assert_eq!((stats.zero_ref_chunks, stats.failed.clone()), (0, vec!["first".to_string()]));
        assert!(p.db.chunk_meta(id).await.unwrap().is_some(), "kept for the next pass");
        assert!(p.second.message_keys().is_empty(), "the reachable copy went");

        p.set_down(false);
        let stats = p.sweeper().sweep_once().await.unwrap();
        assert_eq!((stats.zero_ref_chunks, stats.failed.len()), (1, 0));
        assert!(p.db.chunk_meta(id).await.unwrap().is_none());
        assert!(p.first.inner.message_keys().is_empty());
    }
}
