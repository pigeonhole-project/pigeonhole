//! Stage I: replica repair / backfill with re-packing onto the target instance.

use crate::blob_db::{BlobDb, ChunkId, StoredBlock};
use anyhow::{bail, Context, Result};
use futures::StreamExt;
use pigeonhole_blob::{
    collect_stream, EncodedBlock, OpKind, PartLayout, PartPacker, ReplicaLayout, Replicated, SharedBackend, UploadPin,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default pause between repair passes in the binary.
pub const DEFAULT_REPAIR_INTERVAL: Duration = Duration::from_secs(30);
/// Jobs claimed per pass.
pub const REPAIR_BATCH_SIZE: usize = 16;
/// Jobs of a pass copied at once (the stores' limiters still pace them).
pub const REPAIR_CONCURRENCY: usize = 4;
/// Skip a job when `cost().wait_secs` exceeds this on source get or target put.
pub const DEFAULT_MAX_COST_WAIT_SECS: f64 = 2.0;

/// Runtime knobs for the repairer.
#[derive(Clone, Debug)]
pub struct RepairConfig {
    pub interval: Duration,
    pub batch_size: usize,
    /// Jobs copied at once; their replicas are committed together.
    pub concurrency: usize,
    pub max_cost_wait_secs: f64,
    /// When true, each pass also scrubs for under-replicated chunks.
    pub scrub: bool,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_REPAIR_INTERVAL,
            batch_size: REPAIR_BATCH_SIZE,
            concurrency: REPAIR_CONCURRENCY,
            max_cost_wait_secs: DEFAULT_MAX_COST_WAIT_SECS,
            scrub: true,
        }
    }
}

/// Counts from one [`Repairer::repair_once`] pass.
#[derive(Clone, Debug, Default)]
pub struct RepairStats {
    pub scrub_enqueued: u64,
    pub repaired: u64,
    pub skipped_budget: u64,
    pub skipped_noop: u64,
    pub failed: u64,
}

/// Background repair / backfill worker.
pub struct Repairer {
    db: BlobDb,
    replicated: Arc<Replicated>,
    config: RepairConfig,
}

impl Repairer {
    pub fn new(db: BlobDb, replicated: Arc<Replicated>, config: RepairConfig) -> Self {
        Self { db, replicated, config }
    }

    pub fn config(&self) -> &RepairConfig {
        &self.config
    }

    /// Background loop: scrub (optional), drain queue, sleep; ends once the
    /// tablet stops.
    pub async fn run_loop(self) {
        let period = self.config.interval.max(Duration::from_secs(1));
        loop {
            if self.db.tablet_stopped() {
                info!("metadata tablet stopped; repair exits");
                return;
            }
            let t0 = std::time::Instant::now();
            let pass = self.repair_once().await;
            pigeonhole_blob::record_background_pass("repair", pass.is_ok(), t0.elapsed());
            match pass {
                Ok(stats) => {
                    pigeonhole_blob::record_repair("repaired", stats.repaired);
                    pigeonhole_blob::record_repair("scrub_enqueued", stats.scrub_enqueued);
                    pigeonhole_blob::record_repair("skipped_budget", stats.skipped_budget);
                    pigeonhole_blob::record_repair("skipped_noop", stats.skipped_noop);
                    pigeonhole_blob::record_repair("failed", stats.failed);
                    if let Ok(depth) = self.db.repair_queue_len().await {
                        pigeonhole_blob::set_repair_queue_depth(depth);
                    }
                    if stats.repaired > 0 || stats.scrub_enqueued > 0 || stats.failed > 0 {
                        info!(
                            repaired = stats.repaired,
                            scrub_enqueued = stats.scrub_enqueued,
                            skipped_budget = stats.skipped_budget,
                            failed = stats.failed,
                            "repair pass complete"
                        );
                    } else {
                        debug!(
                            skipped_noop = stats.skipped_noop,
                            skipped_budget = stats.skipped_budget,
                            "repair pass idle"
                        );
                    }
                }
                Err(e) => warn!(error = %e, "repair pass failed"),
            }
            tokio::time::sleep(period).await;
        }
    }

    /// Single pass: optional scrub, then up to `batch_size` jobs, copied
    /// `concurrency` at a time; the copies made are committed in one entry.
    pub async fn repair_once(&self) -> Result<RepairStats> {
        let mut stats = RepairStats::default();
        if self.config.scrub {
            stats.scrub_enqueued = self.scrub_enqueue().await?;
        }
        let jobs = self.db.take_repair_jobs(self.config.batch_size).await.context("take repair jobs")?;
        let copied: Vec<_> = futures::stream::iter(jobs)
            .map(|(chunk_id, instance_id)| async move {
                let copy = self.copy_one(chunk_id, &instance_id).await;
                (chunk_id, instance_id, copy)
            })
            .buffer_unordered(self.config.concurrency.max(1))
            .collect()
            .await;
        let mut ready = Vec::new();
        for (chunk_id, instance_id, copy) in copied {
            match copy {
                Ok(Copied::Ready(copy)) => ready.push(copy),
                Ok(Copied::Noop) => {
                    stats.skipped_noop += 1;
                    let _ = self.db.dequeue_repair(chunk_id, &instance_id).await;
                }
                Ok(Copied::DeferredBudget) => stats.skipped_budget += 1,
                Err(e) => {
                    stats.failed += 1;
                    warn!(
                        chunk_id,
                        instance = %instance_id,
                        error = %e,
                        "repair job failed"
                    );
                }
            }
        }
        self.commit(ready, &mut stats).await;
        Ok(stats)
    }

    /// Commit the copies of a pass in one entry. A copy whose chunk went
    /// meanwhile is dropped (its parts deleted); if the commit fails, the
    /// jobs stay queued.
    async fn commit(&self, ready: Vec<Ready>, stats: &mut RepairStats) {
        if ready.is_empty() {
            return;
        }
        let replicas: Vec<_> = ready.iter().map(|r| (r.chunk_id, &r.layout)).collect();
        let added = match self.db.commit_instance_replicas(&replicas).await {
            Ok(added) => added,
            Err(e) => {
                stats.failed += ready.len() as u64;
                warn!(jobs = ready.len(), error = %e, "commit repaired replicas failed");
                return;
            }
        };
        for (r, added) in ready.into_iter().zip(added) {
            let instance = r.layout.instance.as_str();
            let _ = self.db.dequeue_repair(r.chunk_id, instance).await;
            if added {
                stats.repaired += 1;
                debug!(chunk_id = r.chunk_id, instance, parts = r.layout.parts.len(), "repaired replica");
            } else {
                stats.skipped_noop += 1;
                self.discard(&r).await;
            }
        }
    }

    /// Delete the parts of a copy nothing will refer to; best effort.
    async fn discard(&self, r: &Ready) {
        let Some(target) = self.replicated.member(&r.layout.instance) else { return };
        let keys: Vec<Vec<u8>> = r.layout.parts.iter().map(|p| p.locator.key.clone()).collect();
        if let Err(e) = target.delete(&keys).await {
            debug!(chunk_id = r.chunk_id, instance = %r.layout.instance, error = %e, "repair: copy of a gone chunk stays");
        }
    }

    /// Enqueue repairs for every live chunk of `instance_id`'s shard missing
    /// a replica on it.
    pub async fn enqueue_instance_backfill(&self, instance_id: &str) -> Result<u64> {
        let Some(peers) = self.replicated.shard_peers(instance_id) else {
            return Ok(0);
        };
        let peers: Vec<&str> = peers.iter().map(|m| m.instance().id.as_str()).collect();
        let missing = self.db.chunks_missing_instance(instance_id, &peers).await?;
        let mut n = 0u64;
        for chunk_id in missing {
            self.db.enqueue_repair(chunk_id, instance_id).await?;
            n += 1;
        }
        Ok(n)
    }

    /// Verify `instance_id`: probe each live replica it holds (its first
    /// part) and enqueue a repair for every one that is gone. A store that
    /// lost blobs behind our back (a cleared channel) is found without a
    /// read happening to hit it; scrub only sees what the metadata lacks.
    /// How many replicas it checked, and how many were gone.
    pub async fn verify_instance(&self, instance_id: &str) -> Result<(u64, u64)> {
        let Some(target) = self.replicated.member(instance_id).cloned() else {
            bail!("verify: {instance_id} is not in the placement");
        };
        let (mut checked, mut gone) = (0u64, 0u64);
        for chunk_id in self.db.chunks_on_instance(instance_id).await? {
            let layouts = self.db.get_replica_layouts(chunk_id).await?;
            let Some(layout) = layouts.iter().find(|l| l.instance == instance_id) else { continue };
            checked += 1;
            // A store that cannot answer fails the pass: gone is only what it says is gone.
            let part = layout.parts.first().context("verify: a replica without parts")?;
            if !target.exists(&part.locator).await.with_context(|| format!("verify {instance_id}"))? {
                warn!(chunk_id, instance = instance_id, "verify: the replica is gone");
                self.db.enqueue_repair(chunk_id, instance_id).await?;
                gone += 1;
            }
        }
        info!(instance = instance_id, checked, gone, "verified");
        Ok((checked, gone))
    }

    /// Scrub: enqueue under-replicated (chunk, member) pairs; prioritize happens at take.
    pub async fn scrub_enqueue(&self) -> Result<u64> {
        // Single-member shards have nothing to backfill; scrubbing them only
        // burns budget and can race ingest under heavy S3 suites.
        if self.replicated.members().len() == self.replicated.shard_count() {
            return Ok(0);
        }
        let mut n = 0u64;
        for member in self.replicated.members() {
            let id = member.instance().id.as_str();
            n += self.enqueue_instance_backfill(id).await?;
        }
        Ok(n)
    }

    /// Copy `chunk_id` onto `instance_id`, uncommitted.
    async fn copy_one(&self, chunk_id: ChunkId, instance_id: &str) -> Result<Copied> {
        let Some(target) = self.replicated.member(instance_id).cloned() else {
            // Instance left the placement group — drop the job.
            self.db.dequeue_repair(chunk_id, instance_id).await?;
            return Ok(Copied::Noop);
        };

        let layouts = self.db.get_replica_layouts(chunk_id).await?;
        // A chunk belongs to the shard it was written to; never copy it into
        // another shard.
        let peers = self.replicated.shard_peers(instance_id).unwrap_or_default();
        if !layouts.iter().any(|l| peers.iter().any(|p| p.instance().id == l.instance)) {
            self.db.dequeue_repair(chunk_id, instance_id).await?;
            return Ok(Copied::Noop);
        }
        if layouts.iter().any(|l| l.instance == instance_id) {
            // Already present; still allow re-pack if caller deleted backend blobs
            // and re-enqueued. Detect healthy replica by probing the first part.
            if let Some(existing) = layouts.iter().find(|l| l.instance == instance_id) {
                if replica_probe_ok(&target, existing).await {
                    self.db.dequeue_repair(chunk_id, instance_id).await?;
                    return Ok(Copied::Noop);
                }
            }
        }

        let sources: Vec<ReplicaLayout> = layouts.into_iter().filter(|l| l.instance != instance_id).collect();
        if sources.is_empty() {
            bail!("repair chunk {chunk_id}: no source replica for {instance_id}");
        }

        let source_backend = sources
            .iter()
            .find_map(|s| self.replicated.member(&s.instance).cloned())
            .context("repair: source member missing from placement")?;

        if !within_cost_budget(source_backend.as_ref(), target.as_ref(), self.config.max_cost_wait_secs) {
            return Ok(Copied::DeferredBudget);
        }

        let blocks = self.db.get_blocks(chunk_id).await?;
        let encoded = read_encoded_blocks(self.replicated.as_ref(), &sources, &blocks).await?;

        let mut packer = PartPacker::new(target.clone(), self.replicated.pins().clone());
        let mut parts: Vec<PartLayout> = Vec::new();
        let mut pins = Vec::new();
        for block in encoded {
            if let Some(part) = packer.push(block).await.context("repair packer push")? {
                let (layout, pin) = part.into_layout();
                pins.push(pin);
                parts.push(layout);
            }
        }
        if let Some(part) = packer.finish().await.context("repair packer finish")? {
            let (layout, pin) = part.into_layout();
            pins.push(pin);
            parts.push(layout);
        }
        if parts.is_empty() {
            bail!("repair chunk {chunk_id}: produced no parts for {instance_id}");
        }

        let layout = ReplicaLayout { instance: instance_id.to_string(), parts };
        Ok(Copied::Ready(Ready { chunk_id, layout, _pins: pins }))
    }
}

/// What [`Repairer::copy_one`] did.
enum Copied {
    Ready(Ready),
    Noop,
    DeferredBudget,
}

/// A replica copied, to commit.
struct Ready {
    chunk_id: ChunkId,
    layout: ReplicaLayout,
    /// Held until the commit: the parts are no orphans meanwhile.
    _pins: Vec<UploadPin>,
}

fn within_cost_budget(
    source: &dyn pigeonhole_blob::DynBlobBackend,
    target: &dyn pigeonhole_blob::DynBlobBackend,
    max_wait: f64,
) -> bool {
    let get = source.cost(OpKind::Get, None);
    let put = target.cost(OpKind::Put, None);
    get.wait_secs <= max_wait && put.wait_secs <= max_wait
}

/// The replica's first part is in the store, asked of the store (a cache
/// of ours could still hold a deleted text blob); any failure counts as gone.
async fn replica_probe_ok(backend: &SharedBackend, layout: &ReplicaLayout) -> bool {
    let Some(part) = layout.parts.first() else {
        return false;
    };
    backend.exists(&part.locator).await.unwrap_or(false)
}

/// Read stored block payloads from any live source replica (ChunkId unchanged).
async fn read_encoded_blocks(
    replicated: &Replicated,
    sources: &[ReplicaLayout],
    blocks: &[StoredBlock],
) -> Result<Vec<EncodedBlock>> {
    if blocks.is_empty() {
        // Raw single-part chunk (no chunk_blocks rows).
        let raw = collect_stream(replicated.read(sources, 0..1).await.context("repair read raw")?).await?;
        return Ok(vec![EncodedBlock { stored: raw.clone(), logical_len: raw.len() as u32, codec: "raw".into() }]);
    }

    let end = blocks.len() as u32;
    let blob = collect_stream(replicated.read(sources, 0..end).await.context("repair read blocks")?).await?;

    let mut out = Vec::with_capacity(blocks.len());
    let mut off = 0usize;
    for b in blocks {
        let len = b.stored_len.max(0) as usize;
        if off + len > blob.len() {
            bail!("repair: truncated stored payload at block {} (need {} have {})", b.block_no, off + len, blob.len());
        }
        let stored = blob.slice(off..off + len);
        off += len;
        if crc32fast::hash(&stored) != b.crc32 {
            // Never copy a damaged block onto another member.
            bail!("repair: block {} fails its checksum on the source", b.block_no);
        }
        out.push(EncodedBlock { stored, logical_len: b.logical_len.max(0) as u32, codec: b.codec.clone() });
    }
    if off != blob.len() {
        bail!("repair: stored payload length mismatch (consumed {off}, got {})", blob.len());
    }
    Ok(out)
}

/// Spawn a best-effort enqueue (the read path must not wait for the metadata log).
pub fn spawn_enqueue_repair(db: BlobDb, chunk_id: ChunkId, instance_id: String) {
    tokio::spawn(async move {
        if let Err(e) = db.enqueue_repair(chunk_id, &instance_id).await {
            warn!(
                chunk_id,
                instance = %instance_id,
                error = %e,
                "enqueue repair from read path failed"
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::IngestOptions;
    use crate::layer::ChunkStore;
    use bytes::Bytes;
    use futures::stream;
    use pigeonhole_blob::{erase, CheapestFirst};
    use pigeonhole_codec::ChunkCodec;
    use pigeonhole_storage_memory::MemoryBlobStore;
    use pigeonhole_types::{BackendLimits, RangeSupport};

    const MIB: usize = 1024 * 1024;

    fn lim(id: &str, max_blob_size: usize) -> SharedBackend {
        Arc::new(erase(
            MemoryBlobStore::with_limits(BackendLimits {
                max_blob_size,
                supports_range: RangeSupport::BestEffort,
                can_list: false,
            })
            .with_instance_id(id),
        ))
    }

    async fn open_layer(
        members: Vec<SharedBackend>,
        write_quorum: usize,
        chunk_size: usize,
        block_size: usize,
    ) -> (tempfile::TempDir, ChunkStore, Arc<Replicated>) {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
        let rep = Arc::new(Replicated::new(members, write_quorum, Arc::new(CheapestFirst::new())).unwrap());
        let mut opts = IngestOptions::new(chunk_size, ChunkCodec::Raw);
        opts.block_size = block_size;
        let layer = ChunkStore::open_replicated(db, rep.clone(), opts).await.unwrap();
        (dir, layer, rep)
    }

    #[tokio::test]
    async fn add_smaller_instance_repacks_and_reads() {
        let a = lim("a-19", 19 * MIB);
        let (_dir, layer, rep) = open_layer(vec![a.clone()], 1, 32 * MIB, 6 * MIB).await;

        let n = 18 * MIB;
        let body = Bytes::from((0..n).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        let ingested =
            layer.ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]), None).await.unwrap();
        assert_eq!(ingested.extents.len(), 1);
        let chunk_id = ingested.extents[0].chunk;

        let before = layer.db().get_replica_layouts(chunk_id).await.unwrap();
        assert_eq!(before.len(), 1);
        let parts_a = before[0].parts.len();
        assert!(parts_a >= 1);

        // Add 10 MiB instance into a new placement + repairer.
        let b_store = MemoryBlobStore::with_limits(BackendLimits {
            max_blob_size: 10 * MIB,
            supports_range: RangeSupport::BestEffort,
            can_list: false,
        })
        .with_instance_id("b-10");
        let b: SharedBackend = Arc::new(erase(b_store));
        let rep2 = Arc::new(Replicated::new(vec![a, b.clone()], 1, Arc::new(CheapestFirst::new())).unwrap());
        // Sync new instance into blob.db.
        layer
            .db()
            .sync_instances(&rep2.members().iter().map(|m| m.instance().clone()).collect::<Vec<_>>())
            .await
            .unwrap();

        let repairer =
            Repairer::new(layer.db().clone(), rep2.clone(), RepairConfig { scrub: false, ..RepairConfig::default() });
        let enq = repairer.enqueue_instance_backfill("b-10").await.unwrap();
        assert_eq!(enq, 1);

        let stats = repairer.repair_once().await.unwrap();
        assert_eq!(stats.repaired, 1);

        let after = layer.db().get_replica_layouts(chunk_id).await.unwrap();
        assert_eq!(after.len(), 2);
        let parts_b = after.iter().find(|l| l.instance == "b-10").unwrap().parts.len();
        // 6 MiB blocks into 10 MiB parts → more parts than 19 MiB packing.
        assert!(parts_b > parts_a, "expected different packing: a={parts_a} b={parts_b}");

        let mut opts2 = IngestOptions::new(32 * MIB, ChunkCodec::Raw);
        opts2.block_size = 6 * MIB;
        let layer2 = ChunkStore::open_replicated(layer.db().clone(), rep2, opts2).await.unwrap();

        for layout in &after {
            let got = layer2.read_from_replicas(chunk_id, 0, n, std::slice::from_ref(layout)).await.unwrap();
            assert_eq!(got, body, "replica {}", layout.instance);
        }
        let _ = rep;
    }

    #[tokio::test]
    async fn repair_stays_inside_a_chunks_shard() {
        let (a1, a2, b) = (lim("a1", 4 * MIB), lim("a2", 4 * MIB), lim("b", 4 * MIB));
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let shards = |a: Vec<SharedBackend>| {
            Arc::new(Replicated::sharded(vec![(a, 1), (vec![b.clone()], 1)], Arc::new(CheapestFirst::new())).unwrap())
        };
        let layer =
            ChunkStore::open_replicated(db.clone(), shards(vec![a1.clone()]), IngestOptions::new(MIB, ChunkCodec::Raw))
                .await
                .unwrap();
        let on_a = layer.put_small(Bytes::from_static(b"first")).await.unwrap();
        let on_b = layer.put_small(Bytes::from_static(b"second")).await.unwrap();
        let where_is = |id| {
            let db = db.clone();
            async move {
                let mut ids: Vec<_> =
                    db.get_replica_layouts(id).await.unwrap().into_iter().map(|l| l.instance).collect();
                ids.sort();
                ids
            }
        };
        assert_eq!((where_is(on_a).await, where_is(on_b).await), (vec!["a1".to_string()], vec!["b".to_string()]));

        // a2 joins the first shard: only that shard's chunk is copied to it.
        let rep = shards(vec![a1, a2]);
        db.sync_instances(&rep.members().iter().map(|m| m.instance().clone()).collect::<Vec<_>>()).await.unwrap();
        let repairer = Repairer::new(db.clone(), rep, RepairConfig { scrub: false, ..RepairConfig::default() });
        assert_eq!(repairer.scrub_enqueue().await.unwrap(), 1);
        assert_eq!(repairer.repair_once().await.unwrap().repaired, 1);
        assert_eq!(where_is(on_a).await, vec!["a1".to_string(), "a2".to_string()]);
        assert_eq!(where_is(on_b).await, vec!["b".to_string()]);

        // A job aimed at another shard is dropped, not executed.
        db.enqueue_repair(on_b, "a2").await.unwrap();
        assert_eq!(repairer.repair_once().await.unwrap().repaired, 0);
        assert_eq!(where_is(on_b).await, vec!["b".to_string()]);
    }

    #[tokio::test]
    async fn lost_part_is_repaired() {
        let a: SharedBackend =
            Arc::new(erase(MemoryBlobStore::with_limits(BackendLimits::memory()).with_instance_id("a")));
        let b: SharedBackend =
            Arc::new(erase(MemoryBlobStore::with_limits(BackendLimits::memory()).with_instance_id("b")));
        let (_dir, layer, rep) = open_layer(vec![a.clone(), b], 2, 8 * MIB, MIB).await;

        let data = Bytes::from(vec![7u8; 3 * MIB]);
        let ingested =
            layer.ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]), None).await.unwrap();
        let chunk_id = ingested.extents[0].chunk;
        let layouts = layer.db().get_replica_layouts(chunk_id).await.unwrap();
        assert_eq!(layouts.len(), 2);

        let victim = layouts.iter().find(|l| l.instance == "a").unwrap();
        let keys: Vec<Vec<u8>> = victim.parts.iter().map(|p| p.locator.key.clone()).collect();
        a.delete(&keys).await.unwrap();

        // Verify finds the lost replica without a read; repair puts it back.
        let repairer =
            Repairer::new(layer.db().clone(), rep.clone(), RepairConfig { scrub: false, ..RepairConfig::default() });
        assert_eq!(repairer.verify_instance("b").await.unwrap(), (1, 0), "b is intact");
        assert_eq!(repairer.verify_instance("a").await.unwrap(), (1, 1), "a lost its parts");
        let stats = repairer.repair_once().await.unwrap();
        assert_eq!(stats.repaired, 1);

        let fixed = layer.db().get_replica_layouts(chunk_id).await.unwrap();
        let a_layout = fixed.iter().find(|l| l.instance == "a").unwrap();
        let got = layer.read_from_replicas(chunk_id, 0, data.len(), std::slice::from_ref(a_layout)).await.unwrap();
        assert_eq!(got, data);
    }

    /// The member set `[a]` grown to `[a, b]`, synced into the layer.
    async fn add_b(layer: &ChunkStore, a: SharedBackend) -> Arc<Replicated> {
        let b = lim("b", 4 * MIB);
        let rep = Arc::new(Replicated::new(vec![a, b], 1, Arc::new(CheapestFirst::new())).unwrap());
        let infos: Vec<_> = rep.members().iter().map(|m| m.instance().clone()).collect();
        layer.db().sync_instances(&infos).await.unwrap();
        rep
    }

    #[tokio::test]
    async fn a_pass_commits_its_copies_in_one_entry() {
        let a = lim("a", 4 * MIB);
        let (_dir, layer, _) = open_layer(vec![a.clone()], 1, MIB, MIB).await;
        let mut chunks = Vec::new();
        for i in 0..6u8 {
            chunks.push(layer.put_small(Bytes::from(vec![i; 1000])).await.unwrap());
        }
        let rep = add_b(&layer, a).await;
        let repairer =
            Repairer::new(layer.db().clone(), rep.clone(), RepairConfig { scrub: false, ..RepairConfig::default() });
        assert_eq!(repairer.enqueue_instance_backfill("b").await.unwrap(), 6);

        let before = layer.db().meta().applied_position();
        let stats = repairer.repair_once().await.unwrap();
        assert_eq!((stats.repaired, stats.failed), (6, 0));
        assert_eq!(layer.db().meta().applied_position(), before + 1, "six copies, one entry");
        assert_eq!(layer.db().repair_queue_len().await.unwrap(), 0);
        for (i, id) in chunks.into_iter().enumerate() {
            let layouts = layer.db().get_replica_layouts(id).await.unwrap();
            let b = layouts.iter().find(|l| l.instance == "b").expect("a copy on b");
            let got = collect_stream(rep.read(std::slice::from_ref(b), 0..1).await.unwrap()).await.unwrap();
            assert_eq!(got, Bytes::from(vec![i as u8; 1000]));
        }
    }

    #[tokio::test]
    async fn a_copy_of_a_dropped_chunk_is_rejected_alone() {
        let a = lim("a", 4 * MIB);
        let (_dir, layer, _) = open_layer(vec![a], 1, MIB, MIB).await;
        let db = layer.db();
        let (kept, dropped) = (
            layer.put_small(Bytes::from_static(b"kept")).await.unwrap(),
            layer.put_small(Bytes::from_static(b"dropped")).await.unwrap(),
        );
        let layout = |id| async move { db.get_replica_layouts(id).await.unwrap().remove(0) };
        let (kept_layout, dropped_layout) = (layout(kept).await, layout(dropped).await);
        db.delete_chunk_metadata(dropped).await.unwrap();

        // Dropped while its copy was made: that one is not added, the tablet goes on.
        let added = db.commit_instance_replicas(&[(kept, &kept_layout), (dropped, &dropped_layout)]).await.unwrap();
        assert_eq!(added, vec![true, false]);
        assert!(db.get_replica_layouts(dropped).await.unwrap().is_empty());
        layer.put_small(Bytes::from_static(b"after")).await.unwrap();
    }
}
