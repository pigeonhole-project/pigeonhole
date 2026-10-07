//! Stage I: replica repair / backfill with re-packing onto the target instance.

use crate::blob_db::{BlobDb, ChunkId, StoredBlock};
use anyhow::{bail, Context, Result};
use pigeonhole_blob::{
    collect_stream, EncodedBlock, OpKind, PartLayout, PartPacker, ReplicaLayout, Replicated,
    SharedBackend,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default pause between repair passes in the binary.
pub const DEFAULT_REPAIR_INTERVAL: Duration = Duration::from_secs(30);
/// Jobs claimed per pass.
pub const REPAIR_BATCH_SIZE: usize = 16;
/// Skip a job when `cost().wait_secs` exceeds this on source get or target put.
pub const DEFAULT_MAX_COST_WAIT_SECS: f64 = 2.0;

/// Runtime knobs for the repairer.
#[derive(Clone, Debug)]
pub struct RepairConfig {
    pub interval: Duration,
    pub batch_size: usize,
    pub max_cost_wait_secs: f64,
    /// When true, each pass also scrubs for under-replicated chunks.
    pub scrub: bool,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_REPAIR_INTERVAL,
            batch_size: REPAIR_BATCH_SIZE,
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
        Self {
            db,
            replicated,
            config,
        }
    }

    pub fn config(&self) -> &RepairConfig {
        &self.config
    }

    /// Background loop: scrub (optional), drain queue, sleep.
    pub async fn run_loop(self) {
        let period = self.config.interval.max(Duration::from_secs(1));
        loop {
            match self.repair_once().await {
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

    /// Single pass: optional scrub, then process up to `batch_size` jobs.
    pub async fn repair_once(&self) -> Result<RepairStats> {
        let mut stats = RepairStats::default();
        if self.config.scrub {
            stats.scrub_enqueued = self.scrub_enqueue().await?;
        }
        let jobs = self
            .db
            .take_repair_jobs(self.config.batch_size)
            .await
            .context("take repair jobs")?;
        for (chunk_id, instance_id) in jobs {
            match self.repair_one(chunk_id, &instance_id).await {
                Ok(RepairOutcome::Repaired) => stats.repaired += 1,
                Ok(RepairOutcome::Noop) => {
                    stats.skipped_noop += 1;
                    let _ = self.db.dequeue_repair(chunk_id, &instance_id).await;
                }
                Ok(RepairOutcome::DeferredBudget) => stats.skipped_budget += 1,
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
        Ok(stats)
    }

    /// Enqueue repairs for every live chunk missing a replica on `instance_id`.
    pub async fn enqueue_instance_backfill(&self, instance_id: &str) -> Result<u64> {
        let missing = self.db.chunks_missing_instance(instance_id).await?;
        let mut n = 0u64;
        for chunk_id in missing {
            self.db.enqueue_repair(chunk_id, instance_id).await?;
            n += 1;
        }
        Ok(n)
    }

    /// Scrub: enqueue under-replicated (chunk, member) pairs; prioritize happens at take.
    pub async fn scrub_enqueue(&self) -> Result<u64> {
        // Single-member groups have nothing to backfill; scrubbing them only
        // burns budget and can race ingest under heavy S3 suites.
        if self.replicated.members().len() <= 1 {
            return Ok(0);
        }
        let mut n = 0u64;
        for member in self.replicated.members() {
            let id = member.instance().id.as_str();
            n += self.enqueue_instance_backfill(id).await?;
        }
        Ok(n)
    }

    async fn repair_one(&self, chunk_id: ChunkId, instance_id: &str) -> Result<RepairOutcome> {
        let Some(target) = self.replicated.member(instance_id).cloned() else {
            // Instance left the placement group — drop the job.
            self.db.dequeue_repair(chunk_id, instance_id).await?;
            return Ok(RepairOutcome::Noop);
        };

        let layouts = self.db.get_replica_layouts(chunk_id).await?;
        if layouts.iter().any(|l| l.instance == instance_id) {
            // Already present; still allow re-pack if caller deleted backend blobs
            // and re-enqueued. Detect healthy replica by probing the first part.
            if let Some(existing) = layouts.iter().find(|l| l.instance == instance_id) {
                if replica_probe_ok(&target, existing).await {
                    self.db.dequeue_repair(chunk_id, instance_id).await?;
                    return Ok(RepairOutcome::Noop);
                }
            }
        }

        let sources: Vec<ReplicaLayout> = layouts
            .into_iter()
            .filter(|l| l.instance != instance_id)
            .collect();
        if sources.is_empty() {
            bail!("repair chunk {chunk_id}: no source replica for {instance_id}");
        }

        let source_backend = sources
            .iter()
            .find_map(|s| self.replicated.member(&s.instance).cloned())
            .context("repair: source member missing from placement")?;

        if !within_cost_budget(
            source_backend.as_ref(),
            target.as_ref(),
            self.config.max_cost_wait_secs,
        ) {
            return Ok(RepairOutcome::DeferredBudget);
        }

        let blocks = self.db.get_blocks(chunk_id).await?;
        let encoded = read_encoded_blocks(self.replicated.as_ref(), &sources, &blocks).await?;

        let mut packer = PartPacker::new(target.clone());
        let mut parts: Vec<PartLayout> = Vec::new();
        for block in encoded {
            if let Some(part) = packer.push(block).await.context("repair packer push")? {
                parts.push(part.into());
            }
        }
        if let Some(part) = packer.finish().await.context("repair packer finish")? {
            parts.push(part.into());
        }
        if parts.is_empty() {
            bail!("repair chunk {chunk_id}: produced no parts for {instance_id}");
        }

        let layout = ReplicaLayout {
            instance: instance_id.to_string(),
            parts,
        };
        self.db
            .commit_instance_replica(chunk_id, &layout)
            .await
            .context("commit repaired replica")?;
        self.db.dequeue_repair(chunk_id, instance_id).await?;
        debug!(
            chunk_id,
            instance = %instance_id,
            parts = layout.parts.len(),
            "repaired replica"
        );
        Ok(RepairOutcome::Repaired)
    }
}

enum RepairOutcome {
    Repaired,
    Noop,
    DeferredBudget,
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

async fn replica_probe_ok(backend: &SharedBackend, layout: &ReplicaLayout) -> bool {
    let Some(part) = layout.parts.first() else {
        return false;
    };
    match backend.get(&part.locator, None).await {
        Ok(stream) => collect_stream(stream).await.is_ok(),
        Err(_) => false,
    }
}

/// Read stored block payloads from any live source replica (ChunkId unchanged).
async fn read_encoded_blocks(
    replicated: &Replicated,
    sources: &[ReplicaLayout],
    blocks: &[StoredBlock],
) -> Result<Vec<EncodedBlock>> {
    if blocks.is_empty() {
        // Raw single-part chunk (no chunk_blocks rows).
        let raw = collect_stream(
            replicated
                .read(sources, 0..1)
                .await
                .context("repair read raw")?,
        )
        .await?;
        return Ok(vec![EncodedBlock {
            stored: raw.clone(),
            logical_len: raw.len() as u32,
            codec: "raw".into(),
        }]);
    }

    let end = blocks.len() as u32;
    let blob = collect_stream(
        replicated
            .read(sources, 0..end)
            .await
            .context("repair read blocks")?,
    )
    .await?;

    let mut out = Vec::with_capacity(blocks.len());
    let mut off = 0usize;
    for b in blocks {
        let len = b.stored_len.max(0) as usize;
        if off + len > blob.len() {
            bail!(
                "repair: truncated stored payload at block {} (need {} have {})",
                b.block_no,
                off + len,
                blob.len()
            );
        }
        let stored = blob.slice(off..off + len);
        off += len;
        out.push(EncodedBlock {
            stored,
            logical_len: b.logical_len.max(0) as u32,
            codec: b.codec.clone(),
        });
    }
    if off != blob.len() {
        bail!(
            "repair: stored payload length mismatch (consumed {off}, got {})",
            blob.len()
        );
    }
    Ok(out)
}

/// Whether an error message looks like a missing backend blob (enqueue repair).
pub fn is_part_not_found(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}").to_ascii_lowercase();
    msg.contains("404")
        || msg.contains("not found")
        || msg.contains("unknown file")
        || msg.contains("missing")
        || msg.contains("no such")
}

/// Spawn a best-effort enqueue (read path must not block on SQLite).
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
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let rep = Arc::new(
            Replicated::new(members, write_quorum, Arc::new(CheapestFirst::new())).unwrap(),
        );
        let mut opts = IngestOptions::new(chunk_size, ChunkCodec::Raw);
        opts.block_size = block_size;
        let layer = ChunkStore::open_replicated(db, rep.clone(), opts)
            .await
            .unwrap();
        (dir, layer, rep)
    }

    #[tokio::test]
    async fn add_smaller_instance_repacks_and_reads() {
        let a = lim("a-19", 19 * MIB);
        let (_dir, layer, rep) = open_layer(vec![a.clone()], 1, 32 * MIB, 6 * MIB).await;

        let n = 18 * MIB;
        let body = Bytes::from((0..n).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        let ingested = layer
            .ingest(
                stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]),
                None,
            )
            .await
            .unwrap();
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
        let rep2 = Arc::new(
            Replicated::new(
                vec![a, b.clone()],
                1,
                Arc::new(CheapestFirst::new()),
            )
            .unwrap(),
        );
        // Sync new instance into blob.db.
        layer
            .db()
            .sync_instances(
                &rep2
                    .members()
                    .iter()
                    .map(|m| m.instance().clone())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();

        let repairer = Repairer::new(
            layer.db().clone(),
            rep2.clone(),
            RepairConfig {
                scrub: false,
                ..RepairConfig::default()
            },
        );
        let enq = repairer.enqueue_instance_backfill("b-10").await.unwrap();
        assert_eq!(enq, 1);

        let stats = repairer.repair_once().await.unwrap();
        assert_eq!(stats.repaired, 1);

        let after = layer.db().get_replica_layouts(chunk_id).await.unwrap();
        assert_eq!(after.len(), 2);
        let parts_b = after
            .iter()
            .find(|l| l.instance == "b-10")
            .unwrap()
            .parts
            .len();
        // 6 MiB blocks into 10 MiB parts → more parts than 19 MiB packing.
        assert!(
            parts_b > parts_a,
            "expected different packing: a={parts_a} b={parts_b}"
        );

        let mut opts2 = IngestOptions::new(32 * MIB, ChunkCodec::Raw);
        opts2.block_size = 6 * MIB;
        let layer2 = ChunkStore::open_replicated(layer.db().clone(), rep2, opts2)
            .await
            .unwrap();

        for layout in &after {
            let got = layer2
                .read_from_replicas(chunk_id, 0, n, std::slice::from_ref(layout))
                .await
                .unwrap();
            assert_eq!(got, body, "replica {}", layout.instance);
        }
        let _ = rep;
    }

    #[tokio::test]
    async fn lost_part_is_repaired() {
        let a: SharedBackend = Arc::new(erase(
            MemoryBlobStore::with_limits(BackendLimits::memory()).with_instance_id("a"),
        ));
        let b: SharedBackend = Arc::new(erase(
            MemoryBlobStore::with_limits(BackendLimits::memory()).with_instance_id("b"),
        ));
        let (_dir, layer, rep) = open_layer(vec![a.clone(), b], 2, 8 * MIB, MIB).await;

        let data = Bytes::from(vec![7u8; 3 * MIB]);
        let ingested = layer
            .ingest(
                stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]),
                None,
            )
            .await
            .unwrap();
        let chunk_id = ingested.extents[0].chunk;
        let layouts = layer.db().get_replica_layouts(chunk_id).await.unwrap();
        assert_eq!(layouts.len(), 2);

        let victim = layouts.iter().find(|l| l.instance == "a").unwrap();
        let keys: Vec<Vec<u8>> = victim.parts.iter().map(|p| p.locator.key.clone()).collect();
        a.delete(&keys).await.unwrap();

        // Probe fails → enqueue + repair.
        layer.db().enqueue_repair(chunk_id, "a").await.unwrap();
        let repairer = Repairer::new(
            layer.db().clone(),
            rep.clone(),
            RepairConfig {
                scrub: false,
                ..RepairConfig::default()
            },
        );
        let stats = repairer.repair_once().await.unwrap();
        assert_eq!(stats.repaired, 1);

        let fixed = layer.db().get_replica_layouts(chunk_id).await.unwrap();
        let a_layout = fixed.iter().find(|l| l.instance == "a").unwrap();
        let got = layer
            .read_from_replicas(chunk_id, 0, data.len(), std::slice::from_ref(a_layout))
            .await
            .unwrap();
        assert_eq!(got, data);
    }
}
