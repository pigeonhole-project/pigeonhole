//! Binary-only: long-running tasks spawned by `serve` (durability, snapshots, sweep, repair).

use crate::runtime::Runtime;
use pigeonhole::config::Config;
use pigeonhole::index::Index;
use pigeonhole_blob::{set_checkpoint_age, set_superblock_age};
use pigeonhole_chunk_store::{Repairer, Sweeper};
use pigeonhole_gateway_s3::snapshot::push_index_snapshot_durable;
use std::time::Duration;
use tracing::{info, warn};

const JOURNAL_FLUSH_SECS: u64 = 5;
const DEFAULT_CHECKPOINT_SECS: u64 = 300;

pub fn spawn_background_tasks(
    cfg: &Config,
    rt: &Runtime,
    index: &Index,
    s3_gateway: &pigeonhole::S3Gateway,
) {
    let dur = rt.durability.clone();
    tokio::spawn(async move {
        let period = Duration::from_secs(JOURNAL_FLUSH_SECS);
        loop {
            tokio::time::sleep(period).await;
            if let Err(e) = dur.flush_journal().await {
                warn!(error = %e, "journal flush failed");
            }
        }
    });

    let checkpoint_secs = if cfg.snapshot_interval_secs > 0 {
        cfg.snapshot_interval_secs.max(60)
    } else {
        DEFAULT_CHECKPOINT_SECS
    };
    let dur = rt.durability.clone();
    let store = rt.store.clone();
    tokio::spawn(async move {
        let period = Duration::from_secs(checkpoint_secs);
        loop {
            tokio::time::sleep(period).await;
            if let Err(e) = dur.checkpoint(store.db()).await {
                warn!(error = %e, "blob.db checkpoint failed");
            }
        }
    });

    if cfg.snapshot_interval_secs > 0 {
        let idx = index.clone();
        let st = rt.store.clone();
        let dur = rt.durability.clone();
        let gate = s3_gateway.snapshot_gate.clone();
        let interval = cfg.snapshot_interval_secs;
        tokio::spawn(async move {
            let period = Duration::from_secs(interval);
            loop {
                tokio::time::sleep(period).await;
                let _g = gate.lock().await;
                if let Err(e) = push_index_snapshot_durable(&idx, st.as_ref(), dur.as_ref()).await {
                    warn!(error = %e, "periodic s3 index snapshot failed");
                }
            }
        });
    }

    let members: Vec<_> = rt.store.replicated().members().to_vec();
    let sweeper = Sweeper::with_inflight(
        rt.store.db().clone(),
        rt.durability.clone(),
        members,
        cfg.sweep.clone(),
        rt.store.replicated().inflight().clone(),
    );
    tokio::spawn(async move {
        sweeper.run_loop().await;
    });
    info!(
        grace_secs = cfg.sweep.grace.as_secs(),
        interval_secs = cfg.sweep.interval.as_secs(),
        "sweeper background task started"
    );

    let repairer = Repairer::new(
        rt.store.db().clone(),
        rt.store.replicated().clone(),
        cfg.repair.clone(),
    );
    tokio::spawn(async move {
        repairer.run_loop().await;
    });
    info!(
        interval_secs = cfg.repair.interval.as_secs(),
        batch_size = cfg.repair.batch_size,
        scrub = cfg.repair.scrub,
        "repair background task started"
    );

    if cfg.metrics.enabled {
        let dur = rt.durability.clone();
        tokio::spawn(async move {
            let period = Duration::from_secs(15);
            loop {
                tokio::time::sleep(period).await;
                if let Some(age) = dur.superblock_age().await {
                    set_superblock_age(age);
                }
                if let Some(age) = dur.checkpoint_age().await {
                    set_checkpoint_age(age);
                }
            }
        });
    }
}
