//! Binary-only: long-running tasks spawned by `serve` (sweep, repair).

use crate::runtime::Runtime;
use pigeonhole::config::Config;
use pigeonhole_chunk_store::{Repairer, Sweeper};
use tracing::info;

pub fn spawn_background_tasks(cfg: &Config, rt: &Runtime) {
    let members: Vec<_> = rt.store.replicated().members().to_vec();
    let sweeper = Sweeper::with_inflight(
        rt.store.db().clone(),
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
}
