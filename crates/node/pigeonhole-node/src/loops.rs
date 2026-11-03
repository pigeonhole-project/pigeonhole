//! The node's background loops, each on its dynamic config's schedule:
//! sweep, repair, the cooler, the tablet's gauges; and the followers that
//! keep the knobs and the chunk stores in step with the tablet.

use pigeonhole_admin::Admin;
use pigeonhole_chunk_store::{Repairer, Sweeper};
use pigeonhole_config::Cooling;
use pigeonhole_gateway_s3::{CoolPolicy, S3Gateway};
use pigeonhole_meta::MetaStore;
use pigeonhole_types::schedule::Schedule;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;
use tracing::{info, warn, Instrument};

/// Starts every loop of `admin`'s node (the gauges' with `metrics`), in
/// the caller's span.
pub(crate) fn spawn(admin: &Admin, metrics: bool) -> Vec<JoinHandle<()>> {
    fn start(f: impl Future<Output = ()> + Send + 'static) -> JoinHandle<()> {
        tokio::spawn(f.in_current_span())
    }
    let (d, store) = (&admin.dynamic, &admin.store);
    let replicated = store.replicated();
    let sweeper =
        Sweeper::new(store.db().clone(), replicated.members().to_vec(), d.sweep.get(), replicated.pins().clone());
    let repairer = Repairer::new(store.db().clone(), replicated.clone(), d.repair.get());
    let cooling = d.cool.get();
    info!(
        sweep = ?d.sweep.get().interval,
        repair = ?d.repair.get().interval,
        cool = ?cooling.every,
        inline_on = admin.inline_on,
        budget_bytes = cooling.policy.budget_bytes,
        "background loops started"
    );
    let mut loops = vec![
        start(sweeper.run_loop(d.sweep.schedule())),
        start(repairer.run_loop(d.repair.schedule())),
        start(cooler(d.cool.schedule(), admin.s3.clone(), admin.meta.clone(), admin.inline_on)),
        start(d.clone().follow()),
        // In a cluster the chunk stores are the leader's: taken over when it leads.
        start(store.clone().follow_leadership()),
    ];
    if metrics {
        let chunks = store.db().clone();
        loops.push(start(crate::state_metrics::run(
            admin.meta.clone(),
            admin.index.clone(),
            chunks,
            d.metrics.schedule(),
        )));
    }
    loops
}

/// The cooler (docs/inline-objects.md): every `[inline] cool_every`, move
/// the inline objects that went cold, or are over budget, out to chunks.
/// With inlining off (`inline_on` false) it still runs, moving out every
/// inline object a previous run left, `cool_batch` per pass. Only while
/// this node leads with its quorum; ends once the tablet stops.
async fn cooler(mut schedule: Schedule<Cooling>, s3: S3Gateway, meta: Arc<MetaStore>, inline_on: bool) {
    loop {
        let cooling = schedule.next(|c| c.every).await;
        if meta.is_stopped() {
            return;
        }
        if meta.has_quorum() {
            cool_pass(&s3, &cooling.policy.with_inlining(inline_on)).await;
        } else {
            tracing::debug!("not the leader, or no quorum: no cooler pass");
        }
        schedule.done();
    }
}

/// One pass, its counts recorded (docs/metrics.md).
async fn cool_pass(s3: &S3Gateway, policy: &CoolPolicy) {
    let t0 = Instant::now();
    match s3.cool_pass(policy).await {
        Ok(s) => {
            metrics::counter!("pigeonhole_cooled_total", "reason" => "age").increment(s.by_age);
            metrics::counter!("pigeonhole_cooled_total", "reason" => "budget").increment(s.by_budget);
            metrics::counter!("pigeonhole_cool_rejected_total").increment(s.rejected);
            if s.by_age + s.by_budget + s.rejected > 0 {
                info!(by_age = s.by_age, by_budget = s.by_budget, rejected = s.rejected, "cooled inline objects");
            }
            pigeonhole_blob::record_background_pass("cool", true, t0.elapsed());
        }
        Err(e) => {
            warn!(error = %format!("{e:#}"), "cooler pass failed");
            pigeonhole_blob::record_background_pass("cool", false, t0.elapsed());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn with_inlining_off_the_cooler_moves_everything_out() {
        let policy = CoolPolicy::default();
        let p = policy.with_inlining(false);
        assert_eq!((p.cool_after, p.min_inline), (Some(Duration::ZERO), Duration::ZERO));
        assert_eq!(policy.with_inlining(true), policy);
    }
}
