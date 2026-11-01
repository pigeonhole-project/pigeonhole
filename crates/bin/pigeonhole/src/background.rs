//! Binary-only: long-running tasks spawned by `serve` (sweep, repair, cool),
//! each on its dynamic config's schedule.

use crate::runtime::Runtime;
use pigeonhole_chunk_store::{Repairer, Sweeper};
use pigeonhole_gateway_s3::{CoolPolicy, S3Gateway};
use std::time::Instant;
use tracing::{info, warn};

pub fn spawn_background_tasks(rt: &Runtime) {
    let d = &rt.dynamic;
    let members: Vec<_> = rt.store.replicated().members().to_vec();
    let sweeper = Sweeper::new(rt.store.db().clone(), members, d.sweep.get(), rt.store.replicated().pins().clone());
    tokio::spawn(sweeper.run_loop(d.sweep.schedule()));
    info!(interval = ?d.sweep.get().interval, "sweeper background task started");

    let repair = d.repair.get();
    let repairer = Repairer::new(rt.store.db().clone(), rt.store.replicated().clone(), repair.clone());
    tokio::spawn(repairer.run_loop(d.repair.schedule()));
    info!(
        interval = ?repair.interval,
        batch_size = repair.batch_size,
        scrub = repair.scrub,
        "repair background task started"
    );
}

/// The cooler (docs/inline-objects.md): every `[inline] cool_every`, move
/// the inline objects that went cold, or are over budget, out to chunks.
/// With inlining off (`inline_on` false) it still runs, moving out every
/// inline object a previous run left, `cool_batch` per pass. Ends once the
/// tablet stops.
pub fn spawn_cooler(rt: &Runtime, inline_on: bool, gateway: S3Gateway) {
    let (mut schedule, meta) = (rt.dynamic.cool.schedule(), rt.meta.clone());
    tokio::spawn(async move {
        loop {
            let cooling = schedule.next(|c| c.every).await;
            if meta.is_stopped() {
                return;
            }
            cool_pass(&gateway, &cooling.policy.with_inlining(inline_on)).await;
            schedule.done();
        }
    });
    let cooling = rt.dynamic.cool.get();
    info!(
        inline_on,
        budget_bytes = cooling.policy.budget_bytes,
        every = ?cooling.every,
        "cooler background task started"
    );
}

async fn cool_pass(gateway: &S3Gateway, policy: &CoolPolicy) {
    let t0 = Instant::now();
    match gateway.cool_pass(policy).await {
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
