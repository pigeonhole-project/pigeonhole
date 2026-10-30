//! Binary-only: long-running tasks spawned by `serve` (sweep, repair, cool).

use crate::runtime::Runtime;
use pigeonhole::config::{Config, InlineSettings};
use pigeonhole_chunk_store::{Repairer, Sweeper};
use pigeonhole_gateway_s3::{CoolPolicy, S3Gateway};
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub fn spawn_background_tasks(cfg: &Config, rt: &Runtime) {
    let members: Vec<_> = rt.store.replicated().members().to_vec();
    let sweeper = Sweeper::new(rt.store.db().clone(), members, cfg.sweep.clone(), rt.store.replicated().pins().clone());
    tokio::spawn(async move {
        sweeper.run_loop().await;
    });
    info!(interval_secs = cfg.sweep.interval.as_secs(), "sweeper background task started");

    let repairer = Repairer::new(rt.store.db().clone(), rt.store.replicated().clone(), cfg.repair.clone());
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

/// The cooler (docs/inline-objects.md): every `[inline] cool_every`, move
/// the inline objects that went cold, or are over budget, out to chunks.
/// With inlining off it still runs, moving out every inline object a
/// previous run left, `cool_batch` per pass.
pub fn spawn_cooler(cfg: &Config, gateway: S3Gateway) {
    let (policy, every) = (cooler_policy(&cfg.inline), cfg.inline.cool_every);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(every).await;
            let t0 = Instant::now();
            match gateway.cool_pass(&policy).await {
                Ok(s) => {
                    metrics::counter!("pigeonhole_cooled_total", "reason" => "age").increment(s.by_age);
                    metrics::counter!("pigeonhole_cooled_total", "reason" => "budget").increment(s.by_budget);
                    metrics::counter!("pigeonhole_cool_rejected_total").increment(s.rejected);
                    if s.by_age + s.by_budget + s.rejected > 0 {
                        info!(
                            by_age = s.by_age,
                            by_budget = s.by_budget,
                            rejected = s.rejected,
                            "cooled inline objects"
                        );
                    }
                    pigeonhole_blob::record_background_pass("cool", true, t0.elapsed());
                }
                Err(e) => {
                    warn!(error = %format!("{e:#}"), "cooler pass failed");
                    pigeonhole_blob::record_background_pass("cool", false, t0.elapsed());
                }
            }
        }
    });
    info!(
        max_object_size = ?cfg.inline.max_object_size,
        budget_bytes = cfg.inline.cool.budget_bytes,
        every_secs = every.as_secs(),
        "cooler background task started"
    );
}

/// The cooler's policy: as configured, or, with inlining off, everything out.
fn cooler_policy(inline: &InlineSettings) -> CoolPolicy {
    if inline.max_object_size.is_some() {
        return inline.cool.clone();
    }
    CoolPolicy { cool_after: Some(Duration::ZERO), min_inline: Duration::ZERO, ..inline.cool.clone() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_inlining_off_the_cooler_moves_everything_out() {
        let mut inline = InlineSettings::off();
        let p = cooler_policy(&inline);
        assert_eq!((p.cool_after, p.min_inline), (Some(Duration::ZERO), Duration::ZERO));
        inline.max_object_size = Some(1024);
        assert_eq!(cooler_policy(&inline), inline.cool);
    }
}
