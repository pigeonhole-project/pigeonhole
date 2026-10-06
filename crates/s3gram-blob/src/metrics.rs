//! Lightweight backend I/O counters (tracing-friendly; Prometheus later).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Process-wide counters shared by blob backends / limiters.
#[derive(Default)]
pub struct BackendMetrics {
    pub api_calls: AtomicU64,
    pub rate_limited_429: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub limiter_wait_ns: AtomicU64,
}

impl BackendMetrics {
    pub fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.api_calls.load(Ordering::Relaxed),
            self.rate_limited_429.load(Ordering::Relaxed),
            self.bytes_in.load(Ordering::Relaxed),
            self.bytes_out.load(Ordering::Relaxed),
            self.limiter_wait_ns.load(Ordering::Relaxed),
        )
    }

    pub fn record_api_call(&self) {
        self.api_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_429(&self) {
        self.rate_limited_429.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes_in(&self, n: usize) {
        self.bytes_in.fetch_add(n as u64, Ordering::Relaxed);
    }

    pub fn record_bytes_out(&self, n: usize) {
        self.bytes_out.fetch_add(n as u64, Ordering::Relaxed);
    }

    pub fn record_limiter_wait(&self, d: Duration) {
        self.limiter_wait_ns
            .fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
    }
}

/// Spawn a periodic tracing summary (no-op when `interval_secs == 0`).
pub fn spawn_metrics_logger(metrics: Arc<BackendMetrics>, interval_secs: u64) {
    if interval_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let period = Duration::from_secs(interval_secs);
        loop {
            tokio::time::sleep(period).await;
            let (api, r429, bin, bout, wait_ns) = metrics.snapshot();
            info!(
                api_calls = api,
                rate_limited_429 = r429,
                bytes_in = bin,
                bytes_out = bout,
                limiter_wait_ms = wait_ns / 1_000_000,
                "backend metrics"
            );
        }
    });
}
