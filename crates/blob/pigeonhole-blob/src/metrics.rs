//! Process metrics via the [`metrics`] facade (no recorder required).
//!
//! Libraries only call these helpers. The binary optionally installs a
//! Prometheus recorder behind feature `metrics-prometheus`.

use crate::erase::{DynBlobBackend, DynSweep, SharedBackend};
use crate::typed::{CostHint, InstanceInfo, OpKind, BlobLocator};
use crate::BoxByteStream;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_types::{BackendLimits, ByteRange};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Describe metric metadata once at process start (safe to call multiple times).
pub fn describe_metrics() {
    metrics::describe_histogram!(
        "pigeonhole_gateway_request_duration_seconds",
        "Gateway HTTP request duration"
    );
    metrics::describe_histogram!(
        "pigeonhole_instance_call_duration_seconds",
        "Backend instance call duration"
    );
    metrics::describe_counter!(
        "pigeonhole_instance_rate_limited_total",
        "HTTP 429 responses from backend instances"
    );
    metrics::describe_histogram!(
        "pigeonhole_limiter_wait_seconds",
        "Time spent waiting on ChatLimiter token buckets"
    );
    metrics::describe_gauge!(
        "pigeonhole_instance_inflight",
        "In-flight backend ops per instance"
    );
    metrics::describe_counter!(
        "pigeonhole_cache_requests_total",
        "Block cache lookups (hit / miss / collapsed)"
    );
    metrics::describe_counter!(
        "pigeonhole_bytes_from_backend_total",
        "Bytes read from backend instances"
    );
    metrics::describe_counter!(
        "pigeonhole_bytes_to_clients_total",
        "Bytes written to gateway clients"
    );
    metrics::describe_histogram!(
        "pigeonhole_compression_ratio",
        "Stored/logical size ratio for encoded blocks (lower is better)"
    );
    metrics::describe_histogram!(
        "pigeonhole_ingest_budget_wait_seconds",
        "Time waiting for ingest memory budget permits"
    );
    metrics::describe_histogram!(
        "pigeonhole_parts_per_chunk",
        "Number of backend parts produced per chunk replica"
    );
    metrics::describe_counter!("pigeonhole_sweep_keys_total", "Sweeper key outcomes");
    metrics::describe_counter!("pigeonhole_repair_jobs_total", "Repair job outcomes");
    metrics::describe_gauge!(
        "pigeonhole_repair_queue_depth",
        "Pending repair queue depth"
    );
    metrics::describe_gauge!(
        "pigeonhole_superblock_age_seconds",
        "Seconds since last superblock publish"
    );
    metrics::describe_gauge!(
        "pigeonhole_checkpoint_age_seconds",
        "Seconds since last blob.db checkpoint"
    );
    metrics::describe_counter!(
        "pigeonhole_replica_selected_total",
        "Replica instances chosen for reads"
    );
}

pub fn record_gateway_request(gateway: &str, outcome: &str, d: Duration) {
    metrics::histogram!(
        "pigeonhole_gateway_request_duration_seconds",
        "gateway" => gateway.to_owned(),
        "outcome" => outcome.to_owned()
    )
    .record(d.as_secs_f64());
}

pub fn record_instance_call(instance: &str, op: &str, outcome: &str, d: Duration) {
    metrics::histogram!(
        "pigeonhole_instance_call_duration_seconds",
        "instance" => instance.to_owned(),
        "op" => op.to_owned(),
        "outcome" => outcome.to_owned()
    )
    .record(d.as_secs_f64());
}

pub fn record_429(instance: &str, op: &str) {
    metrics::counter!(
        "pigeonhole_instance_rate_limited_total",
        "instance" => instance.to_owned(),
        "op" => op.to_owned()
    )
    .increment(1);
}

pub fn record_limiter_wait(op: &str, d: Duration) {
    if d.is_zero() {
        return;
    }
    metrics::histogram!(
        "pigeonhole_limiter_wait_seconds",
        "op" => op.to_owned()
    )
    .record(d.as_secs_f64());
}

pub fn inflight_inc(instance: &str, op: &str) {
    metrics::gauge!(
        "pigeonhole_instance_inflight",
        "instance" => instance.to_owned(),
        "op" => op.to_owned()
    )
    .increment(1.0);
}

pub fn inflight_dec(instance: &str, op: &str) {
    metrics::gauge!(
        "pigeonhole_instance_inflight",
        "instance" => instance.to_owned(),
        "op" => op.to_owned()
    )
    .decrement(1.0);
}

pub fn record_cache(level: &str, outcome: &str) {
    metrics::counter!(
        "pigeonhole_cache_requests_total",
        "level" => level.to_owned(),
        "outcome" => outcome.to_owned()
    )
    .increment(1);
}

pub fn record_bytes_from_backend(instance: &str, n: u64) {
    if n == 0 {
        return;
    }
    metrics::counter!(
        "pigeonhole_bytes_from_backend_total",
        "instance" => instance.to_owned()
    )
    .increment(n);
}

pub fn record_bytes_to_clients(gateway: &str, n: u64) {
    if n == 0 {
        return;
    }
    metrics::counter!(
        "pigeonhole_bytes_to_clients_total",
        "gateway" => gateway.to_owned()
    )
    .increment(n);
}

pub fn record_compression_ratio(stored: usize, logical: usize) {
    if logical == 0 {
        return;
    }
    metrics::histogram!("pigeonhole_compression_ratio")
        .record(stored as f64 / logical as f64);
}

pub fn record_ingest_budget_wait(d: Duration) {
    if d.is_zero() {
        return;
    }
    metrics::histogram!("pigeonhole_ingest_budget_wait_seconds").record(d.as_secs_f64());
}

pub fn record_parts_per_chunk(instance: &str, parts: usize) {
    metrics::histogram!(
        "pigeonhole_parts_per_chunk",
        "instance" => instance.to_owned()
    )
    .record(parts as f64);
}

pub fn record_sweep(outcome: &str, n: u64) {
    if n == 0 {
        return;
    }
    metrics::counter!(
        "pigeonhole_sweep_keys_total",
        "outcome" => outcome.to_owned()
    )
    .increment(n);
}

pub fn record_repair(outcome: &str, n: u64) {
    if n == 0 {
        return;
    }
    metrics::counter!(
        "pigeonhole_repair_jobs_total",
        "outcome" => outcome.to_owned()
    )
    .increment(n);
}

pub fn set_repair_queue_depth(n: u64) {
    metrics::gauge!("pigeonhole_repair_queue_depth").set(n as f64);
}

pub fn set_superblock_age(d: Duration) {
    metrics::gauge!("pigeonhole_superblock_age_seconds").set(d.as_secs_f64());
}

pub fn set_checkpoint_age(d: Duration) {
    metrics::gauge!("pigeonhole_checkpoint_age_seconds").set(d.as_secs_f64());
}

pub fn record_replica_selected(instance: &str) {
    metrics::counter!(
        "pigeonhole_replica_selected_total",
        "instance" => instance.to_owned()
    )
    .increment(1);
}

fn outcome_from_err(err: &anyhow::Error) -> (&'static str, bool) {
    let msg = format!("{err:#}");
    if msg.contains("429") {
        ("rate_limited", true)
    } else {
        ("error", false)
    }
}

/// Wraps a [`DynBlobBackend`] and emits instance call / byte / in-flight metrics.
pub struct MetricsBackend {
    inner: SharedBackend,
}

impl MetricsBackend {
    pub fn wrap(inner: SharedBackend) -> SharedBackend {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl DynBlobBackend for MetricsBackend {
    fn instance(&self) -> &InstanceInfo {
        self.inner.instance()
    }

    fn limits(&self) -> &BackendLimits {
        self.inner.limits()
    }

    fn cost(&self, op: OpKind, id: Option<&BlobLocator>) -> CostHint {
        self.inner.cost(op, id)
    }

    async fn put(&self, data: Bytes) -> Result<BlobLocator> {
        let instance = self.inner.instance().id.clone();
        let op = "put";
        inflight_inc(&instance, op);
        let t0 = Instant::now();
        let result = self.inner.put(data).await;
        let elapsed = t0.elapsed();
        inflight_dec(&instance, op);
        match &result {
            Ok(_) => record_instance_call(&instance, op, "ok", elapsed),
            Err(e) => {
                let (outcome, is_429) = outcome_from_err(e);
                record_instance_call(&instance, op, outcome, elapsed);
                if is_429 {
                    record_429(&instance, op);
                }
            }
        }
        result
    }

    async fn get(&self, id: &BlobLocator, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let instance = self.inner.instance().id.clone();
        let op = "get";
        inflight_inc(&instance, op);
        let t0 = Instant::now();
        let result = self.inner.get(id, range).await;
        match result {
            Ok(stream) => {
                record_instance_call(&instance, op, "ok", t0.elapsed());
                inflight_dec(&instance, op);
                let inst = instance;
                Ok(Box::pin(stream.map(move |item| {
                    if let Ok(ref b) = item {
                        record_bytes_from_backend(&inst, b.len() as u64);
                    }
                    item
                })))
            }
            Err(e) => {
                let (outcome, is_429) = outcome_from_err(&e);
                record_instance_call(&instance, op, outcome, t0.elapsed());
                if is_429 {
                    record_429(&instance, op);
                }
                inflight_dec(&instance, op);
                Err(e)
            }
        }
    }

    async fn delete(&self, keys: &[Vec<u8>]) -> Result<()> {
        let instance = self.inner.instance().id.clone();
        let op = "delete";
        inflight_inc(&instance, op);
        let t0 = Instant::now();
        let result = self.inner.delete(keys).await;
        let elapsed = t0.elapsed();
        inflight_dec(&instance, op);
        match &result {
            Ok(()) => record_instance_call(&instance, op, "ok", elapsed),
            Err(e) => {
                let (outcome, is_429) = outcome_from_err(e);
                record_instance_call(&instance, op, outcome, elapsed);
                if is_429 {
                    record_429(&instance, op);
                }
            }
        }
        result
    }

    fn sweeper(&self) -> Option<&dyn DynSweep> {
        self.inner.sweeper()
    }
}
