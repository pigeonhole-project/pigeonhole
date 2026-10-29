//! Process metrics via the [`metrics`] facade (no recorder required).
//!
//! Libraries only call these helpers. The binary optionally installs a
//! Prometheus recorder behind feature `metrics-prometheus`.

use crate::erase::{DynBlobBackend, SharedBackend};
use crate::typed::{CostHint, InstanceInfo, OpKind, BlobLocator};
use crate::BoxByteStream;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_types::{BackendLimits, ByteRange, MetricDoc, MetricKind};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::Instrument;

/// The metrics this crate's helpers record.
pub const METRICS: &[MetricDoc] = &[
    MetricDoc::histogram(
        "pigeonhole_gateway_request_duration_seconds",
        &["gateway", "op", "outcome"],
        "Gateway request time to response headers; op is the S3 operation (unknown: routing or auth failed first), outcome ok / client_error / server_error / cancelled (the client went away first)",
    ),
    MetricDoc::counter("pigeonhole_bytes_from_clients_total", &["gateway", "op"], "Request body bytes received from gateway clients"),
    MetricDoc::counter("pigeonhole_bytes_to_clients_total", &["gateway", "op"], "Response body bytes sent to gateway clients"),
    MetricDoc::histogram("pigeonhole_instance_call_duration_seconds", &["instance", "op", "outcome"], "Backend call duration (get: until the first byte)"),
    MetricDoc::gauge("pigeonhole_instance_inflight", &["instance", "op"], "Backend calls in flight (a get until its stream is read or dropped)"),
    MetricDoc::counter("pigeonhole_bytes_to_backend_total", &["instance"], "Bytes written to backend instances"),
    MetricDoc::counter("pigeonhole_bytes_from_backend_total", &["instance"], "Bytes read from backend instances"),
    MetricDoc::counter(
        "pigeonhole_instance_rate_limited_total",
        &["instance", "op"],
        "Backend calls that failed rate limited (a 429 / FLOOD_WAIT longer than the call's budget)",
    ),
    MetricDoc::counter("pigeonhole_replica_selected_total", &["instance"], "Replicas chosen to read from"),
    MetricDoc::histogram("pigeonhole_parts_per_chunk", &["instance"], "Backend parts written per chunk replica"),
    MetricDoc::counter(
        "pigeonhole_cache_requests_total",
        &["level", "outcome"],
        "Block cache lookups: hit, miss (loaded the block), collapsed (waited for another lookup's load)",
    ),
    MetricDoc::gauge("pigeonhole_cache_bytes", &["level"], "Bytes held by a block cache level"),
    MetricDoc::gauge("pigeonhole_cache_entries", &["level"], "Blocks held by a block cache level"),
    MetricDoc::histogram("pigeonhole_compression_ratio", &[], "Stored / logical size of each block written (lower is better)"),
    MetricDoc::histogram("pigeonhole_ingest_budget_wait_seconds", &[], "Time PUTs wait for the ingest memory budget (waits > 0 only)"),
    MetricDoc::histogram("pigeonhole_background_pass_seconds", &["task", "outcome"], "Duration of a sweep / repair pass"),
    MetricDoc::counter("pigeonhole_sweep_keys_total", &["outcome"], "Sweeper outcomes: deleted keys, live keys skipped, zero-ref chunks reclaimed"),
    MetricDoc::counter("pigeonhole_repair_jobs_total", &["outcome"], "Repair job outcomes"),
    MetricDoc::gauge("pigeonhole_repair_queue_depth", &[], "Pending repair jobs"),
    MetricDoc::counter("pigeonhole_chunk_gone_total", &[], "Retains that failed: the chunk was reclaiming or missing"),
    MetricDoc::counter("pigeonhole_double_release_total", &[], "Releases of a chunk whose refs were already 0"),
];

/// Describe `docs` to the installed recorder (help text in the scrape).
pub fn describe(docs: &[MetricDoc]) {
    for d in docs {
        match d.kind {
            MetricKind::Counter => metrics::describe_counter!(d.name, d.help),
            MetricKind::Gauge => metrics::describe_gauge!(d.name, d.help),
            MetricKind::Histogram => metrics::describe_histogram!(d.name, d.help),
        }
    }
}

pub fn record_background_pass(task: &str, ok: bool, d: Duration) {
    metrics::histogram!(
        "pigeonhole_background_pass_seconds",
        "task" => task.to_owned(),
        "outcome" => if ok { "ok" } else { "error" }
    )
    .record(d.as_secs_f64());
}

pub fn record_gateway_request(gateway: &str, op: &str, outcome: &str, d: Duration) {
    metrics::histogram!(
        "pigeonhole_gateway_request_duration_seconds",
        "gateway" => gateway.to_owned(),
        "op" => op.to_owned(),
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

pub fn record_bytes_to_backend(instance: &str, n: u64) {
    metrics::counter!(
        "pigeonhole_bytes_to_backend_total",
        "instance" => instance.to_owned()
    )
    .increment(n);
}

pub fn record_bytes_to_clients(gateway: &str, op: &str, n: u64) {
    if n == 0 {
        return;
    }
    metrics::counter!(
        "pigeonhole_bytes_to_clients_total",
        "gateway" => gateway.to_owned(),
        "op" => op.to_owned()
    )
    .increment(n);
}

pub fn record_bytes_from_clients(gateway: &str, op: &str, n: u64) {
    if n == 0 {
        return;
    }
    metrics::counter!(
        "pigeonhole_bytes_from_clients_total",
        "gateway" => gateway.to_owned(),
        "op" => op.to_owned()
    )
    .increment(n);
}

pub fn set_cache_size(level: &str, bytes: u64, entries: u64) {
    metrics::gauge!("pigeonhole_cache_bytes", "level" => level.to_owned()).set(bytes as f64);
    metrics::gauge!("pigeonhole_cache_entries", "level" => level.to_owned()).set(entries as f64);
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

pub fn record_chunk_gone() {
    metrics::counter!("pigeonhole_chunk_gone_total").increment(1);
}

pub fn record_double_release() {
    metrics::counter!("pigeonhole_double_release_total").increment(1);
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

pub fn record_replica_selected(instance: &str) {
    metrics::counter!(
        "pigeonhole_replica_selected_total",
        "instance" => instance.to_owned()
    )
    .increment(1);
}

/// `pigeonhole_instance_inflight` up while it lives.
struct Inflight {
    instance: String,
    op: &'static str,
}

impl Inflight {
    fn new(instance: &str, op: &'static str) -> Self {
        inflight_inc(instance, op);
        Self { instance: instance.to_owned(), op }
    }
}

impl Drop for Inflight {
    fn drop(&mut self) {
        inflight_dec(&self.instance, self.op);
    }
}

fn outcome_from_err(err: &anyhow::Error) -> (&'static str, bool) {
    if crate::Failure::of(err) == crate::Failure::RateLimited {
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

/// One store call in the log: `debug` when it worked, `warn` when it failed
/// (a replica's failure inside a write that met its quorum shows only here).
fn log_call<T>(span: &tracing::Span, result: &Result<T>, elapsed: std::time::Duration) {
    let ms = elapsed.as_millis() as u64;
    span.in_scope(|| match result {
        Ok(_) => tracing::debug!(ms, "store call"),
        Err(e) => tracing::warn!(ms, error = %format!("{e:#}"), "store call failed"),
    });
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

    async fn put(&self, data: Bytes, name: &str) -> Result<BlobLocator> {
        let instance = self.inner.instance().id.clone();
        let op = "put";
        let len = data.len() as u64;
        inflight_inc(&instance, op);
        let t0 = Instant::now();
        let span = tracing::debug_span!("store", instance = %instance, op, bytes = len);
        let result = self.inner.put(data, name).instrument(span.clone()).await;
        log_call(&span, &result, t0.elapsed());
        if result.is_ok() {
            record_bytes_to_backend(&instance, len);
        }
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
        // In flight until the stream is read or dropped.
        let inflight = Inflight::new(&instance, op);
        let t0 = Instant::now();
        let span = tracing::debug_span!("store", instance = %instance, op);
        let result = self.inner.get(id, range).instrument(span.clone()).await;
        log_call(&span, &result, t0.elapsed());
        match result {
            Ok(stream) => {
                // Timed to the first byte: a store may answer before any
                // request (Telegram's stream starts lazily).
                let mut first = Some(t0);
                Ok(Box::pin(stream.map(move |item| {
                    let _inflight = &inflight;
                    if let Some(t0) = first.take() {
                        let outcome = item.as_ref().map_or_else(|e| outcome_from_err(e).0, |_| "ok");
                        record_instance_call(&instance, op, outcome, t0.elapsed());
                    }
                    if let Ok(ref b) = item {
                        record_bytes_from_backend(&instance, b.len() as u64);
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
                Err(e)
            }
        }
    }

    async fn delete(&self, keys: &[Vec<u8>]) -> Result<()> {
        let instance = self.inner.instance().id.clone();
        let op = "delete";
        inflight_inc(&instance, op);
        let t0 = Instant::now();
        let span = tracing::debug_span!("store", instance = %instance, op, keys = keys.len());
        let result = self.inner.delete(keys).instrument(span.clone()).await;
        log_call(&span, &result, t0.elapsed());
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

    async fn candidates(&self, after: Option<&[u8]>, upto: &[u8], limit: usize) -> Result<Vec<Vec<u8>>> {
        self.inner.candidates(after, upto, limit).await
    }
}
