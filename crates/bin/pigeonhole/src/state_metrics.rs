//! Gauges of what the tablet holds and of the process, set every few seconds.

use pigeonhole_chunk_store::BlobDb;
use pigeonhole_gateway_s3::Index;
use pigeonhole_meta::MetaStore;
use pigeonhole_types::MetricDoc;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const METRICS: &[MetricDoc] = &[
    MetricDoc::gauge("pigeonhole_build_info", &["version"], "Always 1; the binary's version in a label"),
    MetricDoc::gauge("pigeonhole_start_time_seconds", &[], "Process start, Unix time"),
    MetricDoc::gauge("pigeonhole_s3_buckets", &[], "Buckets"),
    MetricDoc::gauge("pigeonhole_s3_objects", &[], "Objects"),
    MetricDoc::gauge("pigeonhole_s3_object_bytes", &[], "Logical bytes of all objects"),
    MetricDoc::gauge("pigeonhole_s3_uploads", &[], "Multipart uploads in progress"),
    MetricDoc::gauge("pigeonhole_inline_objects", &[], "Objects stored inline in the metadata (docs/inline-objects.md)"),
    MetricDoc::gauge("pigeonhole_inline_bytes", &[], "Bytes of the objects stored inline in the metadata"),
    MetricDoc::counter("pigeonhole_cooled_total", &["reason"], "Inline objects the cooler moved out to chunks: age (not rewritten for cool_after) or budget"),
    MetricDoc::counter("pigeonhole_cool_rejected_total", &[], "Inline objects the cooler picked that changed before it moved them (left as they are)"),
    MetricDoc::gauge("pigeonhole_chunks", &["state"], "Chunks: live, unreferenced (the sweeper's next work), reclaiming"),
    MetricDoc::gauge("pigeonhole_chunk_logical_bytes", &[], "Logical bytes of all chunks (shared extents count once)"),
    MetricDoc::gauge("pigeonhole_chunk_stored_bytes", &["instance"], "Stored bytes of the chunk replicas an instance holds"),
];

/// How often the tablet gauges are refreshed (each refresh walks the state).
const EVERY: Duration = Duration::from_secs(5);

/// Set the process gauges once and the tablet ones every [`EVERY`].
pub fn spawn(meta: Arc<MetaStore>, index: Index, chunks: BlobDb) {
    metrics::gauge!("pigeonhole_build_info", "version" => env!("CARGO_PKG_VERSION")).set(1.0);
    let started = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    metrics::gauge!("pigeonhole_start_time_seconds").set(started.as_secs_f64());
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(EVERY);
        loop {
            tick.tick().await;
            meta.record_gauges();
            let s3 = index.stats();
            metrics::gauge!("pigeonhole_s3_buckets").set(s3.buckets as f64);
            metrics::gauge!("pigeonhole_s3_objects").set(s3.objects as f64);
            metrics::gauge!("pigeonhole_s3_object_bytes").set(s3.object_bytes as f64);
            metrics::gauge!("pigeonhole_s3_uploads").set(s3.uploads as f64);
            metrics::gauge!("pigeonhole_inline_objects").set(s3.inline_objects as f64);
            metrics::gauge!("pigeonhole_inline_bytes").set(s3.inline_bytes as f64);
            let c = chunks.stats();
            metrics::gauge!("pigeonhole_chunks", "state" => "live").set(c.live as f64);
            metrics::gauge!("pigeonhole_chunks", "state" => "unreferenced").set(c.unreferenced as f64);
            metrics::gauge!("pigeonhole_chunks", "state" => "reclaiming").set(c.reclaiming as f64);
            metrics::gauge!("pigeonhole_chunk_logical_bytes").set(c.logical_bytes as f64);
            for (instance, bytes) in c.stored_bytes {
                metrics::gauge!("pigeonhole_chunk_stored_bytes", "instance" => instance).set(bytes as f64);
            }
            if meta.is_stopped() {
                return;
            }
        }
    });
}
