//! Pigeonhole binary facade: re-exports used by tests and scripts.

pub mod gateway_metrics;
pub mod http_timeout;
pub mod logging;
#[cfg(feature = "metrics-prometheus")]
pub mod prometheus;
pub mod request_log;
pub mod state_metrics;

pub use pigeonhole_blob as storage;
pub use pigeonhole_codec as chunker;
pub use pigeonhole_codec::blocks;
pub mod config;
pub mod instances;
mod stores;
pub use pigeonhole_gateway_s3 as service;
pub use pigeonhole_gateway_s3::index;
pub use pigeonhole_gateway_s3::{build_s3_gateway, build_s3_service, S3OpSlot};
pub use pigeonhole_storage_memory as memory;
pub use pigeonhole_storage_telegram as telegram;

pub use config::Config;
pub use pigeonhole_blob::DeleteOutcome;
pub use pigeonhole_chunk_store::{BlobDb, ChunkStore, IngestOptions};
pub use pigeonhole_gateway_s3::{Index, S3Gateway};
pub use pigeonhole_limits::ChatLimiter;
#[cfg(feature = "discord")]
pub use pigeonhole_storage_discord as discord;
pub use pigeonhole_storage_memory::{MemoryBackend, MemoryBlobStore};
pub use pigeonhole_types::{BackendId, BackendLimits, BlobKey, RangeSupport};

/// Every metric the binary records, crate by crate (docs/metrics.md).
pub fn metric_docs() -> [&'static [pigeonhole_types::MetricDoc]; 5] {
    [
        pigeonhole_blob::METRICS,
        pigeonhole_limits::METRICS,
        pigeonhole_meta::METRICS,
        &CHAT_METRICS,
        state_metrics::METRICS,
    ]
}

/// openraft-chat's metrics (a standalone crate with its own list), as docs.
const CHAT_METRICS: [pigeonhole_types::MetricDoc; openraft_chat::telemetry::METRICS.len()] = {
    use openraft_chat::telemetry::{MetricKind as K, METRICS};
    use pigeonhole_types::MetricDoc;
    let mut out = [MetricDoc::gauge("", &[], ""); METRICS.len()];
    let mut i = 0;
    while i < METRICS.len() {
        let m = METRICS[i];
        out[i] = match m.kind {
            K::Counter => MetricDoc::counter(m.name, m.labels, m.help),
            K::Gauge => MetricDoc::gauge(m.name, m.labels, m.help),
            K::Histogram => MetricDoc::histogram(m.name, m.labels, m.help),
        };
        i += 1;
    }
    out
};
