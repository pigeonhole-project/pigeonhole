//! Pigeonhole binary facade: the HTTP layers the binary serves with, and the
//! re-exports its tests use.

pub mod admin_http;
pub mod gateway_metrics;
pub mod http_timeout;
pub mod leader_http;
pub mod logging;
#[cfg(feature = "metrics-prometheus")]
pub mod prometheus;
pub mod request_log;

pub use pigeonhole_config as config;
pub use pigeonhole_config::instances;
pub use pigeonhole_gateway_s3 as service;
pub use pigeonhole_gateway_s3::index;
pub use pigeonhole_gateway_s3::{build_s3_gateway, build_s3_service, S3Gateway, S3OpSlot};
pub use pigeonhole_storage_memory as memory;
/// The Telegram store and the MTProto client it runs on.
pub mod telegram {
    pub use pigeonhole_storage_telegram::*;
    pub use pigeonhole_telegram_mtproto::*;
}

pub use pigeonhole_chunk_store::{BlobDb, ChunkStore, IngestOptions};

/// Every metric the binary records, crate by crate (docs/metrics.md).
pub fn metric_docs() -> [&'static [pigeonhole_types::MetricDoc]; 5] {
    [
        pigeonhole_blob::METRICS,
        pigeonhole_limits::METRICS,
        pigeonhole_meta::METRICS,
        &CHAT_METRICS,
        pigeonhole_node::state_metrics::METRICS,
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
