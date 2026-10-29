//! Pigeonhole binary facade: re-exports used by tests and scripts.

pub mod gateway_metrics;
pub mod http_timeout;
pub mod logging;
pub mod request_log;
pub mod state_metrics;
#[cfg(feature = "metrics-prometheus")]
pub mod prometheus;

pub use pigeonhole_blob as storage;
pub use pigeonhole_codec as chunker;
pub use pigeonhole_codec::blocks;
pub use pigeonhole_chunk_store::config;
pub use pigeonhole_gateway_s3 as service;
pub use pigeonhole_gateway_s3::index;
pub use pigeonhole_gateway_s3::{build_s3_service, build_s3_gateway, S3OpSlot};
pub use pigeonhole_storage_telegram as telegram;
pub use pigeonhole_storage_memory as memory;

pub use pigeonhole_blob::DeleteOutcome;
pub use pigeonhole_limits::ChatLimiter;
pub use pigeonhole_storage_memory::{MemoryBackend, MemoryBlobStore};
pub use pigeonhole_types::{BackendId, BackendLimits, BlobKey, RangeSupport};
pub use pigeonhole_chunk_store::{BlobDb, ChunkStore, Config, IngestOptions};
#[cfg(feature = "discord")]
pub use pigeonhole_storage_discord as discord;
pub use pigeonhole_gateway_s3::{Index, S3Gateway};

/// Every metric the binary records, crate by crate (docs/metrics.md).
pub fn metric_docs() -> [&'static [pigeonhole_types::MetricDoc]; 4] {
    [pigeonhole_blob::METRICS, pigeonhole_limits::METRICS, pigeonhole_meta::METRICS, state_metrics::METRICS]
}
