//! Pigeonhole binary facade: re-exports used by tests and scripts.

pub mod http_timeout;

pub use pigeonhole_blob as storage;
pub use pigeonhole_codec as chunker;
pub use pigeonhole_codec::blocks;
pub use pigeonhole_chunk_store::config;
pub use pigeonhole_chunk_store::ingest;
pub use pigeonhole_chunk_store::snapshot;
pub use pigeonhole_index as index;
pub use pigeonhole_gateway_s3 as service;
pub use pigeonhole_gateway_s3::{build_s3_service, build_s3gram};
pub use pigeonhole_storage_telegram as telegram;
pub use pigeonhole_storage_memory as memory;

pub use pigeonhole_blob::{LegacyBlobStore, ChatLimiter, DeleteOutcome};
pub use pigeonhole_storage_memory::{MemoryBackend, MemoryBlobStore};
pub use pigeonhole_types::{BackendId, BackendLimits, BlobKey, Locator, PutHint, RangeSupport};
pub use pigeonhole_chunk_store::{BackendKind, Config};
#[cfg(feature = "discord")]
pub use pigeonhole_storage_discord as discord;
pub use pigeonhole_index::Index;
pub use pigeonhole_gateway_s3::S3gram;
pub use pigeonhole_storage_telegram::{PinnedContent, TelegramBlobStore, TelegramClient};

/// Rate-limit module path used by older imports.
pub mod rate_limit {
    pub use pigeonhole_blob::{ChatLimiter, ChatLimiterConfig};
}
