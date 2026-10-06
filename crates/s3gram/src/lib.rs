//! S3-compatible gateway backed by Telegram (`BlobStore`) and a SQLite index.
//!
//! Compatibility re-exports keep existing `s3gram::…` paths working for tests
//! and scripts after the workspace split.

pub use s3gram_blob as storage;
pub use s3gram_chunk as chunker;
pub use s3gram_chunk::frames;
pub use s3gram_engine::config;
pub use s3gram_engine::ingest;
pub use s3gram_engine::snapshot;
pub use s3gram_index as index;
pub use s3gram_s3 as service;
pub use s3gram_s3::{build_s3_service, build_s3gram};
pub use s3gram_telegram as telegram;

pub use s3gram_blob::{
    BlobBackend, BlobStore, ChatLimiter, DeleteOutcome, MemoryBackend, MemoryBlobStore,
};
pub use s3gram_core::{BackendId, BackendLimits, BlobKey, Locator, PutHint, RangeSupport};
pub use s3gram_engine::{BackendKind, Config};
#[cfg(feature = "discord")]
pub use s3gram_discord as discord;
pub use s3gram_index::Index;
pub use s3gram_s3::S3gram;
pub use s3gram_telegram::{PinnedContent, TelegramBlobStore, TelegramClient};

/// Rate-limit module path used by older imports.
pub mod rate_limit {
    pub use s3gram_blob::{ChatLimiter, ChatLimiterConfig};
}
