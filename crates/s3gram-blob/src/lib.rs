//! Pluggable blob storage and bootstrap pin abstraction.

mod backend;
mod bootstrap;
pub mod cache;
pub mod rate_limit;
mod store;

pub mod testkit;

pub use backend::{
    bytes_stream, collect_stream, slice_range, store_delete_message, store_get, store_put,
    BlobBackend, BlobStore, BoxByteStream,
};
pub use bootstrap::BootstrapPointer;
pub use cache::{CacheConfig, CacheMetrics, CachedBlob, CachingBackend};
pub use rate_limit::{ChatLimiter, ChatLimiterConfig};
pub use s3gram_core::{
    BackendId, BackendLimits, BlobKey, ByteRange, DeleteOutcome, Locator, PinnedContent, PutHint,
    RangeSupport,
};
pub use store::{MemoryBackend, MemoryBlobStore};
