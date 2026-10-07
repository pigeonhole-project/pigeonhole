//! Pluggable blob storage and bootstrap pin abstraction.

mod backend;
mod bootstrap;
pub mod cache;
pub mod erase;
pub mod metrics;
pub mod rate_limit;
pub mod typed;
pub use backend::{
    bytes_stream, collect_stream, locator_for_store_file_id, slice_range, store_delete_message,
    store_get, store_put, LegacyBlobStore, BoxByteStream,
};
pub use bootstrap::BootstrapPointer;
pub use cache::{CacheConfig, CacheMetrics, CachedBlob, CachingBackend};
pub use erase::{erase, erase_sweep, DynBlobBackend, DynSweep, Erased, ErasedSweep, SharedBackend};
pub use metrics::{spawn_metrics_logger, BackendMetrics};
pub use rate_limit::{ChatLimiter, ChatLimiterConfig, LimitBudget};
pub use typed::{
    load_id, store_id, CostHint, InstanceInfo, InstanceKind, InstanceRole, OpKind, OrderedKey,
    BlobLocator, Sweepable, BlobBackend, TypedBootstrapPointer,
};
pub use pigeonhole_types::{
    BackendId, BackendLimits, BlobKey, ByteRange, DeleteOutcome, Locator, PinnedContent, PutHint,
    RangeSupport,
};
