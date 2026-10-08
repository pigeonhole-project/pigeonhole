//! Pluggable blob storage and bootstrap pin abstraction.

mod backend;
mod bootstrap;
pub mod cache;
pub mod erase;
pub mod inflight;
pub mod metrics;
pub mod part_packer;
pub mod rate_limit;
pub mod replicated;
pub mod typed;
pub use backend::{bytes_stream, collect_stream, slice_range, BoxByteStream};
pub use bootstrap::{BootstrapPointer, PinnedContent};
pub use cache::CacheConfig;
pub use erase::{erase, erase_sweep, DynBlobBackend, DynSweep, Erased, ErasedSweep, SharedBackend};
pub use inflight::{InflightGuard, InflightParts};
pub use metrics::{
    describe_metrics, inflight_dec, inflight_inc, record_429, record_bytes_from_backend,
    record_bytes_to_clients, record_cache, record_chunk_gone, record_compression_ratio,
    record_double_release, record_gateway_request, record_ingest_budget_wait, record_instance_call,
    record_limiter_wait, record_parts_per_chunk, record_repair, record_replica_selected,
    record_sweep, set_checkpoint_age, set_repair_queue_depth, set_superblock_age, MetricsBackend,
};
pub use part_packer::{EncodedBlock, PartPacker, PartUploaded};
pub use rate_limit::{ChatLimiter, ChatLimiterConfig, LimitBudget};
pub use replicated::{
    CheapestFirst, ChunkReplicaWriter, InstanceId, PartLayout, ReplicaLayout, ReplicaSelector,
    Replicated, SealedChunk,
};
pub use typed::{
    load_id, store_id, CostHint, InstanceInfo, InstanceKind, InstanceRole, OpKind, OrderedKey,
    BlobLocator, Sweepable, BlobBackend, TypedBootstrapPointer,
};
pub use pigeonhole_types::{
    BackendId, BackendLimits, BlobKey, ByteRange, DeleteOutcome, RangeSupport,
};
