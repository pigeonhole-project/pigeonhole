//! Pluggable blob storage.

mod backend;
pub mod cache;
pub mod erase;
pub mod metrics;
pub mod part_packer;
pub mod replicated;
pub mod typed;
pub mod upload_pins;
pub use backend::{bytes_stream, collect_stream, slice_range, BoxByteStream};
pub use cache::CacheConfig;
pub use erase::{erase, DynBlobBackend, Erased, SharedBackend};
pub use metrics::{
    describe, inflight_dec, inflight_inc, record_429, record_background_pass, record_bytes_from_backend,
    record_bytes_from_clients, record_bytes_to_backend, record_bytes_to_clients, record_cache, record_chunk_gone,
    record_compression_ratio, record_double_release, record_gateway_request, record_ingest_budget_wait,
    record_instance_call, record_parts_per_chunk, record_repair, record_replica_selected, record_sweep, set_cache_size,
    set_repair_queue_depth, MetricsBackend, METRICS,
};
pub use part_packer::{EncodedBlock, PartPacker, PartUploaded};
pub use pigeonhole_storage_api::{
    text, After, Chain, Failure, LeaseChain, Linked, MakeAfter, NotFound, RateLimited, Unavailable,
};
pub use pigeonhole_types::{BackendId, BackendLimits, BlobKey, ByteRange, DeleteOutcome, RangeSupport};
pub use replicated::{
    CheapestFirst, ChunkReplicaWriter, InstanceId, NotFoundHook, PartLayout, ReplicaLayout, ReplicaSelector,
    Replicated, SealedChunk, ShardSpec,
};
pub use typed::{
    load_id, store_id, BlobBackend, BlobId, BlobLocator, CostHint, InstanceInfo, InstanceRole, OpKind, OrderedKey,
};
pub use upload_pins::{UploadPin, UploadPins};
