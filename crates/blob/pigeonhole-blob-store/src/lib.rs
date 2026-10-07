//! Ingest, snapshot push/restore, and shared runtime config.

pub mod blob_db;
pub mod config;
pub mod frame_cache;
pub mod ingest;
pub mod instances;
pub mod read;
pub mod snapshot;

pub use blob_db::BlobDb;
pub use config::{BackendKind, BytestreamSettings, Config, HttpSettings};
pub use instances::{
    check_fingerprints, discord_fingerprint, discord_location, legacy_default_instance,
    memory_fingerprint, memory_location, telegram_fingerprint, telegram_location, validate_instances,
    InstanceConfig,
};
pub use frame_cache::FrameCache;
pub use pigeonhole_blob::CacheConfig;
pub use ingest::{
    codec_from_sql, codec_to_sql, decode_chunk, decode_chunk_async, decode_chunk_slice_async,
    encode_chunk, ingest_stream_to_store, ingest_stream_with_options, IngestError, IngestHasher,
    IngestOptions, IngestResult, UploadedChunk,
};
pub use read::read_chunk_range_cached;
pub use pigeonhole_blob::{BlobStore, BootstrapPointer, DeleteOutcome, PinnedContent};
pub use pigeonhole_codec::{ByteBudget, ChunkCodec, FrameRecord};
pub use pigeonhole_index::{Index, IndexSnapshot};
// Re-export index helpers gateways need without taking a direct index dep.
pub use pigeonhole_index::{parse_rfc3339, DeleteBucketResult, OrphanMsg, Chunk};
