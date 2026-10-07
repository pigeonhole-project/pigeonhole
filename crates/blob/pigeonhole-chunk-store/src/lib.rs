//! Ingest, snapshot push/restore, and shared runtime config.

pub mod blob_db;
pub mod config;
pub mod durability;
pub mod block_cache;
pub mod ingest;
pub mod instances;
pub mod layer;
pub mod migrate;
pub mod read;

pub use blob_db::{BlobDb, StoredBlock};
pub use config::{BackendKind, BytestreamSettings, Config, HttpSettings, PlacementConfig};
pub use durability::{
    commit_root, start_or_restore, CheckpointPayload, Durability, JournalOp, PinTarget,
    Superblock,
};
pub use layer::{
    check_block_fits_members, default_layer_opts, ChunkId, ChunkStore, Extent, Ingested,
};
pub use migrate::{
    default_instance_for_migrate, legacy_index_has_blobs, migrate_index_to_blob_db, MigrateReport,
};
pub use instances::{
    check_fingerprints, discord_fingerprint, discord_location, legacy_default_instance,
    memory_fingerprint, memory_location, telegram_fingerprint, telegram_location, validate_instances,
    InstanceConfig,
};
pub use block_cache::BlockCache;
pub use pigeonhole_blob::CacheConfig;
pub use ingest::{
    codec_from_sql, codec_to_sql, decode_chunk, decode_chunk_async, decode_chunk_slice_async,
    encode_chunk, ingest_stream_to_store, ingest_stream_with_options, IngestError, IngestHasher,
    IngestOptions, IngestResult, UploadedChunk,
};
pub use read::read_chunk_range_cached;
pub use pigeonhole_blob::{
    collect_stream, store_delete_message, store_get, store_put, BootstrapPointer, BoxByteStream,
    DeleteOutcome, PinnedContent,
};
pub use pigeonhole_codec::{ByteBudget, ChunkCodec, BlockRecord};
