//! Chunk store: metadata namespace, ingest, sweep, repair, and shared runtime config.

pub mod blob_db;
mod commands;
pub mod config;
pub mod block_cache;
pub mod ingest;
pub mod instances;
pub mod layer;
pub mod repair;
pub mod stores;
pub mod sweep;

pub use blob_db::{is_chunk_gone, BlobDb, ChunkGone, ReadPin, StoredBlock};
pub use commands::{ChunkCommand, ChunkReject, ChunkState, InstanceRec, PartRec, Replica};
pub use config::{
    BytestreamSettings, Config, HttpSettings, LogSettings, MetaSettings, MetricsSettings, SnapshotSettings,
    PlacementConfig,
};
pub use layer::{
    check_block_fits_members, default_layer_opts, ChunkId, ChunkStore, Extent, Ingested,
};
pub use instances::{
    check_fingerprints, discord_fingerprint, discord_location, memory_instance,
    memory_fingerprint, memory_location, telegram_fingerprint, telegram_location, validate_instances,
    InstanceConfig,
};
pub use block_cache::BlockCache;
pub use pigeonhole_blob::CacheConfig;
pub use ingest::{codec_from_sql, codec_to_sql, IngestHasher, IngestOptions};
pub use repair::{
    is_part_not_found, spawn_enqueue_repair, RepairConfig, RepairStats, Repairer,
    DEFAULT_MAX_COST_WAIT_SECS, DEFAULT_REPAIR_INTERVAL, REPAIR_BATCH_SIZE,
};
pub use sweep::{
    SweepConfig, SweepStats, Sweeper, DEFAULT_SWEEP_INTERVAL, SWEEP_BATCH_SIZE,
};
pub use pigeonhole_blob::{
    collect_stream, BootstrapPointer, BoxByteStream, DeleteOutcome, PinnedContent, UploadPin,
    UploadPins,
};
pub use pigeonhole_codec::{ByteBudget, ChunkCodec, BlockRecord};
