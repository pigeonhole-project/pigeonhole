//! Chunk store: metadata namespace, ingest, sweep, repair.

pub mod blob_db;
mod commands;
pub mod block_cache;
pub mod ingest;
pub mod layer;
pub mod repair;
pub mod sweep;

pub use blob_db::{is_chunk_gone, BlobDb, ChunkGone, ChunkStats, ReadPin, StoredBlock};
pub use commands::{ChunkCommand, ChunkReject, ChunkState, InstanceRec, PartRec, Replica};
pub use layer::{
    check_block_fits_members, default_layer_opts, ChunkId, ChunkStore, Committing, Extent, Ingested, PendingChunks,
};
pub use block_cache::BlockCache;
pub use pigeonhole_blob::CacheConfig;
pub use ingest::{codec_from_sql, codec_to_sql, IngestHasher, IngestOptions};
pub use repair::{
    spawn_enqueue_repair, RepairConfig, RepairStats, Repairer,
    DEFAULT_MAX_COST_WAIT_SECS, DEFAULT_REPAIR_INTERVAL, REPAIR_BATCH_SIZE,
};
pub use sweep::{
    SweepConfig, SweepStats, Sweeper, DEFAULT_SWEEP_INTERVAL, SWEEP_BATCH_SIZE,
};
pub use pigeonhole_blob::{
    collect_stream, BoxByteStream, DeleteOutcome, UploadPin,
    UploadPins,
};
pub use pigeonhole_codec::{ByteBudget, ChunkCodec, BlockRecord};
