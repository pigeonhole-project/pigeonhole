//! Chunk store: metadata namespace, ingest, sweep, repair.

pub mod blob_db;
pub mod block_cache;
mod commands;
pub mod ingest;
pub mod layer;
pub mod read;
pub mod repair;
pub mod sweep;

pub use blob_db::{
    is_chunk_gone, unique_chunk_ids, BlobDb, ChunkGone, ChunkMeta, ChunkStats, ReadPin, StoredBlock, Unrepairable,
    REPAIR_BACKOFF, REPAIR_BACKOFF_MAX, REPAIR_MAX_ATTEMPTS,
};
pub use block_cache::BlockCache;
pub use commands::{ChunkCommand, ChunkReject, ChunkState, InstanceRec, PartRec, Replica};
pub use ingest::{IngestHasher, IngestOptions};
pub use layer::{
    check_block_fits_members, default_layer_opts, to_the_end, ChunkId, ChunkStore, Committing, Extent, Ingested,
    PendingChunks,
};
pub use pigeonhole_blob::CacheConfig;
pub use pigeonhole_blob::{collect_stream, BoxByteStream, UploadPin, UploadPins};
pub use pigeonhole_codec::{ByteBudget, ChunkCodec};
pub use read::{ReadStream, READ_WINDOW};
pub use repair::{
    spawn_enqueue_repair, RepairConfig, RepairStats, Repairer, DEFAULT_MAX_COST_WAIT_SECS, DEFAULT_REPAIR_INTERVAL,
    REPAIR_BATCH_SIZE, REPAIR_CONCURRENCY,
};
pub use sweep::{SweepConfig, SweepStats, Sweeper, DEFAULT_SWEEP_INTERVAL, SWEEP_BATCH_SIZE};
