//! Chunk codecs and independent block packing.

pub mod chunker;
pub mod blocks;
pub mod types;

pub use chunker::{
    fill_target, max_logical_bytes, validate_chunk_size, ChunkCodec, COMPRESS_PROBE_BYTES,
    COMPRESS_SIZE_MARGIN, DEFAULT_CHUNK_SIZE, DEFAULT_BLOCK_SIZE, DEFAULT_INGEST_MEMORY_BUDGET,
    MAX_CHUNK_SIZE, MAX_LOGICAL_CHUNK,
};
pub use blocks::{
    decode_blocks_range, BudgetPermit, ByteBudget, CompletedChunk, BlockRecord, BlockWriter,
};
pub use types::{codec_from_sql, codec_to_sql, UploadedChunk};
