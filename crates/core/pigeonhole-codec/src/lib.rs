//! Chunk codecs and independent block packing.

pub mod blocks;
pub mod chunker;
pub mod types;

pub use blocks::{
    decode_block_slice, decode_blocks_range, encode_block_bytes, BlockRecord, BlockWriter, BudgetPermit, ByteBudget,
    CompletedChunk,
};
pub use chunker::{
    fill_target, max_logical_bytes, validate_chunk_size, ChunkCodec, COMPRESS_PROBE_BYTES, COMPRESS_SIZE_MARGIN,
    DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_INGEST_MEMORY_BUDGET, DEFAULT_LOGICAL_CHUNK_SIZE, MAX_CHUNK_SIZE,
    MAX_LOGICAL_CHUNK,
};
pub use types::{codec_from_sql, codec_to_sql};
