//! Chunk codecs and independent block packing.

pub mod blocks;
pub mod chunker;

pub use blocks::{decode_block_slice, encode_block_bytes, BudgetPermit, ByteBudget};
pub use chunker::{
    validate_chunk_size, ChunkCodec, BLOCK_HEADROOM, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
    DEFAULT_INGEST_MEMORY_BUDGET, MAX_LOGICAL_CHUNK,
};

/// Runs CPU-bound `f` (compression) off the async workers; see [`pigeonhole_types::blocking`].
pub use pigeonhole_types::blocking;
