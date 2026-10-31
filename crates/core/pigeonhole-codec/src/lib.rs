//! Chunk codecs and independent block packing.

pub mod blocks;
pub mod chunker;
pub mod types;

pub use blocks::{decode_block_slice, decode_blocks_range, encode_block_bytes, BlockRecord, BudgetPermit, ByteBudget};
pub use chunker::{
    validate_chunk_size, ChunkCodec, COMPRESS_SIZE_MARGIN, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
    DEFAULT_INGEST_MEMORY_BUDGET, MAX_LOGICAL_CHUNK,
};
pub use types::{codec_from_sql, codec_to_sql};

/// Runs CPU-bound `f` (compression) off the async workers: on a blocking-pool
/// thread, or, on wasm, which has no threads, right here.
pub async fn blocking<F, R>(f: F) -> anyhow::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    #[cfg(not(target_family = "wasm"))]
    {
        Ok(tokio::task::spawn_blocking(f).await?)
    }
    #[cfg(target_family = "wasm")]
    {
        Ok(f())
    }
}
