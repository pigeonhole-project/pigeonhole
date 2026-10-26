//! Ingest options shared by [`crate::ChunkStore`].
//!
//! Compressing policies (`zstd` / `gzip`) pack data as independent fixed-size
//! blocks ([`pigeonhole_codec::BlockWriter`]) so each block is compressed once.

use pigeonhole_codec::{self as chunker, ByteBudget, ChunkCodec};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub use pigeonhole_codec::{codec_from_sql, codec_to_sql};

/// Optional streaming hasher updated with plaintext body bytes during ingest.
///
/// Gateways (e.g. S3 CRC/SHA) can implement this; the layer stays free of protocol crates.
pub trait IngestHasher: Send {
    fn update(&mut self, data: &[u8]);
}

impl IngestHasher for () {
    fn update(&mut self, _data: &[u8]) {}
}

#[derive(Clone)]
pub struct IngestOptions {
    pub chunk_size: usize,
    pub codec: ChunkCodec,
    pub block_size: usize,
    pub memory_budget: Option<ByteBudget>,
    /// Optional shared counter for tests.
    pub compress_calls: Option<Arc<AtomicU64>>,
}

impl IngestOptions {
    pub fn new(chunk_size: usize, codec: ChunkCodec) -> Self {
        Self {
            chunk_size,
            codec,
            block_size: chunker::DEFAULT_BLOCK_SIZE,
            memory_budget: None,
            compress_calls: None,
        }
    }
}
