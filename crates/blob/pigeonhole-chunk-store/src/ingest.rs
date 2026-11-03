//! Ingest options shared by [`crate::ChunkStore`].
//!
//! Compressing policies (`zstd` / `gzip`) pack data as independent fixed-size
//! blocks so each block is compressed once.

use pigeonhole_codec::{self as chunker, ByteBudget, ChunkCodec};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

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
    /// Most stored (encoded) bytes in a chunk; 256 MiB logical at most.
    pub chunk_size: usize,
    pub codec: ChunkCodec,
    pub block_size: usize,
    /// Bytes of open and uploading chunks; a chunk reserves its share when
    /// it opens and returns it once uploaded.
    pub memory_budget: Option<ByteBudget>,
    /// The body's length when known (S3 Content-Length, a CAS digest): a
    /// chunk reserves no more of the budget than the body still has.
    pub size_hint: Option<u64>,
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
            size_hint: None,
            compress_calls: None,
        }
    }
}
