//! Chunk codecs and independent frame packing.

pub mod chunker;
pub mod frames;
pub mod types;

pub use chunker::{
    fill_target, max_logical_bytes, validate_chunk_size, ChunkCodec, COMPRESS_PROBE_BYTES,
    COMPRESS_SIZE_MARGIN, DEFAULT_CHUNK_SIZE, DEFAULT_FRAME_SIZE, DEFAULT_INGEST_MEMORY_BUDGET,
    MAX_CHUNK_SIZE, MAX_LOGICAL_CHUNK,
};
pub use frames::{
    decode_frames_range, BudgetPermit, ByteBudget, CompletedChunk, FrameRecord, FrameWriter,
};
pub use types::{codec_from_sql, codec_to_sql, UploadedChunk};
