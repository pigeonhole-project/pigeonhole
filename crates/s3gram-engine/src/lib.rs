//! Ingest, snapshot push/restore, and shared runtime config.

pub mod config;
pub mod frame_cache;
pub mod ingest;
pub mod read;
pub mod snapshot;

pub use config::{BackendKind, BytestreamSettings, Config, HttpSettings};
pub use frame_cache::FrameCache;
pub use s3gram_blob::CacheConfig;
pub use ingest::{
    codec_from_sql, codec_to_sql, decode_chunk, decode_chunk_async, decode_chunk_slice_async,
    encode_chunk, ingest_stream_to_store, ingest_stream_with_options, IngestError, IngestOptions,
    IngestResult, UploadedChunk,
};
pub use read::read_chunk_range_cached;
pub use s3gram_blob::{BlobStore, BootstrapPointer, DeleteOutcome, MemoryBlobStore, PinnedContent};
pub use s3gram_chunk::{ByteBudget, ChunkCodec, FrameRecord};
pub use s3gram_index::{Index, IndexSnapshot};
