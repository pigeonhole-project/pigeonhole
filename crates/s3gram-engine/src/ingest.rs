//! Stream an S3 request body into Telegram-sized BlobStore chunks.
//!
//! With compressing policies (`zstd` / `gzip`), data is packed as independent
//! fixed-size frames ([`s3gram_chunk::FrameWriter`]) so each block is compressed
//! once. Chunk codec stored in the index is [`ChunkCodec::Frames`]. Legacy
//! single-blob `raw` / `gzip` / `zstd` chunks remain readable.

use s3gram_blob::{BlobStore, DeleteOutcome};
use s3gram_chunk::{self as chunker, ChunkCodec};
use s3gram_chunk::{self as frames, ByteBudget, CompletedChunk, FrameRecord, FrameWriter};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use md5::{Digest, Md5};
use s3s::checksum::ChecksumHasher;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use s3gram_chunk::{codec_from_sql, codec_to_sql, UploadedChunk};

#[derive(Debug)]
pub struct IngestResult {
    pub etag: String,
    pub size: i64,
    pub chunks: Vec<UploadedChunk>,
    pub md5: [u8; 16],
    pub crc32: u32,
    /// Test hook: compressor invocations (frame packing only).
    pub compress_calls: u64,
}

#[derive(Debug)]
pub struct IngestError {
    pub source: anyhow::Error,
    pub pending_deletes: Vec<i64>,
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl std::error::Error for IngestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

#[derive(Clone)]
pub struct IngestOptions {
    pub chunk_size: usize,
    pub codec: ChunkCodec,
    pub frame_size: usize,
    pub memory_budget: Option<ByteBudget>,
    /// Optional shared counter for tests.
    pub compress_calls: Option<Arc<AtomicU64>>,
}

impl IngestOptions {
    pub fn new(chunk_size: usize, codec: ChunkCodec) -> Self {
        Self {
            chunk_size,
            codec,
            frame_size: chunker::DEFAULT_FRAME_SIZE,
            memory_budget: None,
            compress_calls: None,
        }
    }
}

/// Upload body bytes into documents of at most `chunk_size` on-wire bytes.
pub async fn ingest_stream_to_store(
    store: &Arc<dyn BlobStore>,
    stream: impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    checksum: Option<&mut ChecksumHasher>,
    chunk_size: usize,
    codec: ChunkCodec,
) -> Result<IngestResult, IngestError> {
    ingest_stream_with_options(
        store,
        stream,
        checksum,
        IngestOptions::new(chunk_size, codec),
    )
    .await
}

pub async fn ingest_stream_with_options(
    store: &Arc<dyn BlobStore>,
    mut stream: impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    mut checksum: Option<&mut ChecksumHasher>,
    opts: IngestOptions,
) -> Result<IngestResult, IngestError> {
    if opts.codec == ChunkCodec::Raw {
        return ingest_raw(store, &mut stream, checksum.as_deref_mut(), opts.chunk_size).await;
    }
    ingest_framed(store, &mut stream, checksum.as_deref_mut(), opts).await
}

async fn ingest_framed(
    store: &Arc<dyn BlobStore>,
    stream: &mut (impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin),
    mut checksum: Option<&mut ChecksumHasher>,
    opts: IngestOptions,
) -> Result<IngestResult, IngestError> {
    let max_stored = opts.chunk_size.clamp(1, chunker::MAX_CHUNK_SIZE);
    let max_logical = chunker::MAX_LOGICAL_CHUNK;
    let calls = opts
        .compress_calls
        .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
    let mut writer = FrameWriter::new(
        opts.frame_size,
        max_stored,
        max_logical,
        opts.codec,
        opts.memory_budget,
        calls.clone(),
    );

    let mut md5 = Md5::new();
    let mut crc = crc32fast::Hasher::new();
    let mut part_no: i64 = 0;
    let mut uploaded: Vec<UploadedChunk> = Vec::new();
    let mut total_size: i64 = 0;

    while let Some(item) = stream.next().await {
        let chunk = match item {
            Ok(c) => c,
            Err(e) => {
                let pending = cleanup_uploads(store, &uploaded).await;
                return Err(IngestError {
                    source: e,
                    pending_deletes: pending,
                });
            }
        };
        if chunk.is_empty() {
            continue;
        }
        md5.update(&chunk);
        crc.update(&chunk);
        if let Some(hasher) = checksum.as_mut() {
            hasher.update(&chunk);
        }
        total_size += chunk.len() as i64;

        let completed = match writer.push(&chunk).await {
            Ok(c) => c,
            Err(e) => {
                let pending = cleanup_uploads(store, &uploaded).await;
                return Err(IngestError {
                    source: e,
                    pending_deletes: pending,
                });
            }
        };
        for done in completed {
            match put_completed(store, done, part_no).await {
                Ok(u) => {
                    uploaded.push(u);
                    part_no += 1;
                }
                Err(e) => {
                    let pending = cleanup_uploads(store, &uploaded).await;
                    return Err(IngestError {
                        source: e,
                        pending_deletes: pending,
                    });
                }
            }
        }
    }

    let finished = match writer.finish().await {
        Ok(c) => c,
        Err(e) => {
            let pending = cleanup_uploads(store, &uploaded).await;
            return Err(IngestError {
                source: e,
                pending_deletes: pending,
            });
        }
    };
    for done in finished {
        match put_completed(store, done, part_no).await {
            Ok(u) => {
                uploaded.push(u);
                part_no += 1;
            }
            Err(e) => {
                let pending = cleanup_uploads(store, &uploaded).await;
                return Err(IngestError {
                    source: e,
                    pending_deletes: pending,
                });
            }
        }
    }

    let digest = md5.finalize();
    let mut md5_bytes = [0u8; 16];
    md5_bytes.copy_from_slice(&digest);
    Ok(IngestResult {
        etag: format!("{:x}", digest),
        size: total_size,
        chunks: uploaded,
        md5: md5_bytes,
        crc32: crc.finalize(),
        compress_calls: calls.load(Ordering::Relaxed),
    })
}

async fn ingest_raw(
    store: &Arc<dyn BlobStore>,
    stream: &mut (impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin),
    mut checksum: Option<&mut ChecksumHasher>,
    chunk_size: usize,
) -> Result<IngestResult, IngestError> {
    let max_stored = chunk_size.clamp(1, chunker::MAX_CHUNK_SIZE);
    let mut md5 = Md5::new();
    let mut crc = crc32fast::Hasher::new();
    let mut part_no: i64 = 0;
    let mut uploaded: Vec<UploadedChunk> = Vec::new();
    let mut total_size: i64 = 0;
    let mut buf = Vec::with_capacity(max_stored.min(64 * 1024));

    while let Some(item) = stream.next().await {
        let chunk = match item {
            Ok(c) => c,
            Err(e) => {
                let pending = cleanup_uploads(store, &uploaded).await;
                return Err(IngestError {
                    source: e,
                    pending_deletes: pending,
                });
            }
        };
        if chunk.is_empty() {
            continue;
        }
        md5.update(&chunk);
        crc.update(&chunk);
        if let Some(hasher) = checksum.as_mut() {
            hasher.update(&chunk);
        }
        total_size += chunk.len() as i64;
        buf.extend_from_slice(&chunk);
        while buf.len() >= max_stored {
            let piece: Vec<u8> = buf.drain(..max_stored).collect();
            match put_raw_piece(store, piece, part_no).await {
                Ok(u) => {
                    uploaded.push(u);
                    part_no += 1;
                }
                Err(e) => {
                    let pending = cleanup_uploads(store, &uploaded).await;
                    return Err(IngestError {
                        source: e,
                        pending_deletes: pending,
                    });
                }
            }
        }
    }
    if !buf.is_empty() {
        match put_raw_piece(store, buf, part_no).await {
            Ok(u) => uploaded.push(u),
            Err(e) => {
                let pending = cleanup_uploads(store, &uploaded).await;
                return Err(IngestError {
                    source: e,
                    pending_deletes: pending,
                });
            }
        }
    }

    let digest = md5.finalize();
    let mut md5_bytes = [0u8; 16];
    md5_bytes.copy_from_slice(&digest);
    Ok(IngestResult {
        etag: format!("{:x}", digest),
        size: total_size,
        chunks: uploaded,
        md5: md5_bytes,
        crc32: crc.finalize(),
        compress_calls: 0,
    })
}

async fn put_completed(
    store: &Arc<dyn BlobStore>,
    done: CompletedChunk,
    part_no: i64,
) -> Result<UploadedChunk> {
    if done.payload.is_empty() {
        bail!("refusing empty framed chunk");
    }
    let stored_crc32 = Some(crc32fast::hash(done.payload.as_ref()));
    let filename = format!("{:x}.bin.frames", Md5::digest(&done.payload));
    let (file_id, message_id) = store
        .put(done.payload, &filename, "")
        .await
        .context("blob store put")?;
    Ok(UploadedChunk {
        part_no,
        file_id,
        message_id,
        logical_size: done.logical_size,
        codec: ChunkCodec::Frames,
        frames: done.frames,
        stored_crc32,
    })
}

async fn put_raw_piece(
    store: &Arc<dyn BlobStore>,
    piece: Vec<u8>,
    part_no: i64,
) -> Result<UploadedChunk> {
    let logical_size = piece.len() as i64;
    let stored_crc32 = Some(crc32fast::hash(&piece));
    let filename = format!("{:x}.bin", Md5::digest(&piece));
    let (file_id, message_id) = store
        .put(Bytes::from(piece), &filename, "")
        .await
        .context("blob store put")?;
    Ok(UploadedChunk {
        part_no,
        file_id,
        message_id,
        logical_size,
        codec: ChunkCodec::Raw,
        frames: Vec::new(),
        stored_crc32,
    })
}

async fn cleanup_uploads(store: &Arc<dyn BlobStore>, uploaded: &[UploadedChunk]) -> Vec<i64> {
    let mut pending = Vec::new();
    for u in uploaded {
        match store.delete_message(u.message_id).await {
            Ok(DeleteOutcome::Deleted | DeleteOutcome::Gone) => {}
            Ok(DeleteOutcome::Failed) | Err(_) => pending.push(u.message_id),
        }
    }
    pending
}

fn gunzip_capped(data: &[u8], max_out: usize) -> Result<Vec<u8>> {
    let mut dec = GzDecoder::new(data);
    let mut out = Vec::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = dec.read(&mut buf).context("gunzip")?;
        if n == 0 {
            break;
        }
        if out.len().saturating_add(n) > max_out {
            bail!("gzip output exceeds max_logical {max_out}");
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

/// Decode a legacy single-blob chunk (`raw` / `gzip` / `zstd`).
pub fn decode_chunk(stored: Bytes, codec: ChunkCodec, max_logical: usize) -> Result<Bytes> {
    let max_logical = max_logical.max(1);
    match codec {
        ChunkCodec::Raw => {
            if stored.len() > max_logical {
                bail!(
                    "raw chunk {} exceeds max_logical {max_logical}",
                    stored.len()
                );
            }
            Ok(stored)
        }
        ChunkCodec::Gzip => Ok(Bytes::from(gunzip_capped(stored.as_ref(), max_logical)?)),
        ChunkCodec::Zstd => {
            let out = zstd::bulk::decompress(stored.as_ref(), max_logical)
                .context("zstd decompress")?;
            if out.len() > max_logical {
                bail!(
                    "zstd output {} exceeds max_logical {max_logical}",
                    out.len()
                );
            }
            Ok(Bytes::from(out))
        }
        ChunkCodec::Frames => bail!("decode_chunk does not handle frames; use decode_frames_range"),
    }
}

pub async fn decode_chunk_async(
    stored: Bytes,
    codec: ChunkCodec,
    max_logical: usize,
) -> Result<Bytes> {
    if matches!(codec, ChunkCodec::Raw) || stored.len() < 64 * 1024 {
        return decode_chunk(stored, codec, max_logical);
    }
    tokio::task::spawn_blocking(move || decode_chunk(stored, codec, max_logical))
        .await
        .context("spawn_blocking decode")?
}

/// Decode a slice of a chunk, dispatching on codec.
pub async fn decode_chunk_slice_async(
    stored: Bytes,
    codec: ChunkCodec,
    frames: &[FrameRecord],
    from: usize,
    to: usize,
    logical_size: usize,
) -> Result<Bytes> {
    match codec {
        ChunkCodec::Frames => {
            let frames = frames.to_vec();
            tokio::task::spawn_blocking(move || {
                frames::decode_frames_range(stored.as_ref(), &frames, from, to)
            })
            .await
            .context("spawn_blocking frames decode")?
        }
        other => {
            let logical = decode_chunk_async(stored, other, logical_size.max(1)).await?;
            if from > to || to > logical.len() {
                bail!("slice {from}..{to} outside logical {}", logical.len());
            }
            Ok(logical.slice(from..to))
        }
    }
}

/// Sync encode helper for unit tests (legacy single-blob).
pub fn encode_chunk(logical: Bytes, policy: ChunkCodec) -> (Bytes, ChunkCodec) {
    if policy == ChunkCodec::Raw || logical.is_empty() {
        return (logical, ChunkCodec::Raw);
    }
    let fc = policy.frame_codec();
    match compress_slice(&logical, fc) {
        Ok(c) if c.len() < logical.len() => (Bytes::from(c), fc),
        _ => (logical, ChunkCodec::Raw),
    }
}

fn compress_slice(data: &[u8], codec: ChunkCodec) -> Result<Vec<u8>> {
    match codec {
        ChunkCodec::Raw | ChunkCodec::Frames => Ok(data.to_vec()),
        ChunkCodec::Gzip => {
            let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
            enc.write_all(data).context("gzip write")?;
            enc.finish().context("gzip finish")
        }
        ChunkCodec::Zstd => zstd::bulk::compress(data, 1).context("zstd compress"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3gram_blob::MemoryBlobStore;
    use futures::stream;

    #[tokio::test]
    async fn empty_body_stores_no_chunks() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let r = ingest_stream_to_store(&store, stream::empty(), None, 1024, ChunkCodec::Zstd)
            .await
            .unwrap();
        assert_eq!(r.size, 0);
        assert!(r.chunks.is_empty());
    }

    #[tokio::test]
    async fn raw_policy_never_compresses() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let data = Bytes::from(vec![b'a'; 64 * 1024]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]);
        let r = ingest_stream_to_store(&store, body, None, 64 * 1024, ChunkCodec::Raw)
            .await
            .unwrap();
        assert_eq!(r.chunks[0].codec, ChunkCodec::Raw);
        assert!(r.chunks[0].frames.is_empty());
        assert_eq!(store.get(&r.chunks[0].file_id).await.unwrap(), data);
    }

    #[tokio::test]
    async fn zstd_policy_stores_frames_codec() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let data = Bytes::from(vec![b'a'; 128 * 1024]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]);
        let mut opts = IngestOptions::new(256 * 1024, ChunkCodec::Zstd);
        opts.frame_size = 64 * 1024;
        let r = ingest_stream_with_options(&store, body, None, opts)
            .await
            .unwrap();
        assert_eq!(r.chunks[0].codec, ChunkCodec::Frames);
        assert!(!r.chunks[0].frames.is_empty());
        assert!(r.compress_calls <= 2);
        let stored = store.get(&r.chunks[0].file_id).await.unwrap();
        let got = frames::decode_frames_range(
            stored.as_ref(),
            &r.chunks[0].frames,
            0,
            data.len(),
        )
        .unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn compress_calls_bounded_by_frame_count() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let frame = 32 * 1024;
        let n = 200 * 1024;
        let data = Bytes::from(vec![0u8; n]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data)]);
        let mut opts = IngestOptions::new(128 * 1024, ChunkCodec::Zstd);
        opts.frame_size = frame;
        let r = ingest_stream_with_options(&store, body, None, opts)
            .await
            .unwrap();
        let expected = (n + frame - 1) / frame;
        assert_eq!(r.compress_calls as usize, expected);
        assert!(r.chunks.len() < (n + 128 * 1024 - 1) / (128 * 1024) + 2);
    }

    #[tokio::test]
    async fn mixed_compressible_and_random() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let mut data = vec![0u8; 100 * 1024];
        for (i, b) in data[50 * 1024..].iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(Bytes::from(data.clone()))]);
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Zstd);
        opts.frame_size = 16 * 1024;
        let r = ingest_stream_with_options(&store, body, None, opts)
            .await
            .unwrap();
        let mut out = Vec::new();
        for c in &r.chunks {
            let stored = store.get(&c.file_id).await.unwrap();
            out.extend_from_slice(
                &frames::decode_frames_range(stored.as_ref(), &c.frames, 0, c.logical_size as usize)
                    .unwrap(),
            );
        }
        assert_eq!(out, data);
    }

    #[test]
    fn legacy_zstd_decode_still_works() {
        let data = vec![7u8; 4096];
        let stored = compress_slice(&data, ChunkCodec::Zstd).unwrap();
        assert!(stored.len() < data.len());
        let got = decode_chunk(Bytes::from(stored), ChunkCodec::Zstd, data.len()).unwrap();
        assert_eq!(got.as_ref(), data.as_slice());
    }

    #[test]
    fn encode_raw_ignores_compressibility() {
        let data = Bytes::from(vec![0u8; 4096]);
        let (payload, codec) = encode_chunk(data.clone(), ChunkCodec::Raw);
        assert_eq!(codec, ChunkCodec::Raw);
        assert_eq!(payload, data);
    }
}
