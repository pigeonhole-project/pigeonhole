//! Stream an S3 request body into Telegram-sized LegacyBlobStore chunks.
//!
//! With compressing policies (`zstd` / `gzip`), data is packed as independent
//! fixed-size frames ([`pigeonhole_codec::BlockWriter`]) so each block is compressed
//! once. Chunk codec stored in the index is [`ChunkCodec::Blocks`]. Legacy
//! single-blob `raw` / `gzip` / `zstd` chunks remain readable.

use pigeonhole_blob::{store_delete_message, store_get, store_put, DeleteOutcome, LegacyBlobStore};
use pigeonhole_codec::{self as chunker, ChunkCodec};
use pigeonhole_codec::{ByteBudget, CompletedChunk, BlockRecord, BlockWriter};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use md5::{Digest, Md5};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use pigeonhole_codec::{codec_from_sql, codec_to_sql, UploadedChunk};

/// Optional streaming hasher updated with plaintext body bytes during ingest.
///
/// Gateways (e.g. S3 CRC/SHA) implement this; `blob-store` stays free of protocol crates.
pub trait IngestHasher: Send {
    fn update(&mut self, data: &[u8]);
}

impl IngestHasher for () {
    fn update(&mut self, _data: &[u8]) {}
}

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

/// Upload body bytes into documents of at most `chunk_size` on-wire bytes.
pub async fn ingest_stream_to_store(
    store: &Arc<dyn LegacyBlobStore>,
    stream: impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    checksum: Option<&mut dyn IngestHasher>,
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
    store: &Arc<dyn LegacyBlobStore>,
    mut stream: impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    checksum: Option<&mut dyn IngestHasher>,
    opts: IngestOptions,
) -> Result<IngestResult, IngestError> {
    if opts.codec == ChunkCodec::Raw {
        return ingest_raw(store, &mut stream, checksum, opts.chunk_size).await;
    }
    ingest_framed(store, &mut stream, checksum, opts).await
}

async fn ingest_framed(
    store: &Arc<dyn LegacyBlobStore>,
    stream: &mut (impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin),
    mut checksum: Option<&mut dyn IngestHasher>,
    opts: IngestOptions,
) -> Result<IngestResult, IngestError> {
    let max_stored = opts.chunk_size.clamp(1, chunker::MAX_CHUNK_SIZE);
    let max_logical = chunker::MAX_LOGICAL_CHUNK;
    let calls = opts
        .compress_calls
        .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
    let mut writer = BlockWriter::new(
        opts.block_size,
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

        let mut rest = chunk.as_ref();
        while !rest.is_empty() {
            let (completed, consumed) = match writer.push(rest).await {
                Ok(v) => v,
                Err(e) => {
                    let pending = cleanup_uploads(store, &uploaded).await;
                    return Err(IngestError {
                        source: e,
                        pending_deletes: pending,
                    });
                }
            };
            // Under budget pressure the writer may seal with consumed==0 so the
            // caller can free permits before more input is reserved.
            if consumed == 0 && completed.is_empty() {
                let pending = cleanup_uploads(store, &uploaded).await;
                return Err(IngestError {
                    source: anyhow::anyhow!("ingest made no progress (budget deadlock?)"),
                    pending_deletes: pending,
                });
            }
            rest = &rest[consumed..];
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
    store: &Arc<dyn LegacyBlobStore>,
    stream: &mut (impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin),
    mut checksum: Option<&mut dyn IngestHasher>,
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
    store: &Arc<dyn LegacyBlobStore>,
    mut done: CompletedChunk,
    part_no: i64,
) -> Result<UploadedChunk> {
    if done.payload.is_empty() {
        bail!("refusing empty framed chunk");
    }
    let payload = std::mem::replace(&mut done.payload, Bytes::new());
    let blocks = std::mem::take(&mut done.blocks);
    let logical_size = done.logical_size;
    let stored_crc32 = Some(crc32fast::hash(payload.as_ref()));
    let filename = format!("{:x}.bin.blocks", Md5::digest(&payload));
    let put_result = store_put(store.as_ref(), payload, &filename, "").await;
    // Release ingest-budget permits only after put attempt finishes.
    drop(done);
    let (file_id, message_id) = put_result.context("blob store put")?;
    Ok(UploadedChunk {
        part_no,
        file_id,
        message_id,
        logical_size,
        codec: ChunkCodec::Blocks,
        blocks,
        stored_crc32,
    })
}

async fn put_raw_piece(
    store: &Arc<dyn LegacyBlobStore>,
    piece: Vec<u8>,
    part_no: i64,
) -> Result<UploadedChunk> {
    let logical_size = piece.len() as i64;
    let stored_crc32 = Some(crc32fast::hash(&piece));
    let filename = format!("{:x}.bin", Md5::digest(&piece));
    let (file_id, message_id) = store_put(store.as_ref(), Bytes::from(piece), &filename, "")
        .await
        .context("blob store put")?;
    Ok(UploadedChunk {
        part_no,
        file_id,
        message_id,
        logical_size,
        codec: ChunkCodec::Raw,
        blocks: Vec::new(),
        stored_crc32,
    })
}

async fn cleanup_uploads(store: &Arc<dyn LegacyBlobStore>, uploaded: &[UploadedChunk]) -> Vec<i64> {
    let mut pending = Vec::new();
    for u in uploaded {
        match store_delete_message(store.as_ref(), u.message_id).await {
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
        ChunkCodec::Blocks => bail!("decode_chunk does not handle frames; use decode_blocks_range"),
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
    blocks: &[BlockRecord],
    from: usize,
    to: usize,
    logical_size: usize,
) -> Result<Bytes> {
    match codec {
        ChunkCodec::Blocks => {
            let blocks = blocks.to_vec();
            tokio::task::spawn_blocking(move || {
                pigeonhole_codec::decode_blocks_range(stored.as_ref(), &blocks, from, to)
            })
            .await
            .context("spawn_blocking blocks decode")?
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
    let fc = policy.block_codec();
    match compress_slice(&logical, fc) {
        Ok(c) if c.len() < logical.len() => (Bytes::from(c), fc),
        _ => (logical, ChunkCodec::Raw),
    }
}

fn compress_slice(data: &[u8], codec: ChunkCodec) -> Result<Vec<u8>> {
    match codec {
        ChunkCodec::Raw | ChunkCodec::Blocks => Ok(data.to_vec()),
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
    use pigeonhole_storage_memory::MemoryBlobStore;
    use futures::stream;

    #[tokio::test]
    async fn empty_body_stores_no_chunks() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn LegacyBlobStore> = mem.clone();
        let r = ingest_stream_to_store(&store, stream::empty(), None, 1024, ChunkCodec::Zstd)
            .await
            .unwrap();
        assert_eq!(r.size, 0);
        assert!(r.chunks.is_empty());
    }

    #[tokio::test]
    async fn raw_policy_never_compresses() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn LegacyBlobStore> = mem.clone();
        let data = Bytes::from(vec![b'a'; 64 * 1024]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]);
        let r = ingest_stream_to_store(&store, body, None, 64 * 1024, ChunkCodec::Raw)
            .await
            .unwrap();
        assert_eq!(r.chunks[0].codec, ChunkCodec::Raw);
        assert!(r.chunks[0].blocks.is_empty());
        assert_eq!(store_get(store.as_ref(), &r.chunks[0].file_id).await.unwrap(), data);
    }

    #[tokio::test]
    async fn zstd_policy_stores_blocks_codec() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn LegacyBlobStore> = mem.clone();
        let data = Bytes::from(vec![b'a'; 128 * 1024]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]);
        let mut opts = IngestOptions::new(256 * 1024, ChunkCodec::Zstd);
        opts.block_size = 64 * 1024;
        let r = ingest_stream_with_options(&store, body, None, opts)
            .await
            .unwrap();
        assert_eq!(r.chunks[0].codec, ChunkCodec::Blocks);
        assert!(!r.chunks[0].blocks.is_empty());
        assert!(r.compress_calls <= 2);
        let stored = store_get(store.as_ref(), &r.chunks[0].file_id).await.unwrap();
        let got = pigeonhole_codec::decode_blocks_range(
            stored.as_ref(),
            &r.chunks[0].blocks,
            0,
            data.len(),
        )
        .unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn compress_calls_bounded_by_frame_count() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn LegacyBlobStore> = mem.clone();
        let frame = 32 * 1024;
        let n = 200 * 1024;
        let data = Bytes::from(vec![0u8; n]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data)]);
        let mut opts = IngestOptions::new(128 * 1024, ChunkCodec::Zstd);
        opts.block_size = frame;
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
        let store: Arc<dyn LegacyBlobStore> = mem.clone();
        let mut data = vec![0u8; 100 * 1024];
        for (i, b) in data[50 * 1024..].iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(Bytes::from(data.clone()))]);
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Zstd);
        opts.block_size = 16 * 1024;
        let r = ingest_stream_with_options(&store, body, None, opts)
            .await
            .unwrap();
        let mut out = Vec::new();
        for c in &r.chunks {
            let stored = store_get(store.as_ref(), &c.file_id).await.unwrap();
            out.extend_from_slice(
                &pigeonhole_codec::decode_blocks_range(stored.as_ref(), &c.blocks, 0, c.logical_size as usize)
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

    /// Object larger than the ingest budget must still complete (budget covers
    /// only the open chunk + block, not the whole object).
    #[tokio::test]
    async fn large_put_under_small_budget_completes() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn LegacyBlobStore> = mem.clone();
        let budget = ByteBudget::new(32 * 1024 * 1024);
        let n = 300 * 1024 * 1024;
        // Stream in 4 MiB pieces so we do not hold the whole object in one Bytes.
        let piece = 4 * 1024 * 1024;
        let body = futures::stream::unfold(0usize, move |off| async move {
            if off >= n {
                return None;
            }
            let len = (n - off).min(piece);
            Some((Ok::<_, anyhow::Error>(Bytes::from(vec![0u8; len])), off + len))
        });
        let mut opts = IngestOptions::new(8 * 1024 * 1024, ChunkCodec::Zstd);
        opts.block_size = 1024 * 1024;
        opts.memory_budget = Some(budget.clone());
        let r = ingest_stream_with_options(&store, Box::pin(body), None, opts)
            .await
            .expect("300 MiB ingest under 32 MiB budget");
        assert_eq!(r.size, n as i64);
        assert!(!r.chunks.is_empty());
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    #[tokio::test]
    async fn parallel_puts_share_budget_without_deadlock() {
        let budget = ByteBudget::new(64 * 1024 * 1024);
        let mut joins = Vec::new();
        for _ in 0..8 {
            let budget = budget.clone();
            joins.push(tokio::spawn(async move {
                let mem = Arc::new(MemoryBlobStore::new());
                let store: Arc<dyn LegacyBlobStore> = mem;
                let n = 64 * 1024 * 1024;
                let piece = 2 * 1024 * 1024;
                let body = futures::stream::unfold(0usize, move |off| async move {
                    if off >= n {
                        return None;
                    }
                    let len = (n - off).min(piece);
                    Some((Ok::<_, anyhow::Error>(Bytes::from(vec![1u8; len])), off + len))
                });
                let mut opts = IngestOptions::new(4 * 1024 * 1024, ChunkCodec::Zstd);
                opts.block_size = 512 * 1024;
                opts.memory_budget = Some(budget);
                ingest_stream_with_options(&store, Box::pin(body), None, opts).await
            }));
        }
        for j in joins {
            j.await
                .expect("join")
                .expect("parallel 64 MiB ingest under 64 MiB shared budget");
        }
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    /// 64 parallel 8 MiB PUTs sharing a budget of only `4 * block_size` must
    /// finish (whole-frame acquire prevents fragment deadlock).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn parallel_puts_whole_frame_budget_no_deadlock() {
        let frame = 512 * 1024;
        let budget = ByteBudget::new(4 * frame);
        let mut joins = Vec::new();
        for _ in 0..64 {
            let budget = budget.clone();
            joins.push(tokio::spawn(async move {
                let mem = Arc::new(MemoryBlobStore::new());
                let store: Arc<dyn LegacyBlobStore> = mem;
                let n = 8 * 1024 * 1024;
                let piece = 256 * 1024;
                let body = futures::stream::unfold(0usize, move |off| async move {
                    if off >= n {
                        return None;
                    }
                    let len = (n - off).min(piece);
                    Some((Ok::<_, anyhow::Error>(Bytes::from(vec![3u8; len])), off + len))
                });
                let mut opts = IngestOptions::new(2 * 1024 * 1024, ChunkCodec::Raw);
                opts.block_size = frame;
                opts.memory_budget = Some(budget);
                ingest_stream_with_options(&store, Box::pin(body), None, opts).await
            }));
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            for j in joins {
                j.await
                    .expect("join")
                    .expect("parallel 8 MiB ingest under 4*block_size budget");
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "64 parallel 8 MiB PUTs under 4*block_size budget timed out"
        );
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    #[tokio::test]
    async fn cancel_mid_put_returns_budget() {
        let budget = ByteBudget::new(16 * 1024 * 1024);
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn LegacyBlobStore> = mem;
        let budget_c = budget.clone();
        let handle = tokio::spawn(async move {
            let n = 128 * 1024 * 1024;
            let piece = 1024 * 1024;
            let body = futures::stream::unfold(0usize, move |off| async move {
                if off >= n {
                    return None;
                }
                // Yield so the parent can abort while permits are held.
                tokio::task::yield_now().await;
                let len = (n - off).min(piece);
                Some((Ok::<_, anyhow::Error>(Bytes::from(vec![2u8; len])), off + len))
            });
            let mut opts = IngestOptions::new(4 * 1024 * 1024, ChunkCodec::Zstd);
            opts.block_size = 512 * 1024;
            opts.memory_budget = Some(budget_c);
            ingest_stream_with_options(&store, Box::pin(body), None, opts).await
        });
        // Wait until some budget is taken, then cancel.
        for _ in 0..200 {
            if budget.available_permits() < budget.capacity() {
                break;
            }
            tokio::task::yield_now().await;
        }
        handle.abort();
        let _ = handle.await;
        // Abort drops the task's BlockWriter / CompletedChunks → permits return.
        assert_eq!(budget.available_permits(), budget.capacity());
    }
}
