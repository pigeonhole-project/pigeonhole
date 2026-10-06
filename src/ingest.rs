//! Stream an S3 request body into Telegram-sized BlobStore chunks.
//!
//! `chunk_size` caps **on-wire** payload size. With `zstd`/`gzip`, bytes are packed
//! until compressed output approaches that cap (saving Telegram messages), but
//! uncompressed buffering is also capped at [`chunker::max_logical_bytes`] so a
//! highly compressible PUT cannot grow RAM without bound. A 128 KiB probe skips
//! compression when there is no gain.

use crate::chunker::{self, ChunkCodec};
use crate::storage::{BlobStore, DeleteOutcome};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use md5::{Digest, Md5};
use s3s::checksum::ChecksumHasher;
use std::io::{Read, Write};
use std::sync::Arc;

/// `(part_no, file_id, message_id, logical_size, stored_codec)`.
pub type UploadedChunk = (i64, String, i64, i64, ChunkCodec);

#[derive(Debug)]
pub struct IngestResult {
    pub etag: String,
    pub size: i64,
    pub chunks: Vec<UploadedChunk>,
    pub md5: [u8; 16],
    pub crc32: u32,
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

/// Upload body bytes into documents of at most `chunk_size` on-wire bytes.
pub async fn ingest_stream_to_store(
    store: &Arc<dyn BlobStore>,
    mut stream: impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    mut checksum: Option<&mut ChecksumHasher>,
    chunk_size: usize,
    codec: ChunkCodec,
) -> Result<IngestResult, IngestError> {
    let max_stored = chunk_size.clamp(1, chunker::MAX_CHUNK_SIZE);
    let fill_target = chunker::fill_target(max_stored);
    let max_logical = chunker::max_logical_bytes(max_stored);
    let mut md5 = Md5::new();
    let mut crc = crc32fast::Hasher::new();
    let mut part_no: i64 = 0;
    let mut uploaded: Vec<UploadedChunk> = Vec::new();
    let mut total_size: i64 = 0;
    let mut logical = Vec::with_capacity(64 * 1024);
    // `None` until probe decides; `Raw` policy starts decided.
    let mut mode: Option<ChunkCodec> = (codec == ChunkCodec::Raw).then_some(ChunkCodec::Raw);

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
        logical.extend_from_slice(&chunk);

        loop {
            if mode.is_none() && logical.len() >= chunker::COMPRESS_PROBE_BYTES {
                mode = Some(match probe_worth_compressing(&logical, codec).await {
                    Ok(true) => codec,
                    Ok(false) => ChunkCodec::Raw,
                    Err(e) => {
                        let pending = cleanup_uploads(store, &uploaded).await;
                        return Err(IngestError {
                            source: e,
                            pending_deletes: pending,
                        });
                    }
                });
            }

            let Some(m) = mode else {
                break; // wait for more data before probe
            };

            let ready = match chunk_ready(&logical, m, max_stored, fill_target, max_logical).await {
                Ok(r) => r,
                Err(e) => {
                    let pending = cleanup_uploads(store, &uploaded).await;
                    return Err(IngestError {
                        source: e,
                        pending_deletes: pending,
                    });
                }
            };
            if !ready {
                break;
            }

            let prepared =
                match take_chunk(&mut logical, m, max_stored, fill_target, max_logical).await {
                    Ok(p) => p,
                    Err(e) => {
                        let pending = cleanup_uploads(store, &uploaded).await;
                        return Err(IngestError {
                            source: e,
                            pending_deletes: pending,
                        });
                    }
                };
            match put_prepared(store, prepared).await {
                Ok(c) => {
                    uploaded.push((part_no, c.0, c.1, c.2, c.3));
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

    // Drain remainder (may need several takes if compressed size > max_stored).
    while !logical.is_empty() {
        let m = match mode {
            Some(m) => m,
            None => match probe_worth_compressing(&logical, codec).await {
                Ok(true) => codec,
                Ok(false) => ChunkCodec::Raw,
                Err(e) => {
                    let pending = cleanup_uploads(store, &uploaded).await;
                    return Err(IngestError {
                        source: e,
                        pending_deletes: pending,
                    });
                }
            },
        };
        mode = Some(m);

        let prepared = match drain_one(&mut logical, m, max_stored, fill_target, max_logical).await
        {
            Ok(p) => p,
            Err(e) => {
                let pending = cleanup_uploads(store, &uploaded).await;
                return Err(IngestError {
                    source: e,
                    pending_deletes: pending,
                });
            }
        };
        match put_prepared(store, prepared).await {
            Ok(c) => {
                uploaded.push((part_no, c.0, c.1, c.2, c.3));
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
    })
}

struct Prepared {
    logical: Bytes,
    payload: Bytes,
    codec: ChunkCodec,
}

async fn chunk_ready(
    logical: &[u8],
    mode: ChunkCodec,
    max_stored: usize,
    fill_target: usize,
    max_logical: usize,
) -> Result<bool> {
    if logical.is_empty() {
        return Ok(false);
    }
    match mode {
        ChunkCodec::Raw => Ok(logical.len() >= max_stored),
        ChunkCodec::Gzip | ChunkCodec::Zstd => {
            if logical.len() >= max_logical {
                return Ok(true);
            }
            Ok(estimate_compressed_len(logical, mode).await? >= fill_target)
        }
    }
}

async fn probe_worth_compressing(logical: &[u8], policy: ChunkCodec) -> Result<bool> {
    if !policy.is_compressing() || logical.is_empty() {
        return Ok(false);
    }
    let n = logical.len().min(chunker::COMPRESS_PROBE_BYTES);
    let probe = logical[..n].to_vec();
    let compressed = tokio::task::spawn_blocking(move || compress_slice(&probe, policy))
        .await
        .context("spawn_blocking probe")??;
    Ok(compressed.len() < n)
}

async fn estimate_compressed_len(logical: &[u8], codec: ChunkCodec) -> Result<usize> {
    let data = logical.to_vec();
    tokio::task::spawn_blocking(move || compress_slice(&data, codec).map(|v| v.len()))
        .await
        .context("spawn_blocking estimate")?
}

/// Emit one chunk from the remainder buffer (EOF path).
async fn drain_one(
    logical: &mut Vec<u8>,
    codec: ChunkCodec,
    max_stored: usize,
    fill_target: usize,
    max_logical: usize,
) -> Result<Prepared> {
    if codec == ChunkCodec::Raw {
        return take_chunk(logical, codec, max_stored, fill_target, max_logical).await;
    }
    let est = estimate_compressed_len(logical, codec).await?;
    if est <= max_stored && logical.len() <= max_logical {
        let data = std::mem::take(logical);
        return finalize_chunk(data, codec, max_stored).await;
    }
    take_chunk(logical, codec, max_stored, fill_target, max_logical).await
}

/// Cut a prefix of `logical` that encodes to ≤ `max_stored`, preferring ≥ `fill_target`.
/// Only the first `max_logical` bytes are considered (RAM / decode bound).
async fn take_chunk(
    logical: &mut Vec<u8>,
    codec: ChunkCodec,
    max_stored: usize,
    fill_target: usize,
    max_logical: usize,
) -> Result<Prepared> {
    if logical.is_empty() {
        bail!("take_chunk on empty buffer");
    }
    if codec == ChunkCodec::Raw {
        let n = logical.len().min(max_stored);
        let chunk = logical.drain(..n).collect::<Vec<_>>();
        return Ok(Prepared {
            logical: Bytes::from(chunk.clone()),
            payload: Bytes::from(chunk),
            codec: ChunkCodec::Raw,
        });
    }

    let search_hi = logical.len().min(max_logical);
    let data = logical[..search_hi].to_vec();
    let (end, payload, stored_codec) = tokio::task::spawn_blocking(move || {
        let mut lo = 1usize;
        let mut hi = data.len();
        let mut best: Option<(usize, Vec<u8>)> = None;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let c = compress_slice(&data[..mid], codec)?;
            if c.len() <= max_stored {
                best = Some((mid, c));
                lo = mid + 1;
            } else if mid == 0 {
                break;
            } else {
                hi = mid - 1;
            }
        }
        let (end, mut c) = best.context("no prefix fits under max_stored")?;
        let _ = fill_target;
        // If compression lost, store raw prefix of max_stored.
        if c.len() >= end {
            let n = end.min(max_stored);
            c = data[..n].to_vec();
            return Ok::<_, anyhow::Error>((n, c, ChunkCodec::Raw));
        }
        Ok((end, c, codec))
    })
    .await
    .context("spawn_blocking take_chunk")??;

    let logical_bytes = logical.drain(..end).collect::<Vec<_>>();
    Ok(Prepared {
        logical: Bytes::from(logical_bytes),
        payload: Bytes::from(payload),
        codec: stored_codec,
    })
}

async fn finalize_chunk(logical: Vec<u8>, codec: ChunkCodec, max_stored: usize) -> Result<Prepared> {
    if logical.is_empty() {
        bail!("empty finalize");
    }
    if codec == ChunkCodec::Raw {
        if logical.len() > max_stored {
            bail!(
                "raw remainder {} exceeds max_stored {}",
                logical.len(),
                max_stored
            );
        }
        return Ok(Prepared {
            logical: Bytes::from(logical.clone()),
            payload: Bytes::from(logical),
            codec: ChunkCodec::Raw,
        });
    }
    let data = logical;
    tokio::task::spawn_blocking(move || {
        let c = compress_slice(&data, codec)?;
        if c.len() < data.len() && c.len() <= max_stored {
            Ok(Prepared {
                logical: Bytes::from(data),
                payload: Bytes::from(c),
                codec,
            })
        } else {
            if data.len() > max_stored {
                bail!(
                    "incompressible remainder {} exceeds max_stored {}",
                    data.len(),
                    max_stored
                );
            }
            Ok(Prepared {
                logical: Bytes::from(data.clone()),
                payload: Bytes::from(data),
                codec: ChunkCodec::Raw,
            })
        }
    })
    .await
    .context("spawn_blocking finalize")?
}

async fn put_prepared(
    store: &Arc<dyn BlobStore>,
    prepared: Prepared,
) -> Result<(String, i64, i64, ChunkCodec)> {
    let logical_size = prepared.logical.len() as i64;
    let filename = match prepared.codec {
        ChunkCodec::Zstd => format!("{:x}.bin.zst", Md5::digest(&prepared.payload)),
        ChunkCodec::Gzip => format!("{:x}.bin.gz", Md5::digest(&prepared.payload)),
        ChunkCodec::Raw => format!("{:x}.bin", Md5::digest(&prepared.payload)),
    };
    let (file_id, message_id) = store
        .put(prepared.payload, &filename, "")
        .await
        .context("blob store put")?;
    Ok((file_id, message_id, logical_size, prepared.codec))
}

async fn cleanup_uploads(store: &Arc<dyn BlobStore>, uploaded: &[UploadedChunk]) -> Vec<i64> {
    let mut pending = Vec::new();
    for (_, _, message_id, _, _) in uploaded {
        match store.delete_message(*message_id).await {
            Ok(DeleteOutcome::Deleted | DeleteOutcome::Gone) => {}
            Ok(DeleteOutcome::Failed) | Err(_) => pending.push(*message_id),
        }
    }
    pending
}

fn compress_slice(data: &[u8], codec: ChunkCodec) -> Result<Vec<u8>> {
    match codec {
        ChunkCodec::Raw => Ok(data.to_vec()),
        ChunkCodec::Gzip => {
            let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
            enc.write_all(data).context("gzip write")?;
            enc.finish().context("gzip finish")
        }
        ChunkCodec::Zstd => zstd::bulk::compress(data, 1).context("zstd compress"),
    }
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

/// Decode a stored chunk. `max_logical` is the allowed uncompressed size
/// (typically the `chunks.size` column from the index).
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
    }
}

/// Async decode that offloads CPU work for non-trivial payloads.
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

pub fn codec_to_sql(c: ChunkCodec) -> &'static str {
    c.as_str()
}

pub fn codec_from_sql(s: &str) -> Result<ChunkCodec> {
    match s {
        "0" | "false" => Ok(ChunkCodec::Raw),
        "1" | "true" => Ok(ChunkCodec::Gzip), // legacy compressed flag
        other => ChunkCodec::parse(other)
            .with_context(|| format!("invalid chunk codec in index: {other:?}")),
    }
}

/// Sync encode helper for unit tests.
pub fn encode_chunk(logical: Bytes, policy: ChunkCodec) -> (Bytes, ChunkCodec) {
    if policy == ChunkCodec::Raw || logical.is_empty() {
        return (logical, ChunkCodec::Raw);
    }
    match compress_slice(&logical, policy) {
        Ok(c) if c.len() < logical.len() => (Bytes::from(c), policy),
        _ => (logical, ChunkCodec::Raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBlobStore;
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
        let (_, file_id, _, _, codec) = &r.chunks[0];
        assert_eq!(*codec, ChunkCodec::Raw);
        assert_eq!(store.get(file_id).await.unwrap(), data);
    }

    #[tokio::test]
    async fn zstd_marks_when_smaller() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let data = Bytes::from(vec![b'a'; 128 * 1024]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]);
        let r = ingest_stream_to_store(&store, body, None, 256 * 1024, ChunkCodec::Zstd)
            .await
            .unwrap();
        let (_, file_id, _, logical, codec) = &r.chunks[0];
        assert_eq!(*logical, data.len() as i64);
        assert_eq!(*codec, ChunkCodec::Zstd);
        let stored = store.get(file_id).await.unwrap();
        assert!(stored.len() < data.len());
        assert_eq!(
            decode_chunk(stored, ChunkCodec::Zstd, data.len()).unwrap(),
            data
        );
    }

    #[tokio::test]
    async fn compressible_packs_more_than_one_max_logical() {
        // max_stored small; highly compressible → one message holds >> max_stored logical.
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let max_stored = 8 * 1024;
        let data = Bytes::from(vec![0u8; 64 * 1024]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]);
        let r = ingest_stream_to_store(&store, body, None, max_stored, ChunkCodec::Zstd)
            .await
            .unwrap();
        assert_eq!(r.size, data.len() as i64);
        // With packing, should be fewer chunks than raw ceil(64k/8k)=8.
        assert!(
            r.chunks.len() < 8,
            "expected packing, got {} chunks",
            r.chunks.len()
        );
        let mut out = Vec::new();
        for (_, fid, _, logical_size, codec) in &r.chunks {
            let stored = store.get(fid).await.unwrap();
            assert!(stored.len() <= max_stored);
            assert!(*logical_size as usize <= chunker::max_logical_bytes(max_stored));
            out.extend_from_slice(
                &decode_chunk(stored, *codec, *logical_size as usize).unwrap(),
            );
        }
        assert_eq!(out, data.as_ref());
    }

    #[tokio::test]
    async fn chunk_ready_flushes_at_logical_cap() {
        // Avoid allocating 256 MiB in unit tests: drive chunk_ready directly.
        let max_stored = 4 * 1024;
        let fill_target = chunker::fill_target(max_stored);
        let max_logical = 64 * 1024; // synthetic cap for this test
        let under = vec![0u8; max_logical - 1];
        // Tiny compressible payload stays under fill_target and under cap → not ready.
        assert!(!chunk_ready(&under[..1024], ChunkCodec::Zstd, max_stored, fill_target, max_logical)
            .await
            .unwrap());
        let at_cap = vec![0u8; max_logical];
        assert!(chunk_ready(&at_cap, ChunkCodec::Zstd, max_stored, fill_target, max_logical)
            .await
            .unwrap());
    }

    #[test]
    fn gzip_decode_respects_cap() {
        let logical = vec![b'x'; 8192];
        let stored = compress_slice(&logical, ChunkCodec::Gzip).unwrap();
        assert!(decode_chunk(Bytes::from(stored.clone()), ChunkCodec::Gzip, 8192).is_ok());
        let err = decode_chunk(Bytes::from(stored), ChunkCodec::Gzip, 100).unwrap_err();
        assert!(err.to_string().contains("max_logical"), "{err}");
    }

    #[test]
    fn encode_raw_ignores_compressibility() {
        let data = Bytes::from(vec![0u8; 4096]);
        let (payload, codec) = encode_chunk(data.clone(), ChunkCodec::Raw);
        assert_eq!(codec, ChunkCodec::Raw);
        assert_eq!(payload, data);
    }
}
