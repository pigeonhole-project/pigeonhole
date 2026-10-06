//! Stream an S3 request body into Telegram-sized BlobStore chunks.
//!
//! Encoding policy comes from [`ChunkCodec`]: `raw` stores as-is; `gzip` keeps
//! gzip only when it strictly shrinks the payload. The **stored** codec is what
//! the index / snapshot record per chunk (`raw` or `gzip`).

use crate::chunker::ChunkCodec;
use crate::storage::{BlobStore, DeleteOutcome};
use anyhow::{Context, Result};
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
    /// Hex-encoded MD5 (S3 ETag for single-part PutObject).
    pub etag: String,
    pub size: i64,
    /// Empty for zero-byte objects.
    pub chunks: Vec<UploadedChunk>,
    pub md5: [u8; 16],
    pub crc32: u32,
}

#[derive(Debug)]
pub struct IngestError {
    pub source: anyhow::Error,
    /// Message IDs that failed to delete during cleanup (queue to pending_tg_deletes).
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

/// Upload body bytes into ≤`chunk_size` logical documents using `codec` policy.
/// Empty bodies produce no Telegram uploads (zero-byte S3 objects / "folder" keys).
///
/// When `checksum` is `Some`, every non-empty body chunk is also fed into the hasher.
pub async fn ingest_stream_to_store(
    store: &Arc<dyn BlobStore>,
    mut stream: impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    mut checksum: Option<&mut ChecksumHasher>,
    chunk_size: usize,
    codec: ChunkCodec,
) -> Result<IngestResult, IngestError> {
    let chunk_size = chunk_size.max(1);
    let mut md5 = Md5::new();
    let mut crc = crc32fast::Hasher::new();
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
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

        let mut offset = 0;
        while offset < chunk.len() {
            let space = chunk_size.saturating_sub(buf.len());
            let take = space.min(chunk.len() - offset);
            buf.extend_from_slice(&chunk[offset..offset + take]);
            offset += take;
            if buf.len() >= chunk_size {
                let data = Bytes::from(std::mem::take(&mut buf));
                match put_chunk(store, data, codec).await {
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
    }

    // Non-empty remainder only — never upload a zero-byte Telegram document.
    if !buf.is_empty() {
        let data = Bytes::from(buf);
        match put_chunk(store, data, codec).await {
            Ok(c) => uploaded.push((part_no, c.0, c.1, c.2, c.3)),
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

/// Returns `(file_id, message_id, logical_size, stored_codec)`.
async fn put_chunk(
    store: &Arc<dyn BlobStore>,
    data: Bytes,
    policy: ChunkCodec,
) -> Result<(String, i64, i64, ChunkCodec)> {
    let logical_size = data.len() as i64;
    let (payload, stored) = encode_chunk(data, policy);
    let filename = match stored {
        ChunkCodec::Gzip => format!("{:x}.bin.gz", Md5::digest(&payload)),
        ChunkCodec::Raw => format!("{:x}.bin", Md5::digest(&payload)),
    };
    let (file_id, message_id) = store
        .put(payload, &filename, "")
        .await
        .context("blob store put")?;
    Ok((file_id, message_id, logical_size, stored))
}

/// Apply `policy`; returned codec is what was actually stored.
pub fn encode_chunk(logical: Bytes, policy: ChunkCodec) -> (Bytes, ChunkCodec) {
    match policy {
        ChunkCodec::Raw => (logical, ChunkCodec::Raw),
        ChunkCodec::Gzip => {
            if logical.is_empty() {
                return (logical, ChunkCodec::Raw);
            }
            match gzip_bytes(&logical) {
                Ok(gz) if gz.len() < logical.len() => (Bytes::from(gz), ChunkCodec::Gzip),
                _ => (logical, ChunkCodec::Raw),
            }
        }
    }
}

pub fn decode_chunk(stored: Bytes, codec: ChunkCodec) -> Result<Bytes> {
    match codec {
        ChunkCodec::Raw => Ok(stored),
        ChunkCodec::Gzip => gunzip_bytes(&stored).map(Bytes::from),
    }
}

pub fn codec_to_sql(c: ChunkCodec) -> &'static str {
    c.as_str()
}

pub fn codec_from_sql(s: &str) -> Result<ChunkCodec> {
    // Accept legacy integer 0/1 if somehow stored as text.
    match s {
        "0" | "false" => Ok(ChunkCodec::Raw),
        "1" | "true" => Ok(ChunkCodec::Gzip),
        other => ChunkCodec::parse(other)
            .with_context(|| format!("invalid chunk codec in index: {other:?}")),
    }
}

fn gzip_bytes(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
    enc.write_all(data).context("gzip write")?;
    enc.finish().context("gzip finish")
}

fn gunzip_bytes(data: &[u8]) -> Result<Vec<u8>> {
    let mut dec = GzDecoder::new(data);
    let mut out = Vec::new();
    dec.read_to_end(&mut out).context("gunzip")?;
    Ok(out)
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
        let r = ingest_stream_to_store(&store, stream::empty(), None, 1024, ChunkCodec::Gzip)
            .await
            .unwrap();
        assert_eq!(r.size, 0);
        assert!(r.chunks.is_empty());
        assert_eq!(mem.len(), 0);
    }

    #[tokio::test]
    async fn raw_policy_never_gzips() {
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
    async fn gzip_policy_marks_when_smaller() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let data = Bytes::from(vec![b'a'; 64 * 1024]);
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]);
        let r = ingest_stream_to_store(&store, body, None, 64 * 1024, ChunkCodec::Gzip)
            .await
            .unwrap();
        let (_, file_id, _, logical, codec) = &r.chunks[0];
        assert_eq!(*logical, data.len() as i64);
        assert_eq!(*codec, ChunkCodec::Gzip);
        let stored = store.get(file_id).await.unwrap();
        assert!(stored.len() < data.len());
        assert_eq!(decode_chunk(stored, ChunkCodec::Gzip).unwrap(), data);
    }

    #[test]
    fn encode_raw_ignores_compressibility() {
        let data = Bytes::from(vec![0u8; 4096]);
        let (payload, codec) = encode_chunk(data.clone(), ChunkCodec::Raw);
        assert_eq!(codec, ChunkCodec::Raw);
        assert_eq!(payload, data);
    }

    #[test]
    fn encode_gzip_skips_when_no_gain() {
        let tiny = Bytes::from_static(b"x");
        let (payload, codec) = encode_chunk(tiny.clone(), ChunkCodec::Gzip);
        assert_eq!(codec, ChunkCodec::Raw);
        assert_eq!(payload, tiny);
    }
}
