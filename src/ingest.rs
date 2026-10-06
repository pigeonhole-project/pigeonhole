//! Stream an S3 request body into Telegram-sized BlobStore chunks.

use crate::chunker;
use crate::storage::{BlobStore, DeleteOutcome};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use md5::{Digest, Md5};
use std::sync::Arc;

#[derive(Debug)]
pub struct IngestResult {
    /// Hex-encoded MD5 (S3 ETag for single-part PutObject).
    pub etag: String,
    pub size: i64,
    /// `(part_no, file_id, message_id, size)` — empty for zero-byte objects.
    pub chunks: Vec<(i64, String, i64, i64)>,
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

/// Upload body bytes into ≤19 MiB store documents.
/// Empty bodies produce no Telegram uploads (zero-byte S3 objects / "folder" keys).
pub async fn ingest_stream_to_store(
    store: &Arc<dyn BlobStore>,
    mut stream: impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
) -> Result<IngestResult, IngestError> {
    let mut md5 = Md5::new();
    let mut crc = crc32fast::Hasher::new();
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut part_no: i64 = 0;
    let mut uploaded: Vec<(i64, String, i64, i64)> = Vec::new();
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
        total_size += chunk.len() as i64;

        let mut offset = 0;
        while offset < chunk.len() {
            let space = chunker::CHUNK_SIZE.saturating_sub(buf.len());
            let take = space.min(chunk.len() - offset);
            buf.extend_from_slice(&chunk[offset..offset + take]);
            offset += take;
            if buf.len() >= chunker::CHUNK_SIZE {
                let data = Bytes::from(std::mem::take(&mut buf));
                match put_chunk(store, data).await {
                    Ok(c) => {
                        uploaded.push((part_no, c.0, c.1, c.2));
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
        match put_chunk(store, data).await {
            Ok(c) => uploaded.push((part_no, c.0, c.1, c.2)),
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

async fn cleanup_uploads(store: &Arc<dyn BlobStore>, uploaded: &[(i64, String, i64, i64)]) -> Vec<i64> {
    let mut pending = Vec::new();
    for (_, _, message_id, _) in uploaded {
        match store.delete_message(*message_id).await {
            Ok(DeleteOutcome::Deleted | DeleteOutcome::Gone) => {}
            Ok(DeleteOutcome::Failed) | Err(_) => pending.push(*message_id),
        }
    }
    pending
}

async fn put_chunk(store: &Arc<dyn BlobStore>, data: Bytes) -> Result<(String, i64, i64)> {
    let size = data.len() as i64;
    let filename = format!("{:x}.bin", Md5::digest(&data));
    let (file_id, message_id) = store
        .put(data, &filename, "")
        .await
        .context("blob store put")?;
    Ok((file_id, message_id, size))
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
        let r = ingest_stream_to_store(&store, stream::empty()).await.unwrap();
        assert_eq!(r.size, 0);
        assert!(r.chunks.is_empty());
        assert_eq!(r.etag, format!("{:x}", Md5::digest(b"")));
        assert_eq!(mem.len(), 0);
    }

    #[tokio::test]
    async fn nonempty_uploads_one_chunk() {
        let mem = Arc::new(MemoryBlobStore::new());
        let store: Arc<dyn BlobStore> = mem.clone();
        let body = stream::iter(vec![Ok::<_, anyhow::Error>(Bytes::from_static(b"hi"))]);
        let r = ingest_stream_to_store(&store, body).await.unwrap();
        assert_eq!(r.size, 2);
        assert_eq!(r.chunks.len(), 1);
        assert_eq!(mem.len(), 1);
    }
}
