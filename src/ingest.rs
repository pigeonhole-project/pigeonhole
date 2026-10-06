//! Stream an S3 request body into Telegram-sized BlobStore chunks.

use crate::chunker;
use crate::storage::BlobStore;
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use md5::{Digest, Md5};
use std::sync::Arc;

/// Upload body bytes into ≤19 MiB store documents.
/// Returns (md5_hex etag, total_size, chunks: part_no, file_id, message_id, size).
pub async fn ingest_stream_to_store(
    store: &Arc<dyn BlobStore>,
    mut stream: impl futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
) -> Result<(String, i64, Vec<(i64, String, i64, i64)>)> {
    let mut md5 = Md5::new();
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut part_no: i64 = 0;
    let mut uploaded: Vec<(i64, String, i64, i64)> = Vec::new();
    let mut total_size: i64 = 0;

    while let Some(item) = stream.next().await {
        let chunk = match item {
            Ok(c) => c,
            Err(e) => {
                cleanup_uploads(store, &uploaded).await;
                return Err(e);
            }
        };
        if chunk.is_empty() {
            continue;
        }
        md5.update(&chunk);
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
                        cleanup_uploads(store, &uploaded).await;
                        return Err(e);
                    }
                }
            }
        }
    }

    if !buf.is_empty() || uploaded.is_empty() {
        let data = Bytes::from(buf);
        match put_chunk(store, data).await {
            Ok(c) => uploaded.push((part_no, c.0, c.1, c.2)),
            Err(e) => {
                cleanup_uploads(store, &uploaded).await;
                return Err(e);
            }
        }
    }

    let etag = format!("{:x}", md5.finalize());
    Ok((etag, total_size, uploaded))
}

async fn cleanup_uploads(store: &Arc<dyn BlobStore>, uploaded: &[(i64, String, i64, i64)]) {
    for (_, _, message_id, _) in uploaded {
        let _ = store.delete_message(*message_id).await;
    }
}

async fn put_chunk(
    store: &Arc<dyn BlobStore>,
    data: Bytes,
) -> Result<(String, i64, i64)> {
    let size = data.len() as i64;
    let filename = format!("{:x}.bin", Md5::digest(&data));
    let (file_id, message_id) = store
        .put(data, &filename, "")
        .await
        .context("blob store put")?;
    Ok((file_id, message_id, size))
}
