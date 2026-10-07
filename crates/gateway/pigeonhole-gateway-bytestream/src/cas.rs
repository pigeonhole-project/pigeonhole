use crate::cas_index::CasIndex;
use crate::digest::{digest_hash_hex, verify_sha256};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_chunk_store::{
    collect_stream, ingest_stream_with_options, read_chunk_range_cached, store_delete_message,
    store_get, BoxByteStream, BlockRecord, ChunkCodec, Index, IngestOptions, LegacyBlobStore,
    UploadedChunk,
};
use pigeonhole_codec::DEFAULT_CHUNK_SIZE;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasManifest {
    pub chunks: Vec<CasChunkMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasChunkMeta {
    pub file_id: String,
    pub message_id: i64,
    pub logical_size: i64,
    pub codec: String,
    #[serde(alias = "frames")]
    pub blocks: Vec<BlockRecord>,
}

impl From<&UploadedChunk> for CasChunkMeta {
    fn from(u: &UploadedChunk) -> Self {
        Self {
            file_id: u.file_id.clone(),
            message_id: u.message_id,
            logical_size: u.logical_size,
            codec: u.codec.as_str().to_string(),
            blocks: u.blocks.clone(),
        }
    }
}

/// Store and fetch CAS payloads through [`CasIndex`] + chunked [`LegacyBlobStore`] ingest.
#[derive(Clone)]
pub struct CasStore {
    pub cas: CasIndex,
    pub store: Arc<dyn LegacyBlobStore>,
    pub chat_id: String,
    pub chunk_size: usize,
}

impl CasStore {
    pub fn new(index: Index, store: Arc<dyn LegacyBlobStore>, chat_id: String) -> Self {
        Self {
            cas: CasIndex::new(index),
            store,
            chat_id,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    pub async fn find_missing(
        &self,
        digests: &[crate::reapi::Digest],
    ) -> Result<Vec<crate::reapi::Digest>> {
        let mut pairs = Vec::with_capacity(digests.len());
        for d in digests {
            pairs.push((digest_hash_hex(d)?, d.size_bytes));
        }
        let missing = self.cas.find_missing(&pairs).await?;
        Ok(missing
            .into_iter()
            .map(|(hash, size)| crate::reapi::Digest {
                hash,
                size_bytes: size,
            })
            .collect())
    }

    pub async fn get_bytes(&self, hash_hex: &str, size: i64) -> Result<Option<Bytes>> {
        let Some(stream) = self.read_range(hash_hex, size, 0, size).await? else {
            return Ok(None);
        };
        Ok(Some(collect_stream(stream).await?))
    }

    /// Read `[offset, offset+limit)` of a CAS blob (limit <= 0 means through EOF).
    ///
    /// Returns a byte stream that yields data frame-by-frame (or per legacy blob)
    /// so callers never buffer the whole range.
    pub async fn read_range(
        &self,
        hash_hex: &str,
        size: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Option<BoxByteStream>> {
        let Some(entry) = self.cas.get(hash_hex, size).await? else {
            return Ok(None);
        };
        let from = offset.max(0) as usize;
        if from as i64 > size {
            anyhow::bail!("read offset out of range");
        }
        let to = if limit > 0 {
            (from as i64 + limit).min(size) as usize
        } else {
            size as usize
        };
        if from == to {
            let _ = self.cas.touch(hash_hex, size).await;
            return Ok(Some(Box::pin(futures::stream::once(async {
                Ok(Bytes::new())
            }))));
        }

        let _ = self.cas.touch(hash_hex, size).await;
        let store = self.store.clone();
        // Small channel so the producer cannot race ahead and retain many chunks.
        let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(1);

        if let Some(manifest_json) = entry.manifest.as_deref().filter(|s| !s.is_empty()) {
            let manifest: CasManifest =
                serde_json::from_str(manifest_json).context("parse CAS manifest")?;
            tokio::spawn(async move {
                if let Err(e) = stream_manifest_range(store, &manifest, from, to, &tx).await {
                    let _ = tx.send(Err(e)).await;
                }
            });
        } else {
            // Legacy single-blob row.
            let file_id = entry.file_id.clone();
            let hash_hex = hash_hex.to_string();
            tokio::spawn(async move {
                let result = async {
                    let raw = store_get(store.as_ref(), &file_id)
                        .await
                        .with_context(|| format!("blob get {file_id}"))?;
                    if raw.len() as i64 != size {
                        anyhow::bail!("stored blob size mismatch for {hash_hex}/{size}");
                    }
                    Ok(raw.slice(from..to))
                }
                .await;
                let _ = tx.send(result).await;
            });
        }

        Ok(Some(Box::pin(ReceiverStream::new(rx))))
    }

    pub async fn put_bytes(&self, hash_hex: &str, size: i64, data: Bytes) -> Result<()> {
        verify_sha256(&data, hash_hex, size)?;
        if self.cas.get(hash_hex, size).await?.is_some() {
            let _ = self.cas.touch(hash_hex, size).await;
            return Ok(());
        }
        let stream = futures::stream::iter(std::iter::once(Ok::<_, anyhow::Error>(data)));
        self.put_stream(hash_hex, size, stream).await
    }

    /// Ingest a body stream into chunked CAS storage (SHA-256 already verified by caller).
    pub async fn put_stream<S>(&self, hash_hex: &str, size: i64, stream: S) -> Result<()>
    where
        S: futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send,
    {
        if self.cas.get(hash_hex, size).await?.is_some() {
            let _ = self.cas.touch(hash_hex, size).await;
            // Drain stream so callers do not hang.
            let mut stream = stream;
            while stream.next().await.is_some() {}
            return Ok(());
        }

        let mut opts = IngestOptions::new(self.chunk_size, ChunkCodec::Zstd);
        opts.block_size = 1024 * 1024;
        let ingested = match ingest_stream_with_options(&self.store, stream, None, opts).await {
            Ok(v) => v,
            Err(e) => {
                for mid in e.pending_deletes {
                    let _ = store_delete_message(self.store.as_ref(), mid).await;
                }
                return Err(e.source);
            }
        };
        if ingested.size != size {
            self.abort_chunks(&ingested.chunks).await;
            anyhow::bail!(
                "CAS ingest size {} != digest size {size}",
                ingested.size
            );
        }

        let manifest = CasManifest {
            chunks: ingested.chunks.iter().map(CasChunkMeta::from).collect(),
        };
        let manifest_json = serde_json::to_string(&manifest).context("serialize CAS manifest")?;
        let bump: Vec<(String, i64, i64, Option<u32>)> = ingested
            .chunks
            .iter()
            .map(|c| {
                (
                    c.file_id.clone(),
                    c.message_id,
                    c.logical_size,
                    c.stored_crc32,
                )
            })
            .collect();

        if let Err(e) = self
            .cas
            .store_manifest(hash_hex, size, &self.chat_id, &bump, &manifest_json)
            .await
        {
            self.abort_chunks(&ingested.chunks).await;
            return Err(e);
        }
        Ok(())
    }

    pub async fn abort_chunks(&self, chunks: &[UploadedChunk]) {
        for c in chunks {
            let _ = store_delete_message(self.store.as_ref(), c.message_id).await;
        }
    }
}

async fn stream_manifest_range(
    store: Arc<dyn LegacyBlobStore>,
    manifest: &CasManifest,
    from: usize,
    to: usize,
    tx: &mpsc::Sender<Result<Bytes, anyhow::Error>>,
) -> Result<()> {
    let mut cursor = 0usize;
    let mut produced = 0usize;
    let expected = to.saturating_sub(from);
    for ch in &manifest.chunks {
        let clen = ch.logical_size as usize;
        let start = cursor;
        let end = cursor + clen;
        cursor = end;
        if end <= from || start >= to {
            continue;
        }
        let local_from = from.saturating_sub(start).min(clen);
        let local_to = to.saturating_sub(start).min(clen);
        let codec = match ch.codec.as_str() {
            "gzip" => ChunkCodec::Gzip,
            "zstd" => ChunkCodec::Zstd,
            "blocks" | "frames" => ChunkCodec::Blocks,
            _ => ChunkCodec::Raw,
        };
        stream_chunk_range(
            store.clone(),
            &ch.file_id,
            codec,
            &ch.blocks,
            local_from,
            local_to,
            clen,
            tx,
        )
        .await
        .with_context(|| format!("read CAS chunk {}", ch.file_id))?;
        produced += local_to - local_from;
        if tx.is_closed() {
            return Ok(());
        }
    }
    if produced != expected {
        anyhow::bail!("CAS range decode produced {produced} bytes, expected {expected}");
    }
    Ok(())
}

/// Yield one frame (or one legacy slice) at a time so the caller never holds the
/// whole object range.
async fn stream_chunk_range(
    store: Arc<dyn LegacyBlobStore>,
    file_id: &str,
    codec: ChunkCodec,
    blocks: &[BlockRecord],
    from: usize,
    to: usize,
    logical_size: usize,
    tx: &mpsc::Sender<Result<Bytes, anyhow::Error>>,
) -> Result<()> {
    if codec == ChunkCodec::Blocks && !blocks.is_empty() {
        let stored = store_get(store.as_ref(), file_id).await.context("blob get for blocks")?;
        let mut logical_cursor = 0usize;
        for fr in blocks {
            let flen = fr.logical_len as usize;
            let block_start = logical_cursor;
            let block_end = logical_cursor + flen;
            logical_cursor = block_end;
            if block_end <= from || block_start >= to {
                continue;
            }
            let local_from = from.saturating_sub(block_start).min(flen);
            let local_to = to.saturating_sub(block_start).min(flen);
            let piece = pigeonhole_codec::decode_blocks_range(
                stored.as_ref(),
                std::slice::from_ref(fr),
                local_from,
                local_to,
            )
            .with_context(|| format!("decode block {} of {file_id}", fr.block_no))?;
            if tx.send(Ok(piece)).await.is_err() {
                return Ok(());
            }
        }
        return Ok(());
    }

    let piece = read_chunk_range_cached(
        store,
        file_id,
        codec,
        blocks,
        from,
        to,
        logical_size,
        None,
        false,
    )
    .await?;
    let _ = tx.send(Ok(piece)).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use pigeonhole_chunk_store::DeleteOutcome;
    use pigeonhole_storage_memory::MemoryBlobStore;
    use sha2::{Digest as _, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts `get` calls so tests can assert the stream yields before all chunks load.
    struct CountingStore {
        inner: MemoryBlobStore,
        gets: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl LegacyBlobStore for CountingStore {
        fn id(&self) -> &pigeonhole_types::BackendId {
            self.inner.id()
        }
        fn limits(&self) -> &pigeonhole_types::BackendLimits {
            self.inner.limits()
        }
        async fn put(
            &self,
            data: Bytes,
            hint: pigeonhole_types::PutHint,
        ) -> Result<pigeonhole_types::Locator> {
            LegacyBlobStore::put(&self.inner, data, hint).await
        }
        async fn get(
            &self,
            loc: &pigeonhole_types::Locator,
            range: Option<pigeonhole_types::ByteRange>,
        ) -> Result<BoxByteStream> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            LegacyBlobStore::get(&self.inner, loc, range).await
        }
        async fn delete(&self, loc: &pigeonhole_types::Locator) -> Result<DeleteOutcome> {
            LegacyBlobStore::delete(&self.inner, loc).await
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_range_streams_before_all_chunks_fetched() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("t.db").display());
        let index = Index::connect(&url).await.unwrap();
        let gets = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn LegacyBlobStore> = Arc::new(CountingStore {
            inner: MemoryBlobStore::new(),
            gets: gets.clone(),
        });
        let mut cas = CasStore::new(index, store, String::new());
        // Many raw chunks → many backend gets on a full read.
        cas.chunk_size = 1024 * 1024;
        let n = 512 * 1024 * 1024;
        let piece = 1024 * 1024;
        let mut hasher = Sha256::new();
        let mut off = 0usize;
        while off < n {
            let len = (n - off).min(piece);
            hasher.update(&vec![0u8; len]);
            off += len;
        }
        let hash = hex::encode(hasher.finalize());

        let body = futures::stream::unfold(0usize, move |off| async move {
            if off >= n {
                return None;
            }
            let len = (n - off).min(piece);
            Some((Ok::<_, anyhow::Error>(Bytes::from(vec![0u8; len])), off + len))
        });
        let mut opts = IngestOptions::new(cas.chunk_size, ChunkCodec::Raw);
        opts.block_size = 1024 * 1024;
        let ingested = ingest_stream_with_options(&cas.store, Box::pin(body), None, opts)
            .await
            .expect("ingest 512 MiB");
        assert!(ingested.chunks.len() >= 64, "expected many chunks");
        let manifest = CasManifest {
            chunks: ingested.chunks.iter().map(CasChunkMeta::from).collect(),
        };
        let manifest_json = serde_json::to_string(&manifest).unwrap();
        let bump: Vec<(String, i64, i64, Option<u32>)> = ingested
            .chunks
            .iter()
            .map(|c| {
                (
                    c.file_id.clone(),
                    c.message_id,
                    c.logical_size,
                    c.stored_crc32,
                )
            })
            .collect();
        cas.cas
            .store_manifest(&hash, n as i64, "", &bump, &manifest_json)
            .await
            .unwrap();

        gets.store(0, Ordering::SeqCst);
        let total_chunks = ingested.chunks.len();
        let mut stream = cas
            .read_range(&hash, n as i64, 0, n as i64)
            .await
            .unwrap()
            .expect("stream");
        let first = stream
            .next()
            .await
            .expect("first item")
            .expect("first bytes");
        assert!(!first.is_empty());
        let gets_after_first = gets.load(Ordering::SeqCst);
        assert!(
            gets_after_first < total_chunks / 2,
            "stream yielded after {gets_after_first} gets, but blob has {total_chunks} chunks — still buffering too much"
        );
        let mut got = first.len();
        while let Some(item) = stream.next().await {
            got += item.expect("chunk").len();
        }
        assert_eq!(got, n);
        assert_eq!(gets.load(Ordering::SeqCst), total_chunks);
    }
}
