use crate::digest::{digest_hash_hex, verify_sha256};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_blob_store::{
    ingest_stream_with_options, read_chunk_range_cached, BlobStore, ChunkCodec, FrameRecord,
    Index, IngestOptions, UploadedChunk,
};
use pigeonhole_codec::DEFAULT_CHUNK_SIZE;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
    pub frames: Vec<FrameRecord>,
}

impl From<&UploadedChunk> for CasChunkMeta {
    fn from(u: &UploadedChunk) -> Self {
        Self {
            file_id: u.file_id.clone(),
            message_id: u.message_id,
            logical_size: u.logical_size,
            codec: u.codec.as_str().to_string(),
            frames: u.frames.clone(),
        }
    }
}

/// Store and fetch CAS payloads through the index + chunked [`BlobStore`] ingest.
#[derive(Clone)]
pub struct CasStore {
    pub index: Index,
    pub store: Arc<dyn BlobStore>,
    pub chat_id: String,
    pub chunk_size: usize,
}

impl CasStore {
    pub fn new(index: Index, store: Arc<dyn BlobStore>, chat_id: String) -> Self {
        Self {
            index,
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
        let missing = self.index.cas_find_missing(&pairs).await?;
        Ok(missing
            .into_iter()
            .map(|(hash, size)| crate::reapi::Digest {
                hash,
                size_bytes: size,
            })
            .collect())
    }

    pub async fn get_bytes(&self, hash_hex: &str, size: i64) -> Result<Option<Bytes>> {
        self.read_range(hash_hex, size, 0, size).await
    }

    /// Read `[offset, offset+limit)` of a CAS blob (limit <= 0 means through EOF).
    pub async fn read_range(
        &self,
        hash_hex: &str,
        size: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Option<Bytes>> {
        let Some(entry) = self.index.cas_get(hash_hex, size).await? else {
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
            let _ = self.index.cas_touch(hash_hex, size).await;
            return Ok(Some(Bytes::new()));
        }

        let data = if let Some(manifest_json) = entry.manifest.as_deref().filter(|s| !s.is_empty()) {
            let manifest: CasManifest =
                serde_json::from_str(manifest_json).context("parse CAS manifest")?;
            read_manifest_range(self.store.clone(), &manifest, from, to).await?
        } else {
            // Legacy single-blob row.
            let raw = self
                .store
                .get(&entry.file_id)
                .await
                .with_context(|| format!("blob get {}", entry.file_id))?;
            if raw.len() as i64 != size {
                anyhow::bail!("stored blob size mismatch for {hash_hex}/{size}");
            }
            raw.slice(from..to)
        };
        let _ = self.index.cas_touch(hash_hex, size).await;
        Ok(Some(data))
    }

    pub async fn put_bytes(&self, hash_hex: &str, size: i64, data: Bytes) -> Result<()> {
        verify_sha256(&data, hash_hex, size)?;
        if self.index.cas_get(hash_hex, size).await?.is_some() {
            let _ = self.index.cas_touch(hash_hex, size).await;
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
        if self.index.cas_get(hash_hex, size).await?.is_some() {
            let _ = self.index.cas_touch(hash_hex, size).await;
            // Drain stream so callers do not hang.
            let mut stream = stream;
            while stream.next().await.is_some() {}
            return Ok(());
        }

        let mut opts = IngestOptions::new(self.chunk_size, ChunkCodec::Zstd);
        opts.frame_size = 1024 * 1024;
        let ingested = match ingest_stream_with_options(&self.store, stream, None, opts).await {
            Ok(v) => v,
            Err(e) => {
                for mid in e.pending_deletes {
                    let _ = self.store.delete_message(mid).await;
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
            .index
            .cas_store_manifest(hash_hex, size, &self.chat_id, &bump, &manifest_json)
            .await
        {
            self.abort_chunks(&ingested.chunks).await;
            return Err(e);
        }
        Ok(())
    }

    pub async fn abort_chunks(&self, chunks: &[UploadedChunk]) {
        for c in chunks {
            let _ = self.store.delete_message(c.message_id).await;
        }
    }
}

async fn read_manifest_range(
    store: Arc<dyn BlobStore>,
    manifest: &CasManifest,
    from: usize,
    to: usize,
) -> Result<Bytes> {
    let mut out = Vec::with_capacity(to.saturating_sub(from));
    let mut cursor = 0usize;
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
            "frames" => ChunkCodec::Frames,
            _ => ChunkCodec::Raw,
        };
        let piece = read_chunk_range_cached(
            store.clone(),
            &ch.file_id,
            codec,
            &ch.frames,
            local_from,
            local_to,
            clen,
            None,
            false,
        )
        .await
        .with_context(|| format!("read CAS chunk {}", ch.file_id))?;
        out.extend_from_slice(piece.as_ref());
    }
    if out.len() != to - from {
        anyhow::bail!(
            "CAS range decode produced {} bytes, expected {}",
            out.len(),
            to - from
        );
    }
    Ok(Bytes::from(out))
}
