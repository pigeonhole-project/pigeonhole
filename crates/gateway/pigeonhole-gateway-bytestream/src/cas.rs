use crate::cas_index::{CasEntry, CasIndex};
use crate::digest::{digest_hash_hex, verify_sha256};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_chunk_store::{ChunkStore, IngestOptions};
use pigeonhole_codec::{ChunkCodec, DEFAULT_CHUNK_SIZE};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Store and fetch CAS payloads through [`CasIndex`] + [`ChunkStore`].
#[derive(Clone)]
pub struct CasStore {
    pub cas: CasIndex,
    pub store: Arc<ChunkStore>,
    pub chunk_size: usize,
}

impl CasStore {
    pub fn new(cas: CasIndex, store: Arc<ChunkStore>) -> Self {
        Self {
            cas,
            store,
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
        let mut out = Vec::new();
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            out.extend_from_slice(&item?);
        }
        Ok(Some(Bytes::from(out)))
    }

    /// Read `[offset, offset+limit)` of a CAS blob (limit <= 0 means through EOF).
    pub async fn read_range(
        &self,
        hash_hex: &str,
        size: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Option<pigeonhole_chunk_store::BoxByteStream>> {
        let Some(entry) = self.cas.get(hash_hex, size).await? else {
            return Ok(None);
        };
        let from = offset.max(0) as u64;
        if from as i64 > size {
            anyhow::bail!("read offset out of range");
        }
        let to = if limit > 0 {
            (from as i64 + limit).min(size) as u64
        } else {
            size as u64
        };
        let _ = self.cas.touch(hash_hex, size).await;
        if from == to {
            return Ok(Some(Box::pin(futures::stream::once(async {
                Ok(Bytes::new())
            }))));
        }

        let store = self.store.clone();
        let extents = entry.extents;
        let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(1);
        tokio::spawn(async move {
            let result = store.read(&extents, Some(from..to)).await;
            let _ = tx.send(result).await;
        });
        Ok(Some(Box::pin(ReceiverStream::new(rx))))
    }

    pub async fn put_bytes(&self, hash_hex: &str, size: i64, data: Bytes) -> Result<()> {
        verify_sha256(&data, hash_hex, size)?;
        if self.cas.get(hash_hex, size).await?.is_some() {
            let entry = self.cas.get(hash_hex, size).await?.unwrap();
            let ids: Vec<_> = {
                let mut ids: Vec<_> = entry.extents.iter().map(|e| e.chunk).collect();
                ids.sort_unstable();
                ids.dedup();
                ids
            };
            self.store.retain(&ids).await?;
            let _ = self.cas.touch(hash_hex, size).await;
            return Ok(());
        }
        let stream = futures::stream::iter(std::iter::once(Ok::<_, anyhow::Error>(data)));
        self.put_stream(hash_hex, size, stream).await
    }

    /// Ingest a body stream into chunked CAS storage (SHA-256 already verified by caller).
    ///
    /// Repeat Write of the same `(hash, size)` → `retain` without uploading again.
    pub async fn put_stream<S>(&self, hash_hex: &str, size: i64, stream: S) -> Result<()>
    where
        S: futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send,
    {
        if let Some(existing) = self.cas.get(hash_hex, size).await? {
            let mut stream = stream;
            while stream.next().await.is_some() {}
            let ids: Vec<_> = {
                let mut ids: Vec<_> = existing.extents.iter().map(|e| e.chunk).collect();
                ids.sort_unstable();
                ids.dedup();
                ids
            };
            self.store.retain(&ids).await?;
            let _ = self.cas.touch(hash_hex, size).await;
            return Ok(());
        }

        let mut opts = IngestOptions::new(self.chunk_size, ChunkCodec::Zstd);
        opts.block_size = 1024 * 1024;
        let ingested = self
            .store
            .ingest(stream, Some(opts))
            .await
            .context("CAS ingest")?;
        if ingested.size != size {
            let ids: Vec<_> = {
                let mut ids: Vec<_> = ingested.extents.iter().map(|e| e.chunk).collect();
                ids.sort_unstable();
                ids.dedup();
                ids
            };
            let _ = self.store.release(&ids).await;
            anyhow::bail!(
                "CAS ingest size {} != digest size {size}",
                ingested.size
            );
        }

        let entry = CasEntry {
            hash: hash_hex.to_string(),
            size,
            extents: ingested.extents.clone(),
        };
        match self.cas.store_or_get_existing(&entry).await? {
            None => Ok(()),
            Some(existing_extents) => {
                // Race: another writer won. Keep their extents; drop ours; retain winner.
                let new_ids: Vec<_> = {
                    let mut ids: Vec<_> = ingested.extents.iter().map(|e| e.chunk).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    ids
                };
                let _ = self.store.release(&new_ids).await;
                let keep: Vec<_> = {
                    let mut ids: Vec<_> = existing_extents.iter().map(|e| e.chunk).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    ids
                };
                self.store.retain(&keep).await?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_chunk_store::{BlobDb, IngestOptions};
    use pigeonhole_storage_memory::MemoryBlobStore;
    use sha2::{Digest as _, Sha256};

    async fn open_cas() -> (CasStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cas_url = format!("sqlite:{}?mode=rwc", dir.path().join("cas.db").display());
        let blob_url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let cas = CasIndex::connect(&cas_url).await.unwrap();
        let db = BlobDb::connect(&blob_url).await.unwrap();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let store = Arc::new(
            ChunkStore::open(db, MemoryBlobStore::new(), opts)
                .await
                .unwrap(),
        );
        (CasStore::new(cas, store), dir)
    }

    #[tokio::test]
    async fn put_get_and_retain_duplicate() {
        let (cas, _dir) = open_cas().await;
        let payload = Bytes::from(vec![9u8; 10_000]);
        let hash = hex::encode(Sha256::digest(&payload));
        let size = payload.len() as i64;
        cas.put_bytes(&hash, size, payload.clone()).await.unwrap();
        let got = cas.get_bytes(&hash, size).await.unwrap().unwrap();
        assert_eq!(got, payload);

        // Duplicate write retains without creating a second logical object.
        cas.put_bytes(&hash, size, payload.clone()).await.unwrap();
        let entry = cas.cas.get(&hash, size).await.unwrap().unwrap();
        assert!(!entry.extents.is_empty());
        let meta = cas
            .store
            .db()
            .chunk_meta(entry.extents[0].chunk)
            .await
            .unwrap()
            .unwrap();
        assert!(meta.2 >= 2, "refs should be >= 2 after retain, got {}", meta.2);
    }
}
