use crate::digest::{digest_hash_hex, verify_sha256};
use anyhow::{Context, Result};
use bytes::Bytes;
use pigeonhole_blob_store::{BlobStore, Index};
use std::sync::Arc;

/// Store and fetch CAS payloads through the index + [`BlobStore`].
#[derive(Clone)]
pub struct CasStore {
    pub index: Index,
    pub store: Arc<dyn BlobStore>,
    pub chat_id: String,
}

impl CasStore {
    pub fn new(index: Index, store: Arc<dyn BlobStore>, chat_id: String) -> Self {
        Self {
            index,
            store,
            chat_id,
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
        let Some(file_id) = self.index.cas_lookup(hash_hex, size).await? else {
            return Ok(None);
        };
        let data = self
            .store
            .get(&file_id)
            .await
            .with_context(|| format!("blob get {file_id}"))?;
        if data.len() as i64 != size {
            anyhow::bail!("stored blob size mismatch for {hash_hex}/{size}");
        }
        let _ = self.index.cas_touch(hash_hex, size).await;
        Ok(Some(data))
    }

    pub async fn put_bytes(
        &self,
        hash_hex: &str,
        size: i64,
        data: Bytes,
    ) -> Result<()> {
        verify_sha256(&data, hash_hex, size)?;
        if self.index.cas_lookup(hash_hex, size).await?.is_some() {
            let _ = self.index.cas_touch(hash_hex, size).await;
            return Ok(());
        }
        let filename = format!("cas-{hash_hex}");
        let (file_id, message_id) = self
            .store
            .put(data.clone(), &filename, "")
            .await
            .context("cas blob put")?;
        self.index
            .cas_store_new_blob(
                hash_hex,
                size,
                &file_id,
                message_id,
                data.len() as i64,
                &self.chat_id,
                None,
            )
            .await?;
        Ok(())
    }
}
