//! Pack encoded blocks into instance-sized blob parts (stage D.1).

use crate::erase::DynBlobBackend;
use crate::typed::BlobLocator;
use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;

/// One compressed/encoded block ready to pack into a blob part.
#[derive(Debug, Clone)]
pub struct EncodedBlock {
    pub stored: Bytes,
    pub logical_len: u32,
    pub codec: String,
}

/// Result of uploading one part (one blob / message).
#[derive(Debug, Clone)]
pub struct PartUploaded {
    pub first_block: u32,
    pub block_count: u32,
    pub locator: BlobLocator,
    /// Per-block stored lengths inside this part (for Range reads).
    pub block_stored_lens: Vec<u32>,
}

/// Accumulates encoded blocks into blobs respecting `max_blob_size`.
///
/// Keeps at most one open (unfilled) part in memory.
pub struct PartPacker {
    backend: Arc<dyn DynBlobBackend>,
    max_blob_size: usize,
    /// Absolute block index of the next block to accept.
    next_block: u32,
    open_first: u32,
    open_stored: Vec<Bytes>,
    open_lens: Vec<u32>,
    open_total: usize,
}

impl PartPacker {
    pub fn new(backend: Arc<dyn DynBlobBackend>) -> Self {
        let max_blob_size = backend.limits().max_blob_size;
        Self {
            backend,
            max_blob_size,
            next_block: 0,
            open_first: 0,
            open_stored: Vec::new(),
            open_lens: Vec::new(),
            open_total: 0,
        }
    }

    pub fn backend(&self) -> &Arc<dyn DynBlobBackend> {
        &self.backend
    }

    pub fn max_blob_size(&self) -> usize {
        self.max_blob_size
    }

    pub fn next_block(&self) -> u32 {
        self.next_block
    }

    /// Push one block. Returns a sealed part when the open part was flushed.
    pub async fn push(&mut self, block: EncodedBlock) -> Result<Option<PartUploaded>> {
        let stored_len = block.stored.len();
        if stored_len > self.max_blob_size {
            bail!(
                "configuration error: encoded block {} bytes exceeds instance max_blob_size {}",
                stored_len,
                self.max_blob_size
            );
        }

        let mut sealed = None;
        if !self.open_stored.is_empty() && self.open_total + stored_len > self.max_blob_size {
            sealed = Some(self.flush_open().await?);
        }

        if self.open_stored.is_empty() {
            self.open_first = self.next_block;
        }
        self.open_stored.push(block.stored);
        self.open_lens.push(stored_len as u32);
        self.open_total += stored_len;
        self.next_block = self.next_block.saturating_add(1);
        Ok(sealed)
    }

    /// Flush the last open part, if any.
    pub async fn finish(mut self) -> Result<Option<PartUploaded>> {
        if self.open_stored.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.flush_open().await?))
    }

    async fn flush_open(&mut self) -> Result<PartUploaded> {
        debug_assert!(!self.open_stored.is_empty());
        let payload = concat_bytes(std::mem::take(&mut self.open_stored), self.open_total);
        let lenses = std::mem::take(&mut self.open_lens);
        let first_block = self.open_first;
        let block_count = lenses.len() as u32;
        self.open_total = 0;

        let locator = self
            .backend
            .put(payload)
            .await
            .with_context(|| {
                format!(
                    "part put failed for instance {} (blocks {}..{})",
                    self.backend.instance().id,
                    first_block,
                    first_block + block_count
                )
            })?;

        Ok(PartUploaded {
            first_block,
            block_count,
            locator,
            block_stored_lens: lenses,
        })
    }
}

fn concat_bytes(parts: Vec<Bytes>, total: usize) -> Bytes {
    if parts.len() == 1 {
        return parts.into_iter().next().unwrap();
    }
    let mut buf = BytesMut::with_capacity(total);
    for p in parts {
        buf.extend_from_slice(&p);
    }
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erase::{erase, SharedBackend};
    use crate::typed::{CostHint, InstanceInfo, InstanceKind, InstanceRole, OpKind, BlobBackend};
    use crate::{bytes_stream, collect_stream, slice_range, BoxByteStream};
    use async_trait::async_trait;
    use pigeonhole_types::{BackendLimits, ByteRange};
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct LimMem {
        info: InstanceInfo,
        limits: BackendLimits,
        next: std::sync::atomic::AtomicU64,
        data: Mutex<HashMap<u64, Bytes>>,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Id {
        key: u64,
    }

    impl LimMem {
        fn new(id: &str, max_blob_size: usize) -> Self {
            Self {
                info: InstanceInfo {
                    id: id.into(),
                    kind: InstanceKind::Memory,
                    fingerprint: format!("memory:{id}"),
                    location: format!("memory:{id}"),
                    role: InstanceRole::ReadWrite,
                },
                limits: BackendLimits {
                    max_blob_size,
                    supports_range: pigeonhole_types::RangeSupport::BestEffort,
                    can_list: false,
                },
                next: std::sync::atomic::AtomicU64::new(1),
                data: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl BlobBackend for LimMem {
        type Id = Id;
        type Key = u64;
        fn instance(&self) -> &InstanceInfo {
            &self.info
        }
        fn limits(&self) -> &BackendLimits {
            &self.limits
        }
        fn key(id: &Self::Id) -> Self::Key {
            id.key
        }
        fn cost(&self, _: OpKind, _: Option<&Self::Id>) -> CostHint {
            CostHint::free()
        }
        async fn put(&self, data: Bytes) -> Result<Self::Id> {
            if data.len() > self.limits.max_blob_size {
                bail!("oversize");
            }
            let key = self
                .next
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.data.lock().unwrap().insert(key, data);
            Ok(Id { key })
        }
        async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
            let data = self
                .data
                .lock()
                .unwrap()
                .get(&id.key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing"))?;
            Ok(bytes_stream(slice_range(data, range)?))
        }
        async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
            let mut g = self.data.lock().unwrap();
            for k in keys {
                g.remove(k);
            }
            Ok(())
        }
    }

    fn block(n: usize) -> EncodedBlock {
        EncodedBlock {
            stored: Bytes::from(vec![7u8; n]),
            logical_len: n as u32,
            codec: "raw".into(),
        }
    }

    #[tokio::test]
    async fn packs_until_max_then_puts() {
        let backend: SharedBackend = Arc::new(erase(LimMem::new("a", 100)));
        let mut p = PartPacker::new(backend.clone());
        assert!(p.push(block(40)).await.unwrap().is_none());
        assert!(p.push(block(40)).await.unwrap().is_none());
        let sealed = p.push(block(40)).await.unwrap().expect("flush");
        assert_eq!(sealed.first_block, 0);
        assert_eq!(sealed.block_count, 2);
        assert_eq!(sealed.block_stored_lens, vec![40, 40]);
        let last = p.finish().await.unwrap().expect("last");
        assert_eq!(last.first_block, 2);
        assert_eq!(last.block_count, 1);
        let got = collect_stream(backend.get(&last.locator, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.len(), 40);
    }

    #[tokio::test]
    async fn oversized_block_is_config_error() {
        let backend: SharedBackend = Arc::new(erase(LimMem::new("a", 50)));
        let mut p = PartPacker::new(backend);
        let err = p.push(block(51)).await.unwrap_err();
        assert!(
            err.to_string().contains("configuration error"),
            "{err}"
        );
    }
}
