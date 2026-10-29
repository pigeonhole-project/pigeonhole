//! Object-safe erasure of [`BlobBackend`] (stage 1.2).

use crate::typed::{store_id, BlobBackend, BlobLocator, CostHint, InstanceInfo, OpKind, OrderedKey};
use crate::BoxByteStream;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_types::{BackendLimits, ByteRange};
use std::sync::Arc;

/// Object-safe backend used by blob-store / replication: ids as
/// [`BlobLocator`]s, keys as their order-preserving bytes.
#[async_trait]
pub trait DynBlobBackend: Send + Sync + 'static {
    fn instance(&self) -> &InstanceInfo;
    fn limits(&self) -> &BackendLimits;
    fn cost(&self, op: OpKind, id: Option<&BlobLocator>) -> CostHint;
    /// Upload `data` as one blob named `name` (see [`crate::BlobId::name`]).
    async fn put(&self, data: Bytes, name: &str) -> Result<BlobLocator>;
    async fn get(&self, id: &BlobLocator, range: Option<ByteRange>) -> Result<BoxByteStream>;
    async fn delete(&self, keys: &[Vec<u8>]) -> Result<()>;
    /// [`BlobBackend::candidates`] over key bytes.
    async fn candidates(&self, after: Option<&[u8]>, upto: &[u8], limit: usize) -> Result<Vec<Vec<u8>>>;
}

/// A concrete [`BlobBackend`] as a [`DynBlobBackend`].
pub struct Erased<B> {
    inner: B,
}

impl<B> Erased<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }
}

#[async_trait]
impl<B: BlobBackend> DynBlobBackend for Erased<B> {
    fn instance(&self) -> &InstanceInfo {
        self.inner.instance()
    }

    fn limits(&self) -> &BackendLimits {
        self.inner.limits()
    }

    fn cost(&self, op: OpKind, id: Option<&BlobLocator>) -> CostHint {
        let typed = id.and_then(|s| match postcard::from_bytes::<B::Id>(&s.locator) {
            Ok(id) => Some(id),
            Err(e) => {
                tracing::debug!(store = %self.inner.instance().id, error = %e, "cost of a locator that does not decode");
                None
            }
        });
        self.inner.cost(op, typed.as_ref())
    }

    async fn put(&self, data: Bytes, name: &str) -> Result<BlobLocator> {
        let id = self.inner.put(data, name).await?;
        store_id::<B>(&id)
    }

    async fn get(&self, id: &BlobLocator, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let typed: B::Id = postcard::from_bytes(&id.locator)?;
        self.inner.get(&typed, range).await
    }

    async fn delete(&self, keys: &[Vec<u8>]) -> Result<()> {
        let typed = keys.iter().map(|k| B::Key::from_bytes(k)).collect::<Result<Vec<_>>>()?;
        self.inner.delete(&typed).await
    }

    async fn candidates(&self, after: Option<&[u8]>, upto: &[u8], limit: usize) -> Result<Vec<Vec<u8>>> {
        let after = after.map(B::Key::from_bytes).transpose()?;
        let keys = self.inner.candidates(after, B::Key::from_bytes(upto)?, limit).await?;
        Ok(keys.into_iter().map(|k| k.to_bytes()).collect())
    }
}

/// Arc helper.
pub type SharedBackend = Arc<dyn DynBlobBackend>;

pub fn erase<B: BlobBackend>(backend: B) -> Erased<B> {
    Erased::new(backend)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect_stream;
    use crate::typed::OrderedKey;

    /// Minimal in-crate backend for erasure tests (avoids depending on storage-memory).
    struct TinyMem {
        info: InstanceInfo,
        limits: BackendLimits,
        next: std::sync::atomic::AtomicU64,
        data: std::sync::Mutex<std::collections::HashMap<u64, Bytes>>,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct TinyId {
        key: u64,
    }

    impl TinyMem {
        fn new() -> Self {
            Self {
                info: InstanceInfo {
                    id: "tiny".into(),
                    kind: "memory",
                    fingerprint: "tiny".into(),
                    location: "tiny".into(),
                    role: crate::typed::InstanceRole::ReadWrite,
                },
                limits: BackendLimits::memory(),
                next: std::sync::atomic::AtomicU64::new(1),
                data: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl BlobBackend for TinyMem {
        type Id = TinyId;
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
        async fn put(&self, data: Bytes, _name: &str) -> Result<Self::Id> {
            let key = self
                .next
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.data.lock().unwrap().insert(key, data);
            Ok(TinyId { key })
        }
        async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
            let data = self
                .data
                .lock()
                .unwrap()
                .get(&id.key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing"))?;
            let sliced = crate::slice_range(data, range)?;
            Ok(crate::bytes_stream(sliced))
        }
        async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
            let mut g = self.data.lock().unwrap();
            for k in keys {
                g.remove(k);
            }
            Ok(())
        }

        async fn candidates(
            &self,
            after: Option<Self::Key>,
            upto: Self::Key,
            limit: usize,
        ) -> Result<Vec<Self::Key>> {
            let g = self.data.lock().unwrap();
            let mut keys: Vec<u64> = g
                .keys()
                .copied()
                .filter(|&k| k <= upto && after.map(|a| k > a).unwrap_or(true))
                .collect();
            keys.sort_unstable();
            keys.truncate(limit);
            Ok(keys)
        }
    
    }

    #[tokio::test]
    async fn erased_roundtrip() {
        let backend: SharedBackend = Arc::new(erase(TinyMem::new()));
        let id = backend.put(Bytes::from_static(b"hi"), "hi.bin").await.unwrap();
        let got = collect_stream(backend.get(&id, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"hi");
        let upto = u64::MAX.to_bytes();
        let keys = backend.candidates(None, &upto, 10).await.unwrap();
        assert_eq!(keys.len(), 1);
        backend.delete(&keys).await.unwrap();
        assert!(backend.get(&id, None).await.is_err());
    }
}
