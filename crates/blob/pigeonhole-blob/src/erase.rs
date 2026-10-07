//! Object-safe erasure of [`TypedBlobBackend`] (stage 1.2).

use crate::typed::{
    store_id, CostHint, InstanceInfo, OpKind, OrderedKey, StoredId, Sweepable, TypedBlobBackend,
};
use crate::BoxByteStream;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_types::{BackendLimits, ByteRange};
use std::marker::PhantomData;
use std::sync::Arc;

/// Object-safe backend used by blob-store / replication.
#[async_trait]
pub trait DynBackend: Send + Sync + 'static {
    fn instance(&self) -> &InstanceInfo;
    fn limits(&self) -> &BackendLimits;
    fn cost(&self, op: OpKind, id: Option<&StoredId>) -> CostHint;
    async fn put(&self, data: Bytes) -> Result<StoredId>;
    async fn get(&self, id: &StoredId, range: Option<ByteRange>) -> Result<BoxByteStream>;
    async fn delete(&self, keys: &[Vec<u8>]) -> Result<()>;
    /// Optional sweeper registration.
    fn sweeper(&self) -> Option<&dyn DynSweep>;
}

/// Object-safe sweep API.
#[async_trait]
pub trait DynSweep: Send + Sync {
    async fn candidates(
        &self,
        after: Option<&[u8]>,
        upto: &[u8],
        limit: usize,
    ) -> Result<Vec<Vec<u8>>>;
}

/// Wrap a concrete [`TypedBlobBackend`] as [`DynBackend`].
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
impl<B> DynBackend for Erased<B>
where
    B: TypedBlobBackend,
{
    fn instance(&self) -> &InstanceInfo {
        self.inner.instance()
    }

    fn limits(&self) -> &BackendLimits {
        self.inner.limits()
    }

    fn cost(&self, op: OpKind, id: Option<&StoredId>) -> CostHint {
        let typed = id.and_then(|s| serde_json::from_slice::<B::Id>(&s.locator).ok());
        self.inner.cost(op, typed.as_ref())
    }

    async fn put(&self, data: Bytes) -> Result<StoredId> {
        let id = self.inner.put(data).await?;
        store_id::<B>(&id)
    }

    async fn get(&self, id: &StoredId, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let typed: B::Id = serde_json::from_slice(&id.locator)?;
        self.inner.get(&typed, range).await
    }

    async fn delete(&self, keys: &[Vec<u8>]) -> Result<()> {
        let mut typed = Vec::with_capacity(keys.len());
        for k in keys {
            typed.push(B::Key::from_bytes(k)?);
        }
        self.inner.delete(&typed).await
    }

    fn sweeper(&self) -> Option<&dyn DynSweep> {
        None
    }
}

/// Erased backend that also exposes [`Sweepable`].
pub struct ErasedSweep<B> {
    inner: B,
}

impl<B> ErasedSweep<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }
}

#[async_trait]
impl<B> DynBackend for ErasedSweep<B>
where
    B: Sweepable,
{
    fn instance(&self) -> &InstanceInfo {
        self.inner.instance()
    }

    fn limits(&self) -> &BackendLimits {
        self.inner.limits()
    }

    fn cost(&self, op: OpKind, id: Option<&StoredId>) -> CostHint {
        let typed = id.and_then(|s| serde_json::from_slice::<B::Id>(&s.locator).ok());
        self.inner.cost(op, typed.as_ref())
    }

    async fn put(&self, data: Bytes) -> Result<StoredId> {
        let id = TypedBlobBackend::put(&self.inner, data).await?;
        store_id::<B>(&id)
    }

    async fn get(&self, id: &StoredId, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let typed: B::Id = serde_json::from_slice(&id.locator)?;
        TypedBlobBackend::get(&self.inner, &typed, range).await
    }

    async fn delete(&self, keys: &[Vec<u8>]) -> Result<()> {
        let mut typed = Vec::with_capacity(keys.len());
        for k in keys {
            typed.push(B::Key::from_bytes(k)?);
        }
        TypedBlobBackend::delete(&self.inner, &typed).await
    }

    fn sweeper(&self) -> Option<&dyn DynSweep> {
        Some(self)
    }
}

#[async_trait]
impl<B> DynSweep for ErasedSweep<B>
where
    B: Sweepable,
{
    async fn candidates(
        &self,
        after: Option<&[u8]>,
        upto: &[u8],
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let after = match after {
            Some(b) => Some(B::Key::from_bytes(b)?),
            None => None,
        };
        let upto = B::Key::from_bytes(upto)?;
        let keys = self.inner.candidates(after, upto, limit).await?;
        Ok(keys.into_iter().map(|k| k.to_bytes()).collect())
    }
}

/// Arc helper.
pub type SharedBackend = Arc<dyn DynBackend>;

/// Adapt a non-sweep backend; `PhantomData` keeps unused type params quiet in macros.
pub fn erase<B: TypedBlobBackend>(backend: B) -> Erased<B> {
    let _ = PhantomData::<B>;
    Erased::new(backend)
}

pub fn erase_sweep<B: Sweepable>(backend: B) -> ErasedSweep<B> {
    ErasedSweep::new(backend)
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
                    kind: crate::typed::InstanceKind::Memory,
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
    impl TypedBlobBackend for TinyMem {
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
        async fn put(&self, data: Bytes) -> Result<Self::Id> {
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
    }

    #[async_trait]
    impl Sweepable for TinyMem {
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
    async fn erased_sweep_roundtrip() {
        let backend: SharedBackend = Arc::new(erase_sweep(TinyMem::new()));
        let id = backend.put(Bytes::from_static(b"hi")).await.unwrap();
        let got = collect_stream(backend.get(&id, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"hi");
        let sweep = backend.sweeper().expect("sweeper");
        let upto = u64::MAX.to_bytes();
        let keys = sweep.candidates(None, &upto, 10).await.unwrap();
        assert_eq!(keys.len(), 1);
        backend.delete(&keys).await.unwrap();
        assert!(backend.get(&id, None).await.is_err());
    }
}
