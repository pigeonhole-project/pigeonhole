//! In-memory [`LegacyBlobStore`] / [`LegacyBlobStore`] for tests and `memory = true`.

use pigeonhole_blob::{
    bytes_stream, slice_range, store_delete_message, store_get, store_put, LegacyBlobStore,
    BoxByteStream, CostHint, InstanceInfo, InstanceKind, InstanceRole, OpKind, Sweepable,
    BlobBackend, TypedBootstrapPointer,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_types::{
    BackendId, BackendLimits, ByteRange, DeleteOutcome, Locator, PutHint,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

/// Typed identity for the memory backend (stage 1.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryId {
    pub file_id: String,
    pub message_id: u64,
}

/// In-memory store for unit/integration tests (no network).
pub struct MemoryBlobStore {
    id: BackendId,
    limits: BackendLimits,
    instance: InstanceInfo,
    next_msg: AtomicI64,
    next_fid: AtomicU64,
    files: Mutex<HashMap<String, Bytes>>,
    messages: Mutex<HashMap<i64, String>>,
    /// Bootstrap pin payload (TypedBootstrapPointer).
    pin: Mutex<Option<Bytes>>,
}

/// Alias matching Stage 3 naming.
pub type MemoryBackend = MemoryBlobStore;

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self {
            id: BackendId::memory(),
            limits: BackendLimits::memory(),
            instance: InstanceInfo {
                id: "memory".into(),
                kind: InstanceKind::Memory,
                fingerprint: "memory:local".into(),
                location: "memory:local".into(),
                role: InstanceRole::ReadWrite,
            },
            next_msg: AtomicI64::new(1),
            next_fid: AtomicU64::new(1),
            files: Mutex::new(HashMap::new()),
            messages: Mutex::new(HashMap::new()),
            pin: Mutex::new(None),
        }
    }

    pub fn len(&self) -> usize {
        self.files.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Test hook: how many successful `get` calls completed.
    pub fn get_calls(&self) -> u64 {
        // kept for Stage 3.5; count via optional counter later
        0
    }
}

impl Default for MemoryBlobStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LegacyBlobStore for MemoryBlobStore {
    fn id(&self) -> &BackendId {
        &self.id
    }

    fn limits(&self) -> &BackendLimits {
        &self.limits
    }

    async fn put(&self, data: Bytes, _hint: PutHint) -> Result<Locator> {
        if data.is_empty() {
            bail!("refusing empty blob upload (Telegram rejects empty documents)");
        }
        if data.len() > self.limits.max_blob_size {
            bail!(
                "blob size {} exceeds backend max {}",
                data.len(),
                self.limits.max_blob_size
            );
        }
        let file_id = format!("mem-{}", self.next_fid.fetch_add(1, Ordering::Relaxed));
        let message_id = self.next_msg.fetch_add(1, Ordering::Relaxed);
        self.files.lock().unwrap().insert(file_id.clone(), data);
        self.messages
            .lock()
            .unwrap()
            .insert(message_id, file_id.clone());
        Ok(Locator::memory(file_id, message_id))
    }

    async fn get(&self, loc: &Locator, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let file_id = loc
            .file_id()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("memory get requires file_id"))?;
        let data = self
            .files
            .lock()
            .unwrap()
            .get(file_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown file_id {file_id}"))?;
        let sliced = slice_range(data, range)?;
        Ok(bytes_stream(sliced))
    }

    async fn delete(&self, loc: &Locator) -> Result<DeleteOutcome> {
        let message_id = loc
            .message_id()
            .ok_or_else(|| anyhow::anyhow!("memory delete requires message_id"))?;
        let mut messages = self.messages.lock().unwrap();
        let Some(file_id) = messages.remove(&message_id) else {
            return Ok(DeleteOutcome::Gone);
        };
        let still_used = messages.values().any(|f| f == &file_id);
        drop(messages);
        if !still_used {
            self.files.lock().unwrap().remove(&file_id);
        }
        Ok(DeleteOutcome::Deleted)
    }
}

#[async_trait]
impl BlobBackend for MemoryBlobStore {
    type Id = MemoryId;
    type Key = u64;

    fn instance(&self) -> &InstanceInfo {
        &self.instance
    }

    fn limits(&self) -> &BackendLimits {
        &self.limits
    }

    fn key(id: &Self::Id) -> Self::Key {
        id.message_id
    }

    fn cost(&self, _op: OpKind, _id: Option<&Self::Id>) -> CostHint {
        CostHint::free()
    }

    async fn put(&self, data: Bytes) -> Result<Self::Id> {
        let loc = LegacyBlobStore::put(self, data, PutHint::default()).await?;
        match loc {
            Locator::Memory {
                file_id,
                message_id,
            } => Ok(MemoryId {
                file_id,
                message_id: message_id as u64,
            }),
            other => bail!("memory put returned unexpected locator {other:?}"),
        }
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let loc = Locator::memory(&id.file_id, id.message_id as i64);
        LegacyBlobStore::get(self, &loc, range).await
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        for &k in keys {
            let loc = Locator::memory(String::new(), k as i64);
            let _ = LegacyBlobStore::delete(self, &loc).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Sweepable for MemoryBlobStore {
    async fn candidates(
        &self,
        after: Option<Self::Key>,
        upto: Self::Key,
        limit: usize,
    ) -> Result<Vec<Self::Key>> {
        let messages = self.messages.lock().unwrap();
        let mut keys: Vec<u64> = messages
            .keys()
            .copied()
            .map(|m| m as u64)
            .filter(|&k| k <= upto && after.map(|a| k > a).unwrap_or(true))
            .collect();
        keys.sort_unstable();
        keys.truncate(limit);
        Ok(keys)
    }
}

#[async_trait]
impl TypedBootstrapPointer for MemoryBlobStore {
    async fn read(&self) -> Result<Option<Bytes>> {
        Ok(self.pin.lock().unwrap().clone())
    }

    async fn swap(&self, new: Bytes) -> Result<()> {
        *self.pin.lock().unwrap() = Some(new);
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_blob::{collect_stream, LegacyBlobStore};
    use pigeonhole_types::PutHint;

    #[tokio::test]
    async fn memory_put_get_delete() {
        let store = MemoryBlobStore::new();
        let loc = LegacyBlobStore::put(&store, Bytes::from_static(b"hello"), PutHint::new("a.bin", ""))
            .await
            .unwrap();
        let got = collect_stream(LegacyBlobStore::get(&store, &loc, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"hello");
        assert_eq!(
            LegacyBlobStore::delete(&store, &loc).await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(LegacyBlobStore::get(&store, &loc, None).await.is_err());
        assert_eq!(
            LegacyBlobStore::delete(&store, &loc).await.unwrap(),
            DeleteOutcome::Gone
        );
    }

    #[tokio::test]
    async fn memory_rejects_empty_put() {
        let store = MemoryBlobStore::new();
        assert!(LegacyBlobStore::put(&store, Bytes::new(), PutHint::new("empty.bin", ""))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn memory_range_get() {
        let store = MemoryBlobStore::new();
        let loc = LegacyBlobStore::put(
            &store,
            Bytes::from_static(b"abcdefgh"),
            PutHint::new("a.bin", ""),
        )
        .await
        .unwrap();
        let got = collect_stream(LegacyBlobStore::get(&store, &loc, Some(2..5)).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"cde");
    }

    #[tokio::test]
    async fn typed_backend_put_get_sweep() {
        use pigeonhole_blob::{collect_stream, BlobBackend};
        let store = MemoryBlobStore::new();
        let id = BlobBackend::put(&store, Bytes::from_static(b"typed"))
            .await
            .unwrap();
        let got = collect_stream(BlobBackend::get(&store, &id, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"typed");
        let keys = Sweepable::candidates(&store, None, u64::MAX, 10)
            .await
            .unwrap();
        assert!(keys.contains(&id.message_id));
        BlobBackend::delete(&store, &[id.message_id])
            .await
            .unwrap();
        assert!(BlobBackend::get(&store, &id, None).await.is_err());
    }

    #[tokio::test]
    async fn blob_store_adapter_still_works() {
        let store = MemoryBlobStore::new();
        let (fid, mid) = store_put(&store, Bytes::from_static(b"x"), "a", "")
            .await
            .unwrap();
        assert_eq!(store_get(&store, &fid).await.unwrap().as_ref(), b"x");
        assert_eq!(
            store_delete_message(&store, mid).await.unwrap(),
            DeleteOutcome::Deleted
        );
    }
}
