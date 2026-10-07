//! In-memory [`BlobBackend`] for tests and `memory = true`.

use pigeonhole_blob::{
    bytes_stream, slice_range, BoxByteStream, CostHint, InstanceInfo, InstanceKind, InstanceRole,
    OpKind, Sweepable, BlobBackend, TypedBootstrapPointer,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_types::{BackendLimits, ByteRange};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Typed identity for the memory backend (stage 1.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryId {
    pub file_id: String,
    pub message_id: u64,
}

/// In-memory store for unit/integration tests (no network).
pub struct MemoryBlobStore {
    limits: BackendLimits,
    instance: InstanceInfo,
    next_msg: AtomicI64,
    next_fid: AtomicU64,
    files: Mutex<HashMap<String, Bytes>>,
    messages: Mutex<HashMap<i64, String>>,
    /// Bootstrap pin payload (TypedBootstrapPointer).
    pin: Mutex<Option<Bytes>>,
    /// When set and true, get/put fail (failover tests).
    unavailable: Option<Arc<std::sync::atomic::AtomicBool>>,
}

/// Alias matching Stage 3 naming.
pub type MemoryBackend = MemoryBlobStore;

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::with_limits(BackendLimits::memory())
    }

    /// In-memory store with a custom blob size limit (tests / multi-instance groups).
    pub fn with_limits(limits: BackendLimits) -> Self {
        Self {
            limits,
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
            unavailable: None,
        }
    }

    /// Override stable instance id (for multi-member Replicated tests).
    pub fn with_instance_id(mut self, id: impl Into<String>) -> Self {
        let id = id.into();
        self.instance.id = id.clone();
        self.instance.fingerprint = format!("memory:{id}");
        self.instance.location = format!("memory:{id}");
        self
    }

    /// Use a pre-resolved [`InstanceInfo`] (from `[[instances]]` / legacy default).
    pub fn with_instance_info(mut self, info: InstanceInfo) -> Self {
        self.instance = info;
        self
    }

    /// Attach a shared unavailable flag for failover tests.
    pub fn with_unavailable_flag(mut self) -> Self {
        self.unavailable = Some(Arc::new(AtomicBool::new(false)));
        self
    }

    pub fn unavailable_flag(&self) -> Arc<AtomicBool> {
        self.unavailable
            .clone()
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)))
    }

    fn check_available(&self) -> Result<()> {
        if self
            .unavailable
            .as_ref()
            .is_some_and(|f| f.load(Ordering::Relaxed))
        {
            bail!("503 service unavailable");
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.files.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Test helper: message sort keys currently held by the store.
    pub fn message_keys(&self) -> Vec<u64> {
        let mut keys: Vec<u64> = self
            .messages
            .lock()
            .unwrap()
            .keys()
            .copied()
            .map(|m| m as u64)
            .collect();
        keys.sort_unstable();
        keys
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
        self.check_available()?;
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
        Ok(MemoryId {
            file_id,
            message_id: message_id as u64,
        })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        self.check_available()?;
        let data = self
            .files
            .lock()
            .unwrap()
            .get(&id.file_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown file_id {}", id.file_id))?;
        let sliced = slice_range(data, range)?;
        Ok(bytes_stream(sliced))
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        for &k in keys {
            let mut messages = self.messages.lock().unwrap();
            let Some(file_id) = messages.remove(&(k as i64)) else {
                continue;
            };
            let still_used = messages.values().any(|f| f == &file_id);
            drop(messages);
            if !still_used {
                self.files.lock().unwrap().remove(&file_id);
            }
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
    use pigeonhole_blob::collect_stream;

    #[tokio::test]
    async fn memory_put_get_delete() {
        let store = MemoryBlobStore::new();
        let id = BlobBackend::put(&store, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = collect_stream(BlobBackend::get(&store, &id, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"hello");
        BlobBackend::delete(&store, &[id.message_id]).await.unwrap();
        assert!(BlobBackend::get(&store, &id, None).await.is_err());
        // Repeat delete is ok.
        BlobBackend::delete(&store, &[id.message_id]).await.unwrap();
    }

    #[tokio::test]
    async fn memory_rejects_empty_put() {
        let store = MemoryBlobStore::new();
        assert!(BlobBackend::put(&store, Bytes::new()).await.is_err());
    }

    #[tokio::test]
    async fn memory_range_get() {
        let store = MemoryBlobStore::new();
        let id = BlobBackend::put(&store, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        let got = collect_stream(BlobBackend::get(&store, &id, Some(2..5)).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"cde");
    }

    #[tokio::test]
    async fn typed_backend_put_get_sweep() {
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
}
