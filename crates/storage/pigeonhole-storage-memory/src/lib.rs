//! In-memory [`BlobBackend`] / [`BlobStore`] for tests and `memory = true`.

use pigeonhole_blob::{
    bytes_stream, slice_range, store_delete_message, store_get, store_put, BlobBackend, BlobStore,
    BoxByteStream,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_types::{
    BackendId, BackendLimits, ByteRange, DeleteOutcome, Locator, PutHint,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

/// In-memory store for unit/integration tests (no network).
pub struct MemoryBlobStore {
    id: BackendId,
    limits: BackendLimits,
    next_msg: AtomicI64,
    next_fid: AtomicU64,
    files: Mutex<HashMap<String, Bytes>>,
    messages: Mutex<HashMap<i64, String>>,
}

/// Alias matching Stage 3 naming.
pub type MemoryBackend = MemoryBlobStore;

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self {
            id: BackendId::memory(),
            limits: BackendLimits::memory(),
            next_msg: AtomicI64::new(1),
            next_fid: AtomicU64::new(1),
            files: Mutex::new(HashMap::new()),
            messages: Mutex::new(HashMap::new()),
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
impl BlobBackend for MemoryBlobStore {
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
impl BlobStore for MemoryBlobStore {
    async fn put(
        &self,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)> {
        store_put(self, data, filename, caption).await
    }

    async fn get(&self, file_id: &str) -> Result<Bytes> {
        store_get(self, file_id).await
    }

    async fn delete_message(&self, message_id: i64) -> Result<DeleteOutcome> {
        store_delete_message(self, message_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_blob::{collect_stream, BlobStore};
    use pigeonhole_types::PutHint;

    #[tokio::test]
    async fn memory_put_get_delete() {
        let store = MemoryBlobStore::new();
        let loc = BlobBackend::put(&store, Bytes::from_static(b"hello"), PutHint::new("a.bin", ""))
            .await
            .unwrap();
        let got = collect_stream(BlobBackend::get(&store, &loc, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"hello");
        assert_eq!(
            BlobBackend::delete(&store, &loc).await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(BlobBackend::get(&store, &loc, None).await.is_err());
        assert_eq!(
            BlobBackend::delete(&store, &loc).await.unwrap(),
            DeleteOutcome::Gone
        );
    }

    #[tokio::test]
    async fn memory_rejects_empty_put() {
        let store = MemoryBlobStore::new();
        assert!(BlobBackend::put(&store, Bytes::new(), PutHint::new("empty.bin", ""))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn memory_range_get() {
        let store = MemoryBlobStore::new();
        let loc = BlobBackend::put(
            &store,
            Bytes::from_static(b"abcdefgh"),
            PutHint::new("a.bin", ""),
        )
        .await
        .unwrap();
        let got = collect_stream(BlobBackend::get(&store, &loc, Some(2..5)).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"cde");
    }

    #[tokio::test]
    async fn blob_store_adapter_still_works() {
        let store = MemoryBlobStore::new();
        let (fid, mid) = BlobStore::put(&store, Bytes::from_static(b"x"), "a", "")
            .await
            .unwrap();
        assert_eq!(BlobStore::get(&store, &fid).await.unwrap().as_ref(), b"x");
        assert_eq!(
            BlobStore::delete_message(&store, mid).await.unwrap(),
            DeleteOutcome::Deleted
        );
    }
}
