//! In-memory [`BlobBackend`] for tests and `memory = true`.

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::StoreLimiter;
use pigeonhole_storage_api::{
    bytes_stream, slice_range, After, BlobBackend, BoxByteStream, Chain, CostHint, InstanceInfo, InstanceRole, Linked,
    NotFound, OpKind, Unavailable,
};
use pigeonhole_types::{BackendLimits, ByteRange};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// What [`InstanceInfo::kind`](pigeonhole_storage_api::InstanceInfo::kind) says of this store.
pub const KIND: &str = "memory";

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
    /// Message id → the name the blob was uploaded under.
    names: Mutex<HashMap<i64, String>>,
    /// When set and true, get/put fail (failover tests).
    unavailable: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// The chain ([`Chain`]): successor of each message (`None`: the start).
    chain: Mutex<ChainLinks>,
    /// Deletes wait while [`Self::hold_deletes`]' guard lives (tests of
    /// what must not wait for a delete).
    delete_gate: Arc<tokio::sync::RwLock<()>>,
    /// Every operation in flight; [`Chain::prune`] runs only when idle.
    limiter: Arc<StoreLimiter>,
}

/// Keys [`Chain::prune`] deletes per idle moment.
const PRUNE_BATCH: usize = 100;

#[derive(Default)]
struct ChainLinks {
    next: HashMap<Option<i64>, i64>,
    tail: Option<i64>,
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
                kind: crate::KIND,
                fingerprint: "memory:local".into(),
                location: "memory:local".into(),
                role: InstanceRole::ReadWrite,
            },
            next_msg: AtomicI64::new(1),
            next_fid: AtomicU64::new(1),
            files: Mutex::new(HashMap::new()),
            messages: Mutex::new(HashMap::new()),
            names: Mutex::new(HashMap::new()),
            unavailable: None,
            chain: Mutex::default(),
            delete_gate: Arc::default(),
            limiter: Arc::new(StoreLimiter::new("memory")),
        }
    }

    /// At most `rate_per_sec` operations a second (`burst` at once).
    pub fn with_rate(mut self, rate_per_sec: f64, burst: f64) -> Self {
        self.limiter =
            Arc::new(StoreLimiter::new(&self.instance.id).with_rate(pigeonhole_limits::Rate::new(rate_per_sec, burst)));
        self
    }

    pub fn limiter(&self) -> &Arc<StoreLimiter> {
        &self.limiter
    }

    /// Override stable instance id (for multi-member Replicated tests).
    pub fn with_instance_id(mut self, id: impl Into<String>) -> Self {
        let id = id.into();
        self.instance.id = id.clone();
        self.instance.fingerprint = format!("memory:{id}");
        self.instance.location = format!("memory:{id}");
        self.limiter = Arc::new(StoreLimiter::new(&id));
        self
    }

    /// Use a pre-resolved [`InstanceInfo`] (from `[[store]]`).
    pub fn with_instance_info(mut self, info: InstanceInfo) -> Self {
        self.limiter = Arc::new(StoreLimiter::new(&info.id));
        self.instance = info;
        self
    }

    /// Attach a shared unavailable flag for failover tests.
    pub fn with_unavailable_flag(mut self) -> Self {
        self.unavailable = Some(Arc::new(AtomicBool::new(false)));
        self
    }

    /// Until the guard drops, every delete waits.
    pub fn hold_deletes(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.delete_gate.clone().try_write_owned().expect("deletes held twice")
    }

    pub fn unavailable_flag(&self) -> Arc<AtomicBool> {
        self.unavailable.clone().unwrap_or_else(|| Arc::new(AtomicBool::new(false)))
    }

    fn check_available(&self) -> Result<()> {
        if self.unavailable.as_ref().is_some_and(|f| f.load(Ordering::Relaxed)) {
            return Err(Unavailable(self.instance.id.clone()).into());
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
    /// The name the message's blob was uploaded under.
    pub fn name_of(&self, message_id: u64) -> Option<String> {
        self.names.lock().unwrap().get(&(message_id as i64)).cloned()
    }

    pub fn message_keys(&self) -> Vec<u64> {
        let mut keys: Vec<u64> = self.messages.lock().unwrap().keys().copied().map(|m| m as u64).collect();
        keys.sort_unstable();
        keys
    }

    /// The id of message `message_id`, with the name it was put under.
    fn linked(&self, message_id: i64) -> Linked<MemoryId> {
        let file_id = self.messages.lock().unwrap().get(&message_id).cloned().unwrap_or_default();
        let label = self.names.lock().unwrap().get(&message_id).cloned().unwrap_or_default();
        Linked { id: MemoryId { file_id, message_id: message_id as u64 }, label }
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

    async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
        let _op = self.limiter.op().await;
        self.check_available()?;
        if data.is_empty() {
            bail!("refusing empty blob upload (Telegram rejects empty documents)");
        }
        if data.len() > self.limits.max_blob_size {
            bail!("blob size {} exceeds backend max {}", data.len(), self.limits.max_blob_size);
        }
        let file_id = format!("mem-{}", self.next_fid.fetch_add(1, Ordering::Relaxed));
        let message_id = self.next_msg.fetch_add(1, Ordering::Relaxed);
        self.files.lock().unwrap().insert(file_id.clone(), data);
        self.messages.lock().unwrap().insert(message_id, file_id.clone());
        self.names.lock().unwrap().insert(message_id, name.to_string());
        tracing::debug!(store = %self.instance.id, message_id = message_id, name, bytes = self.files.lock().unwrap().get(&file_id).map_or(0, |d| d.len()), "stored");
        Ok(MemoryId { file_id, message_id: message_id as u64 })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let _op = self.limiter.op().await;
        self.check_available()?;
        let data = self
            .files
            .lock()
            .unwrap()
            .get(&id.file_id)
            .cloned()
            .ok_or_else(|| NotFound(format!("blob {}", id.file_id)))?;
        let sliced = slice_range(data, range)?;
        Ok(bytes_stream(sliced))
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        let _gate = self.delete_gate.read().await;
        let _op = self.limiter.op().await;
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

    async fn candidates(&self, after: Option<Self::Key>, upto: Self::Key, limit: usize) -> Result<Vec<Self::Key>> {
        let _op = self.limiter.op().await;
        let messages = self.messages.lock().unwrap();
        let mut keys: Vec<u64> =
            messages.keys().copied().map(|m| m as u64).filter(|&k| k <= upto && after.is_none_or(|a| k > a)).collect();
        keys.sort_unstable();
        keys.truncate(limit);
        Ok(keys)
    }
}

/// The successor of a message is decided under a lock: the first to link wins.
#[async_trait]
impl Chain for MemoryBlobStore {
    async fn put_after(&self, prev: Option<&Self::Id>, label: &str, data: Bytes) -> Result<After<Self::Id>> {
        let _op = self.limiter.op().await;
        let prev = prev.map(|p| p.message_id as i64);
        if let Some(&winner) = self.chain.lock().unwrap().next.get(&prev) {
            return Ok(After::Lost(self.linked(winner)));
        }
        let id = self.put(data, label).await?;
        let mine = id.message_id as i64;
        let mut chain = self.chain.lock().unwrap();
        if let Some(&winner) = chain.next.get(&prev) {
            drop(chain);
            return Ok(After::Lost(self.linked(winner)));
        }
        chain.next.insert(prev, mine);
        chain.tail = Some(mine);
        tracing::debug!(store = %self.instance.id, label, ?prev, message_id = mine, "linked");
        Ok(After::Won(id))
    }

    async fn tail(&self) -> Result<Option<Linked<Self::Id>>> {
        let _op = self.limiter.op().await;
        self.check_available()?;
        let tail = self.chain.lock().unwrap().tail;
        Ok(tail.map(|t| self.linked(t)))
    }

    async fn successor(&self, id: &Self::Id) -> Result<Option<Linked<Self::Id>>> {
        let _op = self.limiter.op().await;
        self.check_available()?;
        let next = self.chain.lock().unwrap().next.get(&Some(id.message_id as i64)).copied();
        Ok(next.map(|n| self.linked(n)))
    }

    /// Background work: a batch at a time, each when no other operation runs.
    async fn prune(&self, below: &Self::Id) -> Result<()> {
        let below = below.message_id;
        let doomed: Vec<u64> = self.message_keys().into_iter().filter(|&k| k < below).collect();
        for batch in doomed.chunks(PRUNE_BATCH) {
            self.limiter.wait_idle().await;
            BlobBackend::delete(self, batch).await?;
            let mut names = self.names.lock().unwrap();
            for k in batch {
                names.remove(&(*k as i64));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_storage_api::collect_stream;

    #[tokio::test]
    async fn memory_put_get_delete() {
        let store = MemoryBlobStore::new();
        let id = BlobBackend::put(&store, Bytes::from_static(b"hello"), "t.bin").await.unwrap();
        let got = collect_stream(BlobBackend::get(&store, &id, None).await.unwrap()).await.unwrap();
        assert_eq!(got.as_ref(), b"hello");
        BlobBackend::delete(&store, &[id.message_id]).await.unwrap();
        let gone = BlobBackend::get(&store, &id, None).await;
        assert_eq!(
            pigeonhole_storage_api::Failure::of(&gone.err().unwrap()),
            pigeonhole_storage_api::Failure::NotFound
        );
        // Repeat delete is ok.
        BlobBackend::delete(&store, &[id.message_id]).await.unwrap();
    }

    #[tokio::test]
    async fn the_first_successor_wins() {
        let store = MemoryBlobStore::new();
        assert_eq!(store.tail().await.unwrap(), None);
        let After::Won(a) = store.put_after(None, "a", Bytes::from_static(b"a")).await.unwrap() else {
            panic!("the first blob of a chain wins")
        };
        let After::Lost(w) = store.put_after(None, "a2", Bytes::from_static(b"x")).await.unwrap() else {
            panic!("a second start loses")
        };
        assert_eq!((w.id.message_id, w.label.as_str()), (a.message_id, "a"));
        let After::Won(b) = store.put_after(Some(&a), "b", Bytes::from_static(b"b")).await.unwrap() else {
            panic!("a successor of the tail wins")
        };
        assert!(matches!(store.put_after(Some(&a), "c", Bytes::from_static(b"c")).await.unwrap(), After::Lost(_)));
        assert_eq!(store.tail().await.unwrap().map(|t| t.id), Some(b.clone()));
        assert_eq!(store.successor(&a).await.unwrap().map(|s| s.label), Some("b".into()));
        assert_eq!(store.successor(&b).await.unwrap(), None);

        store.prune(&b).await.unwrap();
        assert!(BlobBackend::get(&store, &a, None).await.is_err(), "below the prune point");
        assert!(BlobBackend::get(&store, &b, None).await.is_ok());
        assert_eq!(store.tail().await.unwrap().map(|t| t.label), Some("b".into()));
    }

    #[tokio::test]
    async fn prune_waits_while_another_operation_runs() {
        let store = Arc::new(MemoryBlobStore::new());
        let mut last = None;
        for i in 0..3 {
            last = Some(BlobBackend::put(store.as_ref(), Bytes::from(vec![i + 1]), "b").await.unwrap());
        }
        let below = last.unwrap();
        let op = store.limiter().op().await;
        let prune = {
            let (store, below) = (store.clone(), below.clone());
            tokio::spawn(async move { Chain::prune(store.as_ref(), &below).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert_eq!(store.message_keys().len(), 3, "pruned while an operation was in flight");
        drop(op);
        prune.await.unwrap().unwrap();
        assert_eq!(store.message_keys(), vec![below.message_id]);
    }

    #[tokio::test]
    async fn memory_rejects_empty_put() {
        let store = MemoryBlobStore::new();
        assert!(BlobBackend::put(&store, Bytes::new(), "t.bin").await.is_err());
    }

    #[tokio::test]
    async fn memory_range_get() {
        let store = MemoryBlobStore::new();
        let id = BlobBackend::put(&store, Bytes::from_static(b"abcdefgh"), "t.bin").await.unwrap();
        let got = collect_stream(BlobBackend::get(&store, &id, Some(2..5)).await.unwrap()).await.unwrap();
        assert_eq!(got.as_ref(), b"cde");
    }

    #[tokio::test]
    async fn typed_backend_put_get_sweep() {
        let store = MemoryBlobStore::new();
        let id = BlobBackend::put(&store, Bytes::from_static(b"typed"), "t.bin").await.unwrap();
        let got = collect_stream(BlobBackend::get(&store, &id, None).await.unwrap()).await.unwrap();
        assert_eq!(got.as_ref(), b"typed");
        let keys = BlobBackend::candidates(&store, None, u64::MAX, 10).await.unwrap();
        assert!(keys.contains(&id.message_id));
        BlobBackend::delete(&store, &[id.message_id]).await.unwrap();
        assert!(BlobBackend::get(&store, &id, None).await.is_err());
    }
}
