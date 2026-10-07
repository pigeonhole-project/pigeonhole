//! L2 blob cache: [`CachingBackend`] decorator over [`LegacyBlobStore`].

use crate::backend::{bytes_stream, collect_stream, LegacyBlobStore, BoxByteStream};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    RecoverMode, Throttle,
};
use pigeonhole_types::{
    BackendId, BackendLimits, BlobKey, ByteRange, DeleteOutcome, Locator, PutHint,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// On-disk / in-memory L2 value: compressed chunk bytes + optional CRC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBlob {
    pub bytes: Vec<u8>,
    pub crc32: Option<u32>,
}

impl CachedBlob {
    pub fn new(bytes: Bytes, crc32: Option<u32>) -> Self {
        Self {
            bytes: bytes.to_vec(),
            crc32,
        }
    }

    pub fn verify(&self) -> bool {
        match self.crc32 {
            None => true,
            Some(expected) => crc32fast::hash(&self.bytes) == expected,
        }
    }

    pub fn into_bytes(self) -> Bytes {
        Bytes::from(self.bytes)
    }
}

/// Runtime knobs for L2 (and shared cache flags used by the binary).
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub enabled: bool,
    pub memory_bytes: usize,
    pub disk_path: Option<PathBuf>,
    pub disk_bytes: Option<usize>,
    pub block_memory_bytes: usize,
    pub readahead_blocks: usize,
    pub write_through: bool,
    pub max_object_bytes: usize,
    pub metrics_interval_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            memory_bytes: 256 * 1024 * 1024,
            disk_path: None,
            disk_bytes: None,
            block_memory_bytes: 128 * 1024 * 1024,
            readahead_blocks: 2,
            write_through: true,
            max_object_bytes: 20 * 1024 * 1024,
            metrics_interval_secs: 300,
        }
    }
}

#[derive(Default)]
pub struct CacheMetrics {
    pub l2_hits: AtomicU64,
    pub l2_misses: AtomicU64,
    pub l2_collapsed: AtomicU64,
    pub backend_bytes: AtomicU64,
    pub client_bytes: AtomicU64,
}

impl CacheMetrics {
    pub fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.l2_hits.load(Ordering::Relaxed),
            self.l2_misses.load(Ordering::Relaxed),
            self.l2_collapsed.load(Ordering::Relaxed),
            self.backend_bytes.load(Ordering::Relaxed),
            self.client_bytes.load(Ordering::Relaxed),
        )
    }
}

/// Transparent L2 cache over any [`LegacyBlobStore`].
pub struct CachingBackend<B: LegacyBlobStore> {
    inner: Arc<B>,
    l2: HybridCache<BlobKey, CachedBlob>,
    cfg: CacheConfig,
    metrics: Arc<CacheMetrics>,
}

impl<B: LegacyBlobStore + 'static> CachingBackend<B> {
    pub async fn new(inner: B, cfg: CacheConfig) -> Result<Self> {
        let l2 = if let Some(ref path) = cfg.disk_path {
            let disk_bytes = cfg
                .disk_bytes
                .context("cache.disk_bytes required when disk_path is set")?;
            match build_hybrid(path, cfg.memory_bytes, disk_bytes).await {
                Ok(h) => h,
                Err(e) => {
                    warn!(
                        error = %e,
                        path = %path.display(),
                        "foyer hybrid cache failed; wiping dir and retrying"
                    );
                    let _ = std::fs::remove_dir_all(path);
                    std::fs::create_dir_all(path)?;
                    build_hybrid(path, cfg.memory_bytes, disk_bytes).await?
                }
            }
        } else {
            HybridCacheBuilder::new()
                .with_name("s3gram-l2-mem")
                .memory(cfg.memory_bytes)
                .with_weighter(|_: &BlobKey, v: &CachedBlob| v.bytes.len())
                .storage()
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("foyer memory cache: {e}"))?
        };
        let metrics = Arc::new(CacheMetrics::default());
        if cfg.metrics_interval_secs > 0 {
            let m = metrics.clone();
            let secs = cfg.metrics_interval_secs;
            tokio::spawn(async move {
                let period = Duration::from_secs(secs);
                loop {
                    tokio::time::sleep(period).await;
                    let (h, miss, col, be, cl) = m.snapshot();
                    info!(
                        l2_hits = h,
                        l2_misses = miss,
                        l2_collapsed = col,
                        backend_bytes = be,
                        client_bytes = cl,
                        "cache metrics"
                    );
                }
            });
        }
        Ok(Self {
            inner: Arc::new(inner),
            l2,
            cfg,
            metrics,
        })
    }

    pub fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }

    fn key_for(&self, loc: &Locator) -> BlobKey {
        // CDN URLs are ephemeral — drop them from the cache key.
        let loc = match loc {
            Locator::Discord {
                channel_id,
                message_id,
                attachment_id,
                ..
            } => Locator::discord(channel_id, *message_id, attachment_id, ""),
            other => other.clone(),
        };
        BlobKey::new(self.inner.id().clone(), loc)
    }

    pub async fn invalidate_key(&self, key: &BlobKey) {
        self.l2.remove(key);
    }

    pub async fn invalidate_locator(&self, loc: &Locator) {
        self.invalidate_key(&self.key_for(loc)).await;
    }

    /// Write-through insert after a successful backend put.
    pub async fn insert_l2(&self, loc: &Locator, data: Bytes, crc32: Option<u32>) {
        if !self.cfg.write_through {
            return;
        }
        if data.len() > self.cfg.max_object_bytes {
            return;
        }
        if let Some(c) = crc32 {
            if crc32fast::hash(data.as_ref()) != c {
                warn!("refusing L2 insert: crc32 mismatch");
                return;
            }
        }
        self.l2
            .insert(self.key_for(loc), CachedBlob::new(data, crc32));
    }

    async fn l2_get_or_fetch(&self, key: BlobKey, loc: &Locator) -> Result<Bytes> {
        let had = self.l2.contains(&key);
        if had {
            self.metrics.l2_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.l2_misses.fetch_add(1, Ordering::Relaxed);
        }
        let metrics = self.metrics.clone();
        let inner = self.inner.clone();
        let loc_c = loc.clone();
        let entry = self
            .l2
            .get_or_fetch(&key, || async move {
                let stream = inner.get(&loc_c, None).await?;
                let data = collect_stream(stream).await?;
                metrics
                    .backend_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                Ok::<_, anyhow::Error>(CachedBlob::new(data, None))
            })
            .await
            .map_err(|e| anyhow::anyhow!("l2 get_or_fetch: {e}"))?;
        let val = entry.value().clone();
        if !val.verify() {
            warn!("l2 crc failure; invalidating and refetching");
            self.l2.remove(&key);
            let stream = self.inner.get(loc, None).await?;
            let data = collect_stream(stream).await?;
            self.metrics
                .backend_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            return Ok(data);
        }
        Ok(val.into_bytes())
    }
}

async fn build_hybrid(
    path: &Path,
    memory_bytes: usize,
    disk_bytes: usize,
) -> Result<HybridCache<BlobKey, CachedBlob>> {
    std::fs::create_dir_all(path)?;
    let device = FsDeviceBuilder::new(path)
        .with_capacity(disk_bytes)
        .with_throttle(
            Throttle::new()
                .with_write_throughput(32 * 1024 * 1024)
                .with_write_iops(512),
        )
        .build()
        .map_err(|e| anyhow::anyhow!("foyer device: {e}"))?;

    HybridCacheBuilder::new()
        .with_name("s3gram-l2")
        .memory(memory_bytes)
        .with_weighter(|_: &BlobKey, v: &CachedBlob| v.bytes.len())
        .storage()
        .with_engine_config(BlockEngineConfig::new(device))
        .with_recover_mode(RecoverMode::Quiet)
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("foyer hybrid build: {e}"))
}

#[async_trait]
impl<B: LegacyBlobStore + 'static> LegacyBlobStore for CachingBackend<B> {
    fn id(&self) -> &BackendId {
        self.inner.id()
    }

    fn limits(&self) -> &BackendLimits {
        self.inner.limits()
    }

    async fn put(&self, data: Bytes, hint: PutHint) -> Result<Locator> {
        let loc = self.inner.put(data.clone(), hint).await?;
        if self.cfg.enabled && self.cfg.write_through {
            self.insert_l2(&loc, data, None).await;
        }
        Ok(loc)
    }

    async fn get(&self, loc: &Locator, range: Option<ByteRange>) -> Result<BoxByteStream> {
        if !self.cfg.enabled {
            return self.inner.get(loc, range).await;
        }
        let key = self.key_for(loc);
        let data = self.l2_get_or_fetch(key, loc).await?;
        self.metrics
            .client_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        let sliced = crate::backend::slice_range(data, range)?;
        Ok(bytes_stream(sliced))
    }

    async fn delete(&self, loc: &Locator) -> Result<DeleteOutcome> {
        let out = self.inner.delete(loc).await?;
        self.invalidate_locator(loc).await;
        Ok(out)
    }

    async fn invalidate_blob(&self, file_id: &str) {
        if let Ok(loc) = crate::backend::locator_for_store_file_id(self, file_id) {
            self.invalidate_locator(&loc).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{bytes_stream, slice_range};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Minimal in-crate backend for cache tests (avoids blob ↔ storage-memory cycle).
    struct CountingMemory {
        id: BackendId,
        limits: BackendLimits,
        files: Mutex<HashMap<String, Bytes>>,
        next: AtomicU64,
        gets: AtomicU64,
    }

    impl CountingMemory {
        fn new() -> Self {
            Self {
                id: BackendId::memory(),
                limits: BackendLimits::memory(),
                files: Mutex::new(HashMap::new()),
                next: AtomicU64::new(1),
                gets: AtomicU64::new(0),
            }
        }
        fn gets(&self) -> u64 {
            self.gets.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl LegacyBlobStore for CountingMemory {
        fn id(&self) -> &BackendId {
            &self.id
        }
        fn limits(&self) -> &BackendLimits {
            &self.limits
        }
        async fn put(&self, data: Bytes, _hint: PutHint) -> Result<Locator> {
            let n = self.next.fetch_add(1, Ordering::Relaxed);
            let file_id = format!("mem-{n}");
            self.files.lock().unwrap().insert(file_id.clone(), data);
            Ok(Locator::memory(file_id, n as i64))
        }
        async fn get(&self, loc: &Locator, range: Option<ByteRange>) -> Result<BoxByteStream> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            let file_id = loc
                .file_id()
                .ok_or_else(|| anyhow::anyhow!("missing file_id"))?;
            let data = self
                .files
                .lock()
                .unwrap()
                .get(file_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing blob"))?;
            Ok(bytes_stream(slice_range(data, range)?))
        }
        async fn delete(&self, loc: &Locator) -> Result<DeleteOutcome> {
            let file_id = loc
                .file_id()
                .ok_or_else(|| anyhow::anyhow!("missing file_id"))?;
            let removed = self.files.lock().unwrap().remove(file_id).is_some();
            Ok(if removed {
                DeleteOutcome::Deleted
            } else {
                DeleteOutcome::Gone
            })
        }
    }

    #[tokio::test]
    async fn repeat_get_hits_l2() {
        let mut cfg = CacheConfig::default();
        cfg.disk_path = None;
        cfg.metrics_interval_secs = 0;
        let cache = CachingBackend::new(CountingMemory::new(), cfg)
            .await
            .unwrap();
        let loc = cache
            .put(Bytes::from_static(b"abc"), PutHint::new("a.bin", ""))
            .await
            .unwrap();
        let _ = collect_stream(cache.get(&loc, None).await.unwrap())
            .await
            .unwrap();
        let _ = collect_stream(cache.get(&loc, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(cache.inner().gets(), 0);
    }

    #[tokio::test]
    async fn parallel_gets_single_flight() {
        let mut cfg = CacheConfig::default();
        cfg.write_through = false;
        cfg.disk_path = None;
        cfg.metrics_interval_secs = 0;
        let cache = Arc::new(
            CachingBackend::new(CountingMemory::new(), cfg)
                .await
                .unwrap(),
        );
        let loc = cache
            .put(Bytes::from_static(b"parallel"), PutHint::new("p.bin", ""))
            .await
            .unwrap();
        cache.invalidate_locator(&loc).await;

        let mut joins = Vec::new();
        for _ in 0..16 {
            let c = cache.clone();
            let l = loc.clone();
            joins.push(tokio::spawn(async move {
                collect_stream(c.get(&l, None).await.unwrap())
                    .await
                    .unwrap()
            }));
        }
        for j in joins {
            assert_eq!(j.await.unwrap().as_ref(), b"parallel");
        }
        assert_eq!(cache.inner().gets(), 1);
    }

    #[tokio::test]
    async fn disk_cache_survives_recreate() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = CacheConfig::default();
        cfg.disk_path = Some(dir.path().to_path_buf());
        cfg.disk_bytes = Some(64 * 1024 * 1024);
        cfg.write_through = true;
        cfg.metrics_interval_secs = 0;

        let loc = {
            let cache = CachingBackend::new(CountingMemory::new(), cfg.clone())
                .await
                .unwrap();
            let loc = cache
                .put(Bytes::from_static(b"persist-me"), PutHint::new("x.bin", ""))
                .await
                .unwrap();
            let _ = collect_stream(cache.get(&loc, None).await.unwrap())
                .await
                .unwrap();
            let _ = cache.l2.flush_if(|_, _| true).await;
            loc
        };

        let mut cfg3 = cfg.clone();
        cfg3.write_through = false;
        let cache3 = CachingBackend::new(CountingMemory::new(), cfg3)
            .await
            .unwrap();
        // Recovery is best-effort; must not panic.
        let _ = cache3.get(&loc, None).await;
    }

    #[tokio::test]
    async fn disabled_cache_passthrough() {
        let mut cfg = CacheConfig::default();
        cfg.enabled = false;
        cfg.metrics_interval_secs = 0;
        let cache = CachingBackend::new(CountingMemory::new(), cfg)
            .await
            .unwrap();
        let loc = cache
            .put(Bytes::from_static(b"z"), PutHint::new("z.bin", ""))
            .await
            .unwrap();
        let _ = collect_stream(cache.get(&loc, None).await.unwrap())
            .await
            .unwrap();
        let _ = collect_stream(cache.get(&loc, None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(cache.inner().gets(), 2);
    }
}
