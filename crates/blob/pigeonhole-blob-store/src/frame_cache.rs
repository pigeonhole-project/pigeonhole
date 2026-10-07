//! L1 cache of unpacked frames keyed by ([`BlobKey`], frame_no).

use bytes::Bytes;
use moka::future::Cache;
use pigeonhole_types::BlobKey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

#[derive(Clone, Hash, Eq, PartialEq)]
struct FrameKey {
    blob: BlobKey,
    frame_no: u32,
}

pub struct FrameCache {
    cache: Cache<FrameKey, Bytes>,
    /// Secondary index for GC invalidation by file_id / blob key.
    by_blob: Mutex<HashMap<BlobKey, Vec<u32>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    collapsed: AtomicU64,
    /// Caps concurrent readahead getFile / decode work.
    readahead_sem: Arc<Semaphore>,
    readahead_frames: usize,
}

impl FrameCache {
    pub fn new(capacity_bytes: usize, readahead_frames: usize) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(capacity_bytes as u64)
                .weigher(|_k: &FrameKey, v: &Bytes| v.len().min(u32::MAX as usize) as u32)
                .build(),
            by_blob: Mutex::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            collapsed: AtomicU64::new(0),
            readahead_sem: Arc::new(Semaphore::new(2)),
            readahead_frames,
        }
    }

    pub fn readahead_frames(&self) -> usize {
        self.readahead_frames
    }

    pub fn readahead_sem(&self) -> Arc<Semaphore> {
        self.readahead_sem.clone()
    }

    pub fn metrics(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.collapsed.load(Ordering::Relaxed),
        )
    }

    pub async fn get_or_load<F, Fut>(
        &self,
        blob: BlobKey,
        frame_no: u32,
        loader: F,
    ) -> anyhow::Result<Bytes>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Bytes>>,
    {
        let key = FrameKey {
            blob: blob.clone(),
            frame_no,
        };
        if self.cache.contains_key(&key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        let collapsed = Arc::new(AtomicU64::new(0));
        let c = collapsed.clone();
        let by_blob = &self.by_blob;
        let out = self
            .cache
            .try_get_with(key.clone(), async move {
                c.fetch_add(1, Ordering::Relaxed);
                loader().await
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if collapsed.load(Ordering::Relaxed) == 0 {
            self.collapsed.fetch_add(1, Ordering::Relaxed);
        } else {
            let mut map = by_blob.lock().unwrap();
            map.entry(blob).or_default().push(frame_no);
        }
        Ok(out)
    }

    pub async fn insert(&self, blob: BlobKey, frame_no: u32, data: Bytes) {
        {
            let mut map = self.by_blob.lock().unwrap();
            map.entry(blob.clone()).or_default().push(frame_no);
        }
        self.cache
            .insert(FrameKey { blob, frame_no }, data)
            .await;
    }

    pub async fn invalidate_blob(&self, blob: &BlobKey) {
        let frames = {
            let mut map = self.by_blob.lock().unwrap();
            map.remove(blob).unwrap_or_default()
        };
        for frame_no in frames {
            self.cache
                .invalidate(&FrameKey {
                    blob: blob.clone(),
                    frame_no,
                })
                .await;
        }
    }

    pub async fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        self.by_blob.lock().unwrap().clear();
    }
}
