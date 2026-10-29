//! In-memory block cache keyed by (chunk, block_no): L1 holds decoded
//! blocks, L2 the stored (compressed) ones. Chunk ids are never reused, so
//! entries never go stale; dropped chunks age out.

use bytes::Bytes;
use moka::future::Cache;
use pigeonhole_blob::{record_cache, set_cache_size};
use crate::ChunkId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct BlockCache {
    /// Metrics label: `l1` / `l2`.
    level: &'static str,
    cache: Cache<(ChunkId, u32), Bytes>,
}

impl BlockCache {
    pub fn new(level: &'static str, capacity_bytes: usize) -> Self {
        Self {
            level,
            cache: Cache::builder()
                .max_capacity(capacity_bytes as u64)
                .weigher(|_k, v: &Bytes| v.len().min(u32::MAX as usize) as u32)
                .build(),
        }
    }

    /// The cached block, or `loader`'s; concurrent lookups of one missing
    /// block share one load (`collapsed`: waited for another lookup's).
    pub async fn get_or_load<F, Fut>(&self, chunk: ChunkId, block_no: u32, loader: F) -> anyhow::Result<Bytes>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Bytes>>,
    {
        let key = (chunk, block_no);
        if let Some(hit) = self.cache.get(&key).await {
            record_cache(self.level, "hit");
            return Ok(hit);
        }
        let loaded = Arc::new(AtomicBool::new(false));
        let mine = loaded.clone();
        let out = self
            .cache
            .try_get_with(key, async move {
                mine.store(true, Ordering::Relaxed);
                loader().await
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if loaded.load(Ordering::Relaxed) {
            record_cache(self.level, "miss");
            self.record_size();
        } else {
            record_cache(self.level, "collapsed");
        }
        Ok(out)
    }

    pub async fn insert(&self, chunk: ChunkId, block_no: u32, data: Bytes) {
        self.cache.insert((chunk, block_no), data).await;
        self.record_size();
    }

    fn record_size(&self) {
        set_cache_size(self.level, self.cache.weighted_size(), self.cache.entry_count());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hit_miss_and_collapsed_are_counted_once_each() {
        let cache = Arc::new(BlockCache::new("l1", 1 << 20));
        let loads = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let load = |loads: Arc<std::sync::atomic::AtomicU32>| async move {
            loads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(Bytes::from_static(b"block"))
        };
        let (a, b) = tokio::join!(
            cache.get_or_load(1, 0, || load(loads.clone())),
            cache.get_or_load(1, 0, || load(loads.clone())),
        );
        assert_eq!((a.unwrap(), b.unwrap()), (Bytes::from_static(b"block"), Bytes::from_static(b"block")));
        assert_eq!(loads.load(Ordering::SeqCst), 1, "one load for two concurrent lookups");
        cache.get_or_load(1, 0, || load(loads.clone())).await.unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 1, "then a hit");
        cache.insert(2, 0, Bytes::from_static(b"written")).await;
        let got = cache.get_or_load(2, 0, || load(loads.clone())).await.unwrap();
        assert_eq!((got, loads.load(Ordering::SeqCst)), (Bytes::from_static(b"written"), 1));
    }
}
