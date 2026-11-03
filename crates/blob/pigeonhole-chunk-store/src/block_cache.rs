//! In-memory block cache keyed by (chunk, block_no): L1 holds decoded
//! blocks, L2 the stored (compressed) ones. Committed chunk ids are never
//! reused, so entries never go stale; dropped chunks age out. Only a chunk
//! written but never committed (its leader lost the lead) leaves an entry
//! another leader's chunk may take the id of: a leader clears its caches
//! when it takes over ([`crate::ChunkStore::take_over`]).

use crate::ChunkId;
use bytes::Bytes;
use moka::future::Cache;
use pigeonhole_blob::{record_cache, set_cache_size};

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

    /// The cached block, if any (a hit or a miss, counted).
    pub async fn get(&self, chunk: ChunkId, block_no: u32) -> Option<Bytes> {
        let hit = self.cache.get(&(chunk, block_no)).await;
        record_cache(self.level, if hit.is_some() { "hit" } else { "miss" });
        hit
    }

    pub async fn insert(&self, chunk: ChunkId, block_no: u32, data: Bytes) {
        self.cache.insert((chunk, block_no), data).await;
        self.record_size();
    }

    /// Drop every entry.
    pub fn clear(&self) {
        self.cache.invalidate_all();
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
    async fn a_block_put_in_is_found() {
        let cache = BlockCache::new("l1", 1 << 20);
        assert_eq!(cache.get(1, 0).await, None);
        cache.insert(1, 0, Bytes::from_static(b"block")).await;
        assert_eq!(cache.get(1, 0).await, Some(Bytes::from_static(b"block")));
        cache.clear();
        assert_eq!(cache.get(1, 0).await, None);
    }
}
