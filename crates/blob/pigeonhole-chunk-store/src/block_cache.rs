//! In-memory block cache keyed by (chunk, block_no): L1 holds decoded
//! blocks, L2 the stored (compressed) ones. Committed chunk ids are never
//! reused, so entries never go stale; dropped chunks age out. Only a chunk
//! written but never committed (its leader lost the lead) leaves an entry
//! another leader's chunk may take the id of: a leader clears its caches
//! when it takes over, before it serves a term (`TakeOver::run`). A clear
//! starts a new [`Epoch`]: entries are keyed by theirs and only the current
//! one's are found, so a write that chose its chunk's id before a clear and
//! puts its blocks in after it ([`BlockCache::insert_as_of`]) leaves
//! nothing found.

use crate::ChunkId;
use bytes::Bytes;
use moka::future::Cache;
use pigeonhole_blob::{record_cache, set_cache_size};
use std::sync::atomic::{AtomicU64, Ordering};

/// Block cache knobs (`[cache]`). L1 holds decoded blocks, L2 stored (compressed) ones; a read looks in L1,
/// then L2, then the backend.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub enabled: bool,
    /// L1 capacity (decoded bytes).
    pub l1_bytes: usize,
    /// L2 capacity (stored bytes); `None` = no L2.
    pub l2_bytes: Option<usize>,
    /// Chunks written put their stored blocks into L2.
    pub write_through: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self { enabled: true, l1_bytes: 128 * 1024 * 1024, l2_bytes: Some(256 * 1024 * 1024), write_through: true }
    }
}

pub struct BlockCache {
    /// Metrics label: `l1` / `l2`.
    level: &'static str,
    cache: Cache<(u64, ChunkId, u32), Bytes>,
    /// Bumped by every clear.
    epoch: AtomicU64,
}

/// The cache between two clears ([`BlockCache::epoch`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Epoch(u64);

impl BlockCache {
    pub fn new(level: &'static str, capacity_bytes: usize) -> Self {
        Self {
            level,
            cache: Cache::builder()
                .max_capacity(capacity_bytes as u64)
                .weigher(|_k, v: &Bytes| v.len().min(u32::MAX as usize) as u32)
                .build(),
            epoch: AtomicU64::new(0),
        }
    }

    /// The current epoch: take it before choosing the id of a chunk whose
    /// blocks go in later ([`Self::insert_as_of`]).
    pub fn epoch(&self) -> Epoch {
        Epoch(self.epoch.load(Ordering::SeqCst))
    }

    /// The cached block, if any (a hit or a miss, counted).
    pub async fn get(&self, chunk: ChunkId, block_no: u32) -> Option<Bytes> {
        let hit = self.cache.get(&(self.epoch().0, chunk, block_no)).await;
        record_cache(self.level, if hit.is_some() { "hit" } else { "miss" });
        hit
    }

    /// Put in a block of a committed chunk (its id is never reused).
    pub async fn insert(&self, chunk: ChunkId, block_no: u32, data: Bytes) {
        self.insert_as_of(self.epoch(), chunk, block_no, data).await;
    }

    /// Put in a block of a chunk whose id was chosen in `epoch`: found only
    /// if the cache was not cleared since.
    pub async fn insert_as_of(&self, epoch: Epoch, chunk: ChunkId, block_no: u32, data: Bytes) {
        self.cache.insert((epoch.0, chunk, block_no), data).await;
        self.record_size();
    }

    /// Drop every entry: a new epoch, so none is found again; the memory
    /// is freed as moka gets to it.
    pub fn clear(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
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
        // No sleep before the clear: moka keeps an entry inserted at the very
        // instant of invalidate_all, but the epoch moved on.
        cache.clear();
        assert_eq!(cache.get(1, 0).await, None);
    }

    /// A chunk whose id was chosen before a clear (a leader that lost the
    /// lead meanwhile) puts its blocks in for nobody.
    #[tokio::test]
    async fn a_block_put_in_as_of_before_a_clear_is_not_found() {
        let cache = BlockCache::new("l2", 1 << 20);
        let before = cache.epoch();
        cache.clear();
        cache.insert_as_of(before, 1, 0, Bytes::from_static(b"stale")).await;
        assert_eq!(cache.get(1, 0).await, None);
        let now = cache.epoch();
        cache.insert_as_of(now, 2, 0, Bytes::from_static(b"fresh")).await;
        assert_eq!(cache.get(2, 0).await, Some(Bytes::from_static(b"fresh")));
    }
}
