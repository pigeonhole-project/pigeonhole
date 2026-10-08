//! In-process registry of backend keys that are uploaded but not yet durable
//! in `chunk_parts` / the superblock.
//!
//! The sweeper consults this set so it cannot delete parts of a chunk that is
//! still being assembled (first part → commit can exceed sweep `grace`).
//! After process restart the registry is empty: uncommitted parts of a dead
//! writer are garbage and may be reclaimed.

use crate::replicated::InstanceId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type KeyMap = HashMap<Vec<u8>, usize>;

/// Refcounted set of in-flight `(instance, sort_key)` pairs.
#[derive(Debug, Default)]
pub struct InflightParts {
    inner: Mutex<HashMap<InstanceId, KeyMap>>,
}

impl InflightParts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Register `key` until the returned guard is dropped.
    pub fn guard(self: &Arc<Self>, instance: &str, key: Vec<u8>) -> InflightGuard {
        {
            let mut g = self.inner.lock().expect("inflight lock");
            let map = g.entry(instance.to_string()).or_default();
            *map.entry(key.clone()).or_insert(0) += 1;
        }
        InflightGuard {
            parts: Arc::clone(self),
            instance: instance.to_string(),
            key: Some(key),
        }
    }

    pub fn contains(&self, instance: &str, key: &[u8]) -> bool {
        let g = self.inner.lock().expect("inflight lock");
        g.get(instance)
            .and_then(|m| m.get(key))
            .is_some_and(|n| *n > 0)
    }

    /// Test helper: how many keys are registered for `instance`.
    pub fn len_for(&self, instance: &str) -> usize {
        let g = self.inner.lock().expect("inflight lock");
        g.get(instance).map(|m| m.len()).unwrap_or(0)
    }

    fn release(&self, instance: &str, key: &[u8]) {
        let mut g = self.inner.lock().expect("inflight lock");
        let Some(map) = g.get_mut(instance) else {
            return;
        };
        let Some(n) = map.get_mut(key) else {
            return;
        };
        *n = n.saturating_sub(1);
        if *n == 0 {
            map.remove(key);
        }
        if map.is_empty() {
            g.remove(instance);
        }
    }
}

/// RAII registration; [`Drop`] decrements the refcount for one key.
#[derive(Debug)]
pub struct InflightGuard {
    parts: Arc<InflightParts>,
    instance: String,
    key: Option<Vec<u8>>,
}

impl InflightGuard {
    /// Keep the registration alive without holding the guard value (tests).
    pub fn forget(mut self) {
        self.key.take();
        std::mem::forget(self);
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.parts.release(&self.instance, &key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_registers_and_drop_unregisters() {
        let parts = InflightParts::shared();
        assert!(!parts.contains("a", b"k1"));
        let g = parts.guard("a", b"k1".to_vec());
        assert!(parts.contains("a", b"k1"));
        drop(g);
        assert!(!parts.contains("a", b"k1"));
    }

    #[test]
    fn refcount_allows_overlapping_guards() {
        let parts = InflightParts::shared();
        let g1 = parts.guard("a", b"k".to_vec());
        let g2 = parts.guard("a", b"k".to_vec());
        drop(g1);
        assert!(parts.contains("a", b"k"));
        drop(g2);
        assert!(!parts.contains("a", b"k"));
    }
}
