//! In-process pins of uploads that are not committed yet, and the sweep
//! horizon they leave.
//!
//! An upload takes a pin **before** it sends anything; the pin records the
//! highest key the instance had returned at that moment. Backend keys grow
//! with time (Telegram / Discord message ids, the memory store's counter), so
//! whatever an in-flight upload gets is above every pin's floor. The pin is
//! dropped once the commit that references the part is applied (or the upload
//! failed and the part is garbage).
//!
//! [`UploadPins::horizon`] is therefore a key at or below which every blob of
//! this process is either committed (and visible in the chunk state) or
//! garbage: the sweeper may delete what is not committed up to it. After a
//! restart the registry is empty: uncommitted parts of a dead writer are
//! garbage, and [`UploadPins::seed`] lets them be swept right away.

use crate::replicated::InstanceId;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct InstancePins {
    /// Highest key a finished upload returned (or the seed).
    returned: Option<Vec<u8>>,
    /// Live pins: id → `returned` when the pin was taken.
    live: BTreeMap<u64, Option<Vec<u8>>>,
    next_id: u64,
}

/// Upload pins of every instance of this process.
#[derive(Debug, Default)]
pub struct UploadPins {
    inner: Mutex<HashMap<InstanceId, InstancePins>>,
}

impl UploadPins {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Pin an upload to `instance` that is about to start; hold the pin until
    /// its part is committed.
    pub fn pin(self: &Arc<Self>, instance: &str) -> UploadPin {
        let mut g = self.inner.lock().expect("upload pins lock");
        let pins = g.entry(instance.to_string()).or_default();
        let id = pins.next_id;
        pins.next_id += 1;
        pins.live.insert(id, pins.returned.clone());
        UploadPin { pins: Arc::clone(self), instance: instance.to_string(), id: Some(id) }
    }

    /// An upload to `instance` returned `key`.
    pub fn returned(&self, instance: &str, key: &[u8]) {
        let mut g = self.inner.lock().expect("upload pins lock");
        let pins = g.entry(instance.to_string()).or_default();
        if pins.returned.as_deref().is_none_or(|r| key > r) {
            pins.returned = Some(key.to_vec());
        }
    }

    /// Start the horizon at `key` (the highest committed key after a restart):
    /// anything at or below it that is not committed is a dead writer's garbage.
    pub fn seed(&self, instance: &str, key: &[u8]) {
        self.returned(instance, key);
    }

    /// Highest key the sweeper may consider: the lowest floor of a live pin,
    /// else the highest returned key. `None` = nothing is known to be safe.
    pub fn horizon(&self, instance: &str) -> Option<Vec<u8>> {
        let g = self.inner.lock().expect("upload pins lock");
        let pins = g.get(instance)?;
        if pins.live.is_empty() {
            return pins.returned.clone();
        }
        // A pin taken before anything was returned has no floor: no horizon.
        let mut floors = pins.live.values();
        let mut low = floors.next()?.clone()?;
        for f in floors {
            let f = f.as_ref()?;
            if *f < low {
                low = f.clone();
            }
        }
        Some(low)
    }

    /// Live pins on `instance` (tests / metrics).
    pub fn live(&self, instance: &str) -> usize {
        let g = self.inner.lock().expect("upload pins lock");
        g.get(instance).map_or(0, |p| p.live.len())
    }

    fn release(&self, instance: &str, id: u64) {
        let mut g = self.inner.lock().expect("upload pins lock");
        if let Some(pins) = g.get_mut(instance) {
            pins.live.remove(&id);
        }
    }
}

/// One pinned upload; [`Drop`] releases it.
#[derive(Debug)]
pub struct UploadPin {
    pins: Arc<UploadPins>,
    instance: String,
    id: Option<u64>,
}

impl Drop for UploadPin {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.pins.release(&self.instance, id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(v: u8) -> Vec<u8> {
        vec![0, v]
    }

    #[test]
    fn horizon_stops_below_every_pinned_upload() {
        let pins = UploadPins::shared();
        assert_eq!(pins.horizon("a"), None, "nothing returned yet");
        let first = pins.pin("a");
        assert_eq!(pins.horizon("a"), None, "a pin taken before any return has no floor");
        pins.returned("a", &k(5));
        drop(first);
        assert_eq!(pins.horizon("a"), Some(k(5)));

        let slow = pins.pin("a"); // floor 5: its key will be > 5
        pins.returned("a", &k(7));
        let fast = pins.pin("a"); // floor 7
        pins.returned("a", &k(9));
        assert_eq!(pins.horizon("a"), Some(k(5)), "the oldest pin bounds the horizon");
        drop(slow);
        assert_eq!(pins.horizon("a"), Some(k(7)));
        drop(fast);
        assert_eq!(pins.horizon("a"), Some(k(9)));
        assert_eq!(pins.horizon("b"), None, "instances are independent");
    }

    #[test]
    fn seed_starts_the_horizon_and_never_moves_it_down() {
        let pins = UploadPins::shared();
        pins.seed("a", &k(4));
        assert_eq!(pins.horizon("a"), Some(k(4)));
        pins.returned("a", &k(3));
        assert_eq!(pins.horizon("a"), Some(k(4)));
        let p = pins.pin("a");
        assert_eq!(pins.horizon("a"), Some(k(4)));
        assert_eq!(pins.live("a"), 1);
        drop(p);
        assert_eq!(pins.live("a"), 0);
    }
}
