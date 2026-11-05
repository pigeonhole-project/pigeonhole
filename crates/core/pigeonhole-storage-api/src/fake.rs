//! Fake channels, apart from where they are kept (feature `fake`).
//!
//! A fake Telegram or Discord channel is a [`FakeState`]: the messages, ids
//! and the service's rules, changed only by commands through a deterministic
//! `apply`. A [`FakeHost`] keeps it, and the bytes of documents as blobs the
//! state refers to by an opaque handle: [`MemoryHost`] in process memory (the
//! stores' tests), a metadata tablet in `pigeonhole-fake-channels` (a log and
//! snapshots on a store: the site's demo, the fake-channel build).

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The state of one fake channel.
///
/// `apply` must be deterministic: a host may replay the commands to rebuild
/// the state. Serialized for snapshots, so ordered containers only.
pub trait FakeState: Default + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The state's name where a host needs one (a tablet namespace).
    const NAME: &'static str;
    /// Bumped on any change to the state's or the commands' shape.
    const VERSION: u32;
    type Command: Serialize + DeserializeOwned + Send + 'static;
    type Reply: Serialize + DeserializeOwned + Send + 'static;

    fn apply(&mut self, command: Self::Command) -> Self::Reply;
}

/// Where a fake channel's state and documents live.
#[async_trait]
pub trait FakeHost<S: FakeState>: Send + Sync + 'static {
    /// Applies `command` (durably, where the host is durable).
    async fn apply(&self, command: S::Command) -> Result<S::Reply>;
    /// Reads the state as of the last applied command.
    fn read<R>(&self, f: impl FnOnce(&S) -> R) -> R;
    /// Keeps a document's bytes; the handle the state stores.
    async fn put_blob(&self, data: Bytes) -> Result<Vec<u8>>;
    /// Bytes `range` (`start..end`) of a document, or all of it.
    async fn get_blob(&self, blob: &[u8], range: Option<(u64, u64)>) -> Result<Bytes>;
    /// Drops documents no message holds any more.
    async fn delete_blobs(&self, blobs: &[Vec<u8>]) -> Result<()>;
}

/// A fake channel in process memory: gone with the process.
pub struct MemoryHost<S> {
    state: Mutex<S>,
    blobs: Mutex<HashMap<Vec<u8>, Bytes>>,
    next_blob: AtomicU64,
}

impl<S: FakeState> Default for MemoryHost<S> {
    fn default() -> Self {
        Self { state: Mutex::default(), blobs: Mutex::default(), next_blob: AtomicU64::new(1) }
    }
}

impl<S: FakeState> MemoryHost<S> {
    /// [`FakeHost::apply`] without waiting: memory has nothing to wait for.
    pub fn apply_now(&self, command: S::Command) -> S::Reply {
        self.state.lock().unwrap().apply(command)
    }

    /// [`FakeHost::put_blob`], at once.
    pub fn put_blob_now(&self, data: Bytes) -> Vec<u8> {
        let blob = self.next_blob.fetch_add(1, Ordering::Relaxed).to_be_bytes().to_vec();
        self.blobs.lock().unwrap().insert(blob.clone(), data);
        blob
    }

    /// The whole of a document, at once.
    pub fn blob_now(&self, blob: &[u8]) -> Option<Bytes> {
        self.blobs.lock().unwrap().get(blob).cloned()
    }

    /// [`FakeHost::delete_blobs`], at once.
    pub fn delete_blobs_now(&self, blobs: &[Vec<u8>]) {
        let mut map = self.blobs.lock().unwrap();
        for b in blobs {
            map.remove(b);
        }
    }
}

#[async_trait]
impl<S: FakeState> FakeHost<S> for MemoryHost<S> {
    async fn apply(&self, command: S::Command) -> Result<S::Reply> {
        Ok(self.apply_now(command))
    }

    fn read<R>(&self, f: impl FnOnce(&S) -> R) -> R {
        f(&self.state.lock().unwrap())
    }

    async fn put_blob(&self, data: Bytes) -> Result<Vec<u8>> {
        Ok(self.put_blob_now(data))
    }

    async fn get_blob(&self, blob: &[u8], range: Option<(u64, u64)>) -> Result<Bytes> {
        let Some(data) = self.blob_now(blob) else { bail!("no document blob {blob:?}") };
        Ok(match range {
            None => data,
            Some((start, end)) => {
                let end = end.min(data.len() as u64) as usize;
                data.slice((start as usize).min(end)..end)
            }
        })
    }

    async fn delete_blobs(&self, blobs: &[Vec<u8>]) -> Result<()> {
        self.delete_blobs_now(blobs);
        Ok(())
    }
}

/// A range of delays many fakes share, changed while they run (the demo's
/// settings): each call takes one in `min..=max`.
#[derive(Clone, Default)]
pub struct Latency(Arc<[AtomicU64; 2]>);

impl Latency {
    pub fn new(range: (Duration, Duration)) -> Self {
        let l = Self::default();
        l.set(range);
        l
    }
    pub fn set(&self, (min, max): (Duration, Duration)) {
        let us = |d: Duration| u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        self.0[0].store(us(min), Ordering::Relaxed);
        self.0[1].store(us(max.max(min)), Ordering::Relaxed);
    }
    pub fn get(&self) -> (Duration, Duration) {
        let [min, max] = [0, 1].map(|i| Duration::from_micros(self.0[i].load(Ordering::Relaxed)));
        (min, max.max(min))
    }
    /// Call `n`'s delay: spread over the range, the same for the same `n`.
    pub fn of(&self, n: u64) -> Duration {
        let (min, max) = self.get();
        let span = u64::try_from((max - min).as_micros()).unwrap_or(u64::MAX);
        if span == 0 {
            return min;
        }
        // splitmix64: an even spread without a random source.
        let mut z = n.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        min + Duration::from_micros((z ^ (z >> 31)) % (span + 1))
    }
}

static CALL_IDS: AtomicU64 = AtomicU64::new(0);

/// A fake bot's calls as the demo draws them: each is logged as it starts
/// (`fake call`: the store, the node making it when known, the call, an id,
/// its bytes when known and how long it takes, `ms`), takes that long
/// (the store's [`Latency`]), and is logged as it ends (`fake call done`,
/// the same id, whether it went through).
#[derive(Clone, Default)]
pub struct FakeCalls {
    store: Arc<str>,
    node: Option<u64>,
    latency: Latency,
}

impl FakeCalls {
    pub fn new(store: &str, node: Option<u64>, latency: Latency) -> Self {
        Self { store: store.into(), node, latency }
    }

    /// Call `call` starts: logged, then its delay. Returns its id, for [`Self::done`].
    pub async fn start(&self, call: &'static str, bytes: Option<u64>) -> u64 {
        let (id, delay) = self.draw();
        self.begin(id, call, bytes, delay);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        id
    }

    /// A new call's id, and the delay the latency gives it.
    pub fn draw(&self) -> (u64, Duration) {
        let id = CALL_IDS.fetch_add(1, Ordering::Relaxed) + 1;
        (id, self.latency.of(id))
    }

    /// Call `id` (`call`, of `bytes`) starts and takes `delay`, which its
    /// caller waits out: logged.
    pub fn begin(&self, id: u64, call: &'static str, bytes: Option<u64>, delay: Duration) {
        let ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        tracing::debug!(store = %self.store, node = self.node, call, id, bytes, ms, "fake call");
    }

    /// Call `id` ends: it went through (`ok`) or failed.
    pub fn done(&self, id: u64, ok: bool) {
        tracing::debug!(store = %self.store, id, ok, "fake call done");
    }
}
