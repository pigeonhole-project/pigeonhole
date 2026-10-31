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
use std::sync::Mutex;

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
