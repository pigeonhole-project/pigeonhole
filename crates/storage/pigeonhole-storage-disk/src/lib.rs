//! Local disk [`BlobBackend`] + [`Chain`]: blobs are records in append-only
//! segment files (see `segments.rs` for the layout).
//!
//! A put appends one record and fsyncs it (one fsync, as the old segment log
//! did per batch); its key is the record's byte position, so keys grow with
//! every put (the sweeper's horizon needs it) and a read is one `pread`.
//! `put_after` decides "first successor wins" under the directory's `flock`,
//! so the store holds a metadata log ([`Chain`]) as well as blobs; one
//! store serves one purpose.

pub mod io;
mod segments;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_storage_api::{
    bytes_stream, After, BlobBackend, BoxByteStream, Chain, CostHint, InstanceInfo, InstanceRole, Linked,
    MakeAfter, NotFound, OpKind,
};
use pigeonhole_limits::StoreLimiter;
use pigeonhole_types::{BackendLimits, ByteRange};
use segments::Segments;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub use io::{Fs, SimFs, StdFs};
pub use segments::DEFAULT_SEGMENT_BYTES;

/// What [`InstanceInfo::kind`](pigeonhole_storage_api::InstanceInfo::kind) says of this store.
pub const KIND: &str = "local_disk";

/// Identity of a blob on disk: its key, the record's position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskId {
    pub key: u64,
}

pub fn disk_location(dir: &Path) -> String {
    format!("disk:{}", dir.display())
}

pub struct DiskBlobStore {
    dir: PathBuf,
    instance: InstanceInfo,
    limits: BackendLimits,
    segments: Arc<Mutex<Segments>>,
    fs: Arc<dyn Fs>,
    /// Every operation in flight; [`Chain::prune`] runs only when idle.
    limiter: Arc<StoreLimiter>,
}

/// Records [`Chain::prune`] deletes per idle moment.
const PRUNE_BATCH: usize = 100;

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f).await.context("disk task")?
}

impl DiskBlobStore {
    /// Open (or create) the directory; instance id `local_disk`.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        Self::with_segment_bytes(dir, DEFAULT_SEGMENT_BYTES)
    }

    /// [`Self::open`], rolling over to a new segment past `segment_bytes`
    /// (tests use tiny ones).
    pub fn with_segment_bytes(dir: impl Into<PathBuf>, segment_bytes: u64) -> Result<Self> {
        let dir = dir.into();
        let info = InstanceInfo {
            id: "local_disk".into(),
            kind: crate::KIND,
            fingerprint: disk_location(&dir),
            location: disk_location(&dir),
            role: InstanceRole::ReadWrite,
        };
        Self::open_as(dir, info, segment_bytes)
    }

    /// Open with a pre-resolved [`InstanceInfo`] (from `[[store]]`).
    pub fn with_instance(dir: impl Into<PathBuf>, info: InstanceInfo) -> Result<Self> {
        Self::open_as(dir.into(), info, DEFAULT_SEGMENT_BYTES)
    }

    /// Open `dir` on another file system (a [`SimFs`] in tests and the
    /// simulation).
    pub fn open_on(fs: Arc<dyn Fs>, dir: impl Into<PathBuf>, segment_bytes: u64) -> Result<Self> {
        let dir = dir.into();
        let info = InstanceInfo {
            id: "local_disk".into(),
            kind: crate::KIND,
            fingerprint: disk_location(&dir),
            location: disk_location(&dir),
            role: InstanceRole::ReadWrite,
        };
        Self::open_with(fs, dir, info, segment_bytes)
    }

    fn open_as(dir: PathBuf, info: InstanceInfo, segment_bytes: u64) -> Result<Self> {
        Self::open_with(Arc::new(StdFs), dir, info, segment_bytes)
    }

    fn open_with(fs: Arc<dyn Fs>, dir: PathBuf, info: InstanceInfo, segment_bytes: u64) -> Result<Self> {
        let segments = Segments::open(fs.clone(), &dir, segment_bytes)?;
        Ok(Self {
            dir,
            limits: BackendLimits::local_disk(),
            segments: Arc::new(Mutex::new(segments)),
            fs,
            limiter: Arc::new(StoreLimiter::new(&info.id)),
            instance: info,
        })
    }

    /// At most `rate_per_sec` operations a second (`burst` at once).
    pub fn with_rate(mut self, rate_per_sec: f64, burst: f64) -> Self {
        self.limiter = Arc::new(StoreLimiter::new(&self.instance.id).with_rate(rate_per_sec, burst));
        self
    }

    pub fn limiter(&self) -> &Arc<StoreLimiter> {
        &self.limiter
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Run `f` on the segments under the `flock`, caught up with other
    /// processes first.
    async fn locked<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut segments::Locked<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let _op = self.limiter.op().await;
        let segments = self.segments.clone();
        let out = blocking(move || {
            let mut guard = segments.lock().unwrap();
            let mut locked = guard.locked()?;
            locked.catch_up()?;
            f(&mut locked)
        })
        .await;
        self.charge().await;
        out
    }

    /// Spend the virtual time a simulated disk says the I/O took. Under the
    /// simulator a real disk costs a nominal millisecond per operation, or a
    /// writer that only waits for its disk would stop the clock.
    async fn charge(&self) {
        let cost = self.fs.take_cost();
        #[cfg(madsim)]
        let cost = cost.max(std::time::Duration::from_millis(1));
        if !cost.is_zero() {
            tokio::time::sleep(cost).await;
        }
    }

    fn check_size(&self, data: &Bytes) -> Result<()> {
        if data.is_empty() {
            bail!("refusing an empty blob");
        }
        if data.len() > self.limits.max_blob_size {
            bail!("blob size {} exceeds backend max {}", data.len(), self.limits.max_blob_size);
        }
        Ok(())
    }

    fn linked(segments: &Segments, key: u64) -> Linked<DiskId> {
        let label = segments.records.get(&key).and_then(|r| r.chain.as_ref()).map(|c| c.label.clone());
        Linked { id: DiskId { key }, label: label.unwrap_or_default() }
    }
}

#[async_trait]
impl BlobBackend for DiskBlobStore {
    type Id = DiskId;
    type Key = u64;

    fn instance(&self) -> &InstanceInfo {
        &self.instance
    }

    fn limits(&self) -> &BackendLimits {
        &self.limits
    }

    fn key(id: &Self::Id) -> Self::Key {
        id.key
    }

    fn cost(&self, _op: OpKind, _id: Option<&Self::Id>) -> CostHint {
        CostHint::free()
    }

    async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
        self.check_size(&data)?;
        let (bytes, owned) = (data.len(), name.to_string());
        let key = self.locked(move |s| s.append(None, &owned, &data)).await?;
        tracing::debug!(store = %self.instance().id, key, segment = key >> 40, bytes, name, "stored");
        Ok(DiskId { key })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let _op = self.limiter.op().await;
        let segments = self.segments.clone();
        let key = id.key;
        let data = blocking(move || {
            let mut guard = segments.lock().unwrap();
            if !guard.records.contains_key(&key) {
                // Appended by another process since we last looked?
                guard.locked()?.catch_up()?;
            }
            let len = guard.records.get(&key).filter(|r| !r.dead).map(|r| r.data_len);
            let Some(len) = len else { return Err(NotFound(format!("blob {key}")).into()) };
            let (from, to) = match range {
                Some(r) => (r.start, r.end),
                None => (0, len),
            };
            if from > to {
                bail!("byte range {from}..{to} is reversed");
            }
            guard.read(key, from, to - from)
        })
        .await?;
        tracing::debug!(store = %self.instance().id, key, bytes = data.len(), "read");
        Ok(bytes_stream(Bytes::from(data)))
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        let n = keys.len();
        let keys = keys.to_vec();
        self.locked(move |s| s.delete(&keys)).await?;
        tracing::debug!(store = %self.instance().id, keys = n, "deleted");
        Ok(())
    }

    async fn candidates(&self, after: Option<Self::Key>, upto: Self::Key, limit: usize) -> Result<Vec<Self::Key>> {
        self.locked(move |s| {
            let from = after.map_or(0, |a| a.saturating_add(1));
            if from > upto {
                return Ok(Vec::new());
            }
            Ok(s.records.range(from..=upto).filter(|(_, r)| !r.dead).map(|(k, _)| *k).take(limit).collect())
        })
        .await
    }
}

/// The successor of a record is decided under the `flock`: a `put_after`
/// whose `prev` already has one loses without writing.
#[async_trait]
impl Chain for DiskBlobStore {
    async fn put_after(&self, prev: Option<&Self::Id>, label: &str, data: Bytes) -> Result<After<Self::Id>> {
        self.check_size(&data)?;
        let prev = prev.map(|p| p.key);
        let (bytes, owned) = (data.len(), label.to_string());
        let after = self
            .locked(move |s| {
                if let Some(&winner) = s.next.get(&prev) {
                    return Ok(After::Lost(Self::linked(s, winner)));
                }
                let key = s.append(Some((prev, &owned)), &owned, &data)?;
                Ok(After::Won(DiskId { key }))
            })
            .await?;
        match &after {
            After::Won(id) => tracing::debug!(store = %self.instance().id, label, ?prev, key = id.key, bytes, "linked"),
            After::Lost(w) => {
                tracing::debug!(store = %self.instance().id, label, ?prev, winner = w.id.key, winner_label = %w.label, "lost the link");
            }
        }
        Ok(after)
    }

    async fn tail(&self) -> Result<Option<Linked<Self::Id>>> {
        self.locked(|s| Ok(s.tail.map(|t| Self::linked(s, t)))).await
    }

    async fn successor(&self, id: &Self::Id) -> Result<Option<Linked<Self::Id>>> {
        let key = id.key;
        self.locked(move |s| Ok(s.next.get(&Some(key)).map(|&n| Self::linked(s, n)))).await
    }

    /// Atomic: the tail is read and the blob appended under one `flock`.
    async fn put_after_tail(&self, label: &str, make: &MakeAfter<'_, Self::Id>) -> Result<Option<Self::Id>> {
        let _op = self.limiter.op().await;
        let segments = self.segments.clone();
        let label = label.to_string();
        // `make` is borrowed, so this runs on the caller's thread: one
        // append and its fsync, the price of every claim.
        let out = (move || {
            let mut guard = segments.lock().unwrap();
            let mut s = guard.locked()?;
            s.catch_up()?;
            let tail = s.tail;
            let data = match tail {
                Some(t) => {
                    let len = s.records.get(&t).map_or(0, |r| r.data_len);
                    Some(s.read(t, 0, len)?)
                }
                None => None,
            };
            let tail_id = tail.map(|key| DiskId { key });
            let Some(blob) = make(tail_id.as_ref().zip(data.as_deref()))? else { return Ok(None) };
            self.check_size(&blob)?;
            let key = s.append(Some((tail, &label)), &label, &blob)?;
            tracing::debug!(store = %self.instance().id, label, ?tail, key, bytes = blob.len(), "linked after the tail");
            Ok(Some(DiskId { key }))
        })();
        self.charge().await;
        out
    }

    /// Background work: a batch at a time, each when no other operation runs.
    async fn prune(&self, below: &Self::Id) -> Result<()> {
        let below = below.key;
        let doomed: Vec<u64> =
            self.locked(move |s| Ok(s.records.range(..below).filter(|(_, r)| !r.dead).map(|(k, _)| *k).collect())).await?;
        for batch in doomed.chunks(PRUNE_BATCH) {
            self.limiter.wait_idle().await;
            let batch = batch.to_vec();
            self.locked(move |s| s.delete(&batch)).await?;
        }
        tracing::debug!(store = %self.instance().id, below, deleted = doomed.len(), "pruned");
        Ok(())
    }
}

#[cfg(test)]
mod tests;
