//! Local disk [`BlobBackend`] + [`Chain`]: blobs are records in append-only
//! segment files (see `segments.rs` for the layout).
//!
//! A put appends one record and fsyncs it (one fsync, as the old segment log
//! did per batch); its key is the record's byte position, so keys grow with
//! every put (the sweeper's horizon needs it) and a read is one `pread`.
//! `put_after` decides "first successor wins" under the directory's `flock`,
//! so the store holds a metadata log ([`Chain`]) as well as blobs; one
//! store serves one purpose.

mod segments;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    bytes_stream, After, BlobBackend, BoxByteStream, Chain, CostHint, InstanceInfo, InstanceKind, InstanceRole,
    Linked, OpKind, Sweepable,
};
use pigeonhole_types::{BackendLimits, ByteRange};
use segments::Segments;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub use segments::DEFAULT_SEGMENT_BYTES;

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
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f).await.context("disk task")?
}

impl DiskBlobStore {
    /// Open (or create) the directory; instance id `local_disk`.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        let info = InstanceInfo {
            id: "local_disk".into(),
            kind: InstanceKind::LocalDisk,
            fingerprint: disk_location(&dir),
            location: disk_location(&dir),
            role: InstanceRole::ReadWrite,
        };
        Self::with_instance(dir, info)
    }

    /// Open with a pre-resolved [`InstanceInfo`] (from `[[store]]`).
    pub fn with_instance(dir: impl Into<PathBuf>, info: InstanceInfo) -> Result<Self> {
        Self::with_segment_bytes(dir, info, DEFAULT_SEGMENT_BYTES)
    }

    /// Roll over to a new segment file past `segment_bytes` (tests use tiny ones).
    pub fn with_segment_bytes(dir: impl Into<PathBuf>, info: InstanceInfo, segment_bytes: u64) -> Result<Self> {
        let dir = dir.into();
        let segments = Segments::open(&dir, segment_bytes)?;
        Ok(Self {
            dir,
            instance: info,
            limits: BackendLimits::local_disk(),
            segments: Arc::new(Mutex::new(segments)),
        })
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
        let segments = self.segments.clone();
        blocking(move || {
            let mut guard = segments.lock().unwrap();
            let mut locked = guard.locked()?;
            locked.catch_up()?;
            f(&mut locked)
        })
        .await
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
        let name = name.to_string();
        let key = self.locked(move |s| s.append(None, &name, &data)).await?;
        Ok(DiskId { key })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let segments = self.segments.clone();
        let key = id.key;
        let data = blocking(move || {
            let mut guard = segments.lock().unwrap();
            if !guard.records.contains_key(&key) {
                // Appended by another process since we last looked?
                guard.locked()?.catch_up()?;
            }
            let len = guard.records.get(&key).filter(|r| !r.dead).map(|r| r.data_len);
            let Some(len) = len else { bail!("no blob {key}") };
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
        Ok(bytes_stream(Bytes::from(data)))
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        let keys = keys.to_vec();
        self.locked(move |s| s.delete(&keys)).await
    }
}

#[async_trait]
impl Sweepable for DiskBlobStore {
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
        let label = label.to_string();
        self.locked(move |s| {
            if let Some(&winner) = s.next.get(&prev) {
                return Ok(After::Lost(Self::linked(s, winner)));
            }
            let key = s.append(Some((prev, &label)), &label, &data)?;
            Ok(After::Won(DiskId { key }))
        })
        .await
    }

    async fn tail(&self) -> Result<Option<Linked<Self::Id>>> {
        self.locked(|s| Ok(s.tail.map(|t| Self::linked(s, t)))).await
    }

    async fn successor(&self, id: &Self::Id) -> Result<Option<Linked<Self::Id>>> {
        let key = id.key;
        self.locked(move |s| Ok(s.next.get(&Some(key)).map(|&n| Self::linked(s, n)))).await
    }

    async fn prune(&self, below: &Self::Id) -> Result<()> {
        let below = below.key;
        self.locked(move |s| {
            let doomed: Vec<u64> = s.records.range(..below).filter(|(_, r)| !r.dead).map(|(k, _)| *k).collect();
            s.delete(&doomed)
        })
        .await
    }
}

#[cfg(test)]
mod tests;
