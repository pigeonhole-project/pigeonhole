//! Local disk [`BlobBackend`]: one file per blob in a directory.
//!
//! ```text
//! <dir>/<key>.blob    a blob; <key> grows with every put (20 digits)
//! <dir>/<key>.tmp     a put in progress (never read; swept like any orphan)
//! ```
//!
//! A put writes the tmp file, fsyncs it, renames it and fsyncs the directory,
//! so a returned id is durable. Keys are handed out from the highest key on
//! disk, so they keep growing across restarts (the sweeper's horizon needs it).
//! One process per directory: the metadata tablet's fence covers that.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    bytes_stream, slice_range, BlobBackend, BoxByteStream, CostHint, InstanceInfo, InstanceKind,
    InstanceRole, OpKind, Sweepable,
};
use pigeonhole_types::{BackendLimits, ByteRange};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Identity of a blob on disk: its key, which is also its file name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskId {
    pub key: u64,
}

pub fn disk_location(dir: &Path) -> String {
    format!("disk:{}", dir.display())
}

pub struct DiskBlobStore {
    dir: Arc<PathBuf>,
    instance: InstanceInfo,
    limits: BackendLimits,
    next: AtomicU64,
}

fn blob_path(dir: &Path, key: u64) -> PathBuf {
    dir.join(format!("{key:020}.blob"))
}

/// Keys of the blobs (and torn puts) in `dir`, sorted.
fn keys_on_disk(dir: &Path) -> Result<Vec<u64>> {
    let mut keys = Vec::new();
    for e in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let name = e?.file_name();
        let key = name
            .to_str()
            .and_then(|n| n.strip_suffix(".blob").or_else(|| n.strip_suffix(".tmp")))
            .and_then(|n| n.parse::<u64>().ok());
        keys.extend(key);
    }
    keys.sort_unstable();
    keys.dedup();
    Ok(keys)
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
        let dir = dir.into();
        fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let next = keys_on_disk(&dir)?.last().map_or(1, |k| k + 1);
        Ok(Self { dir: Arc::new(dir), instance: info, limits: BackendLimits::local_disk(), next: AtomicU64::new(next) })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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

    async fn put(&self, data: Bytes, _name: &str) -> Result<Self::Id> {
        if data.is_empty() {
            bail!("refusing an empty blob");
        }
        if data.len() > self.limits.max_blob_size {
            bail!("blob size {} exceeds backend max {}", data.len(), self.limits.max_blob_size);
        }
        let key = self.next.fetch_add(1, Ordering::SeqCst);
        let dir = self.dir.clone();
        blocking(move || {
            let tmp = dir.join(format!("{key:020}.tmp"));
            let mut f = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
            f.write_all(&data)?;
            f.sync_all()?;
            fs::rename(&tmp, blob_path(&dir, key))?;
            File::open(&*dir)?.sync_all()?;
            Ok(())
        })
        .await?;
        Ok(DiskId { key })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let path = blob_path(&self.dir, id.key);
        let data = blocking(move || fs::read(&path).with_context(|| format!("read {}", path.display()))).await?;
        Ok(bytes_stream(slice_range(Bytes::from(data), range)?))
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        let dir = self.dir.clone();
        let keys = keys.to_vec();
        blocking(move || {
            for key in keys {
                for path in [blob_path(&dir, key), dir.join(format!("{key:020}.tmp"))] {
                    match fs::remove_file(&path) {
                        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                            return Err(e).with_context(|| format!("delete {}", path.display()));
                        }
                        _ => {}
                    }
                }
            }
            Ok(())
        })
        .await
    }
}

#[async_trait]
impl Sweepable for DiskBlobStore {
    async fn candidates(&self, after: Option<Self::Key>, upto: Self::Key, limit: usize) -> Result<Vec<Self::Key>> {
        let dir = self.dir.clone();
        blocking(move || {
            let mut keys = keys_on_disk(&dir)?;
            keys.retain(|&k| k <= upto && after.is_none_or(|a| k > a));
            keys.truncate(limit);
            Ok(keys)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_blob::collect_stream;

    #[tokio::test]
    async fn keys_keep_growing_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let a = DiskBlobStore::open(dir.path()).unwrap();
        let first = a.put(Bytes::from_static(b"one"), "x").await.unwrap();
        let second = a.put(Bytes::from_static(b"two"), "x").await.unwrap();
        a.delete(&[second.key]).await.unwrap();

        let b = DiskBlobStore::open(dir.path()).unwrap();
        let third = b.put(Bytes::from_static(b"three"), "x").await.unwrap();
        assert!(third.key > first.key);
        let got = collect_stream(b.get(&first, None).await.unwrap()).await.unwrap();
        assert_eq!(got.as_ref(), b"one");
    }

    #[tokio::test]
    async fn a_torn_put_is_an_orphan_the_sweeper_sees() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskBlobStore::open(dir.path()).unwrap();
        fs::write(dir.path().join(format!("{:020}.tmp", 7)), b"half").unwrap();
        assert_eq!(store.candidates(None, 10, 100).await.unwrap(), vec![7]);
        store.delete(&[7]).await.unwrap();
        assert!(store.candidates(None, 10, 100).await.unwrap().is_empty());
    }
}
