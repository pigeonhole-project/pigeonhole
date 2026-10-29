//! Where tablet snapshots live: a [`SnapshotStore`], configured apart from the
//! log (`[meta] snapshot`).
//!
//! The log records which snapshot it starts from ([`LogStore::rebase`] with
//! the [`SnapshotRef`] `put` returned), so a boot loads exactly that one and
//! never guesses. [`SnapshotStore::newest`] is only for a lossy restore after
//! the log is gone. The tablet removes a snapshot once the log no longer
//! starts from it; a store never drops snapshots on its own.
//!
//! [`LogStore::rebase`]: crate::LogStore::rebase

use crate::{LogId, Position};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use bytes::Bytes;
use pigeonhole_storage_api::{collect_stream, BlobBackend, Chain};

/// Version of the stored envelope ([`StoredSnapshot`] on disk / in a chat).
pub const SNAPSHOT_STORE_FORMAT: u32 = 1;

/// A tablet snapshot as stored: the log it belongs to, the position it
/// covers, and the encoded [`crate::TabletSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub log_id: LogId,
    pub at: Position,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    format: u32,
    snapshot: StoredSnapshot,
}

impl StoredSnapshot {
    /// `crc32 | postcard(Envelope)`: a damaged copy is detected, not misread.
    pub fn encode(&self) -> Vec<u8> {
        let payload = postcard::to_stdvec(&Envelope { format: SNAPSHOT_STORE_FORMAT, snapshot: self.clone() })
            .expect("a snapshot always encodes");
        let mut out = crc32fast::hash(&payload).to_le_bytes().to_vec();
        out.extend_from_slice(&payload);
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let (crc, payload) = data.split_at_checked(4).context("stored snapshot too short")?;
        if crc32fast::hash(payload) != u32::from_le_bytes(crc.try_into().unwrap()) {
            bail!("stored snapshot fails its checksum");
        }
        let (format, _) = postcard::take_from_bytes::<u32>(payload).context("stored snapshot header")?;
        if format != SNAPSHOT_STORE_FORMAT {
            bail!("stored snapshot in format {format}, this binary reads {SNAPSHOT_STORE_FORMAT}");
        }
        Ok(postcard::from_bytes::<Envelope>(payload).context("decode stored snapshot")?.snapshot)
    }
}

/// Opaque handle of a stored snapshot, kept in the log.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotRef(pub String);

#[async_trait]
pub trait SnapshotStore: Send + Sync + 'static {
    /// Durably store `snapshot`; `Ok` = [`Self::get`] returns it from now on.
    async fn put(&self, snapshot: &StoredSnapshot) -> Result<SnapshotRef>;
    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot>;
    /// The most recently stored snapshot, of any log (lossy restore only).
    async fn newest(&self) -> Result<Option<StoredSnapshot>>;
    /// Drop a snapshot nothing starts from any more (best effort).
    async fn remove(&self, r: &SnapshotRef) -> Result<()>;
}

/// Labels of snapshot blobs: `snapshot-<log id>-<at>.snap`.
const SNAPSHOT_PREFIX: &str = "snapshot-";

/// Snapshots as blobs of a [`Chain`] store: no order or fence is needed (a
/// snapshot counts only while the log's base names it); only a lossy
/// restore asks which one is newest ([`Chain::newest_loose`]: by listing
/// where the store can, else by the chain). The ref is the blob
/// id (postcard, hex) and its crc, so a replaced blob is detected.
pub struct BlobSnapshotStore<C> {
    chain: Arc<C>,
}

impl<C: Chain> BlobSnapshotStore<C> {
    pub fn new(chain: Arc<C>) -> Self {
        Self { chain }
    }

    fn reference(id: &C::Id, crc: u32) -> SnapshotRef {
        let bytes = postcard::to_stdvec(id).expect("a blob id encodes");
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        SnapshotRef(format!("{hex}:{crc:08x}"))
    }

    fn parse(r: &SnapshotRef) -> Result<(C::Id, u32)> {
        let bad = || anyhow!("{:?} is not a blob snapshot", r.0);
        let (hex, crc) = r.0.split_once(':').ok_or_else(bad)?;
        if hex.len() % 2 != 0 {
            return Err(bad());
        }
        let bytes = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
            .collect::<Result<Vec<u8>, _>>()
            .map_err(|_| bad())?;
        let id = postcard::from_bytes(&bytes).map_err(|_| bad())?;
        Ok((id, u32::from_str_radix(crc, 16).map_err(|_| bad())?))
    }

    async fn download(&self, id: &C::Id) -> Result<Vec<u8>> {
        Ok(collect_stream(self.chain.get(id, None).await?).await?.to_vec())
    }
}

#[async_trait]
impl<C: Chain> SnapshotStore for BlobSnapshotStore<C> {
    async fn put(&self, snapshot: &StoredSnapshot) -> Result<SnapshotRef> {
        let data = Bytes::from(snapshot.encode());
        let crc = crc32fast::hash(&data);
        let label = format!("{SNAPSHOT_PREFIX}{}-{}.snap", snapshot.log_id, snapshot.at);
        let id = self.chain.put_loose(&label, data).await?;
        Ok(Self::reference(&id, crc))
    }

    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot> {
        let (id, crc) = Self::parse(r)?;
        let data = self.download(&id).await.with_context(|| format!("download snapshot {:?}", r.0))?;
        if crc32fast::hash(&data) != crc {
            bail!("snapshot blob {:?} changed", r.0);
        }
        StoredSnapshot::decode(&data).with_context(|| format!("decode snapshot {:?}", r.0))
    }

    /// The most recently stored snapshot.
    async fn newest(&self) -> Result<Option<StoredSnapshot>> {
        let Some(tail) = self.chain.newest_loose(SNAPSHOT_PREFIX).await? else { return Ok(None) };
        let data = self.download(&tail.id).await.with_context(|| format!("download snapshot {:?}", tail.label))?;
        Ok(Some(StoredSnapshot::decode(&data).with_context(|| format!("decode snapshot {:?}", tail.label))?))
    }

    async fn remove(&self, r: &SnapshotRef) -> Result<()> {
        let (id, _) = Self::parse(r)?;
        self.chain.delete(&[C::key(&id)]).await
    }
}

/// Contract every [`SnapshotStore`] passes; `open` returns a new handle onto
/// the same (initially empty) store each call.
#[cfg(any(test, feature = "testsuite"))]
pub mod conformance {
    use super::*;
    use std::future::Future;

    fn snap(log: u128, at: Position) -> StoredSnapshot {
        StoredSnapshot { log_id: LogId(log), at, data: format!("state {log}/{at}").into_bytes() }
    }

    pub async fn run_all<Open, Fut>(open: Open)
    where
        Open: Fn() -> Fut,
        Fut: Future<Output = Arc<dyn SnapshotStore>>,
    {
        let store = open().await;
        assert_eq!(store.newest().await.unwrap(), None, "a new store is empty");
        let r1 = store.put(&snap(1, 10)).await.unwrap();
        let r2 = store.put(&snap(1, 20)).await.unwrap();
        assert_ne!(r1, r2);

        let again = open().await;
        assert_eq!(again.get(&r1).await.unwrap(), snap(1, 10), "a put survives a new handle");
        assert_eq!(again.get(&r2).await.unwrap(), snap(1, 20));
        assert_eq!(again.newest().await.unwrap(), Some(snap(1, 20)));

        // A chat may keep serving a deleted message's file: only check that
        // removing one leaves the others.
        again.remove(&r1).await.unwrap();
        assert_eq!(again.get(&r2).await.unwrap(), snap(1, 20), "only that one");

        // A restored log starts over at 0: newest means most recently stored.
        let r3 = again.put(&snap(2, 0)).await.unwrap();
        assert_eq!(again.newest().await.unwrap(), Some(snap(2, 0)));
        again.remove(&r3).await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_blob_store_passes_the_conformance_suite_on_disk() {
        let disk = Arc::new(pigeonhole_storage_disk::DiskBlobStore::open(tempfile::tempdir().unwrap().keep()).unwrap());
        conformance::run_all(|| {
            let disk = disk.clone();
            async move { Arc::new(BlobSnapshotStore::new(disk)) as Arc<dyn SnapshotStore> }
        })
        .await;
    }

    #[tokio::test]
    async fn blob_store_passes_the_conformance_suite() {
        let mem = Arc::new(pigeonhole_storage_memory::MemoryBlobStore::new());
        conformance::run_all(|| {
            let mem = mem.clone();
            async move { Arc::new(BlobSnapshotStore::new(mem)) as Arc<dyn SnapshotStore> }
        })
        .await;
    }

    #[tokio::test]
    async fn the_blob_store_detects_a_replaced_snapshot_and_a_bad_ref() {
        use pigeonhole_storage_memory::MemoryBlobStore;
        let mem = Arc::new(MemoryBlobStore::new());
        let store = BlobSnapshotStore::new(mem.clone());
        let r = store.put(&StoredSnapshot { log_id: LogId(1), at: 1, data: vec![1] }).await.unwrap();
        let (id, crc) = BlobSnapshotStore::<MemoryBlobStore>::parse(&r).unwrap();
        let forged = BlobSnapshotStore::<MemoryBlobStore>::reference(&id, crc ^ 1);
        assert!(format!("{:#}", store.get(&forged).await.unwrap_err()).contains("changed"));
        assert!(store.get(&SnapshotRef("zz:1".into())).await.is_err());
    }

    #[tokio::test]
    async fn a_damaged_snapshot_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobSnapshotStore::new(Arc::new(pigeonhole_storage_disk::DiskBlobStore::open(dir.path()).unwrap()));
        store.put(&StoredSnapshot { log_id: LogId(7), at: 3, data: vec![1, 2, 3] }).await.unwrap();
        let r = store.put(&StoredSnapshot { log_id: LogId(7), at: 4, data: vec![1, 2, 3] }).await.unwrap();
        let seg = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.file_name().unwrap().to_string_lossy().starts_with("seg-"))
            .unwrap();
        let mut data = std::fs::read(&seg).unwrap();
        *data.last_mut().unwrap() ^= 0xff;
        std::fs::write(&seg, data).unwrap();
        let err = store.get(&r).await.unwrap_err();
        assert!(format!("{err:#}").contains("changed"), "{err:#}");
    }
}
