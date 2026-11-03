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
use bytes::Bytes;
use pigeonhole_storage_api::{collect_stream, BlobBackend, Chain};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Version of the stored envelope ([`StoredSnapshot`] on disk / in a chat).
pub const SNAPSHOT_STORE_FORMAT: u32 = 2;

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
    body: Body,
}

/// What a stored blob holds: a whole snapshot, or the list of the parts the
/// encoded whole was cut into, for a store whose blobs are smaller.
#[derive(Clone, Serialize, Deserialize)]
enum Body {
    Whole(StoredSnapshot),
    Parts(Vec<Part>),
}

/// One part of a cut snapshot: its blob (postcard of the store's id), length
/// and crc.
#[derive(Clone, Serialize, Deserialize)]
struct Part {
    id: Vec<u8>,
    len: u64,
    crc: u32,
}

impl Body {
    /// `crc32 | postcard(Envelope)`: a damaged copy is detected, not misread.
    fn encode(&self) -> Vec<u8> {
        let payload = postcard::to_stdvec(&Envelope { format: SNAPSHOT_STORE_FORMAT, body: self.clone() })
            .expect("a snapshot always encodes");
        let mut out = crc32fast::hash(&payload).to_le_bytes().to_vec();
        out.extend_from_slice(&payload);
        out
    }

    fn decode(data: &[u8]) -> Result<Self> {
        let (crc, payload) = data.split_at_checked(4).context("stored snapshot too short")?;
        if crc32fast::hash(payload) != u32::from_le_bytes(crc.try_into().unwrap()) {
            bail!("stored snapshot fails its checksum");
        }
        let (format, _) = postcard::take_from_bytes::<u32>(payload).context("stored snapshot header")?;
        if format != SNAPSHOT_STORE_FORMAT {
            bail!("stored snapshot in format {format}, this binary reads {SNAPSHOT_STORE_FORMAT}");
        }
        Ok(postcard::from_bytes::<Envelope>(payload).context("decode stored snapshot")?.body)
    }
}

impl StoredSnapshot {
    /// The snapshot as one blob (see [`Body`]).
    pub fn encode(&self) -> Vec<u8> {
        Body::Whole(self.clone()).encode()
    }

    /// A snapshot stored whole; a cut one is read through its store.
    pub fn decode(data: &[u8]) -> Result<Self> {
        match Body::decode(data)? {
            Body::Whole(s) => Ok(s),
            Body::Parts(_) => bail!("a snapshot cut into parts, read through its store"),
        }
    }
}

/// Opaque handle of a stored snapshot, kept in the log.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotRef(pub String);

#[async_trait]
pub trait SnapshotStore: Send + Sync + 'static {
    /// Durably store `snapshot`; `Ok` = [`Self::get`] returns it from now on.
    /// `after`: the newest snapshot the caller knows this store holds (the
    /// one it put last, or its log starts from), a hint that may save the
    /// store a look for its newest.
    async fn put(&self, snapshot: &StoredSnapshot, after: Option<&SnapshotRef>) -> Result<SnapshotRef>;
    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot>;
    /// The most recently stored snapshot, of any log (lossy restore only).
    async fn newest(&self) -> Result<Option<StoredSnapshot>>;
    /// Drop a snapshot nothing starts from any more (best effort).
    async fn remove(&self, r: &SnapshotRef) -> Result<()>;
}

/// Labels of snapshot blobs: `snapshot-<log id>-<at>.snap`; of the parts of
/// a cut one, `snappart-<log id>-<at>-<n>.bin` (not found as the newest).
const SNAPSHOT_PREFIX: &str = "snapshot-";

/// Snapshots as blobs of a [`Chain`] store: no order or fence is needed (a
/// snapshot counts only while the log's base names it); only a lossy
/// restore asks which one is newest ([`Chain::newest_loose`]: by listing
/// where the store can, else by the chain, each put linked after the one
/// before it). The ref is the blob
/// id (postcard, hex) and its crc, so a replaced blob is detected.
///
/// A snapshot larger than the store's blobs (`max_blob_size`: Discord's
/// 20 MiB) is cut: its parts are put as plain blobs first, then the blob
/// the ref names lists them with their crcs. A failed put leaves its parts
/// behind, as a failed whole put leaves its blob.
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

    /// The blob a snapshot ref names (tests that damage it underneath).
    pub fn blob_id(r: &SnapshotRef) -> Result<C::Id> {
        Ok(Self::parse(r)?.0)
    }

    async fn download(&self, id: &C::Id) -> Result<Vec<u8>> {
        Ok(collect_stream(self.chain.get(id, None).await?).await?.to_vec())
    }

    /// The snapshot a stored blob holds, its parts read and checked.
    async fn read(&self, data: &[u8]) -> Result<StoredSnapshot> {
        match Body::decode(data)? {
            Body::Whole(s) => Ok(s),
            Body::Parts(parts) => StoredSnapshot::decode(&self.join(&parts).await?),
        }
    }

    /// The parts' bytes in order, each checked against the list.
    async fn join(&self, parts: &[Part]) -> Result<Vec<u8>> {
        let mut whole = Vec::with_capacity(parts.iter().map(|p| p.len as usize).sum());
        for (n, p) in parts.iter().enumerate() {
            let data = self.download(&Self::part_id(p)?).await.with_context(|| format!("download part {n}"))?;
            if data.len() as u64 != p.len || crc32fast::hash(&data) != p.crc {
                bail!("snapshot part {n} changed");
            }
            whole.extend_from_slice(&data);
        }
        Ok(whole)
    }

    fn part_id(p: &Part) -> Result<C::Id> {
        postcard::from_bytes(&p.id).context("a snapshot part is not a blob id")
    }

    /// The blob for `snapshot`: whole if the store takes it, else the list
    /// of the parts it is cut into (put first). The list must fit in one blob:
    /// ~15 bytes a part, so 20 MiB lists over a million of them.
    async fn cut(&self, snapshot: &StoredSnapshot) -> Result<Vec<u8>> {
        let whole = snapshot.encode();
        let max = self.chain.limits().max_blob_size.max(1);
        if whole.len() <= max {
            return Ok(whole);
        }
        let list = Body::Parts(self.put_parts(snapshot, &whole, max).await?).encode();
        if list.len() > max {
            bail!("the list of a snapshot's parts takes {} bytes, over the store's {max}", list.len());
        }
        Ok(list)
    }

    async fn put_parts(&self, snapshot: &StoredSnapshot, whole: &[u8], max: usize) -> Result<Vec<Part>> {
        let mut parts = Vec::with_capacity(whole.len().div_ceil(max));
        for (n, piece) in whole.chunks(max).enumerate() {
            let label = format!("snappart-{}-{}-{n}.bin", snapshot.log_id, snapshot.at);
            let id =
                self.chain.put(Bytes::copy_from_slice(piece), &label).await.with_context(|| format!("put {label}"))?;
            let id = postcard::to_stdvec(&id).expect("a blob id encodes");
            parts.push(Part { id, len: piece.len() as u64, crc: crc32fast::hash(piece) });
        }
        Ok(parts)
    }
}

#[async_trait]
impl<C: Chain> SnapshotStore for BlobSnapshotStore<C> {
    /// Linked after `after` where the store links ([`Chain::put_loose`]):
    /// on a channel, no post to learn its top. A ref of another kind is no
    /// hint.
    async fn put(&self, snapshot: &StoredSnapshot, after: Option<&SnapshotRef>) -> Result<SnapshotRef> {
        let data = Bytes::from(self.cut(snapshot).await?);
        let crc = crc32fast::hash(&data);
        let label = format!("{SNAPSHOT_PREFIX}{}-{}.snap", snapshot.log_id, snapshot.at);
        let after = after.and_then(|r| Self::parse(r).ok()).map(|(id, _)| id);
        let id = self.chain.put_loose(after.as_ref(), &label, data).await?;
        Ok(Self::reference(&id, crc))
    }

    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot> {
        let (id, crc) = Self::parse(r)?;
        let data = self.download(&id).await.with_context(|| format!("download snapshot {:?}", r.0))?;
        if crc32fast::hash(&data) != crc {
            bail!("snapshot blob {:?} changed", r.0);
        }
        self.read(&data).await.with_context(|| format!("decode snapshot {:?}", r.0))
    }

    /// The most recently stored snapshot.
    async fn newest(&self) -> Result<Option<StoredSnapshot>> {
        let Some(tail) = self.chain.newest_loose(SNAPSHOT_PREFIX).await? else { return Ok(None) };
        let data = self.download(&tail.id).await.with_context(|| format!("download snapshot {:?}", tail.label))?;
        Ok(Some(self.read(&data).await.with_context(|| format!("decode snapshot {:?}", tail.label))?))
    }

    /// A cut snapshot's parts go with it, if its list can still be read.
    async fn remove(&self, r: &SnapshotRef) -> Result<()> {
        let (id, _) = Self::parse(r)?;
        let mut keys = vec![C::key(&id)];
        if let Ok(Body::Parts(parts)) = self.download(&id).await.and_then(|d| Body::decode(&d)) {
            keys.extend(parts.iter().filter_map(|p| Self::part_id(p).ok()).map(|id| C::key(&id)));
        }
        self.chain.delete(&keys).await
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
        let r1 = store.put(&snap(1, 10), None).await.unwrap();
        let r2 = store.put(&snap(1, 20), Some(&r1)).await.unwrap();
        assert_ne!(r1, r2);

        let again = open().await;
        assert_eq!(again.get(&r1).await.unwrap(), snap(1, 10), "a put survives a new handle");
        assert_eq!(again.get(&r2).await.unwrap(), snap(1, 20));
        assert_eq!(again.newest().await.unwrap(), Some(snap(1, 20)));

        // A stale hint (a newer one followed it) still makes the newest.
        let r3 = again.put(&snap(1, 30), Some(&r1)).await.unwrap();
        assert_eq!(again.get(&r3).await.unwrap(), snap(1, 30));
        assert_eq!(again.newest().await.unwrap(), Some(snap(1, 30)));

        // A chat may keep serving a deleted message's file: only check that
        // removing one leaves the others.
        again.remove(&r1).await.unwrap();
        assert_eq!(again.get(&r2).await.unwrap(), snap(1, 20), "only that one");
        let r4 = again.put(&snap(1, 40), Some(&r3)).await.unwrap();
        assert_eq!(again.newest().await.unwrap(), Some(snap(1, 40)));

        // A restored log starts over at 0: newest means most recently stored.
        let r5 = again.put(&snap(2, 0), None).await.unwrap();
        assert_eq!(again.newest().await.unwrap(), Some(snap(2, 0)));
        for r in [r2, r3, r4, r5] {
            again.remove(&r).await.unwrap();
        }
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

    fn small_blobs(max_blob_size: usize) -> pigeonhole_storage_memory::MemoryBlobStore {
        pigeonhole_storage_memory::MemoryBlobStore::with_limits(pigeonhole_types::BackendLimits {
            max_blob_size,
            ..pigeonhole_storage_memory::BLOB_LIMITS
        })
    }

    /// Discord's case: a snapshot over the store's blob size goes up in
    /// parts, reads back whole, and leaves nothing behind once removed.
    #[tokio::test]
    async fn a_snapshot_over_the_blob_size_is_cut_and_removed_whole() {
        let mem = Arc::new(small_blobs(1000));
        let store = BlobSnapshotStore::new(mem.clone());
        let data: Vec<u8> = (0..10_000u32).map(|i| (i * 7 % 251) as u8).collect();
        let snap = StoredSnapshot { log_id: LogId(5), at: 9, data };
        let r = store.put(&snap, None).await.unwrap();
        assert_eq!(mem.message_keys().len(), 11 + 1, "eleven parts and a list");
        assert_eq!(store.get(&r).await.unwrap(), snap);
        assert_eq!(store.newest().await.unwrap().as_ref(), Some(&snap));
        store.remove(&r).await.unwrap();
        assert!(mem.message_keys().is_empty(), "left behind {:?}", mem.message_keys());
    }

    /// Blobs too small to hold the list of a snapshot's parts.
    #[tokio::test]
    async fn blobs_too_small_for_the_list_are_refused() {
        let store = BlobSnapshotStore::new(Arc::new(small_blobs(16)));
        let err = store.put(&StoredSnapshot { log_id: LogId(1), at: 1, data: vec![0; 100] }, None).await.unwrap_err();
        assert!(format!("{err:#}").contains("the list of a snapshot's parts"), "{err:#}");
    }

    #[tokio::test]
    async fn a_missing_part_is_an_error_not_a_snapshot() {
        let mem = Arc::new(small_blobs(1000));
        let store = BlobSnapshotStore::new(mem.clone());
        let r = store.put(&StoredSnapshot { log_id: LogId(5), at: 9, data: vec![3; 5000] }, None).await.unwrap();
        let head = BlobSnapshotStore::<pigeonhole_storage_memory::MemoryBlobStore>::blob_id(&r).unwrap();
        let head = pigeonhole_storage_memory::MemoryBlobStore::key(&head);
        let part = *mem.message_keys().iter().find(|&&k| k != head).unwrap();
        mem.delete(&[part]).await.unwrap();
        let err = store.get(&r).await.unwrap_err();
        assert!(format!("{err:#}").contains("part"), "{err:#}");
    }

    #[tokio::test]
    async fn the_blob_store_detects_a_replaced_snapshot_and_a_bad_ref() {
        use pigeonhole_storage_memory::MemoryBlobStore;
        let mem = Arc::new(MemoryBlobStore::new());
        let store = BlobSnapshotStore::new(mem.clone());
        let r = store.put(&StoredSnapshot { log_id: LogId(1), at: 1, data: vec![1] }, None).await.unwrap();
        let (id, crc) = BlobSnapshotStore::<MemoryBlobStore>::parse(&r).unwrap();
        let forged = BlobSnapshotStore::<MemoryBlobStore>::reference(&id, crc ^ 1);
        assert!(format!("{:#}", store.get(&forged).await.unwrap_err()).contains("changed"));
        assert!(store.get(&SnapshotRef("zz:1".into())).await.is_err());
    }

    #[tokio::test]
    async fn a_damaged_snapshot_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobSnapshotStore::new(Arc::new(pigeonhole_storage_disk::DiskBlobStore::open(dir.path()).unwrap()));
        store.put(&StoredSnapshot { log_id: LogId(7), at: 3, data: vec![1, 2, 3] }, None).await.unwrap();
        let r = store.put(&StoredSnapshot { log_id: LogId(7), at: 4, data: vec![1, 2, 3] }, None).await.unwrap();
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
