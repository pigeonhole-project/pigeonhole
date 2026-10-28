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

use crate::log::channel::{ChannelApi, ChannelMsg, PAGE};
use crate::{LogId, Position};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use bytes::Bytes;
use pigeonhole_storage_api::{collect_stream, After, BlobBackend, Chain};

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

/// `snapshot-<log id>-<at>.snap` files in a directory; may be the log's own.
pub struct LocalSnapshotStore {
    dir: PathBuf,
}

impl LocalSnapshotStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn path(&self, r: &SnapshotRef) -> Result<PathBuf> {
        let name = &r.0;
        if !(name.starts_with("snapshot-") && name.ends_with(".snap")) || name.contains(['/', '\\']) {
            bail!("{name:?} is not a local snapshot");
        }
        Ok(self.dir.join(name))
    }

    fn write(&self, snapshot: &StoredSnapshot) -> Result<SnapshotRef> {
        fs::create_dir_all(&self.dir).with_context(|| format!("create {}", self.dir.display()))?;
        let name = format!("snapshot-{}-{:020}.snap", snapshot.log_id, snapshot.at);
        let tmp = self.dir.join(format!("{name}.tmp"));
        let mut f = File::create(&tmp)?;
        f.write_all(&snapshot.encode())?;
        f.sync_all()?;
        fs::rename(&tmp, self.dir.join(&name))?;
        File::open(&self.dir)?.sync_all()?;
        Ok(SnapshotRef(name))
    }

    fn newest_file(&self) -> Result<Option<PathBuf>> {
        if !self.dir.exists() {
            return Ok(None);
        }
        let mut best = None;
        for e in fs::read_dir(&self.dir)? {
            let e = e?;
            let name = e.file_name().to_string_lossy().into_owned();
            if !(name.starts_with("snapshot-") && name.ends_with(".snap")) {
                continue;
            }
            let key = (e.metadata()?.modified()?, name);
            if best.as_ref().is_none_or(|(k, _)| key > *k) {
                best = Some((key, e.path()));
            }
        }
        Ok(best.map(|(_, p)| p))
    }
}

#[async_trait]
impl SnapshotStore for LocalSnapshotStore {
    async fn put(&self, snapshot: &StoredSnapshot) -> Result<SnapshotRef> {
        let this = Self::new(self.dir.clone());
        let snapshot = snapshot.clone();
        tokio::task::spawn_blocking(move || this.write(&snapshot)).await?
    }

    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot> {
        let path = self.path(r)?;
        let data = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        StoredSnapshot::decode(&data).with_context(|| format!("decode {}", path.display()))
    }

    async fn newest(&self) -> Result<Option<StoredSnapshot>> {
        let Some(path) = self.newest_file()? else {
            return Ok(None);
        };
        let data = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        Ok(Some(StoredSnapshot::decode(&data).with_context(|| format!("decode {}", path.display()))?))
    }

    async fn remove(&self, r: &SnapshotRef) -> Result<()> {
        match fs::remove_file(self.path(r)?) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }
}

/// Snapshots as blobs of any store that can chain them ([`Chain`]): no
/// order or fence is needed (a snapshot counts only while the log's base
/// names it), the chain only tells which one is newest. The ref is the blob
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
        let label = format!("snapshot-{}-{}.snap", snapshot.log_id, snapshot.at);
        // Another writer may link first: put after the new tail then.
        loop {
            let tail = self.chain.tail().await?;
            match self.chain.put_after(tail.as_ref().map(|t| &t.id), &label, data.clone()).await? {
                After::Won(id) => return Ok(Self::reference(&id, crc)),
                After::Lost(_) => continue,
            }
        }
    }

    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot> {
        let (id, crc) = Self::parse(r)?;
        let data = self.download(&id).await.with_context(|| format!("download snapshot {:?}", r.0))?;
        if crc32fast::hash(&data) != crc {
            bail!("snapshot blob {:?} changed", r.0);
        }
        StoredSnapshot::decode(&data).with_context(|| format!("decode snapshot {:?}", r.0))
    }

    /// The tail of the chain: the most recently stored snapshot.
    async fn newest(&self) -> Result<Option<StoredSnapshot>> {
        let Some(tail) = self.chain.tail().await? else { return Ok(None) };
        let data = self.download(&tail.id).await.with_context(|| format!("download snapshot {:?}", tail.label))?;
        Ok(Some(StoredSnapshot::decode(&data).with_context(|| format!("decode snapshot {:?}", tail.label))?))
    }

    async fn remove(&self, r: &SnapshotRef) -> Result<()> {
        let (id, _) = Self::parse(r)?;
        self.chain.delete(&[C::key(&id)]).await
    }
}

/// Snapshots as documents in a channel read by id (no order, no fence: a
/// snapshot counts only while the log's base names it). The ref is the
/// message id and the document's crc, so a replaced message is detected.
pub struct ChannelSnapshotStore<C> {
    chat: Arc<C>,
}

const SNAPSHOT_PROBE: &str = "snapshot-probe.bin";

impl<C: ChannelApi> ChannelSnapshotStore<C> {
    pub fn new(chat: Arc<C>) -> Self {
        Self { chat }
    }

    fn parse(r: &SnapshotRef) -> Result<(i64, u32)> {
        let bad = || anyhow!("{:?} is not a channel snapshot", r.0);
        let (id, crc) = r.0.split_once(':').ok_or_else(bad)?;
        Ok((id.parse().map_err(|_| bad())?, u32::from_str_radix(crc, 16).map_err(|_| bad())?))
    }

    fn is_snapshot(msg: &ChannelMsg) -> bool {
        matches!(msg, ChannelMsg::Doc { name } if name.starts_with("snapshot-") && name.ends_with(".snap"))
    }
}

#[async_trait]
impl<C: ChannelApi> SnapshotStore for ChannelSnapshotStore<C> {
    async fn put(&self, snapshot: &StoredSnapshot) -> Result<SnapshotRef> {
        let data = snapshot.encode();
        let crc = crc32fast::hash(&data);
        let name = format!("snapshot-{}-{}.snap", snapshot.log_id, snapshot.at);
        let id = self.chat.send_document(&name, data).await?;
        Ok(SnapshotRef(format!("{id}:{crc:08x}")))
    }

    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot> {
        let (id, crc) = Self::parse(r)?;
        let data = self.chat.download(id).await.with_context(|| format!("download snapshot {id}"))?;
        if crc32fast::hash(&data) != crc {
            bail!("snapshot message {id} changed");
        }
        StoredSnapshot::decode(&data).with_context(|| format!("decode snapshot {id}"))
    }

    /// The highest message that is a snapshot. The top of the channel is
    /// only learnt by posting: a probe, deleted again.
    async fn newest(&self) -> Result<Option<StoredSnapshot>> {
        let probe = self.chat.send_document(SNAPSHOT_PROBE, vec![0]).await?;
        let _ = self.chat.delete(&[probe]).await;
        let mut hi = probe - 1;
        while hi >= 1 {
            let lo = (hi - PAGE as i64 + 1).max(1);
            let ids: Vec<i64> = (lo..=hi).collect();
            let msgs = self.chat.get_messages(&ids).await?;
            for (id, msg) in ids.iter().zip(msgs).rev() {
                if Self::is_snapshot(&msg) {
                    let data = self.chat.download(*id).await.with_context(|| format!("download snapshot {id}"))?;
                    return Ok(Some(StoredSnapshot::decode(&data).with_context(|| format!("decode snapshot {id}"))?));
                }
            }
            hi = lo - 1;
        }
        Ok(None)
    }

    async fn remove(&self, r: &SnapshotRef) -> Result<()> {
        self.chat.delete(&[Self::parse(r)?.0]).await
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
    use crate::log::channel::fake::FakeChannel;

    #[tokio::test]
    async fn local_store_passes_the_conformance_suite() {
        let dir = Arc::new(tempfile::tempdir().unwrap());
        conformance::run_all(|| {
            let dir = dir.clone();
            async move { Arc::new(LocalSnapshotStore::new(dir.path())) as Arc<dyn SnapshotStore> }
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
    async fn channel_store_passes_the_conformance_suite() {
        let chat = Arc::new(FakeChannel::default());
        conformance::run_all(|| {
            let chat = chat.clone();
            async move { Arc::new(ChannelSnapshotStore::new(chat)) as Arc<dyn SnapshotStore> }
        })
        .await;
    }

    #[tokio::test]
    async fn the_channel_store_skips_other_messages_and_detects_a_replaced_one() {
        let chat = Arc::new(FakeChannel::default());
        let store = ChannelSnapshotStore::new(chat.clone());
        let snap = StoredSnapshot { log_id: LogId(1), at: 1, data: vec![1] };
        let r = store.put(&snap).await.unwrap();
        chat.post_text();
        chat.post_document("photo.jpg", b"x");
        assert_eq!(store.newest().await.unwrap(), Some(snap));
        chat.edit(ChannelSnapshotStore::<FakeChannel>::parse(&r).unwrap().0);
        assert!(format!("{:#}", store.get(&r).await.unwrap_err()).contains("changed"));
        assert!(store.get(&SnapshotRef("12 x".into())).await.is_err());
    }

    #[tokio::test]
    async fn a_damaged_snapshot_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSnapshotStore::new(dir.path());
        let r = store.put(&StoredSnapshot { log_id: LogId(7), at: 3, data: vec![1, 2, 3] }).await.unwrap();
        let path = dir.path().join(&r.0);
        let mut data = fs::read(&path).unwrap();
        *data.last_mut().unwrap() ^= 0xff;
        fs::write(&path, data).unwrap();
        let err = store.get(&r).await.unwrap_err();
        assert!(format!("{err:#}").contains("checksum"), "{err:#}");
        assert!(store.get(&SnapshotRef("../etc/passwd".into())).await.is_err());
    }
}
