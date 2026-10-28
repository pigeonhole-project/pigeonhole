//! Where tablet snapshots live: a [`SnapshotStore`], configured apart from the
//! log (`[meta.snapshot]`).
//!
//! The log records which snapshot it starts from ([`LogStore::rebase`] with
//! the [`SnapshotRef`] `put` returned), so a boot loads exactly that one and
//! never guesses. [`SnapshotStore::newest`] is only for a lossy restore after
//! the log is gone. The tablet removes a snapshot once the log no longer
//! starts from it; a store never drops snapshots on its own.
//!
//! [`LogStore::rebase`]: crate::LogStore::rebase

use crate::log::chat::ChatApi;
use crate::{LogId, Position};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

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

/// Snapshots as documents in a dedicated chat; the newest is pinned, so a
/// restore finds it without reading the history.
pub struct ChatSnapshotStore<C> {
    chat: Arc<C>,
}

impl<C: ChatApi> ChatSnapshotStore<C> {
    pub fn new(chat: Arc<C>) -> Self {
        Self { chat }
    }
}

fn chat_ref(message_id: i64, download_ref: &str) -> SnapshotRef {
    SnapshotRef(format!("{message_id} {download_ref}"))
}

fn parse_chat_ref(r: &SnapshotRef) -> Result<(i64, &str)> {
    let (id, download_ref) = r.0.split_once(' ').ok_or_else(|| anyhow!("{:?} is not a chat snapshot", r.0))?;
    Ok((id.parse().with_context(|| format!("{:?} is not a chat snapshot", r.0))?, download_ref))
}

#[async_trait]
impl<C: ChatApi> SnapshotStore for ChatSnapshotStore<C> {
    async fn put(&self, snapshot: &StoredSnapshot) -> Result<SnapshotRef> {
        if self.chat.registers().await?.pinned_other {
            bail!("the snapshot chat has a pinned message that is not ours; use a dedicated chat");
        }
        let name = format!("snapshot-{}-{}.snap", snapshot.log_id, snapshot.at);
        let sent = self.chat.send_document(&name, snapshot.encode()).await?;
        self.chat.pin(sent.message_id).await?;
        Ok(chat_ref(sent.message_id, &sent.download_ref))
    }

    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot> {
        let (id, download_ref) = parse_chat_ref(r)?;
        let data = self.chat.download(download_ref).await.with_context(|| format!("download snapshot {id}"))?;
        StoredSnapshot::decode(&data).with_context(|| format!("decode snapshot {id}"))
    }

    async fn newest(&self) -> Result<Option<StoredSnapshot>> {
        let regs = self.chat.registers().await?;
        if regs.pinned_other {
            bail!("the snapshot chat's pinned message is not a snapshot");
        }
        let Some(doc) = regs.pinned else {
            return Ok(None);
        };
        Ok(Some(self.get(&chat_ref(doc.message_id, &doc.download_ref)).await?))
    }

    async fn remove(&self, r: &SnapshotRef) -> Result<()> {
        // Deleting an older (not the newest) pinned message leaves the newest pinned.
        self.chat.delete(&[parse_chat_ref(r)?.0]).await
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
    use crate::log::chat::fake::FakeChat;

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
    async fn chat_store_passes_the_conformance_suite() {
        let chat = Arc::new(FakeChat::default());
        conformance::run_all(|| {
            let chat = chat.clone();
            async move { Arc::new(ChatSnapshotStore::new(chat)) as Arc<dyn SnapshotStore> }
        })
        .await;
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

    #[tokio::test]
    async fn the_chat_store_refuses_a_shared_chat() {
        let chat = Arc::new(FakeChat::default());
        chat.post_text_and_pin();
        let store = ChatSnapshotStore::new(chat);
        assert!(store.newest().await.is_err());
        assert!(store.put(&StoredSnapshot { log_id: LogId(1), at: 1, data: vec![] }).await.is_err());
    }
}
