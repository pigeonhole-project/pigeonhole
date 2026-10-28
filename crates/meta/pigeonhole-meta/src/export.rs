//! Snapshot export: copies of tablet snapshots outside the log, and seeding a
//! new log from one (lossy restore).
//!
//! An export is only a snapshot: whatever the log held after it is gone. After
//! a restore, chunks committed later are orphans (the sweeper deletes them);
//! chunks reclaimed later are safe only because reclaim waits for an export
//! taken after `BeginReclaim` (see `MetaStore::exported_position`).

use crate::log::chat::ChatApi;
use crate::{LogStore, Position, Snapshot, TabletSnapshot};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Where exported snapshots go.
#[async_trait]
pub trait SnapshotSink: Send + Sync + 'static {
    /// Store an encoded [`TabletSnapshot`] taken at `position`.
    async fn put(&self, position: Position, data: Vec<u8>) -> Result<()>;
    /// The newest export, if any.
    async fn newest(&self) -> Result<Option<TabletSnapshot>>;
}

/// Exports kept in a directory; older ones are deleted.
pub const KEEP_EXPORTS: usize = 3;

/// `tablet-<pos>.snap` files in a directory (another disk, a mount).
pub struct DirSink {
    dir: PathBuf,
}

impl DirSink {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn exports(dir: &Path) -> Result<Vec<(Position, PathBuf)>> {
        let mut out = Vec::new();
        for e in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
            let e = e?;
            let name = e.file_name();
            let Some(pos) = name
                .to_str()
                .and_then(|n| n.strip_prefix("tablet-"))
                .and_then(|n| n.strip_suffix(".snap"))
                .and_then(|n| n.parse().ok())
            else {
                continue;
            };
            out.push((pos, e.path()));
        }
        out.sort_unstable_by_key(|(pos, _)| *pos);
        Ok(out)
    }

    fn write(dir: &Path, position: Position, data: &[u8]) -> Result<()> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let name = format!("tablet-{position:020}.snap");
        let tmp = dir.join(format!("{name}.tmp"));
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        fs::rename(&tmp, dir.join(&name))?;
        File::open(dir)?.sync_all()?;
        let all = Self::exports(dir)?;
        for (_, path) in all.iter().take(all.len().saturating_sub(KEEP_EXPORTS)) {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

#[async_trait]
impl SnapshotSink for DirSink {
    async fn put(&self, position: Position, data: Vec<u8>) -> Result<()> {
        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || Self::write(&dir, position, &data)).await?
    }

    async fn newest(&self) -> Result<Option<TabletSnapshot>> {
        if !self.dir.exists() {
            return Ok(None);
        }
        let Some((_, path)) = Self::exports(&self.dir)?.pop() else {
            return Ok(None);
        };
        let data = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        Ok(Some(TabletSnapshot::decode(&data).with_context(|| format!("decode {}", path.display()))?))
    }
}

/// The newest export pinned in a dedicated chat; the previous one is deleted.
pub struct ChatSink<C> {
    chat: Arc<C>,
}

impl<C: ChatApi> ChatSink<C> {
    pub fn new(chat: Arc<C>) -> Self {
        Self { chat }
    }
}

#[async_trait]
impl<C: ChatApi> SnapshotSink for ChatSink<C> {
    async fn put(&self, position: Position, data: Vec<u8>) -> Result<()> {
        let regs = self.chat.registers().await?;
        if regs.pinned_other {
            bail!("the export chat has a pinned message that is not ours; use a dedicated chat");
        }
        let sent = self.chat.send_document(&format!("tablet-{position}.snap"), data).await?;
        self.chat.pin(sent.message_id).await?;
        // Deleting an older (not the newest) pin leaves the head visible.
        if let Some(old) = regs.pinned.filter(|old| old.message_id != sent.message_id) {
            let _ = self.chat.delete(&[old.message_id]).await;
        }
        Ok(())
    }

    async fn newest(&self) -> Result<Option<TabletSnapshot>> {
        let regs = self.chat.registers().await?;
        if regs.pinned_other {
            bail!("the export chat's pinned message is not a snapshot export");
        }
        let Some(doc) = regs.pinned else {
            return Ok(None);
        };
        let data = self.chat.download(&doc.download_ref).await?;
        Ok(Some(TabletSnapshot::decode(&data).context("decode the pinned snapshot export")?))
    }
}

/// Start an empty log from `snapshot`: the states as of the export, positions
/// numbered from 0 again. Refuses a log that was ever claimed.
pub async fn seed_log(log: &dyn LogStore, mut snapshot: TabletSnapshot) -> Result<()> {
    if log.discover().await.context("discover log")?.is_some() {
        bail!("the log is not empty; restore only seeds a new log");
    }
    snapshot.position = 0;
    log.claim(1).await.context("claim the new log")?;
    log.install_snapshot(1, 0, Snapshot { data: snapshot.encode() })
        .await
        .context("install the restored snapshot")?;
    Ok(())
}
