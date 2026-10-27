//! Snapshot export: copies of tablet snapshots outside the log, and seeding a
//! new log from one (lossy restore).
//!
//! An export is only a snapshot: whatever the log held after it is gone. After
//! a restore from an export, chunks committed later are orphans (the sweeper
//! deletes them) and chunks reclaimed later are referenced but deleted.

use crate::{LogStore, Position, Snapshot, TabletSnapshot};
use anyhow::{bail, Context, Result};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Exports kept in the directory; older ones are deleted.
pub const KEEP_EXPORTS: usize = 3;

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

/// Write `data` (an encoded [`TabletSnapshot`] at `position`) into `dir`
/// atomically and keep the newest [`KEEP_EXPORTS`].
pub fn write_export(dir: &Path, position: Position, data: &[u8]) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let name = format!("tablet-{position:020}.snap");
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = File::create(&tmp)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(&tmp, dir.join(&name))?;
    File::open(dir)?.sync_all()?;
    let all = exports(dir)?;
    for (_, path) in all.iter().take(all.len().saturating_sub(KEEP_EXPORTS)) {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// The newest export in `dir`.
pub fn newest_export(dir: &Path) -> Result<Option<TabletSnapshot>> {
    let Some((_, path)) = exports(dir)?.pop() else {
        return Ok(None);
    };
    let data = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    Ok(Some(TabletSnapshot::decode(&data).with_context(|| format!("decode {}", path.display()))?))
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
