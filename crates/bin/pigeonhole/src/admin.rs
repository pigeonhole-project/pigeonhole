//! Binary-only: offline maintenance subcommands (`purge`, `restore`).

use crate::runtime;
use anyhow::{bail, Context};
use pigeonhole::config::Config;
use pigeonhole_node::Stack;
use tracing::{info, warn};

/// Delete every upload, object and bucket, releasing their chunks. Backend
/// blobs are then reclaimed by a sweeper pass (the dynamic config's knobs);
/// what it leaves, the next server's sweeper takes.
pub async fn cmd_purge(yes: bool, expect_chat: Option<&str>) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    if cfg.instances.iter().all(|i| i.kind == pigeonhole::instances::InstanceKind::Memory) {
        bail!("refuse to purge: only memory instances are configured");
    }
    let scope = cfg.primary_scope_id().context("primary scope_id")?;
    if let Some(expected) = expect_chat {
        if expected != scope {
            bail!("--expect-chat mismatch: config scope_id={scope} got {expected}");
        }
    }
    // Its loops stay off: the one sweep is this command's.
    let node = runtime::open(&cfg, |ns| ns, &[]).await.context("open the node")?;
    let purged = purge(&node, scope, yes).await;
    let closed = node.close().await.context("release the metadata log");
    crate::fakes::close().await?;
    purged.and(closed)
}

async fn purge(node: &Stack, scope: &str, yes: bool) -> anyhow::Result<()> {
    let objects = node.index.count_objects().await.unwrap_or(0);
    let buckets = node.index.count_buckets().await.unwrap_or(0);
    info!(scope_id = %scope, objects, buckets, "purge plan: delete every upload, object and bucket");
    if !yes {
        bail!(
            "refusing to purge (scope_id={scope}, objects={objects}). \
             Re-run with: pigeonhole purge --yes [--expect-chat {scope}]"
        );
    }
    let released = node.index.delete_everything().await.context("delete gateway index")?;
    node.store.release(&released).await.context("release object chunks")?;
    // Reclaim the released chunks now; orphans below the horizon (seeded from
    // the committed parts) go too.
    match node.sweep().await {
        Ok(stats) => info!(
            zero_ref_chunks = stats.zero_ref_chunks,
            keys_deleted = stats.keys_deleted,
            failed = ?stats.failed,
            "purge sweeper pass complete"
        ),
        Err(e) => warn!(error = %format!("{e:#}"), "purge sweeper pass failed; the background sweeper will retry"),
    }
    info!("purge complete: gateway index emptied, chunks released");
    Ok(())
}

/// Start a new metadata log from the newest stored snapshot (after losing the
/// log, with `[meta] snapshot` elsewhere). Lossy: everything after it is gone.
pub async fn cmd_restore(lossy: bool) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    if !lossy {
        bail!("restore from a snapshot loses every change after it; re-run with --lossy to accept");
    }
    let stores = runtime::open_meta_stores(&cfg).await?;
    let position = pigeonhole_meta::restore_lossy(stores.log.as_ref(), stores.snapshots.as_ref()).await?;
    warn!(position, "metadata restored from a stored snapshot; changes after it are lost");
    crate::fakes::close().await
}
