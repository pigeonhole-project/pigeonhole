//! Binary-only: offline maintenance subcommands (`restore`, `purge`).

use crate::runtime::open_runtime;
use anyhow::{bail, Context};
use pigeonhole::config::Config;
use pigeonhole::index::Index;
use pigeonhole_chunk_store::{
    start_or_restore, JournalOp, Sweeper,
};
use tracing::{info, warn};

/// Release all roots, delete every upload, object and bucket (releasing their
/// chunks). Backend blobs are then reclaimed by the sweeper.
pub async fn cmd_purge(yes: bool, expect_chat: Option<&str>) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    if cfg
        .instances
        .iter()
        .all(|i| i.info.kind == pigeonhole_blob::InstanceKind::Memory)
    {
        bail!("refuse to purge: only memory instances are configured");
    }
    let scope = cfg.primary_scope_id().context("primary scope_id")?;
    if let Some(expected) = expect_chat {
        if expected != scope {
            bail!("--expect-chat mismatch: config scope_id={scope} got {expected}");
        }
    }
    let rt = open_runtime(&cfg).await.context("open runtime")?;
    let index = Index::open(rt.meta.clone()).await.context("open index")?;
    let objects = index.count_objects().await.unwrap_or(0);
    let buckets = index.count_buckets().await.unwrap_or(0);
    let roots = rt.store.db().list_roots().await.context("list roots")?;
    info!(
        scope_id = %scope,
        objects,
        buckets,
        roots = roots.len(),
        "purge plan: release all roots, wipe gateway index"
    );
    if !yes {
        bail!(
            "refusing to purge (scope_id={scope}, objects={objects}, roots={}). \
             Re-run with: pigeonhole purge --yes [--expect-chat {scope}]",
            roots.len(),
        );
    }

    for (name, extents) in &roots {
        let ids: Vec<_> = extents.iter().map(|e| e.chunk).collect();
        if !ids.is_empty() {
            rt.store.release(&ids).await.with_context(|| format!("release root {name}"))?;
        }
        rt.store.db().delete_root(name).await?;
        rt.durability
            .enqueue(JournalOp::SetRoot {
                name: name.clone(),
                extents: vec![],
            })
            .await;
    }
    if let Err(e) = rt.durability.flush_journal().await {
        warn!(error = %e, "purge journal flush failed");
    }
    // Attempt a checkpoint so pins reflect cleared roots (best-effort).
    if let Err(e) = rt.durability.checkpoint(rt.store.db()).await {
        warn!(error = %e, "purge checkpoint failed");
    }

    let released = index.delete_everything().await.context("delete gateway index")?;
    rt.store.release(&released).await.context("release object chunks")?;

    // Immediate reclaim pass (background sweeper uses the same path; grace still applies).
    let members: Vec<_> = rt.store.replicated().members().to_vec();
    // Purge reclaim pass; grace still protects in-flight puts.
    let sweeper = Sweeper::new(
        rt.store.db().clone(),
        rt.durability.clone(),
        members,
        cfg.sweep.clone(),
    );
    match sweeper.sweep_once().await {
        Ok(stats) => info!(
            zero_ref_chunks = stats.zero_ref_chunks,
            keys_deleted = stats.keys_deleted,
            "purge sweeper pass complete"
        ),
        Err(e) => warn!(error = %e, "purge sweeper pass failed; background sweeper will retry"),
    }
    info!("purge complete: roots released, gateway index wiped");
    Ok(())
}

/// Restore chunk metadata from superblock pins. The gateway index is rebuilt
/// from the metadata log on every boot and has no separate snapshot.
pub async fn cmd_restore(force: bool) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    let rt = open_runtime(&cfg).await.context("open runtime")?;
    start_or_restore(
        rt.store.db(),
        rt.durability.as_ref(),
        &rt.primary_fingerprint,
        force,
    )
    .await
    .context("restore from superblocks")?;
    info!("chunk metadata restored from superblock pins");
    Ok(())
}
