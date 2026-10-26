//! Binary-only: offline maintenance subcommands (`restore`, `purge`, `migrate`).

use crate::runtime::{open_runtime, sibling_db_url};
use anyhow::{bail, Context};
use pigeonhole::config::Config;
use pigeonhole::index::Index;
use pigeonhole_chunk_store::{
    migrate_index_to_blob_db, start_or_restore, BlobDb, JournalOp, Sweeper,
};
use pigeonhole_gateway_s3::snapshot::restore_index_snapshot;
use tracing::{info, warn};

pub async fn cmd_migrate(dry_run: bool) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    let blob_url = sibling_db_url(&cfg.database_url, "blob");
    info!(%blob_url, dry_run, "migrate legacy index → blob.db");
    let blob_db = BlobDb::connect(&blob_url)
        .await
        .context("open blob.db")?;
    let primary = cfg.primary_instance()?;
    let report = migrate_index_to_blob_db(&cfg.database_url, &blob_db, primary, dry_run).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

/// Release all roots + wipe gateway index. Backend object reclaim is stage H (sweeper).
pub async fn cmd_purge(yes: bool, expect_chat: Option<&str>) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    if cfg.memory_store {
        bail!("refuse to purge when memory = true");
    }
    let scope = cfg.primary_scope_id().context("primary scope_id")?;
    if let Some(expected) = expect_chat {
        if expected != scope {
            bail!("--expect-chat mismatch: config scope_id={scope} got {expected}");
        }
    }
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    let objects = index.count_objects().await.unwrap_or(0);
    let buckets = index.count_buckets().await.unwrap_or(0);

    let rt = open_runtime(&cfg).await.context("open runtime")?;
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

    index.wipe_all().await.context("wipe sqlite index")?;

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

/// Restore blob.db (+ gateway index) from superblock pins.
pub async fn cmd_restore(force: bool) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    if index.has_data().await.context("check index")? && !force {
        bail!(
            "index is not empty (buckets={}, objects={}); pass --force to overwrite",
            index.count_buckets().await.unwrap_or(0),
            index.count_objects().await.unwrap_or(0)
        );
    }

    let rt = open_runtime(&cfg).await.context("open runtime")?;
    start_or_restore(
        rt.store.db(),
        rt.durability.as_ref(),
        &rt.primary_fingerprint,
        force,
    )
    .await
    .context("restore from superblocks")?;

    if rt.store.get_root("s3/index").await?.is_some() {
        restore_index_snapshot(&index, rt.store.as_ref())
            .await
            .context("restore s3/index root into gateway index")?;
        info!("gateway index restored from chunk-store root s3/index");
    } else {
        info!("no s3/index root after superblock restore; gateway index left unchanged");
    }
    Ok(())
}
