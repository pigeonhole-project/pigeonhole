//! Binary-only: offline maintenance subcommands (`purge`).

use crate::runtime::open_runtime;
use anyhow::{bail, Context};
use pigeonhole::config::Config;
use pigeonhole::index::Index;
use pigeonhole_chunk_store::Sweeper;
use tracing::{info, warn};

/// Delete every upload, object and bucket, releasing their chunks. Backend
/// blobs are then reclaimed by the sweeper.
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
    info!(
        scope_id = %scope,
        objects,
        buckets,
        "purge plan: delete every upload, object and bucket"
    );
    if !yes {
        bail!(
            "refusing to purge (scope_id={scope}, objects={objects}). \
             Re-run with: pigeonhole purge --yes [--expect-chat {scope}]"
        );
    }

    let released = index.delete_everything().await.context("delete gateway index")?;
    rt.store.release(&released).await.context("release object chunks")?;

    // Immediate reclaim pass (background sweeper uses the same path; grace still applies).
    let members: Vec<_> = rt.store.replicated().members().to_vec();
    // Purge reclaim pass; grace still protects in-flight puts.
    let sweeper = Sweeper::new(rt.store.db().clone(), members, cfg.sweep.clone());
    match sweeper.sweep_once().await {
        Ok(stats) => info!(
            zero_ref_chunks = stats.zero_ref_chunks,
            keys_deleted = stats.keys_deleted,
            "purge sweeper pass complete"
        ),
        Err(e) => warn!(error = %e, "purge sweeper pass failed; background sweeper will retry"),
    }
    info!("purge complete: gateway index emptied, chunks released");
    Ok(())
}
