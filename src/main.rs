use anyhow::{bail, Context};
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::Router;
use s3gram::config::Config;
use s3gram::index::{Index, IndexSnapshot};
use s3gram::snapshot;
use s3gram::storage::{BlobStore, DeleteOutcome, MemoryBlobStore, TelegramBlobStore};
use s3gram::telegram::TelegramClient;
use s3gram::{build_s3_service, build_s3gram};
use s3s::{Body, HttpError};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("restore") => {
            // `s3gram restore` → pinned manifest bootstrap
            // `s3gram restore <file_id>` → legacy explicit handle
            let file_id = args.next();
            cmd_restore(file_id.as_deref()).await
        }
        Some("purge") => cmd_purge().await,
        Some(other) => {
            anyhow::bail!(
                "unknown command {other:?}; usage: s3gram [restore [file_id] | purge]"
            )
        }
        None => cmd_serve().await,
    }
}

async fn cmd_serve() -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    info!(path = %cfg.config_path.display(), "loaded config");
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    let migrated = index
        .migrate_legacy_chat_ids(&cfg.chat_id)
        .await
        .context("migrate legacy chat_id")?;
    if migrated > 0 {
        info!(migrated, "backfilled empty chat_id from config chat_id");
    }

    let (store, tg_for_snap): (
        Arc<dyn s3gram::storage::BlobStore>,
        Option<(TelegramClient, String)>,
    ) = if cfg.memory_store {
        warn!("memory = true: using MemoryBlobStore (no Telegram)");
        (Arc::new(MemoryBlobStore::new()), None)
    } else {
        let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;
        tg.ensure_chat_admin(&cfg.chat_id)
            .await
            .context("chat_id access check")?;
        let chat_id = cfg.chat_id.clone();
        let store = Arc::new(TelegramBlobStore::new(
            tg.clone(),
            chat_id.clone(),
            cfg.chat_limiter(),
        ));
        (store, Some((tg, chat_id)))
    };

    let s3gram = build_s3gram(cfg.clone(), index.clone(), store.clone());
    if let Some((tg, chat_id)) = tg_for_snap {
        snapshot::spawn_periodic(
            index.clone(),
            store.clone(),
            tg,
            chat_id,
            s3gram.snapshot_gate.clone(),
            cfg.snapshot_interval_secs,
            cfg.chunk_size,
        );
        snapshot::spawn_pending_deletes(index, store);
    }

    let s3_service = build_s3_service(s3gram, &cfg.access_key, &cfg.secret_key);
    let s3_service = HandleError::new(s3_service, handle_s3_error);
    let app = Router::new().fallback_service(s3_service);

    let addr = cfg.listen_addr.clone();
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    info!(
        memory = cfg.memory_store,
        chat_id = %cfg.chat_id,
        chunk_size = cfg.chunk_size,
        chunk_codec = %cfg.chunk_codec,
        "s3gram (s3s) listening on http://{addr}"
    );
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

/// Delete every Telegram message tracked by the local index, then wipe SQLite.
/// Intended for a stopped server. Refuses `memory = true`.
async fn cmd_purge() -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    if cfg.memory_store {
        bail!("refuse to purge when memory = true");
    }
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;
    tg.ensure_chat_admin(&cfg.chat_id)
        .await
        .context("chat_id access check")?;
    let store = TelegramBlobStore::new(tg.clone(), cfg.chat_id.clone(), cfg.chat_limiter());

    let mut ids: BTreeSet<i64> = index
        .list_tracked_message_ids()
        .await
        .context("list tracked message ids")?
        .into_iter()
        .collect();

    // Snapshot meta may reference messages not present in blobs (e.g. manifest-only).
    if let Some(raw) = index.get_meta("snapshot_message_id").await? {
        if let Ok(id) = raw.parse::<i64>() {
            ids.insert(id);
        }
    }
    if let Some(raw) = index.get_meta("snapshot_parts").await? {
        if let Ok(parts) = serde_json::from_str::<Vec<serde_json::Value>>(&raw) {
            for part in parts {
                if let Some(id) = part.get("message_id").and_then(|v| v.as_i64()) {
                    ids.insert(id);
                }
            }
        }
    }
    // Also collect the currently pinned bootstrap pointer (if any).
    if let Ok(Some(pinned)) = tg.get_pinned_content(&cfg.chat_id).await {
        match pinned {
            s3gram::telegram::PinnedContent::Text { message_id, text } => {
                ids.insert(message_id);
                if let Ok(m) = snapshot::parse_pin_manifest(&text) {
                    for p in m.parts {
                        ids.insert(p.message_id);
                    }
                }
            }
            s3gram::telegram::PinnedContent::Document { message_id, .. } => {
                ids.insert(message_id);
            }
        }
    }

    info!(
        chat_id = %cfg.chat_id,
        messages = ids.len(),
        "purging Telegram messages tracked by the index"
    );

    let mut deleted = 0u64;
    let mut gone = 0u64;
    let mut failed = 0u64;
    for message_id in &ids {
        match store.delete_message(*message_id).await {
            Ok(DeleteOutcome::Deleted) => deleted += 1,
            Ok(DeleteOutcome::Gone) => gone += 1,
            Ok(DeleteOutcome::Failed) => {
                failed += 1;
                warn!(message_id, "deleteMessage not confirmed");
            }
            Err(e) => {
                failed += 1;
                warn!(error = %e, message_id, "deleteMessage error");
            }
        }
        // Stay under Bot API flood limits on large indexes.
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    index.wipe_all().await.context("wipe sqlite index")?;
    info!(
        deleted,
        gone,
        failed,
        remaining_tracked = 0,
        "purge complete; index wiped"
    );
    if failed > 0 {
        bail!("{failed} Telegram deletes were not confirmed; index was still wiped");
    }
    Ok(())
}

/// Restore the SQLite index from Telegram.
/// - `None` → bootstrap from pinned manifest (`getChat` → pin → parts)
/// - `Some(file_id)` → legacy explicit document / manifest handle
async fn cmd_restore(file_id: Option<&str>) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;
    tg.ensure_chat_admin(&cfg.chat_id)
        .await
        .context("chat_id access check")?;
    let store = TelegramBlobStore::new(tg.clone(), cfg.chat_id.clone(), cfg.chat_limiter());

    let bytes = if let Some(fid) = file_id {
        info!(%fid, "downloading snapshot by file_id");
        snapshot::download_snapshot_bytes(&store, &index, Some(fid))
            .await
            .context("download snapshot")?
    } else {
        info!(chat_id = %cfg.chat_id, "bootstrapping snapshot from pinned manifest");
        snapshot::download_from_pinned(&tg, &cfg.chat_id, &store)
            .await
            .context("download from pin")?
    };
    let snap: IndexSnapshot = serde_json::from_slice(&bytes).context("parse snapshot JSON")?;
    index
        .import_snapshot(&snap)
        .await
        .context("import snapshot")?;
    info!(
        buckets = snap.buckets.len(),
        objects = snap.objects.len(),
        "index restored from snapshot"
    );
    Ok(())
}

async fn handle_s3_error(err: HttpError) -> Response<Body> {
    tracing::error!(?err, "s3s HTTP error");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("Internal Server Error".to_string()))
        .unwrap()
}
