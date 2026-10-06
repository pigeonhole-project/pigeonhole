use anyhow::Context;
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::Router;
use s3gram::config::Config;
use s3gram::index::{Index, IndexSnapshot};
use s3gram::snapshot;
use s3gram::storage::{MemoryBlobStore, TelegramBlobStore};
use s3gram::telegram::TelegramClient;
use s3gram::{build_s3_service, build_s3gram};
use s3s::{Body, HttpError};
use std::sync::Arc;
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
            let file_id = args
                .next()
                .context("usage: s3gram restore <file_id>")?;
            cmd_restore(&file_id).await
        }
        Some(other) => {
            anyhow::bail!("unknown command {other:?}; usage: s3gram [restore <file_id>]")
        }
        None => cmd_serve().await,
    }
}

async fn cmd_serve() -> anyhow::Result<()> {
    let cfg = Config::from_env().context("load config")?;
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    let migrated = index
        .migrate_legacy_chat_ids(&cfg.chat_id)
        .await
        .context("migrate legacy chat_id")?;
    if migrated > 0 {
        info!(migrated, "backfilled empty chat_id from CHAT_ID");
    }

    let store: Arc<dyn s3gram::storage::BlobStore> = if cfg.memory_store {
        warn!("S3GRAM_MEMORY=1: using MemoryBlobStore (no Telegram)");
        Arc::new(MemoryBlobStore::new())
    } else {
        let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;
        tg.ensure_chat_admin(&cfg.chat_id)
            .await
            .context("CHAT_ID access check")?;
        Arc::new(TelegramBlobStore::new(tg, cfg.chat_id.clone()))
    };

    let s3gram = build_s3gram(cfg.clone(), index.clone(), store.clone());
    if !cfg.memory_store {
        snapshot::spawn_periodic(
            index.clone(),
            store.clone(),
            s3gram.snapshot_gate.clone(),
            cfg.snapshot_interval_secs,
        );
        snapshot::spawn_pending_deletes(index, store);
    }

    if std::env::var("S3GRAM_INSECURE").ok().as_deref() == Some("1") {
        warn!("S3GRAM_INSECURE=1 is ignored under s3s; SigV4 with AWS_* keys is required");
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
        "s3gram (s3s) listening on http://{addr}"
    );
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

/// Restore the SQLite index from a Telegram snapshot `file_id` (manifest or single gzip).
/// Intended for a stopped server / clean machine.
async fn cmd_restore(file_id: &str) -> anyhow::Result<()> {
    let cfg = Config::from_env().context("load config")?;
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;
    tg.ensure_chat_admin(&cfg.chat_id)
        .await
        .context("CHAT_ID access check")?;
    let store = TelegramBlobStore::new(tg, cfg.chat_id.clone());

    info!(%file_id, "downloading snapshot");
    let bytes = snapshot::download_snapshot_bytes(&store, &index, Some(file_id))
        .await
        .context("download snapshot")?;
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
