mod chunker;
mod config;
mod index;
mod s3;
mod snapshot;
mod storage;
mod telegram;

use anyhow::Context;
use config::Config;
use index::Index;
use s3::{router, AppState};
use std::sync::Arc;
use storage::TelegramBlobStore;
use telegram::TelegramClient;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

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

    let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;
    tg.ensure_chat_admin(&cfg.chat_id)
        .await
        .context("CHAT_ID access check")?;

    let store: Arc<dyn storage::BlobStore> =
        Arc::new(TelegramBlobStore::new(tg, cfg.chat_id.clone()));

    let snapshot_gate = Arc::new(Mutex::new(()));
    snapshot::spawn_periodic(
        index.clone(),
        store.clone(),
        snapshot_gate.clone(),
        cfg.snapshot_interval_secs,
    );
    snapshot::spawn_pending_deletes(index.clone(), store.clone());

    info!(chat_id = %cfg.chat_id, "s3gram using single Telegram chat for all blobs");

    let addr = cfg.listen_addr.clone();
    let app = router(AppState {
        cfg,
        index,
        store,
        snapshot_gate,
    });

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    info!("s3gram listening on http://{addr}");
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}
