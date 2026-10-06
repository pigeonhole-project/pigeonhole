mod admin;
mod chunker;
mod config;
mod index;
mod registry;
mod s3;
mod snapshot;
mod telegram;

use anyhow::Context;
use config::Config;
use index::Index;
use s3::{router, AppState};
use std::sync::Arc;
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
    index
        .ensure_service_bucket(&cfg.service_bucket, &cfg.service_chat_id)
        .await
        .context("ensure service bucket")?;
    let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;

    let snapshot_gate = Arc::new(Mutex::new(()));
    snapshot::spawn_periodic(
        index.clone(),
        tg.clone(),
        cfg.service_chat_id.clone(),
        snapshot_gate.clone(),
        cfg.snapshot_interval_secs,
    );

    admin::spawn(index.clone(), tg.clone(), cfg.clone());

    info!(
        service_bucket = %cfg.service_bucket,
        service_chat = %cfg.service_chat_id,
        admin_chat = %cfg.admin_chat_id,
        "service chat is registry/snapshots only; data buckets need their own chats"
    );

    let addr = cfg.listen_addr.clone();
    let app = router(AppState {
        cfg,
        index,
        tg,
        snapshot_gate,
    });

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    info!("s3gram listening on http://{addr}");
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}
