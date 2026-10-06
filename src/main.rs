mod chunker;
mod config;
mod index;
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
    let tg = TelegramClient::new(cfg.bot_token.clone(), cfg.chat_id.clone())
        .context("telegram client")?;

    let snapshot_gate = Arc::new(Mutex::new(()));
    snapshot::spawn_periodic(
        index.clone(),
        tg.clone(),
        snapshot_gate.clone(),
        cfg.snapshot_interval_secs,
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
