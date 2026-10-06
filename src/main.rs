mod chunker;
mod config;
mod index;
mod s3;
mod telegram;

use anyhow::Context;
use config::Config;
use index::Index;
use s3::{router, AppState};
use telegram::TelegramClient;
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

    let addr = cfg.listen_addr.clone();
    let app = router(AppState {
        cfg,
        index,
        tg,
    });

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    info!("tg3 listening on http://{addr}");
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}
