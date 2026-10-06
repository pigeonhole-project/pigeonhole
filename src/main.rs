mod chunker;
mod config;
mod index;
mod ingest;
mod service;
mod snapshot;
mod storage;
mod telegram;

use anyhow::Context;
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::Router;
use config::Config;
use index::Index;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s::{Body, HttpError};
use service::S3gram;
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

    let s3gram = S3gram {
        cfg: cfg.clone(),
        index,
        store,
        snapshot_gate,
    };

    let mut builder = S3ServiceBuilder::new(s3gram);
    // s3s without set_auth only accepts unsigned requests; AWS CLI always signs,
    // so we always install SimpleAuth. (S3GRAM_INSECURE is ignored.)
    if std::env::var("S3GRAM_INSECURE").ok().as_deref() == Some("1") {
        tracing::warn!("S3GRAM_INSECURE=1 is ignored under s3s; SigV4 with AWS_* keys is required");
    }
    builder.set_auth(SimpleAuth::from_single(
        cfg.access_key.clone(),
        cfg.secret_key.clone(),
    ));
    let s3_service = builder.build();
    let s3_service = HandleError::new(s3_service, handle_s3_error);

    let app = Router::new().fallback_service(s3_service);

    let addr = cfg.listen_addr.clone();
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    info!(chat_id = %cfg.chat_id, "s3gram (s3s) listening on http://{addr}");
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

async fn handle_s3_error(err: HttpError) -> Response<Body> {
    tracing::error!(?err, "s3s HTTP error");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("Internal Server Error".to_string()))
        .unwrap()
}
