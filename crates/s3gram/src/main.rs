use anyhow::{bail, Context};
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::Router;
use s3gram::config::{BackendKind, Config};
use s3gram::index::{Index, IndexSnapshot};
use s3gram::snapshot;
use s3gram::storage::{BlobStore, DeleteOutcome, MemoryBlobStore};
use s3gram::telegram::{PinnedContent, TelegramBlobStore, TelegramClient};
use s3gram::{build_s3_service, build_s3gram};
use s3gram_blob::{BlobBackend, BootstrapPointer, CachingBackend};
use s3s::{Body, HttpError};
use std::collections::BTreeSet;
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
            let mut force = false;
            let mut file_id = None;
            for a in args {
                match a.as_str() {
                    "--force" | "--yes" => force = true,
                    other if other.starts_with('-') => {
                        bail!("unknown restore flag {other}; usage: s3gram restore [--force] [file_id]")
                    }
                    other => file_id = Some(other.to_string()),
                }
            }
            cmd_restore(file_id.as_deref(), force).await
        }
        Some("purge") => {
            let mut yes = false;
            let mut expect_messages: Option<usize> = None;
            let mut expect_chat: Option<String> = None;
            let mut rest = args;
            while let Some(a) = rest.next() {
                match a.as_str() {
                    "--yes" => yes = true,
                    "--expect-messages" => {
                        let v = rest
                            .next()
                            .context("--expect-messages requires a number")?;
                        expect_messages = Some(v.parse().context("--expect-messages")?);
                    }
                    "--expect-chat" => {
                        expect_chat = Some(
                            rest.next()
                                .context("--expect-chat requires chat_id")?
                                .to_string(),
                        );
                    }
                    other => bail!(
                        "unknown purge flag {other}; usage: s3gram purge --yes \
                         [--expect-messages N] [--expect-chat CHAT_ID]"
                    ),
                }
            }
            cmd_purge(yes, expect_messages, expect_chat.as_deref()).await
        }
        Some(other) => {
            bail!("unknown command {other:?}; usage: s3gram [restore [--force] [file_id] | purge --yes]")
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

    let limiter = cfg.chat_limiter();
    let (store, pin, max_blob): (
        Arc<dyn BlobStore>,
        Option<Arc<dyn BootstrapPointer>>,
        usize,
    ) = if cfg.memory_store {
        warn!("memory = true: using MemoryBlobStore (no Telegram)");
        let mem = MemoryBlobStore::new();
        let max_blob = mem.limits().max_blob_size;
        let store: Arc<dyn BlobStore> = if cfg.cache.enabled {
            info!(
                memory_bytes = cfg.cache.memory_bytes,
                disk = ?cfg.cache.disk_path,
                "L2 CachingBackend enabled"
            );
            Arc::new(
                CachingBackend::new(mem, cfg.cache.clone())
                    .await
                    .context("init CachingBackend")?,
            )
        } else {
            Arc::new(mem)
        };
        (store, None, max_blob)
    } else if cfg.backend_kind == BackendKind::Discord {
        #[cfg(feature = "discord")]
        {
            use s3gram_discord::{DiscordBlobStore, DiscordClient};
            let dc = DiscordClient::new(cfg.bot_token.clone()).context("discord client")?;
            dc.ensure_channel_permissions(&cfg.chat_id, Some(limiter.as_ref()))
                .await
                .context("channel_id permission check")?;
            let dc_store = Arc::new(DiscordBlobStore::new(
                dc,
                cfg.chat_id.clone(),
                limiter,
                cfg.discord_max_blob_size,
            ));
            let max_blob = dc_store.limits().max_blob_size;
            let pin: Arc<dyn BootstrapPointer> = dc_store.clone();
            let store: Arc<dyn BlobStore> = if cfg.cache.enabled {
                info!(
                    memory_bytes = cfg.cache.memory_bytes,
                    disk = ?cfg.cache.disk_path,
                    "L2 CachingBackend enabled"
                );
                Arc::new(
                    CachingBackend::new(ArcBackend(dc_store.clone()), cfg.cache.clone())
                        .await
                        .context("init CachingBackend")?,
                )
            } else {
                dc_store
            };
            (store, Some(pin), max_blob)
        }
        #[cfg(not(feature = "discord"))]
        {
            bail!(
                "config selects discord backend but this binary was built without `--features discord`"
            );
        }
    } else {
        let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;
        tg.ensure_chat_admin(&cfg.chat_id)
            .await
            .context("chat_id access check")?;
        let tg_store = Arc::new(TelegramBlobStore::new(
            tg,
            cfg.chat_id.clone(),
            limiter,
        ));
        let max_blob = tg_store.limits().max_blob_size;
        let pin: Arc<dyn BootstrapPointer> = tg_store.clone();
        let store: Arc<dyn BlobStore> = if cfg.cache.enabled {
            info!(
                memory_bytes = cfg.cache.memory_bytes,
                disk = ?cfg.cache.disk_path,
                "L2 CachingBackend enabled"
            );
            // Cache wraps the same Arc so pin + blob I/O share one client/limiter.
            Arc::new(
                CachingBackend::new(ArcBackend(tg_store.clone()), cfg.cache.clone())
                    .await
                    .context("init CachingBackend")?,
            )
        } else {
            tg_store
        };
        (store, Some(pin), max_blob)
    };
    if cfg.chunk_size > max_blob {
        bail!(
            "chunk.size {} exceeds backend max_blob_size {max_blob}",
            cfg.chunk_size
        );
    }

    let s3gram = build_s3gram(cfg.clone(), index.clone(), store.clone());
    if let Some(pin) = pin {
        snapshot::spawn_periodic(
            index.clone(),
            store.clone(),
            pin,
            s3gram.snapshot_gate.clone(),
            cfg.snapshot_interval_secs,
            cfg.chunk_size,
        );
        snapshot::spawn_pending_deletes(index.clone(), store.clone());
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
        backend = ?cfg.backend_kind,
        chat_id = %cfg.chat_id,
        chunk_size = cfg.chunk_size,
        chunk_codec = %cfg.chunk_codec,
        "s3gram (s3s) listening on http://{addr}"
    );
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

/// Thin Arc wrapper so [`CachingBackend`] can own a cloneable backend handle.
struct ArcBackend<T>(Arc<T>);

#[async_trait::async_trait]
impl<T: BlobBackend + 'static> BlobBackend for ArcBackend<T> {
    fn id(&self) -> &s3gram_blob::BackendId {
        self.0.id()
    }
    fn limits(&self) -> &s3gram_blob::BackendLimits {
        self.0.limits()
    }
    async fn put(
        &self,
        data: bytes::Bytes,
        hint: s3gram_blob::PutHint,
    ) -> anyhow::Result<s3gram_blob::Locator> {
        self.0.put(data, hint).await
    }
    async fn get(
        &self,
        loc: &s3gram_blob::Locator,
        range: Option<s3gram_blob::ByteRange>,
    ) -> anyhow::Result<s3gram_blob::BoxByteStream> {
        self.0.get(loc, range).await
    }
    async fn delete(&self, loc: &s3gram_blob::Locator) -> anyhow::Result<DeleteOutcome> {
        self.0.delete(loc).await
    }
}

/// Delete every Telegram message tracked by the local index, then wipe SQLite.
async fn cmd_purge(
    yes: bool,
    expect_messages: Option<usize>,
    expect_chat: Option<&str>,
) -> anyhow::Result<()> {
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
    let limiter = cfg.chat_limiter();
    let store = TelegramBlobStore::new(tg.clone(), cfg.chat_id.clone(), limiter);

    let mut ids: BTreeSet<i64> = index
        .list_tracked_message_ids()
        .await
        .context("list tracked message ids")?
        .into_iter()
        .collect();

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
    if let Ok(Some(pinned)) = tg.get_pinned_content(&cfg.chat_id).await {
        match pinned {
            PinnedContent::Text { message_id, text } => {
                ids.insert(message_id);
                if let Ok(m) = snapshot::parse_pin_manifest(&text) {
                    for p in m.parts {
                        ids.insert(p.message_id);
                    }
                }
            }
            PinnedContent::Document { message_id, .. } => {
                ids.insert(message_id);
            }
        }
    }

    let objects = index.count_objects().await.unwrap_or(0);
    let buckets = index.count_buckets().await.unwrap_or(0);
    info!(
        chat_id = %cfg.chat_id,
        messages = ids.len(),
        objects,
        buckets,
        "purge plan"
    );

    if let Some(expected) = expect_chat {
        if expected != cfg.chat_id {
            bail!(
                "--expect-chat mismatch: config chat_id={} got {expected}",
                cfg.chat_id
            );
        }
    }
    if let Some(n) = expect_messages {
        if n != ids.len() {
            bail!(
                "--expect-messages mismatch: planned {} deletes, expected {n}",
                ids.len()
            );
        }
    }
    if !yes {
        bail!(
            "refusing to purge {} Telegram messages (chat_id={}, objects={objects}). \
             Re-run with: s3gram purge --yes [--expect-chat {}] [--expect-messages {}]",
            ids.len(),
            cfg.chat_id,
            cfg.chat_id,
            ids.len()
        );
    }

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

async fn cmd_restore(file_id: Option<&str>, force: bool) -> anyhow::Result<()> {
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
    let tg = TelegramClient::new(cfg.bot_token.clone()).context("telegram client")?;
    tg.ensure_chat_admin(&cfg.chat_id)
        .await
        .context("chat_id access check")?;
    let limiter = cfg.chat_limiter();
    let store = Arc::new(TelegramBlobStore::new(
        tg,
        cfg.chat_id.clone(),
        limiter,
    ));
    let pin: Arc<dyn BootstrapPointer> = store.clone();

    if let Some(fid) = file_id {
        info!(%fid, "downloading snapshot by file_id");
        let bytes = snapshot::download_snapshot_bytes(store.as_ref(), &index, Some(fid))
            .await
            .context("download snapshot")?;
        let snap: IndexSnapshot =
            serde_json::from_slice(&bytes).context("parse snapshot JSON")?;
        index
            .import_snapshot(&snap)
            .await
            .context("import snapshot")?;
        info!(
            buckets = snap.buckets.len(),
            objects = snap.objects.len(),
            "index restored from snapshot"
        );
    } else {
        info!(chat_id = %cfg.chat_id, "bootstrapping snapshot from pinned manifest");
        let restored = snapshot::download_from_pinned(pin.as_ref(), store.as_ref())
            .await
            .context("download from pin")?;
        let snap: IndexSnapshot =
            serde_json::from_slice(&restored.bytes).context("parse snapshot JSON")?;
        index
            .import_snapshot(&snap)
            .await
            .context("import snapshot")?;
        snapshot::record_restored_pin(&index, &restored)
            .await
            .context("record restored pin meta")?;
        info!(
            buckets = snap.buckets.len(),
            objects = snap.objects.len(),
            generation = restored.manifest.generation,
            "index restored from pinned snapshot"
        );
    }
    Ok(())
}

async fn handle_s3_error(err: HttpError) -> Response<Body> {
    tracing::error!(?err, "s3s HTTP error");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(err.to_string()))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}
