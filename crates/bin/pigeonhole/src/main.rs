use anyhow::{bail, Context};
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::Router;
use pigeonhole::config::{BackendKind, Config};
use pigeonhole::http_timeout::with_http_timeouts;
use pigeonhole::index::{Index, IndexSnapshot};
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::snapshot;
use pigeonhole::telegram::{TelegramBlobStore, TelegramClient};
use pigeonhole::{build_s3_service, build_s3gram};
use pigeonhole_blob::{spawn_metrics_logger, BackendMetrics, BootstrapPointer};
use pigeonhole_blob::InstanceKind;
use pigeonhole_chunk_store::{
    default_instance_for_migrate, migrate_index_to_blob_db, BlockCache, BlobDb, ChunkStore,
    IngestOptions,
};
use pigeonhole_gateway_s3::snapshot::{push_index_snapshot, restore_index_snapshot};
use s3s::{Body, HttpError};
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
            let mut expect_chat: Option<String> = None;
            let mut rest = args;
            while let Some(a) = rest.next() {
                match a.as_str() {
                    "--yes" => yes = true,
                    "--expect-messages" => {
                        let _ = rest.next().context("--expect-messages requires a number")?;
                        warn!("--expect-messages ignored after stage F (refs live in blob.db)");
                    }
                    "--expect-chat" => {
                        expect_chat = Some(
                            rest.next()
                                .context("--expect-chat requires chat_id")?
                                .to_string(),
                        );
                    }
                    other => bail!(
                        "unknown purge flag {other}; usage: s3gram purge --yes [--expect-chat CHAT_ID]"
                    ),
                }
            }
            cmd_purge(yes, expect_chat.as_deref()).await
        }
        Some("migrate") => {
            let mut dry_run = false;
            for a in args {
                match a.as_str() {
                    "--dry-run" => dry_run = true,
                    other => bail!("unknown migrate flag {other}; usage: pigeonhole migrate [--dry-run]"),
                }
            }
            cmd_migrate(dry_run).await
        }
        Some(other) => {
            bail!(
                "unknown command {other:?}; usage: pigeonhole \
                 [restore [--force] [file_id] | purge --yes | migrate [--dry-run]]"
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
        .context("open s3 index")?;
    let migrated = index
        .migrate_legacy_chat_ids(&cfg.chat_id)
        .await
        .context("migrate legacy chat_id")?;
    if migrated > 0 {
        info!(migrated, "backfilled empty chat_id from config chat_id");
    }

    let blob_url = blob_db_url_from_index(&cfg.database_url);
    let blob_db = BlobDb::connect(&blob_url)
        .await
        .context("open blob.db")?;

    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    opts.memory_budget = cfg.ingest_budget.clone();

    let limiter = cfg.chat_limiter();
    let store: Arc<ChunkStore> = if cfg.memory_store {
        warn!("memory = true: using MemoryBlobStore (no Telegram)");
        let layer = ChunkStore::open(blob_db, MemoryBlobStore::new(), opts)
            .await
            .context("open ChunkStore(memory)")?;
        Arc::new(with_optional_caches(layer, &cfg))
    } else if cfg.backend_kind == BackendKind::Discord {
        #[cfg(feature = "discord")]
        {
            use pigeonhole_storage_discord::{DiscordBlobStore, DiscordClient};
            let dc = DiscordClient::new(cfg.bot_token.clone()).context("discord client")?;
            dc.ensure_channel_permissions(&cfg.chat_id, Some(limiter.as_ref()))
                .await
                .context("channel_id permission check")?;
            let dc_store = DiscordBlobStore::new(
                dc,
                cfg.chat_id.clone(),
                limiter,
                cfg.discord_max_blob_size,
            );
            let layer = ChunkStore::open(blob_db, dc_store, opts)
                .await
                .context("open ChunkStore(discord)")?;
            Arc::new(with_optional_caches(layer, &cfg))
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
        let tg_store = TelegramBlobStore::new(tg, cfg.chat_id.clone(), limiter);
        let layer = ChunkStore::open(blob_db, tg_store, opts)
            .await
            .context("open ChunkStore(telegram)")?;
        Arc::new(with_optional_caches(layer, &cfg))
    };

    let s3gram = build_s3gram(cfg.clone(), index.clone(), store.clone());

    // Stage F: gateway snapshot → set_root("s3/index"); full pin/durability cutover is Stage G.
    if cfg.snapshot_interval_secs > 0 {
        let idx = index.clone();
        let st = store.clone();
        let gate = s3gram.snapshot_gate.clone();
        let interval = cfg.snapshot_interval_secs;
        tokio::spawn(async move {
            let period = Duration::from_secs(interval);
            loop {
                tokio::time::sleep(period).await;
                let _g = gate.lock().await;
                if let Err(e) = push_index_snapshot(&idx, st.as_ref()).await {
                    warn!(error = %e, "periodic s3 index snapshot failed");
                }
            }
        });
    }

    #[cfg(feature = "bytestream")]
    if cfg.bytestream.enabled {
        use pigeonhole_gateway_bytestream::cas_index::CasIndex;
        use pigeonhole_gateway_bytestream::config::BytestreamConfig;
        let bs_cfg = BytestreamConfig {
            enabled: true,
            listen_addr: cfg.bytestream.listen_addr.clone(),
            instance_name: cfg.bytestream.instance_name.clone(),
            max_batch_total_size_bytes: cfg.bytestream.max_batch_total_size_bytes,
            gc_ttl_secs: cfg.bytestream.gc_ttl_secs,
            ..Default::default()
        };
        let cas_url = cas_db_url_from_index(&cfg.database_url);
        let cas = CasIndex::connect(&cas_url)
            .await
            .context("open cas index")?;
        let bs_store = store.clone();
        tokio::spawn(async move {
            if let Err(e) =
                pigeonhole_gateway_bytestream::server::serve(bs_cfg, cas, bs_store).await
            {
                warn!(error = %e, "bytestream server exited");
            }
        });
    }

    let metrics = Arc::new(BackendMetrics::default());
    spawn_metrics_logger(metrics, cfg.cache.metrics_interval_secs);

    let max_headers = cfg.http.max_headers;
    let s3_service = build_s3_service(s3gram, &cfg.access_key, &cfg.secret_key);
    let s3_service = HandleError::new(s3_service, handle_s3_error);
    let body_idle = Duration::from_secs(cfg.http.request_timeout_secs);
    let headers_timeout = Duration::from_secs(cfg.http.headers_timeout_secs);
    let concurrency = cfg.http.max_concurrent_requests;
    let app = Router::new()
        .fallback_service(s3_service)
        .layer(axum::middleware::from_fn(move |req, next| {
            let max = max_headers;
            async move { limit_request_headers(max, req, next).await }
        }));
    let app = with_http_timeouts(app, headers_timeout, body_idle, concurrency);

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
        bytestream = cfg.bytestream.enabled,
        body_idle_timeout_secs = cfg.http.request_timeout_secs,
        headers_timeout_secs = cfg.http.headers_timeout_secs,
        max_concurrent = cfg.http.max_concurrent_requests,
        max_headers = cfg.http.max_headers,
        "s3gram (s3s) listening on http://{addr}"
    );
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

fn with_optional_caches(layer: ChunkStore, cfg: &Config) -> ChunkStore {
    if cfg.cache.enabled {
        let l1 = Arc::new(BlockCache::new(
            cfg.cache.block_memory_bytes,
            cfg.cache.readahead_blocks,
        ));
        layer.with_caches(Some(l1), None)
    } else {
        layer
    }
}

async fn limit_request_headers(
    max_headers: usize,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    if req.headers().len() > max_headers {
        return Err(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
    }
    Ok(next.run(req).await)
}

async fn cmd_migrate(dry_run: bool) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    let blob_url = blob_db_url_from_index(&cfg.database_url);
    info!(%blob_url, dry_run, "migrate legacy index → blob.db");
    let blob_db = BlobDb::connect(&blob_url)
        .await
        .context("open blob.db")?;

    let kind = if cfg.memory_store {
        InstanceKind::Memory
    } else {
        match cfg.backend_kind {
            BackendKind::Telegram => InstanceKind::Telegram,
            BackendKind::Discord => InstanceKind::Discord,
        }
    };
    let token = if cfg.memory_store {
        ""
    } else {
        cfg.bot_token.as_str()
    };
    let inst = default_instance_for_migrate(kind, token, &cfg.chat_id)?;
    let report = migrate_index_to_blob_db(&cfg.database_url, &blob_db, &inst, dry_run).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn blob_db_url_from_index(database_url: &str) -> String {
    if let Some(rest) = database_url.strip_prefix("sqlite:") {
        let (path, query) = match rest.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (rest, None),
        };
        let path = if let Some(stem) = path.strip_suffix(".db") {
            format!("{stem}-blob.db")
        } else {
            format!("{path}-blob.db")
        };
        return match query {
            Some(q) => format!("sqlite:{path}?{q}"),
            None => format!("sqlite:{path}?mode=rwc"),
        };
    }
    format!("{database_url}-blob")
}

fn cas_db_url_from_index(database_url: &str) -> String {
    if let Some(rest) = database_url.strip_prefix("sqlite:") {
        let (path, query) = match rest.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (rest, None),
        };
        let path = if let Some(stem) = path.strip_suffix(".db") {
            format!("{stem}-cas.db")
        } else {
            format!("{path}-cas.db")
        };
        return match query {
            Some(q) => format!("sqlite:{path}?{q}"),
            None => format!("sqlite:{path}?mode=rwc"),
        };
    }
    format!("{database_url}-cas")
}

/// Wipe the S3 gateway index (blob.db GC / backend purge is Stage G).
async fn cmd_purge(yes: bool, expect_chat: Option<&str>) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    if cfg.memory_store {
        bail!("refuse to purge when memory = true");
    }
    if let Some(expected) = expect_chat {
        if expected != cfg.chat_id {
            bail!(
                "--expect-chat mismatch: config chat_id={} got {expected}",
                cfg.chat_id
            );
        }
    }
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    let objects = index.count_objects().await.unwrap_or(0);
    let buckets = index.count_buckets().await.unwrap_or(0);
    info!(
        chat_id = %cfg.chat_id,
        objects,
        buckets,
        "purge plan (gateway index only; chunk refs remain in blob.db until sweeper)"
    );
    if !yes {
        bail!(
            "refusing to wipe gateway index (chat_id={}, objects={objects}). \
             Re-run with: s3gram purge --yes [--expect-chat {}]",
            cfg.chat_id,
            cfg.chat_id,
        );
    }
    index.wipe_all().await.context("wipe sqlite index")?;
    info!("purge complete; gateway index wiped");
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

    if let Some(fid) = file_id {
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
        info!(%fid, "downloading snapshot by file_id");
        let bytes = snapshot::download_snapshot_bytes(store.as_ref(), fid)
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
        return Ok(());
    }

    let blob_url = blob_db_url_from_index(&cfg.database_url);
    let blob_db = BlobDb::connect(&blob_url).await.context("open blob.db")?;
    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    let layer = ChunkStore::open(blob_db, MemoryBlobStore::new(), opts)
        .await
        .context("open ChunkStore")?;
    if layer.get_root("s3/index").await?.is_some() {
        restore_index_snapshot(&index, &layer)
            .await
            .context("restore s3/index root")?;
        info!("index restored from chunk-store root s3/index");
        return Ok(());
    }

    if cfg.memory_store {
        bail!("no s3/index root in blob.db");
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
    info!(
        buckets = snap.buckets.len(),
        objects = snap.objects.len(),
        "index restored from pinned manifest"
    );
    Ok(())
}

async fn handle_s3_error(err: HttpError) -> Response<Body> {
    tracing::error!(?err, "s3s HTTP error");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(err.to_string()))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}
