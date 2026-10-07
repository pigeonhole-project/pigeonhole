use anyhow::{bail, Context};
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::Router;
use pigeonhole::config::Config;
use pigeonhole::gateway_metrics::track_gateway;
use pigeonhole::http_timeout::with_http_timeouts;
use pigeonhole::index::Index;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::telegram::{TelegramBlobStore, TelegramClient};
use pigeonhole::{build_s3_service, build_s3gram};
use pigeonhole_blob::{
    erase_sweep, set_checkpoint_age, set_superblock_age, CheapestFirst, InstanceKind,
    MetricsBackend, Replicated, SharedBackend, TypedBootstrapPointer,
};
use pigeonhole_chunk_store::{
    check_fingerprints, legacy_index_has_blobs, migrate_index_to_blob_db, start_or_restore,
    BlockCache, BlobDb, ChunkStore, Durability, IngestOptions, JournalOp, PinTarget, Repairer,
    Superblock, Sweeper, WatermarkBackend,
};
use pigeonhole_gateway_s3::snapshot::{
    push_index_snapshot_durable, restore_index_snapshot,
};
use s3s::{Body, HttpError};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const JOURNAL_FLUSH_SECS: u64 = 5;
const DEFAULT_CHECKPOINT_SECS: u64 = 300;

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
            for a in args {
                match a.as_str() {
                    "--force" | "--yes" => force = true,
                    other => {
                        bail!(
                            "unknown restore flag {other}; usage: pigeonhole restore [--force]"
                        )
                    }
                }
            }
            cmd_restore(force).await
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
                        warn!("--expect-messages ignored (refs live in blob.db; sweeper is stage H)");
                    }
                    "--expect-chat" => {
                        expect_chat = Some(
                            rest.next()
                                .context("--expect-chat requires chat_id")?
                                .to_string(),
                        );
                    }
                    other => bail!(
                        "unknown purge flag {other}; usage: pigeonhole purge --yes [--expect-chat CHAT_ID]"
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
                    other => {
                        bail!("unknown migrate flag {other}; usage: pigeonhole migrate [--dry-run]")
                    }
                }
            }
            cmd_migrate(dry_run).await
        }
        Some(other) => {
            bail!(
                "unknown command {other:?}; usage: pigeonhole \
                 [restore [--force] | purge --yes | migrate [--dry-run]]"
            )
        }
        None => cmd_serve().await,
    }
}

struct Runtime {
    store: Arc<ChunkStore>,
    durability: Arc<Durability>,
    primary_fingerprint: String,
}

async fn cmd_serve() -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    info!(path = %cfg.config_path.display(), "loaded config");
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open s3 index")?;

    let rt = open_runtime(&cfg).await.context("open runtime")?;
    start_or_restore(
        rt.store.db(),
        rt.durability.as_ref(),
        &rt.primary_fingerprint,
        false,
    )
    .await
    .context("start_or_restore")?;

    let s3gram = build_s3gram(cfg.clone(), index.clone(), rt.store.clone());
    spawn_background_tasks(&cfg, &rt, &index, &s3gram);

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
        let bs_store = rt.store.clone();
        tokio::spawn(async move {
            if let Err(e) =
                pigeonhole_gateway_bytestream::server::serve(bs_cfg, cas, bs_store).await
            {
                warn!(error = %e, "bytestream server exited");
            }
        });
    }

    let metrics_on_main = setup_metrics(&cfg)?;

    let max_headers = cfg.http.max_headers;
    let s3_service = build_s3_service(s3gram, &cfg.access_key, &cfg.secret_key);
    let s3_service = HandleError::new(s3_service, handle_s3_error);
    let body_idle = Duration::from_secs(cfg.http.request_timeout_secs);
    let headers_timeout = Duration::from_secs(cfg.http.headers_timeout_secs);
    let concurrency = cfg.http.max_concurrent_requests;
    let mut app = Router::new();
    #[cfg(feature = "metrics-prometheus")]
    if metrics_on_main {
        app = pigeonhole::prometheus::layer_metrics_route(app);
    }
    #[cfg(not(feature = "metrics-prometheus"))]
    let _ = metrics_on_main;
    let app = app
        .fallback_service(s3_service)
        .layer(axum::middleware::from_fn(move |req, next| {
            let max = max_headers;
            async move { limit_request_headers(max, req, next).await }
        }))
        .layer(axum::middleware::from_fn(|req, next| {
            track_gateway("s3", req, next)
        }));
    let app = with_http_timeouts(app, headers_timeout, body_idle, concurrency);

    let addr = cfg.listen_addr.clone();
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    let scope = cfg.primary_scope_id().unwrap_or("?");
    info!(
        memory = cfg.memory_store,
        backend = ?cfg.backend_kind,
        scope_id = %scope,
        instances = cfg.instances.len(),
        placement = ?cfg.placement.group,
        chunk_size = cfg.chunk_size,
        chunk_codec = %cfg.chunk_codec,
        bytestream = cfg.bytestream.enabled,
        metrics = cfg.metrics.enabled,
        body_idle_timeout_secs = cfg.http.request_timeout_secs,
        headers_timeout_secs = cfg.http.headers_timeout_secs,
        max_concurrent = cfg.http.max_concurrent_requests,
        max_headers = cfg.http.max_headers,
        "pigeonhole listening on http://{addr}"
    );
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

/// Returns true when `/metrics` should be mounted on the main HTTP router.
fn setup_metrics(cfg: &Config) -> anyhow::Result<bool> {
    if !cfg.metrics.enabled {
        return Ok(false);
    }
    #[cfg(feature = "metrics-prometheus")]
    {
        pigeonhole::prometheus::install_recorder()?;
        match &cfg.metrics.listen_addr {
            Some(addr) => {
                let sock: std::net::SocketAddr = addr
                    .parse()
                    .with_context(|| format!("parse metrics.listen_addr {addr}"))?;
                pigeonhole::prometheus::spawn_dedicated_listener(sock);
                Ok(false)
            }
            None => Ok(true),
        }
    }
    #[cfg(not(feature = "metrics-prometheus"))]
    {
        warn!("[metrics] enabled but binary built without feature metrics-prometheus");
        Ok(false)
    }
}

fn spawn_background_tasks(
    cfg: &Config,
    rt: &Runtime,
    index: &Index,
    s3gram: &pigeonhole::S3gram,
) {
    let dur = rt.durability.clone();
    tokio::spawn(async move {
        let period = Duration::from_secs(JOURNAL_FLUSH_SECS);
        loop {
            tokio::time::sleep(period).await;
            if let Err(e) = dur.flush_journal().await {
                warn!(error = %e, "journal flush failed");
            }
        }
    });

    let checkpoint_secs = if cfg.snapshot_interval_secs > 0 {
        cfg.snapshot_interval_secs.max(60)
    } else {
        DEFAULT_CHECKPOINT_SECS
    };
    let dur = rt.durability.clone();
    let store = rt.store.clone();
    tokio::spawn(async move {
        let period = Duration::from_secs(checkpoint_secs);
        loop {
            tokio::time::sleep(period).await;
            if let Err(e) = dur.checkpoint(store.db()).await {
                warn!(error = %e, "blob.db checkpoint failed");
            }
        }
    });

    if cfg.snapshot_interval_secs > 0 {
        let idx = index.clone();
        let st = rt.store.clone();
        let dur = rt.durability.clone();
        let gate = s3gram.snapshot_gate.clone();
        let interval = cfg.snapshot_interval_secs;
        tokio::spawn(async move {
            let period = Duration::from_secs(interval);
            loop {
                tokio::time::sleep(period).await;
                let _g = gate.lock().await;
                if let Err(e) = push_index_snapshot_durable(&idx, st.as_ref(), dur.as_ref()).await {
                    warn!(error = %e, "periodic s3 index snapshot failed");
                }
            }
        });
    }

    let members: Vec<_> = rt.store.replicated().members().to_vec();
    let sweeper = Sweeper::new(
        rt.store.db().clone(),
        rt.durability.clone(),
        members,
        cfg.sweep.clone(),
    );
    tokio::spawn(async move {
        sweeper.run_loop().await;
    });
    info!(
        grace_secs = cfg.sweep.grace.as_secs(),
        interval_secs = cfg.sweep.interval.as_secs(),
        "sweeper background task started"
    );

    let repairer = Repairer::new(
        rt.store.db().clone(),
        rt.store.replicated().clone(),
        cfg.repair.clone(),
    );
    tokio::spawn(async move {
        repairer.run_loop().await;
    });
    info!(
        interval_secs = cfg.repair.interval.as_secs(),
        batch_size = cfg.repair.batch_size,
        scrub = cfg.repair.scrub,
        "repair background task started"
    );

    if cfg.metrics.enabled {
        let dur = rt.durability.clone();
        tokio::spawn(async move {
            let period = Duration::from_secs(15);
            loop {
                tokio::time::sleep(period).await;
                if let Some(age) = dur.superblock_age().await {
                    set_superblock_age(age);
                }
                if let Some(age) = dur.checkpoint_age().await {
                    set_checkpoint_age(age);
                }
            }
        });
    }
}

async fn open_runtime(cfg: &Config) -> anyhow::Result<Runtime> {
    let blob_url = blob_db_url_from_index(&cfg.database_url);
    let blob_db = BlobDb::connect(&blob_url)
        .await
        .context("open blob.db")?;

    let stored = blob_db
        .list_instance_fingerprints()
        .await
        .context("list instance fingerprints")?;
    check_fingerprints(&cfg.instances, &stored).context("fingerprint check")?;

    if blob_db.is_empty_metadata().await? {
        if legacy_index_has_blobs(&cfg.database_url)
            .await
            .context("probe legacy index")?
        {
            let primary = cfg.primary_instance()?;
            info!("legacy s3gram index has blobs; migrating into blob.db");
            let report =
                migrate_index_to_blob_db(&cfg.database_url, &blob_db, primary, false).await?;
            info!(
                blobs = report.blobs,
                roots = report.roots,
                "legacy index migrated"
            );
        }
    }

    let (replicated, pins, primary_fingerprint, primary_id) =
        build_placement(cfg, &blob_db).await.context("build placement group")?;

    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    opts.memory_budget = cfg.ingest_budget.clone();

    let layer = ChunkStore::open_replicated(blob_db, replicated.clone(), opts)
        .await
        .context("open ChunkStore")?;
    let store = Arc::new(with_optional_caches(layer, cfg));

    let genesis = Superblock::new(0, primary_id, primary_fingerprint.clone());
    let durability = Arc::new(Durability::new_replicated(replicated, pins, genesis));

    Ok(Runtime {
        store,
        durability,
        primary_fingerprint,
    })
}

async fn build_placement(
    cfg: &Config,
    blob_db: &BlobDb,
) -> anyhow::Result<(Arc<Replicated>, Vec<PinTarget>, String, String)> {
    let limiter = cfg.chat_limiter();
    let mut members: Vec<SharedBackend> = Vec::new();
    let mut pins: Vec<PinTarget> = Vec::new();
    let grace = cfg.sweep.grace;

    for id in &cfg.placement.group {
        let inst = cfg
            .instances
            .iter()
            .find(|i| i.info.id == *id)
            .with_context(|| format!("placement member {id:?} missing from instances"))?;

        match inst.info.kind {
            InstanceKind::Memory => {
                warn!("memory instance {id}: in-process MemoryBlobStore (no durable pin across processes)");
                let mem = Arc::new(
                    MemoryBlobStore::new().with_instance_info(inst.info.clone()),
                );
                let pin: Arc<dyn TypedBootstrapPointer> = mem.clone();
                let backend = MetricsBackend::wrap(WatermarkBackend::wrap(
                    Arc::new(erase_sweep(mem)),
                    blob_db.clone(),
                    grace,
                ));
                pins.push(PinTarget {
                    instance_id: inst.info.id.clone(),
                    fingerprint: inst.info.fingerprint.clone(),
                    pin,
                    backend: backend.clone(),
                });
                members.push(backend);
            }
            InstanceKind::Telegram => {
                let tg = TelegramClient::new(inst.bot_token.clone()).context("telegram client")?;
                tg.ensure_chat_admin(&inst.scope_id)
                    .await
                    .context("chat_id access check")?;
                let store = Arc::new(TelegramBlobStore::with_instance(
                    tg,
                    inst.scope_id.clone(),
                    limiter.clone(),
                    inst.info.clone(),
                ));
                let pin: Arc<dyn TypedBootstrapPointer> = store.clone();
                let backend = MetricsBackend::wrap(WatermarkBackend::wrap(
                    Arc::new(erase_sweep(store)),
                    blob_db.clone(),
                    grace,
                ));
                pins.push(PinTarget {
                    instance_id: inst.info.id.clone(),
                    fingerprint: inst.info.fingerprint.clone(),
                    pin,
                    backend: backend.clone(),
                });
                members.push(backend);
            }
            InstanceKind::Discord => {
                #[cfg(feature = "discord")]
                {
                    use pigeonhole_storage_discord::{DiscordBlobStore, DiscordClient};
                    let dc = DiscordClient::new(inst.bot_token.clone()).context("discord client")?;
                    dc.ensure_channel_permissions(&inst.scope_id, Some(limiter.as_ref()))
                        .await
                        .context("channel_id permission check")?;
                    let store = Arc::new(DiscordBlobStore::with_instance(
                        dc,
                        inst.scope_id.clone(),
                        limiter.clone(),
                        cfg.discord_max_blob_size,
                        inst.info.clone(),
                    ));
                    let pin: Arc<dyn TypedBootstrapPointer> = store.clone();
                    let backend = MetricsBackend::wrap(WatermarkBackend::wrap(
                        Arc::new(erase_sweep(store)),
                        blob_db.clone(),
                        grace,
                    ));
                    pins.push(PinTarget {
                        instance_id: inst.info.id.clone(),
                        fingerprint: inst.info.fingerprint.clone(),
                        pin,
                        backend: backend.clone(),
                    });
                    members.push(backend);
                }
                #[cfg(not(feature = "discord"))]
                {
                    bail!(
                        "instance {id:?} is discord but this binary was built without `--features discord`"
                    );
                }
            }
        }
    }

    if members.is_empty() {
        bail!("placement.group produced no backends");
    }
    let replicated = Arc::new(
        Replicated::new(
            members,
            cfg.placement.write_quorum,
            Arc::new(CheapestFirst::new()),
        )
        .context("Replicated::new")?,
    );
    let primary = cfg.primary_instance()?;
    Ok((
        replicated,
        pins,
        primary.info.fingerprint.clone(),
        primary.info.id.clone(),
    ))
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
    let primary = cfg.primary_instance()?;
    let report = migrate_index_to_blob_db(&cfg.database_url, &blob_db, primary, dry_run).await?;
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

#[cfg(feature = "bytestream")]
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

/// Release all roots + wipe gateway index. Backend object reclaim is stage H (sweeper).
async fn cmd_purge(yes: bool, expect_chat: Option<&str>) -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    if cfg.memory_store {
        bail!("refuse to purge when memory = true");
    }
    let scope = cfg.primary_scope_id().context("primary scope_id")?;
    if let Some(expected) = expect_chat {
        if expected != scope {
            bail!("--expect-chat mismatch: config scope_id={scope} got {expected}");
        }
    }
    let index = Index::connect(&cfg.database_url)
        .await
        .context("open index")?;
    let objects = index.count_objects().await.unwrap_or(0);
    let buckets = index.count_buckets().await.unwrap_or(0);

    let rt = open_runtime(&cfg).await.context("open runtime")?;
    let roots = rt.store.db().list_roots().await.context("list roots")?;
    info!(
        scope_id = %scope,
        objects,
        buckets,
        roots = roots.len(),
        "purge plan: release all roots, wipe gateway index"
    );
    if !yes {
        bail!(
            "refusing to purge (scope_id={scope}, objects={objects}, roots={}). \
             Re-run with: pigeonhole purge --yes [--expect-chat {scope}]",
            roots.len(),
        );
    }

    for (name, extents) in &roots {
        let ids: Vec<_> = extents.iter().map(|e| e.chunk).collect();
        if !ids.is_empty() {
            rt.store.release(&ids).await.with_context(|| format!("release root {name}"))?;
        }
        rt.store.db().delete_root(name).await?;
        rt.durability
            .enqueue(JournalOp::SetRoot {
                name: name.clone(),
                extents: vec![],
            })
            .await;
    }
    if let Err(e) = rt.durability.flush_journal().await {
        warn!(error = %e, "purge journal flush failed");
    }
    // Attempt a checkpoint so pins reflect cleared roots (best-effort).
    if let Err(e) = rt.durability.checkpoint(rt.store.db()).await {
        warn!(error = %e, "purge checkpoint failed");
    }

    index.wipe_all().await.context("wipe sqlite index")?;

    // Immediate reclaim pass (background sweeper uses the same path; grace still applies).
    let members: Vec<_> = rt.store.replicated().members().to_vec();
    // Purge reclaim pass; grace still protects in-flight puts.
    let sweeper = Sweeper::new(
        rt.store.db().clone(),
        rt.durability.clone(),
        members,
        cfg.sweep.clone(),
    );
    match sweeper.sweep_once().await {
        Ok(stats) => info!(
            zero_ref_chunks = stats.zero_ref_chunks,
            keys_deleted = stats.keys_deleted,
            "purge sweeper pass complete"
        ),
        Err(e) => warn!(error = %e, "purge sweeper pass failed; background sweeper will retry"),
    }
    info!("purge complete: roots released, gateway index wiped");
    Ok(())
}

/// Restore blob.db (+ gateway index) from superblock pins.
async fn cmd_restore(force: bool) -> anyhow::Result<()> {
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

    let rt = open_runtime(&cfg).await.context("open runtime")?;
    start_or_restore(
        rt.store.db(),
        rt.durability.as_ref(),
        &rt.primary_fingerprint,
        force,
    )
    .await
    .context("restore from superblocks")?;

    if rt.store.get_root("s3/index").await?.is_some() {
        restore_index_snapshot(&index, rt.store.as_ref())
            .await
            .context("restore s3/index root into gateway index")?;
        info!("gateway index restored from chunk-store root s3/index");
    } else {
        info!("no s3/index root after superblock restore; gateway index left unchanged");
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
