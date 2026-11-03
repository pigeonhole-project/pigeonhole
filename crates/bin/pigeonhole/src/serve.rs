//! Binary-only: `pigeonhole` (no subcommand) — run the S3 gateway and optional REAPI / metrics.

use crate::runtime;
use anyhow::Context;
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::Router;
use pigeonhole::config::dynamic::Task;
use pigeonhole::config::Config;
use pigeonhole::gateway_metrics::track_gateway;
use pigeonhole::http_timeout::with_http_timeouts;
use pigeonhole::request_log::trace_request;
use pigeonhole::{build_s3_service, S3Gateway};
use pigeonhole_node::Stack;
use s3s::{Body, HttpError};
use std::future::IntoFuture;
use tokio::sync::watch;
use tracing::info;
#[cfg(any(feature = "bytestream", not(feature = "metrics-prometheus")))]
use tracing::warn;

pub async fn cmd_serve() -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    info!(path = %cfg.config_path.display(), "loaded config");
    // Before the node: booting the tablet records metrics too.
    let metrics_on_main = setup_metrics(&cfg)?;
    let mut node = runtime::open(&cfg, namespaces, &extra_loops(&cfg)).await.context("open the node")?;
    node.start();
    // Set on SIGINT / SIGTERM: the servers finish their requests in flight and stop.
    let (stop, stopping) = watch::channel(false);
    spawn_admin(&cfg, &node, stopping.clone()).await?;

    #[cfg(feature = "bytestream")]
    spawn_bytestream(&cfg, &node).await?;

    let app = s3_app(&cfg, node.s3.clone(), node.meta.clone(), metrics_on_main);

    let addr = cfg.listen_addr.clone();
    let listener = tokio::net::TcpListener::bind(&addr).await.with_context(|| format!("bind {addr}"))?;
    let instances: Vec<String> = cfg.instances.iter().map(|i| format!("{}:{}", i.info.id, i.info.kind)).collect();
    info!(
        ?instances,
        shards = ?cfg.placement.shards.iter().map(|s| s.group.clone()).collect::<Vec<_>>(),
        chunk_size = cfg.chunk_size,
        chunk_codec = %cfg.chunk_codec,
        bytestream = cfg.bytestream.enabled,
        metrics = cfg.metrics.enabled,
        body_idle_timeout = ?cfg.http.request_timeout,
        headers_timeout = ?cfg.http.headers_timeout,
        max_concurrent = cfg.http.max_concurrent_requests,
        max_headers = cfg.http.max_headers,
        "pigeonhole listening on http://{addr}"
    );
    let mut serving = tokio::spawn(axum::serve(listener, app).with_graceful_shutdown(stopped(stopping)).into_future());
    // A fenced tablet must not answer again: exit and let a restart claim anew.
    tokio::select! {
        r = &mut serving => r.context("the S3 server")?.context("serve")?,
        stop = node.meta.stopped() => anyhow::bail!("{stop}"),
        signal = shutdown_signal() => {
            info!(%signal, "shutting down: finishing the requests in flight");
            stop.send_replace(true);
            match tokio::time::timeout(cfg.http.request_timeout, serving).await {
                Ok(served) => served.context("the S3 server")?.context("serve")?,
                Err(_) => tracing::warn!("requests still in flight after the request timeout: dropped"),
            }
        }
    }
    close(&node).await
}

/// The node's tablet released (a lease log given back), then the fake
/// channels' (feature `fake`).
async fn close(node: &Stack) -> anyhow::Result<()> {
    info!("releasing the metadata log");
    node.close().await?;
    crate::fakes::close().await
}

/// Ready once `stopping` turns true (or its sender is gone).
async fn stopped(mut stopping: watch::Receiver<bool>) {
    let _ = stopping.wait_for(|&s| s).await;
}

/// The tablet's namespaces beyond the node's: the REAPI's CAS index.
fn namespaces(ns: pigeonhole_meta::Namespaces) -> pigeonhole_meta::Namespaces {
    #[cfg(feature = "bytestream")]
    let ns = pigeonhole_gateway_bytestream::cas_index::CasIndex::register(ns);
    ns
}

/// The loops this process runs beyond the node's own: the tablet's gauges
/// (`[metrics]`), the REAPI's GCs (`[bytestream]`).
fn extra_loops(cfg: &Config) -> Vec<Task> {
    let mut extra = Vec::new();
    if cfg.metrics.enabled {
        extra.push(Task::Metrics);
    }
    if cfg!(feature = "bytestream") && cfg.bytestream.enabled {
        extra.extend([Task::CasGc, Task::UploadGc]);
    }
    extra
}

/// The REAPI / ByteStream server on its own port, if enabled.
#[cfg(feature = "bytestream")]
async fn spawn_bytestream(cfg: &Config, node: &Stack) -> anyhow::Result<()> {
    use pigeonhole_gateway_bytestream::cas_index::CasIndex;
    use pigeonhole_gateway_bytestream::config::{BytestreamConfig, Gc};
    if !cfg.bytestream.enabled {
        return Ok(());
    }
    let bs_cfg = BytestreamConfig {
        enabled: true,
        listen_addr: cfg.bytestream.listen_addr.clone(),
        instance_name: cfg.bytestream.instance_name.clone(),
        max_batch_total_size_bytes: cfg.bytestream.max_batch_total_size as i64,
        ..Default::default()
    };
    let gc = Gc { cas: node.dynamic.cas_gc.schedule(), uploads: node.dynamic.upload_gc.schedule() };
    let cas = CasIndex::open(node.meta.clone()).await.context("open cas index")?;
    let bs_store = node.store.clone();
    tokio::spawn(async move {
        if let Err(e) = pigeonhole_gateway_bytestream::server::serve(bs_cfg, cas, bs_store, gc).await {
            warn!(error = %e, "bytestream server exited");
        }
    });
    Ok(())
}

/// The admin API (`[admin] listen_addr`, off by default): the dynamic
/// config and passes on demand (docs/dynamic-config.md), the UI's ops.
async fn spawn_admin(cfg: &Config, node: &Stack, stopping: watch::Receiver<bool>) -> anyhow::Result<()> {
    let Some(addr) = &cfg.admin.listen_addr else { return Ok(()) };
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("bind admin {addr}"))?;
    let sock = listener.local_addr()?;
    if !sock.ip().is_loopback() {
        tracing::warn!(%sock, "the admin API has no authentication and listens beyond this host");
    }
    info!(%sock, "admin API listening");
    let app = pigeonhole::admin_http::with_ui(node.admin.clone(), cfg.admin.ui_dir.as_deref());
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).with_graceful_shutdown(stopped(stopping)).await {
            tracing::error!(error = %e, "admin API stopped");
        }
    });
    Ok(())
}

/// The S3 gateway as an HTTP app: header limits, metrics, request spans
/// and timeouts around it (and `/metrics` when served here).
fn s3_app(
    cfg: &Config,
    s3_gateway: S3Gateway,
    meta: std::sync::Arc<pigeonhole_meta::MetaStore>,
    metrics_on_main: bool,
) -> Router {
    let max_headers = cfg.http.max_headers;
    let slow_request = cfg.log.slow_request;
    let s3_service = build_s3_service(s3_gateway, &cfg.access_key, &cfg.secret_key);
    let s3_service = HandleError::new(s3_service, handle_s3_error);
    #[cfg(feature = "metrics-prometheus")]
    let app = match metrics_on_main {
        true => pigeonhole::prometheus::layer_metrics_route(Router::new()),
        false => Router::new(),
    };
    #[cfg(not(feature = "metrics-prometheus"))]
    let app = {
        let _ = metrics_on_main;
        Router::new()
    };
    let clustered = cfg.cluster.is_some();
    let app = app.merge(pigeonhole::leader_http::routes(meta.clone())).fallback_service(s3_service);
    let app = match clustered {
        true => app.layer(axum::middleware::from_fn_with_state(meta, pigeonhole::leader_http::only_on_the_leader)),
        false => app,
    };
    let app = app
        .layer(axum::middleware::from_fn(move |req, next| {
            let max = max_headers;
            async move { limit_request_headers(max, req, next).await }
        }))
        .layer(axum::middleware::from_fn(|req, next| track_gateway("s3", req, next)))
        // Outermost: the request's span covers everything below.
        .layer(axum::middleware::from_fn(move |req, next| trace_request("s3", slow_request, req, next)));
    with_http_timeouts(app, cfg.http.headers_timeout, cfg.http.request_timeout, cfg.http.max_concurrent_requests)
}

/// SIGINT or SIGTERM: which one came.
async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install a SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = term.recv() => "SIGTERM",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "Ctrl-C"
    }
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
                let sock: std::net::SocketAddr =
                    addr.parse().with_context(|| format!("parse metrics.listen_addr {addr}"))?;
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

async fn handle_s3_error(err: HttpError) -> Response<Body> {
    tracing::error!(?err, "s3s HTTP error");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(err.to_string()))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}
