//! Binary-only: `pigeonhole` (no subcommand) — run the S3 gateway and optional REAPI / metrics.

use crate::background::spawn_background_tasks;
use crate::runtime::open_runtime;
use anyhow::Context;
use axum::error_handling::HandleError;
use axum::http::{Response, StatusCode};
use axum::Router;
use pigeonhole::config::Config;
use pigeonhole::gateway_metrics::track_gateway;
use pigeonhole::http_timeout::with_http_timeouts;
use pigeonhole::index::Index;
use pigeonhole::{build_s3_gateway, build_s3_service};
use s3s::{Body, HttpError};
use std::time::Duration;
use tracing::info;
#[cfg(any(feature = "bytestream", not(feature = "metrics-prometheus")))]
use tracing::warn;

pub async fn cmd_serve() -> anyhow::Result<()> {
    let cfg = Config::load().context("load config")?;
    info!(path = %cfg.config_path.display(), "loaded config");
    let rt = open_runtime(&cfg).await.context("open runtime")?;
    let index = Index::open(rt.meta.clone())
        .await
        .context("open s3 index")?;

    let s3_gateway = build_s3_gateway(cfg.clone(), index.clone(), rt.store.clone());
    spawn_background_tasks(&cfg, &rt);

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
        let cas = CasIndex::open(rt.meta.clone())
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
    let s3_service = build_s3_service(s3_gateway, &cfg.access_key, &cfg.secret_key);
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
    let instances: Vec<String> = cfg
        .instances
        .iter()
        .map(|i| format!("{}:{}", i.info.id, i.info.kind.as_str()))
        .collect();
    info!(
        ?instances,
        shards = ?cfg.placement.shards.iter().map(|s| s.group.clone()).collect::<Vec<_>>(),
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
    // A fenced tablet must not answer again: exit and let a restart claim anew.
    tokio::select! {
        r = axum::serve(listener, app) => r.context("serve")?,
        stop = rt.meta.stopped() => anyhow::bail!("{stop}"),
    }
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
