//! Optional Prometheus recorder + scrape endpoint (feature `metrics-prometheus`).

use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use pigeonhole_blob::describe;
use std::net::SocketAddr;
use std::sync::OnceLock;
use tracing::{info, warn};

/// Buckets for `*_seconds` histograms: a local disk answers in well under a
/// millisecond, a chat upload or a flood wait takes seconds to minutes.
const SECONDS_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0, 30.0, 60.0, 300.0,
];

/// Stored / logical size of a block: 0 (all zeroes) … 1 (incompressible).
const RATIO_BUCKETS: &[f64] = &[0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0, 1.05];

/// Small counts: parts per chunk, commands per log entry.
const COUNT_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0];

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global Prometheus recorder. Idempotent within a process.
pub fn install_recorder() -> anyhow::Result<&'static PrometheusHandle> {
    if let Some(h) = HANDLE.get() {
        return Ok(h);
    }
    let buckets = |e| anyhow::anyhow!("prometheus buckets: {e}");
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), SECONDS_BUCKETS)
        .map_err(buckets)?
        .set_buckets_for_metric(Matcher::Suffix("_ratio".into()), RATIO_BUCKETS)
        .map_err(buckets)?
        .set_buckets_for_metric(Matcher::Suffix("_per_chunk".into()), COUNT_BUCKETS)
        .map_err(buckets)?
        .set_buckets_for_metric(Matcher::Suffix("_per_entry".into()), COUNT_BUCKETS)
        .map_err(buckets)?
        .install_recorder()
        .map_err(|e| anyhow::anyhow!("install prometheus recorder: {e}"))?;
    for docs in crate::metric_docs() {
        describe(docs);
    }
    let _ = HANDLE.set(handle);
    Ok(HANDLE.get().expect("prometheus handle just set"))
}

/// Axum route handler for `GET /metrics`.
async fn render_metrics() -> impl IntoResponse {
    match HANDLE.get() {
        Some(h) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            h.render(),
        )
            .into_response(),
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// Attach `GET /metrics` to an existing router (same listen address as the gateway).
pub fn layer_metrics_route(app: Router) -> Router {
    app.route("/metrics", get(render_metrics))
}

/// Serve Prometheus on a dedicated address (does not share the S3 gateway port).
pub fn spawn_dedicated_listener(addr: SocketAddr) {
    tokio::spawn(async move {
        let app = Router::new().route("/metrics", get(render_metrics));
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                info!(%addr, "prometheus metrics listening");
                if let Err(e) = axum::serve(listener, app).await {
                    warn!(error = %e, "prometheus metrics server exited");
                }
            }
            Err(e) => warn!(error = %e, %addr, "bind prometheus metrics failed"),
        }
    });
}
