//! Optional Prometheus recorder + scrape endpoint (feature `metrics-prometheus`).

use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use pigeonhole_blob::describe_metrics;
use std::net::SocketAddr;
use std::sync::OnceLock;
use tracing::{info, warn};

/// Histogram buckets for `*_seconds` metrics (0.01 … 60).
const SECONDS_BUCKETS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global Prometheus recorder. Idempotent within a process.
pub fn install_recorder() -> anyhow::Result<&'static PrometheusHandle> {
    if let Some(h) = HANDLE.get() {
        return Ok(h);
    }
    let handle = PrometheusBuilder::new()
        .set_buckets(SECONDS_BUCKETS)
        .map_err(|e| anyhow::anyhow!("prometheus buckets: {e}"))?
        .install_recorder()
        .map_err(|e| anyhow::anyhow!("install prometheus recorder: {e}"))?;
    describe_metrics();
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
