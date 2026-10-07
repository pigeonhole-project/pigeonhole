//! Gateway request duration middleware (metrics facade; no-op without a recorder).

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use pigeonhole_blob::{record_bytes_to_clients, record_gateway_request};
use std::time::Instant;

/// Record wall-clock duration of each HTTP request with label `gateway`.
pub async fn track_gateway(gateway: &'static str, req: Request, next: Next) -> Response {
    let t0 = Instant::now();
    let resp = next.run(req).await;
    let status = resp.status().as_u16();
    let outcome = if status < 400 {
        "ok"
    } else if status < 500 {
        "client_error"
    } else {
        "server_error"
    };
    record_gateway_request(gateway, outcome, t0.elapsed());
    if let Some(len) = resp
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
    {
        record_bytes_to_clients(gateway, len);
    }
    resp
}
