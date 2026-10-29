//! One span per S3 request (docs/observability.md): an id (also sent back as
//! `x-pigeonhole-request-id`), the method, the path and, once s3s routed it,
//! the operation. Everything logged while serving it — chunk store, tablet
//! commit, store calls — carries the span. The request itself is logged
//! once: `warn` for a 5xx, `info` when slower than `[log] slow_request_ms`,
//! `debug` otherwise. The query string is never logged (presigned URLs carry
//! credentials in it).

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use pigeonhole_gateway_s3::S3OpSlot;
use std::time::{Duration, Instant};
use tracing::field::Empty;
use tracing::{debug, info, info_span, warn, Instrument};

/// Response header carrying the request's id.
pub const REQUEST_ID_HEADER: &str = "x-pigeonhole-request-id";

/// A short random id for one request.
fn request_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..16].to_string()
}

pub async fn trace_request(gateway: &'static str, slow: Duration, mut req: Request, next: Next) -> Response {
    let id = request_id();
    let slot = req.extensions().get::<S3OpSlot>().cloned().unwrap_or_default();
    req.extensions_mut().insert(slot.clone());
    let span = info_span!("request", gateway, id = %id, method = %req.method(), path = %req.uri().path(), op = Empty);
    let t0 = Instant::now();
    let mut resp = next.run(req).instrument(span.clone()).await;
    let elapsed = t0.elapsed();
    if let Some(op) = slot.0.get() {
        span.record("op", op.as_str());
    }
    let (status, ms) = (resp.status().as_u16(), elapsed.as_millis() as u64);
    span.in_scope(|| {
        if status >= 500 {
            warn!(status, ms, "request failed");
        } else if elapsed >= slow {
            info!(status, ms, "slow request");
        } else {
            debug!(status, ms, "request");
        }
    });
    if let Ok(v) = HeaderValue::from_str(&id) {
        resp.headers_mut().insert(REQUEST_ID_HEADER, v);
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::Router;
    use tower::ServiceExt;

    #[tokio::test]
    async fn every_answer_carries_a_request_id() {
        let app = Router::new()
            .fallback(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") })
            .layer(axum::middleware::from_fn(|req, next| trace_request("s3", Duration::from_secs(5), req, next)));
        let a = app.clone().oneshot(Request::builder().uri("/b/k?X-Amz-Signature=secret").body(Body::empty()).unwrap());
        let b = app.oneshot(Request::builder().uri("/b/k").body(Body::empty()).unwrap());
        let (a, b) = (a.await.unwrap(), b.await.unwrap());
        let id = |r: &Response| r.headers().get(REQUEST_ID_HEADER).unwrap().to_str().unwrap().to_string();
        assert_eq!(a.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(id(&a).len(), 16);
        assert_ne!(id(&a), id(&b), "one id per request");
    }
}
