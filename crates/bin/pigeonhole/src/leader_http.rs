//! Health checks on the S3 port (docs/cluster.md § Clients). A bucket
//! can't be named `_pigeonhole`: no S3 request lands here.
//!
//! - `GET /_pigeonhole/leader`: 200 on the leader that has its quorum, 503
//!   elsewhere: a load balancer's check that sends clients where writes work.
//! - `GET /_pigeonhole/readable`: 200 on the leader with or without its
//!   quorum: a check for reads, which a read-only leader still serves.

use axum::extract::State;
use axum::http::{Method, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use pigeonhole_meta::MetaStore;
use std::sync::Arc;

pub const PATH: &str = "/_pigeonhole/leader";
pub const READABLE: &str = "/_pigeonhole/readable";

/// On a node that does not lead, every S3 request gets 503
/// `ServiceUnavailable`: writes would fail there, and reads would see a
/// state that may lag the leader's. Clients ask the leader. A leader
/// without its quorum (read-only) serves reads and turns writes away the
/// same way, before reading their bodies: nothing gets uploaded.
pub async fn only_on_the_leader(
    State(meta): State<Arc<MetaStore>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // These routes and `/metrics` (when served here) answer on every node.
    if matches!(req.uri().path(), PATH | READABLE | "/metrics") {
        return next.run(req).await;
    }
    let op = || format!("{} {}", req.method(), req.uri().path());
    if !meta.is_leader() {
        let why = match meta.serving_term() {
            Err(e) => format!("{e:#}"),
            Ok(_) => "not the leader".to_string(),
        };
        tracing::info!(op = %op(), leader = meta.leader(), why, "not serving");
        return unavailable(&why);
    }
    let reads = matches!(*req.method(), Method::GET | Method::HEAD);
    if meta.is_read_only() && !reads {
        tracing::info!(op = %op(), "read-only: no quorum");
        return unavailable("read-only: this leader has no quorum; writes wait for one");
    }
    next.run(req).await
}

/// S3's 503 `ServiceUnavailable` with `message`: retried by SDKs.
fn unavailable(message: &str) -> axum::response::Response {
    use axum::response::IntoResponse;
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>ServiceUnavailable</Code><Message>{message}</Message></Error>"
    );
    (StatusCode::SERVICE_UNAVAILABLE, [("content-type", "application/xml"), ("retry-after", "1")], body).into_response()
}

pub fn routes(meta: Arc<MetaStore>) -> Router {
    Router::new().route(PATH, get(leader)).route(READABLE, get(readable)).with_state(meta)
}

/// 200 where writes work: the leader, with its quorum.
async fn leader(State(meta): State<Arc<MetaStore>>) -> (StatusCode, Json<serde_json::Value>) {
    health(&meta, !meta.is_read_only())
}

/// 200 where reads work: the leader, read-only or not.
async fn readable(State(meta): State<Arc<MetaStore>>) -> (StatusCode, Json<serde_json::Value>) {
    health(&meta, true)
}

fn health(meta: &MetaStore, need: bool) -> (StatusCode, Json<serde_json::Value>) {
    let leading = meta.is_leader() && !meta.is_stopped();
    let read_only = leading && meta.is_read_only();
    let status = if leading && need { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    let body =
        serde_json::json!({ "node": meta.node(), "leader": meta.leader(), "leading": leading, "read_only": read_only });
    (status, Json(body))
}
