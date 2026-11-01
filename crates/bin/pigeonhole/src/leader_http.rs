//! `GET /_pigeonhole/leader` on the S3 port: whether this node leads its
//! tablet's cluster. 200 on the leader, 503 elsewhere, so a load balancer's
//! health check sends clients to the leader (docs/raft-over-chat.md §
//! Clients). A bucket can't be named `_pigeonhole`: no S3 request lands here.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use pigeonhole_meta::MetaStore;
use std::sync::Arc;

pub const PATH: &str = "/_pigeonhole/leader";

/// On a node that does not lead, every S3 request gets 503
/// `ServiceUnavailable`: writes would fail there, and reads would see a
/// state that may lag the leader's. Clients ask the leader.
pub async fn only_on_the_leader(
    State(meta): State<Arc<MetaStore>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    // This route and `/metrics` (when served here) answer on every node.
    if meta.is_leader() || matches!(req.uri().path(), PATH | "/metrics") {
        return next.run(req).await;
    }
    let leader = meta.leader();
    tracing::info!(op = %format!("{} {}", req.method(), req.uri().path()), leader, "not the leader");
    let message = match leader {
        Some(id) => format!("not the leader; node {id} is"),
        None => "not the leader; no leader known".to_string(),
    };
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>ServiceUnavailable</Code><Message>{message}</Message></Error>"
    );
    (StatusCode::SERVICE_UNAVAILABLE, [("content-type", "application/xml"), ("retry-after", "1")], body).into_response()
}

pub fn routes(meta: Arc<MetaStore>) -> Router {
    Router::new().route(PATH, get(leader)).with_state(meta)
}

async fn leader(State(meta): State<Arc<MetaStore>>) -> (StatusCode, Json<serde_json::Value>) {
    let leading = meta.is_leader() && !meta.is_stopped();
    let status = if leading { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (status, Json(serde_json::json!({ "node": meta.node(), "leader": meta.leader(), "leading": leading })))
}
