//! The admin API (`[admin] listen_addr`, docs/dynamic-config.md): the
//! dynamic config and passes of the background loops on demand.
//!
//! - `GET /admin/config`: every dynamic key (its value, the file's, the
//!   override) and every task (its period, when it passes next);
//! - `PUT /admin/config/{key}`, the value as the body (`5s`, `off`, `16`):
//!   override it; `DELETE /admin/config/{key}`: back to the file's;
//! - `POST /admin/run/{task}`: a pass now, even while the task is off;
//! - `POST /admin/ops`: the UI's ops (pigeonhole-admin), as the site's demo
//!   sends them to its node: a JSON line, then an upload's bytes;
//! - `/`: the UI itself (`[admin] ui_dir`), from this address, so that no
//!   other site may call the API from a browser (no CORS).

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use pigeonhole_admin::{Admin, Request};
use pigeonhole_config::DynConfig;
use std::sync::Arc;

pub fn router(dynamic: Arc<DynConfig>) -> Router {
    Router::new()
        .route("/admin/config", get(show))
        .route("/admin/config/{key}", put(set).delete(unset))
        .route("/admin/run/{task}", post(run))
        .with_state(dynamic)
}

/// [`router`], the UI's ops on `admin`, and the UI from `ui_dir`.
pub fn with_ui(dynamic: Arc<DynConfig>, admin: Arc<Admin>, ui_dir: Option<&std::path::Path>) -> Router {
    let app = router(dynamic).merge(Router::new().route("/admin/ops", post(ops)).with_state(admin));
    match ui_dir {
        Some(dir) => app.fallback_service(tower_http::services::ServeDir::new(dir)),
        None => app,
    }
}

/// One op: a JSON line, then the body (an upload's bytes); the answer is
/// `{"reply": …}`, as the demo's worker answers.
async fn ops(State(admin): State<Arc<Admin>>, body: Bytes) -> Json<serde_json::Value> {
    let split = body.iter().position(|&b| b == b'\n').unwrap_or(body.len());
    let reply = match serde_json::from_slice::<Request>(&body[..split]) {
        Ok(r) => admin.handle(&r, body.slice((split + 1).min(body.len())..)).await,
        Err(e) => Err(anyhow::anyhow!("bad op: {e}")),
    };
    Json(match reply {
        Ok(v) => serde_json::json!({ "reply": v }),
        Err(e) => serde_json::json!({ "reply": { "error": format!("{e:#}") } }),
    })
}

async fn show(State(d): State<Arc<DynConfig>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "keys": d.keys(), "tasks": d.tasks() }))
}

async fn set(State(d): State<Arc<DynConfig>>, Path(key): Path<String>, body: Bytes) -> Response {
    let Ok(value) = std::str::from_utf8(&body) else {
        return (StatusCode::BAD_REQUEST, "the value is not UTF-8\n").into_response();
    };
    match d.set(&key, value.trim()).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e:#}\n")).into_response(),
    }
}

async fn unset(State(d): State<Arc<DynConfig>>, Path(key): Path<String>) -> Response {
    match d.unset(&key).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, format!("{key} has no override\n")).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e:#}\n")).into_response(),
    }
}

async fn run(State(d): State<Arc<DynConfig>>, Path(task): Path<String>) -> Response {
    match d.run_now(&task) {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("{e:#}\n")).into_response(),
    }
}
