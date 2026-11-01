//! The admin API (`[admin] listen_addr`, docs/dynamic-config.md): the
//! dynamic config and passes of the background loops on demand.
//!
//! - `GET /admin/config`: every dynamic key (its value, the file's, the
//!   override) and every task (its period, when it passes next);
//! - `PUT /admin/config/{key}`, the value as the body (`5s`, `off`, `16`):
//!   override it; `DELETE /admin/config/{key}`: back to the file's;
//! - `POST /admin/run/{task}`: a pass now, even while the task is off.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use pigeonhole_config::DynConfig;
use std::sync::Arc;

pub fn router(dynamic: Arc<DynConfig>) -> Router {
    Router::new()
        .route("/admin/config", get(show))
        .route("/admin/config/{key}", put(set).delete(unset))
        .route("/admin/run/{task}", post(run))
        .with_state(dynamic)
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
