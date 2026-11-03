//! The admin API over the dynamic config (docs/dynamic-config.md).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use pigeonhole::config::dynamic::Task;
use pigeonhole::config::{Config, DynConfig};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

async fn call(app: &axum::Router, method: &str, uri: &str, body: &str) -> (StatusCode, String) {
    let req = Request::builder().method(method).uri(uri).body(Body::from(body.to_string())).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn the_admin_api_overrides_and_runs() {
    let dir = tempfile::tempdir().unwrap();
    let text = "[repair]\ninterval = \"30s\"\n[chunk]\nstore = \"mem\"\n[[store]]\nid = \"mem\"\nkind = \"memory\"\n";
    let cfg = Config::from_toml(text, Path::new("test.toml")).unwrap();
    let meta = pigeonhole_testkit::tablet(dir.path(), DynConfig::register).await;
    let dynamic = Arc::new(DynConfig::open(cfg, meta, &Task::ALL));
    let app = pigeonhole::admin_http::router(dynamic.clone());

    assert_eq!(call(&app, "PUT", "/admin/config/repair.interval", "off").await.0, StatusCode::NO_CONTENT);
    assert_eq!(dynamic.repair.get().interval, None);
    let (status, why) = call(&app, "PUT", "/admin/config/listen_addr", "0.0.0.0:1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(why.contains("cannot change while running"), "{why}");
    assert_eq!(call(&app, "PUT", "/admin/config/sweep.interval", "0s").await.0, StatusCode::BAD_REQUEST);

    let (status, body) = call(&app, "GET", "/admin/config", "").await;
    assert_eq!(status, StatusCode::OK);
    let view: serde_json::Value = serde_json::from_str(&body).unwrap();
    let repair = view["keys"].as_array().unwrap().iter().find(|k| k["key"] == "repair.interval").unwrap();
    assert_eq!((&repair["value"], &repair["file"], &repair["override"]), (&"off".into(), &"30s".into(), &"off".into()));
    let task = view["tasks"].as_array().unwrap().iter().find(|t| t["task"] == "repair").unwrap();
    assert_eq!((&task["every"], &task["next_in_ms"]), (&"off".into(), &serde_json::Value::Null));

    assert_eq!(call(&app, "POST", "/admin/run/repair", "").await.0, StatusCode::ACCEPTED);
    assert_eq!(call(&app, "POST", "/admin/run/nap", "").await.0, StatusCode::NOT_FOUND);

    assert_eq!(call(&app, "DELETE", "/admin/config/repair.interval", "").await.0, StatusCode::NO_CONTENT);
    assert_eq!(dynamic.repair.get().interval, Some(Duration::from_secs(30)));
    assert_eq!(call(&app, "DELETE", "/admin/config/repair.interval", "").await.0, StatusCode::NOT_FOUND);
}
