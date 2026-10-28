//! The gateway middleware: S3 operation label, body bytes that really crossed
//! the wire (none for a HEAD, whatever its Content-Length says).

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, Method, StatusCode};
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use metrics_util::MetricKind;
use pigeonhole::gateway_metrics::track_gateway;
use pigeonhole::S3OpSlot;
use tower::ServiceExt;

/// Stands in for s3s: names the operation, answers HEAD like GET without a body.
async fn fake_s3(req: Request) -> Response {
    let op = if req.method() == Method::HEAD { "HeadObject" } else { "GetObject" };
    let _ = req.extensions().get::<S3OpSlot>().expect("middleware adds a slot").0.set(op.into());
    let body = if req.method() == Method::HEAD { Body::empty() } else { Body::from(vec![7u8; 1000]) };
    Response::builder().status(StatusCode::OK).header(header::CONTENT_LENGTH, 1000).body(body).unwrap()
}

fn counter(snapshot: &[(metrics_util::CompositeKey, Option<metrics::Unit>, Option<metrics::SharedString>, DebugValue)], name: &str, op: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, ..)| k.kind() == MetricKind::Counter && k.key().name() == name)
        .filter(|(k, ..)| k.key().labels().any(|l| l.key() == "op" && l.value() == op))
        .map(|(.., v)| match v {
            DebugValue::Counter(n) => *n,
            _ => 0,
        })
        .sum()
}

#[test]
fn head_sends_no_bytes_and_ops_are_labelled() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let app = Router::new()
                .route("/{*key}", any(fake_s3))
                .layer(axum::middleware::from_fn(|req, next| track_gateway("s3", req, next)));
            for method in [Method::HEAD, Method::GET, Method::PUT] {
                let req = Request::builder().method(method.clone()).uri("/b/k").body(Body::from(vec![1u8; 300])).unwrap();
                let resp = app.clone().oneshot(req).await.unwrap();
                axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            }
        });
    });
    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(counter(&snapshot, "pigeonhole_bytes_to_clients_total", "HeadObject"), 0);
    // The fake answers PUT like GET: 1000 each, and reads no request body.
    assert_eq!(counter(&snapshot, "pigeonhole_bytes_to_clients_total", "GetObject"), 2000);
    assert_eq!(counter(&snapshot, "pigeonhole_bytes_from_clients_total", "GetObject"), 0);
    let ops: Vec<String> = snapshot
        .iter()
        .filter(|(k, ..)| k.key().name() == "pigeonhole_gateway_request_duration_seconds")
        .flat_map(|(k, ..)| k.key().labels().filter(|l| l.key() == "op").map(|l| l.value().to_owned()).collect::<Vec<_>>())
        .collect();
    assert!(ops.contains(&"HeadObject".to_string()) && ops.contains(&"GetObject".to_string()), "{ops:?}");
}
