//! Idle body + headers-after-body timeouts (no external network).

use axum::body::{to_bytes, Body};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;
use futures::stream;
use pigeonhole::http_timeout::with_http_timeouts;
use std::convert::Infallible;
use std::time::Duration;
use tokio::time::sleep;
use tower::ServiceExt;

fn test_app(headers: Duration, idle: Duration) -> Router {
    let router = Router::new()
        .route(
            "/slow-body",
            get(|| async {
                let s = stream::unfold(0u8, |i| async move {
                    if i >= 8 {
                        return None;
                    }
                    sleep(Duration::from_millis(400)).await;
                    Some((
                        Ok::<_, Infallible>(bytes::Bytes::from_static(b"x")),
                        i + 1,
                    ))
                });
                Body::from_stream(s)
            }),
        )
        .route(
            "/hang-body",
            get(|| async {
                let s = stream::once(async {
                    sleep(Duration::from_secs(30)).await;
                    Ok::<_, Infallible>(bytes::Bytes::from_static(b"late"))
                });
                Body::from_stream(s)
            }),
        )
        .route(
            "/hang-headers",
            get(|| async {
                sleep(Duration::from_secs(30)).await;
                "ok"
            }),
        )
        .route(
            "/echo",
            post(|body: Body| async move {
                match to_bytes(body, usize::MAX).await {
                    Ok(bytes) => Ok::<_, StatusCode>(bytes),
                    Err(_) => Err(StatusCode::REQUEST_TIMEOUT),
                }
            }),
        );
    with_http_timeouts(router, headers, idle, 32)
}

#[tokio::test]
async fn slow_response_body_survives_past_idle_window() {
    // Idle 800ms; drip a byte every 400ms → must complete (~3.2s wall clock).
    let app = test_app(Duration::from_secs(5), Duration::from_millis(800));
    let req = axum::http::Request::builder()
        .uri("/slow-body")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("slow body should finish");
    assert_eq!(bytes.len(), 8, "expected all 8 dripped bytes");
}

#[tokio::test]
async fn stalled_response_body_is_cut_off() {
    let app = test_app(Duration::from_secs(5), Duration::from_millis(200));
    let req = axum::http::Request::builder()
        .uri("/hang-body")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let err = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect_err("idle timeout should fail the body");
    let _ = err;
}

#[tokio::test]
async fn slow_request_body_survives_past_idle_window() {
    let app = test_app(Duration::from_secs(5), Duration::from_millis(800));
    let s = stream::unfold(0u8, |i| async move {
        if i >= 8 {
            return None;
        }
        sleep(Duration::from_millis(400)).await;
        Some((
            Ok::<_, Infallible>(bytes::Bytes::from_static(b"y")),
            i + 1,
        ))
    });
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/echo")
        .body(Body::from_stream(s))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(bytes.len(), 8);
}

#[tokio::test]
async fn headers_timeout_after_empty_body() {
    let app = test_app(Duration::from_millis(100), Duration::from_secs(5));
    let req = axum::http::Request::builder()
        .uri("/hang-headers")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
}
