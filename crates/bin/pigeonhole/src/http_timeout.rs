//! HTTP timeouts for streaming S3 uploads/downloads.
//!
//! - **Headers**: time to produce response headers, measured only after the request
//!   body reaches EOF (large PUTs are not killed by wall-clock while bytes flow).
//! - **Body idle**: no frame for N seconds on request or response → abort.

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use http_body::{Body as HttpBody, Frame};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::oneshot;
use tower::ServiceBuilder;
use tower_http::timeout::{RequestBodyTimeoutLayer, ResponseBodyTimeoutLayer};

/// Apply headers-after-body timeout + idle body timeouts + concurrency limit.
pub fn with_http_timeouts(
    router: Router,
    headers_timeout: Duration,
    body_idle_timeout: Duration,
    max_concurrent: usize,
) -> Router {
    let headers = Arc::new(headers_timeout);
    // Last `.layer` is outermost: headers middleware wraps the idle/concurrency stack.
    router
        .layer(
            ServiceBuilder::new()
                .layer(RequestBodyTimeoutLayer::new(body_idle_timeout))
                .layer(ResponseBodyTimeoutLayer::new(body_idle_timeout))
                .concurrency_limit(max_concurrent),
        )
        .layer(axum::middleware::from_fn(move |req, next| {
            let headers = headers.clone();
            async move { headers_after_body_timeout(*headers, req, next).await }
        }))
}

async fn headers_after_body_timeout(timeout: Duration, req: Request, next: Next) -> Response {
    if timeout.is_zero() {
        return next.run(req).await;
    }

    let (parts, body) = req.into_parts();
    let (eof_tx, eof_rx) = oneshot::channel();
    let req = Request::from_parts(parts, Body::new(WatchEofBody::new(body, eof_tx)));

    tokio::select! {
        biased;
        resp = next.run(req) => resp,
        _ = headers_deadline(eof_rx, timeout) => StatusCode::REQUEST_TIMEOUT.into_response(),
    }
}

async fn headers_deadline(eof_rx: oneshot::Receiver<()>, timeout: Duration) {
    match eof_rx.await {
        Ok(()) => tokio::time::sleep(timeout).await,
        Err(_) => std::future::pending::<()>().await,
    }
}

pin_project! {
    struct WatchEofBody<B> {
        #[pin]
        inner: B,
        eof_tx: Option<oneshot::Sender<()>>,
    }
}

impl<B: HttpBody> WatchEofBody<B> {
    fn new(inner: B, eof_tx: oneshot::Sender<()>) -> Self {
        let mut eof_tx = Some(eof_tx);
        if inner.is_end_stream() {
            if let Some(tx) = eof_tx.take() {
                let _ = tx.send(());
            }
        }
        Self { inner, eof_tx }
    }
}

impl<B: HttpBody> HttpBody for WatchEofBody<B>
where
    B::Error: Into<axum::BoxError>,
{
    type Data = B::Data;
    type Error = axum::BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match this.inner.poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => {
                if let Some(tx) = this.eof_tx.take() {
                    let _ = tx.send(());
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}
