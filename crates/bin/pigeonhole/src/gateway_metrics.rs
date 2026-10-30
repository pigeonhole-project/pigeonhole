//! Gateway request metrics middleware (metrics facade; no-op without a recorder).

use axum::body::Body;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use http_body::{Body as HttpBody, Frame};
use pigeonhole_blob::{record_bytes_from_clients, record_bytes_to_clients, record_gateway_request};
use pigeonhole_gateway_s3::S3OpSlot;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

/// Per request: time to response headers by S3 operation and outcome, and
/// the body bytes that actually crossed the wire each way (a HEAD sends none,
/// whatever its `Content-Length` says; an aborted download counts what left).
pub async fn track_gateway(gateway: &'static str, req: Request, next: Next) -> Response {
    let (mut parts, body) = req.into_parts();
    // The request log may have put one in already: one slot per request.
    let slot = parts.extensions.get::<S3OpSlot>().cloned().unwrap_or_default();
    parts.extensions.insert(slot.clone());
    let body = Body::new(CountingBody::new(body, gateway, slot.clone(), Direction::FromClient));
    // Dropped unanswered (the client went away): recorded as `cancelled`.
    let mut pending = Pending { gateway, slot: slot.clone(), t0: Instant::now(), outcome: "cancelled" };
    let resp = next.run(Request::from_parts(parts, body)).await;
    let status = resp.status().as_u16();
    pending.outcome = if status < 400 {
        "ok"
    } else if status < 500 {
        "client_error"
    } else {
        "server_error"
    };
    drop(pending);
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, Body::new(CountingBody::new(body, gateway, slot, Direction::ToClient)))
}

/// Records a request's duration and outcome when dropped.
struct Pending {
    gateway: &'static str,
    slot: S3OpSlot,
    t0: Instant,
    outcome: &'static str,
}

impl Drop for Pending {
    fn drop(&mut self) {
        record_gateway_request(self.gateway, op_of(&self.slot), self.outcome, self.t0.elapsed());
    }
}

/// The operation s3s routed the request to, or `unknown` (bad route or auth).
fn op_of(slot: &S3OpSlot) -> &str {
    slot.0.get().map_or("unknown", String::as_str)
}

#[derive(Clone, Copy)]
enum Direction {
    FromClient,
    ToClient,
}

pin_project! {
    /// Counts data bytes polled through it; records them once, on drop.
    struct CountingBody<B> {
        #[pin]
        inner: B,
        gateway: &'static str,
        slot: S3OpSlot,
        direction: Direction,
        bytes: u64,
    }

    impl<B> PinnedDrop for CountingBody<B> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            match this.direction {
                Direction::FromClient => record_bytes_from_clients(this.gateway, op_of(this.slot), *this.bytes),
                Direction::ToClient => record_bytes_to_clients(this.gateway, op_of(this.slot), *this.bytes),
            }
        }
    }
}

impl<B> CountingBody<B> {
    fn new(inner: B, gateway: &'static str, slot: S3OpSlot, direction: Direction) -> Self {
        Self { inner, gateway, slot, direction, bytes: 0 }
    }
}

impl<B: HttpBody> HttpBody for CountingBody<B>
where
    B::Data: bytes::Buf,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let polled = this.inner.poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &polled {
            if let Some(data) = frame.data_ref() {
                *this.bytes += bytes::Buf::remaining(data) as u64;
            }
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}
