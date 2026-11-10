//! Only the leader answers (docs/cluster.md), as on the S3 port: a
//! follower's reads would see a state that may lag the leader's (a blob
//! written there reported missing) and its writes would fail. A follower,
//! or a leader not serving yet, answers every call `UNAVAILABLE`; a leader
//! without its quorum answers reads and turns writes away the same way,
//! before reading their messages.

use pigeonhole_meta::MetaStore;
use std::convert::Infallible;
use std::sync::Arc;
use tonic::body::BoxBody;
use tonic::codegen::http::{Request, Response};
use tonic::codegen::{BoxFuture, Context, Poll, Service};
use tonic::server::NamedService;

/// The calls that write; every other one reads.
const WRITES: [&str; 5] = ["Write", "BatchUpdateBlobs", "SpliceBlob", "UpdateActionResult", "RegisterChunkMapping"];

/// A gRPC service answered only while `meta` serves.
#[derive(Clone)]
pub(crate) struct OnlyOnTheLeader<S> {
    inner: S,
    meta: Arc<MetaStore>,
}

impl<S> OnlyOnTheLeader<S> {
    pub(crate) fn new(inner: S, meta: Arc<MetaStore>) -> Self {
        Self { inner, meta }
    }
}

impl<S: NamedService> NamedService for OnlyOnTheLeader<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> Service<Request<B>> for OnlyOnTheLeader<S>
where
    S: Service<Request<B>, Response = Response<BoxBody>, Error = Infallible>,
    S::Future: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let method = req.uri().path().rsplit('/').next().unwrap_or_default();
        if let Err(e) = self.meta.serves(WRITES.contains(&method)) {
            let why = format!("{e:#}");
            tracing::info!(method, leader = self.meta.leader(), why, "not serving");
            let refused = tonic::Status::unavailable(why).into_http();
            return Box::pin(async move { Ok(refused) });
        }
        Box::pin(self.inner.call(req))
    }
}
