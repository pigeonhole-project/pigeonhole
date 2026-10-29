//! s3s [`S3`](s3s::S3) backend over the pigeonhole chunk store.

mod checksums;
mod commands;
pub mod index;
mod service;

pub use index::{
    parse_rfc3339, slice_extents, unique_chunk_ids, DeleteBucketResult, Index, ObjectMeta,
};
pub use pigeonhole_chunk_store::{ChunkStore, Extent};
pub use commands::{Pairs, Released, S3Command, S3Reject, S3State};
pub use service::{S3Gateway, S3Options};

use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::{s3_error, S3Result};
use std::sync::{Arc, OnceLock};

/// Build an [`S3Gateway`] backend from an existing index + chunk store.
pub fn build_s3_gateway(opts: S3Options, index: Index, store: Arc<ChunkStore>) -> S3Gateway {
    S3Gateway::new(opts, index, store)
}

/// Wrap [`S3Gateway`] in an authenticated s3s HTTP service.
pub fn build_s3_service(gateway: S3Gateway, access_key: &str, secret_key: &str) -> S3Service {
    let mut builder = S3ServiceBuilder::new(gateway);
    builder.set_auth(SimpleAuth::from_single(
        access_key.to_string(),
        secret_key.to_string(),
    ));
    builder.set_access(NameOp);
    builder.build()
}

/// Put one into a request's extensions to learn the S3 operation s3s routed
/// it to (`GetObject`, `PutObject`, …); empty if routing or auth failed first.
#[derive(Clone, Default)]
pub struct S3OpSlot(pub Arc<OnceLock<String>>);

/// s3s's default access check (a signature is required), also filling the
/// request's [`S3OpSlot`].
struct NameOp;

#[async_trait::async_trait]
impl S3Access for NameOp {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let op = cx.s3_op().name().to_owned();
        if let Some(slot) = cx.extensions_mut().get::<S3OpSlot>() {
            let _ = slot.0.set(op);
        }
        match cx.credentials() {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
    }
}
