//! s3s [`S3`](s3s::S3) backend over the s3gram engine.

mod service;

pub use service::S3gram;
pub use s3gram_blob::{BlobStore, DeleteOutcome, MemoryBlobStore};
pub use s3gram_engine::Config;
pub use s3gram_index::Index;

use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use std::sync::Arc;

/// Build an [`S3gram`] backend from an existing index + blob store (tests / custom wiring).
pub fn build_s3gram(cfg: Config, index: Index, store: Arc<dyn BlobStore>) -> S3gram {
    S3gram::new(cfg, index, store)
}

/// Wrap [`S3gram`] in an authenticated s3s HTTP service.
pub fn build_s3_service(s3gram: S3gram, access_key: &str, secret_key: &str) -> S3Service {
    let mut builder = S3ServiceBuilder::new(s3gram);
    builder.set_auth(SimpleAuth::from_single(
        access_key.to_string(),
        secret_key.to_string(),
    ));
    builder.build()
}
