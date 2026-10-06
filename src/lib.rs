//! S3-compatible gateway backed by Telegram (`BlobStore`) and a SQLite index.

pub mod chunker;
pub mod config;
pub mod index;
pub mod ingest;
pub mod rate_limit;
pub mod service;
pub mod snapshot;
pub mod storage;
pub mod telegram;

use config::Config;
use index::Index;
use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use service::S3gram;
use std::sync::Arc;
use storage::BlobStore;
use tokio::sync::Mutex;

/// Build an [`S3gram`] backend from an existing index + blob store (tests / custom wiring).
pub fn build_s3gram(cfg: Config, index: Index, store: Arc<dyn BlobStore>) -> S3gram {
    S3gram {
        cfg,
        index,
        store,
        snapshot_gate: Arc::new(Mutex::new(())),
    }
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
