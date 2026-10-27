//! s3s [`S3`](s3s::S3) backend over the pigeonhole chunk store.

mod commands;
pub mod index;
mod service;

pub use index::{
    parse_rfc3339, slice_extents, unique_chunk_ids, DeleteBucketResult, Index, ObjectMeta,
};
pub use pigeonhole_chunk_store::{ChunkStore, Config, Extent};
pub use commands::S3State;
pub use service::S3Gateway;

use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use std::sync::Arc;

/// Build an [`S3Gateway`] backend from an existing index + chunk store.
pub fn build_s3_gateway(cfg: Config, index: Index, store: Arc<ChunkStore>) -> S3Gateway {
    S3Gateway::new(cfg, index, store)
}

/// Wrap [`S3Gateway`] in an authenticated s3s HTTP service.
pub fn build_s3_service(gateway: S3Gateway, access_key: &str, secret_key: &str) -> S3Service {
    let mut builder = S3ServiceBuilder::new(gateway);
    builder.set_auth(SimpleAuth::from_single(
        access_key.to_string(),
        secret_key.to_string(),
    ));
    builder.build()
}
