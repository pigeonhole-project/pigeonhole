//! s3s [`S3`](s3s::S3) backend over the pigeonhole chunk store.

pub mod index;
mod service;
pub mod snapshot;

pub use index::{
    parse_rfc3339, slice_extents, unique_chunk_ids, DeleteBucketResult, Index, IndexSnapshot,
    ObjectMeta,
};
pub use pigeonhole_chunk_store::{ChunkStore, Config, Extent};
pub use service::S3gram;
pub use snapshot::{push_index_snapshot, restore_index_snapshot, PushOutcome, ROOT_NAME};

use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use std::sync::Arc;

/// Build an [`S3gram`] backend from an existing index + chunk store.
pub fn build_s3gram(cfg: Config, index: Index, store: Arc<ChunkStore>) -> S3gram {
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
