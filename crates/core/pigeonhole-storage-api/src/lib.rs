//! What a pigeonhole store implements: [`BlobBackend`] (immutable blobs under
//! keys the store chooses), [`Sweepable`] (listing for the sweeper).
//! Crates.io only, so that both the blob layer and the metadata tablet can
//! build on it (docs/chain-log.md § Layering).

mod backend;
mod stream;

pub use backend::{
    BlobBackend, BlobId, CostHint, InstanceInfo, InstanceKind, InstanceRole, OpKind, OrderedKey,
    Sweepable,
};
pub use pigeonhole_types::{BackendLimits, ByteRange, RangeSupport};
pub use stream::{bytes_stream, collect_stream, slice_range, BoxByteStream};
