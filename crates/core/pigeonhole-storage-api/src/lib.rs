//! What a pigeonhole store implements: [`BlobBackend`] (immutable blobs under
//! keys the store chooses), [`Sweepable`] (listing for the sweeper), [`Chain`]
//! (blobs linked first-successor-wins: what a log needs).
//! Crates.io only, so that both the blob layer and the metadata tablet can
//! build on it (docs/chain-log.md § Layering).

mod backend;
mod chain;
mod stream;
pub mod text;

pub use backend::{
    BlobBackend, BlobId, CostHint, InstanceInfo, InstanceKind, InstanceRole, OpKind, OrderedKey,
    Sweepable,
};
pub use chain::{After, Chain, Linked, MakeAfter};
pub use pigeonhole_types::{BackendLimits, ByteRange, RangeSupport};
pub use stream::{bytes_stream, collect_stream, slice_range, BoxByteStream};
