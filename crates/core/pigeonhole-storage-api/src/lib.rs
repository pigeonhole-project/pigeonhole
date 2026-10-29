//! What a pigeonhole store implements: [`BlobBackend`] (immutable blobs under
//! keys the store chooses, listed in order for the sweeper), [`Chain`]
//! (blobs linked first-successor-wins: what a log needs), [`LeaseChain`] (a
//! chain that holds a log by a lease), and the failures a
//! store reports by type ([`Failure`]).
//! Crates.io only, so that both the blob layer and the metadata tablet can
//! build on it (docs/chain-log.md § Layering).

mod backend;
mod chain;
mod error;
mod stream;
pub mod text;

pub use backend::{
    BlobBackend, BlobId, CostHint, InstanceInfo, InstanceRole, OpKind, OrderedKey,
};
pub use error::{Failure, NotFound, RateLimited, Unavailable};
pub use chain::{After, Chain, LeaseChain, Linked, MakeAfter, PUT_AFTER_TAIL_ATTEMPTS};
pub use pigeonhole_types::{BackendLimits, ByteRange, RangeSupport};
pub use stream::{bytes_stream, collect_stream, slice_range, BoxByteStream};
