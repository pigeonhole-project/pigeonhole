//! What a pigeonhole store implements: [`BlobBackend`] (immutable blobs under
//! keys the store chooses, listed in order for the sweeper), [`Chain`]
//! (blobs linked first-successor-wins: what a log needs), [`LeaseChain`] (a
//! chain that holds a log by a lease), and the failures a
//! store reports by type ([`Failure`]).
//! Crates.io only, so that both the blob layer and the metadata tablet can
//! build on it (docs/chain-log.md § Layering).

mod backend;
mod chain;
#[cfg(any(test, feature = "conformance"))]
pub mod conformance;
mod error;
#[cfg(feature = "fake")]
pub mod fake;
pub mod inspect;
mod stream;
pub mod text;

pub use backend::{
    exists_by_get, BlobBackend, BlobId, CostHint, InstanceInfo, InstanceRole, OpKind, OrderedKey, Owner,
};
pub use chain::{
    newest_loose_at_tail, put_after_tail_by_links, put_loose_linked, After, Chain, LeaseChain, Linked, MakeAfter,
    PUT_AFTER_TAIL_ATTEMPTS,
};
pub use error::{Failure, NotFound, RateLimited, Unavailable};
pub use pigeonhole_types::{BackendLimits, ByteRange, RangeSupport};
pub use stream::{bytes_stream, collect_stream, slice_range, BoxByteStream};
