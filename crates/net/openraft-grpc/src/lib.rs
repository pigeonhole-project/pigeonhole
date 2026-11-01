//! [openraft](https://docs.rs/openraft)'s networks besides the chat: gRPC
//! between nodes that reach each other, the same in memory, and a network
//! that mixes two of them by peer.
//!
//! - [`GrpcNetwork`] (feature `transport`, on by default): one unary RPC per
//!   Raft call; the request and its response are each one frame of
//!   openraft-chat (postcard, sealed with the cluster's key) holding one
//!   envelope, so both transports speak the same wire and check the same key.
//! - [`Direct`]: what gRPC is, in memory (tests, the simulation, the
//!   in-browser demo, where no socket opens), with delay and cut-off nodes.
//! - [`Mixed`]: openraft asks a node's network for a client per peer; this
//!   one hands out a client of one network for some peers and of the other
//!   for the rest (gRPC to most, the chat to a node that only has channels).
//!
//! Design: pigeonhole's docs/raft-over-chat.md § Transports.

mod direct;
#[cfg(feature = "transport")]
mod grpc;
mod mixed;

pub use direct::{Direct, DirectClient, DirectNetwork};
#[cfg(all(feature = "transport", not(madsim)))]
pub use grpc::serve;
#[cfg(feature = "transport")]
pub use grpc::{GrpcClient, GrpcNetwork, Identity, MAX_MESSAGE};
pub use mixed::{Either, Mixed};
