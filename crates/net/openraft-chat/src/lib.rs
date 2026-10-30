//! [openraft](https://docs.rs/openraft)'s network over chat channels: Raft
//! nodes talk by posting into channels and reading them, with no open port.
//!
//! Every node owns an outbox ([`Mailbox`]): it posts there everything it
//! sends, one [`frame`] per tick holding all its envelopes (requests to any
//! peer, responses to requests it got), and reads every peer's outbox for
//! what is addressed to it. Frames are sealed with an HMAC of the cluster's
//! key. Raft tolerates the lossy, slow wire: a lost frame is a timeout,
//! a duplicate is dropped by its sequence number.
//!
//! The chat itself is behind [`Mailbox`]; [`FakeChat`] is an in-memory one
//! for tests. Design: pigeonhole's docs/raft-over-chat.md.

pub mod frame;
mod mailbox;
mod network;
pub mod telemetry;

pub use frame::Key;
pub use mailbox::{FakeChat, Mailbox};
pub use network::{CallError, ChatClient, ChatNetwork, Handler, Settings};
