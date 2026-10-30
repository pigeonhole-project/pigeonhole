//! Telegram over MTProto, logged in as a bot (`auth.importBotAuthorization`):
//! a [`BlobBackend`] over one channel, and the channel operations the
//! metadata log and snapshots use.
//!
//! Unlike the Bot API, a bot here may read any message of a channel by id
//! (`channels.getMessages`), so a channel can be listed (ids are dense) and a
//! blob is just its message id; files are not capped at 20 MiB and are read
//! in exact windows. Only channels and supergroups: their message ids are
//! per chat.

mod chain;
mod channel;
mod client;
pub mod limits;
pub mod fake;
mod session;
mod store;

pub use chain::ChainOptions;
pub use limits::limits;
pub use channel::{windows, ChannelApi, ChannelMessage, FileLocation, FileReferenceExpired, Sent, IDS_PER_CALL, TEXT_MAX_CHARS, WINDOW};
pub use client::{bot_id_of, channel_id_of, Lane, MtprotoChannel, MtprotoConfig, TelegramMtproto, UpdatesRx, CALL_DEADLINE};
pub use pigeonhole_storage_api::BlobBackend;
pub use session::FileSession;
pub use store::{blob_random_id, TelegramBlobStore, TelegramId};

/// What [`InstanceInfo::kind`](pigeonhole_storage_api::InstanceInfo::kind) says of this store.
pub const KIND: &str = "telegram";
