//! Telegram over MTProto, logged in as a bot (`auth.importBotAuthorization`):
//! the [`ChannelApi`](pigeonhole_storage_telegram::ChannelApi) the Telegram
//! store runs on, over grammers. One login per process ([`TelegramMtproto`]),
//! a channel over it ([`MtprotoChannel`]), several bots on one channel
//! ([`PooledChannel`]) and the session file ([`FileSession`]).
//!
//! Unlike the Bot API, a bot here may read any message of a channel by id
//! (`channels.getMessages`), so a channel can be listed (ids are dense) and a
//! blob is just its message id; files are not capped at 20 MiB and are read
//! in exact windows. Only channels and supergroups: their message ids are
//! per chat.

mod client;
mod pool;
mod session;

pub use client::{
    bot_id_of, channel_id_of, Lane, MtprotoChannel, MtprotoConfig, TelegramMtproto, UpdatesRx, CALL_DEADLINE,
};
pub use pool::{pooled, PooledChannel};
pub use session::FileSession;
