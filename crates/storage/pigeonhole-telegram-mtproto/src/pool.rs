//! One channel through several bots over MTProto: the store crate's
//! [`pool::PooledChannel`](pigeonhole_storage_telegram::pool::PooledChannel)
//! of [`MtprotoChannel`]s.

use crate::client::CALL_DEADLINE;
use crate::MtprotoChannel;
use pigeonhole_limits::chat::Pooled;
use pigeonhole_storage_telegram::ChannelApi;

/// A channel through several bots; the first is the primary.
pub type PooledChannel = pigeonhole_storage_telegram::pool::PooledChannel<MtprotoChannel>;

/// `members`: the same channel through each bot, the primary first.
pub fn pooled(members: Vec<MtprotoChannel>) -> PooledChannel {
    let chat = members.first().map(|m| m.channel_id().to_string()).unwrap_or_default();
    PooledChannel::new(&chat, Pooled::<MtprotoChannel>::members(members)).with_deadline(CALL_DEADLINE)
}
