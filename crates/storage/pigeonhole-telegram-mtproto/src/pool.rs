//! One channel through several bots over MTProto: the store crate's
//! [`pool::PooledChannel`](pigeonhole_storage_telegram::pool::PooledChannel)
//! of [`MtprotoChannel`]s.

use crate::client::CALL_DEADLINE;
use crate::MtprotoChannel;
use pigeonhole_storage_telegram::ChannelApi;

/// A channel through several bots; the first is the primary.
pub type PooledChannel = pigeonhole_storage_telegram::pool::PooledChannel<MtprotoChannel>;

/// `members`: the same channel through each bot, the primary first.
pub fn pooled(members: Vec<MtprotoChannel>) -> PooledChannel {
    let chat = members.first().map(|m| m.channel_id().to_string()).unwrap_or_default();
    // Alone, a bot waits its rate limits out: there is no one to move to.
    let alone = members.len() == 1;
    let members =
        members.into_iter().map(|m| (if alone { m.clone() } else { m.in_pool() }, m.limiter().clone())).collect();
    PooledChannel::new(&chat, members).with_deadline(CALL_DEADLINE)
}
