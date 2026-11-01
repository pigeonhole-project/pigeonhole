//! One channel through several bots over HTTP: the store crate's
//! [`pool::PooledDiscord`](pigeonhole_storage_discord::pool::PooledDiscord)
//! of [`DiscordChannel`]s.

use crate::client::DiscordChannel;
use pigeonhole_storage_discord::DiscordApi;

/// A channel through several bots; the first is the primary.
pub type PooledDiscord = pigeonhole_storage_discord::pool::PooledDiscord<DiscordChannel>;

/// `members`: the same channel through each bot, the primary first.
pub fn pooled(members: Vec<DiscordChannel>) -> PooledDiscord {
    let chat = members.first().map(|m| m.channel_id().to_string()).unwrap_or_default();
    // Alone, a bot waits its rate limits out: there is no one to move to.
    let alone = members.len() == 1;
    let members =
        members.into_iter().map(|m| (if alone { m.clone() } else { m.in_pool() }, m.limiter().clone())).collect();
    PooledDiscord::new(&chat, members)
}
