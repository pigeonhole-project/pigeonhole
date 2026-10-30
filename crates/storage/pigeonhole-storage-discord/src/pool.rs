//! One channel through several bots (docs/shared-limiter.md, § Choosing the
//! account: a pool). Every API call goes to the bot whose budget for it is
//! ready soonest, and moves to another when Discord answers 429 (the call
//! was not applied). A send keeps its nonce when it moves: the other bot
//! never saw it (nonces are per bot), and the first did not apply it.
//!
//! Message ids and CDN URLs are the channel's, not a bot's, so any bot reads
//! what another posted. CDN downloads are not API calls: the primary (the
//! first bot) makes them.

use crate::api::{DcMessage, DiscordApi, ThreadStart};
use crate::client::DiscordChannel;
use crate::limits::{DELETE, READ, SEND};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::{ChatLimiter, Pool, Spend};
use pigeonhole_storage_api::RateLimited;
use std::sync::Arc;
use std::time::Duration;

/// How long an operation keeps moving and waiting before it gives up.
const DEADLINE: Duration = Duration::from_secs(3600);

/// A channel through several bots; the first is the primary.
pub struct PooledDiscord<C = DiscordChannel> {
    members: Vec<C>,
    pool: Pool,
}

impl PooledDiscord<DiscordChannel> {
    /// `members`: the same channel through each bot, the primary first.
    pub fn of(members: Vec<DiscordChannel>) -> Self {
        let chat = members.first().map(|m| m.channel_id().to_string()).unwrap_or_default();
        let members = members.into_iter().map(|m| (m.in_pool(), m.limiter().clone())).collect();
        Self::new(&chat, members)
    }
}

impl<C: DiscordApi> PooledDiscord<C> {
    /// `members` with their limiters, the primary first. A member must give
    /// way on a 429 (end the call rate limited instead of waiting it out):
    /// [`DiscordChannel::in_pool`].
    pub fn new(chat: &str, members: Vec<(C, Arc<ChatLimiter>)>) -> Self {
        let (members, limiters) = members.into_iter().unzip();
        Self { members, pool: Pool::new(chat, limiters) }
    }

    /// The primary's limiter.
    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        self.pool.member(0)
    }
}

/// The wait of an error that was a 429 given way.
fn rate_limited(e: &anyhow::Error) -> Option<Duration> {
    e.downcast_ref::<RateLimited>().map(|r| r.retry_after)
}

#[async_trait]
impl<C: DiscordApi> DiscordApi for PooledDiscord<C> {
    fn channel_id(&self) -> u64 {
        self.members[0].channel_id()
    }

    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
        self.pool
            .run(Spend::Class(SEND), DEADLINE, rate_limited, |m| {
                let file = file.clone();
                self.members[m].send(content, file, nonce)
            })
            .await
    }

    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
        self.pool.run(Spend::Class(READ), DEADLINE, rate_limited, |m| self.members[m].get_message(id)).await
    }

    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
        self.pool.run(Spend::Class(READ), DEADLINE, rate_limited, |m| self.members[m].list(before, after, limit)).await
    }

    async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
        self.pool.run(Spend::Class(DELETE), DEADLINE, rate_limited, |m| self.members[m].delete_messages(ids)).await
    }

    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
        self.pool
            .run(Spend::Class(SEND), DEADLINE, rate_limited, |m| self.members[m].start_thread(message_id, name))
            .await
    }

    async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
        self.pool.run(Spend::Class(READ), DEADLINE, rate_limited, |m| self.members[m].thread_name(message_id)).await
    }

    async fn archive_thread(&self, message_id: u64) -> Result<()> {
        self.pool
            .run(Spend::Class(DELETE), DEADLINE, rate_limited, |m| self.members[m].archive_thread(message_id))
            .await
    }

    async fn delete_thread(&self, message_id: u64) -> Result<()> {
        self.pool
            .run(Spend::Class(DELETE), DEADLINE, rate_limited, |m| self.members[m].delete_thread(message_id))
            .await
    }

    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        self.members[0].download(url, range).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeDiscord;
    use pigeonhole_limits::{Level, LimiterRegistry, Rate};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// One bot's view of a shared channel: its first `floods` sends answer
    /// a 429 of 40 s, as a member in a pool does (cooled, given way).
    struct Bot {
        channel: Arc<FakeDiscord>,
        limiter: Arc<ChatLimiter>,
        floods: AtomicU32,
        sends: Arc<AtomicU32>,
    }

    #[async_trait]
    impl DiscordApi for Bot {
        fn channel_id(&self) -> u64 {
            self.channel.channel_id()
        }
        async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
            self.limiter.acquire(Spend::Class(SEND)).await;
            if self.floods.load(Ordering::SeqCst) > 0 {
                self.floods.fetch_sub(1, Ordering::SeqCst);
                let after = Duration::from_secs(40);
                self.limiter.cool(Spend::Class(SEND), Level::Class, after);
                return Err(RateLimited { method: "send message", retry_after: after }.into());
            }
            self.sends.fetch_add(1, Ordering::SeqCst);
            self.channel.send(content, file, nonce).await
        }
        async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
            self.channel.get_message(id).await
        }
        async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
            self.channel.list(before, after, limit).await
        }
        async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
            self.channel.delete_messages(ids).await
        }
        async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
            self.channel.start_thread(message_id, name).await
        }
        async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
            self.channel.thread_name(message_id).await
        }
        async fn archive_thread(&self, message_id: u64) -> Result<()> {
            self.channel.archive_thread(message_id).await
        }
        async fn delete_thread(&self, message_id: u64) -> Result<()> {
            self.channel.delete_thread(message_id).await
        }
        async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
            self.channel.download(url, range).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_429_moves_the_send_to_the_other_bot_and_reads_see_it() {
        let reg = LimiterRegistry::new(crate::limits().with(SEND, Rate::new(100.0, 10.0)));
        let channel = Arc::new(FakeDiscord::new(7));
        let (a_sends, b_sends) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
        let bot = |name: &str, floods: u32, sends: &Arc<AtomicU32>| {
            let limiter = reg.chat(name, "7");
            (Bot { channel: channel.clone(), limiter: limiter.clone(), floods: AtomicU32::new(floods), sends: sends.clone() }, limiter)
        };
        let pooled = PooledDiscord::new("7", vec![bot("a", 1, &a_sends), bot("b", 0, &b_sends)]);
        let t0 = tokio::time::Instant::now();
        let m = pooled.send("entry", None, "n-1").await.unwrap();
        assert!(t0.elapsed() < Duration::from_secs(1), "the 40 s 429 was not waited out");
        assert_eq!((a_sends.load(Ordering::SeqCst), b_sends.load(Ordering::SeqCst)), (0, 1));
        assert_eq!(pooled.get_message(m.id).await.unwrap().map(|m| m.content), Some("entry".into()));
        assert!(pooled.limiter().peek_wait(SEND) > Duration::from_secs(30), "the primary stays cooled");
    }
}
