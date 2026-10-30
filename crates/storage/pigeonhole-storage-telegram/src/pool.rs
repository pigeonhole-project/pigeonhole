//! One channel through several bots (docs/shared-limiter.md, § Choosing the
//! account: a pool). Sends and deletes go to the bot whose budget is ready
//! soonest, and move to another when Telegram answers a flood wait (the
//! call was not applied). A send keeps its `random_id` when it moves: the
//! other bot never saw it, and the first did not apply it.
//!
//! Reads stay on the primary (the first bot). A document's location
//! (`access_hash`, file reference) is the bot's that read it: another bot
//! downloading with it gets `FILE_REFERENCE_EXPIRED` (measured). So a send
//! by another bot comes back without its location, and the store reads it
//! through the primary when it needs it.

use crate::channel::{ChannelApi, ChannelMessage, FileLocation, Sent};
use crate::client::CALL_DEADLINE;
use crate::limits::{DELETE, SEND};
use crate::MtprotoChannel;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::{ChatLimiter, Pool, Spend};
use pigeonhole_storage_api::RateLimited;
use std::sync::Arc;
use std::time::Duration;

/// A channel through several bots; the first is the primary.
pub struct PooledChannel<C = MtprotoChannel> {
    members: Vec<C>,
    pool: Pool,
}

impl PooledChannel<MtprotoChannel> {
    /// `members`: the same channel through each bot, the primary first.
    pub fn of(members: Vec<MtprotoChannel>) -> Self {
        let chat = members.first().map(|m| m.channel_id().to_string()).unwrap_or_default();
        let members = members.into_iter().map(|m| (m.in_pool(), m.limiter().clone())).collect();
        Self::new(&chat, members)
    }
}

impl<C: ChannelApi> PooledChannel<C> {
    /// `members` with their limiters, the primary first. A member must give
    /// way on a flood wait (end the call rate limited instead of waiting it
    /// out): [`MtprotoChannel::in_pool`].
    pub fn new(chat: &str, members: Vec<(C, Arc<ChatLimiter>)>) -> Self {
        let (members, limiters) = members.into_iter().unzip();
        Self { members, pool: Pool::new(chat, limiters) }
    }

    /// The primary's limiter.
    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        self.pool.member(0)
    }

    fn primary(&self) -> &C {
        &self.members[0]
    }
}

/// The wait of an error that was a flood wait given way.
fn rate_limited(e: &anyhow::Error) -> Option<Duration> {
    e.downcast_ref::<RateLimited>().map(|r| r.retry_after)
}

/// A send's answer as the pool hands it on: another bot's location is no
/// use to the primary.
fn sent_by(member: usize, sent: Sent) -> Sent {
    if member == 0 {
        sent
    } else {
        Sent { file: None, ..sent }
    }
}

#[async_trait]
impl<C: ChannelApi> ChannelApi for PooledChannel<C> {
    fn channel_id(&self) -> i64 {
        self.primary().channel_id()
    }

    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        self.primary().get_messages(ids).await
    }

    async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
        self.pool
            .run(Spend::Class(SEND), CALL_DEADLINE, rate_limited, |m| {
                let data = data.clone();
                async move { Ok(sent_by(m, self.members[m].send_document(name, data, random_id).await?)) }
            })
            .await
    }

    async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
        self.pool
            .run(Spend::Class(SEND), CALL_DEADLINE, rate_limited, |m| async move {
                Ok(sent_by(m, self.members[m].send_text(text, random_id).await?))
            })
            .await
    }

    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
        self.primary().get_file(file, offset, limit).await
    }

    async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
        self.pool
            .run(Spend::Class(DELETE), CALL_DEADLINE, rate_limited, |m| self.members[m].delete_messages(ids))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeChannel;
    use pigeonhole_limits::{Level, LimiterRegistry, Rate};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// One bot's view of a shared channel: its first `floods` sends answer
    /// a flood wait of `secs`, as a member in a pool does (cooled, given way).
    struct Bot {
        channel: Arc<FakeChannel>,
        limiter: Arc<ChatLimiter>,
        floods: AtomicU32,
        secs: u64,
        sends: AtomicU32,
    }

    #[async_trait]
    impl ChannelApi for Bot {
        fn channel_id(&self) -> i64 {
            self.channel.channel_id()
        }
        async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
            self.channel.get_messages(ids).await
        }
        async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
            self.channel.send_document(name, data, random_id).await
        }
        async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
            self.limiter.acquire(Spend::Class(SEND)).await;
            if self.floods.load(Ordering::SeqCst) > 0 {
                self.floods.fetch_sub(1, Ordering::SeqCst);
                let after = Duration::from_secs(self.secs);
                self.limiter.cool(Spend::Class(SEND), Level::Class, after);
                return Err(RateLimited { method: "messages.sendMessage", retry_after: after }.into());
            }
            self.sends.fetch_add(1, Ordering::SeqCst);
            self.channel.send_text(text, random_id).await
        }
        async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
            self.channel.get_file(file, offset, limit).await
        }
        async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
            self.channel.delete_messages(ids).await
        }
    }

    fn pool_of(floods: [u32; 2]) -> (PooledChannel<Arc<Bot>>, [Arc<Bot>; 2]) {
        let reg = LimiterRegistry::new(crate::limits().with(SEND, Rate::new(100.0, 10.0)));
        let channel = Arc::new(FakeChannel::new(7));
        let bot = |name: &str, floods: u32| {
            Arc::new(Bot { channel: channel.clone(), limiter: reg.chat(name, "7"), floods: AtomicU32::new(floods), secs: 40, sends: AtomicU32::new(0) })
        };
        let bots = [bot("a", floods[0]), bot("b", floods[1])];
        let pooled = PooledChannel::new("7", bots.iter().map(|b| (b.clone(), b.limiter.clone())).collect());
        (pooled, bots)
    }

    #[tokio::test(start_paused = true)]
    async fn a_flood_wait_moves_the_send_to_the_other_bot_with_its_random_id() {
        let (pooled, [a, b]) = pool_of([1, 0]);
        let t0 = tokio::time::Instant::now();
        let sent = pooled.send_text("entry", 42).await.unwrap();
        assert!(t0.elapsed() < Duration::from_secs(1), "the 40 s flood was not waited out");
        assert_eq!((a.sends.load(Ordering::SeqCst), b.sends.load(Ordering::SeqCst)), (0, 1));
        assert!(sent.file.is_none(), "another bot's location is not handed on");
        // The cooled bot is skipped while it cools.
        pooled.send_text("next", 43).await.unwrap();
        assert_eq!(b.sends.load(Ordering::SeqCst), 2);
        assert!(a.limiter.peek_wait(SEND) > Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn with_both_bots_flooded_the_send_waits_and_still_lands_once() {
        let (pooled, [a, b]) = pool_of([1, 1]);
        let t0 = tokio::time::Instant::now();
        pooled.send_text("entry", 42).await.unwrap();
        assert!(t0.elapsed() >= Duration::from_secs(40), "both bots were cooled: {:?}", t0.elapsed());
        assert_eq!(a.sends.load(Ordering::SeqCst) + b.sends.load(Ordering::SeqCst), 1);
    }
}
