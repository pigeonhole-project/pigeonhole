//! One channel through several bots (docs/shared-limiter.md, § Choosing the
//! account: a pool). Sends and deletes go to the bot whose budget is ready
//! soonest, and move to another when Telegram answers a flood wait (the
//! call was not applied). A send keeps its `random_id` when it moves: the
//! other bot never saw it, and the first did not apply it.
//!
//! Reads spread too, with one catch: a document's location (`access_hash`,
//! file reference) is the bot's that read or posted it, and another bot
//! downloading with it gets `FILE_REFERENCE_EXPIRED` (measured). So every
//! location carries its bot ([`FileLocation::bot`]), a download goes to that
//! bot, and the store keeps each bot's location of a document (replicas).
//! [`ChannelApi::read_via`] picks the holder ready soonest, or has a freer
//! bot read the message and get its own.

use crate::channel::{ChannelApi, ChannelMessage, FileLocation, ReadVia, Sent};
use crate::client::CALL_DEADLINE;
use crate::limits::{DELETE, READ, SEND};
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
}

/// The wait of an error that was a flood wait given way.
fn rate_limited(e: &anyhow::Error) -> Option<Duration> {
    e.downcast_ref::<RateLimited>().map(|r| r.retry_after)
}

/// A location `member` got: its own.
fn held_by(member: usize, file: FileLocation) -> FileLocation {
    FileLocation { bot: member, ..file }
}

/// Messages `member` read, their locations its own.
fn read_by(member: usize, msgs: Vec<ChannelMessage>) -> Vec<ChannelMessage> {
    msgs.into_iter()
        .map(|m| match m {
            ChannelMessage::Document { name, size, file } => {
                ChannelMessage::Document { name, size, file: held_by(member, file) }
            }
            other => other,
        })
        .collect()
}

/// A send's answer from `member`, its location its own.
fn sent_by(member: usize, sent: Sent) -> Sent {
    Sent { file: sent.file.map(|f| held_by(member, f)), ..sent }
}

#[async_trait]
impl<C: ChannelApi> ChannelApi for PooledChannel<C> {
    fn channel_id(&self) -> i64 {
        self.members[0].channel_id()
    }

    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        self.pool
            .run(Spend::Class(READ), CALL_DEADLINE, rate_limited, |m| async move {
                Ok(read_by(m, self.members[m].get_messages(ids).await?))
            })
            .await
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

    /// Through the bot whose location `file` is: no other can use it.
    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
        self.pool.run_on(file.bot, CALL_DEADLINE, rate_limited, |m| self.members[m].get_file(file, offset, limit)).await
    }

    async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
        self.pool.run(Spend::Class(DELETE), CALL_DEADLINE, rate_limited, |m| self.members[m].delete_messages(ids)).await
    }

    /// The holder whose download budget (its bot's) is ready soonest, unless
    /// a bot without a location could read the message (`read`, and its bot)
    /// sooner still: then [`ReadVia::Locate`], and it gets one.
    fn read_via(&self, holders: &[usize]) -> ReadVia {
        let holder = holders.iter().map(|&h| (self.pool.member(h).peek(Spend::Bot), h)).min();
        let other = (0..self.pool.len())
            .filter(|m| !holders.contains(m))
            .map(|m| self.pool.member(m).peek(Spend::Class(READ)))
            .min();
        match (holder, other) {
            (Some((wait, _)), Some(sooner)) if sooner < wait => ReadVia::Locate,
            (Some((_, h)), _) => ReadVia::Holder(h),
            (None, _) => ReadVia::Locate,
        }
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
        index: usize,
        channel: Arc<FakeChannel>,
        limiter: Arc<ChatLimiter>,
        floods: AtomicU32,
        secs: u64,
        sends: AtomicU32,
        reads: AtomicU32,
    }

    impl Bot {
        /// The first `floods` sends answer a flood wait.
        async fn send_or_flood(&self) -> Result<()> {
            self.limiter.acquire(Spend::Class(SEND)).await;
            if self.floods.load(Ordering::SeqCst) > 0 {
                self.floods.fetch_sub(1, Ordering::SeqCst);
                let after = Duration::from_secs(self.secs);
                self.limiter.cool(Spend::Class(SEND), Level::Class, after);
                return Err(RateLimited { method: "messages.send", retry_after: after }.into());
            }
            self.sends.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl ChannelApi for Bot {
        fn channel_id(&self) -> i64 {
            self.channel.channel_id()
        }
        async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
            self.limiter.acquire(Spend::Class(READ)).await;
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.channel.get_messages(ids).await
        }
        async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
            self.send_or_flood().await?;
            self.channel.send_document(name, data, random_id).await
        }
        async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
            self.send_or_flood().await?;
            self.channel.send_text(text, random_id).await
        }
        /// Only with its own location, as Telegram (another bot's:
        /// `FILE_REFERENCE_EXPIRED`).
        async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
            if file.bot != self.index {
                return Err(crate::FileReferenceExpired.into());
            }
            self.limiter.acquire(Spend::Bot).await;
            self.channel.get_file(file, offset, limit).await
        }
        async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
            self.channel.delete_messages(ids).await
        }
    }

    fn pool_of(floods: [u32; 2]) -> (PooledChannel<Arc<Bot>>, [Arc<Bot>; 2]) {
        let reg = LimiterRegistry::new(crate::limits().with(SEND, Rate::new(100.0, 10.0)));
        let channel = Arc::new(FakeChannel::new(7));
        let bot = |index: usize, floods: u32| {
            Arc::new(Bot {
                index,
                channel: channel.clone(),
                limiter: reg.chat(&format!("bot-{index}"), "7"),
                floods: AtomicU32::new(floods),
                secs: 40,
                sends: AtomicU32::new(0),
                reads: AtomicU32::new(0),
            })
        };
        let bots = [bot(0, floods[0]), bot(1, floods[1])];
        let pooled = PooledChannel::new("7", bots.iter().map(|b| (b.clone(), b.limiter.clone())).collect());
        (pooled, bots)
    }

    #[tokio::test(start_paused = true)]
    async fn a_flood_wait_moves_the_send_to_the_other_bot_with_its_random_id() {
        let (pooled, [a, b]) = pool_of([1, 0]);
        let t0 = tokio::time::Instant::now();
        pooled.send_text("entry", 42).await.unwrap();
        assert!(t0.elapsed() < Duration::from_secs(1), "the 40 s flood was not waited out");
        assert_eq!((a.sends.load(Ordering::SeqCst), b.sends.load(Ordering::SeqCst)), (0, 1));
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

    fn store_on(pooled: PooledChannel<Arc<Bot>>) -> crate::TelegramBlobStore<PooledChannel<Arc<Bot>>> {
        let info = pigeonhole_storage_api::InstanceInfo {
            id: "tg".into(),
            kind: crate::KIND,
            fingerprint: "tg:1:7".into(),
            location: "tg:chat:7".into(),
            role: pigeonhole_storage_api::InstanceRole::ReadWrite,
        };
        let limiter = pooled.limiter().clone();
        crate::TelegramBlobStore::new(Arc::new(pooled), limiter, info).with_inline(false)
    }

    async fn get(store: &crate::TelegramBlobStore<PooledChannel<Arc<Bot>>>, id: &crate::TelegramId) -> Bytes {
        use pigeonhole_storage_api::BlobBackend;
        pigeonhole_storage_api::collect_stream(store.get(id, None).await.unwrap()).await.unwrap()
    }

    fn holders(store: &crate::TelegramBlobStore<PooledChannel<Arc<Bot>>>, id: i64) -> Vec<usize> {
        match store.reader.locations.lock().unwrap().peek(&id) {
            Some(crate::store::Located::Doc { files, .. }) => {
                let mut bots: Vec<usize> = files.iter().map(|f| f.bot).collect();
                bots.sort();
                bots
            }
            _ => Vec::new(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_document_another_bot_posted_is_read_with_its_location_without_a_lookup() {
        use pigeonhole_storage_api::BlobBackend;
        let (pooled, [a, b]) = pool_of([1, 0]);
        let store = store_on(pooled);
        let data = Bytes::from(vec![7u8; 100_000]);
        let id = store.put(data.clone(), &pigeonhole_storage_api::BlobId::random().name()).await.unwrap();
        assert_eq!(b.sends.load(Ordering::SeqCst), 1, "the flood moved the put to b");
        assert_eq!(holders(&store, id.message_id), vec![1], "b's location, from its send");
        assert_eq!(get(&store, &id).await, data);
        assert_eq!(
            (a.reads.load(Ordering::SeqCst), b.reads.load(Ordering::SeqCst)),
            (0, 0),
            "read after write without getMessages"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_holder_has_a_free_bot_read_the_message_and_both_replicas_stay() {
        use pigeonhole_storage_api::BlobBackend;
        let (pooled, [a, b]) = pool_of([0, 0]);
        let store = store_on(pooled);
        let data = Bytes::from(vec![9u8; 100_000]);
        let id = store.put(data.clone(), &pigeonhole_storage_api::BlobId::random().name()).await.unwrap();
        let first = if holders(&store, id.message_id) == vec![0] { &a } else { &b };
        let other = if std::ptr::eq(first, &a) { &b } else { &a };
        first.limiter.cool(Spend::Bot, Level::Bot, Duration::from_secs(100));
        let t0 = tokio::time::Instant::now();
        assert_eq!(get(&store, &id).await, data);
        assert!(t0.elapsed() < Duration::from_secs(1), "the free bot read it: {:?}", t0.elapsed());
        assert_eq!(other.reads.load(Ordering::SeqCst), 1, "after one getMessages of its own");
        assert_eq!(holders(&store, id.message_id), vec![0, 1], "both replicas kept");
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_location_is_dropped_and_read_again() {
        use pigeonhole_storage_api::BlobBackend;
        let (pooled, [a, _b]) = pool_of([0, 0]);
        let store = store_on(pooled);
        let data = Bytes::from(vec![3u8; 100_000]);
        let id = store.put(data.clone(), &pigeonhole_storage_api::BlobId::random().name()).await.unwrap();
        a.channel.expire_file_references();
        assert_eq!(get(&store, &id).await, data, "read again with a fresh reference");
        assert_eq!(holders(&store, id.message_id).len(), 1, "the stale replica was replaced");
    }
}
