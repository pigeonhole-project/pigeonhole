//! One channel through several bots (docs/shared-limiter.md, § Choosing the
//! account: a pool). Sends and deletes go to the bot whose budget is ready
//! soonest, and move to another when Telegram answers a flood wait (the
//! call was not applied). A send keeps its `random_id` when it moves: the
//! other bot never saw it, and the first did not apply it.
//!
//! Reads spread too, with one catch: a document's location (`access_hash`,
//! file reference) is the bot's that read or posted it, and another bot
//! downloading with it gets `FILE_REFERENCE_EXPIRED` (measured). So the pool
//! remembers which bot got which location of a document (its holders,
//! from the reads and sends that went through it): a download goes to the
//! holder whose budget is ready soonest, or, when a bot without a location
//! could read the message sooner still, that bot reads it and gets its own.
//! The store above sees one location per document, as from one bot.

use crate::limits::{DELETE, READ, SEND};
use crate::{ChannelApi, ChannelMessage, FileLocation, FileReferenceExpired, Sent};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use pigeonhole_limits::chat::Pooled;
use pigeonhole_limits::{ChatLimiter, Spend};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How long a call keeps moving and waiting before it gives up, unless
/// [`PooledChannel::with_deadline`] says otherwise.
const DEADLINE: Duration = Duration::from_secs(300);
/// Documents whose holders the pool remembers.
const HOLDERS: usize = 4096;

/// A channel through several bots; the first is the primary.
pub struct PooledChannel<C> {
    members: Pooled<C>,
    /// By document id: its message and each holder's location.
    held: Mutex<LruCache<i64, Held>>,
}

/// The locations of one document the bots hold.
#[derive(Clone)]
struct Held {
    message_id: i64,
    by: Vec<(usize, FileLocation)>,
}

/// How to download a document.
enum ReadVia {
    /// With this holder's location.
    Holder(usize, FileLocation),
    /// Have this bot read the message first: it has no location, and is
    /// free sooner than every holder.
    Locate(usize, i64),
    /// No holder known (forgotten): the caller reads the message again.
    Unknown,
}

impl<C: ChannelApi> PooledChannel<C> {
    /// `members` with their limiters, the primary first. A member must give
    /// way on a flood wait (end the call rate limited instead of waiting it
    /// out), unless it is alone: there is no one to move to.
    pub fn new(chat: &str, members: Vec<(C, Arc<ChatLimiter>)>) -> Self {
        Self {
            members: Pooled::new(chat, members, DEADLINE),
            held: Mutex::new(LruCache::new(NonZeroUsize::new(HOLDERS).unwrap())),
        }
    }

    /// How long a call keeps moving and waiting before it gives up.
    pub fn with_deadline(self, deadline: Duration) -> Self {
        Self { members: self.members.with_deadline(deadline), ..self }
    }

    /// The primary's limiter.
    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        self.members.limiter()
    }

    /// Bot `i`.
    pub fn member(&self, i: usize) -> &C {
        self.members.member(i)
    }

    /// The bots holding a location of message `message_id`'s document, sorted.
    pub fn holders(&self, message_id: i64) -> Vec<usize> {
        let held = self.held.lock().unwrap();
        let mut bots: Vec<usize> = held
            .iter()
            .filter(|(_, h)| h.message_id == message_id)
            .flat_map(|(_, h)| h.by.iter().map(|(bot, _)| *bot))
            .collect();
        bots.sort_unstable();
        bots
    }

    /// `bot` holds `file`, the document of message `message_id`.
    fn hold(&self, bot: usize, message_id: i64, file: &FileLocation) {
        let mut held = self.held.lock().unwrap();
        let h = held.get_or_insert_mut(file.id, || Held { message_id, by: Vec::new() });
        h.by.retain(|(b, _)| *b != bot);
        h.by.push((bot, file.clone()));
    }

    /// `bot`'s location of document `doc` no longer works.
    fn drop_holder(&self, doc: i64, bot: usize) {
        let mut held = self.held.lock().unwrap();
        if let Some(h) = held.get_mut(&doc) {
            h.by.retain(|(b, _)| *b != bot);
            if h.by.is_empty() {
                held.pop(&doc);
            }
        }
    }

    /// Messages `bot` read: their documents' locations are its own.
    fn read_by(&self, bot: usize, ids: &[i64], msgs: &[ChannelMessage]) {
        for (id, m) in ids.iter().zip(msgs) {
            if let ChannelMessage::Document { file, .. } = m {
                self.hold(bot, *id, file);
            }
        }
    }

    /// Telegram pushed message `message_id`, `message`, to bot `bot`: a
    /// document's location in it is that bot's. A push comes past the pool,
    /// so without this the pool knows no holder of its document and a
    /// download fails before it asks Telegram (2026-11-06, a Raft follower
    /// stuck on its leader's frame).
    pub fn pushed_to(&self, bot: usize, message_id: i64, message: &ChannelMessage) {
        self.read_by(bot, &[message_id], std::slice::from_ref(message));
    }

    /// A send's answer from `bot`: the location is its own.
    fn sent_by(&self, bot: usize, sent: &Sent) {
        if let Some(file) = &sent.file {
            self.hold(bot, sent.message_id, file);
        }
    }

    /// The holder of document `doc` whose download budget (its bot's) is
    /// ready soonest, unless a bot without a location could read the message
    /// (`read`, and its bot) sooner still.
    fn read_via(&self, doc: i64) -> ReadVia {
        let Some(held) = self.held.lock().unwrap().get(&doc).cloned() else { return ReadVia::Unknown };
        let pool = self.members.pool();
        let holder =
            held.by.iter().map(|(h, file)| (pool.member(*h).peek(Spend::Bot), *h, file)).min_by_key(|x| (x.0, x.1));
        let other = (0..pool.len())
            .filter(|m| held.by.iter().all(|(h, _)| h != m))
            .map(|m| (pool.member(m).peek(Spend::Class(READ)), m))
            .min();
        match (holder, other) {
            (Some((wait, _, _)), Some((sooner, m))) if sooner < wait => ReadVia::Locate(m, held.message_id),
            (Some((_, h, file)), _) => ReadVia::Holder(h, file.clone()),
            (None, Some((_, m))) => ReadVia::Locate(m, held.message_id),
            (None, None) => ReadVia::Unknown,
        }
    }

    /// `bot` reads message `message_id` for its own location of document `doc`.
    async fn locate_on(&self, bot: usize, message_id: i64, doc: i64) -> Result<FileLocation> {
        let ids = [message_id];
        let msgs = self.members.run_on(bot, |m| self.member(m).get_messages(&ids)).await?;
        self.read_by(bot, &[message_id], &msgs);
        match msgs.into_iter().next() {
            Some(ChannelMessage::Document { file, .. }) if file.id == doc => Ok(file),
            _ => Err(FileReferenceExpired.into()),
        }
    }
}

#[async_trait]
impl<C: ChannelApi> ChannelApi for PooledChannel<C> {
    fn channel_id(&self) -> i64 {
        self.members.primary().channel_id()
    }

    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        self.members
            .run(Spend::Class(READ), |m| async move {
                let msgs = self.member(m).get_messages(ids).await?;
                self.read_by(m, ids, &msgs);
                Ok(msgs)
            })
            .await
    }

    async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
        self.members
            .run(Spend::Class(SEND), |m| {
                let data = data.clone();
                async move {
                    let sent = self.member(m).send_document(name, data, random_id).await?;
                    self.sent_by(m, &sent);
                    Ok(sent)
                }
            })
            .await
    }

    async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
        self.members.run(Spend::Class(SEND), |m| self.member(m).send_text(text, random_id)).await
    }

    /// Through a bot holding a location of the document (only it can use
    /// it); a stale one is dropped and answers [`FileReferenceExpired`], so
    /// that the caller reads the message again.
    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
        let (bot, location) = match self.read_via(file.id) {
            ReadVia::Holder(bot, location) => (bot, location),
            ReadVia::Locate(bot, message_id) => (bot, self.locate_on(bot, message_id, file.id).await?),
            ReadVia::Unknown => return Err(FileReferenceExpired.into()),
        };
        let got = self.members.run_on(bot, |m| self.member(m).get_file(&location, offset, limit)).await;
        if got.as_ref().is_err_and(|e| e.is::<FileReferenceExpired>()) {
            self.drop_holder(file.id, bot);
        }
        got
    }

    async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
        self.members.run(Spend::Class(DELETE), |m| self.member(m).delete_messages(ids)).await
    }

    async fn pinned(&self) -> Result<Option<i64>> {
        self.members.run(Spend::Class(READ), |m| self.member(m).pinned()).await
    }

    async fn pin(&self, id: i64) -> Result<()> {
        self.members.run(Spend::Class(SEND), |m| self.member(m).pin(id)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::bot::FakeBot;
    use crate::fake::FakeChannel;
    use pigeonhole_limits::{Level, LimiterRegistry, Rate};
    use pigeonhole_storage_api::{BlobBackend, BlobId};

    type Bot = FakeBot<FakeChannel>;
    type Store = crate::TelegramBlobStore<PooledChannel<Bot>>;

    /// Two bots on one fake channel, sends fast enough to never wait.
    fn pool() -> PooledChannel<Bot> {
        let reg =
            LimiterRegistry::new(crate::limits().with(SEND, Rate::new(100.0, 10.0)), crate::limits::CLASSES).unwrap();
        let channel = Arc::new(FakeChannel::new(7));
        let bot = |index: usize| {
            let limiter = reg.chat(&format!("bot-{index}"), "7");
            (FakeBot::new(channel.clone(), limiter.clone(), index), limiter)
        };
        PooledChannel::new("7", vec![bot(0), bot(1)])
    }

    fn sends(bot: &Bot) -> usize {
        bot.calls().send_text + bot.calls().send_document
    }

    #[tokio::test(start_paused = true)]
    async fn a_flood_wait_moves_the_send_to_the_other_bot_with_its_random_id() {
        let pooled = pool();
        pooled.member(0).flood_next(1, Duration::from_secs(40));
        let t0 = tokio::time::Instant::now();
        pooled.send_text("entry", 42).await.unwrap();
        assert!(t0.elapsed() < Duration::from_secs(1), "the 40 s flood was not waited out");
        assert_eq!((sends(pooled.member(0)), sends(pooled.member(1))), (0, 1));
        // The cooled bot is skipped while it cools.
        pooled.send_text("next", 43).await.unwrap();
        assert_eq!(sends(pooled.member(1)), 2);
        assert!(pooled.member(0).limiter().peek_wait(SEND) > Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn with_both_bots_flooded_the_send_waits_and_still_lands_once() {
        let pooled = pool();
        pooled.member(0).flood_next(1, Duration::from_secs(40));
        pooled.member(1).flood_next(1, Duration::from_secs(40));
        let t0 = tokio::time::Instant::now();
        pooled.send_text("entry", 42).await.unwrap();
        assert!(t0.elapsed() >= Duration::from_secs(40), "both bots were cooled: {:?}", t0.elapsed());
        assert_eq!(sends(pooled.member(0)) + sends(pooled.member(1)), 1);
    }

    fn store_on(pooled: PooledChannel<Bot>) -> Store {
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

    async fn get(store: &Store, id: &crate::TelegramId) -> Bytes {
        pigeonhole_storage_api::collect_stream(store.get(id, None).await.unwrap()).await.unwrap()
    }

    fn reads(store: &Store, bot: usize) -> usize {
        store.channel().member(bot).calls().get_messages
    }

    #[tokio::test(start_paused = true)]
    async fn a_document_another_bot_posted_is_read_with_its_location_without_a_lookup() {
        let pooled = pool();
        pooled.member(0).flood_next(1, Duration::from_secs(40));
        let store = store_on(pooled);
        let data = Bytes::from(vec![7u8; 100_000]);
        let id = store.put(data.clone(), &BlobId::random().name()).await.unwrap();
        assert_eq!(sends(store.channel().member(1)), 1, "the flood moved the put to b");
        assert_eq!(store.channel().holders(id.message_id), vec![1], "b's location, from its send");
        assert_eq!(get(&store, &id).await, data);
        assert_eq!((reads(&store, 0), reads(&store, 1)), (0, 0), "read after write without getMessages");
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_holder_has_a_free_bot_read_the_message_and_both_replicas_stay() {
        let store = store_on(pool());
        let data = Bytes::from(vec![9u8; 100_000]);
        let id = store.put(data.clone(), &BlobId::random().name()).await.unwrap();
        let first = store.channel().holders(id.message_id)[0];
        let other = 1 - first;
        store.channel().member(first).limiter().cool(Spend::Bot, Level::Bot, Duration::from_secs(100));
        let t0 = tokio::time::Instant::now();
        assert_eq!(get(&store, &id).await, data);
        assert!(t0.elapsed() < Duration::from_secs(1), "the free bot read it: {:?}", t0.elapsed());
        assert_eq!(reads(&store, other), 1, "after one getMessages of its own");
        assert_eq!(store.channel().holders(id.message_id), vec![0, 1], "both replicas kept");
    }

    #[tokio::test(start_paused = true)]
    async fn a_pushed_document_is_downloaded_by_the_bot_it_was_pushed_to_without_a_lookup() {
        let pooled = pool();
        let data = Bytes::from(vec![5u8; 50_000]);
        // Posted by someone else; Telegram pushes it to bot 1, in its location.
        let sent = pooled.member(0).channel().send_document("raft.bin", data.clone(), 9).await.unwrap();
        let mut file = sent.file.unwrap();
        file.file_reference.push(1);
        let pushed = ChannelMessage::Document { name: "raft.bin".into(), size: data.len() as u64, file: file.clone() };
        let err = pooled.get_file(&file, 0, crate::WINDOW).await.unwrap_err();
        assert!(err.is::<FileReferenceExpired>(), "a push the pool was not told of: no holder");
        pooled.pushed_to(1, sent.message_id, &pushed);
        assert_eq!(pooled.holders(sent.message_id), vec![1]);
        assert_eq!(pooled.get_file(&file, 0, crate::WINDOW).await.unwrap(), data);
        let reads = pooled.member(0).calls().get_messages + pooled.member(1).calls().get_messages;
        assert_eq!(reads, 0, "downloaded with the pushed location, no getMessages");
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_location_is_dropped_and_read_again() {
        let store = store_on(pool());
        let data = Bytes::from(vec![3u8; 100_000]);
        let id = store.put(data.clone(), &BlobId::random().name()).await.unwrap();
        store.channel().member(0).channel().expire_file_references();
        assert_eq!(get(&store, &id).await, data, "read again with a fresh reference");
        assert_eq!(store.channel().holders(id.message_id).len(), 1, "the stale replica was replaced");
    }
}
