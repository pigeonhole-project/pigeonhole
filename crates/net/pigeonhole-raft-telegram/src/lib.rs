//! A node's Raft outbox in a Telegram channel (docs/cluster.md §
//! Topology): openraft-chat's [`Mailbox`] over the store's `ChannelApi`,
//! the binary's real channels and the demo's fake ones alike.
//!
//! A frame is one message: a text when it fits (`raft.bin`, inline as the
//! store writes small blobs), a document otherwise. Its position is the
//! message id. Bots cannot ask a channel for its newest messages, only for
//! messages by id.
//!
//! Reading is push first ([`TelegramMailbox::pushed`]): Telegram pushes
//! every new post, message and all, and a reader takes the frames from
//! what was pushed without a call. It reads the chat (ids upward, 100 a
//! call) in two cases only: a hole, ids between the pushed ones that no
//! push brought (read whole, up to the newest pushed), or nothing pushed
//! for `poll` ([`TelegramMailbox::polling`]): a push lost on a reconnect
//! is found then. How far up the channel goes it learns from its own
//! posts, the pushes, and at the start by looking ids up
//! ([`TelegramMailbox::head`]).
//!
//! Pruning deletes the owner's frames below a position; the frame at that
//! position is pinned first, as an anchor: the lookup of the newest id
//! starts at the pinned message, above which nothing was deleted.
//!
//! A mailbox [named](TelegramMailbox::named) as a store logs its posts and
//! deletions as the stores do (`posted`, `deleted`, at debug).

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use openraft_chat::Mailbox;
use pigeonhole_storage_api::text;
use pigeonhole_storage_telegram::{ChannelApi, ChannelMessage, FileLocation, IDS_PER_CALL, TEXT_MAX_CHARS, WINDOW};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

/// The name every frame is posted under.
pub const FRAME_NAME: &str = "raft.bin";

/// Largest frame a reader downloads (a snapshot chunk and some).
const MAX_FRAME: u64 = 64 << 20;

/// Most delete calls (of 100 ids each) one prune makes; the rest wait for
/// the next one, so a long unpruned outbox is cleared over several.
const DELETES_PER_PRUNE: usize = 10;

/// Most pushed messages kept for the reader (its own outbox's pushes have
/// none): past it the oldest go, and a reader reads them as a hole.
const MAX_PUSHED: usize = 1_000;

pub struct TelegramMailbox<C> {
    channel: Arc<C>,
    /// The newest message id known to be in the channel (0: none yet).
    top: watch::Sender<i64>,
    /// Pruning: the lowest id that may still hold a message below the
    /// anchor (0: not known yet, the pinned anchor's or 1).
    pruned_to: AtomicI64,
    /// The store it is to the logs; none: it logs nothing.
    store: Option<String>,
    /// What Telegram pushed that the reader has not taken yet.
    pushed: Mutex<Pushed>,
    /// A read of the chat when nothing was pushed for this long (zero: on
    /// every ask, a mailbox nobody pushes to).
    poll: Duration,
}

#[derive(Default)]
struct Pushed {
    /// Message id → the message; `None`: posted, what it holds not known
    /// (read it as a hole).
    messages: BTreeMap<i64, Option<ChannelMessage>>,
    /// The reader has taken every id up to here: a hole starts above it,
    /// and pushes at or below it are old news.
    taken: i64,
    /// The last push or read of the chat; `None`: never (a read is due).
    heard: Option<Instant>,
}

impl<C: ChannelApi> TelegramMailbox<C> {
    pub fn new(channel: Arc<C>) -> Self {
        Self {
            channel,
            top: watch::channel(0).0,
            pruned_to: AtomicI64::new(0),
            store: None,
            pushed: Mutex::default(),
            poll: Duration::ZERO,
        }
    }

    /// Reads the chat only after `poll` without a push (or for a hole).
    pub fn polling(self, poll: Duration) -> Self {
        Self { poll, ..self }
    }

    /// Logs its posts and deletions as store `store`'s.
    pub fn named(self, store: impl Into<String>) -> Self {
        Self { store: Some(store.into()), ..self }
    }

    /// Telegram pushed post `id`: `message`, or `None` when the push did not
    /// say what it holds (read then). A reader waiting for it wakes.
    pub fn pushed(&self, id: i64, message: Option<ChannelMessage>) {
        {
            let mut p = self.pushed.lock().unwrap();
            p.heard = Some(Instant::now());
            if id > p.taken {
                p.messages.insert(id, message);
                while p.messages.len() > MAX_PUSHED {
                    p.messages.pop_first();
                }
            }
        }
        self.seen(id);
    }

    /// Message `id` is in the channel (we posted or read it, or it was
    /// pushed): a reader waiting for it wakes.
    pub fn seen(&self, id: i64) {
        self.top.send_if_modified(|top| {
            let newer = id > *top;
            *top = (*top).max(id);
            newer
        });
    }

    /// The frame in message `m`, if it is one.
    async fn frame(&self, m: ChannelMessage) -> Result<Option<Bytes>> {
        Ok(match m {
            ChannelMessage::Text { text } => text::decode(&text).filter(|(name, _)| name == FRAME_NAME).map(|(_, b)| b),
            ChannelMessage::Document { name, size, file } if name == FRAME_NAME && size <= MAX_FRAME => {
                Some(self.download(&file, size).await?)
            }
            _ => None,
        })
    }

    async fn download(&self, file: &FileLocation, size: u64) -> Result<Bytes> {
        let mut out = BytesMut::with_capacity(size as usize);
        while (out.len() as u64) < size {
            let part = self.channel.get_file(file, out.len() as u64, WINDOW).await.context("download a frame")?;
            if part.is_empty() {
                break;
            }
            out.extend_from_slice(&part);
        }
        Ok(out.freeze())
    }

    /// Whether any of ids `[from, from + 100)` holds a message, and the
    /// newest that does.
    async fn newest_in(&self, from: i64) -> Result<Option<i64>> {
        let ids: Vec<i64> = (from..from + IDS_PER_CALL as i64).collect();
        let msgs = self.channel.get_messages(&ids).await?;
        Ok(ids.into_iter().zip(msgs).rev().find(|(_, m)| *m != ChannelMessage::Empty).map(|(id, _)| id))
    }

    /// The newest message id, looked up: windows of 100 ids doubling their
    /// stride up from the pinned anchor (or the bottom) until one is empty,
    /// then halving between the last full and the first empty one
    /// (~2·log2(top / 100) reads). Assumes no run of 100 deleted messages
    /// between the anchor and the top: pruning deletes only below it.
    async fn find_top(&self) -> Result<i64> {
        let window = IDS_PER_CALL as i64;
        let (mut found, mut stride) = (None, window);
        let mut at = self.channel.pinned().await?.unwrap_or(1).max(1);
        let empty = loop {
            match self.newest_in(at).await? {
                Some(id) => found = Some(id),
                None => break at,
            }
            at += stride;
            stride *= 2;
        };
        let Some(mut newest) = found else { return Ok(0) };
        let (mut full, mut empty) = (newest, empty);
        while empty - full > window {
            let mid = full + (empty - full) / 2;
            match self.newest_in(mid).await? {
                Some(id) => {
                    newest = newest.max(id);
                    full = id;
                }
                None => empty = mid,
            }
        }
        // The last window up to the empty one.
        if let Some(id) = self.newest_in(full + 1).await? {
            newest = newest.max(id);
        }
        Ok(newest)
    }
}

impl<C: ChannelApi> TelegramMailbox<C> {
    /// Post one frame: its position, and the message as a push of it
    /// would carry it (`None`: a document whose location the answer did
    /// not give).
    pub async fn post_message(&self, frame: Bytes) -> Result<(u64, Option<ChannelMessage>)> {
        let random_id = uuid::Uuid::new_v4().as_u64_pair().0 as i64;
        let (bytes, inline) = (frame.len(), text::fits(FRAME_NAME, frame.len(), TEXT_MAX_CHARS));
        let (sent, message) = match inline {
            true => {
                let text = text::encode(FRAME_NAME, &frame);
                (self.channel.send_text(&text, random_id).await?, Some(ChannelMessage::Text { text }))
            }
            false => {
                let sent = self.channel.send_document(FRAME_NAME, frame, random_id).await?;
                let message = sent.file.clone().map(|file| ChannelMessage::Document {
                    name: FRAME_NAME.into(),
                    size: bytes as u64,
                    file,
                });
                (sent, message)
            }
        };
        if let Some(store) = &self.store {
            let kind = if inline { "text" } else { "document" };
            tracing::debug!(%store, message_id = sent.message_id, name = FRAME_NAME, bytes, kind, "posted");
        }
        self.seen(sent.message_id);
        Ok((sent.message_id as u64, message))
    }

    /// The frames pushed after `after`, at most `limit` ids of them; a hole
    /// among them read from the chat. `None`: nothing pushed past what the
    /// reader took.
    async fn take_pushed(&self, after: i64, limit: usize) -> Result<Option<Vec<(u64, Bytes)>>> {
        // What was pushed above what the reader has, and the ids it lacks.
        let (from, to, mut messages) = {
            let mut p = self.pushed.lock().unwrap();
            let from = after.max(p.taken) + 1;
            p.messages = p.messages.split_off(&from);
            let Some(&newest) = p.messages.keys().next_back() else { return Ok(None) };
            let to = newest.min(from + limit.clamp(1, IDS_PER_CALL) as i64 - 1);
            let known: BTreeMap<i64, ChannelMessage> =
                p.messages.range(from..=to).filter_map(|(id, m)| Some((*id, m.clone()?))).collect();
            (from, to, known)
        };
        let hole: Vec<i64> = (from..=to).filter(|id| !messages.contains_key(id)).collect();
        if !hole.is_empty() {
            messages.extend(hole.iter().copied().zip(self.channel.get_messages(&hole).await?));
        }
        let mut frames = Vec::new();
        for (id, m) in messages {
            if let Some(bytes) = self.frame(m).await? {
                frames.push((id as u64, bytes));
            }
        }
        // Taken once read whole: a failed call leaves it all for the next.
        let mut p = self.pushed.lock().unwrap();
        p.messages = p.messages.split_off(&(to + 1));
        p.taken = p.taken.max(to);
        p.heard = Some(Instant::now());
        Ok(Some(frames))
    }

    /// The frames in the ids after `after`, read from the chat a window of
    /// 100 at a time. A window with none is skipped while the channel is
    /// known to go past it (deleted or foreign messages), so a reader never
    /// sticks there.
    async fn read_chat(&self, after: i64, limit: usize) -> Result<Vec<(u64, Bytes)>> {
        let (mut from, mut newest) = (after + 1, 0);
        let frames = loop {
            let n = limit.clamp(1, IDS_PER_CALL);
            let ids: Vec<i64> = (from..from + n as i64).collect();
            let msgs = self.channel.get_messages(&ids).await?;
            let mut frames = Vec::new();
            for (id, m) in ids.iter().zip(msgs) {
                if m != ChannelMessage::Empty {
                    self.seen(*id);
                    newest = *id;
                }
                if let Some(bytes) = self.frame(m).await? {
                    frames.push((*id as u64, bytes));
                }
            }
            let end = from + n as i64 - 1;
            if !frames.is_empty() || *self.top.borrow() <= end {
                break frames;
            }
            from = end + 1;
        };
        let mut p = self.pushed.lock().unwrap();
        p.taken = p.taken.max(newest);
        p.heard = Some(Instant::now());
        Ok(frames)
    }
}

#[async_trait]
impl<C: ChannelApi> Mailbox for TelegramMailbox<C> {
    async fn post(&self, frame: Bytes) -> Result<u64> {
        Ok(self.post_message(frame).await?.0)
    }

    /// Push first: the frames Telegram pushed (a hole among them read);
    /// with none, the chat read only if nothing came for `poll`.
    async fn read_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Bytes)>> {
        let after = after as i64;
        if let Some(frames) = self.take_pushed(after, limit).await? {
            return Ok(frames);
        }
        let quiet = self.pushed.lock().unwrap().heard.is_none_or(|t| t.elapsed() >= self.poll);
        if !quiet {
            return Ok(Vec::new());
        }
        self.read_chat(after, limit).await
    }

    async fn head(&self) -> Result<u64> {
        let known = *self.top.borrow();
        if known > 0 {
            return Ok(known as u64);
        }
        let top = self.find_top().await?;
        self.seen(top);
        Ok(top as u64)
    }

    /// Wakes on a post past what the reader took (a trailing message that
    /// is no frame, a pin's, does not keep waking it).
    async fn wait(&self, after: u64, timeout: Duration) {
        let after = (after as i64).max(self.pushed.lock().unwrap().taken);
        let mut top = self.top.subscribe();
        let _ = tokio::time::timeout(timeout, top.wait_for(|t| *t > after)).await;
    }

    /// Pin the frame at `keep_from` as the anchor, then delete the ids below
    /// it, up from the last anchor, 100 a call, at most
    /// [`DELETES_PER_PRUNE`] calls (the rest next time). Nothing is deleted
    /// unless the anchor is pinned: the lookup of the top needs it.
    async fn prune(&self, keep_from: u64) -> Result<()> {
        let keep_from = keep_from as i64;
        let pinned = self.channel.pinned().await?;
        let mut low = match self.pruned_to.load(Ordering::Relaxed) {
            0 => pinned.unwrap_or(1).max(1),
            low => low,
        };
        if low >= keep_from {
            return Ok(());
        }
        if pinned != Some(keep_from) {
            self.channel.pin(keep_from).await.context("pin the outbox's anchor")?;
        }
        for _ in 0..DELETES_PER_PRUNE {
            if low >= keep_from {
                break;
            }
            let ids: Vec<i64> = (low..keep_from.min(low + IDS_PER_CALL as i64)).collect();
            self.channel.delete_messages(&ids).await.context("delete old frames")?;
            if let Some(store) = &self.store {
                tracing::debug!(%store, messages = ?ids, "deleted");
            }
            low += ids.len() as i64;
            self.pruned_to.store(low, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_storage_telegram::fake::FakeChannel;

    fn mailbox() -> (Arc<FakeChannel>, TelegramMailbox<FakeChannel>) {
        let channel = Arc::new(FakeChannel::new(1));
        (channel.clone(), TelegramMailbox::new(channel))
    }

    #[tokio::test]
    async fn frames_go_as_texts_or_documents_and_come_back_in_order() {
        let (_, outbox) = mailbox();
        let small = Bytes::from_static(b"a small frame");
        let big = Bytes::from(vec![7u8; 20_000]);
        let a = outbox.post(small.clone()).await.unwrap();
        let b = outbox.post(big.clone()).await.unwrap();
        assert!(b > a);
        let reader = TelegramMailbox::new(outbox.channel.clone());
        assert_eq!(reader.read_after(0, 100).await.unwrap(), vec![(a, small), (b, big.clone())]);
        assert_eq!(reader.read_after(a, 100).await.unwrap(), vec![(b, big)]);
        assert!(reader.read_after(b, 100).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn foreign_and_deleted_messages_are_skipped_even_a_window_of_them() {
        let (channel, outbox) = mailbox();
        let first = outbox.post(Bytes::from_static(b"one")).await.unwrap();
        for _ in 0..150 {
            channel.post_text();
        }
        let gone = outbox.post(Bytes::from_static(b"deleted")).await.unwrap();
        channel.remove(gone as i64);
        let last = outbox.post(Bytes::from_static(b"two")).await.unwrap();
        let reader = TelegramMailbox::new(channel.clone());
        reader.seen(last as i64); // as a push would tell it
        assert_eq!(reader.read_after(first, 100).await.unwrap(), vec![(last, Bytes::from_static(b"two"))]);
    }

    #[tokio::test]
    async fn the_head_is_found_by_looking_ids_up() {
        let (channel, outbox) = mailbox();
        let reader = TelegramMailbox::new(channel.clone());
        assert_eq!(reader.head().await.unwrap(), 0, "an empty channel");
        for n in [1, 99, 100, 101, 250, 1_000] {
            let (channel, outbox) = mailbox();
            let mut last = 0;
            for _ in 0..n {
                last = channel.post_text();
            }
            assert_eq!(TelegramMailbox::new(channel).head().await.unwrap(), last as u64, "{n} messages");
            drop(outbox);
        }
        let top = outbox.post(Bytes::from_static(b"x")).await.unwrap();
        assert_eq!(outbox.head().await.unwrap(), top, "its own post is its head");
    }

    #[tokio::test]
    async fn a_pruned_outbox_keeps_its_anchor_and_the_frames_above() {
        let (channel, outbox) = mailbox();
        let mut ids = Vec::new();
        for i in 0..300u32 {
            ids.push(outbox.post(Bytes::from(i.to_le_bytes().to_vec())).await.unwrap());
        }
        let (anchor, top) = (ids[249], ids[299]);
        outbox.prune(anchor).await.unwrap();
        assert_eq!(channel.pinned_now(), Some(anchor as i64), "the anchor is pinned");
        let below: Vec<i64> = (1..anchor as i64).collect();
        assert!(
            channel.get_messages(&below[..100]).await.unwrap().iter().all(|m| *m == ChannelMessage::Empty),
            "the frames below the anchor are gone"
        );
        // A reader that comes up now finds the top past 249 deleted ids (the
        // pin's own service message, if the chat posts one, is newer still),
        // and reads what is left: the frames, not the service message.
        let reader = TelegramMailbox::new(channel.clone());
        assert!(reader.head().await.unwrap() >= top);
        let left = reader.read_after(anchor - 1, 100).await.unwrap();
        assert_eq!(left.len(), 51);
        assert_eq!(left[0].0, anchor);
        // Pruning again, higher up, moves the anchor and deletes the old one.
        outbox.prune(ids[289]).await.unwrap();
        assert_eq!(channel.pinned_now(), Some(ids[289] as i64));
        assert_eq!(channel.get_messages(&[anchor as i64]).await.unwrap(), vec![ChannelMessage::Empty]);
        assert!(TelegramMailbox::new(channel).head().await.unwrap() >= top);
    }

    /// `n` frames posted to `outbox`, as a push of each would carry it.
    async fn post_n(outbox: &TelegramMailbox<FakeChannel>, n: usize) -> Vec<(u64, Option<ChannelMessage>)> {
        let mut posted = Vec::new();
        for i in 0..n {
            posted.push(outbox.post_message(Bytes::from(format!("frame {i}"))).await.unwrap());
        }
        posted
    }

    fn frames(posted: &[(u64, Option<ChannelMessage>)]) -> Vec<(u64, Bytes)> {
        posted.iter().enumerate().map(|(i, (id, _))| (*id, Bytes::from(format!("frame {i}")))).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn pushed_frames_are_read_without_a_call() {
        let (channel, outbox) = mailbox();
        let reader = TelegramMailbox::new(channel.clone()).polling(Duration::from_secs(3));
        let posted = post_n(&outbox, 3).await;
        let big = Bytes::from(vec![7u8; 20_000]);
        let doc = outbox.post_message(big.clone()).await.unwrap();
        for (id, m) in posted.iter().chain([&doc]) {
            reader.pushed(*id as i64, m.clone());
        }
        let before = channel.calls();
        let mut want = frames(&posted);
        want.push((doc.0, big));
        assert_eq!(reader.read_after(0, 100).await.unwrap(), want);
        assert_eq!(channel.calls().get_messages, before.get_messages, "no read: the pushes held the frames");
        assert!(channel.calls().get_file > before.get_file, "a document is still downloaded");
        assert!(reader.read_after(doc.0, 100).await.unwrap().is_empty());
        assert_eq!(channel.calls().get_messages, before.get_messages, "nothing pushed since, the poll not due");
    }

    #[tokio::test(start_paused = true)]
    async fn a_hole_is_read_whole_up_to_the_newest_push() {
        let (channel, outbox) = mailbox();
        let reader = TelegramMailbox::new(channel.clone()).polling(Duration::from_secs(3));
        let posted = post_n(&outbox, 6).await;
        // Pushes lost for the middle four (a reconnect): the next one shows the hole.
        for (id, m) in [&posted[0], &posted[5]] {
            reader.pushed(*id as i64, m.clone());
        }
        let before = channel.calls().get_messages;
        assert_eq!(reader.read_after(0, 100).await.unwrap(), frames(&posted));
        assert_eq!(channel.calls().get_messages, before + 1, "the hole in one read");
        // A push with no message (its content not said) is a hole of one.
        let next = post_n(&outbox, 1).await;
        reader.pushed(next[0].0 as i64, None);
        let got = reader.read_after(posted[5].0, 100).await.unwrap();
        assert_eq!(got, vec![(next[0].0, Bytes::from_static(b"frame 0"))]);
        assert_eq!(channel.calls().get_messages, before + 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_chat_is_read_only_after_a_poll_without_a_push() {
        let (channel, outbox) = mailbox();
        let poll = Duration::from_secs(3);
        let reader = TelegramMailbox::new(channel.clone()).polling(poll);
        let first = post_n(&outbox, 2).await;
        let calls = || channel.calls().get_messages;
        let c0 = calls();
        assert_eq!(reader.read_after(0, 100).await.unwrap(), frames(&first), "never heard: a read is due");
        assert_eq!(calls(), c0 + 1);
        // Posted with its push lost: not read before the poll is due...
        let lost = outbox.post_message(Bytes::from_static(b"lost")).await.unwrap();
        tokio::time::sleep(poll / 2).await;
        assert!(reader.read_after(first[1].0, 100).await.unwrap().is_empty());
        assert_eq!(calls(), c0 + 1, "heard from the chat less than a poll ago");
        // ...then read.
        tokio::time::sleep(poll).await;
        assert_eq!(reader.read_after(first[1].0, 100).await.unwrap(), vec![(lost.0, Bytes::from_static(b"lost"))]);
        assert_eq!(calls(), c0 + 2);
        // Pushes keep it quiet: one every half a poll, never a read.
        let mut at = lost.0;
        for (id, m) in post_n(&outbox, 4).await {
            tokio::time::sleep(poll / 2).await;
            reader.pushed(id as i64, m);
            assert_eq!(reader.read_after(at, 100).await.unwrap().len(), 1);
            at = id;
            tokio::time::sleep(poll / 2).await;
            assert!(reader.read_after(at, 100).await.unwrap().is_empty());
        }
        assert_eq!(calls(), c0 + 2, "each push came within a poll of the last");
    }

    #[tokio::test(start_paused = true)]
    async fn a_pushed_message_that_is_no_frame_does_not_keep_waking_the_reader() {
        let (channel, outbox) = mailbox();
        let reader = TelegramMailbox::new(channel.clone()).polling(Duration::from_secs(3));
        let (frame, m) = outbox.post_message(Bytes::from_static(b"x")).await.unwrap();
        reader.pushed(frame as i64, m);
        let pin = channel.post_text(); // a pin's service message, say
        reader.pushed(pin, Some(ChannelMessage::Other));
        assert_eq!(reader.read_after(0, 100).await.unwrap().len(), 1);
        assert!(reader.read_after(frame, 100).await.unwrap().is_empty());
        let t0 = Instant::now();
        reader.wait(frame, Duration::from_secs(2)).await;
        // The simulator's timers fire a few nanoseconds late.
        let slept = t0.elapsed();
        assert!(
            (Duration::from_secs(2)..Duration::from_millis(2001)).contains(&slept),
            "slept till the timeout: {slept:?}"
        );
    }

    #[tokio::test]
    async fn a_reader_wakes_when_a_post_is_seen() {
        let (channel, outbox) = mailbox();
        let reader = Arc::new(TelegramMailbox::new(channel));
        let waiting = {
            let reader = reader.clone();
            tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                reader.wait(0, Duration::from_secs(30)).await;
                t0.elapsed()
            })
        };
        let id = outbox.post(Bytes::from_static(b"x")).await.unwrap();
        reader.seen(id as i64);
        assert!(waiting.await.unwrap() < Duration::from_secs(5));
    }
}
