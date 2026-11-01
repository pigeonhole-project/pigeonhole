//! A node's Raft outbox in a Telegram channel (docs/raft-over-chat.md §
//! Topology): openraft-chat's [`Mailbox`] over the store's `ChannelApi`.
//!
//! A frame is one message: a text when it fits (`raft.bin`, inline as the
//! store writes small blobs), a document otherwise. Its position is the
//! message id. Bots cannot ask a channel for its newest messages, only for
//! messages by id: a reader reads ids upward 100 at a time, and learns how
//! far up the channel goes from its own posts, from the posts Telegram
//! pushes to it ([`TelegramMailbox::seen`]), and, at the start, by looking
//! ids up ([`TelegramMailbox::head`]).

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use openraft_chat::Mailbox;
use pigeonhole_storage_api::text;
use pigeonhole_storage_telegram::{ChannelApi, ChannelMessage, FileLocation, IDS_PER_CALL, TEXT_MAX_CHARS, WINDOW};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// The name every frame is posted under.
pub const FRAME_NAME: &str = "raft.bin";

/// Largest frame a reader downloads (a snapshot chunk and some).
const MAX_FRAME: u64 = 64 << 20;

pub struct TelegramMailbox<C> {
    channel: Arc<C>,
    /// The newest message id known to be in the channel (0: none yet).
    top: watch::Sender<i64>,
}

impl<C: ChannelApi> TelegramMailbox<C> {
    pub fn new(channel: Arc<C>) -> Self {
        Self { channel, top: watch::channel(0).0 }
    }

    /// Message `id` is in the channel (Telegram pushed it, or we read it):
    /// a reader waiting for it wakes.
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
    /// stride up from the bottom until one is empty, then halving between
    /// the last full and the first empty one (~2·log2(top / 100) reads).
    /// Assumes no run of 100 deleted messages below the top.
    async fn find_top(&self) -> Result<i64> {
        let window = IDS_PER_CALL as i64;
        let (mut found, mut stride) = (None, window);
        let mut at = 1;
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

#[async_trait]
impl<C: ChannelApi> Mailbox for TelegramMailbox<C> {
    async fn post(&self, frame: Bytes) -> Result<u64> {
        let random_id = uuid::Uuid::new_v4().as_u64_pair().0 as i64;
        let sent = match text::fits(FRAME_NAME, frame.len(), TEXT_MAX_CHARS) {
            true => self.channel.send_text(&text::encode(FRAME_NAME, &frame), random_id).await?,
            false => self.channel.send_document(FRAME_NAME, frame, random_id).await?,
        };
        self.seen(sent.message_id);
        Ok(sent.message_id as u64)
    }

    /// The frames in the ids after `after`, a window of 100 at a time. A
    /// window with none is skipped while the channel is known to go past
    /// it (deleted or foreign messages), so a reader never sticks there.
    async fn read_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Bytes)>> {
        let mut from = after as i64 + 1;
        loop {
            let n = limit.clamp(1, IDS_PER_CALL);
            let ids: Vec<i64> = (from..from + n as i64).collect();
            let msgs = self.channel.get_messages(&ids).await?;
            let mut frames = Vec::new();
            for (id, m) in ids.iter().zip(msgs) {
                if m != ChannelMessage::Empty {
                    self.seen(*id);
                }
                if let Some(bytes) = self.frame(m).await? {
                    frames.push((*id as u64, bytes));
                }
            }
            let end = from + n as i64 - 1;
            if !frames.is_empty() || *self.top.borrow() <= end {
                return Ok(frames);
            }
            from = end + 1;
        }
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

    async fn wait(&self, after: u64, timeout: Duration) {
        let mut top = self.top.subscribe();
        let _ = tokio::time::timeout(timeout, top.wait_for(|t| *t > after as i64)).await;
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
