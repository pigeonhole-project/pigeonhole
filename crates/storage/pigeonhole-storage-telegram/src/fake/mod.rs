//! A fake channel (feature `fake`): ids grow, sends deduplicate by
//! `random_id`, `get_file` checks the window rules and file references. Its
//! state ([`state`]) lives in a host: in memory for tests ([`FakeChannel::new`]),
//! or wherever `pigeonhole-fake-channels` keeps it.

pub mod bot;
pub mod state;

use crate::channel::{ChannelApi, ChannelMessage, FileLocation, Sent};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_storage_api::fake::{FakeHost, MemoryHost};
use state::{check_ids, check_text, check_window, now_ms, Command, Message, Reply, TelegramChannelState};
use std::collections::HashMap;
use std::sync::Mutex;

/// How often each call was made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Calls {
    pub get_messages: usize,
    pub send_document: usize,
    pub send_text: usize,
    pub get_file: usize,
    pub delete_messages: usize,
    pub pin: usize,
}

/// A call that changes the channel, whose answer a test can lose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutation {
    /// `send_document` or `send_text`.
    Send,
    Delete,
    Pin,
}

/// What tests make a channel do wrong; not part of its state.
#[derive(Default)]
struct Knobs {
    calls: Calls,
    /// message id → reads it stays invisible for.
    hidden: HashMap<i64, u32>,
    /// The next send whose name starts with this is invisible for that many reads.
    hide_next: Option<(String, u32)>,
    /// The next call of each of these is applied but answers an error.
    lose_next: Vec<Mutation>,
}

/// The host of a fake Telegram channel in memory.
pub type Host = MemoryHost<TelegramChannelState>;

pub struct FakeChannel<H = Host> {
    id: i64,
    host: H,
    knobs: Mutex<Knobs>,
}

impl FakeChannel {
    /// A channel in memory.
    pub fn new(channel_id: i64) -> Self {
        Self::on(channel_id, Host::default())
    }

    /// Someone else posts a message that is not ours (a service message).
    pub fn post_text(&self) -> i64 {
        posted(self.host.apply_now(Command::Post { message: Message::Other, now_ms: now_ms() }))
    }

    /// Someone else posts a document; returns its id.
    pub fn post_document(&self, name: &str, data: &[u8]) -> i64 {
        let size = data.len() as u64;
        let blob = self.host.put_blob_now(Bytes::copy_from_slice(data));
        posted(
            self.host
                .apply_now(Command::Post { message: Message::Doc { name: name.into(), size, blob }, now_ms: now_ms() }),
        )
    }

    /// Every file reference handed out so far stops working.
    pub fn expire_file_references(&self) {
        self.host.apply_now(Command::ExpireReferences);
    }

    /// Change the bytes of document `id` in place (an edit).
    pub fn edit(&self, id: i64) {
        let message = match self.host.read(|s| s.message(id).cloned()) {
            Some(Message::Doc { name, size, blob }) => {
                let mut changed = self.host.blob_now(&blob).unwrap_or_default().to_vec();
                if let Some(last) = changed.last_mut() {
                    *last ^= 0xff;
                }
                Message::Doc { name, size, blob: self.host.put_blob_now(Bytes::from(changed)) }
            }
            // The first payload character changes its top bit (always a data
            // bit): still a valid encoding, other bytes.
            Some(Message::Text(text)) => {
                let Some((head, payload)) = text.split_once('\n') else { return };
                let mut chars = payload.chars();
                let Some(first) = chars.next() else { return };
                let flipped = char::from_u32(first as u32 ^ (1 << 14)).unwrap_or(first);
                Message::Text(format!("{head}\n{flipped}{}", chars.as_str()))
            }
            _ => return,
        };
        if let Reply::Released { blobs } = self.host.apply_now(Command::Edit { id, message }) {
            self.host.delete_blobs_now(&blobs);
        }
    }

    /// Delete message `id` behind everyone's back.
    pub fn remove(&self, id: i64) {
        if let Reply::Released { blobs } = self.host.apply_now(Command::Delete { ids: vec![id] }) {
            self.host.delete_blobs_now(&blobs);
        }
    }

    /// Ids and names of the messages still there (`(text)` for text).
    pub fn names(&self) -> Vec<(i64, String)> {
        self.host.read(TelegramChannelState::names)
    }

    /// Ids of the messages still there.
    pub fn message_ids(&self) -> Vec<i64> {
        self.host.read(TelegramChannelState::ids)
    }

    /// The pinned message.
    pub fn pinned_now(&self) -> Option<i64> {
        self.host.read(TelegramChannelState::pinned)
    }

    /// Someone pins message `id`.
    pub fn pin_now(&self, id: i64) {
        self.host.apply_now(Command::Pin { id, now_ms: now_ms() });
    }
}

fn posted(reply: Reply) -> i64 {
    match reply {
        Reply::Sent { id, .. } => id,
        other => panic!("a post answered {other:?}"),
    }
}

impl<H> FakeChannel<H> {
    /// A channel kept by `host`.
    pub fn on(channel_id: i64, host: H) -> Self {
        Self { id: channel_id, host, knobs: Mutex::default() }
    }

    pub fn host(&self) -> &H {
        &self.host
    }

    pub fn calls(&self) -> Calls {
        self.knobs.lock().unwrap().calls
    }

    /// The next document posted whose name starts with `prefix` is invisible
    /// for `reads` reads that include it.
    pub fn hide_next(&self, prefix: &str, reads: u32) {
        self.knobs.lock().unwrap().hide_next = Some((prefix.to_string(), reads));
    }

    /// The next send posts, but the sender learns nothing.
    pub fn lose_next_answer(&self) {
        self.lose_next_answer_of(Mutation::Send);
    }

    /// The next `call` is applied, but its caller gets an error (a lost
    /// answer: the outcome is unknown to it).
    pub fn lose_next_answer_of(&self, call: Mutation) {
        self.knobs.lock().unwrap().lose_next.push(call);
    }

    /// After `call` was applied: an error if its answer is to be lost.
    fn answer(&self, call: Mutation) -> Result<()> {
        let mut k = self.knobs.lock().unwrap();
        match k.lose_next.iter().position(|c| *c == call) {
            Some(i) => {
                k.lose_next.remove(i);
                bail!("connection reset before the answer to {call:?}")
            }
            None => Ok(()),
        }
    }

    /// After a send: the hide and lose knobs it triggers.
    fn after_send(&self, id: i64, name: &str) -> Result<()> {
        {
            let mut k = self.knobs.lock().unwrap();
            if k.hide_next.as_ref().is_some_and(|(prefix, _)| name.starts_with(prefix.as_str())) {
                let (_, reads) = k.hide_next.take().unwrap();
                k.hidden.insert(id, reads);
            }
        }
        self.answer(Mutation::Send)
    }
}

impl<H: FakeHost<TelegramChannelState>> FakeChannel<H> {
    /// A send through the host: the message's id, and whether it is new.
    async fn send(&self, random_id: i64, message: Message) -> Result<(i64, bool)> {
        match self.host.apply(Command::Send { random_id, message, now_ms: now_ms() }).await? {
            Reply::Sent { id, fresh } => Ok((id, fresh)),
            other => bail!("a send answered {other:?}"),
        }
    }
}

#[async_trait]
impl<H: FakeHost<TelegramChannelState>> ChannelApi for FakeChannel<H> {
    fn channel_id(&self) -> i64 {
        self.id
    }

    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        check_ids(ids.len(), "getMessages")?;
        let mut k = self.knobs.lock().unwrap();
        k.calls.get_messages += 1;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(left) = k.hidden.get_mut(id) {
                if *left > 0 {
                    *left -= 1;
                    out.push(ChannelMessage::Empty);
                    continue;
                }
            }
            out.push(self.host.read(|s| s.view(self.id, *id)));
        }
        Ok(out)
    }

    async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
        if data.is_empty() {
            bail!("FILE_PARTS_INVALID: empty document");
        }
        self.knobs.lock().unwrap().calls.send_document += 1;
        let size = data.len() as u64;
        let blob = self.host.put_blob(data).await?;
        let (id, fresh) = self.send(random_id, Message::Doc { name: name.into(), size, blob: blob.clone() }).await?;
        // A repeat of a send returns the message it posted, its document too.
        let file = match self.host.read(|s| s.view(self.id, id)) {
            ChannelMessage::Document { file, .. } => Some(file),
            _ => None,
        };
        if !fresh {
            self.host.delete_blobs(&[blob]).await?;
            return Ok(Sent { message_id: id, file });
        }
        self.after_send(id, name)?;
        Ok(Sent { message_id: id, file })
    }

    async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
        check_text(text)?;
        self.knobs.lock().unwrap().calls.send_text += 1;
        let (id, fresh) = self.send(random_id, Message::Text(text.to_string())).await?;
        if !fresh {
            return Ok(Sent { message_id: id, file: None });
        }
        tracing::debug!(message_id = id, chars = text.chars().count(), preview = %preview(text), "fake channel: text");
        self.after_send(id, text)?;
        Ok(Sent { message_id: id, file: None })
    }

    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
        check_window(offset, limit)?;
        self.knobs.lock().unwrap().calls.get_file += 1;
        let doc = self.host.read(|s| s.document(file).map(|d| d.map(|(blob, size)| (blob.to_vec(), size))))?;
        let Some((blob, size)) = doc else {
            bail!("FILE_ID_INVALID: no document {}", file.id);
        };
        self.host.get_blob(&blob, Some((offset.min(size), (offset + limit).min(size)))).await
    }

    async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
        check_ids(ids.len(), "deleteMessages")?;
        self.knobs.lock().unwrap().calls.delete_messages += 1;
        if let Reply::Released { blobs } = self.host.apply(Command::Delete { ids: ids.to_vec() }).await? {
            self.host.delete_blobs(&blobs).await?;
        }
        self.answer(Mutation::Delete)
    }

    async fn pinned(&self) -> Result<Option<i64>> {
        Ok(self.host.read(|s| s.pinned()))
    }

    async fn pin(&self, id: i64) -> Result<()> {
        self.knobs.lock().unwrap().calls.pin += 1;
        self.host.apply(Command::Pin { id, now_ms: now_ms() }).await?;
        self.answer(Mutation::Pin)
    }
}

/// The start of a text message's payload (after the `name len` line), for
/// the demo's view of the channel.
fn preview(text: &str) -> String {
    text.lines().nth(1).unwrap_or(text).chars().take(24).collect()
}
