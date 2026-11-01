//! A fake channel (feature `fake`), behaving as the live checks found
//! ([`state`]). Its state lives in a host: in memory for tests
//! ([`FakeDiscord::new`]), or wherever `pigeonhole-fake-channels` keeps it.

pub mod bot;
pub mod state;

use crate::api::{CdnStale, DcMessage, DiscordApi, ThreadStart};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_storage_api::fake::{FakeHost, MemoryHost};
use state::{check_send, check_thread_name, now_ms, Command, DiscordChannelState, File, Message, Reply};
use std::sync::Mutex;

/// How often each call was made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Calls {
    pub send: usize,
    pub get_message: usize,
    pub list: usize,
    pub delete: usize,
    pub start_thread: usize,
    pub archive_thread: usize,
    pub download: usize,
}

/// What tests make a channel do wrong; not part of its state.
#[derive(Default)]
struct Knobs {
    calls: Calls,
    lose_next_answer: bool,
    /// The next send's answer comes this much later (it lands at once).
    delay_next_answer: Option<std::time::Duration>,
}

/// The host of a fake Discord channel in memory.
pub type Host = MemoryHost<DiscordChannelState>;

pub struct FakeDiscord<H = Host> {
    id: u64,
    host: H,
    knobs: Mutex<Knobs>,
}

impl FakeDiscord {
    /// A channel in memory.
    pub fn new(channel_id: u64) -> Self {
        Self::on(channel_id, Host::default())
    }

    /// Someone else posts a text; its id.
    pub fn post_text(&self, text: &str) -> u64 {
        let message = Message { content: text.into(), file: None };
        match self.host.apply_now(Command::Post { message, now_ms: now_ms() }) {
            Reply::Sent { id: Some(id), .. } => id,
            other => panic!("a post answered {other:?}"),
        }
    }

    /// Every attachment URL handed out so far stops working.
    pub fn expire_urls(&self) {
        self.host.apply_now(Command::ExpireUrls);
    }

    /// Delete message `id` behind everyone's back.
    pub fn remove(&self, id: u64) {
        if let Reply::Released { blobs } = self.host.apply_now(Command::Delete { ids: vec![id] }) {
            self.host.delete_blobs_now(&blobs);
        }
    }

    /// Change one byte of message `id`'s attachment, or one data character
    /// of its text (an edit).
    pub fn edit(&self, id: u64) {
        let Some(mut m) = self.host.read(|s| s.message(id).cloned()) else { return };
        if let Some(f) = m.file.as_mut() {
            let mut changed = self.host.blob_now(&f.blob).unwrap_or_default().to_vec();
            if let Some(last) = changed.last_mut() {
                *last ^= 0xff;
            }
            f.blob = self.host.put_blob_now(Bytes::from(changed));
        } else if let Some((head, payload)) = m.content.split_once('\n') {
            let mut chars = payload.chars();
            let Some(first) = chars.next() else { return };
            let flipped = char::from_u32(first as u32 ^ (1 << 14)).unwrap_or(first);
            m.content = format!("{head}\n{flipped}{}", chars.as_str());
        }
        if let Reply::Released { blobs } = self.host.apply_now(Command::Edit { id, message: m }) {
            self.host.delete_blobs_now(&blobs);
        }
    }

    /// Ids of the messages still there.
    pub fn message_ids(&self) -> Vec<u64> {
        self.host.read(DiscordChannelState::ids)
    }

    /// Starter ids of the threads still there.
    pub fn thread_ids(&self) -> Vec<u64> {
        self.host.read(DiscordChannelState::thread_ids)
    }
}

impl<H> FakeDiscord<H> {
    /// A channel kept by `host`.
    pub fn on(channel_id: u64, host: H) -> Self {
        Self { id: channel_id, host, knobs: Mutex::default() }
    }

    pub fn host(&self) -> &H {
        &self.host
    }

    pub fn calls(&self) -> Calls {
        self.knobs.lock().unwrap().calls
    }

    /// The next send posts, but the sender learns nothing.
    pub fn lose_next_answer(&self) {
        self.knobs.lock().unwrap().lose_next_answer = true;
    }

    /// The next send lands at once, but its answer comes `after` later (a
    /// slow connection).
    pub fn delay_next_answer(&self, after: std::time::Duration) {
        self.knobs.lock().unwrap().delay_next_answer = Some(after);
    }

    fn count(&self, f: impl FnOnce(&mut Calls)) {
        f(&mut self.knobs.lock().unwrap().calls);
    }
}

#[async_trait]
impl<H: FakeHost<DiscordChannelState>> DiscordApi for FakeDiscord<H> {
    fn channel_id(&self) -> u64 {
        self.id
    }

    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
        check_send(content, file.is_some(), nonce)?;
        self.count(|c| c.send += 1);
        let file = match file {
            Some((name, data)) => {
                let size = data.len() as u64;
                Some(File { name: name.into(), size, blob: self.host.put_blob(data).await? })
            }
            None => None,
        };
        let blob = file.as_ref().map(|f| f.blob.clone());
        let message = Message { content: content.into(), file };
        let reply = self.host.apply(Command::Send { nonce: nonce.into(), message, now_ms: now_ms() }).await?;
        let Reply::Sent { id, fresh } = reply else { bail!("a send answered {reply:?}") };
        if !fresh {
            if let Some(blob) = blob {
                self.host.delete_blobs(&[blob]).await?;
            }
        }
        let Some(answer) = id.and_then(|id| self.host.read(|s| s.view(self.id, id))) else {
            bail!("404 10008: Unknown Message");
        };
        if !fresh {
            return Ok(answer);
        }
        if answer.attachment.is_none() {
            let preview: String = content.lines().nth(1).unwrap_or(content).chars().take(24).collect();
            tracing::debug!(message_id = answer.id, chars = content.chars().count(), %preview, "fake channel: text");
        }
        let delay = {
            let mut k = self.knobs.lock().unwrap();
            if std::mem::take(&mut k.lose_next_answer) {
                bail!("connection reset before the answer");
            }
            k.delay_next_answer.take()
        };
        if let Some(after) = delay {
            tokio::time::sleep(after).await;
        }
        Ok(answer)
    }

    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
        self.count(|c| c.get_message += 1);
        Ok(self.host.read(|s| s.view(self.id, id)))
    }

    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
        self.count(|c| c.list += 1);
        Ok(self.host.read(|s| s.list(self.id, before, after, limit)))
    }

    async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
        self.count(|c| c.delete += 1);
        if let Reply::Released { blobs } = self.host.apply(Command::Delete { ids: ids.to_vec() }).await? {
            self.host.delete_blobs(&blobs).await?;
        }
        Ok(())
    }

    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
        check_thread_name(name)?;
        self.count(|c| c.start_thread += 1);
        match self.host.apply(Command::StartThread { id: message_id, name: name.into() }).await? {
            Reply::Thread(start) => Ok(start),
            other => bail!("a thread start answered {other:?}"),
        }
    }

    async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
        self.count(|c| c.get_message += 1);
        Ok(self.host.read(|s| s.thread(message_id).cloned()))
    }

    async fn archive_thread(&self, message_id: u64) -> Result<()> {
        self.count(|c| c.archive_thread += 1);
        if self.host.read(|s| s.thread(message_id).is_none()) {
            bail!("404: no thread {message_id}");
        }
        Ok(())
    }

    async fn delete_thread(&self, message_id: u64) -> Result<()> {
        self.host.apply(Command::DeleteThread { id: message_id }).await?;
        Ok(())
    }

    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        self.count(|c| c.download += 1);
        let Some((blob, size)) = self.host.read(|s| s.attachment(url).map(|f| f.map(|f| (f.blob.clone(), f.size))))?
        else {
            return Err(CdnStale.into());
        };
        match range {
            None => self.host.get_blob(&blob, None).await,
            Some((start, end)) if start < end && end <= size => self.host.get_blob(&blob, Some((start, end))).await,
            Some((start, end)) => bail!("416: range {start}..{end} of {size}"),
        }
    }
}
