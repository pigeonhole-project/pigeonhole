//! An in-memory channel for tests, behaving as the live checks found
//! (docs/discord-log.md § Spike findings): ids grow, a message has at most
//! one thread ever (until the thread is deleted), a thread outlives its
//! starter, a nonce returns the first message, CDN URLs expire.

use crate::api::{CdnStale, DcAttachment, DcMessage, DiscordApi, ThreadStart, PAGE};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// Characters a message's content takes (Discord: 2000 code points).
pub const CONTENT_MAX_CHARS: usize = 2000;

#[derive(Debug, Clone)]
struct Stored {
    content: String,
    file: Option<(String, Bytes)>,
}

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

#[derive(Default)]
struct Inner {
    top: u64,
    messages: BTreeMap<u64, Stored>,
    /// Thread names by starter message id (a thread outlives its starter).
    threads: HashMap<u64, String>,
    by_nonce: HashMap<String, u64>,
    /// Bumped by [`FakeDiscord::expire_urls`].
    url_epoch: u32,
    lose_next_answer: bool,
    /// The next send's answer comes this much later (it lands at once).
    delay_next_answer: Option<std::time::Duration>,
    calls: Calls,
}

impl Inner {
    /// A snowflake: the time now, above every id so far.
    fn next_id(&mut self) -> u64 {
        let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        self.top = (self.top + 1).max((ms - crate::client::DISCORD_EPOCH_MS) << 22);
        self.top
    }
}

pub struct FakeDiscord {
    id: u64,
    inner: Mutex<Inner>,
}

impl FakeDiscord {
    pub fn new(channel_id: u64) -> Self {
        Self { id: channel_id, inner: Mutex::new(Inner::default()) }
    }

    /// Someone else posts a text; its id.
    pub fn post_text(&self, text: &str) -> u64 {
        let mut g = self.inner.lock().unwrap();
        let id = g.next_id();
        g.messages.insert(id, Stored { content: text.into(), file: None });
        id
    }

    /// Every attachment URL handed out so far stops working.
    pub fn expire_urls(&self) {
        self.inner.lock().unwrap().url_epoch += 1;
    }

    /// The next send posts, but the sender learns nothing.
    pub fn lose_next_answer(&self) {
        self.inner.lock().unwrap().lose_next_answer = true;
    }

    /// The next send lands at once, but its answer comes `after` later (a
    /// slow connection).
    pub fn delay_next_answer(&self, after: std::time::Duration) {
        self.inner.lock().unwrap().delay_next_answer = Some(after);
    }

    /// Delete message `id` behind everyone's back.
    pub fn remove(&self, id: u64) {
        self.inner.lock().unwrap().messages.remove(&id);
    }

    /// Change one byte of message `id`'s attachment, or one data character
    /// of its text (an edit).
    pub fn edit(&self, id: u64) {
        let mut g = self.inner.lock().unwrap();
        let Some(m) = g.messages.get_mut(&id) else { return };
        if let Some((_, data)) = m.file.as_mut() {
            let mut changed = data.to_vec();
            if let Some(last) = changed.last_mut() {
                *last ^= 0xff;
            }
            *data = Bytes::from(changed);
        } else if let Some((head, payload)) = m.content.split_once('\n') {
            let mut chars = payload.chars();
            if let Some(first) = chars.next() {
                let flipped = char::from_u32(first as u32 ^ (1 << 14)).unwrap_or(first);
                m.content = format!("{head}\n{flipped}{}", chars.as_str());
            }
        }
    }

    pub fn calls(&self) -> Calls {
        self.inner.lock().unwrap().calls
    }

    /// Ids of the messages still there.
    pub fn message_ids(&self) -> Vec<u64> {
        self.inner.lock().unwrap().messages.keys().copied().collect()
    }

    /// Starter ids of the threads still there.
    pub fn thread_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.inner.lock().unwrap().threads.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// A send, under the lock: the answer, and how late it comes.
    fn post(
        &self,
        content: &str,
        file: Option<(&str, Bytes)>,
        nonce: &str,
    ) -> Result<(DcMessage, Option<std::time::Duration>)> {
        let mut g = self.inner.lock().unwrap();
        g.calls.send += 1;
        if let Some(&id) = g.by_nonce.get(nonce) {
            let m = g.messages.get(&id).cloned();
            return match m {
                Some(m) => Ok((self.view(&g, id, &m), None)),
                None => bail!("404 10008: Unknown Message"),
            };
        }
        let id = g.next_id();
        // Discord keeps only some characters of a file name (it drops `~`).
        let clean = |n: &str| n.chars().filter(|c| c.is_ascii_alphanumeric() || "._-".contains(*c)).collect::<String>();
        let stored = Stored { content: content.into(), file: file.map(|(n, d)| (clean(n), d)) };
        g.messages.insert(id, stored.clone());
        g.by_nonce.insert(nonce.into(), id);
        if std::mem::take(&mut g.lose_next_answer) {
            bail!("connection reset before the answer");
        }
        Ok((self.view(&g, id, &stored), g.delay_next_answer.take()))
    }

    fn view(&self, g: &Inner, id: u64, m: &Stored) -> DcMessage {
        DcMessage {
            id,
            kind: 0,
            content: m.content.clone(),
            attachment: m.file.as_ref().map(|(name, data)| DcAttachment {
                filename: name.clone(),
                size: data.len() as u64,
                url: format!("fake://{}/{id}/{}", self.id, g.url_epoch),
            }),
            thread: g.threads.get(&id).cloned(),
        }
    }
}

#[async_trait]
impl DiscordApi for FakeDiscord {
    fn channel_id(&self) -> u64 {
        self.id
    }

    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
        if content.is_empty() && file.is_none() {
            bail!("50006: cannot send an empty message");
        }
        if content.chars().count() > CONTENT_MAX_CHARS {
            bail!("50035: content must be 2000 or fewer in length");
        }
        if nonce.len() > 25 {
            bail!("50035: nonce must be 25 or fewer characters long");
        }
        let (answer, delay) = self.post(content, file, nonce)?;
        if let Some(after) = delay {
            tokio::time::sleep(after).await;
        }
        Ok(answer)
    }

    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
        let mut g = self.inner.lock().unwrap();
        g.calls.get_message += 1;
        let m = g.messages.get(&id).cloned();
        Ok(m.map(|m| self.view(&g, id, &m)))
    }

    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
        let mut g = self.inner.lock().unwrap();
        g.calls.list += 1;
        let limit = limit.clamp(1, PAGE);
        let lo = after.map_or(0, |a| a + 1);
        let hi = before.unwrap_or(u64::MAX);
        let range: Vec<(u64, Stored)> = g.messages.range(lo..hi).map(|(id, m)| (*id, m.clone())).collect();
        let picked: Vec<(u64, Stored)> = if after.is_some() {
            range.into_iter().take(limit).collect()
        } else {
            let skip = range.len().saturating_sub(limit);
            range.into_iter().skip(skip).collect()
        };
        Ok(picked.iter().map(|(id, m)| self.view(&g, *id, m)).collect())
    }

    async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.calls.delete += 1;
        for id in ids {
            g.messages.remove(id);
        }
        Ok(())
    }

    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
        if name.chars().count() > 100 {
            bail!("50035: thread name must be 100 or fewer in length");
        }
        let mut g = self.inner.lock().unwrap();
        g.calls.start_thread += 1;
        if !g.messages.contains_key(&message_id) {
            return Ok(ThreadStart::NoMessage);
        }
        if g.threads.contains_key(&message_id) {
            return Ok(ThreadStart::Taken);
        }
        g.threads.insert(message_id, name.into());
        Ok(ThreadStart::Created)
    }

    async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
        let mut g = self.inner.lock().unwrap();
        g.calls.get_message += 1;
        Ok(g.threads.get(&message_id).cloned())
    }

    async fn archive_thread(&self, message_id: u64) -> Result<()> {
        self.inner.lock().unwrap().calls.archive_thread += 1;
        if !self.inner.lock().unwrap().threads.contains_key(&message_id) {
            bail!("404: no thread {message_id}");
        }
        Ok(())
    }

    async fn delete_thread(&self, message_id: u64) -> Result<()> {
        self.inner.lock().unwrap().threads.remove(&message_id);
        Ok(())
    }

    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        let mut g = self.inner.lock().unwrap();
        g.calls.download += 1;
        let parts: Vec<&str> = url.trim_start_matches("fake://").split('/').collect();
        let (Some(id), Some(epoch)) = (parts.get(1).and_then(|s| s.parse::<u64>().ok()), parts.get(2)) else {
            bail!("not a fake URL: {url}");
        };
        if epoch.parse::<u32>().ok() != Some(g.url_epoch) {
            return Err(CdnStale.into());
        }
        let Some((_, data)) = g.messages.get(&id).and_then(|m| m.file.clone()) else {
            return Err(CdnStale.into());
        };
        Ok(match range {
            None => data,
            Some((start, end)) if start < end && end <= data.len() as u64 => data.slice(start as usize..end as usize),
            Some((start, end)) => bail!("416: range {start}..{end} of {}", data.len()),
        })
    }
}
