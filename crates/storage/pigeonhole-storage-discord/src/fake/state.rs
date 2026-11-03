//! The state of a fake Discord channel, as the live checks found
//! (docs/discord-log.md § Spike findings): ids are snowflakes and grow, a
//! message has at most one thread ever (until the thread is deleted), a
//! thread outlives its starter, a nonce returns the first message, CDN URLs
//! expire. Kept by a [`FakeHost`](pigeonhole_storage_api::fake::FakeHost); an
//! attachment's bytes are a blob of the host's, the state holds its handle.

use crate::api::{DcAttachment, DcMessage, ThreadStart, CONTENT_MAX_CHARS, PAGE};
use crate::snowflake::DISCORD_EPOCH_MS;
use anyhow::{bail, Result};
use pigeonhole_storage_api::fake::FakeState;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// An attachment: its file name as Discord keeps it, size, and blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct File {
    pub name: String,
    pub size: u64,
    pub blob: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub content: String,
    pub file: Option<File>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscordChannelState {
    top: u64,
    messages: BTreeMap<u64, Message>,
    /// Thread names by starter message id (a thread outlives its starter).
    threads: BTreeMap<u64, String>,
    by_nonce: BTreeMap<String, u64>,
    /// Bumped by [`Command::ExpireUrls`]: older URLs fail.
    url_epoch: u32,
    /// Pinned message ids, the latest pinned first (as Discord lists them).
    pinned: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// A send of ours at `now_ms` (the snowflake's time; in the command, so
    /// a replay gives the same id). A nonce sent before posts nothing again.
    Send {
        nonce: String,
        message: Message,
        now_ms: u64,
    },
    /// Someone else posts (tests).
    Post {
        message: Message,
        now_ms: u64,
    },
    Delete {
        ids: Vec<u64>,
    },
    StartThread {
        id: u64,
        name: String,
    },
    DeleteThread {
        id: u64,
    },
    /// Every attachment URL handed out so far stops working.
    ExpireUrls,
    /// Message `id` changes in place (tests: an edit).
    Edit {
        id: u64,
        message: Message,
    },
    Pin {
        id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    /// The message's id; `fresh` false: the nonce was sent before. `None`:
    /// the nonce's message is gone (Discord answers 404).
    Sent {
        id: Option<u64>,
        fresh: bool,
    },
    Thread(ThreadStart),
    /// The blobs of attachments that are gone.
    Released {
        blobs: Vec<Vec<u8>>,
    },
    Done,
}

impl FakeState for DiscordChannelState {
    const NAME: &'static str = "fake-discord";
    const VERSION: u32 = 3;
    type Command = Command;
    type Reply = Reply;

    fn apply(&mut self, command: Command) -> Reply {
        match command {
            Command::Send { nonce, message, now_ms } => {
                if let Some(&id) = self.by_nonce.get(&nonce) {
                    return Reply::Sent { id: self.messages.contains_key(&id).then_some(id), fresh: false };
                }
                let id = self.post(message, now_ms);
                self.by_nonce.insert(nonce, id);
                Reply::Sent { id: Some(id), fresh: true }
            }
            Command::Post { message, now_ms } => Reply::Sent { id: Some(self.post(message, now_ms)), fresh: true },
            Command::Delete { ids } => {
                self.pinned.retain(|p| !ids.contains(p));
                Reply::Released {
                    blobs: ids
                        .iter()
                        .filter_map(|id| self.messages.remove(id))
                        .filter_map(|m| m.file.map(|f| f.blob))
                        .collect(),
                }
            }
            Command::Pin { id } => {
                if self.messages.contains_key(&id) {
                    self.pinned.retain(|p| *p != id);
                    self.pinned.insert(0, id);
                }
                Reply::Done
            }
            Command::StartThread { id, name } => Reply::Thread(if !self.messages.contains_key(&id) {
                ThreadStart::NoMessage
            } else {
                match self.threads.entry(id) {
                    std::collections::btree_map::Entry::Occupied(_) => ThreadStart::Taken,
                    std::collections::btree_map::Entry::Vacant(v) => {
                        v.insert(name);
                        ThreadStart::Created
                    }
                }
            }),
            Command::DeleteThread { id } => {
                self.threads.remove(&id);
                Reply::Done
            }
            Command::ExpireUrls => {
                self.url_epoch += 1;
                Reply::Done
            }
            Command::Edit { id, message } => {
                let old = match self.messages.get_mut(&id) {
                    Some(m) => std::mem::replace(m, message),
                    None => message,
                };
                Reply::Released { blobs: old.file.map(|f| f.blob).into_iter().collect() }
            }
        }
    }
}

impl DiscordChannelState {
    /// The pinned messages, the latest pinned first.
    pub fn pinned(&self) -> Vec<u64> {
        self.pinned.clone()
    }

    /// A snowflake for `now_ms`, above every id so far.
    fn post(&mut self, mut message: Message, now_ms: u64) -> u64 {
        // Discord keeps only some characters of a file name (it drops `~`).
        if let Some(f) = message.file.as_mut() {
            f.name = f.name.chars().filter(|c| c.is_ascii_alphanumeric() || "._-".contains(*c)).collect();
        }
        self.top = (self.top + 1).max(now_ms.saturating_sub(DISCORD_EPOCH_MS) << 22);
        self.messages.insert(self.top, message);
        self.top
    }

    pub fn message(&self, id: u64) -> Option<&Message> {
        self.messages.get(&id)
    }

    /// Message `id` as the API shows it in channel `channel`.
    pub fn view(&self, channel: u64, id: u64) -> Option<DcMessage> {
        let m = self.messages.get(&id)?;
        Some(DcMessage {
            id,
            kind: 0,
            content: m.content.clone(),
            attachment: m.file.as_ref().map(|f| DcAttachment {
                filename: f.name.clone(),
                size: f.size,
                url: format!("fake://{channel}/{id}/{}", self.url_epoch),
            }),
            thread: self.threads.get(&id).cloned(),
        })
    }

    /// A page of messages: `limit` after `after`, or the last `limit` before `before`.
    pub fn list(&self, channel: u64, before: Option<u64>, after: Option<u64>, limit: usize) -> Vec<DcMessage> {
        let limit = limit.clamp(1, PAGE);
        let range = self.messages.range(after.map_or(0, |a| a + 1)..before.unwrap_or(u64::MAX)).map(|(id, _)| *id);
        let ids: Vec<u64> = if after.is_some() {
            range.take(limit).collect()
        } else {
            let all: Vec<u64> = range.collect();
            all[all.len().saturating_sub(limit)..].to_vec()
        };
        ids.into_iter().filter_map(|id| self.view(channel, id)).collect()
    }

    pub fn thread(&self, id: u64) -> Option<&String> {
        self.threads.get(&id)
    }

    /// The attachment a URL points at, if the URL still works.
    pub fn attachment(&self, url: &str) -> Result<Option<&File>> {
        let parts: Vec<&str> = url.trim_start_matches("fake://").split('/').collect();
        let (Some(id), Some(epoch)) = (parts.get(1).and_then(|s| s.parse::<u64>().ok()), parts.get(2)) else {
            bail!("not a fake URL: {url}");
        };
        if epoch.parse::<u32>().ok() != Some(self.url_epoch) {
            return Ok(None);
        }
        Ok(self.messages.get(&id).and_then(|m| m.file.as_ref()))
    }

    /// Every message, by id.
    pub fn messages(&self) -> impl Iterator<Item = (u64, &Message)> {
        self.messages.iter().map(|(id, m)| (*id, m))
    }

    pub fn ids(&self) -> Vec<u64> {
        self.messages.keys().copied().collect()
    }

    pub fn thread_ids(&self) -> Vec<u64> {
        self.threads.keys().copied().collect()
    }
}

/// What Discord refuses before a send touches the channel.
pub fn check_send(content: &str, has_file: bool, nonce: &str) -> Result<()> {
    if content.is_empty() && !has_file {
        bail!("50006: cannot send an empty message");
    }
    if content.chars().count() > CONTENT_MAX_CHARS {
        bail!("50035: content must be 2000 or fewer in length");
    }
    if nonce.len() > 25 {
        bail!("50035: nonce must be 25 or fewer characters long");
    }
    Ok(())
}

pub fn check_thread_name(name: &str) -> Result<()> {
    if name.chars().count() > 100 {
        bail!("50035: thread name must be 100 or fewer in length");
    }
    Ok(())
}

/// Milliseconds since the Unix epoch, for a send's snowflake.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}
