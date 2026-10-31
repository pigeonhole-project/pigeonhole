//! The state of a fake Telegram channel: its messages and the rules a send
//! and a read follow, as the live checks found (docs/telegram-mtproto.md).
//! Kept by a [`FakeHost`](pigeonhole_storage_api::fake::FakeHost); a
//! document's bytes are a blob of the host's, the state holds its handle.

use crate::channel::{ChannelMessage, FileLocation, IDS_PER_CALL, TEXT_MAX_CHARS, WINDOW};
use anyhow::{bail, Result};
use pigeonhole_storage_api::fake::FakeState;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One message of the channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// A foreign message that is neither text nor a document.
    Other,
    Text(String),
    Doc {
        name: String,
        size: u64,
        blob: Vec<u8>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelegramChannelState {
    top: i64,
    messages: BTreeMap<i64, Message>,
    by_random_id: BTreeMap<i64, i64>,
    /// Bumped by [`Command::ExpireReferences`]: older file references fail.
    reference_epoch: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// A send of ours; a `random_id` sent before posts nothing again.
    Send {
        random_id: i64,
        message: Message,
    },
    /// Someone else posts (tests).
    Post {
        message: Message,
    },
    Delete {
        ids: Vec<i64>,
    },
    /// Every file reference handed out so far stops working.
    ExpireReferences,
    /// Message `id` changes in place (tests: an edit).
    Edit {
        id: i64,
        message: Message,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    /// The message's id; `fresh` false: the random id was sent before.
    Sent {
        id: i64,
        fresh: bool,
    },
    /// The blobs of documents that are gone.
    Released {
        blobs: Vec<Vec<u8>>,
    },
    Done,
}

impl FakeState for TelegramChannelState {
    const NAME: &'static str = "fake-telegram";
    const VERSION: u32 = 1;
    type Command = Command;
    type Reply = Reply;

    fn apply(&mut self, command: Command) -> Reply {
        match command {
            Command::Send { random_id, message } => {
                if let Some(&id) = self.by_random_id.get(&random_id) {
                    return Reply::Sent { id, fresh: false };
                }
                let id = self.post(message);
                self.by_random_id.insert(random_id, id);
                Reply::Sent { id, fresh: true }
            }
            Command::Post { message } => Reply::Sent { id: self.post(message), fresh: true },
            Command::Delete { ids } => Reply::Released {
                blobs: ids.iter().filter_map(|id| self.messages.remove(id)).filter_map(blob).collect(),
            },
            Command::ExpireReferences => {
                self.reference_epoch = self.reference_epoch.wrapping_add(1);
                Reply::Done
            }
            Command::Edit { id, message } => match self.messages.get_mut(&id) {
                Some(m) => Reply::Released { blobs: blob(std::mem::replace(m, message)).into_iter().collect() },
                None => Reply::Released { blobs: blob(message).into_iter().collect() },
            },
        }
    }
}

fn blob(m: Message) -> Option<Vec<u8>> {
    match m {
        Message::Doc { blob, .. } => Some(blob),
        _ => None,
    }
}

impl TelegramChannelState {
    fn post(&mut self, message: Message) -> i64 {
        self.top += 1;
        self.messages.insert(self.top, message);
        self.top
    }

    pub fn message(&self, id: i64) -> Option<&Message> {
        self.messages.get(&id)
    }

    /// Message `id` as `getMessages` shows it in channel `channel`.
    pub fn view(&self, channel: i64, id: i64) -> ChannelMessage {
        match self.messages.get(&id) {
            None => ChannelMessage::Empty,
            Some(Message::Other) => ChannelMessage::Other,
            Some(Message::Text(text)) => ChannelMessage::Text { text: text.clone() },
            Some(Message::Doc { name, size, .. }) => {
                ChannelMessage::Document { name: name.clone(), size: *size, file: self.location(channel, id) }
            }
        }
    }

    /// Where document `id` is read from, with the current file reference.
    pub fn location(&self, channel: i64, id: i64) -> FileLocation {
        FileLocation { id, access_hash: channel, file_reference: vec![self.reference_epoch], dc_id: 2, bot: 0 }
    }

    /// The blob and size of document `file`, if its reference still holds.
    pub fn document(&self, file: &FileLocation) -> Result<Option<(&[u8], u64)>> {
        if file.file_reference != [self.reference_epoch] {
            return Err(crate::FileReferenceExpired.into());
        }
        Ok(match self.messages.get(&file.id) {
            Some(Message::Doc { blob, size, .. }) => Some((blob, *size)),
            _ => None,
        })
    }

    pub fn ids(&self) -> Vec<i64> {
        self.messages.keys().copied().collect()
    }

    /// Ids and names of the messages (`(other)`, a text's first line).
    pub fn names(&self) -> Vec<(i64, String)> {
        self.messages
            .iter()
            .map(|(id, m)| match m {
                Message::Other => (*id, "(other)".to_string()),
                Message::Text(t) => (*id, t.lines().next().unwrap_or_default().to_string()),
                Message::Doc { name, .. } => (*id, name.clone()),
            })
            .collect()
    }
}

/// What Telegram refuses before a call touches the channel.
pub fn check_ids(n: usize, call: &str) -> Result<()> {
    if n > IDS_PER_CALL {
        bail!("{call} with {n} ids");
    }
    Ok(())
}

pub fn check_text(text: &str) -> Result<()> {
    if text.is_empty() || text.chars().count() > TEXT_MAX_CHARS {
        bail!("MESSAGE_TOO_LONG / MESSAGE_EMPTY: {} chars", text.chars().count());
    }
    Ok(())
}

pub fn check_window(offset: u64, limit: u64) -> Result<()> {
    if !offset.is_multiple_of(4096) || !limit.is_multiple_of(4096) || limit == 0 || limit > WINDOW {
        bail!("LIMIT_INVALID: offset {offset} limit {limit}");
    }
    if offset >> 20 != (offset + limit - 1) >> 20 {
        bail!("LIMIT_INVALID: window {offset}+{limit} crosses a 1 MiB boundary");
    }
    Ok(())
}
