//! An in-memory channel for tests: ids grow, sends deduplicate by
//! `random_id`, `get_file` checks the window rules and file references.

use crate::channel::{ChannelApi, ChannelMessage, FileLocation, FileReferenceExpired, Sent, IDS_PER_CALL, WINDOW};

/// Longest text message the fake takes (Telegram: 4096).
pub const TEXT_MAX_CHARS: usize = 4096;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

#[derive(Debug, Clone)]
enum Stored {
    /// A foreign message that is neither text nor a document.
    Other,
    Text(String),
    Doc { name: String, data: Bytes },
}

#[derive(Default)]
struct Inner {
    top: i64,
    messages: BTreeMap<i64, Stored>,
    by_random_id: HashMap<i64, i64>,
    /// Bumped by [`FakeChannel::expire_file_references`].
    reference_epoch: u8,
    calls: Calls,
    /// message id → reads it stays invisible for.
    hidden: HashMap<i64, u32>,
    /// The next send whose name starts with this is invisible for that many reads.
    hide_next: Option<(String, u32)>,
    /// The next send posts but answers with an error.
    lose_next_answer: bool,
}

/// How often each call was made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Calls {
    pub get_messages: usize,
    pub send_document: usize,
    pub send_text: usize,
    pub get_file: usize,
    pub delete_messages: usize,
}

#[derive(Default)]
pub struct FakeChannel {
    id: i64,
    inner: Mutex<Inner>,
}

impl FakeChannel {
    pub fn new(channel_id: i64) -> Self {
        Self { id: channel_id, inner: Mutex::default() }
    }

    /// Someone else posts a message that is not ours (a service message).
    pub fn post_text(&self) -> i64 {
        let mut g = self.inner.lock().unwrap();
        g.top += 1;
        let id = g.top;
        g.messages.insert(id, Stored::Other);
        id
    }

    /// Someone else posts a document; returns its id.
    pub fn post_document(&self, name: &str, data: &[u8]) -> i64 {
        let mut g = self.inner.lock().unwrap();
        g.top += 1;
        let id = g.top;
        g.messages.insert(id, Stored::Doc { name: name.into(), data: Bytes::copy_from_slice(data) });
        id
    }

    /// Every file reference handed out so far stops working.
    pub fn expire_file_references(&self) {
        let mut g = self.inner.lock().unwrap();
        g.reference_epoch = g.reference_epoch.wrapping_add(1);
    }

    /// The next document posted whose name starts with `prefix` is invisible
    /// for `reads` reads that include it.
    pub fn hide_next(&self, prefix: &str, reads: u32) {
        self.inner.lock().unwrap().hide_next = Some((prefix.to_string(), reads));
    }

    /// The next send posts, but the sender learns nothing.
    pub fn lose_next_answer(&self) {
        self.inner.lock().unwrap().lose_next_answer = true;
    }

    /// Change the bytes of document `id` in place (an edit).
    pub fn edit(&self, id: i64) {
        match self.inner.lock().unwrap().messages.get_mut(&id) {
            Some(Stored::Doc { data, .. }) => {
                let mut changed = data.to_vec();
                if let Some(last) = changed.last_mut() {
                    *last ^= 0xff;
                }
                *data = Bytes::from(changed);
            }
            Some(Stored::Text(text)) => {
                // The first payload character changes its top bit (always a
                // data bit): still a valid encoding, other bytes.
                if let Some((head, payload)) = text.split_once('\n') {
                    let mut chars = payload.chars();
                    if let Some(first) = chars.next() {
                        let flipped = char::from_u32(first as u32 ^ (1 << 14)).unwrap_or(first);
                        *text = format!("{head}\n{flipped}{}", chars.as_str());
                    }
                }
            }
            _ => {}
        }
    }

    /// Delete message `id` behind everyone's back.
    pub fn remove(&self, id: i64) {
        self.inner.lock().unwrap().messages.remove(&id);
    }

    /// Ids and names of the messages still there (`(text)` for text).
    pub fn names(&self) -> Vec<(i64, String)> {
        let g = self.inner.lock().unwrap();
        g.messages
            .iter()
            .map(|(id, m)| match m {
                Stored::Other => (*id, "(other)".to_string()),
                Stored::Text(t) => (*id, t.lines().next().unwrap_or_default().to_string()),
                Stored::Doc { name, .. } => (*id, name.clone()),
            })
            .collect()
    }

    pub fn calls(&self) -> Calls {
        self.inner.lock().unwrap().calls
    }

    /// Ids of the messages still there.
    pub fn message_ids(&self) -> Vec<i64> {
        self.inner.lock().unwrap().messages.keys().copied().collect()
    }

    fn location(&self, g: &Inner, id: i64) -> FileLocation {
        FileLocation { id, access_hash: self.id, file_reference: vec![g.reference_epoch], dc_id: 2 }
    }
}

#[async_trait]
impl ChannelApi for FakeChannel {
    fn channel_id(&self) -> i64 {
        self.id
    }

    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        if ids.len() > IDS_PER_CALL {
            bail!("getMessages with {} ids", ids.len());
        }
        let mut g = self.inner.lock().unwrap();
        g.calls.get_messages += 1;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(left) = g.hidden.get_mut(id) {
                if *left > 0 {
                    *left -= 1;
                    out.push(ChannelMessage::Empty);
                    continue;
                }
            }
            out.push(match g.messages.get(id) {
                None => ChannelMessage::Empty,
                Some(Stored::Other) => ChannelMessage::Other,
                Some(Stored::Text(text)) => ChannelMessage::Text { text: text.clone() },
                Some(Stored::Doc { name, data }) => ChannelMessage::Document {
                    name: name.clone(),
                    size: data.len() as u64,
                    file: self.location(&g, *id),
                },
            });
        }
        Ok(out)
    }

    async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
        if data.is_empty() {
            bail!("FILE_PARTS_INVALID: empty document");
        }
        let mut g = self.inner.lock().unwrap();
        g.calls.send_document += 1;
        if let Some(&id) = g.by_random_id.get(&random_id) {
            return Ok(Sent { message_id: id, file: None });
        }
        g.top += 1;
        let id = g.top;
        g.messages.insert(id, Stored::Doc { name: name.into(), data });
        g.by_random_id.insert(random_id, id);
        if g.hide_next.as_ref().is_some_and(|(prefix, _)| name.starts_with(prefix.as_str())) {
            let (_, reads) = g.hide_next.take().unwrap();
            g.hidden.insert(id, reads);
        }
        if std::mem::take(&mut g.lose_next_answer) {
            bail!("connection reset before the answer");
        }
        Ok(Sent { message_id: id, file: Some(self.location(&g, id)) })
    }

    async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
        if text.is_empty() || text.chars().count() > TEXT_MAX_CHARS {
            bail!("MESSAGE_TOO_LONG / MESSAGE_EMPTY: {} chars", text.chars().count());
        }
        let mut g = self.inner.lock().unwrap();
        g.calls.send_text += 1;
        if let Some(&id) = g.by_random_id.get(&random_id) {
            return Ok(Sent { message_id: id, file: None });
        }
        g.top += 1;
        let id = g.top;
        g.messages.insert(id, Stored::Text(text.to_string()));
        g.by_random_id.insert(random_id, id);
        if g.hide_next.as_ref().is_some_and(|(prefix, _)| text.starts_with(prefix.as_str())) {
            let (_, reads) = g.hide_next.take().unwrap();
            g.hidden.insert(id, reads);
        }
        if std::mem::take(&mut g.lose_next_answer) {
            bail!("connection reset before the answer");
        }
        Ok(Sent { message_id: id, file: None })
    }

    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
        if offset % 4096 != 0 || limit % 4096 != 0 || limit == 0 || limit > WINDOW {
            bail!("LIMIT_INVALID: offset {offset} limit {limit}");
        }
        if offset >> 20 != (offset + limit - 1) >> 20 {
            bail!("LIMIT_INVALID: window {offset}+{limit} crosses a 1 MiB boundary");
        }
        let mut g = self.inner.lock().unwrap();
        g.calls.get_file += 1;
        if file.file_reference != [g.reference_epoch] {
            return Err(FileReferenceExpired.into());
        }
        match g.messages.get(&file.id) {
            Some(Stored::Doc { data, .. }) => {
                let start = (offset as usize).min(data.len());
                let end = (offset + limit).min(data.len() as u64) as usize;
                Ok(data.slice(start..end))
            }
            _ => bail!("FILE_ID_INVALID: no document {}", file.id),
        }
    }

    async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
        if ids.len() > IDS_PER_CALL {
            bail!("deleteMessages with {} ids", ids.len());
        }
        let mut g = self.inner.lock().unwrap();
        g.calls.delete_messages += 1;
        for id in ids {
            g.messages.remove(id);
        }
        Ok(())
    }
}
