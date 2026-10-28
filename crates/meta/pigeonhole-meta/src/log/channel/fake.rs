//! An in-memory channel for tests of the channel LogStore and snapshots.

use super::{ChannelApi, ChannelMsg, PAGE};
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// A channel as MTProto shows it to a bot: ids grow by one per message,
/// any message can be read by id, deleted or never-posted ids are empty.
#[derive(Default)]
pub(crate) struct FakeChannel {
    pub(crate) inner: Mutex<FakeInner>,
}

#[derive(Default)]
pub(crate) struct FakeInner {
    top: i64,
    /// message id → (file name, bytes); `None` for a text message.
    pub(crate) messages: BTreeMap<i64, Option<(String, Vec<u8>)>>,
    /// message id → reads it stays invisible for.
    hidden: HashMap<i64, u32>,
    /// The next send is invisible for this many reads.
    hide_next: Option<u32>,
    /// The next send posts but answers with an error.
    lose_next_answer: bool,
    pub(crate) reads: usize,
}

impl FakeChannel {
    pub(crate) fn post_text(&self) -> i64 {
        let mut g = self.inner.lock().unwrap();
        g.top += 1;
        let id = g.top;
        g.messages.insert(id, None);
        id
    }

    pub(crate) fn post_document(&self, name: &str, data: &[u8]) -> i64 {
        let mut g = self.inner.lock().unwrap();
        g.top += 1;
        let id = g.top;
        g.messages.insert(id, Some((name.into(), data.to_vec())));
        id
    }

    /// The next message posted is invisible for `reads` reads that include it.
    pub(crate) fn hide_next(&self, reads: u32) {
        self.inner.lock().unwrap().hide_next = Some(reads);
    }

    /// The next send posts, but the sender learns nothing.
    pub(crate) fn lose_next_answer(&self) {
        self.inner.lock().unwrap().lose_next_answer = true;
    }

    pub(crate) fn names(&self) -> Vec<(i64, String)> {
        let g = self.inner.lock().unwrap();
        g.messages.iter().map(|(id, m)| (*id, m.as_ref().map_or("(text)".into(), |(n, _)| n.clone()))).collect()
    }

    pub(crate) fn reads(&self) -> usize {
        self.inner.lock().unwrap().reads
    }

    /// Replace the bytes of message `id` in place (an edit).
    pub(crate) fn edit(&self, id: i64) {
        if let Some(Some((_, data))) = self.inner.lock().unwrap().messages.get_mut(&id) {
            data.push(0);
        }
    }
}

#[async_trait]
impl ChannelApi for FakeChannel {
    async fn send_document(&self, name: &str, data: Vec<u8>) -> anyhow::Result<i64> {
        let mut g = self.inner.lock().unwrap();
        g.top += 1;
        let id = g.top;
        g.messages.insert(id, Some((name.into(), data)));
        if let Some(reads) = g.hide_next.take() {
            g.hidden.insert(id, reads);
        }
        if std::mem::take(&mut g.lose_next_answer) {
            bail!("connection reset before the answer");
        }
        Ok(id)
    }

    async fn get_messages(&self, ids: &[i64]) -> anyhow::Result<Vec<ChannelMsg>> {
        if ids.len() > PAGE {
            bail!("getMessages with {} ids", ids.len());
        }
        let mut g = self.inner.lock().unwrap();
        g.reads += 1;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(left) = g.hidden.get_mut(id) {
                if *left > 0 {
                    *left -= 1;
                    out.push(ChannelMsg::Empty);
                    continue;
                }
            }
            out.push(match g.messages.get(id) {
                None => ChannelMsg::Empty,
                Some(None) => ChannelMsg::Other,
                Some(Some((name, _))) => ChannelMsg::Doc { name: name.clone() },
            });
        }
        Ok(out)
    }

    async fn download(&self, message_id: i64) -> anyhow::Result<Vec<u8>> {
        let g = self.inner.lock().unwrap();
        match g.messages.get(&message_id) {
            Some(Some((_, data))) => Ok(data.clone()),
            _ => Err(anyhow!("no document {message_id}")),
        }
    }

    async fn delete(&self, message_ids: &[i64]) -> anyhow::Result<()> {
        if message_ids.len() > PAGE {
            bail!("deleteMessages with {} ids", message_ids.len());
        }
        let mut g = self.inner.lock().unwrap();
        for id in message_ids {
            g.messages.remove(id);
        }
        Ok(())
    }
}
