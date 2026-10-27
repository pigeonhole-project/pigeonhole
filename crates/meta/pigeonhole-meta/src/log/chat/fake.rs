//! An in-memory chat for tests of the chat LogStore and the tablet over it.

use super::{ChatApi, ChatDoc, Registers};
use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

/// A chat as the Bot API shows it: message ids grow, the pinned message is the
/// newest pinned one by id (sending date), the description is a register.
#[derive(Default)]
pub(crate) struct FakeChat {
    pub(crate) inner: Mutex<FakeInner>,
}

#[derive(Default)]
pub(crate) struct FakeInner {
    next_id: i64,
    /// message id → (file id, bytes)
    pub(crate) docs: BTreeMap<i64, (String, Vec<u8>)>,
    texts: BTreeSet<i64>,
    pinned: BTreeSet<i64>,
    description: Option<String>,
}

impl FakeChat {
    pub(crate) fn post_text_and_pin(&self) {
        let mut g = self.inner.lock().unwrap();
        g.next_id += 1;
        let id = g.next_id;
        g.texts.insert(id);
        g.pinned.insert(id);
    }

    pub(crate) fn set_raw_description(&self, text: &str) {
        self.inner.lock().unwrap().description = Some(text.into());
    }
}

#[async_trait]
impl ChatApi for FakeChat {
    async fn send_document(&self, _name: &str, data: Vec<u8>) -> anyhow::Result<ChatDoc> {
        let mut g = self.inner.lock().unwrap();
        g.next_id += 1;
        let id = g.next_id;
        let download_ref = format!("ref-{id}");
        g.docs.insert(id, (download_ref.clone(), data));
        Ok(ChatDoc { message_id: id, download_ref })
    }

    async fn pin(&self, message_id: i64) -> anyhow::Result<()> {
        self.inner.lock().unwrap().pinned.insert(message_id);
        Ok(())
    }

    async fn registers(&self) -> anyhow::Result<Registers> {
        let g = self.inner.lock().unwrap();
        let top = g.pinned.iter().next_back().copied();
        let pinned = top.and_then(|id| g.docs.get(&id).map(|(f, _)| ChatDoc { message_id: id, download_ref: f.clone() }));
        Ok(Registers {
            description: g.description.clone(),
            pinned_other: top.is_some() && pinned.is_none(),
            pinned,
        })
    }

    async fn set_description(&self, text: &str) -> anyhow::Result<bool> {
        let mut g = self.inner.lock().unwrap();
        if g.description.as_deref() == Some(text) {
            return Ok(false);
        }
        g.description = Some(text.into());
        Ok(true)
    }

    async fn download(&self, download_ref: &str) -> anyhow::Result<Vec<u8>> {
        let g = self.inner.lock().unwrap();
        g.docs
            .values()
            .find(|(f, _)| f == download_ref)
            .map(|(_, d)| d.clone())
            .ok_or_else(|| anyhow!("no file {download_ref}"))
    }

    async fn delete(&self, message_ids: &[i64]) -> anyhow::Result<()> {
        let mut g = self.inner.lock().unwrap();
        for id in message_ids {
            g.docs.remove(id);
            g.pinned.remove(id);
        }
        Ok(())
    }
}

