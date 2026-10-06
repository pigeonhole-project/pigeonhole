//! Pluggable blob storage. Production uses Telegram; tests use in-memory.

use crate::telegram::TelegramClient;

pub use crate::telegram::DeleteOutcome;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

/// Backend that stores object chunk bytes (Telegram documents or a test double).
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Upload bytes → (`file_id`, `message_id`).
    async fn put(
        &self,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)>;

    /// Re-post an existing `file_id` (Telegram: sendDocument by id; no byte transfer).
    async fn put_by_file_id(&self, file_id: &str, caption: &str) -> Result<(String, i64)>;

    async fn get(&self, file_id: &str) -> Result<Bytes>;

    async fn delete_message(&self, message_id: i64) -> Result<DeleteOutcome>;
}

/// Production adapter: all blobs go to a single Telegram chat.
pub struct TelegramBlobStore {
    tg: TelegramClient,
    chat_id: String,
}

impl TelegramBlobStore {
    pub fn new(tg: TelegramClient, chat_id: String) -> Self {
        Self { tg, chat_id }
    }

    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    pub fn client(&self) -> &TelegramClient {
        &self.tg
    }
}

#[async_trait]
impl BlobStore for TelegramBlobStore {
    async fn put(
        &self,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)> {
        self.tg
            .send_document(&self.chat_id, data, filename, caption)
            .await
    }

    async fn put_by_file_id(&self, file_id: &str, caption: &str) -> Result<(String, i64)> {
        self.tg
            .send_document_by_file_id(&self.chat_id, file_id, caption)
            .await
    }

    async fn get(&self, file_id: &str) -> Result<Bytes> {
        self.tg.download_file(file_id).await
    }

    async fn delete_message(&self, message_id: i64) -> Result<DeleteOutcome> {
        self.tg.delete_message(&self.chat_id, message_id).await
    }
}

/// In-memory store for unit/integration tests (no network).
pub struct MemoryBlobStore {
    next_msg: AtomicI64,
    next_fid: AtomicU64,
    files: Mutex<HashMap<String, Bytes>>,
    /// message_id → file_id
    messages: Mutex<HashMap<i64, String>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self {
            next_msg: AtomicI64::new(1),
            next_fid: AtomicU64::new(1),
            files: Mutex::new(HashMap::new()),
            messages: Mutex::new(HashMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.files.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for MemoryBlobStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put(
        &self,
        data: Bytes,
        _filename: &str,
        _caption: &str,
    ) -> Result<(String, i64)> {
        let file_id = format!("mem-{}", self.next_fid.fetch_add(1, Ordering::Relaxed));
        let message_id = self.next_msg.fetch_add(1, Ordering::Relaxed);
        self.files.lock().unwrap().insert(file_id.clone(), data);
        self.messages
            .lock()
            .unwrap()
            .insert(message_id, file_id.clone());
        Ok((file_id, message_id))
    }

    async fn put_by_file_id(&self, file_id: &str, _caption: &str) -> Result<(String, i64)> {
        let _data = self
            .files
            .lock()
            .unwrap()
            .get(file_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown file_id {file_id}"))?;
        let message_id = self.next_msg.fetch_add(1, Ordering::Relaxed);
        self.messages
            .lock()
            .unwrap()
            .insert(message_id, file_id.to_string());
        Ok((file_id.to_string(), message_id))
    }

    async fn get(&self, file_id: &str) -> Result<Bytes> {
        self.files
            .lock()
            .unwrap()
            .get(file_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown file_id {file_id}"))
    }

    async fn delete_message(&self, message_id: i64) -> Result<DeleteOutcome> {
        let mut messages = self.messages.lock().unwrap();
        let Some(file_id) = messages.remove(&message_id) else {
            return Ok(DeleteOutcome::Gone);
        };
        let still_used = messages.values().any(|f| f == &file_id);
        drop(messages);
        if !still_used {
            self.files.lock().unwrap().remove(&file_id);
        }
        Ok(DeleteOutcome::Deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_put_get_delete() {
        let store = MemoryBlobStore::new();
        let (fid, mid) = store
            .put(Bytes::from_static(b"hello"), "a.bin", "")
            .await
            .unwrap();
        assert_eq!(store.get(&fid).await.unwrap().as_ref(), b"hello");
        assert_eq!(
            store.delete_message(mid).await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(store.get(&fid).await.is_err());
        assert_eq!(
            store.delete_message(mid).await.unwrap(),
            DeleteOutcome::Gone
        );
    }
}
