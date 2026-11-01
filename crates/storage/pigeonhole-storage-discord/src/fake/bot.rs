//! A fake channel as one bot sees it: every API call first takes the bot's
//! budget from its limiter, as `DiscordChannel`'s calls do, so a fake
//! channel is paced like Discord and several bots go faster than one
//! ([`crate::pool::PooledDiscord`]). CDN downloads are not API calls.

use crate::limits::{DELETE, READ, SEND};
use crate::{DcMessage, DiscordApi, ThreadStart};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::{ChatLimiter, Spend};
use std::sync::Arc;

/// Channel `A` through bot number `bot` (0: the primary).
pub struct FakeBot<A> {
    api: Arc<A>,
    limiter: Arc<ChatLimiter>,
    bot: usize,
}

impl<A> FakeBot<A> {
    pub fn new(api: Arc<A>, limiter: Arc<ChatLimiter>, bot: usize) -> Self {
        Self { api, limiter, bot }
    }

    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        &self.limiter
    }

    async fn spend(&self, spend: Spend, call: &'static str) {
        self.limiter.acquire(spend).await;
        tracing::debug!(bot = self.bot, call, "fake bot call");
    }
}

#[async_trait]
impl<A: DiscordApi> DiscordApi for FakeBot<A> {
    fn channel_id(&self) -> u64 {
        self.api.channel_id()
    }

    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
        self.spend(Spend::Class(SEND), "send").await;
        self.api.send(content, file, nonce).await
    }

    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
        self.spend(Spend::Class(READ), "get_message").await;
        self.api.get_message(id).await
    }

    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
        self.spend(Spend::Class(READ), "list").await;
        self.api.list(before, after, limit).await
    }

    async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
        self.spend(Spend::Class(DELETE), "delete_messages").await;
        self.api.delete_messages(ids).await
    }

    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
        self.spend(Spend::Class(SEND), "start_thread").await;
        self.api.start_thread(message_id, name).await
    }

    async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
        self.spend(Spend::Class(READ), "thread_name").await;
        self.api.thread_name(message_id).await
    }

    async fn archive_thread(&self, message_id: u64) -> Result<()> {
        self.spend(Spend::Class(DELETE), "archive_thread").await;
        self.api.archive_thread(message_id).await
    }

    async fn delete_thread(&self, message_id: u64) -> Result<()> {
        self.spend(Spend::Class(DELETE), "delete_thread").await;
        self.api.delete_thread(message_id).await
    }

    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        self.api.download(url, range).await
    }
}
