//! A fake channel as one bot sees it: every call first takes the bot's
//! budget from its limiter, as `MtprotoChannel`'s calls do, so a fake
//! channel is paced like Telegram and several bots go faster than one
//! ([`crate::pool::PooledChannel`]).

use crate::channel::{ChannelApi, ChannelMessage, FileLocation, Sent};
use crate::limits::{DELETE, READ, SEND};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::{ChatLimiter, Spend};
use std::sync::Arc;

/// Channel `C` through bot number `bot` (0: the primary).
pub struct FakeBot<C> {
    channel: Arc<C>,
    limiter: Arc<ChatLimiter>,
    bot: usize,
}

impl<C> FakeBot<C> {
    pub fn new(channel: Arc<C>, limiter: Arc<ChatLimiter>, bot: usize) -> Self {
        Self { channel, limiter, bot }
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
impl<C: ChannelApi> ChannelApi for FakeBot<C> {
    fn channel_id(&self) -> i64 {
        self.channel.channel_id()
    }

    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        self.spend(Spend::Class(READ), "get_messages").await;
        self.channel.get_messages(ids).await
    }

    async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
        self.spend(Spend::Class(SEND), "send_document").await;
        self.channel.send_document(name, data, random_id).await
    }

    async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
        self.spend(Spend::Class(SEND), "send_text").await;
        self.channel.send_text(text, random_id).await
    }

    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
        self.spend(Spend::Bot, "get_file").await;
        self.channel.get_file(file, offset, limit).await
    }

    async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
        self.spend(Spend::Class(DELETE), "delete_messages").await;
        self.channel.delete_messages(ids).await
    }
}
