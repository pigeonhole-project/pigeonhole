//! A fake channel as one bot sees it: every API call first takes the bot's
//! budget from its limiter, as `DiscordChannel`'s calls do, so a fake
//! channel is paced like Discord and several bots go faster than one
//! ([`crate::pool::PooledDiscord`]). CDN downloads take no budget. Tests
//! can make the next sends answer a 429 ([`FakeBot::flood_next`]).

use super::Calls;
use crate::limits::{DELETE, READ, SEND};
use crate::{DcMessage, DiscordApi, ThreadStart};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::{ChatLimiter, Level, Spend};
use pigeonhole_storage_api::fake::FakeCalls;
use pigeonhole_storage_api::RateLimited;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Channel `A` through one bot (its limiter: its budget).
pub struct FakeBot<A> {
    api: Arc<A>,
    limiter: Arc<ChatLimiter>,
    /// While set, every call fails: the channel is down (the site's demo).
    down: Arc<AtomicBool>,
    /// Each call logged as it starts and ends, the store's latency between.
    calls: FakeCalls,
    knobs: Mutex<Knobs>,
}

#[derive(Default)]
struct Knobs {
    calls: Calls,
    /// Sends left to answer a 429, and its wait.
    floods: Option<(u32, Duration)>,
}

impl<A> FakeBot<A> {
    pub fn new(api: Arc<A>, limiter: Arc<ChatLimiter>) -> Self {
        Self { api, limiter, down: Arc::default(), calls: FakeCalls::default(), knobs: Mutex::default() }
    }

    /// Logs and delays every call as `calls` says (its store, its latency).
    pub fn with_calls(self, calls: FakeCalls) -> Self {
        Self { calls, ..self }
    }

    /// Fails every call while `down` is set (shared by a channel's bots).
    pub fn with_switch(self, down: Arc<AtomicBool>) -> Self {
        Self { down, ..self }
    }

    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        &self.limiter
    }

    /// This bot's sends that went through (not refused by a 429).
    pub fn calls(&self) -> Calls {
        self.knobs.lock().unwrap().calls
    }

    /// The next `sends` sends answer 429 with `wait`: nothing is posted, the
    /// bot's send budget in the channel is cooled, and the call gives way
    /// ([`RateLimited`]), as a bot in a pool does.
    pub fn flood_next(&self, sends: u32, wait: Duration) {
        self.knobs.lock().unwrap().floods = Some((sends, wait));
    }

    /// Call `call` (`bytes` its payload, when known): the bot's budget
    /// (none: a CDN download), then the call's time; a channel that is
    /// down fails it after that.
    async fn spend(&self, spend: Option<Spend>, call: &'static str, bytes: Option<u64>) -> Result<()> {
        if let Some(spend) = spend.filter(|_| !self.down.load(Ordering::SeqCst)) {
            self.limiter.acquire(spend).await;
        }
        let id = self.calls.start(call, bytes).await;
        let up = !self.down.load(Ordering::SeqCst);
        self.calls.done(id, up);
        if !up {
            bail!("{call}: the channel is down");
        }
        Ok(())
    }
}

#[async_trait]
impl<A: DiscordApi> DiscordApi for FakeBot<A> {
    fn channel_id(&self) -> u64 {
        self.api.channel_id()
    }

    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
        self.spend(Some(Spend::Class(SEND)), "send", file.as_ref().map(|(_, b)| b.len() as u64)).await?;
        {
            let mut k = self.knobs.lock().unwrap();
            if let Some((left, wait)) = k.floods.filter(|(left, _)| *left > 0) {
                k.floods = Some((left - 1, wait));
                self.limiter.cool(Spend::Class(SEND), Level::Class, wait);
                return Err(RateLimited { method: "send message", retry_after: wait }.into());
            }
            k.calls.send += 1;
        }
        self.api.send(content, file, nonce).await
    }

    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
        self.spend(Some(Spend::Class(READ)), "get_message", None).await?;
        self.api.get_message(id).await
    }

    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
        self.spend(Some(Spend::Class(READ)), "list", None).await?;
        self.api.list(before, after, limit).await
    }

    async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
        self.spend(Some(Spend::Class(DELETE)), "delete_messages", None).await?;
        self.api.delete_messages(ids).await
    }

    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
        self.spend(Some(Spend::Class(SEND)), "start_thread", None).await?;
        self.api.start_thread(message_id, name).await
    }

    async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
        self.spend(Some(Spend::Class(READ)), "thread_name", None).await?;
        self.api.thread_name(message_id).await
    }

    async fn archive_thread(&self, message_id: u64) -> Result<()> {
        self.spend(Some(Spend::Class(DELETE)), "archive_thread", None).await?;
        self.api.archive_thread(message_id).await
    }

    async fn delete_thread(&self, message_id: u64) -> Result<()> {
        self.spend(Some(Spend::Class(DELETE)), "delete_thread", None).await?;
        self.api.delete_thread(message_id).await
    }

    /// Not an API call (no budget), but a channel that is down is down
    /// for its CDN too.
    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        self.spend(None, "download", range.map(|(from, to)| to.saturating_sub(from) + 1)).await?;
        self.api.download(url, range).await
    }

    async fn pinned(&self) -> Result<Vec<DcMessage>> {
        self.spend(Some(Spend::Class(READ)), "pinned", None).await?;
        self.api.pinned().await
    }

    async fn pin(&self, id: u64) -> Result<()> {
        self.spend(Some(Spend::Class(SEND)), "pin", None).await?;
        self.api.pin(id).await
    }
}
