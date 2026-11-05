//! A fake channel as one bot sees it: every call first takes the bot's
//! budget from its limiter, as `MtprotoChannel`'s calls do, so a fake
//! channel is paced like Telegram and several bots go faster than one
//! ([`crate::pool::PooledChannel`]).
//!
//! A document's location is the bot's that read or posted it, as on
//! Telegram: another bot's download with it answers `FILE_REFERENCE_EXPIRED`
//! (the bot's number rides on the file reference). Tests can make the next
//! sends answer a flood wait ([`FakeBot::flood_next`]). Each call is
//! logged as it starts and ends, and takes the store's latency between
//! ([`FakeCalls`]: the site's demo draws them).

use super::Calls;
use crate::channel::{ChannelApi, ChannelMessage, FileLocation, Sent};
use crate::limits::{DELETE, READ, SEND};
use crate::FileReferenceExpired;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::{ChatLimiter, Level, Spend};
use pigeonhole_storage_api::fake::FakeCalls;
use pigeonhole_storage_api::RateLimited;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Channel `C` through bot number `bot` (0: the primary).
pub struct FakeBot<C> {
    channel: Arc<C>,
    limiter: Arc<ChatLimiter>,
    bot: usize,
    /// While set, every call fails: the channel is down (the site's demo).
    down: Arc<AtomicBool>,
    calls: FakeCalls,
    knobs: Mutex<Knobs>,
}

#[derive(Default)]
struct Knobs {
    calls: Calls,
    /// Sends left to answer a flood wait, and the wait.
    floods: Option<(u32, Duration)>,
}

impl<C> FakeBot<C> {
    pub fn new(channel: Arc<C>, limiter: Arc<ChatLimiter>, bot: usize) -> Self {
        Self { channel, limiter, bot, down: Arc::default(), calls: FakeCalls::default(), knobs: Mutex::default() }
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

    /// The channel as no bot in particular sees it.
    pub fn channel(&self) -> &Arc<C> {
        &self.channel
    }

    /// This bot's calls that went through (not refused by a flood wait).
    pub fn calls(&self) -> Calls {
        self.knobs.lock().unwrap().calls
    }

    /// The next `sends` sends answer `FLOOD_WAIT` of `wait`: nothing is
    /// posted, the bot's send budget in the channel is cooled, and the call
    /// gives way ([`RateLimited`]), as a bot in a pool does.
    pub fn flood_next(&self, sends: u32, wait: Duration) {
        self.knobs.lock().unwrap().floods = Some((sends, wait));
    }

    /// Call `call` (`bytes` its payload, when known): the bot's budget,
    /// then the call's time; a channel that is down fails it after that.
    async fn spend(&self, spend: Spend, call: &'static str, bytes: Option<u64>) -> Result<()> {
        if !self.down.load(Ordering::SeqCst) {
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

    /// A send's budget, and a flood wait if one is due.
    async fn send(&self, call: &'static str, bytes: u64) -> Result<()> {
        self.spend(Spend::Class(SEND), call, Some(bytes)).await?;
        let mut k = self.knobs.lock().unwrap();
        if let Some((left, wait)) = k.floods.filter(|(left, _)| *left > 0) {
            k.floods = Some((left - 1, wait));
            self.limiter.cool(Spend::Class(SEND), Level::Class, wait);
            return Err(RateLimited { method: call, retry_after: wait }.into());
        }
        Ok(())
    }

    fn count(&self, f: impl FnOnce(&mut Calls)) {
        f(&mut self.knobs.lock().unwrap().calls);
    }

    /// `file` as this bot holds it.
    fn ours(&self, mut file: FileLocation) -> FileLocation {
        file.file_reference.push(self.bot as u8);
        file
    }
}

#[async_trait]
impl<C: ChannelApi> ChannelApi for FakeBot<C> {
    fn channel_id(&self) -> i64 {
        self.channel.channel_id()
    }

    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        self.spend(Spend::Class(READ), "get_messages", None).await?;
        self.count(|c| c.get_messages += 1);
        let msgs = self.channel.get_messages(ids).await?;
        Ok(msgs
            .into_iter()
            .map(|m| match m {
                ChannelMessage::Document { name, size, file } => {
                    ChannelMessage::Document { name, size, file: self.ours(file) }
                }
                other => other,
            })
            .collect())
    }

    async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
        self.send("send_document", data.len() as u64).await?;
        self.count(|c| c.send_document += 1);
        let sent = self.channel.send_document(name, data, random_id).await?;
        Ok(Sent { file: sent.file.map(|f| self.ours(f)), ..sent })
    }

    async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
        self.send("send_text", text.len() as u64).await?;
        self.count(|c| c.send_text += 1);
        self.channel.send_text(text, random_id).await
    }

    /// Only with this bot's own location.
    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
        self.spend(Spend::Bot, "get_file", Some(limit)).await?;
        let mut theirs = file.clone();
        if theirs.file_reference.pop() != Some(self.bot as u8) {
            return Err(FileReferenceExpired.into());
        }
        self.count(|c| c.get_file += 1);
        self.channel.get_file(&theirs, offset, limit).await
    }

    async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
        self.spend(Spend::Class(DELETE), "delete_messages", None).await?;
        self.count(|c| c.delete_messages += 1);
        self.channel.delete_messages(ids).await
    }

    async fn pinned(&self) -> Result<Option<i64>> {
        self.spend(Spend::Class(READ), "pinned", None).await?;
        self.channel.pinned().await
    }

    async fn pin(&self, id: i64) -> Result<()> {
        self.spend(Spend::Class(SEND), "pin", None).await?;
        self.count(|c| c.pin += 1);
        self.channel.pin(id).await
    }
}
