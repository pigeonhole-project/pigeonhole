//! The fake channels of a process: each opened once and shared by every
//! store that posts to it (a restart over the same [`World`] finds them as
//! they were), with a down switch per channel, and the Telegram and Discord
//! stores built on them through fake bots, as a server's on real ones.

use crate::{DiscordChannel, Stores, TelegramChannel};
use anyhow::{bail, Context, Result};
use pigeonhole_config::instances::InstanceKind;
use pigeonhole_config::stores::{FakeBacking, FakeChannel};
use pigeonhole_config::Config;
use pigeonhole_storage_api::fake::FakeHost;
use pigeonhole_storage_api::InstanceInfo;
use pigeonhole_storage_discord::fake::bot::FakeBot as DiscordBot;
use pigeonhole_storage_discord::fake::state::DiscordChannelState;
use pigeonhole_storage_discord::pool::PooledDiscord;
use pigeonhole_storage_discord::DiscordBlobStore;
use pigeonhole_storage_telegram::fake::bot::FakeBot as TelegramBot;
use pigeonhole_storage_telegram::fake::state::TelegramChannelState;
use pigeonhole_storage_telegram::pool::PooledChannel;
use pigeonhole_storage_telegram::TelegramBlobStore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub type TelegramStore = TelegramBlobStore<PooledChannel<TelegramBot<TelegramChannel>>>;
pub type DiscordStore = DiscordBlobStore<PooledDiscord<DiscordBot<DiscordChannel>>>;

#[derive(Default)]
pub struct World {
    telegram: Mutex<HashMap<(i64, String), Arc<TelegramChannel>>>,
    discord: Mutex<HashMap<(u64, String), Arc<DiscordChannel>>>,
    /// Channels taken down (`kind:scope`): every bot's call fails.
    outages: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

/// The stores a fake channel's tablet keeps itself in.
fn channel_stores(backing: &FakeBacking) -> Result<Stores> {
    match backing {
        FakeBacking::Memory => Ok(Stores::memory()),
        FakeBacking::Dir(dir) => Stores::in_dir(dir),
        FakeBacking::Store(id) => bail!("fake = {id:?} was not resolved"),
    }
}

/// Fake bot `i`'s name: its budget is shared by every channel it posts to.
fn fake_bot(i: usize) -> String {
    format!("fake-bot-{}", i + 1)
}

/// The store of the log or the snapshots in fake channel `f`, by its id.
pub fn meta_info(f: &FakeChannel) -> InstanceInfo {
    let location = format!("fake:{}:{}", f.kind.as_str(), f.scope);
    InstanceInfo {
        id: f.id.clone(),
        kind: f.kind.as_str(),
        fingerprint: location.clone(),
        location,
        role: pigeonhole_storage_api::InstanceRole::ReadWrite,
    }
}

impl World {
    pub async fn telegram(&self, chat: &str, backing: &FakeBacking) -> Result<Arc<TelegramChannel>> {
        let key = (chat.parse().with_context(|| format!("chat id {chat:?}"))?, format!("{backing:?}"));
        if let Some(c) = self.telegram.lock().unwrap().get(&key) {
            return Ok(c.clone());
        }
        let channel = Arc::new(crate::telegram(key.0, channel_stores(backing)?).await?);
        self.telegram.lock().unwrap().insert(key, channel.clone());
        Ok(channel)
    }

    pub async fn discord(&self, channel: &str, backing: &FakeBacking) -> Result<Arc<DiscordChannel>> {
        let key = (channel.parse().with_context(|| format!("channel id {channel:?}"))?, format!("{backing:?}"));
        if let Some(c) = self.discord.lock().unwrap().get(&key) {
            return Ok(c.clone());
        }
        let ch = Arc::new(crate::discord(key.0, channel_stores(backing)?).await?);
        self.discord.lock().unwrap().insert(key, ch.clone());
        Ok(ch)
    }

    /// The down switch of fake channel `scope`, shared by its bots.
    fn outage(&self, kind: InstanceKind, scope: &str) -> Arc<AtomicBool> {
        let key = format!("{}:{scope}", kind.as_str());
        self.outages.lock().unwrap().entry(key).or_default().clone()
    }

    /// Takes channel `f` down (every call fails) or brings it back.
    pub fn set_down(&self, f: &FakeChannel, down: bool) {
        self.outage(f.kind, &f.scope).store(down, Ordering::SeqCst);
    }

    pub fn is_down(&self, f: &FakeChannel) -> bool {
        self.outage(f.kind, &f.scope).load(Ordering::SeqCst)
    }

    /// Channel `f` loses message `id` (someone deleted it in the app).
    pub async fn delete_message(&self, f: &FakeChannel, id: &str) -> Result<()> {
        match f.kind {
            InstanceKind::Telegram => {
                use pigeonhole_storage_telegram::ChannelApi;
                let id = id.parse().with_context(|| format!("message id {id:?}"))?;
                self.telegram(&f.scope, &f.backing).await?.delete_messages(&[id]).await
            }
            InstanceKind::Discord => {
                use pigeonhole_storage_discord::DiscordApi;
                let id = id.parse().with_context(|| format!("message id {id:?}"))?;
                self.discord(&f.scope, &f.backing).await?.delete_messages(&[id]).await
            }
            _ => bail!("{}: not a fake channel", f.id),
        }
    }

    /// Channel `f` loses every message (an admin cleared it); how many.
    pub async fn wipe(&self, f: &FakeChannel) -> Result<usize> {
        match f.kind {
            InstanceKind::Telegram => {
                use pigeonhole_storage_telegram::ChannelApi;
                let ch = self.telegram(&f.scope, &f.backing).await?;
                let ids = ch.host().read(|s: &TelegramChannelState| s.ids());
                for part in ids.chunks(100) {
                    ch.delete_messages(part).await?;
                }
                Ok(ids.len())
            }
            InstanceKind::Discord => {
                use pigeonhole_storage_discord::DiscordApi;
                let ch = self.discord(&f.scope, &f.backing).await?;
                let ids = ch.host().read(|s: &DiscordChannelState| s.ids());
                for part in ids.chunks(100) {
                    ch.delete_messages(part).await?;
                }
                Ok(ids.len())
            }
            _ => bail!("{}: not a fake channel", f.id),
        }
    }

    /// A Telegram store on fake channel `f`, through its bots: each with
    /// its own budget (`[telegram.send]` is per bot and chat), as a server's.
    pub async fn telegram_store(&self, cfg: &Config, f: &FakeChannel, info: InstanceInfo) -> Result<TelegramStore> {
        let channel = self.telegram(&f.scope, &f.backing).await?;
        let bots = (0..f.bots)
            .map(|i| {
                let limiter = cfg.chat_limiter(InstanceKind::Telegram, &fake_bot(i), &f.scope);
                (
                    TelegramBot::new(channel.clone(), limiter.clone(), i).with_switch(self.outage(f.kind, &f.scope)),
                    limiter,
                )
            })
            .collect();
        let pooled = PooledChannel::new(&f.scope, bots);
        let limiter = pooled.limiter().clone();
        Ok(TelegramBlobStore::new(Arc::new(pooled), limiter, info))
    }

    /// A Discord store on fake channel `f`, through its bots.
    pub async fn discord_store(
        &self,
        cfg: &Config,
        f: &FakeChannel,
        max_blob_size: Option<usize>,
        info: InstanceInfo,
    ) -> Result<DiscordStore> {
        let channel = self.discord(&f.scope, &f.backing).await?;
        let bots = (0..f.bots)
            .map(|i| {
                let limiter = cfg.chat_limiter(InstanceKind::Discord, &fake_bot(i), &f.scope);
                (
                    DiscordBot::new(channel.clone(), limiter.clone(), i).with_switch(self.outage(f.kind, &f.scope)),
                    limiter,
                )
            })
            .collect();
        let pooled = PooledDiscord::new(&f.scope, bots);
        let limiter = pooled.limiter().clone();
        Ok(DiscordBlobStore::new(Arc::new(pooled), limiter, max_blob_size, info))
    }

    /// Stops every fake channel's tablet (a process exit).
    pub async fn close(&self) -> Result<()> {
        let tg: Vec<_> = self.telegram.lock().unwrap().drain().map(|(_, c)| c).collect();
        let dc: Vec<_> = self.discord.lock().unwrap().drain().map(|(_, c)| c).collect();
        for c in tg {
            c.host().close().await?;
        }
        for c in dc {
            c.host().close().await?;
        }
        Ok(())
    }
}
