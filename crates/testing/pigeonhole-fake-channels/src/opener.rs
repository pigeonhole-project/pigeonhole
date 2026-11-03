//! A node's stores on the fake channels of a [`World`]: the opener its
//! assembly (pigeonhole-node) asks for every telegram or discord store,
//! the same in the site's demo and in the binary's fake-channel build.

use crate::world::meta_info;
use crate::{FakeView, World};
use anyhow::{bail, Result};
use async_trait::async_trait;
use pigeonhole_admin::{Channels, StoreRef};
use pigeonhole_blob::{erase, SharedBackend};
use pigeonhole_config::instances::{InstanceConfig, InstanceKind};
use pigeonhole_config::stores::FakeChannel;
use pigeonhole_config::{Config, LogSettings, SnapshotSettings};
use pigeonhole_meta::{LogStore, SnapshotStore};
use pigeonhole_node::{chain_log, leased_log, snapshot_store, Opener};
use std::sync::Arc;

/// Opens every channel of a node as a fake in `world`; a real one is refused.
#[derive(Clone, Default)]
pub struct Fakes(pub Arc<World>);

fn no_real_channel(what: &str) -> anyhow::Error {
    anyhow::anyhow!("{what}: real Telegram and Discord channels are not reachable from here: add `fake = \"memory\"`")
}

impl Fakes {
    /// The log in fake channel `f` (a Discord one held by `[meta] lease`).
    pub async fn log(&self, cfg: &Config, f: &FakeChannel) -> Result<Arc<dyn LogStore>> {
        Ok(match f.kind {
            InstanceKind::Telegram => chain_log(Arc::new(self.0.telegram_store(cfg, f, meta_info(f)).await?)),
            InstanceKind::Discord => leased_log(Arc::new(self.0.discord_store(cfg, f, None, meta_info(f)).await?), cfg),
            _ => bail!("{}: a fake channel is telegram or discord", f.id),
        })
    }

    /// The snapshots in fake channel `f`.
    pub async fn snapshots(&self, cfg: &Config, f: &FakeChannel) -> Result<Arc<dyn SnapshotStore>> {
        Ok(match f.kind {
            InstanceKind::Telegram => snapshot_store(Arc::new(self.0.telegram_store(cfg, f, meta_info(f)).await?)),
            InstanceKind::Discord => snapshot_store(Arc::new(self.0.discord_store(cfg, f, None, meta_info(f)).await?)),
            _ => bail!("{}: a fake channel is telegram or discord", f.id),
        })
    }

    /// Blob store `inst` on its fake channel `f`.
    pub async fn member(&self, cfg: &Config, inst: &InstanceConfig, f: &FakeChannel) -> Result<SharedBackend> {
        let info = inst.info.clone();
        Ok(match f.kind {
            InstanceKind::Telegram => Arc::new(erase(Arc::new(self.0.telegram_store(cfg, f, info).await?))),
            InstanceKind::Discord => {
                Arc::new(erase(Arc::new(self.0.discord_store(cfg, f, inst.max_blob_size, info).await?)))
            }
            _ => bail!("store {:?}: a fake channel is telegram or discord", inst.info.id),
        })
    }

    /// The fake channels among `stores`, as the admin's UI draws them.
    pub fn view(&self, stores: &[StoreRef], key: Option<[u8; 32]>) -> Arc<dyn Channels> {
        Arc::new(FakeView::new(self.0.clone(), stores, key))
    }
}

#[async_trait]
impl Opener for Fakes {
    async fn log(&self, cfg: &Config, place: &LogSettings) -> Result<Arc<dyn LogStore>> {
        match place {
            LogSettings::Fake(f) => Fakes::log(self, cfg, f).await,
            _ => Err(no_real_channel("[meta] log")),
        }
    }

    async fn snapshots(&self, cfg: &Config, place: &SnapshotSettings) -> Result<Arc<dyn SnapshotStore>> {
        match place {
            SnapshotSettings::Fake(f) => Fakes::snapshots(self, cfg, f).await,
            _ => Err(no_real_channel("[meta] snapshot")),
        }
    }

    async fn member(&self, cfg: &Config, inst: &InstanceConfig) -> Result<SharedBackend> {
        match &inst.fake {
            Some(f) => Fakes::member(self, cfg, inst, f).await,
            None => Err(no_real_channel(&format!("store {:?}", inst.info.id))),
        }
    }

    fn view(&self, stores: &[StoreRef], key: Option<[u8; 32]>) -> Option<Arc<dyn Channels>> {
        Some(Fakes::view(self, stores, key))
    }
}
