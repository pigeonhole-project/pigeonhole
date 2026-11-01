//! Telegram and Discord stores on fake channels (`fake = …` on a store; the
//! feature `fake`): channels this process keeps (pigeonhole-fake-channels),
//! as the site's demo has them. The site's recordings run on them, and so
//! can a node to play with. Without the feature such a config is refused.

use pigeonhole::config::stores::FakeChannel;
use pigeonhole::config::Config;
use pigeonhole::instances::InstanceConfig;
use pigeonhole_admin::{Channels, StoreRef};
use pigeonhole_blob::SharedBackend;
use pigeonhole_meta::{LogStore, SnapshotStore};
use std::sync::Arc;

#[cfg(feature = "fake")]
mod on {
    use super::*;
    use pigeonhole::instances::InstanceKind;
    use pigeonhole_blob::{erase, MetricsBackend};
    use pigeonhole_fake_channels::world::meta_info;
    use pigeonhole_fake_channels::{FakeView, World};
    use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore};
    use std::sync::LazyLock;

    /// The process's fake channels: every store on one shares it.
    static WORLD: LazyLock<Arc<World>> = LazyLock::new(Default::default);

    pub async fn log(cfg: &Config, f: &FakeChannel) -> anyhow::Result<Arc<dyn LogStore>> {
        Ok(match f.kind {
            InstanceKind::Telegram => {
                Arc::new(ChainLogStore::new(Arc::new(WORLD.telegram_store(cfg, f, meta_info(f)).await?)))
            }
            InstanceKind::Discord => {
                let log = ChainLogStore::new(Arc::new(WORLD.discord_store(cfg, f, None, meta_info(f)).await?));
                Arc::new(match cfg.meta.lease {
                    Some(lease) => log.with_lease(pigeonhole_meta::Lease::new(lease)),
                    None => log,
                })
            }
            _ => anyhow::bail!("[meta] log: a fake channel is telegram or discord"),
        })
    }

    pub async fn snapshots(cfg: &Config, f: &FakeChannel) -> anyhow::Result<Arc<dyn SnapshotStore>> {
        Ok(match f.kind {
            InstanceKind::Telegram => {
                Arc::new(BlobSnapshotStore::new(Arc::new(WORLD.telegram_store(cfg, f, meta_info(f)).await?)))
            }
            InstanceKind::Discord => {
                Arc::new(BlobSnapshotStore::new(Arc::new(WORLD.discord_store(cfg, f, None, meta_info(f)).await?)))
            }
            _ => anyhow::bail!("[meta] snapshot: a fake channel is telegram or discord"),
        })
    }

    pub async fn member(cfg: &Config, inst: &InstanceConfig, f: &FakeChannel) -> anyhow::Result<SharedBackend> {
        let info = inst.info.clone();
        Ok(MetricsBackend::wrap(match f.kind {
            InstanceKind::Telegram => Arc::new(erase(Arc::new(WORLD.telegram_store(cfg, f, info).await?))),
            InstanceKind::Discord => {
                Arc::new(erase(Arc::new(WORLD.discord_store(cfg, f, inst.max_blob_size, info).await?)))
            }
            _ => anyhow::bail!("store {:?}: a fake channel is telegram or discord", inst.info.id),
        }))
    }

    pub fn view(stores: &[StoreRef]) -> Option<Arc<dyn Channels>> {
        Some(Arc::new(FakeView::new(WORLD.clone(), stores)))
    }
}

#[cfg(not(feature = "fake"))]
mod on {
    use super::*;

    fn refused(what: &str) -> anyhow::Error {
        anyhow::anyhow!("{what} is a fake channel: this build has no fake channels (`--features fake`)")
    }

    pub async fn log(_cfg: &Config, _f: &FakeChannel) -> anyhow::Result<Arc<dyn LogStore>> {
        Err(refused("[meta] log"))
    }

    pub async fn snapshots(_cfg: &Config, _f: &FakeChannel) -> anyhow::Result<Arc<dyn SnapshotStore>> {
        Err(refused("[meta] snapshot"))
    }

    pub async fn member(_cfg: &Config, inst: &InstanceConfig, _f: &FakeChannel) -> anyhow::Result<SharedBackend> {
        Err(refused(&format!("store {:?}", inst.info.id)))
    }

    pub fn view(_stores: &[StoreRef]) -> Option<Arc<dyn Channels>> {
        None
    }
}

pub use on::{log, member, snapshots, view};
