//! Telegram and Discord stores on fake channels (`fake = …` on a store; the
//! feature `fake`): channels this process keeps (pigeonhole-fake-channels),
//! opened as the site's demo opens them. The site's recordings run on them,
//! and so can a node to play with. Without the feature such a config is
//! refused.

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
    use pigeonhole_fake_channels::Fakes;
    use std::sync::OnceLock;

    /// The process's fake channels: every store on one shares them.
    static FAKES: OnceLock<Fakes> = OnceLock::new();

    fn fakes() -> &'static Fakes {
        FAKES.get_or_init(Fakes::default)
    }

    pub async fn log(cfg: &Config, f: &FakeChannel) -> anyhow::Result<Arc<dyn LogStore>> {
        fakes().log(cfg, f).await
    }

    pub async fn snapshots(cfg: &Config, f: &FakeChannel) -> anyhow::Result<Arc<dyn SnapshotStore>> {
        fakes().snapshots(cfg, f).await
    }

    pub async fn member(cfg: &Config, inst: &InstanceConfig, f: &FakeChannel) -> anyhow::Result<SharedBackend> {
        fakes().member(cfg, inst, f).await
    }

    pub fn view(stores: &[StoreRef], key: Option<[u8; 32]>) -> Option<Arc<dyn Channels>> {
        Some(fakes().view(stores, key))
    }

    /// Stops the fake channels' tablets, as the process exits.
    pub async fn close() -> anyhow::Result<()> {
        match FAKES.get() {
            Some(fakes) => fakes.0.close().await,
            None => Ok(()),
        }
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

    pub fn view(_stores: &[StoreRef], _key: Option<[u8; 32]>) -> Option<Arc<dyn Channels>> {
        None
    }

    pub async fn close() -> anyhow::Result<()> {
        Ok(())
    }
}

pub use on::{close, log, member, snapshots, view};
