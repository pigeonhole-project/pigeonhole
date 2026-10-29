//! Where bytes live (`[[store]]`) and how stores are combined: copies
//! (`[[replicas]]`) and parts (`[[shards]]`). `[meta] log` / `snapshot` and
//! `[chunk] store` refer to them by id.

use crate::instances::{
    discord_app_id_from_token, discord_fingerprint, discord_location, local_disk_location,
    memory_fingerprint, memory_location, telegram_location, telegram_fingerprint,
    InstanceConfig, TelegramLogin,
};
use crate::config::{PlacementConfig, ShardPlacement};
use anyhow::{bail, Context, Result};
use crate::instances::InstanceKind;
use pigeonhole_blob::{InstanceInfo, InstanceRole};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// One `[[store]]`, by `kind`; a field the kind does not take is an error.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum FileStore {
    LocalDisk {
        id: String,
        path: PathBuf,
    },
    Memory {
        id: String,
    },
    Telegram {
        id: String,
        chat_id: i64,
        bot_token_env: Option<String>,
        /// Env var with the app's api id (my.telegram.org).
        api_id_env: Option<String>,
        /// Env var with the app's api hash.
        api_hash_env: Option<String>,
        /// The MTProto session file; default `pigeonhole-tg-<bot id>.session`.
        /// One per bot, shared by its stores.
        session: Option<PathBuf>,
    },
    Discord {
        id: String,
        channel_id: u64,
        bot_token_env: Option<String>,
        #[serde(default, deserialize_with = "crate::config::units::opt_size")]
        max_blob_size: Option<usize>,
    },
}

impl FileStore {
    fn id(&self) -> &str {
        match self {
            FileStore::LocalDisk { id, .. }
            | FileStore::Memory { id }
            | FileStore::Telegram { id, .. }
            | FileStore::Discord { id, .. } => id.trim(),
        }
    }

    /// The store, with its secrets read from the environment.
    fn resolve(self) -> Result<Store> {
        let id = self.id().to_string();
        Ok(match self {
            FileStore::LocalDisk { path, .. } => Store::LocalDisk { path },
            FileStore::Memory { .. } => Store::Memory,
            FileStore::Telegram { chat_id, bot_token_env, api_id_env, api_hash_env, session, .. } => {
                let bot_token_env = bot_token_env.unwrap_or_else(|| "BOT_TOKEN".into());
                let bot_token = token(&id, &bot_token_env)?;
                let api_id_env = api_id_env.unwrap_or_else(|| "TG_API_ID".into());
                let api_id = token(&id, &api_id_env)?
                    .trim()
                    .parse()
                    .with_context(|| format!("[[store]] {id}: env {api_id_env} is not a number"))?;
                let api_hash = token(&id, api_hash_env.as_deref().unwrap_or("TG_API_HASH"))?;
                let bot_id = bot_token.split(':').next().unwrap_or_default();
                let session = session.unwrap_or_else(|| format!("pigeonhole-tg-{bot_id}.session").into());
                Store::Telegram {
                    chat_id: chat_id.to_string(),
                    bot_token_env,
                    login: TelegramLogin { bot_token, api_id, api_hash, session },
                }
            }
            FileStore::Discord { channel_id, bot_token_env, max_blob_size, .. } => {
                let bot_token_env = bot_token_env.unwrap_or_else(|| "DISCORD_BOT_TOKEN".into());
                Store::Discord {
                    channel_id: channel_id.to_string(),
                    bot_token: token(&id, &bot_token_env)?,
                    bot_token_env,
                    max_blob_size,
                }
            }
        })
    }
}

/// One `[[replicas]]`: every write goes to all members, `write_quorum` of
/// them must succeed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileReplicas {
    id: String,
    members: Vec<String>,
    #[serde(default = "one")]
    write_quorum: usize,
}

fn one() -> usize {
    1
}

/// One `[[shards]]`: every write goes to one member (a store or a
/// `[[replicas]]` group), the least loaded.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileShards {
    id: String,
    members: Vec<String>,
}

/// A resolved `[[store]]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Store {
    LocalDisk { path: PathBuf },
    Telegram { chat_id: String, bot_token_env: String, login: TelegramLogin },
    Discord { channel_id: String, bot_token_env: String, bot_token: String, max_blob_size: Option<usize> },
    Memory,
}

impl Store {
    /// Where it lives, if two stores could name the same place.
    fn location(&self) -> Option<String> {
        match self {
            Store::Telegram { chat_id, .. } => Some(telegram_location(chat_id)),
            Store::Discord { channel_id, .. } => Some(discord_location(channel_id)),
            Store::LocalDisk { path } => Some(local_disk_location(path)),
            Store::Memory => None,
        }
    }

    /// One store may serve both `a` and `b`: only the log and the snapshots,
    /// and only in a directory or memory (a chat's pinned message serves one;
    /// the blob sweeper deletes everything it does not know in a blob store).
    fn may_share(&self, a: &str, b: &str) -> bool {
        let meta = |p: &str| p == "[meta] log" || p == "[meta] snapshot";
        a == b || (meta(a) && meta(b) && matches!(self, Store::LocalDisk { .. } | Store::Memory))
    }
}

/// Every `[[store]]` and `[[replicas]]` by id, and which purpose took each store.
pub(crate) struct Topology {
    stores: HashMap<String, Store>,
    replicas: HashMap<String, FileReplicas>,
    shards: HashMap<String, FileShards>,
    /// Store id → purpose that uses it.
    used: HashMap<String, &'static str>,
    used_groups: HashSet<String>,
}

/// Store, replica group and shard ids share one namespace.
fn unique(ids: &mut HashSet<String>, id: &str) -> Result<()> {
    if !ids.insert(id.to_string()) {
        bail!("duplicate id {id:?}");
    }
    Ok(())
}

fn check_members(section: &str, id: &str, members: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for m in members {
        if !seen.insert(m) {
            bail!("{section} {id}: member {m:?} listed twice");
        }
    }
    Ok(())
}

fn token(store: &str, env_name: &str) -> Result<String> {
    std::env::var(env_name).with_context(|| format!("[[store]] {store}: env {env_name} is not set"))
}

impl Topology {
    pub(crate) fn new(stores: Vec<FileStore>, replicas: Vec<FileReplicas>, shards: Vec<FileShards>) -> Result<Self> {
        let mut ids = HashSet::new();
        let mut by_id = HashMap::new();
        let mut locations: HashMap<String, String> = HashMap::new();
        for f in stores {
            let id = f.id().to_string();
            if id.is_empty() {
                bail!("[[store]] id must be non-empty");
            }
            let store = f.resolve()?;
            if let Some(loc) = store.location() {
                if let Some(prev) = locations.insert(loc.clone(), id.clone()) {
                    bail!("[[store]] {prev:?} and {id:?} are the same place ({loc})");
                }
            }
            unique(&mut ids, &id)?;
            by_id.insert(id, store);
        }
        let mut groups = HashMap::new();
        for r in replicas {
            let id = r.id.trim().to_string();
            unique(&mut ids, &id)?;
            if r.members.is_empty() || r.write_quorum == 0 || r.write_quorum > r.members.len() {
                bail!(
                    "[[replicas]] {id}: needs members and 1 <= write_quorum ({}) <= {}",
                    r.write_quorum,
                    r.members.len()
                );
            }
            check_members("[[replicas]]", &id, &r.members)?;
            groups.insert(id, r);
        }
        let mut sharded = HashMap::new();
        for g in shards {
            let id = g.id.trim().to_string();
            unique(&mut ids, &id)?;
            if g.members.is_empty() {
                bail!("[[shards]] {id}: needs members");
            }
            check_members("[[shards]]", &id, &g.members)?;
            sharded.insert(id, g);
        }
        Ok(Self {
            stores: by_id,
            replicas: groups,
            shards: sharded,
            used: HashMap::new(),
            used_groups: HashSet::new(),
        })
    }

    /// Take store `id` for `purpose` (see [`Store::may_share`]).
    fn take(&mut self, id: &str, purpose: &'static str) -> Result<Store> {
        let store = self.stores.get(id).cloned().with_context(|| {
            if self.replicas.contains_key(id) || self.shards.contains_key(id) {
                format!("{purpose} = {id:?}: a group is not supported here yet; use a [[store]]")
            } else {
                format!("{purpose} = {id:?}: no [[store]] with that id")
            }
        })?;
        if let Some(prev) = self.used.insert(id.to_string(), purpose) {
            if !store.may_share(prev, purpose) {
                bail!(
                    "[[store]] {id} is used for both {prev} and {purpose}: a store serves one purpose \
                     (a chat's pinned message serves the log or the snapshots, the blob sweeper \
                     deletes everything it does not know in a blob store); only a local_disk or \
                     memory store holds both the log and the snapshots"
                );
            }
        }
        Ok(store)
    }

    /// A store the metadata log or snapshots live in: `local_disk`,
    /// `memory` or `telegram` (`discord` not yet).
    pub(crate) fn meta_store(&mut self, id: &str, purpose: &'static str) -> Result<Store> {
        self.take(id, purpose)
    }

    /// `[chunk] store`: a blob store, a `[[replicas]]` group of them, or
    /// `[[shards]]` over stores and replica groups.
    pub(crate) fn blob_placement(&mut self, id: &str) -> Result<(Vec<InstanceConfig>, PlacementConfig)> {
        let shard_ids = match self.shards.get(id) {
            Some(g) => {
                self.used_groups.insert(id.to_string());
                g.members.clone()
            }
            None => vec![id.to_string()],
        };
        let mut instances = Vec::new();
        let mut shards = Vec::new();
        for shard in &shard_ids {
            if self.shards.contains_key(shard) {
                bail!("[[shards]] {id}: member {shard:?} is a [[shards]] group; shards of shards are not supported");
            }
            let (group, write_quorum) = match self.replicas.get(shard) {
                Some(r) => {
                    self.used_groups.insert(shard.clone());
                    (r.members.clone(), r.write_quorum)
                }
                None => (vec![shard.clone()], 1),
            };
            for m in &group {
                if self.replicas.contains_key(m) || self.shards.contains_key(m) {
                    bail!("[[replicas]] {shard}: member {m:?} is a group; replicas of groups are not supported");
                }
                if self.used.get(m.as_str()) == Some(&"[chunk] store") {
                    bail!("[[store]] {m} is in two shards / replica groups of [chunk] store");
                }
                let store = self.take(m, "[chunk] store")?;
                instances.push(blob_instance(m, &store)?);
            }
            shards.push(ShardPlacement { group, write_quorum });
        }
        Ok((instances, PlacementConfig { shards }))
    }

    /// Every `[[store]]`, `[[replicas]]` and `[[shards]]` must be used: a
    /// leftover is almost always a typo in a reference.
    pub(crate) fn check_all_used(&self) -> Result<()> {
        for id in self.replicas.keys().chain(self.shards.keys()) {
            if !self.used_groups.contains(id) {
                bail!("group {id} is not used by [chunk] store");
            }
        }
        for id in self.stores.keys() {
            if !self.used.contains_key(id) {
                bail!("[[store]] {id} is not used by [meta] log / snapshot or [chunk] store");
            }
        }
        Ok(())
    }
}

/// A blob store as the chunk store sees it.
fn blob_instance(id: &str, store: &Store) -> Result<InstanceConfig> {
    let mut telegram = None;
    let (kind, bot_token_env, bot_token, scope_id, fingerprint, location, max_blob_size) = match store {
        Store::Memory => (
            InstanceKind::Memory,
            String::new(),
            String::new(),
            "local".to_string(),
            memory_fingerprint(id),
            memory_location(id),
            None,
        ),
        Store::Telegram { chat_id, bot_token_env, login } => {
            telegram = Some(login.clone());
            (
                InstanceKind::Telegram,
                bot_token_env.clone(),
                login.bot_token.clone(),
                chat_id.clone(),
                telegram_fingerprint(&login.bot_token, chat_id)?,
                telegram_location(chat_id),
                None,
            )
        }
        Store::Discord { channel_id, bot_token_env, bot_token, max_blob_size } => (
            InstanceKind::Discord,
            bot_token_env.clone(),
            bot_token.clone(),
            channel_id.clone(),
            discord_fingerprint(&discord_app_id_from_token(bot_token), channel_id),
            discord_location(channel_id),
            *max_blob_size,
        ),
        Store::LocalDisk { path } => (
            InstanceKind::LocalDisk,
            String::new(),
            String::new(),
            path.display().to_string(),
            local_disk_location(path),
            local_disk_location(path),
            None,
        ),
    };
    Ok(InstanceConfig {
        kind,
        info: InstanceInfo { id: id.to_string(), kind: kind.as_str(), fingerprint, location, role: InstanceRole::ReadWrite },
        bot_token_env,
        bot_token,
        scope_id,
        max_blob_size,
        telegram,
    })
}
