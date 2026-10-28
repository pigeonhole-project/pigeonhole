//! Where bytes live (`[[store]]`) and how stores are combined: copies
//! (`[[replicas]]`) and parts (`[[shards]]`). `[meta] log` / `snapshot` and
//! `[chunk] store` refer to them by id.

use crate::instances::{
    discord_app_id_from_token, discord_fingerprint, discord_location, local_disk_location,
    memory_fingerprint, memory_location, telegram_location, telegram_fingerprint,
    InstanceConfig, TelegramLogin,
};
use crate::{PlacementConfig, ShardPlacement};
use anyhow::{bail, Context, Result};
use pigeonhole_blob::{InstanceInfo, InstanceKind, InstanceRole};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// One `[[store]]`; the fields a kind does not take must be absent.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileStore {
    id: String,
    kind: String,
    /// `dir`.
    path: Option<PathBuf>,
    /// `telegram`.
    chat_id: Option<i64>,
    /// `discord`.
    channel_id: Option<u64>,
    /// `telegram` / `discord`.
    bot_token_env: Option<String>,
    /// `discord`.
    max_blob_size: Option<usize>,
    /// `telegram`: env var with the app's api id (my.telegram.org).
    api_id_env: Option<String>,
    /// `telegram`: env var with the app's api hash.
    api_hash_env: Option<String>,
    /// `telegram`: the MTProto session file; default
    /// `pigeonhole-tg-<bot id>.session`. One per bot, shared by its stores.
    session: Option<PathBuf>,
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

fn token(store: &str, env_name: &str) -> Result<String> {
    std::env::var(env_name).with_context(|| format!("[[store]] {store}: env {env_name} is not set"))
}

impl Topology {
    pub(crate) fn new(stores: Vec<FileStore>, replicas: Vec<FileReplicas>, shards: Vec<FileShards>) -> Result<Self> {
        let mut by_id = HashMap::new();
        let mut locations: HashMap<String, String> = HashMap::new();
        for f in stores {
            let id = f.id.trim().to_string();
            if id.is_empty() {
                bail!("[[store]] id must be non-empty");
            }
            let refuse = |field: &str, present: bool| -> Result<()> {
                if present {
                    bail!("[[store]] {id}: kind = {:?} does not take {field}", f.kind);
                }
                Ok(())
            };
            if f.kind != "telegram" {
                refuse("api_id_env", f.api_id_env.is_some())?;
                refuse("api_hash_env", f.api_hash_env.is_some())?;
                refuse("session", f.session.is_some())?;
            }
            let store = match f.kind.as_str() {
                "local_disk" => {
                    refuse("chat_id", f.chat_id.is_some())?;
                    refuse("channel_id", f.channel_id.is_some())?;
                    refuse("bot_token_env", f.bot_token_env.is_some())?;
                    refuse("max_blob_size", f.max_blob_size.is_some())?;
                    Store::LocalDisk { path: f.path.with_context(|| format!("[[store]] {id}: local_disk needs path"))? }
                }
                "telegram" => {
                    refuse("path", f.path.is_some())?;
                    refuse("channel_id", f.channel_id.is_some())?;
                    refuse("max_blob_size", f.max_blob_size.is_some())?;
                    let chat = f.chat_id.with_context(|| format!("[[store]] {id}: telegram needs chat_id"))?;
                    let env_name = f.bot_token_env.clone().unwrap_or_else(|| "BOT_TOKEN".into());
                    let bot_token = token(&id, &env_name)?;
                    let api_id_env = f.api_id_env.clone().unwrap_or_else(|| "TG_API_ID".into());
                    let api_id = token(&id, &api_id_env)?
                        .trim()
                        .parse()
                        .with_context(|| format!("[[store]] {id}: env {api_id_env} is not a number"))?;
                    let api_hash = token(&id, f.api_hash_env.as_deref().unwrap_or("TG_API_HASH"))?;
                    let bot_id = bot_token.split(':').next().unwrap_or_default();
                    let session = f.session.clone().unwrap_or_else(|| format!("pigeonhole-tg-{bot_id}.session").into());
                    Store::Telegram {
                        chat_id: chat.to_string(),
                        bot_token_env: env_name,
                        login: TelegramLogin { bot_token, api_id, api_hash, session },
                    }
                }
                "discord" => {
                    refuse("path", f.path.is_some())?;
                    refuse("chat_id", f.chat_id.is_some())?;
                    let channel = f.channel_id.with_context(|| format!("[[store]] {id}: discord needs channel_id"))?;
                    let env_name = f.bot_token_env.unwrap_or_else(|| "DISCORD_BOT_TOKEN".into());
                    Store::Discord {
                        channel_id: channel.to_string(),
                        bot_token: token(&id, &env_name)?,
                        bot_token_env: env_name,
                        max_blob_size: f.max_blob_size,
                    }
                }
                "memory" => {
                    refuse("path", f.path.is_some())?;
                    refuse("chat_id", f.chat_id.is_some())?;
                    refuse("channel_id", f.channel_id.is_some())?;
                    refuse("bot_token_env", f.bot_token_env.is_some())?;
                    refuse("max_blob_size", f.max_blob_size.is_some())?;
                    Store::Memory
                }
                other => bail!("[[store]] {id}: unknown kind {other:?}; expected local_disk | memory | telegram | discord"),
            };
            if let Some(loc) = store.location() {
                if let Some(prev) = locations.insert(loc.clone(), id.clone()) {
                    bail!("[[store]] {prev:?} and {id:?} are the same place ({loc})");
                }
            }
            if by_id.insert(id.clone(), store).is_some() {
                bail!("duplicate id {id:?}");
            }
        }
        let mut groups = HashMap::new();
        for r in replicas {
            let id = r.id.trim().to_string();
            if by_id.contains_key(&id) || groups.contains_key(&id) {
                bail!("duplicate id {id:?}");
            }
            if r.members.is_empty() || r.write_quorum == 0 || r.write_quorum > r.members.len() {
                bail!(
                    "[[replicas]] {id}: needs members and 1 <= write_quorum ({}) <= {}",
                    r.write_quorum,
                    r.members.len()
                );
            }
            let mut seen = HashSet::new();
            for m in &r.members {
                if !seen.insert(m) {
                    bail!("[[replicas]] {id}: member {m:?} listed twice");
                }
            }
            groups.insert(id, r);
        }
        let mut sharded = HashMap::new();
        for g in shards {
            let id = g.id.trim().to_string();
            if by_id.contains_key(&id) || groups.contains_key(&id) || sharded.contains_key(&id) {
                bail!("duplicate id {id:?}");
            }
            if g.members.is_empty() {
                bail!("[[shards]] {id}: needs members");
            }
            let mut seen = HashSet::new();
            for m in &g.members {
                if !seen.insert(m) {
                    bail!("[[shards]] {id}: member {m:?} listed twice");
                }
            }
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
        info: InstanceInfo { id: id.to_string(), kind, fingerprint, location, role: InstanceRole::ReadWrite },
        bot_token_env,
        bot_token,
        scope_id,
        max_blob_size,
        telegram,
    })
}
