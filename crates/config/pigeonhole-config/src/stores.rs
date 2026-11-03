//! Where bytes live (`[[store]]`) and how stores are combined: copies
//! (`[[replicas]]`) and parts (`[[shards]]`). `[meta] log` / `snapshot` and
//! `[chunk] store` refer to them by id.

use crate::instances::InstanceKind;
use crate::instances::{
    discord_app_id_from_token, discord_fingerprint, discord_location, local_disk_location, memory_fingerprint,
    memory_location, telegram_fingerprint, telegram_location, InstanceConfig, TelegramLogin,
};
use crate::{PlacementConfig, ShardPlacement};
use anyhow::{bail, Context, Result};
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
        /// Env vars with the bots' tokens, the primary first (it reads and
        /// names the store); more bots, more sends a minute
        /// (docs/shared-limiter.md). Default `["BOT_TOKEN"]`.
        bot_token_envs: Option<Vec<String>>,
        /// The bots' ids, one per `bot_token_envs` entry and in its order
        /// (a Telegram token starts with its bot's id): no secret, so the
        /// file can say whose bots post where; checked against the tokens
        /// at start. A fake channel's: its fake bots (default 1, 2, …).
        bot_ids: Option<Vec<u64>>,
        /// Env var with the app's api id (my.telegram.org).
        api_id_env: Option<String>,
        /// Env var with the app's api hash.
        api_hash_env: Option<String>,
        /// The MTProto session file of a store with one bot; default
        /// `pigeonhole-tg-<bot id>.session`, one per bot, shared by its stores.
        session: Option<PathBuf>,
        /// A fake channel instead of Telegram (builds with feature `fake`).
        fake: Option<String>,
        /// How many bots post to the fake channel, each with its own budget
        /// (`[telegram.send]` is per bot); default 1.
        bots: Option<usize>,
    },
    Discord {
        id: String,
        channel_id: u64,
        /// As for telegram; default `["DISCORD_BOT_TOKEN"]`.
        bot_token_envs: Option<Vec<String>>,
        /// As for telegram (a Discord token's first part is its bot's id,
        /// in base64).
        bot_ids: Option<Vec<u64>>,
        #[serde(default, deserialize_with = "crate::units::opt_size")]
        max_blob_size: Option<usize>,
        /// A fake channel instead of Discord (builds with feature `fake`).
        fake: Option<String>,
        /// As for telegram (`[discord.send]` is per bot).
        bots: Option<usize>,
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
        if let Some(fake) = self.fake_channel(&id)? {
            return Ok(fake);
        }
        Ok(match self {
            FileStore::LocalDisk { path, .. } => Store::LocalDisk { path },
            FileStore::Memory { .. } => Store::Memory,
            FileStore::Telegram { chat_id, bot_token_envs, bot_ids, api_id_env, api_hash_env, session, .. } => {
                let bot_token_envs = token_envs(&id, bot_token_envs, "BOT_TOKEN")?;
                let bots = TelegramBots { envs: &bot_token_envs, api_id_env, api_hash_env, session };
                let what = format!("[[store]] {id}");
                let logins = bots.logins(&what)?;
                let tokens: Vec<&str> = logins.iter().map(|l| l.bot_token.as_str()).collect();
                check_bot_ids(&what, &bot_token_envs, &tokens, telegram_bot_id, bot_ids.as_deref())?;
                Store::Telegram { chat_id: chat_id.to_string(), bot_token_envs, logins }
            }
            FileStore::Discord { channel_id, bot_token_envs, bot_ids, max_blob_size, .. } => {
                let bot_token_envs = token_envs(&id, bot_token_envs, "DISCORD_BOT_TOKEN")?;
                let bot_tokens = bot_token_envs.iter().map(|env| token(&id, env)).collect::<Result<Vec<_>>>()?;
                distinct_bots(&id, bot_tokens.iter().map(|t| t.split('.').next().unwrap_or_default()))?;
                let tokens: Vec<&str> = bot_tokens.iter().map(String::as_str).collect();
                check_bot_ids(
                    &format!("[[store]] {id}"),
                    &bot_token_envs,
                    &tokens,
                    discord_bot_id,
                    bot_ids.as_deref(),
                )?;
                Store::Discord { channel_id: channel_id.to_string(), bot_token_envs, bot_tokens, max_blob_size }
            }
        })
    }
}

impl FileStore {
    /// The bots it says it posts with (a fake channel's too: its default
    /// ones unless named), before anything is resolved: what the file
    /// checks the cluster's bots against.
    pub(crate) fn declared_bot_ids(&self) -> Vec<u64> {
        let (fake, ids, bots) = match self {
            FileStore::Telegram { fake, bot_ids, bots, .. } | FileStore::Discord { fake, bot_ids, bots, .. } => {
                (fake.is_some(), bot_ids, bots)
            }
            _ => return Vec::new(),
        };
        match (ids, fake) {
            (Some(ids), _) => ids.clone(),
            (None, true) => (1..=bots.unwrap_or(1) as u64).collect(),
            (None, false) => Vec::new(),
        }
    }

    pub(crate) fn store_id(&self) -> &str {
        self.id()
    }

    /// A telegram or discord store, fake or not: a chat.
    pub(crate) fn is_chat(&self) -> bool {
        matches!(self, FileStore::Telegram { .. } | FileStore::Discord { .. })
    }

    /// A telegram or discord store with `fake`: a channel without the
    /// service, no bots and no secrets.
    fn fake_channel(&self, id: &str) -> Result<Option<Store>> {
        let (kind, scope, fake, real_bots, bots, bot_ids) = match self {
            FileStore::Telegram {
                chat_id,
                fake,
                bot_token_envs,
                api_id_env,
                api_hash_env,
                session,
                bots,
                bot_ids,
                ..
            } => (
                InstanceKind::Telegram,
                chat_id.to_string(),
                fake,
                bot_token_envs.is_some() || api_id_env.is_some() || api_hash_env.is_some() || session.is_some(),
                bots,
                bot_ids,
            ),
            FileStore::Discord { channel_id, fake, bot_token_envs, bots, bot_ids, .. } => {
                (InstanceKind::Discord, channel_id.to_string(), fake, bot_token_envs.is_some(), bots, bot_ids)
            }
            _ => return Ok(None),
        };
        let Some(fake) = fake else {
            if bots.is_some() {
                bail!("[[store]] {id}: `bots` counts a fake channel's bots; a real store names them in bot_token_envs");
            }
            return Ok(None);
        };
        if !cfg!(feature = "fake") {
            bail!("[[store]] {id}: `fake` channels are only in builds with the `fake` feature");
        }
        if real_bots {
            bail!("[[store]] {id}: a fake channel has no real bots: drop bot_token_envs, api_id_env, api_hash_env, session (`bots` counts its fake ones)");
        }
        let backing = match fake.as_str() {
            "memory" => FakeBacking::Memory,
            store => FakeBacking::Store(store.to_string()),
        };
        if let (Some(n), Some(ids)) = (bots, bot_ids) {
            if *n != ids.len() {
                bail!("[[store]] {id}: bots = {n} but bot_ids names {}: give one of them", ids.len());
            }
        }
        let bot_ids = self.declared_bot_ids();
        if bot_ids.is_empty() {
            bail!("[[store]] {id}: a fake channel needs at least one bot");
        }
        distinct_bots(id, bot_ids.iter().map(|b| b.to_string()).collect::<Vec<_>>().iter().map(String::as_str))?;
        let bots = bot_ids.len();
        Ok(Some(Store::Fake(FakeChannel { id: id.to_string(), kind, scope, backing, bots, bot_ids })))
    }
}

/// A telegram or discord store on a fake channel (`fake = …`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FakeChannel {
    /// Its `[[store]]` id.
    pub id: String,
    pub kind: InstanceKind,
    /// The chat or channel id.
    pub scope: String,
    pub backing: FakeBacking,
    /// How many bots post to it, the primary first.
    pub bots: usize,
    /// Their ids (`bot_ids`, else 1, 2, …): each its own budget.
    pub bot_ids: Vec<u64>,
}

/// Where a fake channel keeps its messages (its tablet: log, snapshots,
/// documents).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FakeBacking {
    /// Process memory: gone with the process.
    Memory,
    /// A directory of its own (`<store path>/<kind>-<scope>`): kept.
    Dir(PathBuf),
    /// `fake = "<store id>"` before the stores are resolved; never in a Config.
    Store(String),
}

/// The env vars of a store's bots: `default` if unset, never none.
fn token_envs(id: &str, envs: Option<Vec<String>>, default: &str) -> Result<Vec<String>> {
    let envs = envs.unwrap_or_else(|| vec![default.into()]);
    if envs.is_empty() {
        bail!("[[store]] {id}: bot_token_envs is empty");
    }
    Ok(envs)
}

/// Telegram bots named by env vars (a store's, or a cluster node's).
pub(crate) struct TelegramBots<'a> {
    pub envs: &'a [String],
    pub api_id_env: Option<String>,
    pub api_hash_env: Option<String>,
    /// One bot's session file; default `pigeonhole-tg-<bot id>.session`.
    pub session: Option<PathBuf>,
}

impl TelegramBots<'_> {
    /// Their logins, from the env; `what` names them in errors.
    pub(crate) fn logins(self, what: &str) -> Result<Vec<TelegramLogin>> {
        if self.session.is_some() && self.envs.len() > 1 {
            bail!("{what}: `session` names one bot's session file; with several bots each has its own default");
        }
        let env = |name: &str| std::env::var(name).with_context(|| format!("{what}: env {name} is not set"));
        let api_id_env = self.api_id_env.unwrap_or_else(|| "TG_API_ID".into());
        let api_id =
            env(&api_id_env)?.trim().parse().with_context(|| format!("{what}: env {api_id_env} is not a number"))?;
        let api_hash = env(self.api_hash_env.as_deref().unwrap_or("TG_API_HASH"))?;
        let logins = self
            .envs
            .iter()
            .map(|name| {
                let bot_token = env(name)?;
                let bot_id = bot_token.split(':').next().unwrap_or_default();
                let session = self.session.clone().unwrap_or_else(|| format!("pigeonhole-tg-{bot_id}.session").into());
                Ok(TelegramLogin { bot_token, api_id, api_hash: api_hash.clone(), session })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut seen = HashSet::new();
        for bot in logins.iter().map(|l| l.bot_token.split(':').next().unwrap_or_default()) {
            if !seen.insert(bot) {
                bail!("{what}: bot {bot} is listed twice in bot_token_envs");
            }
        }
        Ok(logins)
    }
}

/// A Telegram token's bot: the number before its `:`.
pub(crate) fn telegram_bot_id(token: &str) -> Option<u64> {
    token.split(':').next()?.trim().parse().ok()
}

/// A Discord token's bot: its first part, the bot's id in base64.
pub(crate) fn discord_bot_id(token: &str) -> Option<u64> {
    use base64::Engine as _;
    let first = token.split('.').next()?.trim();
    let raw = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(first.trim_end_matches('='))
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(first.trim_end_matches('=')))
        .ok()?;
    String::from_utf8(raw).ok()?.parse().ok()
}

/// The bots `bot_ids` names are the ones the tokens in `envs` belong to,
/// one per env var and in its order: what the file says is what runs.
/// Errors name the env var and the ids, never a token.
pub(crate) fn check_bot_ids(
    what: &str,
    envs: &[String],
    tokens: &[&str],
    id_of: fn(&str) -> Option<u64>,
    declared: Option<&[u64]>,
) -> Result<()> {
    let Some(declared) = declared else {
        bail!("{what}: bot_ids = [...] is required: the bots' ids, one per bot_token_envs entry, in its order (a Telegram token starts with its bot's id)");
    };
    if declared.len() != envs.len() {
        bail!("{what}: bot_ids names {} bots, bot_token_envs {}: one id per env var", declared.len(), envs.len());
    }
    for ((env, token), want) in envs.iter().zip(tokens).zip(declared) {
        match id_of(token) {
            None => bail!("{what}: env {env} holds no bot token (no bot id in it)"),
            Some(got) if got != *want => {
                bail!("{what}: env {env} holds bot {got}'s token, bot_ids says bot {want}")
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// No bot twice in one store (one bot cannot add to its own budget).
fn distinct_bots<'a>(id: &str, bots: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen = HashSet::new();
    for bot in bots {
        if !seen.insert(bot) {
            bail!("[[store]] {id}: bot {bot} is listed twice in bot_token_envs");
        }
    }
    Ok(())
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
    LocalDisk {
        path: PathBuf,
    },
    /// `logins`: the primary first.
    Telegram {
        chat_id: String,
        bot_token_envs: Vec<String>,
        logins: Vec<TelegramLogin>,
    },
    Discord {
        channel_id: String,
        bot_token_envs: Vec<String>,
        bot_tokens: Vec<String>,
        max_blob_size: Option<usize>,
    },
    Memory,
    /// A telegram or discord store on a fake channel (`fake = …`).
    Fake(FakeChannel),
}

impl Store {
    /// Where it lives, if two stores could name the same place.
    fn location(&self) -> Option<String> {
        match self {
            Store::Telegram { chat_id, .. } => Some(telegram_location(chat_id)),
            Store::Discord { channel_id, .. } => Some(discord_location(channel_id)),
            Store::LocalDisk { path } => Some(local_disk_location(path)),
            Store::Memory => None,
            Store::Fake(f) => Some(fake_location(f.kind, &f.scope)),
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
    /// Other nodes' log and snapshot stores: never resolved here (their
    /// bots' secrets are on their hosts), never taken for anything else.
    others: HashMap<String, String>,
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
    /// `others`: store id → the other node whose log or snapshots it is.
    pub(crate) fn new(
        stores: Vec<FileStore>,
        replicas: Vec<FileReplicas>,
        shards: Vec<FileShards>,
        others: HashMap<String, String>,
    ) -> Result<Self> {
        let mut ids = HashSet::new();
        let mut by_id = HashMap::new();
        let mut locations: HashMap<String, String> = HashMap::new();
        let mut elsewhere = HashMap::new();
        for f in stores {
            let id = f.id().to_string();
            if id.is_empty() {
                bail!("[[store]] id must be non-empty");
            }
            if let Some(whose) = others.get(&id) {
                unique(&mut ids, &id)?;
                elsewhere.insert(id, whose.clone());
                continue;
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
        if let Some(id) = others.keys().find(|id| !elsewhere.contains_key(*id)) {
            bail!("{}: no [[store]] {id:?}", others[id]);
        }
        let mut topology = Self {
            stores: by_id,
            replicas: groups,
            shards: sharded,
            used: HashMap::new(),
            used_groups: HashSet::new(),
            others: elsewhere,
        };
        topology.resolve_fakes()?;
        Ok(topology)
    }

    /// `fake = "<store id>"`: the channel's own directory in that local_disk
    /// store (or memory, for a memory store), which then serves fake channels only.
    fn resolve_fakes(&mut self) -> Result<()> {
        let mut ids: Vec<String> = self.stores.keys().cloned().collect();
        ids.sort();
        for id in ids {
            let Some(Store::Fake(fake)) = self.stores.get(&id).cloned() else { continue };
            let FakeBacking::Store(under) = &fake.backing else { continue };
            let (kind, scope) = (fake.kind, &fake.scope);
            let backing = match self.take(under, "fake channels").with_context(|| format!("[[store]] {id}: fake"))? {
                Store::LocalDisk { path } => FakeBacking::Dir(path.join(format!("{}-{scope}", kind.as_str()))),
                Store::Memory => FakeBacking::Memory,
                _ => bail!("[[store]] {id}: fake = {under:?}: a local_disk or memory store keeps a fake channel"),
            };
            self.stores.insert(id, Store::Fake(FakeChannel { backing, ..fake }));
        }
        Ok(())
    }

    /// What fake channels named `fake = under` keep themselves in: memory,
    /// or the directory of a local_disk store (each channel one inside).
    pub(crate) fn fake_base(&mut self, under: &str) -> Result<FakeBacking> {
        if under == "memory" {
            return Ok(FakeBacking::Memory);
        }
        Ok(match self.take(under, "fake channels")? {
            Store::LocalDisk { path } => FakeBacking::Dir(path),
            Store::Memory => FakeBacking::Memory,
            _ => bail!("fake = {under:?}: a local_disk or memory store keeps fake channels"),
        })
    }

    /// Take store `id` for `purpose` (see [`Store::may_share`]): the
    /// metadata log or snapshots, a blob store, fake channels.
    pub(crate) fn take(&mut self, id: &str, purpose: &'static str) -> Result<Store> {
        if let Some(whose) = self.others.get(id) {
            bail!("{purpose} = {id:?}: that [[store]] is {whose}; a store serves one node");
        }
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
            if !self.used.contains_key(id) && !self.others.contains_key(id) {
                bail!("[[store]] {id} is not used by [meta] log / snapshot or [chunk] store");
            }
        }
        Ok(())
    }
}

/// A blob store as the chunk store sees it.
fn blob_instance(id: &str, store: &Store) -> Result<InstanceConfig> {
    let mut telegram = Vec::new();
    let (kind, bot_token_envs, discord_tokens, scope_id, fingerprint, location, max_blob_size) = match store {
        Store::Memory => (
            InstanceKind::Memory,
            Vec::new(),
            Vec::new(),
            "local".to_string(),
            memory_fingerprint(id),
            memory_location(id),
            None,
        ),
        // The store is named by its primary bot.
        Store::Telegram { chat_id, bot_token_envs, logins } => {
            telegram = logins.clone();
            (
                InstanceKind::Telegram,
                bot_token_envs.clone(),
                Vec::new(),
                chat_id.clone(),
                telegram_fingerprint(&logins[0].bot_token, chat_id)?,
                telegram_location(chat_id),
                None,
            )
        }
        Store::Discord { channel_id, bot_token_envs, bot_tokens, max_blob_size } => (
            InstanceKind::Discord,
            bot_token_envs.clone(),
            bot_tokens.clone(),
            channel_id.clone(),
            discord_fingerprint(&discord_app_id_from_token(&bot_tokens[0]), channel_id),
            discord_location(channel_id),
            *max_blob_size,
        ),
        Store::LocalDisk { path } => (
            InstanceKind::LocalDisk,
            Vec::new(),
            Vec::new(),
            path.display().to_string(),
            local_disk_location(path),
            local_disk_location(path),
            None,
        ),
        // Marked `fake:` so that a real binary never takes its chunks for its own.
        Store::Fake(f) => (
            f.kind,
            Vec::new(),
            Vec::new(),
            f.scope.clone(),
            fake_location(f.kind, &f.scope),
            fake_location(f.kind, &f.scope),
            None,
        ),
    };
    Ok(InstanceConfig {
        kind,
        info: InstanceInfo {
            id: id.to_string(),
            kind: kind.as_str(),
            fingerprint,
            location,
            role: InstanceRole::ReadWrite,
        },
        bot_token_envs,
        discord_tokens,
        scope_id,
        max_blob_size,
        telegram,
        fake: match store {
            Store::Fake(f) => Some(f.clone()),
            _ => None,
        },
    })
}

/// Fingerprint and location of a fake channel.
fn fake_location(kind: InstanceKind, scope: &str) -> String {
    format!("fake:{}:{scope}", kind.as_str())
}
