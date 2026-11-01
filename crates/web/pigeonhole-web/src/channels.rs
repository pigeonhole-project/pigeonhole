//! What the node's fake channels hold, read from their state: the site draws
//! them as they are when a page loads (a reload, a restart on a new config).

use crate::World;
use anyhow::Result;
use pigeonhole_config::instances::InstanceKind;
use pigeonhole_config::stores::{FakeBacking, FakeChannel};
use pigeonhole_config::{Config, LogSettings, SnapshotSettings};
use pigeonhole_storage_api::fake::FakeHost;
use pigeonhole_storage_discord::fake::state::{DiscordChannelState, Message as DcMessage};
use pigeonhole_storage_telegram::fake::state::{Message as TgMessage, TelegramChannelState};
use serde_json::{json, Value};

/// One fake channel of the config, and what the node uses it for.
#[derive(Clone, Debug)]
pub struct ChannelRef {
    pub store: String,
    /// `data` (a chunk store), `log` or `snapshots`.
    pub role: &'static str,
    pub kind: InstanceKind,
    pub scope: String,
    pub backing: FakeBacking,
    /// How many bots post to it.
    pub bots: usize,
    /// A data channel's place: its shard of how many, its copy of how many.
    pub place: Option<Place>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Place {
    pub shard: usize,
    pub shards: usize,
    pub copy: usize,
    pub copies: usize,
}

impl ChannelRef {
    fn of(f: FakeChannel, role: &'static str) -> Self {
        Self { store: f.id, role, kind: f.kind, scope: f.scope, backing: f.backing, bots: f.bots, place: None }
    }
}

/// Where store `id` sits in `[chunk] store`'s placement.
fn place(cfg: &Config, id: &str) -> Option<Place> {
    let shards = &cfg.placement.shards;
    shards.iter().enumerate().find_map(|(shard, s)| {
        let copy = s.group.iter().position(|m| m == id)?;
        Some(Place { shard, shards: shards.len(), copy, copies: s.group.len() })
    })
}

/// Every fake channel `cfg` uses.
pub fn of(cfg: &Config) -> Vec<ChannelRef> {
    let mut out: Vec<ChannelRef> = cfg
        .instances
        .iter()
        .filter_map(|i| {
            let f = i.fake.clone()?;
            Some(ChannelRef { place: place(cfg, &f.id), ..ChannelRef::of(f, "data") })
        })
        .collect();
    if let LogSettings::Fake(f) = &cfg.meta.log {
        out.push(ChannelRef::of(f.clone(), "log"));
    }
    if let SnapshotSettings::Fake(f) = &cfg.meta.snapshot {
        out.push(ChannelRef::of(f.clone(), "snapshots"));
    }
    out
}

/// A text message's blob: `name len\n<payload>`, its name, size and the start
/// of the payload; in the log's channel, what the post is to the log.
fn text(content: &str, role: &str, kind: InstanceKind) -> Value {
    let (head, payload) = content.split_once('\n').unwrap_or((content, ""));
    let (name, len) = head.rsplit_once(' ').unwrap_or((head, "0"));
    let preview: String = payload.chars().take(24).collect();
    let mut v = json!({ "kind": "text", "name": name, "size": len.parse::<u64>().unwrap_or(0), "preview": preview });
    if role == "log" {
        v["log"] = log_post(name, pigeonhole_storage_api::text::decode(content).map(|(_, b)| b), kind);
    }
    v
}

/// What a post of the log's channel is: a probe of the channel's order, a
/// generation's claim, or a batch of entries (decoded: which positions).
fn log_post(name: &str, data: Option<bytes::Bytes>, kind: InstanceKind) -> Value {
    let label = name.split('~').next().unwrap_or(name);
    let gen = |prefix: &str| label.strip_prefix(prefix).and_then(|g| g.parse::<u64>().ok());
    if label.starts_with("chain-probe") {
        json!({ "what": "probe" })
    } else if let Some(g) = gen("log-claim-") {
        json!({ "what": "claim", "gen": g })
    } else if let Some(g) = gen("log-batch-") {
        use pigeonhole_meta::log::chain::summarize;
        let summary = data.and_then(|d| match kind {
            InstanceKind::Telegram => summarize::<pigeonhole_storage_telegram::TelegramId>(&d).ok(),
            InstanceKind::Discord => summarize::<pigeonhole_storage_discord::DiscordId>(&d).ok(),
            _ => None,
        });
        match summary {
            Some(b) if b.entries > 0 => {
                json!({ "what": "batch", "gen": g, "first": b.first, "last": b.first + b.entries as u64 - 1 })
            }
            Some(b) => json!({ "what": "barrier", "gen": g, "first": b.first }),
            None => json!({ "what": "batch", "gen": g }),
        }
    } else {
        json!({ "what": "other", "label": label })
    }
}

/// Message `id`, posted at `at` (ms since the epoch).
fn telegram(id: i64, at: Option<u64>, m: &TgMessage, role: &str) -> Value {
    let mut v = match m {
        TgMessage::Other => json!({ "kind": "other" }),
        TgMessage::Text(t) => text(t, role, InstanceKind::Telegram),
        TgMessage::Doc { name, size, .. } => {
            let mut v = json!({ "kind": "document", "name": name, "size": size });
            if role == "log" {
                v["log"] = log_post(name, None, InstanceKind::Telegram);
            }
            v
        }
    };
    v["id"] = json!(id);
    v["at"] = json!(at);
    v
}

fn discord(id: u64, m: &DcMessage, role: &str) -> Value {
    let mut v = match &m.file {
        Some(f) => {
            let mut v = json!({ "kind": "attachment", "name": f.name, "size": f.size });
            if role == "log" {
                v["log"] = log_post(&f.name, None, InstanceKind::Discord);
            }
            v
        }
        None => text(&m.content, role, InstanceKind::Discord),
    };
    v["id"] = json!(id.to_string());
    v["at"] = json!(pigeonhole_storage_discord::snowflake_timestamp_ms(id));
    v
}

impl World {
    /// Channel `c` as it is: how many messages it holds and the last `limit`.
    pub async fn describe(&self, c: &ChannelRef, limit: usize) -> Result<Value> {
        // The last `limit` messages, and the pinned ones wherever they are.
        let (total, messages, pinned): (usize, Vec<Value>, Vec<Value>) = match c.kind {
            InstanceKind::Telegram => {
                let ch = self.telegram(&c.scope, &c.backing).await?;
                ch.host().read(|s: &TelegramChannelState| {
                    let one = |(id, m): (i64, &TgMessage)| telegram(id, s.date_ms(id), m, c.role);
                    let all: Vec<Value> = s.messages().map(one).collect();
                    let pinned = s.pinned().and_then(|p| s.message(p).map(|m| one((p, m)))).into_iter().collect();
                    (all.len(), all[all.len().saturating_sub(limit)..].to_vec(), pinned)
                })
            }
            InstanceKind::Discord => {
                let ch = self.discord(&c.scope, &c.backing).await?;
                ch.host().read(|s: &DiscordChannelState| {
                    let all: Vec<Value> = s.messages().map(|(id, m)| discord(id, m, c.role)).collect();
                    let pins = s.pinned();
                    let pinned = s
                        .messages()
                        .filter(|(id, _)| pins.contains(id))
                        .map(|(id, m)| discord(id, m, c.role))
                        .collect();
                    (all.len(), all[all.len().saturating_sub(limit)..].to_vec(), pinned)
                })
            }
            _ => (0, Vec::new(), Vec::new()),
        };
        let place =
            c.place.map(|p| json!({ "shard": p.shard, "shards": p.shards, "copy": p.copy, "copies": p.copies }));
        Ok(json!({
            "store": c.store,
            "role": c.role,
            "kind": c.kind.as_str(),
            "scope": c.scope,
            "bots": c.bots,
            "down": self.is_down(c),
            "place": place,
            "total": total,
            "messages": messages,
            "pinned": pinned,
        }))
    }
}

/// The demo's channels for the admin ops: fakes it can break.
pub struct DemoChannels {
    pub world: std::sync::Arc<World>,
    pub channels: Vec<ChannelRef>,
}

impl DemoChannels {
    fn find(&self, store: &str) -> Result<&ChannelRef> {
        self.channels.iter().find(|c| c.store == store).ok_or_else(|| anyhow::anyhow!("no fake channel {store:?}"))
    }
}

#[async_trait::async_trait]
impl pigeonhole_admin::Channels for DemoChannels {
    async fn describe(&self, limit: usize) -> Result<Value> {
        let mut out = Vec::new();
        for c in &self.channels {
            out.push(self.world.describe(c, limit).await?);
        }
        Ok(json!({ "channels": out }))
    }

    async fn delete_message(&self, store: &str, id: &str) -> Result<()> {
        self.world.delete_message(self.find(store)?, id).await
    }

    async fn fault(&self, op: &str, store: &str, down: bool) -> Result<Value> {
        let c = self.find(store)?;
        Ok(match op {
            "outage" => {
                self.world.set_down(c, down);
                json!({ "down": down })
            }
            _ => json!({ "deleted": self.world.wipe(c).await? }),
        })
    }
}
