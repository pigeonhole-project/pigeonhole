//! What the node's fake channels hold, read from their state: the site draws
//! them as they are when a page loads (a reload, a restart on a new config).

use crate::World;
use anyhow::Result;
use pigeonhole_config::instances::InstanceKind;
use pigeonhole_config::stores::FakeBacking;
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
}

/// Every fake channel `cfg` uses.
pub fn of(cfg: &Config) -> Vec<ChannelRef> {
    let mut out: Vec<ChannelRef> = cfg
        .instances
        .iter()
        .filter_map(|i| {
            let backing = i.fake.clone()?;
            Some(ChannelRef {
                store: i.info.id.clone(),
                role: "data",
                kind: i.kind,
                scope: i.scope_id.clone(),
                backing,
            })
        })
        .collect();
    if let LogSettings::Fake { kind, scope, backing } = &cfg.meta.log {
        out.push(ChannelRef {
            store: "meta-log".into(),
            role: "log",
            kind: *kind,
            scope: scope.clone(),
            backing: backing.clone(),
        });
    }
    if let SnapshotSettings::Fake { kind, scope, backing } = &cfg.meta.snapshot {
        out.push(ChannelRef {
            store: "meta-snapshots".into(),
            role: "snapshots",
            kind: *kind,
            scope: scope.clone(),
            backing: backing.clone(),
        });
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

fn telegram(id: i64, m: &TgMessage, role: &str) -> Value {
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
    v
}

impl World {
    /// Channel `c` as it is: how many messages it holds and the last `limit`.
    pub async fn describe(&self, c: &ChannelRef, limit: usize) -> Result<Value> {
        let (total, messages): (usize, Vec<Value>) = match c.kind {
            InstanceKind::Telegram => {
                let ch = self.telegram(&c.scope, &c.backing).await?;
                ch.host().read(|s: &TelegramChannelState| {
                    let all: Vec<Value> = s.messages().map(|(id, m)| telegram(id, m, c.role)).collect();
                    (all.len(), all[all.len().saturating_sub(limit)..].to_vec())
                })
            }
            InstanceKind::Discord => {
                let ch = self.discord(&c.scope, &c.backing).await?;
                ch.host().read(|s: &DiscordChannelState| {
                    let all: Vec<Value> = s.messages().map(|(id, m)| discord(id, m, c.role)).collect();
                    (all.len(), all[all.len().saturating_sub(limit)..].to_vec())
                })
            }
            _ => (0, Vec::new()),
        };
        Ok(
            json!({ "store": c.store, "role": c.role, "kind": c.kind.as_str(), "scope": c.scope, "total": total, "messages": messages }),
        )
    }
}
