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
/// of the payload.
fn text(content: &str) -> Value {
    let (head, payload) = content.split_once('\n').unwrap_or((content, ""));
    let (name, len) = head.rsplit_once(' ').unwrap_or((head, "0"));
    let preview: String = payload.chars().take(24).collect();
    json!({ "kind": "text", "name": name, "size": len.parse::<u64>().unwrap_or(0), "preview": preview })
}

fn telegram(id: i64, m: &TgMessage) -> Value {
    let mut v = match m {
        TgMessage::Other => json!({ "kind": "other" }),
        TgMessage::Text(t) => text(t),
        TgMessage::Doc { name, size, .. } => json!({ "kind": "document", "name": name, "size": size }),
    };
    v["id"] = json!(id);
    v
}

fn discord(id: u64, m: &DcMessage) -> Value {
    let mut v = match &m.file {
        Some(f) => json!({ "kind": "attachment", "name": f.name, "size": f.size }),
        None => text(&m.content),
    };
    v["id"] = json!(id.to_string());
    v
}

impl World {
    /// Channel `c` as it is: its last `limit` messages.
    pub async fn describe(&self, c: &ChannelRef, limit: usize) -> Result<Value> {
        let messages: Vec<Value> = match c.kind {
            InstanceKind::Telegram => {
                let ch = self.telegram(&c.scope, &c.backing).await?;
                ch.host().read(|s: &TelegramChannelState| {
                    let all: Vec<Value> = s.messages().map(|(id, m)| telegram(id, m)).collect();
                    all[all.len().saturating_sub(limit)..].to_vec()
                })
            }
            InstanceKind::Discord => {
                let ch = self.discord(&c.scope, &c.backing).await?;
                ch.host().read(|s: &DiscordChannelState| {
                    let all: Vec<Value> = s.messages().map(|(id, m)| discord(id, m)).collect();
                    all[all.len().saturating_sub(limit)..].to_vec()
                })
            }
            _ => Vec::new(),
        };
        Ok(json!({ "store": c.store, "role": c.role, "kind": c.kind.as_str(), "scope": c.scope, "messages": messages }))
    }
}
