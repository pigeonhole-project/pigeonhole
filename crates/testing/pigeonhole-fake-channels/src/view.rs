//! The fake channels as the admin's UI sees them: what each holds, read
//! from its state (the last messages, the pinned ones, the log's posts
//! and an outbox's Raft frames decoded), and the faults only fakes allow
//! (down, wiped).

use crate::world::World;
use anyhow::Result;
use openraft_chat::Key;
use pigeonhole_admin::stores::{log_post, StoreRef};
use pigeonhole_admin::Channels;
use pigeonhole_config::instances::InstanceKind;
use pigeonhole_config::stores::FakeChannel;
use pigeonhole_meta::log::chain::summarize;
use pigeonhole_meta::raft::TypeConfig;
use pigeonhole_storage_api::fake::FakeHost;
use pigeonhole_storage_discord::fake::state::{DiscordChannelState, Message as DcMessage};
use pigeonhole_storage_telegram::fake::state::{Message as TgMessage, TelegramChannelState};
use serde_json::{json, Value};
use std::sync::Arc;

pub struct FakeView {
    pub world: Arc<World>,
    /// The fake channels among the node's stores.
    stores: Vec<StoreRef>,
    /// The cluster's key, to open the outboxes' frames with.
    key: Option<Key>,
}

impl FakeView {
    pub fn new(world: Arc<World>, stores: &[StoreRef], key: Option<Key>) -> Self {
        Self { world, stores: stores.iter().filter(|s| s.fake.is_some()).cloned().collect(), key }
    }

    fn find(&self, store: &str) -> Result<&FakeChannel> {
        self.stores
            .iter()
            .find_map(|s| s.fake.as_ref().filter(|_| s.store == store))
            .ok_or_else(|| anyhow::anyhow!("no fake channel {store:?}"))
    }

    /// Channel `s` as it is: how many messages it holds, the last `limit`
    /// and the pinned ones wherever they are.
    async fn describe_one(&self, s: &StoreRef, limit: usize) -> Result<Value> {
        let f = s.fake.as_ref().expect("a fake");
        let (total, messages, pinned): (usize, Vec<Value>, Vec<Value>) = match f.kind {
            InstanceKind::Telegram => {
                let ch = self.world.telegram(&f.scope, &f.backing).await?;
                ch.host().read(|st: &TelegramChannelState| {
                    let one = |(id, m): (i64, &TgMessage)| {
                        let mut v = telegram(id, st.date_ms(id), m, s.role);
                        if s.role == "outbox" {
                            v["frame"] = frame(m, self.key.as_ref());
                        }
                        v
                    };
                    let all: Vec<Value> = st.messages().map(one).collect();
                    let pinned = st.pinned().and_then(|p| st.message(p).map(|m| one((p, m)))).into_iter().collect();
                    (all.len(), all[all.len().saturating_sub(limit)..].to_vec(), pinned)
                })
            }
            InstanceKind::Discord => {
                let ch = self.world.discord(&f.scope, &f.backing).await?;
                ch.host().read(|st: &DiscordChannelState| {
                    let all: Vec<Value> = st.messages().map(|(id, m)| discord(id, m, s.role)).collect();
                    let pins = st.pinned();
                    let pinned = st
                        .messages()
                        .filter(|(id, _)| pins.contains(id))
                        .map(|(id, m)| discord(id, m, s.role))
                        .collect();
                    (all.len(), all[all.len().saturating_sub(limit)..].to_vec(), pinned)
                })
            }
            _ => (0, Vec::new(), Vec::new()),
        };
        let mut v = s.json();
        v["down"] = json!(self.world.is_down(f));
        v["total"] = json!(total);
        v["messages"] = json!(messages);
        v["pinned"] = json!(pinned);
        Ok(v)
    }
}

#[async_trait::async_trait]
impl Channels for FakeView {
    async fn describe(&self, limit: usize) -> Result<Value> {
        let mut out = Vec::new();
        for s in &self.stores {
            out.push(self.describe_one(s, limit).await?);
        }
        Ok(json!({ "channels": out }))
    }

    fn holds(&self, store: &str) -> bool {
        self.stores.iter().any(|s| s.store == store)
    }

    async fn delete_message(&self, store: &str, id: &str) -> Result<()> {
        self.world.delete_message(self.find(store)?, id).await
    }

    async fn fault(&self, op: &str, store: &str, down: bool) -> Result<Value> {
        let f = self.find(store)?;
        Ok(match op {
            "outage" => {
                self.world.set_down(f, down);
                json!({ "down": down })
            }
            _ => json!({ "deleted": self.world.wipe(f).await? }),
        })
    }
}

/// A text message's blob: `name len\n<payload>`, its name, size and the start
/// of the payload; in the log's channel, what the post is to the log.
fn text(content: &str, role: &str, kind: InstanceKind) -> Value {
    let (head, payload) = content.split_once('\n').unwrap_or((content, ""));
    let (name, len) = head.rsplit_once(' ').unwrap_or((head, "0"));
    let preview: String = payload.chars().take(24).collect();
    let mut v = json!({ "kind": "text", "name": name, "size": len.parse::<u64>().unwrap_or(0), "preview": preview });
    if role == "log" {
        let data = pigeonhole_storage_api::text::decode(content).map(|(_, b)| b);
        let summary = data.and_then(|d| match kind {
            InstanceKind::Telegram => summarize::<pigeonhole_storage_telegram::TelegramId>(&d).ok(),
            InstanceKind::Discord => summarize::<pigeonhole_storage_discord::DiscordId>(&d).ok(),
            _ => None,
        });
        v["log"] = log_post(name, summary);
    }
    v
}

/// An outbox's message as the Raft frame it holds (openraft_chat::view):
/// `{error}` when it holds none, or one of another key.
fn frame(m: &TgMessage, key: Option<&Key>) -> Value {
    let bytes = match m {
        TgMessage::Text(t) => pigeonhole_storage_api::text::decode(t).map(|(_, b)| b.to_vec()),
        TgMessage::Doc { blob, .. } => Some(blob.clone()),
        TgMessage::Other => None,
    };
    let opened = match (bytes, key) {
        (Some(b), Some(key)) => openraft_chat::view::inspect::<TypeConfig>(&b, key).map_err(|e| format!("{e:#}")),
        (None, _) => Err("not a frame".into()),
        (_, None) => Err("no cluster key".into()),
    };
    match opened {
        Ok(view) => serde_json::to_value(view).expect("a frame view encodes"),
        Err(error) => json!({ "error": error }),
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
                v["log"] = log_post(name, None);
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
                v["log"] = log_post(&f.name, None);
            }
            v
        }
        None => text(&m.content, role, InstanceKind::Discord),
    };
    v["id"] = json!(id.to_string());
    v["at"] = json!(pigeonhole_storage_discord::snowflake_timestamp_ms(id));
    v
}
