//! The node's metadata log as it lies in its store, read through the public
//! LogStore API (what a restore tool would do), its commands decoded per
//! namespace into JSON: the site's log inspector.

use anyhow::{Context, Result};
use futures::TryStreamExt;
use openraft::EntryPayload;
use pigeonhole_chunk_store::ChunkState;
use pigeonhole_config::dynamic::Overrides;
use pigeonhole_gateway_s3::S3State;
use pigeonhole_meta::raft::RaftEntry;
use pigeonhole_meta::{Command, LogStore, StateMachine};
use serde_json::{json, Value};

/// A command's typed form as JSON, by its namespace.
fn decode(command: &Command) -> Value {
    fn typed<S: StateMachine>(bytes: &[u8]) -> Value {
        match postcard::from_bytes::<S::Command>(bytes) {
            Ok(c) => serde_json::to_value(&c).unwrap_or_else(|e| json!({ "undecodable": e.to_string() })),
            Err(e) => json!({ "undecodable": e.to_string() }),
        }
    }
    match command.namespace.as_str() {
        ChunkState::NAME => typed::<ChunkState>(&command.command),
        S3State::NAME => typed::<S3State>(&command.command),
        Overrides::NAME => typed::<Overrides>(&command.command),
        _ => json!({ "bytes": command.command.len() }),
    }
}

/// Entries of `log` up to its head: the last `limit`, or `limit` from `from`.
pub async fn read(log: &dyn LogStore, from: Option<u64>, limit: usize) -> Result<Value> {
    let Some(found) = log.discover().await.context("discover the log")? else {
        return Ok(json!({ "head": 0, "base": null, "entries": [] }));
    };
    let head = found.head.position;
    let base = found.base.as_ref().map_or(0, |b| b.at);
    let start = from.unwrap_or_else(|| head.saturating_sub(limit as u64)).max(base);
    let entries: Vec<_> = log.read(start, found.head).try_collect().await.context("read the log")?;
    let mut out = Vec::new();
    for (position, entry) in entries.into_iter().take(limit) {
        let decoded: RaftEntry = postcard::from_bytes(&entry.payload).with_context(|| format!("entry {position}"))?;
        let (kind, commands) = match decoded.payload {
            EntryPayload::Blank => ("blank", Vec::new()),
            EntryPayload::Membership(_) => ("membership", Vec::new()),
            EntryPayload::Normal(batch) => (
                "commands",
                batch
                    .iter()
                    .map(|c| json!({ "namespace": c.namespace, "version": c.version, "command": decode(c) }))
                    .collect(),
            ),
        };
        out.push(json!({ "position": position, "term": entry.term, "kind": kind, "commands": commands }));
    }
    let base = found.base.map(|b| json!({ "at": b.at, "term": b.term }));
    Ok(json!({ "head": head, "base": base, "entries": out }))
}
