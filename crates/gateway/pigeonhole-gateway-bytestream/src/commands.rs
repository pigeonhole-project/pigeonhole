//! The CAS namespace of the metadata tablet: [`CasState`] is the in-memory
//! digest → extents map of the CAS and the action digest → `ActionResult`
//! map of the ActionCache, changed only by [`CasCommand`]s (timestamps come
//! in the command).

use anyhow::Result;
use pigeonhole_chunk_store::{unique_chunk_ids, ChunkId, Extent};
use pigeonhole_meta::{Outcome, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const NAMESPACE: &str = "cas";

/// `(hash, size)` of a digest.
pub type DigestKey = (String, i64);

/// A digest read at `at` (RFC 3339).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Access {
    pub hash: String,
    pub size: i64,
    pub at: String,
}

/// Commands of the `cas` namespace. Changing the shape of a variant or the
/// order of variants needs a new [`CasState::VERSION`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasCommand {
    /// Store `(hash, size)` → extents unless it exists; an existing entry is touched.
    PutEntry { hash: String, size: i64, extents: Vec<Extent>, at: String },
    /// The reads noted in memory since the last of these: each entry's last
    /// access moves up to its `at` (missing ones are skipped).
    TouchMany { blobs: Vec<Access>, actions: Vec<Access> },
    /// Remove a CAS entry (no-op if missing).
    DropEntry { hash: String, size: i64 },
    /// Remove the entries among these last accessed before `before`; those
    /// accessed since, and those missing, stay as they are.
    DropStale { blobs: Vec<DigestKey>, actions: Vec<DigestKey>, before: String },
    /// Store (or replace) the `ActionResult` of the action with this digest.
    PutAction { hash: String, size: i64, result: Vec<u8>, at: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasReply {
    Stored,
    /// `PutEntry` found the digest already stored with these extents.
    Existing(Vec<Extent>),
    Touched,
    /// Chunk ids the dropped entries held, to release (empty if none).
    Dropped(Vec<ChunkId>),
}

/// One stored digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CasRecord {
    pub extents: Vec<Extent>,
    /// RFC 3339, set by the proposer.
    pub last_access: String,
}

/// One cached action result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ActionRecord {
    /// The encoded `ActionResult`, as the client sent it.
    pub result: Vec<u8>,
    pub last_access: String,
}

/// `(hash, size)` → record, for CAS blobs and for actions.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CasState {
    pub(crate) blobs: BTreeMap<DigestKey, CasRecord>,
    pub(crate) actions: BTreeMap<DigestKey, ActionRecord>,
}

impl StateMachine for CasState {
    const NAME: &'static str = NAMESPACE;
    const VERSION: u32 = 2;
    type Command = CasCommand;
    type Reply = CasReply;
    type Reject = pigeonhole_meta::Never;

    fn apply(&mut self, command: CasCommand) -> Result<Outcome<Self>> {
        Ok(Ok(match command {
            CasCommand::PutEntry { hash, size, extents, at } => match self.blobs.get_mut(&(hash.clone(), size)) {
                Some(r) => {
                    touch(&mut r.last_access, at);
                    CasReply::Existing(r.extents.clone())
                }
                None => {
                    self.blobs.insert((hash, size), CasRecord { extents, last_access: at });
                    CasReply::Stored
                }
            },
            CasCommand::TouchMany { blobs, actions } => {
                self.touch_many(blobs, actions);
                CasReply::Touched
            }
            CasCommand::DropEntry { hash, size } => {
                let dropped = self.blobs.remove(&(hash, size));
                CasReply::Dropped(unique_chunk_ids(dropped.iter().flat_map(|r| &r.extents)))
            }
            CasCommand::DropStale { blobs, actions, before } => {
                CasReply::Dropped(self.drop_stale(blobs, actions, &before))
            }
            CasCommand::PutAction { hash, size, result, at } => {
                self.actions.insert((hash, size), ActionRecord { result, last_access: at });
                CasReply::Stored
            }
        }))
    }
}

impl CasState {
    fn touch_many(&mut self, blobs: Vec<Access>, actions: Vec<Access>) {
        for a in blobs {
            if let Some(r) = self.blobs.get_mut(&(a.hash, a.size)) {
                touch(&mut r.last_access, a.at);
            }
        }
        for a in actions {
            if let Some(r) = self.actions.get_mut(&(a.hash, a.size)) {
                touch(&mut r.last_access, a.at);
            }
        }
    }

    /// The chunks the dropped entries held: each holds one ref on each of
    /// its chunks.
    fn drop_stale(&mut self, blobs: Vec<DigestKey>, actions: Vec<DigestKey>, before: &str) -> Vec<ChunkId> {
        let mut dropped = Vec::new();
        for key in blobs {
            if self.blobs.get(&key).is_some_and(|r| r.last_access.as_str() < before) {
                dropped.extend(self.blobs.remove(&key).map(|r| unique_chunk_ids(&r.extents)).unwrap_or_default());
            }
        }
        for key in actions {
            if self.actions.get(&key).is_some_and(|r| r.last_access.as_str() < before) {
                self.actions.remove(&key);
            }
        }
        dropped.sort_unstable();
        dropped
    }
}

/// A last access only moves forward (touches flushed late carry older times).
fn touch(last_access: &mut String, at: String) {
    if at > *last_access {
        *last_access = at;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_meta::{decode_state, encode_state};

    fn apply(s: &mut CasState, cmd: CasCommand) -> CasReply {
        let Ok(reply) = s.apply(cmd).unwrap();
        reply
    }

    fn put(hash: &str, chunk: i64, at: &str) -> CasCommand {
        let extents = vec![Extent { chunk, offset: 0, len: 3 }];
        CasCommand::PutEntry { hash: hash.into(), size: 3, extents, at: at.into() }
    }

    fn key(hash: &str) -> DigestKey {
        (hash.to_string(), 3)
    }

    #[test]
    fn put_touch_drop_and_snapshot_roundtrip() {
        let mut s = CasState::default();
        assert_eq!(apply(&mut s, put("a", 1, "t1")), CasReply::Stored);
        let existing = vec![Extent { chunk: 1, offset: 0, len: 3 }];
        assert_eq!(apply(&mut s, put("a", 9, "t2")), CasReply::Existing(existing), "existing entry wins");
        apply(&mut s, put("b", 2, "t3"));
        assert_eq!(s.blobs[&key("a")].last_access, "t2");
        let action = CasCommand::PutAction { hash: "x".into(), size: 3, result: vec![1, 2], at: "t3".into() };
        assert_eq!(apply(&mut s, action), CasReply::Stored);

        let snap = encode_state(&s);
        assert_eq!(encode_state(&decode_state::<CasState>(&snap).unwrap()), snap);

        let drop = CasCommand::DropEntry { hash: "a".into(), size: 3 };
        assert_eq!(apply(&mut s, drop.clone()), CasReply::Dropped(vec![1]));
        assert_eq!(apply(&mut s, drop), CasReply::Dropped(vec![]));
        assert_eq!(s.blobs.len(), 1);
    }

    /// Touches only move forward; `DropStale` spares what was read since.
    #[test]
    fn touches_and_stale_drops() {
        let mut s = CasState::default();
        apply(&mut s, put("a", 1, "t1"));
        apply(&mut s, put("b", 2, "t1"));
        apply(&mut s, CasCommand::PutAction { hash: "x".into(), size: 3, result: vec![], at: "t1".into() });
        let at = |hash: &str, at: &str| Access { hash: hash.into(), size: 3, at: at.into() };
        let touch =
            CasCommand::TouchMany { blobs: vec![at("a", "t5"), at("gone", "t5")], actions: vec![at("x", "t5")] };
        assert_eq!(apply(&mut s, touch), CasReply::Touched);
        apply(&mut s, CasCommand::TouchMany { blobs: vec![at("a", "t4")], actions: vec![] });
        assert_eq!(s.blobs[&key("a")].last_access, "t5", "an older touch does not move it back");

        let stale = CasCommand::DropStale {
            blobs: vec![key("a"), key("b"), key("gone")],
            actions: vec![key("x")],
            before: "t3".into(),
        };
        assert_eq!(apply(&mut s, stale), CasReply::Dropped(vec![2]), "a was read since: it stays");
        assert_eq!(s.blobs.keys().collect::<Vec<_>>(), [&key("a")]);
        assert_eq!(s.actions.len(), 1);
    }

    /// Fails when the encoding of a command changes: bump `VERSION` and update
    /// the expected bytes (old logs then refuse to replay, as they should).
    #[test]
    fn command_encoding_is_pinned() {
        let cmd = CasCommand::DropEntry { hash: "h".into(), size: 3 };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [2, 1, b'h', 6]);
        let cmd = CasCommand::PutAction { hash: "h".into(), size: 3, result: vec![7], at: "t".into() };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [4, 1, b'h', 6, 1, 7, 1, b't']);
        assert_eq!(CasState::VERSION, 2);
    }
}
