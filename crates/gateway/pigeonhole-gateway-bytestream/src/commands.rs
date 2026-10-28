//! The CAS namespace of the metadata tablet: [`CasState`] is the in-memory
//! digest → extents map, changed only by [`CasCommand`]s (timestamps come in
//! the command).

use anyhow::Result;
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{Outcome, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const NAMESPACE: &str = "cas";

/// Commands of the `cas` namespace. Changing the shape of a variant or the
/// order of variants needs a new [`CasState::VERSION`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasCommand {
    /// Store `(hash, size)` → extents unless it exists; an existing entry is touched.
    PutEntry { hash: String, size: i64, extents: Vec<Extent>, at: String },
    TouchEntry { hash: String, size: i64, at: String },
    /// Remove an entry (no-op if missing).
    DropEntry { hash: String, size: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasReply {
    Stored,
    /// `PutEntry` found the digest already stored with these extents.
    Existing(Vec<Extent>),
    Touched,
    /// Chunk ids the dropped entry held, to release (empty if it was missing).
    Dropped(Vec<ChunkId>),
}

/// One stored digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CasRecord {
    pub extents: Vec<Extent>,
    /// RFC 3339, set by the proposer.
    pub last_access: String,
}

/// `(hash, size)` → record.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CasState {
    pub(crate) blobs: BTreeMap<(String, i64), CasRecord>,
}

impl StateMachine for CasState {
    const NAME: &'static str = NAMESPACE;
    const VERSION: u32 = 1;
    type Command = CasCommand;
    type Reply = CasReply;
    type Reject = pigeonhole_meta::Never;

    fn apply(&mut self, command: CasCommand) -> Result<Outcome<Self>> {
        Ok(Ok(match command {
            CasCommand::PutEntry { hash, size, extents, at } => {
                match self.blobs.get_mut(&(hash.clone(), size)) {
                    Some(r) => {
                        r.last_access = at;
                        CasReply::Existing(r.extents.clone())
                    }
                    None => {
                        self.blobs.insert((hash, size), CasRecord { extents, last_access: at });
                        CasReply::Stored
                    }
                }
            }
            CasCommand::TouchEntry { hash, size, at } => {
                if let Some(r) = self.blobs.get_mut(&(hash, size)) {
                    r.last_access = at;
                }
                CasReply::Touched
            }
            CasCommand::DropEntry { hash, size } => {
                let mut ids: Vec<ChunkId> = self
                    .blobs
                    .remove(&(hash, size))
                    .map(|r| r.extents.iter().map(|e| e.chunk).collect())
                    .unwrap_or_default();
                ids.sort_unstable();
                ids.dedup();
                CasReply::Dropped(ids)
            }
        }))
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

    #[test]
    fn put_touch_drop_and_snapshot_roundtrip() {
        let mut s = CasState::default();
        assert_eq!(apply(&mut s, put("a", 1, "t1")), CasReply::Stored);
        let existing = vec![Extent { chunk: 1, offset: 0, len: 3 }];
        assert_eq!(apply(&mut s, put("a", 9, "t2")), CasReply::Existing(existing), "existing entry wins");
        apply(&mut s, put("b", 2, "t3"));
        assert_eq!(s.blobs[&("a".to_string(), 3)].last_access, "t2");

        let snap = encode_state(&s);
        assert_eq!(encode_state(&decode_state::<CasState>(&snap).unwrap()), snap);

        let drop = CasCommand::DropEntry { hash: "a".into(), size: 3 };
        assert_eq!(apply(&mut s, drop.clone()), CasReply::Dropped(vec![1]));
        assert_eq!(apply(&mut s, drop), CasReply::Dropped(vec![]));
        assert_eq!(s.blobs.len(), 1);
    }

    /// Fails when the encoding of a command changes: bump `VERSION` and update
    /// the expected bytes (old logs then refuse to replay, as they should).
    #[test]
    fn command_encoding_is_pinned() {
        let cmd = CasCommand::TouchEntry { hash: "h".into(), size: 3, at: "t".into() };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [1, 1, b'h', 6, 1, b't']);
        assert_eq!(CasState::VERSION, 1);
    }
}
