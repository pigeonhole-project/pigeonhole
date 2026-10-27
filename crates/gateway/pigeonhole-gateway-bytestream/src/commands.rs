//! The CAS namespace of the metadata tablet: [`CasState`] is the in-memory
//! digest → extents map, changed only by `cas` commands (timestamps come in
//! the command).

use anyhow::{Context, Result};
use pigeonhole_chunk_store::Extent;
use pigeonhole_meta::{Applied, StateMachine};
use prost::Message;
use std::collections::BTreeMap;

pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/pigeonhole.cas.v1.rs"));
}

pub(crate) const NAMESPACE: &str = "cas";

/// One stored digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CasRecord {
    pub extents: Vec<Extent>,
    /// RFC 3339, set by the proposer.
    pub last_access: String,
}

/// `(hash, size)` → record.
#[derive(Debug, Default)]
pub struct CasState {
    pub(crate) blobs: BTreeMap<(String, i64), CasRecord>,
}

impl StateMachine for CasState {
    const NAME: &'static str = NAMESPACE;

    fn apply(&mut self, command: &[u8]) -> Result<Applied> {
        use pb::command::Op;
        let cmd = pb::Command::decode(command).context("decode cas command")?;
        let reply = match cmd.op.context("empty cas command")? {
            Op::PutEntry(c) => self.put_entry(c).encode_to_vec(),
            Op::TouchEntry(c) => {
                self.touch(c.hash, c.size, c.at);
                Vec::new()
            }
            Op::DropEntry(c) => self.drop_entry(c.hash, c.size).encode_to_vec(),
        };
        Ok(Applied::Commit(reply))
    }

    fn snapshot(&self) -> Vec<u8> {
        let entries = self
            .blobs
            .iter()
            .map(|((hash, size), r)| pb::SnapshotEntry {
                hash: hash.clone(),
                size: *size,
                extents: to_pb(&r.extents),
                last_access: r.last_access.clone(),
            })
            .collect();
        pb::Snapshot { entries }.encode_to_vec()
    }

    fn restore(data: &[u8]) -> Result<Self> {
        let snap = pb::Snapshot::decode(data).context("decode cas snapshot")?;
        let blobs = snap
            .entries
            .into_iter()
            .map(|e| {
                let record = CasRecord { extents: from_pb(e.extents), last_access: e.last_access };
                ((e.hash, e.size), record)
            })
            .collect();
        Ok(Self { blobs })
    }
}

impl CasState {
    fn touch(&mut self, hash: String, size: i64, at: String) {
        if let Some(r) = self.blobs.get_mut(&(hash, size)) {
            r.last_access = at;
        }
    }

    fn put_entry(&mut self, c: pb::PutEntry) -> pb::PutResult {
        match self.blobs.get_mut(&(c.hash.clone(), c.size)) {
            Some(r) => {
                r.last_access = c.at;
                pb::PutResult { existed: true, existing: to_pb(&r.extents) }
            }
            None => {
                let record = CasRecord { extents: from_pb(c.extents), last_access: c.at };
                self.blobs.insert((c.hash, c.size), record);
                pb::PutResult::default()
            }
        }
    }

    fn drop_entry(&mut self, hash: String, size: i64) -> pb::Released {
        let Some(r) = self.blobs.remove(&(hash, size)) else {
            return pb::Released::default();
        };
        let mut chunk_ids: Vec<_> = r.extents.iter().map(|e| e.chunk).collect();
        chunk_ids.sort_unstable();
        chunk_ids.dedup();
        pb::Released { chunk_ids }
    }
}

pub(crate) fn to_pb(extents: &[Extent]) -> Vec<pb::Extent> {
    extents
        .iter()
        .map(|e| pb::Extent {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        })
        .collect()
}

pub(crate) fn from_pb(extents: Vec<pb::Extent>) -> Vec<Extent> {
    extents
        .into_iter()
        .map(|e| Extent {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(s: &mut CasState, op: pb::command::Op) -> Vec<u8> {
        match s.apply(&pb::Command { op: Some(op) }.encode_to_vec()).unwrap() {
            Applied::Commit(reply) => reply,
            Applied::Reject(_) => panic!("cas never rejects"),
        }
    }

    fn put(hash: &str, chunk: i64, at: &str) -> pb::command::Op {
        let extents = vec![pb::Extent { chunk, offset: 0, len: 3 }];
        pb::command::Op::PutEntry(pb::PutEntry { hash: hash.into(), size: 3, extents, at: at.into() })
    }

    #[test]
    fn put_touch_drop_and_snapshot_roundtrip() {
        let mut s = CasState::default();
        let r = pb::PutResult::decode(apply(&mut s, put("a", 1, "t1")).as_slice()).unwrap();
        assert!(!r.existed);
        let r = pb::PutResult::decode(apply(&mut s, put("a", 9, "t2")).as_slice()).unwrap();
        assert_eq!((r.existed, r.existing[0].chunk), (true, 1), "existing entry wins");
        apply(&mut s, put("b", 2, "t3"));
        let key = ("a".to_string(), 3);
        assert_eq!(s.blobs[&key].last_access, "t2");

        let snap = s.snapshot();
        assert_eq!(CasState::restore(&snap).unwrap().snapshot(), snap);

        let drop = pb::command::Op::DropEntry(pb::DropEntry { hash: "a".into(), size: 3 });
        let released = pb::Released::decode(apply(&mut s, drop.clone()).as_slice()).unwrap();
        assert_eq!(released.chunk_ids, vec![1]);
        assert!(pb::Released::decode(apply(&mut s, drop).as_slice()).unwrap().chunk_ids.is_empty());
        assert_eq!(s.blobs.len(), 1);
    }
}
