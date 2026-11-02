//! A frame as a person reads it (an admin UI's message inspector): who sent
//! it, and what each envelope asks or answers. Log ids are their indexes.

use crate::frame::{open, Body, Envelope, Frame, Key};
use anyhow::Result;
use openraft::{LogId, RaftLogId, RaftTypeConfig, Vote};
use serde::Serialize;

#[derive(Serialize)]
#[serde(bound = "")]
pub struct FrameView<C: RaftTypeConfig> {
    pub from: C::NodeId,
    pub seq: u64,
    pub envelopes: Vec<EnvelopeView<C>>,
}

/// One envelope; a field is left out where its body has none.
#[derive(Serialize)]
#[serde(bound = "")]
pub struct EnvelopeView<C: RaftTypeConfig> {
    pub to: C::NodeId,
    pub rpc: u64,
    /// [`Body::kind`].
    pub kind: &'static str,
    pub reply: bool,
    /// The vote it carries: its term, the node it is for, whether a
    /// quorum granted it (a leader's).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by: Option<C::NodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed: Option<bool>,
    /// An append: the entry before its own, its own (first, last), the
    /// leader's commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries: Option<(u64, u64)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<u64>,
    /// A vote request's or response's last log id; a snapshot's last entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<u64>,
    /// A snapshot chunk: where in the snapshot, how many bytes, the last one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub done: Option<bool>,
    /// A response: `ok`, `partial`, `conflict`, `higher_vote`, `granted`,
    /// `refused`, `error`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<&'static str>,
    /// An error response's error; a partial success's last matched entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched: Option<u64>,
}

/// The frame in `bytes` (sealed with `key`), as [`FrameView`].
pub fn inspect<C: RaftTypeConfig>(bytes: &[u8], key: &Key) -> Result<FrameView<C>> {
    Ok(FrameView::of(&open::<C>(bytes, key)?))
}

impl<C: RaftTypeConfig> FrameView<C> {
    pub fn of(frame: &Frame<C>) -> Self {
        Self {
            from: frame.from.clone(),
            seq: frame.seq,
            envelopes: frame.envelopes.iter().map(EnvelopeView::of).collect(),
        }
    }
}

fn index<NID: openraft::NodeId>(id: Option<&LogId<NID>>) -> Option<u64> {
    id.map(|l| l.index)
}

impl<C: RaftTypeConfig> EnvelopeView<C> {
    fn of(env: &Envelope<C>) -> Self {
        let mut v = Self {
            to: env.to.clone(),
            rpc: env.rpc,
            kind: env.body.kind(),
            reply: !env.body.is_request(),
            term: None,
            by: None,
            committed: None,
            prev: None,
            entries: None,
            commit: None,
            last: None,
            offset: None,
            bytes: None,
            done: None,
            result: None,
            error: None,
            matched: None,
        };
        match &env.body {
            Body::AppendEntries(r) => {
                v.vote(&r.vote);
                v.prev = index(r.prev_log_id.as_ref());
                let ids = r.entries.first().zip(r.entries.last());
                v.entries = ids.map(|(a, b)| (a.get_log_id().index, b.get_log_id().index));
                v.commit = index(r.leader_commit.as_ref());
            }
            Body::Appended(Ok(r)) => {
                use openraft::raft::AppendEntriesResponse as R;
                v.result = Some(match r {
                    R::Success => "ok",
                    R::PartialSuccess(m) => {
                        v.matched = index(m.as_ref());
                        "partial"
                    }
                    R::Conflict => "conflict",
                    R::HigherVote(h) => {
                        v.vote(h);
                        "higher_vote"
                    }
                });
            }
            Body::Vote(r) => {
                v.vote(&r.vote);
                v.last = index(r.last_log_id.as_ref());
            }
            Body::Voted(Ok(r)) => {
                v.vote(&r.vote);
                v.last = index(r.last_log_id.as_ref());
                v.result = Some(if r.vote_granted { "granted" } else { "refused" });
            }
            Body::InstallSnapshot(r) => {
                v.vote(&r.vote);
                v.last = index(r.meta.last_log_id.as_ref());
                (v.offset, v.bytes, v.done) = (Some(r.offset), Some(r.data.len()), Some(r.done));
            }
            Body::Installed(Ok(r)) => {
                v.vote(&r.vote);
                v.result = Some("ok");
            }
            Body::Appended(Err(e)) | Body::Voted(Err(e)) => v.failed(e.to_string()),
            Body::Installed(Err(e)) => v.failed(e.to_string()),
        }
        v
    }

    fn vote(&mut self, vote: &Vote<C::NodeId>) {
        self.term = Some(vote.leader_id().get_term());
        self.by = vote.leader_id().voted_for();
        self.committed = Some(vote.is_committed());
    }

    fn failed(&mut self, error: String) {
        self.result = Some("error");
        self.error = Some(error);
    }
}
