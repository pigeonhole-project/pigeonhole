//! A frame as the message inspector reads it.

use openraft::error::{Fatal, RaftError};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
use openraft_chat::frame::{seal, Body, Envelope, Frame, FRAME_VERSION};
use openraft_chat::view::inspect;
use std::io::Cursor;

openraft::declare_raft_types!(
    pub Config: D = String, R = String,
);

const KEY: [u8; 32] = [7; 32];

fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

fn envelope(to: u64, rpc: u64, body: Body<Config>) -> Envelope<Config> {
    Envelope { to, rpc, body }
}

#[test]
fn every_envelope_says_what_it_asks_or_answers() {
    let entries = (5..=7).map(|i| Entry::<Config> { log_id: log_id(3, i), payload: EntryPayload::Blank }).collect();
    let frame = Frame::<Config> {
        version: FRAME_VERSION,
        cluster: 42,
        from: 1,
        seq: 812,
        envelopes: vec![
            envelope(
                2,
                10,
                Body::AppendEntries(AppendEntriesRequest {
                    vote: Vote::new_committed(3, 1),
                    prev_log_id: Some(log_id(3, 4)),
                    entries,
                    leader_commit: Some(log_id(3, 4)),
                }),
            ),
            envelope(3, 11, Body::Appended(Ok(AppendEntriesResponse::Conflict))),
            envelope(2, 12, Body::Vote(VoteRequest::new(Vote::new(4, 1), Some(log_id(3, 7))))),
            envelope(3, 13, Body::Voted(Ok(VoteResponse::new(Vote::new(4, 3), None, false)))),
            envelope(2, 14, Body::Appended(Err(RaftError::Fatal(Fatal::Stopped)))),
        ],
    };
    let v = inspect::<Config>(&seal(&frame, &KEY), &KEY).unwrap();
    assert_eq!((v.from, v.seq, v.envelopes.len()), (1, 812, 5));

    let append = &v.envelopes[0];
    assert_eq!((append.to, append.rpc, append.kind, append.reply), (2, 10, "append", false));
    assert_eq!((append.term, append.by, append.committed), (Some(3), Some(1), Some(true)));
    assert_eq!((append.prev, append.entries, append.commit), (Some(4), Some((5, 7)), Some(4)));

    assert_eq!((v.envelopes[1].reply, v.envelopes[1].result), (true, Some("conflict")));

    let vote = &v.envelopes[2];
    assert_eq!(
        (vote.kind, vote.term, vote.by, vote.committed, vote.last),
        ("vote", Some(4), Some(1), Some(false), Some(7))
    );

    let voted = &v.envelopes[3];
    assert_eq!((voted.kind, voted.reply, voted.result, voted.by), ("vote", true, Some("refused"), Some(3)));

    let failed = &v.envelopes[4];
    assert_eq!(failed.result, Some("error"));
    assert!(failed.error.as_deref().unwrap().contains("stopped"), "{:?}", failed.error);
}

#[test]
fn a_heartbeat_has_no_entries_and_a_foreign_key_no_view() {
    let frame = Frame::<Config> {
        version: FRAME_VERSION,
        cluster: 42,
        from: 1,
        seq: 1,
        envelopes: vec![envelope(
            2,
            1,
            Body::AppendEntries(AppendEntriesRequest {
                vote: Vote::new_committed(2, 1),
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            }),
        )],
    };
    let sealed = seal(&frame, &KEY);
    let hb = &inspect::<Config>(&sealed, &KEY).unwrap().envelopes[0];
    assert_eq!((hb.kind, hb.entries, hb.prev, hb.commit), ("heartbeat", None, None, None));
    assert!(inspect::<Config>(&sealed, &[8; 32]).is_err());
}
