//! The tablet's metrics (the [`metrics`] facade: no-op without a recorder).

use crate::raft::NodeId;
use openraft::{BasicNode, RaftMetrics, ServerState};
use pigeonhole_types::MetricDoc;
use std::time::Duration;

pub const METRICS: &[MetricDoc] = &[
    MetricDoc::histogram(
        "pigeonhole_meta_commit_seconds",
        &["outcome"],
        "Time to commit and apply one log entry (a batch of proposals), from the batcher",
    ),
    MetricDoc::histogram(
        "pigeonhole_meta_propose_seconds",
        &["outcome"],
        "A proposal's whole wait, as its caller sees it: queued, then committed and applied",
    ),
    MetricDoc::histogram(
        "pigeonhole_meta_queue_wait_seconds",
        &[],
        "Time a proposal waits in the batcher's queue (behind the entry being written)",
    ),
    MetricDoc::gauge("pigeonhole_meta_queue_depth", &[], "Proposals waiting in the batcher's queue"),
    MetricDoc::histogram("pigeonhole_meta_commands_per_entry", &[], "Commands group-committed in one log entry"),
    MetricDoc::histogram("pigeonhole_meta_proposals_per_entry", &[], "Proposals group-committed in one log entry"),
    MetricDoc::counter(
        "pigeonhole_meta_batch_full_total",
        &[],
        "Entries that reached MAX_BATCH commands (a proposal that did not fit waits for the next)",
    ),
    MetricDoc::histogram("pigeonhole_meta_log_append_seconds", &["outcome"], "LogStore append of a batch of entries (fsync / chat message)"),
    MetricDoc::histogram(
        "pigeonhole_meta_log_write_seconds",
        &["op", "outcome"],
        "The log writer's other operations (rebase, truncate, vote): appends queue behind them",
    ),
    MetricDoc::histogram(
        "pigeonhole_meta_log_collect_seconds",
        &["outcome"],
        "Dropping what lies behind a new base (in the background, after a rebase)",
    ),
    MetricDoc::histogram(
        "pigeonhole_meta_lease_wait_seconds",
        &["reason"],
        "A lease log's waits: renew (a renewal before a write), claim (a claimant waiting out the old lease)",
    ),
    MetricDoc::counter("pigeonhole_meta_log_entries_total", &[], "Entries appended to the log"),
    MetricDoc::counter("pigeonhole_meta_log_bytes_total", &[], "Entry payload bytes appended to the log"),
    MetricDoc::histogram("pigeonhole_meta_snapshot_seconds", &[], "Time to encode and store a snapshot"),
    MetricDoc::gauge("pigeonhole_meta_snapshot_bytes", &[], "Size of the newest snapshot (stored, or loaded on boot)"),
    MetricDoc::counter("pigeonhole_meta_snapshots_total", &[], "Snapshots stored"),
    MetricDoc::gauge("pigeonhole_meta_applied_position", &[], "Position of the last applied entry"),
    MetricDoc::gauge("pigeonhole_meta_base_position", &[], "Position of the snapshot the log starts from (the log holds applied - base entries)"),
    MetricDoc::gauge("pigeonhole_meta_generation", &[], "Generation this tablet claimed on boot"),
    MetricDoc::gauge("pigeonhole_meta_stopped", &[], "1 once the tablet stopped (fenced or failed)"),
    MetricDoc::gauge("pigeonhole_meta_boot_seconds", &[], "Time the last boot took: claim, load the snapshot, replay the log"),
    MetricDoc::gauge("pigeonhole_meta_boot_replayed_entries", &[], "Entries the last boot replayed on top of its snapshot"),
    MetricDoc::counter(
        "pigeonhole_meta_snapshot_fallbacks_total",
        &[],
        "Boots that could not load the newest base's snapshot and started from an older one the log keeps",
    ),
    MetricDoc::gauge("pigeonhole_meta_raft_node", &[], "This node's Raft id"),
    MetricDoc::gauge(
        "pigeonhole_meta_raft_state",
        &["state"],
        "1 for the node's Raft role (leader, follower, candidate, learner, shutdown), 0 for the others",
    ),
    MetricDoc::gauge("pigeonhole_meta_raft_term", &[], "The node's current Raft term"),
    MetricDoc::gauge("pigeonhole_meta_raft_leader", &[], "The leader's node id as this node knows it, -1 if none"),
    MetricDoc::gauge("pigeonhole_meta_raft_last_log_index", &[], "Index of the last entry in this node's log"),
    MetricDoc::gauge("pigeonhole_meta_raft_last_applied_index", &[], "Index of the last entry this node applied"),
    MetricDoc::gauge(
        "pigeonhole_meta_raft_replication_lag",
        &["peer"],
        "On the leader: entries a peer's log is behind the leader's (NaN on a follower)",
    ),
    MetricDoc::gauge(
        "pigeonhole_meta_raft_quorum_ack_age_seconds",
        &[],
        "On the leader: time since a quorum last acknowledged it (NaN on a follower); grows when the leader is cut off",
    ),
];

fn outcome(ok: bool) -> &'static str {
    if ok {
        "ok"
    } else {
        "error"
    }
}

pub(crate) fn record_commit(ok: bool, commands: usize, proposals: usize, d: Duration) {
    metrics::histogram!("pigeonhole_meta_commit_seconds", "outcome" => outcome(ok)).record(d.as_secs_f64());
    metrics::histogram!("pigeonhole_meta_commands_per_entry").record(commands as f64);
    metrics::histogram!("pigeonhole_meta_proposals_per_entry").record(proposals as f64);
}

pub(crate) fn record_propose(ok: bool, d: Duration) {
    metrics::histogram!("pigeonhole_meta_propose_seconds", "outcome" => outcome(ok)).record(d.as_secs_f64());
}

pub(crate) fn record_queue_wait(d: Duration) {
    metrics::histogram!("pigeonhole_meta_queue_wait_seconds").record(d.as_secs_f64());
}

pub(crate) fn record_enqueued() {
    metrics::gauge!("pigeonhole_meta_queue_depth").increment(1.0);
}

pub(crate) fn record_dequeued(n: usize) {
    metrics::gauge!("pigeonhole_meta_queue_depth").decrement(n as f64);
}

pub(crate) fn record_batch_full() {
    metrics::counter!("pigeonhole_meta_batch_full_total").increment(1);
}

pub(crate) fn record_append(ok: bool, entries: usize, bytes: usize, d: Duration) {
    metrics::histogram!("pigeonhole_meta_log_append_seconds", "outcome" => outcome(ok)).record(d.as_secs_f64());
    if ok {
        metrics::counter!("pigeonhole_meta_log_entries_total").increment(entries as u64);
        metrics::counter!("pigeonhole_meta_log_bytes_total").increment(bytes as u64);
    }
}

pub(crate) fn record_snapshot(bytes: usize, d: Duration) {
    metrics::histogram!("pigeonhole_meta_snapshot_seconds").record(d.as_secs_f64());
    metrics::gauge!("pigeonhole_meta_snapshot_bytes").set(bytes as f64);
    metrics::counter!("pigeonhole_meta_snapshots_total").increment(1);
}

/// The snapshot a boot started from.
pub(crate) fn record_loaded_snapshot(bytes: usize) {
    metrics::gauge!("pigeonhole_meta_snapshot_bytes").set(bytes as f64);
}

pub(crate) fn record_snapshot_fallback() {
    metrics::counter!("pigeonhole_meta_snapshot_fallbacks_total").increment(1);
}

pub(crate) fn record_boot(replayed: u64, d: Duration) {
    metrics::gauge!("pigeonhole_meta_boot_seconds").set(d.as_secs_f64());
    metrics::gauge!("pigeonhole_meta_boot_replayed_entries").set(replayed as f64);
}

pub(crate) fn record_positions(applied: u64, base: u64, generation: u64, stopped: bool) {
    metrics::gauge!("pigeonhole_meta_applied_position").set(applied as f64);
    metrics::gauge!("pigeonhole_meta_base_position").set(base as f64);
    metrics::gauge!("pigeonhole_meta_generation").set(generation as f64);
    metrics::gauge!("pigeonhole_meta_stopped").set(if stopped { 1.0 } else { 0.0 });
}

const RAFT_STATES: [(ServerState, &str); 5] = [
    (ServerState::Leader, "leader"),
    (ServerState::Follower, "follower"),
    (ServerState::Candidate, "candidate"),
    (ServerState::Learner, "learner"),
    (ServerState::Shutdown, "shutdown"),
];

pub(crate) fn record_raft(m: &RaftMetrics<NodeId, BasicNode>) {
    metrics::gauge!("pigeonhole_meta_raft_node").set(m.id as f64);
    for (state, name) in RAFT_STATES {
        metrics::gauge!("pigeonhole_meta_raft_state", "state" => name).set(if m.state == state { 1.0 } else { 0.0 });
    }
    metrics::gauge!("pigeonhole_meta_raft_term").set(m.current_term as f64);
    metrics::gauge!("pigeonhole_meta_raft_leader").set(m.current_leader.map_or(-1.0, |l| l as f64));
    let last = m.last_log_index.map_or(-1.0, |i| i as f64);
    metrics::gauge!("pigeonhole_meta_raft_last_log_index").set(last);
    metrics::gauge!("pigeonhole_meta_raft_last_applied_index").set(m.last_applied.map_or(-1.0, |l| l.index as f64));
    let ack = m.millis_since_quorum_ack.map_or(f64::NAN, |ms| ms as f64 / 1000.0);
    metrics::gauge!("pigeonhole_meta_raft_quorum_ack_age_seconds").set(ack);
    // Every voter but this node gets a value, so a former leader's lags go
    // NaN rather than freeze.
    for peer in m.membership_config.membership().voter_ids().filter(|p| *p != m.id) {
        let matched = m.replication.as_ref().map(|r| r.get(&peer).map(|l| l.map_or(-1.0, |l| l.index as f64)));
        let lag = match matched {
            Some(Some(matched)) => last - matched,
            _ => f64::NAN,
        };
        metrics::gauge!("pigeonhole_meta_raft_replication_lag", "peer" => peer.to_string()).set(lag);
    }
}

pub(crate) fn record_log_write(op: &'static str, ok: bool, d: Duration) {
    metrics::histogram!("pigeonhole_meta_log_write_seconds", "op" => op, "outcome" => outcome(ok)).record(d.as_secs_f64());
}

pub(crate) fn record_lease_wait(reason: &'static str, d: Duration) {
    metrics::histogram!("pigeonhole_meta_lease_wait_seconds", "reason" => reason).record(d.as_secs_f64());
}

pub(crate) fn record_collect(ok: bool, d: Duration) {
    metrics::histogram!("pigeonhole_meta_log_collect_seconds", "outcome" => outcome(ok)).record(d.as_secs_f64());
}
