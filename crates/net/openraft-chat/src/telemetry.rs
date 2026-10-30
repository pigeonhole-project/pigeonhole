//! The transport's metrics (the [`metrics`] facade: no-op without a recorder).

use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// One metric the transport records, for a binary to describe and document.
#[derive(Clone, Copy, Debug)]
pub struct MetricInfo {
    pub name: &'static str,
    pub kind: MetricKind,
    pub labels: &'static [&'static str],
    pub help: &'static str,
}

const fn m(name: &'static str, kind: MetricKind, labels: &'static [&'static str], help: &'static str) -> MetricInfo {
    MetricInfo { name, kind, labels, help }
}

pub const METRICS: &[MetricInfo] = &[
    m(
        "openraft_chat_posts_total",
        MetricKind::Counter,
        &["outcome"],
        "Frames this node posted into its outbox (outcome: ok, error)",
    ),
    m("openraft_chat_post_seconds", MetricKind::Histogram, &["outcome"], "Time one post into the outbox took"),
    m("openraft_chat_envelopes_per_frame", MetricKind::Histogram, &[], "Envelopes (requests and responses) coalesced in one posted frame"),
    m(
        "openraft_chat_call_seconds",
        MetricKind::Histogram,
        &["peer", "rpc", "outcome"],
        "A request's round trip, from queued to its response (outcome: ok, timeout)",
    ),
    m(
        "openraft_chat_frames_received_total",
        MetricKind::Counter,
        &["peer", "outcome"],
        "Frames read from a peer's outbox (outcome: ok, duplicate, bad_frame, foreign)",
    ),
    m("openraft_chat_read_errors_total", MetricKind::Counter, &["peer"], "Failed reads of a peer's outbox"),
    m("openraft_chat_requests_served_total", MetricKind::Counter, &["peer", "rpc"], "Requests from a peer handed to this node's Raft"),
];

fn outcome(ok: bool) -> &'static str {
    if ok {
        "ok"
    } else {
        "error"
    }
}

pub(crate) fn record_post(ok: bool, envelopes: usize, d: Duration) {
    metrics::counter!("openraft_chat_posts_total", "outcome" => outcome(ok)).increment(1);
    metrics::histogram!("openraft_chat_post_seconds", "outcome" => outcome(ok)).record(d.as_secs_f64());
    metrics::histogram!("openraft_chat_envelopes_per_frame").record(envelopes as f64);
}

pub(crate) fn record_call(peer: String, rpc: &'static str, outcome: &'static str, d: Duration) {
    metrics::histogram!("openraft_chat_call_seconds", "peer" => peer, "rpc" => rpc, "outcome" => outcome)
        .record(d.as_secs_f64());
}

pub(crate) fn record_frame(peer: String, outcome: &'static str) {
    metrics::counter!("openraft_chat_frames_received_total", "peer" => peer, "outcome" => outcome).increment(1);
}

pub(crate) fn record_read_error(peer: String) {
    metrics::counter!("openraft_chat_read_errors_total", "peer" => peer).increment(1);
}

pub(crate) fn record_served(peer: String, rpc: &'static str) {
    metrics::counter!("openraft_chat_requests_served_total", "peer" => peer, "rpc" => rpc).increment(1);
}
