//! The tablet's metrics (the [`metrics`] facade: no-op without a recorder).

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
