//! Every integration test of this crate, as modules of one test binary:
//! a binary per file linked the whole dependency graph again for each
//! file on every change. Run one file's tests with
//! `cargo test -p pigeonhole --test it <file>::` (or nextest's
//! `-E 'binary_id(pigeonhole::it) & test(/^<file>::/)'`).

mod admin_http;
mod e2e_memory_restore;
mod gateway_metrics;
mod http_timeout;
mod leader_http;
mod meta_replay;
mod metrics_doc;
mod raft_telegram;
mod raft_telegram_live;
mod s3_api;
mod sweep_stress;
