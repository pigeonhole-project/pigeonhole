//! Every integration test of this crate, as modules of one test binary:
//! a binary per file linked the whole dependency graph again for each
//! file on every change. Run one file's tests with
//! `cargo test -p pigeonhole-sim --test it <file>::` (or nextest's
//! `-E 'binary_id(pigeonhole-sim::it) & test(/^<file>::/)'`).

mod cluster;
mod disk;
mod log_model;
mod retention;
mod s3_stress;
mod snapshot_crash;
mod takeover;
