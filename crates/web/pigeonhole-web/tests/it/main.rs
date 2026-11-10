//! Every integration test of this crate, as modules of one test binary:
//! a binary per file linked the whole dependency graph again for each
//! file on every change. Run one file's tests with
//! `cargo test -p pigeonhole-web --test it <file>::` (or nextest's
//! `-E 'binary_id(pigeonhole-web::it) & test(/^<file>::/)'`).

mod admin_ops;
mod bots;
mod cluster;
mod disk_view;
mod faults;
mod reopen;
