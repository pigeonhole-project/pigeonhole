# Vendored grammers-mtsender 0.11.0

The crates.io package of `grammers-mtsender` 0.11.0 (upstream
https://codeberg.org/Lonami/grammers, commit
15b7afa46a9c6853aa9d8efc9d70a0f46b1781c3, path `grammers-mtsender`), used
through `[patch.crates-io]` in the workspace `Cargo.toml`, with one change on
top (see the git history of this directory): a request whose caller stopped
waiting is dropped before it is serialized, instead of being sent whenever
its turn comes. Offered upstream; drop this copy once a release has it.

License: MIT OR Apache-2.0 (upstream's).
