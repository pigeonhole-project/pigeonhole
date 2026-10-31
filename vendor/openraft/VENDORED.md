# openraft 0.9.25, vendored

Unchanged from crates.io except two lines: `Cargo.toml` drops tokio's
`rt-multi-thread` feature, and `testing::run_fut` (the storage test suite's
`block_on`) builds a current-thread runtime instead of the multi-thread one. openraft does not build a runtime of its
own (it runs on the caller's), but the feature alone stops tokio from
compiling for wasm, where the site's in-browser build (crates/web) runs it.
Crates that want the multi-thread runtime enable it themselves.

To update: copy the new release from the registry and make both changes again.
