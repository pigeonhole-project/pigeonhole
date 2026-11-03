# pigeonhole in the browser

`pigeonhole-web` reads a `pigeonhole.toml` with pigeonhole-config, as the
binary does, and builds the stack the binary builds from it — the metadata
tablet on openraft, the blob stores and their replica group, the chunk store
with zstd, the S3 handlers — in one process, without HTTP, for
`wasm32-wasip1`. The site runs it in a Web Worker under a WASI shim
(`site/src/assets/web-worker.js`, `wasi.js`).

The demo's config (`pigeonhole-web/demo.toml`) has the real `telegram` and
`discord` stores on fake channels (`fake = "channel-data"`,
pigeonhole-fake-channels): two hold the chunks (a replica group), two more
the metadata log and its snapshots. Each fake channel keeps itself in the
`local_disk` store `channel-data` (`/data/channels`), which in the browser
is the site's OPFS: a reload finds everything where it was. A new config is
a restart: the channels and memory stores outlive the node, and the tablet
replays its log.

```
rustup target add wasm32-wasip1
rustup component add llvm-tools   # llvm-ar: zstd's C objects for wasm
make web                          # → target/wasm32-wasip1/release/pigeonhole_web.wasm
make web-smoke                    # make web, then run it under Node (24: 22's WASI segfaults)
```

On Linux the system clang has no WASI headers (it picks up glibc's): set
`WASI_SDK` to a [wasi-sdk](https://github.com/WebAssembly/wasi-sdk) release
and `make web` compiles zstd with its clang and llvm-ar instead, as the
Pages workflow does (`.github/workflows/pages.yml`).

`make web-smoke` runs `src/main.rs` (a few S3 calls, a restart on another
config) under Node's WASI (`run-wasi.mjs`), then the library under the
site's own shim with its file pool on buffers instead of OPFS
(`run-shim.mjs`), in four modes: `smoke` (the same binary), `reload` (two
module instances on one pool: a page load, then a reload), `snapshot`
(enough puts for a snapshot) and `events` (every event the demo draws, from
the wizard's cluster of three). `cargo run -p pigeonhole-web` runs the smoke
natively.

The demo draws what the node logs: `site/src/assets/demo/events.js` reads
the JSON log lines by their message text (its `MESSAGES` table: "posted",
"store call", "raft frame sent", "waiting out the lease", …) and their
fields, as logged at the levels `ph_open`'s env filter lets through (most
are `debug!`). Those strings, their fields and those levels are a contract
with the tracing sites across the workspace: `make web-smoke`'s `events`
mode fails when one no longer comes out of a real run, so a reworded or
quieter line is caught there rather than as pigeons missing on the page.

What wasm needed from the rest of the workspace:

- tokio with only `io-util`, `macros`, `rt`, `sync`, `time` (the workspace
  default now; the binary and the ByteStream gateway add the rest);
- openraft without `rt-multi-thread` (a fork: `[patch.crates-io]` in the
  workspace `Cargo.toml`);
- compression through `pigeonhole_codec::blocking`, which runs inline where
  there are no threads.
