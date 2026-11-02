# Site

The project site: a home page and the demo, in English (`/`) and Russian
(`/ru/`), static, no server behind it. Published by GitHub Pages at
https://pigeonhole-project.github.io/pigeonhole/.

```
make web                       # the demo's wasm (crates/web); the home page needs none
python3 site/build.py          # → target/site (open target/site/index.html)
```

- `src/` — page templates and assets. `{{key}}` is a string from
  `i18n/<lang>.json`; `build.py` lists the other placeholders. Every
  `src/*.html` is built once per language.
  - `index.html` — the home page; its stage plays a recorded trace
    (`assets/replay.js`, `assets/stage.js`).
  - `demo.html` — the demo (`assets/demo/`): pigeonhole itself, built for
    `wasm32-wasip1` from `crates/web` ([README](../crates/web/README.md)),
    run in a Web Worker (`assets/web-worker.js`) under the site's WASI shim
    (`assets/wasi.js`, its files in the browser's OPFS). `build.py` copies
    `target/wasm32-wasip1/release/pigeonhole_web.wasm` to
    `assets/pigeonhole.wasm`, and warns when it is missing (`make web`).
- `i18n/` — the strings, one file per language. Keys starting with `js.`
  go to the page scripts (`t("key")` in `assets/site.js`).
- `traces/` — recordings the pages play back. `record.py` makes them: it runs
  the release binary on fake Telegram and Discord channels (the demo's) with
  JSON logs, drives it with the AWS CLI and keeps the posts, store calls, log
  batches and S3 operations, then the log's entries and its channel (the
  admin API's `log` and `channels` ops).

```
cargo build --release -p pigeonhole --features fake
python3 site/record.py         # every scenario; needs the aws CLI
```

## The demo on a real node

The built site is also a node's admin UI: with `[admin] ui_dir` naming it
(`target/site`), the node serves it at `/` of its admin address
(`[admin] listen_addr`). `demo.html` then finds the admin API
(`/admin/config`) and sends its ops to that node over HTTP (`/admin/ops`)
instead of to the wasm in a worker (`assets/demo/node.js`).

## Checks

```
make lint-js                   # biome (biome.json): lint, format, imports
make test-js                   # node --test site/test/*.test.mjs: the modules without DOM
make web-smoke                 # build the wasm and run it under Node (crates/web)
```

CI runs `make lint-js test-js` (`.github/workflows/ci.yml`). The Pages
workflow (`.github/workflows/pages.yml`, on every push to `main`) runs
`make web-smoke`, then `python3 site/build.py -o _site`, and deploys that.
