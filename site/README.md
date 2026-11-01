# Site

The project site: a home page in English (`/`) and Russian (`/ru/`), static,
no server behind it.

```
python3 site/build.py          # → target/site (open target/site/index.html)
```

- `src/` — page templates and assets. `{{key}}` is a string from
  `i18n/<lang>.json`; `build.py` lists the other placeholders.
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
