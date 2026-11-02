.PHONY: web web-smoke lint-js test-js cluster-smoke sim sim-all run smoke metrics-demo metrics-demo-down compat compat-all compat-memory compat-s3s-boto3 compat-s3s-e2e compat-rclone compat-telegram compat-discord test test-all check-deps report lint

run:
	cargo run --release

# Same as `run` with Discord backend compiled in (default feature).
run-discord:
	cargo run --release --features discord

smoke:
	./scripts/smoke.sh

# Three processes as one Raft cluster over gRPC on localhost; needs `aws`.
cluster-smoke:
	cargo build -p pigeonhole
	./scripts/cluster-smoke.sh

# The in-browser build (crates/web). zstd's C objects need an archiver that
# knows wasm: llvm-ar from `rustup component add llvm-tools`. With WASI_SDK
# (a wasi-sdk release) its clang and libc headers compile them: the system
# clang on Linux has no WASI headers and picks up glibc's (CI does this).
WASM_AR = $(shell rustc --print sysroot)/lib/rustlib/$(shell rustc -vV | sed -n 's/^host: //p')/bin/llvm-ar
WASM_CC_ENV = $(if $(WASI_SDK),CC_wasm32_wasip1=$(WASI_SDK)/bin/clang AR_wasm32_wasip1=$(WASI_SDK)/bin/llvm-ar,AR_wasm32_wasip1=$(WASM_AR))
# Without symbols: 3 MB less to download (the name section; panics still
# say where).
web:
	$(WASM_CC_ENV) CARGO_PROFILE_RELEASE_STRIP=symbols cargo build --release --target wasm32-wasip1 -p pigeonhole-web

# The smoke under Node's WASI, then under the site's own shim (its file pool
# on buffers), then a page load and a reload on one pool.
web-smoke: web
	node --no-warnings crates/web/run-wasi.mjs target/wasm32-wasip1/release/pigeonhole-web.wasm >/dev/null
	node --no-warnings crates/web/run-shim.mjs smoke target/wasm32-wasip1/release/pigeonhole-web.wasm 2>&1 | grep -q "smoke ok"
	node --no-warnings crates/web/run-shim.mjs reload target/wasm32-wasip1/release/pigeonhole_web.wasm 2>&1 | grep "reload"
	node --no-warnings crates/web/run-shim.mjs snapshot target/wasm32-wasip1/release/pigeonhole_web.wasm 2>&1 | grep 'base {"at":60'
	node --no-warnings crates/web/run-shim.mjs events target/wasm32-wasip1/release/pigeonhole_web.wasm 2>&1 | grep "events ok"

# The site's JavaScript: biome (biome.json) checks its lint, format and
# imports, as `lint` does for Rust; the tests are the modules without DOM.
BIOME = npx --yes @biomejs/biome@2.6.4
lint-js:
	$(BIOME) ci

test-js:
	node --test site/test/*.test.mjs

# Curated ceph/s3-tests (see compat/known-good.txt)
compat:
	./scripts/compat.sh

# Full s3-tests functional suite (expect many failures)
compat-all:
	./scripts/compat.sh --all

# Full s3-tests against MemoryBlobStore (no Telegram). Starts a temp server.
compat-memory:
	./scripts/compat-memory.sh

# s3s upstream tests/boto3 against MemoryBlobStore (no Telegram).
compat-s3s-boto3:
	./scripts/compat-s3s-boto3.sh

# s3s-e2e suite against MemoryBlobStore (no Telegram).
# Extra args are forwarded, e.g. `make compat-s3s-e2e ARGS='--filter ^Basic'`
compat-s3s-e2e:
	./scripts/compat-s3s-e2e.sh $(ARGS)

# s3s upstream rclone S3 e2e against MemoryBlobStore (no Telegram / no Docker).
compat-rclone:
	./scripts/compat-rclone.sh

# Purge real Telegram + run the full client suite against TelegramBlobStore.
# Extra args forwarded to compat.sh, e.g. `make compat-telegram ARGS=--all`
compat-telegram:
	./scripts/compat-telegram.sh $(ARGS)

# Manual Discord Bot API suite (not in CI). Needs DISCORD_BOT_TOKEN + channel_id.
compat-discord:
	./scripts/compat-discord.sh $(ARGS)

# Local pigeonhole on disk + Prometheus + Grafana + S3 traffic (docs/metrics.md).
metrics-demo:
	./scripts/metrics-demo.sh up

metrics-demo-down:
	./scripts/metrics-demo.sh down

# cargo-nextest runs the tests of every binary at once (cargo test runs one
# binary after another): ~30 s instead of ~100 s, most tests wait on timers.
# No doctests here: the workspace has none. `cargo install --locked cargo-nextest`.
test:
	cargo nextest run --workspace

check-deps:
	./scripts/check-deps.sh --strict

# Size, complexity hotspots and the trait graph (Mermaid) → target/code-report.md.
# Needs `cargo install --locked rust-code-analysis-cli`.
report:
	./scripts/code_report.py --out target/code-report.md

test-all: check-deps test smoke

# Deterministic simulation (docs/simulation.md). MADSIM_TEST_SEED=<seed>
# replays one run; MADSIM_TEST_NUM=<n> runs n seeds.
#
# The tests run in an empty TMPDIR of their own, removed after them. In the
# simulator temp dir names repeat from one process to the next (tempfile's
# fastrand is seeded from the virtual clock and the thread id): next to a
# dir an earlier run left, tempfile retries and then reads getrandom, which
# the simulator answers and MADSIM_TEST_CHECK_DETERMINISM counts — the two
# runs of a seed differ. The build comes first, outside it: an sccache
# server started under that TMPDIR would keep it after it is gone.
SIM_CARGO = RUSTFLAGS="--cfg madsim" CARGO_TARGET_DIR=target/madsim cargo test
SIM = $(SIM_CARGO) --no-run $(1) && t=$$(mktemp -d) && TMPDIR=$$t $(SIM_CARGO) $(1); s=$$?; rm -rf $$t; exit $$s
sim:
	$(call SIM,-p pigeonhole-sim)

# Every library's tests under the simulator (the binary and its servers are
# not built there).
sim-all:
	$(call SIM,--workspace --exclude pigeonhole)

# rustfmt and clippy as CI runs them: every warning fails (lints: [workspace.lints] in Cargo.toml).
lint:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo clippy --workspace --all-targets --no-default-features -- -D warnings
