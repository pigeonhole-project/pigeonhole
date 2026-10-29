.PHONY: run smoke metrics-demo metrics-demo-down compat compat-all compat-memory compat-s3s-boto3 compat-s3s-e2e compat-rclone compat-telegram compat-discord test test-all check-deps report

run:
	cargo run --release

# Same as `run` with Discord backend compiled in (default feature).
run-discord:
	cargo run --release --features discord

smoke:
	./scripts/smoke.sh

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

test:
	cargo test --workspace

check-deps:
	./scripts/check-deps.sh --strict

# Size, complexity hotspots and the trait graph (Mermaid) → target/code-report.md.
# Needs `cargo install --locked rust-code-analysis-cli`.
report:
	./scripts/code_report.py --out target/code-report.md

test-all: check-deps test smoke
