.PHONY: run smoke compat compat-all compat-memory test test-all

run:
	cargo run --release

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

test:
	cargo test

test-all: test smoke
