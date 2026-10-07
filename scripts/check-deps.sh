#!/usr/bin/env bash
# Enforce workspace layering rules via `cargo metadata`.
#
# Soft mode (default): rules that already hold on the legacy s3gram-* layout.
# Strict mode (--strict): full pigeonhole target rules (blob-store must not
# pull s3s/tonic/storage/gateway; gateways only blob-store/codec/types).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PY_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --strict) PY_ARGS+=(--strict) ;;
    -h|--help)
      cat <<'EOF'
Usage: scripts/check-deps.sh [--strict]

Soft (default): storage must not depend on index/engine/gateways/other storage;
gateways must not depend on storage or each other.

Strict: also require blob-store purity and gateway → {blob-store, codec, types} only.
EOF
      exit 0
      ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

need() { command -v "$1" >/dev/null || { echo "missing dependency: $1" >&2; exit 1; }; }
need cargo
need python3

cargo metadata --format-version 1 --no-deps | python3 "$ROOT/scripts/check_deps.py" ${PY_ARGS[@]+"${PY_ARGS[@]}"}
