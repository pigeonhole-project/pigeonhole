#!/usr/bin/env bash
# Run ceph/s3-tests against a local s3gram instance.
#
# Prerequisites:
#   - s3gram listening (cargo run --release)
#   - python3, tox, git
#   - AWS keys matching the running server (.env)
#
# Usage:
#   ./scripts/compat.sh              # curated known-good list
#   ./scripts/compat.sh --all        # entire functional suite (many failures expected)
#   ./scripts/compat.sh -- <pytest args...>

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CACHE="${S3TESTS_DIR:-$ROOT/.cache/s3-tests}"
REPO="${S3TESTS_REPO:-https://github.com/ceph/s3-tests.git}"
REF="${S3TESTS_REF:-master}"
CONF_OUT="${S3TEST_CONF:-$ROOT/.cache/s3tests.conf}"
ENDPOINT_HOST="${S3GRAM_HOST:-127.0.0.1}"
ENDPOINT_PORT="${S3GRAM_PORT:-8333}"

cd "$ROOT"
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

ACCESS_KEY="${AWS_ACCESS_KEY_ID:-s3gram}"
SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-s3gramsecret}"

mode="known-good"
pytest_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all) mode="all"; shift ;;
    --) shift; pytest_args+=("$@"); break ;;
    *) pytest_args+=("$1"); shift ;;
  esac
done

need() { command -v "$1" >/dev/null || { echo "missing dependency: $1" >&2; exit 1; }; }
need git
need python3
need tox

if ! curl -sf "http://${ENDPOINT_HOST}:${ENDPOINT_PORT}/" >/dev/null 2>&1; then
  # Unsigned GET / may 403; treat any HTTP response as "up"
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://${ENDPOINT_HOST}:${ENDPOINT_PORT}/" || true)"
  if [[ -z "$code" || "$code" == "000" ]]; then
    echo "s3gram does not appear to be running at ${ENDPOINT_HOST}:${ENDPOINT_PORT}" >&2
    echo "start it with: cargo run --release" >&2
    exit 1
  fi
fi

mkdir -p "$(dirname "$CACHE")" "$(dirname "$CONF_OUT")"
if [[ ! -d "$CACHE/.git" ]]; then
  echo "cloning s3-tests into $CACHE ..."
  git clone --depth 1 --branch "$REF" "$REPO" "$CACHE"
else
  echo "updating s3-tests in $CACHE ..."
  git -C "$CACHE" fetch --depth 1 origin "$REF"
  git -C "$CACHE" checkout -q FETCH_HEAD
fi

sed \
  -e "s/__ACCESS_KEY__/${ACCESS_KEY//\//\\/}/g" \
  -e "s/__SECRET_KEY__/${SECRET_KEY//\//\\/}/g" \
  -e "s/^host = .*/host = ${ENDPOINT_HOST}/" \
  -e "s/^port = .*/port = ${ENDPOINT_PORT}/" \
  "$ROOT/compat/s3tests.conf.template" >"$CONF_OUT"

export S3TEST_CONF="$CONF_OUT"

cd "$CACHE"
if [[ ${#pytest_args[@]} -eq 0 ]]; then
  if [[ "$mode" == "all" ]]; then
    pytest_args=(s3tests/functional)
  else
    while IFS= read -r line || [[ -n "$line" ]]; do
      [[ -z "$line" || "$line" =~ ^# ]] && continue
      pytest_args+=("$line")
    done <"$ROOT/compat/known-good.txt"
  fi
fi

echo "S3TEST_CONF=$S3TEST_CONF"
echo "running: tox -- ${pytest_args[*]}"
tox -- "${pytest_args[@]}"
