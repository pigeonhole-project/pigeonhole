#!/usr/bin/env bash
# The same workload (scripts/bench-backends.py) on each backend, one after the
# other, then a Markdown comparison in .cache/bench/report.md.
#
#   ./scripts/bench.sh discord telegram     # BENCH_MINUTES=15 per backend
#   BENCH_MINUTES=1 ./scripts/bench.sh disk # a dry run
#   BENCH_PHASES=blob_put,cold_get,ranged_get ./scripts/bench.sh discord   # some phases only
#   BENCH_BOTS=2 ./scripts/bench.sh telegram   # each chat store through two bots (a run labelled telegram-2bots)
#
# Per backend: a fresh server from scripts/metrics-demo.sh (no block cache,
# no demo traffic; Prometheus and Grafana come up too, so the run can be
# watched), the benchmark, the server down, and the chat channels emptied of
# what the run left: Discord's three test channels are emptied whole,
# Telegram's between two marker messages (tests/live.rs bench_window).
# Needs uv (the benchmark runs with boto3 through it).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/.cache/bench"
MINUTES="${BENCH_MINUTES:-15}"
BOTS="${BENCH_BOTS:-1}"
mkdir -p "$OUT"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi
command -v uv >/dev/null || { echo "missing dependency: uv (brew install uv)" >&2; exit 1; }
[[ $# -gt 0 ]] || set -- discord telegram

before() {
  case "$1" in
    telegram)
      TG_BENCH=mark cargo test -q -p pigeonhole-storage-telegram --test live bench_window -- --ignored --nocapture \
        | grep -E "from|test result" ;;
  esac
}

after() {
  case "$1" in
    discord)
      cargo test -q -p pigeonhole-storage-discord --test live empty_test_channels -- --ignored --nocapture --test-threads 1 \
        | grep -E "emptied|test result" ;;
    telegram)
      TG_BENCH=clean cargo test -q -p pigeonhole-storage-telegram --test live bench_window -- --ignored --nocapture \
        | grep -E "deleted|test result" ;;
  esac
}

runs=()
for backend in "$@"; do
  echo "##### $backend"
  label="$backend"
  [[ "$BOTS" == 1 || "$backend" == disk ]] || label+="-${BOTS}bots"
  export METRICS_DEMO_BACKEND="$backend" METRICS_DEMO_CACHE=off NO_LOAD=1 METRICS_DEMO_BOTS="$BOTS"
  "$ROOT/scripts/metrics-demo.sh" down >/dev/null
  # A fresh tablet each time (a chat log starts empty once its channel is).
  state="$ROOT/.cache/metrics-demo"
  [[ "$backend" == disk ]] || state+="-$backend"
  rm -rf "$state/meta" "$state/blobs" "$state/prometheus"
  before "$backend"
  "$ROOT/scripts/metrics-demo.sh" up | grep -E "running|dashboard"
  status=0
  uv run -q --with boto3 "$ROOT/scripts/bench-backends.py" run --label "$label" --minutes "$MINUTES" \
    ${BENCH_PHASES:+--phases "$BENCH_PHASES"} --out "$OUT/$label.json" || status=$?
  "$ROOT/scripts/metrics-demo.sh" down
  after "$backend"
  [[ $status -eq 0 ]] || { echo "benchmark on $backend failed ($status)" >&2; exit "$status"; }
  runs+=("$OUT/$label.json")
done

uv run -q --with boto3 "$ROOT/scripts/bench-backends.py" report "${runs[@]}" >"$OUT/report.md"
echo "report: $OUT/report.md"
