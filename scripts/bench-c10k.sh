#!/bin/bash
# C10K acceptance benchmark for rudis
#
# Profile: 10K active connections, GET/SET 8:2, no pipeline, p99 < 5ms, zero errors.
#
# Usage:
#   ./scripts/bench-c10k.sh [start|bench|all]
#
# Environment:
#   PORT=6379  CLIENTS=10000  REQUESTS=100  WORKERS=0 (auto)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6379}"
CLIENTS="${CLIENTS:-10000}"
REQUESTS="${REQUESTS:-100}"
WORKERS="${WORKERS:-0}"
SHARDS="${SHARDS:-0}"
PIDFILE="/tmp/rudis-bench-${PORT}.pid"
ULIMIT_TARGET="${ULIMIT_TARGET:-65536}"

need_ulimit() {
  local cur
  cur="$(ulimit -n)"
  if [ "$cur" -lt "$ULIMIT_TARGET" ]; then
    echo "raising ulimit -n from $cur to $ULIMIT_TARGET"
    ulimit -n "$ULIMIT_TARGET"
  fi
}

build() {
  echo "==> building release binaries"
  (cd "$ROOT" && cargo build --release --bin rudis --bin rudis-bench)
}

start_server() {
  need_ulimit
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "server already running on port $PORT (pid $(cat "$PIDFILE"))"
    return
  fi
  echo "==> starting rudis on :$PORT (workers=$WORKERS shards=$SHARDS)"
  local workers_arg=()
  local shards_arg=()
  [ "$WORKERS" != "0" ] && workers_arg=(--workers "$WORKERS")
  [ "$SHARDS" != "0" ] && shards_arg=(--shards "$SHARDS")
  "$ROOT/target/release/rudis" \
    --bind "$HOST" \
    --port "$PORT" \
    --maxclients "$((CLIENTS + 1024))" \
    "${workers_arg[@]}" \
    "${shards_arg[@]}" \
    &
  echo $! >"$PIDFILE"
  sleep 1
  if ! kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "server failed to start"
    exit 1
  fi
  echo "server pid $(cat "$PIDFILE")"
}

stop_server() {
  if [ -f "$PIDFILE" ]; then
    kill "$(cat "$PIDFILE")" 2>/dev/null || true
    rm -f "$PIDFILE"
    echo "server stopped"
  fi
}

run_bench() {
  need_ulimit
  echo "==> C10K benchmark: clients=$CLIENTS requests/client=$REQUESTS"
  "$ROOT/target/release/rudis-bench" \
    --host "$HOST" \
    --port "$PORT" \
    -c "$CLIENTS" \
    -n "$REQUESTS" \
    --warmup 10 \
    --get-ratio 0.8 \
    --keys "$CLIENTS"
}

run_redis_bench() {
  if command -v redis-benchmark >/dev/null 2>&1; then
    echo "==> redis-benchmark sanity (100 clients)"
    redis-benchmark -h "$HOST" -p "$PORT" -c 100 -n 10000 -t get,set -P 1 -q
  else
    echo "(redis-benchmark not installed, skipping)"
  fi
}

case "${1:-all}" in
  build) build ;;
  start) build; start_server ;;
  stop) stop_server ;;
  bench)
    run_bench
    ;;
  all)
    build
    trap stop_server EXIT
    start_server
    run_redis_bench
    run_bench
    ;;
  *)
    echo "Usage: $0 [build|start|stop|bench|all]"
    exit 1
    ;;
esac