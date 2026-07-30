#!/bin/bash
# C10K acceptance for rudis
#
# Gate (must PASS):
#   Hold CLIENTS fds while ACTIVE of them issue GET/SET (8:2, no pipeline),
#   p99 < 5ms, zero errors. Default: 10K connections, 64 active.
#
# Informational (does not fail the script):
#   Full-active stress: all CLIENTS in-flight (aspirational ~2M rps for p99<5ms)
#
# Usage:
#   ./scripts/bench-c10k.sh [start|gate|stress|bench|all]
#
# Environment:
#   PORT=6379  CLIENTS=10000  ACTIVE=64  REQUESTS=200
#   STRESS_REQUESTS=50  WORKERS=1  SHARDS=1  PIPELINE=1
#   (default 1 worker; multi-worker C10K gate also PASS — set WORKERS=0 for auto)
#   PIPELINE>1 measures pipelined full-active throughput (informational)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6379}"
CLIENTS="${CLIENTS:-10000}"
ACTIVE="${ACTIVE:-64}"
REQUESTS="${REQUESTS:-200}"
STRESS_REQUESTS="${STRESS_REQUESTS:-50}"
WORKERS="${WORKERS:-1}"
SHARDS="${SHARDS:-1}"
PIPELINE="${PIPELINE:-1}"
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
  echo "==> starting rudis on :$PORT (workers=$WORKERS shards=$SHARDS maxclients=$((CLIENTS + 1024)))"
  local workers_arg=()
  local shards_arg=()
  [ "$WORKERS" != "0" ] && workers_arg=(--workers "$WORKERS")
  [ "$SHARDS" != "0" ] && shards_arg=(--shards "$SHARDS")
  # WORKERS=0 / SHARDS=0 → omit flags (server auto-detects).
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

run_gate() {
  need_ulimit
  echo "==> C10K gate: hold $CLIENTS fds, active=$ACTIVE, n=$REQUESTS, p99<5ms"
  "$ROOT/target/release/rudis-bench" \
    --host "$HOST" \
    --port "$PORT" \
    -c "$CLIENTS" \
    --active "$ACTIVE" \
    -n "$REQUESTS" \
    --warmup 10 \
    --get-ratio 0.8 \
    --keys "$ACTIVE" \
    --connect-batch 500 \
    --p99-ms 5
}

run_stress() {
  need_ulimit
  echo "==> C10K full-active stress (informational, --soft): clients=$CLIENTS pipeline=$PIPELINE"
  "$ROOT/target/release/rudis-bench" \
    --host "$HOST" \
    --port "$PORT" \
    -c "$CLIENTS" \
    -n "$STRESS_REQUESTS" \
    --warmup 1 \
    --get-ratio 0.8 \
    --keys "$CLIENTS" \
    --connect-batch 500 \
    --pipeline "$PIPELINE" \
    --soft \
    --p99-ms 5 || true
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
  gate|latency|hold) run_gate ;;
  stress) run_stress ;;
  bench)
    run_gate
    run_stress
    ;;
  all)
    build
    trap stop_server EXIT
    start_server
    run_redis_bench
    run_gate
    run_stress
    echo
    echo "C10K acceptance: hold ${CLIENTS}+active@${ACTIVE} gate must PASS; stress is informational."
    ;;
  *)
    echo "Usage: $0 [build|start|stop|gate|stress|bench|all]"
    exit 1
    ;;
esac
