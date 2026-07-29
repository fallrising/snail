#!/bin/bash
# C100K connection-hold acceptance for rudis
#
# Opens N connections, holds for HOLD_SECS with periodic PING.
# Pass: ≥99% connected, connect_fail=0, ping errors < 1%.
#
# Usage:
#   ./scripts/bench-c100k.sh [start|hold|all]
#
# Environment:
#   PORT=6379  CLIENTS=100000  HOLD_SECS=30  PING_MS=1000  WORKERS=0

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-6379}"
CLIENTS="${CLIENTS:-100000}"
HOLD_SECS="${HOLD_SECS:-30}"
PING_MS="${PING_MS:-1000}"
WORKERS="${WORKERS:-0}"
SHARDS="${SHARDS:-0}"
# Spread across 127.0.0.1..127.0.0.N to avoid ephemeral-port exhaustion on one dest.
LOOPBACK_SPREAD="${LOOPBACK_SPREAD:-64}"
PIDFILE="/tmp/rudis-c100k-${PORT}.pid"
ULIMIT_TARGET="${ULIMIT_TARGET:-1048576}"

need_ulimit() {
  local cur
  cur="$(ulimit -n)"
  if [ "$cur" -lt "$ULIMIT_TARGET" ]; then
    echo "raising ulimit -n from $cur to $ULIMIT_TARGET"
    ulimit -n "$ULIMIT_TARGET" || true
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
  echo "==> starting rudis on :$PORT (workers=$WORKERS shards=$SHARDS maxclients=$((CLIENTS + 4096)))"
  local workers_arg=()
  local shards_arg=()
  [ "$WORKERS" != "0" ] && workers_arg=(--workers "$WORKERS")
  [ "$SHARDS" != "0" ] && shards_arg=(--shards "$SHARDS")
  "$ROOT/target/release/rudis" \
    --bind "$HOST" \
    --port "$PORT" \
    --maxclients "$((CLIENTS + 4096))" \
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

run_hold() {
  need_ulimit
  echo "==> C100K hold: clients=$CLIENTS hold=${HOLD_SECS}s ping=${PING_MS}ms"
  "$ROOT/target/release/rudis-bench" \
    --hold \
    --host 127.0.0.1 \
    --port "$PORT" \
    -c "$CLIENTS" \
    --hold-secs "$HOLD_SECS" \
    --ping-interval-ms "$PING_MS" \
    --connect-batch 2000 \
    --loopback-spread "$LOOPBACK_SPREAD"
}

cmd="${1:-all}"
case "$cmd" in
  start) build; start_server ;;
  hold) need_ulimit; run_hold ;;
  stop) stop_server ;;
  all)
    build
    start_server
    set +e
    run_hold
    rc=$?
    set -e
    stop_server
    exit $rc
    ;;
  *)
    echo "Usage: $0 [start|hold|stop|all]"
    exit 1
    ;;
esac
