#!/bin/bash
# Standard benchmark wrapper for rudis
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6379}"
CLIENTS="${CLIENTS:-100}"
REQUESTS="${REQUESTS:-100000}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if command -v redis-benchmark >/dev/null 2>&1; then
  redis-benchmark -h "$HOST" -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t get,set -P 1 -q
elif [ -x "$ROOT/target/release/rudis-bench" ]; then
  "$ROOT/target/release/rudis-bench" \
    --host "$HOST" --port "$PORT" \
    -c "$CLIENTS" -n "$REQUESTS" --get-ratio 0.8 --warmup 5
else
  echo "redis-benchmark not found; run: cargo build --release --bin rudis-bench"
  exit 1
fi