#!/bin/bash
# Standard benchmark wrapper for rudis
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6379}"
CLIENTS="${CLIENTS:-100}"
REQUESTS="${REQUESTS:-100000}"

if command -v redis-benchmark >/dev/null 2>&1; then
  redis-benchmark -h "$HOST" -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t get,set -q
else
  echo "redis-benchmark not found; install redis-tools"
  exit 1
fi