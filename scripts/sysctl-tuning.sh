#!/bin/bash
# OS tuning for high connection counts (C100K / C1M profile)
# Run as root. Saves previous values and restores with --restore.

set -euo pipefail

SAVE_FILE="${RUDIS_SYSCTL_BACKUP:-/tmp/rudis-sysctl-backup.txt}"

KEYS=(
  fs.file-max
  net.core.somaxconn
  net.ipv4.tcp_max_syn_backlog
  net.core.netdev_max_backlog
  net.ipv4.tcp_tw_reuse
  net.ipv4.ip_local_port_range
)

save_one() {
  local key="$1"
  local val
  val="$(sysctl -n "$key" 2>/dev/null || true)"
  if [ -n "$val" ]; then
    printf '%s=%s\n' "$key" "$val" >>"$SAVE_FILE"
  fi
}

apply() {
  if [ -f "$SAVE_FILE" ]; then
    echo "Backup already exists at $SAVE_FILE (not overwriting). Run restore first if needed."
  else
    echo "Saving current sysctl values to $SAVE_FILE"
    : >"$SAVE_FILE"
    for k in "${KEYS[@]}"; do
      save_one "$k"
    done
    # multi-value knobs
    {
      echo "net.ipv4.tcp_rmem=$(sysctl -n net.ipv4.tcp_rmem | tr '\t' ' ')"
      echo "net.ipv4.tcp_wmem=$(sysctl -n net.ipv4.tcp_wmem | tr '\t' ' ')"
    } >>"$SAVE_FILE"
  fi

  echo "Applying rudis sysctl tuning..."
  sysctl -w fs.file-max=2000000
  sysctl -w net.core.somaxconn=65535
  sysctl -w net.ipv4.tcp_max_syn_backlog=65535
  sysctl -w net.core.netdev_max_backlog=65536
  sysctl -w net.ipv4.tcp_tw_reuse=1
  sysctl -w net.ipv4.ip_local_port_range="1024 65535"
  sysctl -w net.ipv4.tcp_rmem="4096 87380 6291456"
  sysctl -w net.ipv4.tcp_wmem="4096 65536 4194304"
  echo "Done. Also ensure: ulimit -n 1048576 (or higher)"
  echo "Restore with: $0 restore"
}

restore() {
  if [ ! -f "$SAVE_FILE" ]; then
    echo "No backup at $SAVE_FILE — nothing to restore."
    exit 1
  fi
  echo "Restoring sysctl from $SAVE_FILE ..."
  while IFS= read -r line || [ -n "$line" ]; do
    [ -z "$line" ] && continue
    key="${line%%=*}"
    val="${line#*=}"
    sysctl -w "$key=$val"
  done <"$SAVE_FILE"
  rm -f "$SAVE_FILE"
  echo "Restored and removed $SAVE_FILE"
}

case "${1:-apply}" in
  apply) apply ;;
  restore) restore ;;
  *)
    echo "Usage: $0 [apply|restore]"
    exit 1
    ;;
esac
