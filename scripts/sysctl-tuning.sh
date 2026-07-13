#!/bin/bash
# OS tuning for high connection counts (C1M profile)
# Run as root. Use --restore to revert to saved values.

set -euo pipefail

SAVE_FILE="/tmp/rudis-sysctl-backup.txt"

apply() {
  echo "Applying rudis sysctl tuning..."
  sysctl -w fs.file-max=2000000
  sysctl -w net.core.somaxconn=65535
  sysctl -w net.ipv4.tcp_max_syn_backlog=65535
  sysctl -w net.core.netdev_max_backlog=65536
  sysctl -w net.ipv4.tcp_rmem="4096 87380 6291456"
  sysctl -w net.ipv4.tcp_wmem="4096 65536 4194304"
  echo "Done. Also ensure: ulimit -n 2000000"
}

restore() {
  echo "Restore not automated; reboot or manually reset sysctl values."
}

case "${1:-apply}" in
  apply) apply ;;
  restore) restore ;;
  *) echo "Usage: $0 [apply|restore]" ;;
esac