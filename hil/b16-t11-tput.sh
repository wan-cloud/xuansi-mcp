#!/usr/bin/env bash
# B16 T11 windowed-upload throughput leg (T7-④) — board #1 d75884 over
# TCP, no daemon (direct replclient FsStreamWrite, 1MiB + 700KB ×3
# sha-verified + read-back byte account). Evidence goes to the
# caller's LOGPATH.
#
# Usage: b16-t11-tput.sh [LOGPATH]
set -euo pipefail
cd "$(dirname "$0")/.."
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi
go run ./hil -scenario b16t11tput
