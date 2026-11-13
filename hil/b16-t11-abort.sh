#!/usr/bin/env bash
# B16 T11 M9 aux-abort stress leg (T9-②) — board #1 d75884 over TCP,
# no daemon (two direct replclient connections dance the controller
# seat ×100 rounds). Evidence goes to the caller's LOGPATH.
#
# Usage: b16-t11-abort.sh [LOGPATH]
set -euo pipefail
cd "$(dirname "$0")/.."
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi
go run ./hil -scenario b16t11abort
