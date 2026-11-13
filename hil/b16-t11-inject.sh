#!/usr/bin/env bash
# B16 T11 corruption injector leg (T6-④) — board #1 d75884. One
# DEDICATED daemon life on a private sock (the working-tree build is
# the code under test); the injector rides the daemon's own CDC-open
# reboot. Evidence goes to the caller's LOGPATH (the mgmt repo's
# evidence/ is the canonical home — this public repo carries none).
#
# Usage: b16-t11-inject.sh [LOGPATH]
set -euo pipefail
cd "$(dirname "$0")/.."

RUN="${T11_RUN:-/tmp/opencode/b16-t11-inject}"
BIN="${T11_BIN:-$RUN/xuansi}"
mkdir -p "$RUN"
SOCK="$RUN/d.sock"
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi

echo "=== b16 T11 corruption injector — $(date -Is) ==="
echo "run dir: $RUN  sock: $SOCK"

go build -o "$BIN" ./cmd/xuansi
echo "daemon binary built (working tree): $BIN"

cleanup() {
  pkill -f "xuansi serve --sock $SOCK" 2>/dev/null || true
}
trap cleanup EXIT

rm -f "$SOCK"
"$BIN" serve --sock "$SOCK" --grace-ttl 30s --boards-file /dev/null 2>&1 &
SRV=$!
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
echo "daemon pid=$SRV serving $SOCK"

ok=""
for _ in $(seq 1 30); do
  if "$BIN" list -sock "$SOCK" 2>/dev/null | grep -q d75884; then ok=1; break; fi
  sleep 0.5
done
[ -n "$ok" ] || { echo "board d75884 not discovered — abort"; exit 1; }

T11_SOCK="$SOCK" go run ./hil -scenario b16t11inject
RC=$?

kill "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
echo "daemon stopped; injector leg rc=$RC"
exit "$RC"
