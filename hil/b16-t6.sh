#!/usr/bin/env bash
# B16 T6 board legs (C1 stream subscription + D1 boot receipts) — board
# #1 d75884, radio untouched (TCP legs dial only; the usb legs are the
# §2-normal CDC-open reboots the D1 capture REQUIRES). One DEDICATED
# daemon life on a private sock; the daemon is the working-tree build
# (the code under test). Evidence goes to the caller's LOGPATH (the
# mgmt repo's evidence/ is the canonical home — this public repo
# carries none).
#
# Usage: b16-t6.sh [LOGPATH]
set -euo pipefail
cd "$(dirname "$0")/.."

RUN="${T6_RUN:-/tmp/opencode/b16-t6}"
BIN="${T6_BIN:-$RUN/xuansi}"
mkdir -p "$RUN"
SOCK="$RUN/d.sock"
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi

echo "=== b16 T6 board legs — $(date -Is) ==="
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

T6_SOCK="$SOCK" T6_BIN="$BIN" go run ./hil -scenario b16t6
RC=$?

kill "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
echo "daemon stopped; legs rc=$RC"
exit "$RC"
