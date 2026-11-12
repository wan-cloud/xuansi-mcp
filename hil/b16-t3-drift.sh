#!/usr/bin/env bash
# B16 T3 (F3-①) drift carry-over board leg — board #1 d75884 over USB,
# radio untouched (T2's patient: no flash, no wifi state change; the
# only board-side effects are the §2-normal CDC-open reboot of the usb
# lease and read-only ops). One DEDICATED daemon life on a private
# sock (the stock daemon's socket is never touched); the daemon is the
# working-tree build (the fix under test).
#
# Usage: b16-t3-drift.sh [LOGPATH]   (no LOGPATH = stdout only; the
# committed repo carries no evidence — the mgmt repo's evidence/ is the
# canonical home, per the privacy discipline.)
set -euo pipefail
cd "$(dirname "$0")/.."

RUN="${T3_RUN:-/tmp/opencode/b16-t3-drift}"
BIN="${T3_BIN:-$RUN/xuansi}"
mkdir -p "$RUN"
SOCK="$RUN/d.sock"
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi

echo "=== b16 T3 drift carry-over board leg — $(date -Is) ==="
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
echo "daemon pid=$SRV serving $SOCK (keepalive at the serve default, 3s)"

# the board must be present over USB before the leg starts (the
# sweep is async — poll briefly)
ok=""
for _ in $(seq 1 30); do
  if "$BIN" list -sock "$SOCK" 2>/dev/null | grep -q d75884; then ok=1; break; fi
  sleep 0.5
done
[ -n "$ok" ] || { echo "board d75884 not discovered over USB — abort"; exit 1; }

go run ./hil -scenario b16t3drift -t3sock "$SOCK" -t3board d75884 -t3holder b16-t3drift
RC=$?

kill "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
echo "daemon stopped; leg rc=$RC"
exit "$RC"
