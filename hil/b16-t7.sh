#!/usr/bin/env bash
# B16 T7 board legs (C2 upload lane + D2 panic face) — board #1 d75884.
# One DEDICATED daemon life on a private sock; the daemon is the
# working-tree build (the code under test). Evidence goes to the
# caller's LOGPATH (the mgmt repo's evidence/ is the canonical home —
# this public repo carries none).
#
# Usage: b16-t7.sh [LOGPATH]
set -euo pipefail
cd "$(dirname "$0")/.."

RUN="${T7_RUN:-/tmp/opencode/b16-t7}"
BIN="${T7_BIN:-$RUN/xuansi}"
mkdir -p "$RUN"
SOCK="$RUN/d.sock"
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi

echo "=== b16 T7 board legs — $(date -Is) ==="
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

T7_SOCK="$SOCK" T7_BIN="$BIN" go run ./hil -scenario b16t7
RC=$?

# ——— D2: the panic face end-to-end ————————————————————————————
# The watcher holds the daemon's events face; the trigger is the fw
# vector suite's OWN wdttest leg over USB (its connection owns the
# controller through the reboot the CDC open causes — no slot race).
if [ "$RC" -eq 0 ] && [ -n "${T7_FWVEC:-}" ]; then
  ( T7_SOCK="$SOCK" T7_BIN="$BIN" go run ./hil -scenario b16t7d2 ) &
  WPID=$!
  sleep 2
  ( cd "$T7_FWVEC" && python3 tools/selftest-vectors.py --port /dev/ttyACM1 \
      --only panic.summary-after-wdt --out "$RUN/d2-vector.log" )
  VRC=$?
  wait $WPID
  WRC=$?
  echo "D2 vector rc=$VRC watcher rc=$WRC"
  [ "$VRC" -eq 0 ] && [ "$WRC" -eq 0 ] || RC=1
else
  echo "D2 leg: skipped (T7_FWVEC unset or earlier legs failed)"
fi

kill "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
echo "daemon stopped; legs rc=$RC"
exit "$RC"
