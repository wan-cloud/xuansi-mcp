#!/usr/bin/env bash
# B15 W1-T5 wave-1 HIL matrix — one DEDICATED daemon life on a private
# sock (the stock daemon's socket is never touched) driving the two
# real boards: #2 97c1ac on USB (/dev/ttyACM1, fw v7 d42a07d, radio
# dormant) and #1 d75884 cross-subnet at 10.1.11.204:7777 (fw 0.4.0 —
# the pin-route dogfood). Legs S1–S9 in hil/b15w1t5.go; the sysfs
# unbind leg needs passwordless sudo (skipped honestly without it).
#
# Usage: b15-w1t5.sh [LOGPATH]   (no LOGPATH = stdout only; the
# committed repo carries no evidence — mgmt evidence/ is the canonical
# home, per the privacy discipline.)
set -euo pipefail
cd "$(dirname "$0")/.."

RUN=/tmp/opencode/b15-w1t5
mkdir -p "$RUN"
SOCK="$RUN/d.sock"
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi

echo "=== b15 W1-T5 wave-1 HIL matrix — $(date -Is) ==="
echo "run dir: $RUN  sock: $SOCK"
git log --oneline -1

go build -o "$RUN/xuansi" ./cmd/xuansi
echo "daemon binary built at HEAD: $RUN/xuansi"

cleanup() {
  pkill -f "xuansi serve --sock $SOCK" 2>/dev/null || true
}
trap cleanup EXIT

: > "$RUN/boards.conf"
pkill -f "xuansi serve --sock $SOCK" 2>/dev/null || true
sleep 0.3
rm -f "$SOCK"
"$RUN/xuansi" serve --sock "$SOCK" --grace-ttl 30s --boards-file "$RUN/boards.conf" &
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
echo "dedicated daemon up on $SOCK (zero-config boards.conf)"

go run ./hil -scenario b15w1t5 -w5sock "$SOCK"
RC=$?

# stop the daemon BEFORE the stray check (the EXIT trap alone would
# leave it alive at check time — attempt 2's trailing false STRAY), then
# give the graceful shutdown a bounded wait.
pkill -f "xuansi serve --sock $SOCK" 2>/dev/null || true
for _ in $(seq 1 50); do
  pgrep -f "xuansi serve --sock $SOCK" >/dev/null 2>&1 || break
  sleep 0.1
done
if pgrep -af "xuansi serve --sock $SOCK" >/dev/null 2>&1; then
  echo "STRAY daemon on $SOCK:"; pgrep -af "xuansi serve --sock $SOCK"; exit 1
fi
echo "dedicated daemon down; sock clean"
echo "=== matrix complete (driver rc=$RC) — $(date -Is) ==="
exit $RC
