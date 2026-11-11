#!/usr/bin/env bash
# B15 W1.T5 fix1 load leg (spec §8.1 clause 2, controller ruling — the
# camera-free loaded-board shape): one dedicated daemon, board #2 USB
# lease, 3×4s busy-loop jobs occupying the VM queue, then log.read +
# eval interleaved DURING the load, board_status link survival, one
# job_wait landing. The daemon's own log rides the same tee — a
# teardown spiral would show as repeated link/redial/USB-open lines.
#
# Usage: b15-w1t5-load.sh [LOGPATH]   (no LOGPATH = stdout only; the
# committed repo carries no evidence — mgmt evidence/ is the canonical
# home, per the privacy discipline.)
set -euo pipefail
cd "$(dirname "$0")/.."

RUN=/tmp/opencode/b15-w1t5-load
mkdir -p "$RUN"
SOCK="$RUN/d.sock"
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi

echo "=== b15 W1-T5 load leg — $(date -Is) ==="
echo "run dir: $RUN  sock: $SOCK"
git log --oneline -1

go build -o "$RUN/xuansi" ./cmd/xuansi
echo "daemon binary built at HEAD: $RUN/xuansi"

cleanup() {
  pkill -f "xuansi serve --sock $SOCK" 2>/dev/null || true
}
trap cleanup EXIT

: > "$RUN/boards.conf"
rm -f "$SOCK"
"$RUN/xuansi" serve --sock "$SOCK" --grace-ttl 30s --boards-file "$RUN/boards.conf" &
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
echo "dedicated daemon up on $SOCK (zero-config boards.conf)"

RC=0
go run ./hil -scenario b15w1t5load -w5sock "$SOCK" || RC=$?

# stop the daemon BEFORE the stray check, bounded wait for the exit
pkill -f "xuansi serve --sock $SOCK" 2>/dev/null || true
for _ in $(seq 1 50); do
  pgrep -f "xuansi serve --sock $SOCK" >/dev/null 2>&1 || break
  sleep 0.1
done
# the spiral cross-check: exactly ONE USB controller open may appear in
# the daemon's own log lines (the lease-time one) — a redial/teardown
# spiral under load would print a second. The patterns anchor on the
# daemon's slog shape (level=… msg="…") so free-text notes in the tee
# (this script's own echoes, the driver's hint lines) cannot match.
OPENS=$(grep -c 'msg="board .*: USB fallback connection up' "$LOG" 2>/dev/null || true)
DEAD=$(grep -c 'msg=".*link dead\|msg=".*connection closed\|msg=".*read error' "$LOG" 2>/dev/null || true)
echo "daemon-log spiral cross-check: USB opens=$OPENS (want 1), link-dead lines=$DEAD (want 0)"
if [ "$OPENS" != "1" ] || [ "$DEAD" != "0" ]; then
  echo "SPIRAL SIGNATURE SUSPECTED — see the log"
  exit 1
fi
if pgrep -af "xuansi serve --sock $SOCK" >/dev/null 2>&1; then
  echo "STRAY daemon on $SOCK:"; pgrep -af "xuansi serve --sock $SOCK"; exit 1
fi
echo "dedicated daemon down; sock clean"
echo "=== load leg complete (driver rc=$RC) — $(date -Is) ==="
exit $RC
