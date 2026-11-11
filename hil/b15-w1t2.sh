#!/usr/bin/env bash
# B15 W1-T2 (⑨) cross-subnet pin dogfood — board #1 d75884 at
# 10.1.11.204:7777 (mDNS unreachable from this host, TCP direct
# reachable). Three DEDICATED daemon lives on one private sock (the
# stock daemon's socket is never touched):
#   A (zero-config boards.conf)  — CLI legs: boards pin / list / lease /
#                                  eval / release / unpin / list
#   B (seed boards.conf ⑨②)      — name=mac6@ip:port line: the entry
#                                  materializes from the seed alone
#   C (zero-config again)         — internal-API legs (hil b15w1t2):
#                                  pin → list → lease → eval → op
#                                  log.read → watch_events → board_status
#                                  → release → unpin
#
# Usage: b15-w1t2.sh [LOGPATH]   (no LOGPATH = stdout only; the
# committed repo carries no evidence — the mgmt repo's evidence/ is the
# canonical home, per the privacy discipline.)
set -euo pipefail
cd "$(dirname "$0")/.."

RUN=/tmp/opencode/b15-w1t2
mkdir -p "$RUN"
SOCK="$RUN/d.sock"
LOG="${1:-}"
if [ -n "$LOG" ]; then
  mkdir -p "$(dirname "$LOG")"
  exec > >(tee "$LOG") 2>&1
fi

echo "=== b15 W1-T2 cross-subnet pin dogfood (CLI + seed + internal API) — $(date -Is) ==="
echo "run dir: $RUN  sock: $SOCK"

go build -o "$RUN/xuansi" ./cmd/xuansi
echo "daemon binary built: $RUN/xuansi"

cleanup() {
  pkill -f "xuansi serve --sock $SOCK" 2>/dev/null || true
}
trap cleanup EXIT

# one daemon life at a time on the sock
start_daemon() { # $1 = boards file
  pkill -f "xuansi serve --sock $SOCK" 2>/dev/null || true
  sleep 0.3
  rm -f "$SOCK"
  "$RUN/xuansi" serve --sock "$SOCK" --grace-ttl 30s --boards-file "$1" &
  SRV=$!
  for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
  echo "daemon pid=$SRV serving $SOCK (boards-file $1)"
}
stop_daemon() {
  kill "${SRV:-}" 2>/dev/null || true
  wait "${SRV:-}" 2>/dev/null || true
  SRV=""
}

echo
echo "——— daemon A: CLI legs ———"
: > "$RUN/boards.conf"
start_daemon "$RUN/boards.conf"
"$RUN/xuansi" boards pin --sock "$SOCK" d75884 10.1.11.204:7777
"$RUN/xuansi" list --sock "$SOCK"
"$RUN/xuansi" lease --sock "$SOCK" --holder b15-w1t2 d75884
"$RUN/xuansi" eval --sock "$SOCK" --holder b15-w1t2 d75884 'return "pin-route-eval-ok", 6*7'
"$RUN/xuansi" release --sock "$SOCK" --holder b15-w1t2 d75884
"$RUN/xuansi" boards unpin --sock "$SOCK" d75884
"$RUN/xuansi" list --sock "$SOCK"
stop_daemon

echo
echo "——— daemon B: boards.conf seed leg (⑨②: name=mac6@ip:port) ———"
printf '# b15 w1t2 seed fixture\nbench1=d75884@10.1.11.204:7777\n' > "$RUN/boards-seed.conf"
start_daemon "$RUN/boards-seed.conf"
# the seed's pin entries materialize on the async initial sweep — poll
# for the row (bounded), the same await the unit suite's awaitSweep does
for _ in $(seq 1 50); do
  OUT=$("$RUN/xuansi" list --sock "$SOCK") || true
  echo "$OUT" | grep -q "bench1" && break
  sleep 0.1
done
echo "$OUT"
echo "$OUT" | grep -q "bench1.*pin.*10.1.11.204:7777" || { echo "SEED LEG FAILED: no bench1 pin row"; exit 1; }
stop_daemon

echo
echo "——— daemon C: internal-API legs (hil b15w1t2) ———"
start_daemon "$RUN/boards.conf"
go run ./hil -scenario b15w1t2 -b15sock "$SOCK" -b15board d75884 -b15endpoint 10.1.11.204:7777 -b15holder b15-w1t2-api
stop_daemon

echo
echo "——— teardown ———"
if pgrep -af "xuansi serve --sock $SOCK" >/dev/null 2>&1; then
  echo "STRAY daemon on $SOCK:"; pgrep -af "xuansi serve --sock $SOCK"; exit 1
fi
echo "dedicated daemons down; sock clean"
echo "=== dogfood complete — $(date -Is) ==="
