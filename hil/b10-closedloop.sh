#!/usr/bin/env bash
# b10 task-7 HIL (B12-P2 v6 all-module upgrade): the eyes+hands CLOSED
# LOOP with every device on the module path - MCP drives the st7789
# screen (wasm), the GC2145 camera rides gc2145+jpge (init/settle/shot
# through per-call budget_ms run_lua; the reply frames land the JPEG),
# ledwatch@2 judges the frame (geometry pairing vs the aim reference +
# the R/B color-order verdict through the clear-RED/clear-BLUE
# mirrors), and the GPIO1->2 wired pair confirms electrically - one
# continuous capture from driver_fetch of ALL FOUR modules (gc2145,
# jpge, ledwatch@2, st7789 — co-resident in the 2MiB-pool era) off the
# real public index to a board-clean teardown. The fw op camera face
# is gone (protocol_ver 6): there is no camera.off op and no
# camera_shot tool anywhere in this scenario.
#
# MODE=aim  : one interactive aim round (ROUND=N) - the module
#             pipeline, camera-alone FPC differential (module shot, no
#             screen work), border pattern up, two shot+analyze
#             passes, AIM_OK / AIM_ADJUST / BLOCKED-FPC verdict, JPEGs
#             to OUTDIR for the owner.
# MODE=loop : the closed loop (ATTEMPT=N honest count; REF=x,y,w,h
#             reference screen bbox from a converged aim round, TOL px
#             default 20; no REF = the brief's step-2 honest red).
#
# SERVING DAEMON (honesty note, b10t5 pattern): the RESIDENT
# user-session daemon may be an older npm binary - this script SIGINTs
# it for the duration, serves the run with a freshly built HEAD binary
# on a private socket + XDG sandbox, and RESTORES the resident daemon
# on exit.
#
# Evidence (owner ruling 2026-11-06): daemon HIL transcripts live in
# the PRIVATE knowledge base's evidence dir, never in this public repo
# - pass EVIDDIR (absolute) at invocation; it defaults to the run dir.
# Failing runs keep their own attempt-N transcript; rerun with
# ATTEMPT=N after a fix. ASCII only.
set -euo pipefail
cd "$(dirname "$0")/.."
mode="${MODE:-loop}"
run="${RUN:-/tmp/opencode/b10-t7}"
rm -rf "$run"; mkdir -p "$run"
eviddir="${EVIDDIR:-$run/evidence}"
outdir="${OUTDIR:-$eviddir}"
mkdir -p "$eviddir" "$outdir"
attempt="${ATTEMPT:-1}"
round="${ROUND:-1}"
ref="${REF:-}"
tol="${TOL:-20}"
case "$mode" in
  aim)  log="$eviddir/2026-11-08-b10-t7-aim-round${round}.log" ;;
  loop) log="$eviddir/2026-11-08-b10-t7-closedloop-attempt${attempt}.log" ;;
  *)    echo "MODE must be aim or loop" >&2; exit 2 ;;
esac
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
echo "repo HEAD: $(git rev-parse --short HEAD) - dirty files: $(git status --porcelain | wc -l)"
echo "mode: $mode (attempt=${attempt} round=${round} ref='${ref}' tol=${tol})"
echo "evidence dir: $eviddir (JPEG dir: $outdir)"

# The RESIDENT daemon is discovered at runtime (pgrep its serve line,
# readlink /proc/PID/exe for the binary) - no user paths are baked into
# this public repo's script.
RESIDENT_SOCK="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/xuansi/daemon.sock"
SERVE_LOG_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/xuansi"
NPM_BIN="" # resolved from the resident process at stop time

restore_resident() {
  echo ""
  echo "--- restore the resident daemon (detached, standard socket) ---"
  if [ -S "$RESIDENT_SOCK" ]; then
    echo "restore: $RESIDENT_SOCK already present - a daemon owns it; not respawning"
    return 0
  fi
  if [ -z "$NPM_BIN" ] || [ ! -x "$NPM_BIN" ]; then
    echo "restore: resident binary unknown (was it running?) - SKIPPED (the next 'xuansi mcp' start will ensure-spawn one)"
    return 0
  fi
  mkdir -p "$SERVE_LOG_DIR"
  setsid "$NPM_BIN" serve --sock "$RESIDENT_SOCK" >>"$SERVE_LOG_DIR/serve.log" 2>&1 </dev/null &
  local i=0
  while [ $i -lt 50 ]; do
    [ -S "$RESIDENT_SOCK" ] && break
    sleep 0.2; i=$((i+1))
  done
  if [ -S "$RESIDENT_SOCK" ]; then
    echo "restore: resident daemon respawned ($NPM_BIN), socket $RESIDENT_SOCK bound (new client connections work; already-open MCP sessions stay failed-fast until restarted)"
  else
    echo "restore: socket did not come back within 10s - check $SERVE_LOG_DIR/serve.log"
  fi
}
trap restore_resident EXIT

echo "--- unit gates (must be green before the board is touched) ---"
go vet ./...
go test ./... -count=1

echo "--- build the daemon binary fresh at HEAD ---"
make build
go version -m bin/xuansi-linux-amd64 | sed -n '1,4p'

echo ""
echo "--- stop the RESIDENT daemon for this run (it holds the controller slot; the shipped npm build lacks driver_fetch) ---"
resident_pid="$(pgrep -f "serve --sock $RESIDENT_SOCK" | head -1 || true)"
if [ -n "${resident_pid:-}" ]; then
  NPM_BIN="$(readlink -f "/proc/$resident_pid/exe" 2>/dev/null || true)"
  echo "resident serve pid=$resident_pid exe=${NPM_BIN:-unknown}"
  kill -INT "$resident_pid"
  i=0
  while kill -0 "$resident_pid" 2>/dev/null && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
  if kill -0 "$resident_pid" 2>/dev/null; then
    echo "resident did not exit within 10s of SIGINT - SIGKILL"
    kill -KILL "$resident_pid" 2>/dev/null || true
  fi
  echo "resident stopped (exit; socket state: $( [ -S "$RESIDENT_SOCK" ] && echo present || echo removed))"
else
  echo "no resident serve found on $RESIDENT_SOCK (already stopped)"
fi

set +e
case "$mode" in
  aim)  go run ./hil -scenario b10t7aim -round "${round}" -daemon bin/xuansi-linux-amd64 -run "$run" -outdir "$outdir" ;;
  loop) go run ./hil -scenario b10t7 -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run" -outdir "$outdir" -ref "$ref" -tol "$tol" ;;
esac
rc=$?
set -e
echo "driver exit: ${rc}"

if [ "$rc" -eq 0 ] && [ "$mode" = "loop" ]; then
  cp "$log" "$eviddir/2026-11-08-b10-t7-closedloop.log"
  echo "ALL PASS: canonical evidence copied alongside the attempt log (private evidence dir; this public repo keeps nothing)"
fi
echo "sandbox workspace (fetched artifacts stay for inspection): $run/xdg/xuansi/workspace/"
exit "$rc"
