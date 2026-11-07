#!/usr/bin/env bash
# Batch-10 task-5 HIL: the driver_fetch leg end to end - MCP
# xuansi_driver_fetch pulls REAL module versions from the live public
# index (wan-cloud/xuansi-drivers, GitHub raw) into the daemon-side
# workspace, the EXISTING xuansi_driver_push deploys them (magic-gated
# .wasm leg), the fw's bench.wasm face loads/calls hello and ledwatch,
# and the deferred >16KB chunked-transport proof runs with a synthetic
# ~40KB payload through the same MCP face. Unit TDD red/green logs are
# copied alongside.
#
# SERVING DAEMON (honesty note): the RESIDENT user-session daemon is
# the npm 0.1.1 binary - it does NOT have B10-T1's chunked transport.
# This script SIGINTs it for the duration (it holds the board's
# controller slot and would starve the run's HEAD daemon), serves the
# run with a freshly built HEAD binary on a private socket + XDG
# sandbox, and RESTORES the resident daemon on exit (the same detached
# spawn contract `xuansi mcp`'s ensureBeforeDial applies).
#
# Evidence (owner ruling 2026-11-06): daemon HIL transcripts live in
# the PRIVATE knowledge base's evidence dir, never in this public repo
# - pass EVIDDIR (absolute) at invocation; it defaults to the run dir.
# Failing runs keep their own attempt-N transcript; rerun with
# ATTEMPT=N after a fix. ASCII only.
set -euo pipefail
cd "$(dirname "$0")/.."
run="${RUN:-/tmp/opencode/b10-t5}"
rm -rf "$run"; mkdir -p "$run"
eviddir="${EVIDDIR:-$run/evidence}"
mkdir -p "$eviddir"
attempt="${ATTEMPT:-1}"
# naming per the task brief (the b10 series' convention)
log="$eviddir/2026-11-08-b10-t5-driver-fetch-attempt${attempt}.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
echo "repo HEAD: $(git rev-parse --short HEAD) - dirty files: $(git status --porcelain | wc -l)"
echo "attempt: ${attempt} (honest count - failures keep their own transcript)"

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
echo "--- stop the RESIDENT daemon for this run (it holds the controller slot; the shipped npm build lacks T1 chunking) ---"
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
go run ./hil -scenario b10t5 -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

# the unit TDD red/green transcripts, when present at their canonical scratch spot
for f in /tmp/opencode/b10t5/tdd-red.log /tmp/opencode/b10t5/tdd-green.log; do
  [ -f "$f" ] && cp "$f" "$eviddir/2026-11-08-b10-t5-$(basename "$f" .log).log" && echo "copied: $(basename "$f")"
done

if [ "$rc" -eq 0 ]; then
  cp "$log" "$eviddir/2026-11-08-b10-t5-driver-fetch.log"
  echo "ALL PASS: canonical evidence copied alongside the attempt log (private evidence dir; this public repo keeps nothing)"
fi
echo "sandbox workspace (fetched artifacts stay for inspection): $run/xdg/xuansi/workspace/"
exit "$rc"
