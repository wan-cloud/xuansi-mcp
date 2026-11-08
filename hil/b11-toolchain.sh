#!/usr/bin/env bash
# Batch-11 task-11 linux HIL: the toolchain-distribution chain end to
# end - MCP xuansi_toolchain_status reads the (isolated, run-scoped)
# cache as EMPTY and self-guides, xuansi_toolchain_fetch REALLY
# downloads the live index's pinned wasi-sdk (119MB) + wamrc (33MB),
# a second fetch proves the idempotent all-Skipped path, status then
# reads present with the index pins, xuansi_wasm_build compiles a
# hello-scale module from C source, xuansi_driver_push deploys it,
# the fw's bench.wasm face loads and CALLS it (echo round-trip), and
# xuansi_ws_list reconciles the entry.
#
# CACHE ISOLATION (brief's hard rule): XUANSI_WASM_CACHE points
# inside the run dir (wiped per attempt - the "missing" premise holds
# honestly every run); the real ~/.cache/xuansi-wasm is never read
# or written.
#
# SERVING DAEMON (honesty note, b10t5 contract): the RESIDENT
# user-session daemon (npm build) is SIGINTed for the duration (it
# holds the board's controller slot), the run serves with a freshly
# built HEAD binary on a private socket + XDG sandbox, and the
# resident daemon is RESTORED on exit (npx-cache fallback included,
# b10t8b's candidate loop).
#
# Evidence (owner ruling 2026-11-06): daemon HIL transcripts live in
# the PRIVATE knowledge base's evidence dir, never in this public repo
# - pass EVIDDIR (absolute) at invocation; it defaults to the run dir.
# Failing runs keep their own attempt-N transcript; rerun with
# ATTEMPT=N after a fix. ASCII only.
set -euo pipefail
cd "$(dirname "$0")/.."
run="${RUN:-/tmp/opencode/b11-toolchain}"
rm -rf "$run"; mkdir -p "$run"
eviddir="${EVIDDIR:-$run/evidence}"
mkdir -p "$eviddir"
attempt="${ATTEMPT:-1}"
log="$eviddir/2026-11-08-b11-toolchain-linux-attempt${attempt}.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
echo "repo HEAD: $(git rev-parse --short HEAD) - dirty files: $(git status --porcelain | wc -l)"
echo "attempt: ${attempt} (honest count - failures keep their own transcript)"

# The RESIDENT daemon is discovered at runtime (pgrep its serve line,
# readlink /proc/PID/exe for the binary) - no user paths are baked into
# this public repo's script.
RESIDENT_SOCK="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/xuansi/daemon.sock"
SERVE_LOG_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/xuansi"
NPX_BIN=""     # resolved from the resident process at stop time, or the
               # npx cache fallback at restore time
WAS_RUNNING=0

restore_resident() {
  echo ""
  echo "--- restore the resident daemon (detached, standard socket) ---"
  if [ -S "$RESIDENT_SOCK" ]; then
    echo "restore: $RESIDENT_SOCK already present - a daemon owns it; not respawning"
    return 0
  fi
  if [ -z "$NPX_BIN" ] || [ ! -x "$NPX_BIN" ]; then
    # try each npx-cached launcher until one actually binds the socket
    # (a cache hash can exist without its platform subpackage - the
    # MODULE_NOT_FOUND spawn lesson, b10t8b)
    while [ ! -S "$RESIDENT_SOCK" ]; do
      NPX_BIN="$(ls -1 "$HOME"/.npm/_npx/*/node_modules/.bin/xuansi 2>/dev/null | head -1 || true)"
      [ -z "$NPX_BIN" ] && break
      echo "restore: trying $NPX_BIN"
      mkdir -p "$SERVE_LOG_DIR"
      setsid "$NPX_BIN" serve --sock "$RESIDENT_SOCK" >>"$SERVE_LOG_DIR/serve.log" 2>&1 </dev/null &
      local i=0
      while [ $i -lt 25 ]; do
        [ -S "$RESIDENT_SOCK" ] && break
        sleep 0.2; i=$((i+1))
      done
      [ -S "$RESIDENT_SOCK" ] && break
      echo "restore: $NPX_BIN did not bind within 5s - next candidate"
      failed="$NPX_BIN"
      NPX_BIN=""
      CANDIDATES="$(ls -1 "$HOME"/.npm/_npx/*/node_modules/.bin/xuansi 2>/dev/null || true)"
      for c in $CANDIDATES; do
        [ "$c" = "$failed" ] && continue
        NPX_BIN="$c"; break
      done
      [ -z "$NPX_BIN" ] && break
    done
  fi
  if [ -z "$NPX_BIN" ] || [ ! -x "$NPX_BIN" ]; then
    echo "restore: no xuansi launcher found - SKIPPED (the next 'xuansi mcp' start will ensure-spawn one)"
    return 0
  fi
  if [ ! -S "$RESIDENT_SOCK" ]; then
    mkdir -p "$SERVE_LOG_DIR"
    setsid "$NPX_BIN" serve --sock "$RESIDENT_SOCK" >>"$SERVE_LOG_DIR/serve.log" 2>&1 </dev/null &
    local i=0
    while [ $i -lt 50 ]; do
      [ -S "$RESIDENT_SOCK" ] && break
      sleep 0.2; i=$((i+1))
    done
  fi
  if [ -S "$RESIDENT_SOCK" ]; then
    echo "restore: resident daemon respawned ($NPX_BIN), socket $RESIDENT_SOCK bound"
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
echo "--- stop the RESIDENT daemon for this run (it holds the controller slot) ---"
resident_pid="$(pgrep -f "serve --sock $RESIDENT_SOCK" | head -1 || true)"
if [ -n "${resident_pid:-}" ]; then
  NPX_BIN="$(readlink -f "/proc/$resident_pid/exe" 2>/dev/null || true)"
  # readlink lands on the node binary for npm launchers; the npx
  # fallback above covers the restore either way (b10t8b note)
  echo "resident serve pid=$resident_pid exe=${NPX_BIN:-unknown} - SIGINT for the duration of this run"
  kill -INT "$resident_pid"
  i=0
  while kill -0 "$resident_pid" 2>/dev/null && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
  if kill -0 "$resident_pid" 2>/dev/null; then
    echo "resident did not exit within 10s of SIGINT - SIGKILL"
    kill -KILL "$resident_pid" 2>/dev/null || true
  fi
  WAS_RUNNING=1
  echo "resident stopped (socket state: $( [ -S "$RESIDENT_SOCK" ] && echo present || echo removed))"
else
  echo "no resident serve on $RESIDENT_SOCK (restore will respawn it so the bench ends standard)"
fi

set +e
go run ./hil -scenario b11toolchain -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

if [ "$rc" -eq 0 ]; then
  cp "$log" "$eviddir/2026-11-08-b11-toolchain-linux.log"
  echo "ALL PASS: canonical evidence copied alongside the attempt log (private evidence dir; this public repo keeps nothing)"
fi
echo "sandbox kept for inspection: workspace $run/xdg/xuansi/workspace/ - toolchain cache $run/wasm-cache/ (wiped by the next attempt)"
exit "$rc"
