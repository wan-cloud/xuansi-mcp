#!/usr/bin/env bash
# Batch-10 task-8b HIL: reboot visibility (panic->agent notification).
# A CONTROLLED reboot round-trip - the harness opens the CDC port
# (open reboots the board, fw-measured +1 boot_epoch; the panic path
# freezes on the IDF 5.5.5 panic-entry stall, carry item 15) while the
# run's daemon holds the TCP controller. The next daemon touch redials,
# its tap hello sights the changed boot_epoch, and the synthetic
# board.reboot event reaches the MCP agent face (events ring +
# xuansi_events_recent - zero new tools). Unit TDD red/green logs are
# copied alongside when present.
#
# SERVING DAEMON (honesty note): the RESIDENT user-session daemon had
# already idle-exited (03:44, no clients for 5m0s - logged in
# ~/.local/state/xuansi/serve.log) before this run; if one IS found it
# is SIGINTed for the duration and restored on exit (b10t5 contract),
# else the npx-cached launcher is respawned detached at teardown so the
# bench ends in its standard state.
#
# Evidence (owner ruling 2026-11-06): daemon HIL transcripts live in
# the PRIVATE knowledge base's evidence dir, never in this public repo
# - pass EVIDDIR (absolute) at invocation; it defaults to the run dir.
# Failing runs keep their own attempt-N transcript; rerun with
# ATTEMPT=N after a fix. ASCII only.
set -euo pipefail
cd "$(dirname "$0")/.."
run="${RUN:-/tmp/opencode/b10-t8b}"
rm -rf "$run"; mkdir -p "$run"
eviddir="${EVIDDIR:-$run/evidence}"
mkdir -p "$eviddir"
attempt="${ATTEMPT:-1}"
# naming per the task brief (the b10 series' convention)
log="$eviddir/2026-11-08-b10-t8b-reboot-visibility-attempt${attempt}.log"
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
    # (a cache hash can exist without its platform subpackage — the
    # MODULE_NOT_FOUND spawn in the 06:40 attempt-2 run)
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
      # blacklist the failed candidate for this glob by renaming nothing:
      # instead glob the next by using the remaining list minus the head
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
echo "--- resident daemon state (pre-run) ---"
resident_pid="$(pgrep -f "serve --sock $RESIDENT_SOCK" | head -1 || true)"
if [ -n "${resident_pid:-}" ]; then
  NPX_BIN="$(readlink -f "/proc/$resident_pid/exe" 2>/dev/null || true)"
  # readlink lands on the node binary for npm launchers; prefer the
  # cmdline's own path shape (b10t5 keeps readlink - the spawned npm
  # binary works either way; here the npx fallback above covers it)
  echo "resident serve pid=$resident_pid exe=${NPX_BIN:-unknown} - SIGINT for the duration of this run"
  kill -INT "$resident_pid"
  i=0
  while kill -0 "$resident_pid" 2>/dev/null && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
  if kill -0 "$resident_pid" 2>/dev/null; then
    echo "resident did not exit within 10s of SIGINT - SIGKILL"
    kill -KILL "$resident_pid" 2>/dev/null || true
  fi
  WAS_RUNNING=1
else
  echo "no resident serve on $RESIDENT_SOCK (it had idle-exited; restore will respawn it so the bench ends standard)"
fi

set +e
go run ./hil -scenario b10t8b -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

# the unit TDD red/green transcripts, when present at their canonical scratch spot
for f in /tmp/opencode/b10t8b/tdd-red.log /tmp/opencode/b10t8b/tdd-green.log; do
  [ -f "$f" ] && cp "$f" "$eviddir/2026-11-08-b10-t8b-$(basename "$f" .log).log" && echo "copied: $(basename "$f")"
done

if [ "$rc" -eq 0 ]; then
  cp "$log" "$eviddir/2026-11-08-b10-t8b-reboot-visibility.log"
  echo "ALL PASS: canonical evidence copied alongside the attempt log (private evidence dir; this public repo keeps nothing)"
fi
exit "$rc"
