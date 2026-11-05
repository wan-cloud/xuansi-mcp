#!/usr/bin/env bash
# Task-7 full spawn/idle HIL: the batch's auto-spawn + idle self-exit +
# adopt story on the real bench board in ONE capture (b6/b7 house
# style). Orchestrates: unit gates -> build at HEAD -> `go run ./hil
# -scenario spawnidle` (cold auto-spawn + protocol-clean list ->
# resident holder + script -> SIGKILL the client -> adopt in-window ->
# staged --idle-exit 15s self-exit waited out for real -> spawn #2
# survivor audit + bench cleanup -> concurrent spawn race (one winner,
# loser exit 0, deliberate double-start proof) -> second-cycle smoke ->
# teardown + sandbox accounting). Failing runs keep their own
# attempt-N transcript; the canonical results/2026-11-06-spawn-idle-full.log
# is written only by the ALL-PASS run. Rerun with ATTEMPT=N after a fix.
# Batch evidence discipline: ASCII only, no em-dashes.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
run="${RUN:-/tmp/opencode/spawnidle-full}"
attempt="${ATTEMPT:-1}"
log="results/2026-11-06-spawn-idle-full-attempt${attempt}.log"

# pre-clean: a failed earlier attempt may have left a detached sandbox
# serve on this run dir's sock (the dir is wiped below, but the process
# would linger and poison the S0 bench-clean check)
if pkill -f "serve --sock ${run}/runtime" 2>/dev/null; then
  echo "pre-clean: killed leftover sandbox serve(s) from a failed attempt"
  sleep 1
fi

rm -rf "$run"; mkdir -p "$run"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
echo "repo HEAD: $(git rev-parse --short HEAD) - dirty files: $(git status --porcelain | wc -l)"
echo "attempt: ${attempt} (honest count - failures keep their own transcript)"

echo "--- unit gates (must be green before the board is touched) ---"
go vet ./...
go test ./... -count=1

echo "--- build the daemon binary fresh at HEAD ---"
make build
go version -m bin/xuansi-linux-amd64 | sed -n '1,8p'

set +e
go run ./hil -scenario spawnidle -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

if [ "$rc" -eq 0 ]; then
  cp "$log" results/2026-11-06-spawn-idle-full.log
  echo "ALL PASS: canonical evidence copied to results/2026-11-06-spawn-idle-full.log"
fi
exit "$rc"
