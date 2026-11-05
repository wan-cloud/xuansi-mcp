#!/usr/bin/env bash
# Batch-8 task-2 mini-HIL: xuansi_session_reset - the b7 S4b detour's
# proper fix - on the real bench board, in ONE capture. The b7 S4b
# finding: bare-eval pin claims live in ns-0 (owner _eval) and the only
# release was release-lease + re-lease; this capture proves the
# dedicated tool replaces that detour (ONE lease held throughout).
# Orchestrates: unit gates -> build at HEAD -> `go run ./hil -scenario
# b8t2` (serve+mcp+list_boards -> lease -> eval i2c claims ns-0 ->
# cross-prim persistence proof + diag.pins cap-1 -> session_reset
# {omitted} -> 8/9 free + cross-prim ok + i2c re-claim -> script-ns
# variant with reset{0} negative -> release -> cap-2 final pins ->
# teardown + sandbox accounting). Failing runs keep their own
# attempt-N transcript; the canonical results/2026-11-05-b8-t2.log is
# written only by the ALL-PASS run. Rerun with ATTEMPT=N after a fix.
# Batch-8 evidence discipline: ASCII only, no em-dashes.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
run="${RUN:-/tmp/opencode/b8-t2}"
rm -rf "$run"; mkdir -p "$run"
attempt="${ATTEMPT:-1}"
log="results/2026-11-05-b8-t2-attempt${attempt}.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
echo "repo HEAD: $(git rev-parse --short HEAD) - dirty files: $(git status --porcelain | wc -l)"
echo "attempt: ${attempt} (honest count - failures keep their own transcript)"

echo "--- unit gates (must be green before the board is touched) ---"
go vet ./...
go test ./... -count=1 -race

echo "--- build the daemon binary fresh at HEAD ---"
make build
go version -m bin/xuansi-linux-amd64 | sed -n '1,8p'

set +e
go run ./hil -scenario b8t2 -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

if [ "$rc" -eq 0 ]; then
  cp "$log" results/2026-11-05-b8-t2.log
  echo "ALL PASS: canonical evidence copied to results/2026-11-05-b8-t2.log"
fi
exit "$rc"
