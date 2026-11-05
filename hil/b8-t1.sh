#!/usr/bin/env bash
# Batch-8 task-1 mini-HIL: the pipeline's REMOVAL path (fs_rm +
# xuansi_driver_pull) on the real bench board, in ONE capture. Closes
# the b7 full-HIL finding (deploy + read back but no cleanup - /drv had
# to be cleaned by an out-of-pipeline direct REPL). Orchestrates:
# unit gates -> build at HEAD -> `go run ./hil -scenario b8t1`
# (serve+mcp+list_boards -> lease -> fs_write/fs_rm/fs_read gone-proof
# -> whitelist negatives -> ws_put/push/require/pull/history-kept/
# rollback-repush/require/pull -> double-cleanup honesty + release +
# teardown + sandbox accounting). Failing runs keep their own
# attempt-N transcript; the canonical results/2026-11-05-b8-t1.log is
# written only by the ALL-PASS run. Rerun with ATTEMPT=N after a fix.
# Batch-8 evidence discipline: ASCII only, no em-dashes.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
run="${RUN:-/tmp/opencode/b8-t1}"
rm -rf "$run"; mkdir -p "$run"
attempt="${ATTEMPT:-1}"
log="results/2026-11-05-b8-t1-attempt${attempt}.log"
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
go run ./hil -scenario b8t1 -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

if [ "$rc" -eq 0 ]; then
  cp "$log" results/2026-11-05-b8-t1.log
  echo "ALL PASS: canonical evidence copied to results/2026-11-05-b8-t1.log"
fi
exit "$rc"
