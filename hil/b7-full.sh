#!/usr/bin/env bash
# Task-5 full MCP HIL: the REAL b7 loopback drivers (i2cecho +
# spiloop, fw-repo bodies, sha-bound) pushed through the WHOLE MCP
# pipeline on the real bench board in ONE capture — the v1 driver-story
# closeout. Orchestrates: build + vet + unit green -> the REAL daemon
# (bin/xuansi-linux-amd64 serve, XDG-sandboxed) -> `xuansi mcp` as a
# subprocess whose stdio pipes carry the MCP frames of a go-sdk client
# driven by `go run ./hil -scenario b7mcp` (serve+mcp+list_boards ->
# lease -> fixture on+verify -> i2cecho put/push/attach/vectors ->
# spiloop put/push/attach/vectors -> lease cycle -> resident script
# with BOTH drivers + measure loop + capture + stop + pins -> dynamic
# tool cross-check -> version swap 1.1.0 + rollback 1.0.0 + sha
# reconciliation -> /drv fs_read reconciliation -> fixture-off
# negative -> teardown + out-of-pipeline /drv cleanup + sandbox
# accounting). Failing runs keep their own attempt-N transcript; the
# canonical results/2026-11-05-b7-full.log is written only by the
# ALL-PASS run. Rerun with ATTEMPT=N after a fix.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
run="${RUN:-/tmp/opencode/b7-full}"
rm -rf "$run"; mkdir -p "$run"
attempt="${ATTEMPT:-1}"
log="results/2026-11-05-b7-full-attempt${attempt}.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
echo "repo HEAD: $(git rev-parse --short HEAD) — dirty files: $(git status --porcelain | wc -l)"
echo "attempt: ${attempt} (honest count — failures keep their own transcript)"

echo "--- driver bodies: regenerated + sha-asserted from the fw repo (verbatim, never retyped) ---"
python3 hil/gen-b7-drivers.py

echo "--- unit gates (must be green before the board is touched) ---"
go vet ./...
go test ./... -count=1

echo "--- build the daemon binary fresh at HEAD ---"
make build
go version -m bin/xuansi-linux-amd64 | sed -n '1,8p'

set +e
go run ./hil -scenario b7mcp -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

if [ "$rc" -eq 0 ]; then
  cp "$log" results/2026-11-05-b7-full.log
  echo "ALL PASS: canonical evidence copied to results/2026-11-05-b7-full.log"
fi
exit "$rc"
