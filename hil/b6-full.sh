#!/usr/bin/env bash
# Task-13 full MCP HIL: the entire batch-6 MCP face on the real bench
# board through a REAL MCP client, in ONE capture. Orchestrates:
# build + vet + unit green -> the REAL daemon (bin/xuansi-linux-amd64
# serve, XDG-sandboxed) -> `xuansi mcp` as a subprocess whose stdio
# pipes carry the MCP frames of a go-sdk client driven by
# `go run ./hil -scenario b6mcp` (discovery -> lease -> dynamic tools ->
# args bridge -> submit+job event -> gpio edges -> events_recent ->
# stdout capture -> workspace put/push/require -> ops -> release ->
# clean teardown). Failing runs keep their own attempt-N transcript;
# the canonical results/2026-11-05-b6-full.log is written only by the
# ALL-PASS run. Rerun with ATTEMPT=N after a fix.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
run="${RUN:-/tmp/opencode/b6-full}"
rm -rf "$run"; mkdir -p "$run"
attempt="${ATTEMPT:-1}"
log="results/2026-11-05-b6-full-attempt${attempt}.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
echo "repo HEAD: $(git rev-parse --short HEAD) — dirty files: $(git status --porcelain | wc -l)"
echo "attempt: ${attempt} (honest count — failures keep their own transcript)"

echo "--- unit gates (must be green before the board is touched) ---"
go vet ./...
go test ./... -count=1

echo "--- build the daemon binary fresh at HEAD ---"
make build
go version -m bin/xuansi-linux-amd64 | sed -n '1,8p'

set +e
go run ./hil -scenario b6mcp -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

if [ "$rc" -eq 0 ]; then
  cp "$log" results/2026-11-05-b6-full.log
  echo "ALL PASS: canonical evidence copied to results/2026-11-05-b6-full.log"
fi
exit "$rc"
