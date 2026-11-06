#!/usr/bin/env bash
# Batch-9 task-5 HIL: the camera face end to end - MCP
# xuansi_camera_shot through the whole pipeline, JPEG landing +
# verification (SOI/EOI + Go image/jpeg dims + sha256), the refusal
# matrix (no-lease, bad framesize/frames, the fw observer vector), the
# warm path, camera.off + the 14-pin release, board as found.
# Orchestrates: unit gates -> build at HEAD -> `go run ./hil -scenario
# b9camera` (serve+mcp+list_boards -> no-lease refusal -> first shot
# VGA -> status warm -> frames=2 burst -> QVGA -> input refusals ->
# warm repeat -> observer vector -> camera.off + diag.pins -> release
# -> teardown + sandbox accounting with the landed-JPEG inventory LEFT
# IN PLACE for visual color-order verification).
#
# Evidence (owner ruling 2026-11-06): daemon HIL transcripts live in
# the PRIVATE knowledge base's evidence dir, never in this public repo
# - pass EVIDDIR (absolute) at invocation; it defaults to the run dir.
# The landed JPEGs stay under $RUN/xdg/xuansi/captures after the run.
# Failing runs keep their own attempt-N transcript; rerun with
# ATTEMPT=N after a fix. Batch-9 evidence discipline: ASCII only.
set -euo pipefail
cd "$(dirname "$0")/.."
run="${RUN:-/tmp/opencode/b9-camera}"
rm -rf "$run"; mkdir -p "$run"
eviddir="${EVIDDIR:-$run/evidence}"
mkdir -p "$eviddir"
attempt="${ATTEMPT:-1}"
# local-date naming, the evidence convention the earlier batches use
log="$eviddir/$(date +%F)-b9-t5-camera-hil-attempt${attempt}.log"
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
go run ./hil -scenario b9camera -attempt "${attempt}" -daemon bin/xuansi-linux-amd64 -run "$run"
rc=$?
set -e
echo "driver exit: ${rc}"

if [ "$rc" -eq 0 ]; then
  cp "$log" "$eviddir/$(date +%F)-b9-t5-camera-hil.log"
  echo "ALL PASS: canonical evidence copied alongside the attempt log (private evidence dir; this public repo keeps nothing)"
fi
echo "landed JPEGs (left in place for visual verification): $run/xdg/xuansi/captures/"
exit "$rc"
