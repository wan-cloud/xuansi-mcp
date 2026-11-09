#!/usr/bin/env bash
# b9 camera HIL (v6 recipe scenario, B12-P2 T8): the agent camera
# recipe end to end on the de-camerad face - driver_fetch (live index)
# -> push -> ONE budgeted run_lua whose reply frames carry the JPEG ->
# the agent (this harness) decodes, lands and verifies the files
# itself (SOI/EOI + Go image/jpeg dims + sha256), the v6 refusal
# matrix (retired op face / client budget domain / no-lease / fw
# observer gate), the warm repeat, the module-path cold-board finish
# (gc2145.off + pins), board as found. Ruling 3: same scenario on the
# module path; the version stamp (v6/recipe, protocol_ver 6, fw dcaa9ae)
# distinguishes the eras - the B9-T5 op-face original lives in git
# history with its 2026-11-07 evidence.
# Orchestrates: unit gates -> build at HEAD -> `go run ./hil -scenario
# b9camera` (serve+mcp+list_boards -> lease-first ladder -> sweep ->
# fetch x2 -> push x2 -> load x2 -> THE RECIPE (budget_ms=10000,
# frames->JPEG) -> warm repeat -> refusals -> cold finish -> release
# -> teardown + the landed-JPEG inventory in $RUN/recipe-jpegs).
#
# Evidence (owner ruling 2026-11-06): daemon HIL transcripts live in
# the PRIVATE knowledge base's evidence dir, never in this public repo
# - pass EVIDDIR (absolute) at invocation; it defaults to the run dir.
# Failing runs keep their own attempt-N transcript; rerun with
# ATTEMPT=N after a fix. ASCII only.
set -euo pipefail
cd "$(dirname "$0")/.."
run="${RUN:-/tmp/opencode/b9-camera}"
rm -rf "$run"; mkdir -p "$run"
eviddir="${EVIDDIR:-$run/evidence}"
mkdir -p "$eviddir"
attempt="${ATTEMPT:-1}"
# local-date naming + the v6 recipe version stamp (ruling 3)
log="$eviddir/$(date +%F)-b9-camera-recipe-v6-attempt${attempt}.log"
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
  cp "$log" "$eviddir/$(date +%F)-b9-camera-recipe-v6.log"
  echo "ALL PASS: canonical evidence copied alongside the attempt log (private evidence dir; this public repo keeps nothing)"
fi
echo "landed JPEGs (agent-landed by the harness): $run/recipe-jpegs/"
exit "$rc"
