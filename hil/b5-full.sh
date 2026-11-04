#!/usr/bin/env bash
# Task-12 full-suite HIL: the REAL daemon (bin/xuansi-linux-amd64 serve,
# built fresh at HEAD) driven as a subprocess through its unix-socket API —
# every batch-5 component in ONE capture: serve up -> discovery (dual
# source) -> lease+structured eval -> fs 16KB -> submit long job -> gpio
# events -> grace adopt (in-flight job continuity) -> grace expiry ->
# link-loss resync (orphan policy) -> release -> mDNS-degraded wifi.off
# phase (LAST) -> SIGINT clean shutdown. Captures the transcript to
# results/ (the ONE evidence file). Rerun with ATTEMPT=N after a fix.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
run="${RUN:-/tmp/opencode/b5-full}"
rm -rf "$run"; mkdir -p "$run"
log="results/2026-11-05-b5-full.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
echo "repo HEAD: $(git rev-parse --short HEAD) — dirty files: $(git status --porcelain | wc -l)"
echo "daemon binary: built fresh from this tree (make build)"
make build
go run ./hil -scenario full -attempt "${ATTEMPT:-1}" -daemon bin/xuansi-linux-amd64 -run "$run"
