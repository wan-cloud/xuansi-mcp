#!/usr/bin/env bash
# Task-8 USB HIL: run the Go HIL runner against the live board over CDC
# and capture the transcript to results/. In-log hello lines bind the
# run to protocol_ver + fw_build; the runner asserts both in-process.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
log="results/2026-11-05-b5-t8-usb.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
go run ./hil -scenario usb
