#!/usr/bin/env bash
# Task-9 discovery HIL: live board on WiFi+USB — Refresh merges both
# faces into one mac6-keyed entry, Resolve hits by mac6/alias/hostname,
# lazy enrich via an observer hello, wifi.off downgrades the mdns
# source away (last-known kept), CDC-reboot restore brings the dual
# source back. Captures the transcript to results/.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
log="results/2026-11-05-b5-t9-discover.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
go run ./hil -scenario discover
