#!/usr/bin/env bash
# Task-8 TCP HIL: no-drain hello, USB/TCP coexistence arbitration
# (§12), keepalive dead-link (iptables DROP 4-tuple preferred; the
# wifi.off net-down path measured as the recorded alternative).
# Captures the transcript to results/.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
log="results/2026-11-05-b5-t8-tcp.log"
exec > >(tee "$log") 2>&1
echo "capture: $log ($(date -Is))"
go run ./hil -scenario tcp -deadlink auto
