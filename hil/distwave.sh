#!/usr/bin/env bash
# dist-wave HIL (Task 9, plan 2026-11-18): the four legs against the
# REAL distribution faces and boards - drivers v2 over the release
# CDN, the fw rc chain end-to-end (fw_versions -> fw_fetch ->
# ota_push -> re-verify, board 97c1ac the sacrificial one), and the
# toolchain new entry. Board #1 (d75884, product state) stays
# zero-touch; board #2 ENDS ON the rc image (the canonical 0.7.1
# wave-close overwrites it - ledgered deviation).
#
# SERVING DAEMON (honesty note): a resident user-session daemon is
# SIGINTed for the duration if present; the npx-cached launcher is
# respawned detached at teardown so the bench ends in its standard
# state (the b10t8b contract).
#
# Evidence (owner ruling 2026-11-06): daemon HIL transcripts live in
# the PRIVATE knowledge base's evidence dir, never in this public
# repo. ASCII filenames.

set -uo pipefail

ATTEMPT="${1:-1}"
EVID="${XUANSI_EVID_DIR:-../../evidence/xuansi-daemon}"
STAMP="$(date -u +%Y-%m-%d)"
OUT="$EVID/${STAMP}-dist-wave-hil${ATTEMPT:+-attempt$ATTEMPT}.log"
[ "$ATTEMPT" = "1" ] && OUT="$EVID/${STAMP}-dist-wave-hil.log"

mkdir -p "$EVID"
RUN=/tmp/opencode/dist-wave
rm -rf "$RUN"; mkdir -p "$RUN"

echo "building HEAD daemon + hil runner..." >&2
make build >/dev/null 2>&1 || { echo "make build failed" >&2; exit 1; }
go build -o "$RUN/hil-bin" ./hil >/dev/null 2>&1 || { echo "hil build failed" >&2; exit 1; }

# the resident daemon contract: SIGINT if present, respawn at teardown
RESIDENT_PGID=""
pgrep -f "xuansi serve" >/dev/null 2>&1 && { pkill -INT -f "xuansi serve"; sleep 2; RESIDENT_PGID=1; }
cleanup() {
  if [ -n "$RESIDENT_PGID" ]; then
    nohup npx -y -p xuansi-mcp xuansi serve >/dev/null 2>&1 &
  fi
}
trap cleanup EXIT

"$RUN/hil-bin" -scenario distwave -attempt "$ATTEMPT" -daemon bin/xuansi-linux-amd64 -run "$RUN" 2>&1 | tee "$OUT"
rc=${PIPESTATUS[0]}
echo "evidence: $OUT (exit $rc)" >&2
exit $rc
