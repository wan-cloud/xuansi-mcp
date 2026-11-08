//go:build !windows

package main

// b6proc_unix.go — the unix leg of the hil driver's process
// primitives + the bench stale-server scan. Every helper here is the
// ORIGINAL pre-windows code, MOVED VERBATIM: the linux/mac transcript
// must not drift by one byte (the S0/S8 verdict lines and notes print
// through these paths). The windows twin (b6proc_windows.go) carries
// the selection ruling for the ssh-no-console environment.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

// gracefulStopSupported: unix can deliver a real SIGINT and watch the
// daemon's own clean shutdown — the "shutdown complete" + sock-file
// removal contract is asserted on this leg (b11toolchain S8).
const gracefulStopSupported = true

// stopLabel / killPast / killGerund spell the stop/kill mechanism in
// transcript lines (S8 verdicts and the die-safe kill note) — the
// unix spellings reproduce the original output byte for byte.
const (
	stopLabel  = "SIGINT"
	killPast   = "SIGKILLed"
	killGerund = "SIGKILLing"
)

// stopProc delivers the graceful stop to a live child.
func stopProc(p *os.Process) error { return p.Signal(syscall.SIGINT) }

// killProc delivers the hard kill to a (possibly stuck) child.
func killProc(p *os.Process) error { return p.Signal(syscall.SIGKILL) }

// benchStaleServers scans the bench for stale xuansi serve daemons
// (the b11 S0 prelude): pgrep -af xuansi, parsing each line's argv —
// only `serve` counts as stale (the controller-slot owner); an idle
// `mcp` face owns nothing without its daemon. Returns (scanned,
// stale): scanned=false means pgrep matched NOTHING at all (its exit
// 1 — the cleanest bench) and the caller emits the note line only,
// never a verdict — the original b11toolchain.go control flow, kept
// so the linux transcript is byte-identical.
func benchStaleServers(h *harness) (bool, int) {
	out, err := exec.Command("pgrep", "-af", "xuansi").CombinedOutput()
	if err != nil {
		h.note("pgrep: no xuansi processes found (clean bench)")
		return false, 0
	}
	stale := 0
	for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Fields(ln)
		// pgrep -af lines LEAD WITH THE PID; only `serve` counts as
		// stale (the controller-slot owner). An idle `mcp` face owns
		// nothing without its daemon.
		if len(f) >= 3 && strings.Contains(filepath.Base(f[1]), "xuansi") && f[2] == "serve" {
			stale++
			fmt.Printf("  [note] stale xuansi serve process: %s\n", asciiLine(ln))
		}
	}
	return true, stale
}
