//go:build windows

package main

// b6proc_windows.go — the windows leg of the hil driver's process
// primitives + the bench stale-server scan.
//
// SELECTION RULING (audited 2026-11-09, the B11 win-leg prerequisite;
// the alternative was GenerateConsoleCtrlEvent): the graceful-stop
// candidate spawns the child with CREATE_NEW_PROCESS_GROUP and sends
// it CTRL_BREAK_EVENT. That route is NOT reliable in this driver's
// real environment: the win HIL leg is driven over ssh from the linux
// bench (exactly the mac leg's pattern), and a process sshd spawns
// gets PIPES, not a console. GenerateConsoleCtrlEvent only "sends a
// signal to processes that share the console associated with the
// calling process" (Microsoft docs) — a console-less caller cannot
// deliver one at all; the AttachConsole(child) workaround requires
// the CHILD to own a console (a pipe-spawned child has none —
// AttachConsole then fails ERROR_INVALID_HANDLE), and forcing one via
// CREATE_NEW_CONSOLE/CREATE_NO_WINDOW drags in window flash plus
// undocumented conhost behavior under service sessions. RULING: stop
// children with `taskkill /PID <pid> /T /F` (a hard kill — the only
// mechanism that works deterministically without a console) and
// degrade the S8 stop assertions to "process exited + sock no longer
// occupied" (gracefulStopSupported=false); the daemon's graceful
// "shutdown complete" contract stays owned by the unix legs, which
// deliver a real SIGINT. Hence also: NO CREATE_NEW_PROCESS_GROUP at
// spawn — that flag only serves the ruled-out ctrl-event route.
//
// SIGKILL note: Go maps Process.Signal(syscall.SIGKILL) on windows to
// Process.Kill (TerminateProcess) — the documented mapping — but this
// leg calls Kill() outright so the hard-kill path says what it does.

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// gracefulStopSupported=false: the windows leg hard-kills (taskkill)
// and cannot observe a graceful shutdown — see the ruling above. The
// b11toolchain S8 assertions degrade accordingly.
const gracefulStopSupported = false

// stopLabel / killPast / killGerund spell the stop/kill mechanism in
// transcript lines (S8 verdicts and the die-safe kill note).
const (
	stopLabel  = "taskkill"
	killPast   = "TerminateProcess'd"
	killGerund = "hard-killing"
)

// stopProc stops a live child the only reliable way this driver's
// environment has: taskkill /T /F — a hard kill of the process tree
// (the graceful ctrl-event route needs a shared console; see the file
// header). Output is captured, never teed: taskkill's text is
// locale-localized and would break the ASCII-only evidence
// discipline; only a FAILURE surfaces it (through asciiLine).
func stopProc(p *os.Process) error {
	out, err := exec.Command("taskkill", "/PID", strconv.Itoa(p.Pid), "/T", "/F").CombinedOutput()
	if err != nil {
		return fmt.Errorf("taskkill /PID %d /T /F: %v (%s)", p.Pid, err, asciiLine(string(out)))
	}
	return nil
}

// killProc hard-kills a stuck child directly: Process.Kill is
// TerminateProcess on windows (the documented mapping Signal(SIGKILL)
// rides too — this says it outright).
func killProc(p *os.Process) error { return p.Kill() }

// benchStaleServers scans the bench for stale xuansi processes: no
// pgrep on windows — `tasklist /FI "IMAGENAME eq xuansi*.exe" /FO CSV
// /NH` instead. HONEST LIMIT: tasklist shows image names, not argv,
// so the unix leg's serve-only filter cannot run here; ANY
// xuansi*.exe image counts as potentially stale (the note line says
// so; the resident npm daemon must be stopped before the run either
// way — it holds the controller slot). CSV rows look like
// "xuansi.exe","1234","Console","1","5,240 K" — quoted, so the
// filter's localized INFO line (no match) never parses as a row.
// Returns (scanned, stale) with the unix helper's shape.
func benchStaleServers(h *harness) (bool, int) {
	out, err := exec.Command("tasklist", "/FI", "IMAGENAME eq xuansi*.exe", "/FO", "CSV", "/NH").CombinedOutput()
	if err != nil {
		h.note("tasklist scan failed: " + asciiLine(string(out)) + " (" + err.Error() + ")")
		return false, 0
	}
	stale := 0
	for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		ln = strings.TrimRight(ln, "\r") // windows CRLF
		if !strings.HasPrefix(ln, `"`) {
			continue // the filter's INFO line — not a CSV row
		}
		f := strings.Split(strings.Trim(ln, `"`), `","`)
		if len(f) >= 2 {
			stale++
			fmt.Printf("  [note] stale xuansi process image: %s pid=%s (tasklist sees no argv - any xuansi image counts)\n", f[0], f[1])
		}
	}
	return true, stale
}
