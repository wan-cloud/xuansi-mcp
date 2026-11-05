//go:build !windows

package main

// spawn_unix.go — the non-Windows half of the client auto-spawn: how
// the spawned `xuansi serve` is detached from the client. Setsid puts
// it in its own session (new session id, no controlling terminal), so
// it survives the client's exit and ignores the client's terminal
// signals — the daemon outlives whoever started it.

import (
	"os/exec"
	"syscall"
)

// detach puts cmd in its own session, cut loose from the client. The
// attempt count exists for the Windows spawn's job-breakaway retry;
// non-Windows spawns have no job concept and every attempt detaches
// the same way.
func detach(cmd *exec.Cmd, _ int) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
