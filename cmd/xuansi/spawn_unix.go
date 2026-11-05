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

// detach puts cmd in its own session, cut loose from the client.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
