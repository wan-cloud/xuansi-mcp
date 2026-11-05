//go:build windows

package main

// spawn_windows.go — the Windows half of the client auto-spawn: how
// the spawned `xuansi serve` is detached from the client.
// CREATE_NEW_PROCESS_GROUP breaks Ctrl-C inheritance (the client's
// console interrupt must not reach the daemon); DETACHED_PROCESS
// leaves it without a console of its own; HideWindow suppresses any
// window a GUI-subsystem parent might otherwise flash.

import (
	"os/exec"
	"syscall"
)

// detachedProcess is the Win32 DETACHED_PROCESS creation flag
// (0x8) — the Go stdlib does not name it.
const detachedProcess = 0x8

// detach cuts cmd loose from the client's console and process group.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | detachedProcess,
		HideWindow:    true,
	}
}
