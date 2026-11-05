//go:build windows

package main

// spawn_windows.go — the Windows half of the client auto-spawn: how
// the spawned `xuansi serve` is detached from the client.
// CREATE_NEW_PROCESS_GROUP breaks Ctrl-C inheritance (the client's
// console interrupt must not reach the daemon); DETACHED_PROCESS
// leaves it without a console of its own; HideWindow suppresses any
// window a GUI-subsystem parent might otherwise flash.
//
// None of that, however, escapes a kill-on-close JOB: sshd, CI
// runners and service wrappers place session processes in jobs whose
// handles carry JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, and job
// membership is inherited by children regardless of detach flags —
// when the session exits the job closes and the detached daemon dies
// mid-sweep (the T9 live finding: the daemon vanished seconds after
// the ssh client exited, before its discovery sweep logged anything).
// So the FIRST spawn attempt also asks to CREATE_BREAKAWAY_FROM_JOB;
// a job without JOB_OBJECT_LIMIT_BREAKAWAY_OK denies it, and
// startServe (spawn.go) retries once with the plain flags — a daemon
// inside the job beats no daemon.

import (
	"errors"
	"os/exec"
	"syscall"
)

// detachedProcess is the Win32 DETACHED_PROCESS creation flag
// (0x8) — the Go stdlib does not name it.
const detachedProcess = 0x8

// createBreakawayFromJob is the Win32 CREATE_BREAKAWAY_FROM_JOB
// creation flag (0x01000000) — likewise unnamed by the stdlib. It
// asks CreateProcess to move the child out of the caller's job; the
// job must permit it (JOB_OBJECT_LIMIT_BREAKAWAY_OK) or the call
// fails with ERROR_ACCESS_DENIED, which is the retry signal.
const createBreakawayFromJob = 0x01000000

// baseCreationFlags is the plain detach flag set shared by every
// spawn attempt.
const baseCreationFlags uint32 = syscall.CREATE_NEW_PROCESS_GROUP | detachedProcess

// detach cuts cmd loose from the client's console and process group.
// attempt 0 additionally asks to break away from the client's job
// (see the file comment); attempt 1 is the plain set after a denied
// breakaway.
func detach(cmd *exec.Cmd, attempt int) {
	flags := baseCreationFlags
	if attempt == 0 {
		flags |= createBreakawayFromJob
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: flags,
		HideWindow:    true,
	}
}

// retryWithoutBreakaway reports whether err is the job refusing a
// breakaway: ERROR_ACCESS_DENIED out of CreateProcess, the shape a
// job without JOB_OBJECT_LIMIT_BREAKAWAY_OK answers with (and, on
// some builds, a spawn from outside any job). The one Start error a
// plainer rebuild can survive — every other failure (missing binary,
// unwritable log, ...) would repeat identically. A genuinely
// access-denied exe just costs one extra failed attempt before the
// error surfaces.
func retryWithoutBreakaway(err error) bool {
	return errors.Is(err, syscall.ERROR_ACCESS_DENIED)
}

// spawnRetryOnStartErr is the platform hook startServe consults: on
// Windows a denied job breakaway is worth exactly one retry without
// the flag.
func init() { spawnRetryOnStartErr = retryWithoutBreakaway }
