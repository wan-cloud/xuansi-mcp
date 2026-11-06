//go:build windows

package main

// spawn_windows_test.go — the windows-only halves of the spawn
// detach/retry: the breakaway-denial classifier (which errno a
// kill-on-close job answers CREATE_BREAKAWAY_FROM_JOB with) and the
// per-attempt creation flags. Runs on the T9 machine through the
// cross-compiled test binary; type-checks on any builder via
// `GOOS=windows go vet`.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// TestRetryWithoutBreakaway pins the retry decision: ERROR_ACCESS_DENIED
// — raw or wrapped in the os.PathError chain exec.Cmd.Start produces —
// is the job's breakaway refusal and is retryable; every other Start
// error would repeat identically on a plainer rebuild.
func TestRetryWithoutBreakaway(t *testing.T) {
	denied := &os.PathError{Op: "CreateProcess", Path: "xuansi.exe", Err: syscall.ERROR_ACCESS_DENIED}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"raw errno", syscall.ERROR_ACCESS_DENIED, true},
		{"patherror chain (exec.Start's shape)", denied, true},
		{"further wrapped", fmt.Errorf("start serve: %w", denied), true},
		{"file-not-found is not a denial", &os.PathError{Op: "CreateProcess", Path: "xuansi.exe", Err: syscall.ERROR_FILE_NOT_FOUND}, false},
		{"plain error", errors.New("exec: no command"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := retryWithoutBreakaway(c.err); got != c.want {
			t.Errorf("%s: retryWithoutBreakaway(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}

// TestDetachAttemptFlags pins the two attempts' creation flags:
// attempt 0 asks to break away from the client's job on top of the
// plain detach set; attempt 1 (the retry after a denial) is the plain
// set alone.
func TestDetachAttemptFlags(t *testing.T) {
	var first exec.Cmd
	detach(&first, 0)
	if got, want := first.SysProcAttr.CreationFlags, baseCreationFlags|createBreakawayFromJob; got != want {
		t.Errorf("attempt 0 flags = %#x, want %#x (plain detach + CREATE_BREAKAWAY_FROM_JOB)", got, want)
	}
	if !first.SysProcAttr.HideWindow {
		t.Error("attempt 0 lost HideWindow")
	}
	var retry exec.Cmd
	detach(&retry, 1)
	if got, want := retry.SysProcAttr.CreationFlags, baseCreationFlags; got != want {
		t.Errorf("attempt 1 flags = %#x, want %#x (plain detach only)", got, want)
	}
	if !retry.SysProcAttr.HideWindow {
		t.Error("attempt 1 lost HideWindow")
	}
}
