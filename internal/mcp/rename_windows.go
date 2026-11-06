//go:build windows

package mcp

import (
	"errors"
	"os"
	"syscall"
	"time"
)

// replaceFile renames src over dst (the atomic-publish half of
// writeDurableFile). Windows cannot replace a file while ANY handle
// holds it without FILE_SHARE_DELETE — Go's os.Open family omits it
// (go issue #32031), so a handle on dst (or a virus scanner briefly
// holding a freshly-written file) makes MoveFileEx fail
// ERROR_SHARING_VIOLATION/ERROR_ACCESS_DENIED where unix renames
// happily. The READ side now opens share-delete (readFileShared —
// every local reader of a writeDurableFile path routes through it),
// which is what makes replacement possible against OPEN readers at
// all: round 3 tried a writer-side retry alone and the CI hammer
// disproved it — four continuous readers hold the file ~99% of the
// time, so no bounded window can await their common close gap. This
// retry stays as the belt for holders that are NOT our readers (a
// scanner touching the temp or target): transient, and every
// SUCCESSFUL rename is still MoveFileEx-atomic — the
// absent-or-complete contract holds on windows exactly as on unix;
// only the moment of replacement can be delayed.
const (
	shareViolation = syscall.Errno(32) // ERROR_SHARING_VIOLATION
	accessDenied   = syscall.Errno(5)  // ERROR_ACCESS_DENIED

	replaceAttempts = 200
	replaceSleep    = 10 * time.Millisecond // ≈2s worst case: far beyond any reader gap
)

func replaceFile(src, dst string) error {
	var err error
	for try := 0; try < replaceAttempts; try++ {
		err = os.Rename(src, dst)
		if err == nil {
			return nil
		}
		var errno syscall.Errno
		if !errors.As(err, &errno) || (errno != shareViolation && errno != accessDenied) {
			return err // a real failure (missing src, permissions): retrying cannot help
		}
		time.Sleep(replaceSleep)
	}
	return err
}
