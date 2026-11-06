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
// holds it without FILE_SHARE_DELETE, and Go's os.Open family opens
// with only FILE_SHARE_READ|FILE_SHARE_WRITE — so a concurrent reader
// of dst (or a virus scanner briefly holding a freshly-written file)
// makes MoveFileEx fail ERROR_SHARING_VIOLATION/ERROR_ACCESS_DENIED
// where unix renames happily. Those holders are transient; retry with
// a small backoff and the rename lands in a gap between reads. Every
// SUCCESSFUL rename is still MoveFileEx-atomic — the absent-or-complete
// contract holds on windows exactly as on unix; only the moment of
// replacement can be delayed (first CI matrix round 3 windows finding:
// the never-torn hammer's 4 concurrent readers made ~1 in 100 renames
// bounce; the brief's suspected rename-of-open-source was already
// closed — writeDurableFile closes the temp before renaming — the open
// handle is always the TARGET's).
const (
	shareViolation = syscall.Errno(32) // ERROR_SHARING_VIOLATION
	accessDenied   = syscall.Errno(5)  // ERROR_ACCESS_DENIED

	replaceAttempts  = 200
	replaceSleep     = 10 * time.Millisecond // ≈2s worst case: far beyond any reader gap
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
