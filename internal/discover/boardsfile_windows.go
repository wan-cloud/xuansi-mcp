//go:build windows

package discover

import (
	"errors"
	"os"
	"syscall"
	"time"
)

// replaceBoardsFileOS renames src over dst (writeBoardsFile's atomic
// publish). Windows cannot replace a file while ANY handle holds it
// without FILE_SHARE_DELETE — Go's os.Open family omits it (go issue
// #32031) — so a virus scanner briefly holding the freshly written
// temp file, or an editor holding boards.conf, makes the rename fail
// with a sharing violation where unix renames happily. The daemon's
// own reader is closed by then; this bounded retry is the belt for
// holders that are not ours. Every SUCCESSFUL rename is still
// MoveFileEx-atomic: the file is absent-or-complete, only the moment
// of replacement can be delayed.
const (
	shareViolation = syscall.Errno(32) // ERROR_SHARING_VIOLATION
	accessDenied   = syscall.Errno(5)  // ERROR_ACCESS_DENIED

	replaceAttempts = 200
	replaceSleep    = 10 * time.Millisecond // ≈2s worst case: far beyond any scanner's hold
)

func replaceBoardsFileOS(src, dst string) error {
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

// fsyncBoardsDirOS is a deliberate no-op on windows: there is no POSIX
// directory fsync there — os.Open yields a read-only handle and Sync
// (FlushFileBuffers) needs FILE_WRITE_DATA, so it only fails with
// Access is denied. The temp file's own fsync in writeBoardsFile (a
// writable handle — that does work) persists the data before the
// rename, and NTFS journals the metadata change; what stays is the
// whole-file half of the contract: a reader never sees a torn write
// on any platform.
func fsyncBoardsDirOS(string) error { return nil }
