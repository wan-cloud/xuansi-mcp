//go:build windows

package mcp

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

// readFileShared reads the whole file with a handle opened
// FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE. The DELETE share
// is the point: Go's os.Open/os.ReadFile omit it (deliberate, tracked
// by go issue #32031), and without it NOBODY can replace the file
// while a reader holds it. The workspace's durability contract is
// "concurrent readers never see a torn file, writers replace
// atomically"; on windows that needs the READ side to permit
// replacement — every local reader of a writeDurableFile-managed path
// comes through here: loadManifest, ws_get, the push read-back, and
// the never-torn hammer's reader loop.
//
// Transient contention on BOTH sides (CI matrix round 5, run
// 42165202791): FILE_SHARE_DELETE permits DeleteFile but NOT
// MoveFileEx-replace-over, so the writer's rename still collides with
// an open reader (errno 5/32 on the writer, retried by replaceFile) —
// and symmetrically, opening the target DURING an in-flight rename
// transiently fails ERROR_ACCESS_DENIED on the reader. Both are
// contention, not failure: this reader retries the same errno class
// with a small backoff (mirroring replaceFile) before giving up.
const (
	readSharedAttempts = 200
	readSharedSleep    = 10 * time.Millisecond // ≈2s worst case, past any in-flight rename
)

func readFileShared(name string) ([]byte, error) {
	var err error
	for try := 0; try < readSharedAttempts; try++ {
		var b []byte
		b, err = readFileSharedOnce(name)
		if err == nil {
			return b, nil
		}
		var errno syscall.Errno
		if !errors.As(err, &errno) || (errno != accessDenied && errno != shareViolation) {
			return nil, err // missing file, permissions, anything non-transient: retrying cannot help
		}
		time.Sleep(readSharedSleep)
	}
	return nil, err
}

// readFileSharedOnce is one attempt: open share-delete and read all.
func readFileSharedOnce(name string) ([]byte, error) {
	p, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		if err == windows.ERROR_FILE_NOT_FOUND || err == windows.ERROR_PATH_NOT_FOUND {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	f := os.NewFile(uintptr(h), name)
	b, rerr := io.ReadAll(f)
	cerr := f.Close() // closes the handle
	if rerr != nil {
		return nil, rerr
	}
	if cerr != nil {
		return nil, cerr
	}
	return b, nil
}
