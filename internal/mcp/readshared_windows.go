//go:build windows

package mcp

import (
	"io/fs"
	"os"

	"io"

	"golang.org/x/sys/windows"
)

// readFileShared reads the whole file with a handle opened
// FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE. The DELETE share
// is the point: Go's os.Open/os.ReadFile omit it (deliberate, tracked
// by go issue #32031), and without it NOBODY can rename over the file
// while a reader holds it — MoveFileEx fails ERROR_SHARING_VIOLATION.
// The workspace's durability contract is "concurrent readers never see
// a torn file, writers replace atomically"; on windows that needs the
// READ side to permit replacement (CI matrix round 4 finding: round 3's
// writer-side retry cannot land against continuous readers — four
// tight-loop os.ReadFile readers hold the file ~99% of the time, so
// the probability of all four sitting in their sub-microsecond close
// gap simultaneously is nil, and no bounded retry window helps).
// Every local reader of a writeDurableFile-managed path must come
// through here: loadManifest, ws_get, and the push read-back, plus the
// never-torn hammer's reader loop.
func readFileShared(name string) ([]byte, error) {
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
