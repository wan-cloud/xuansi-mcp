//go:build !windows

package mcp

import "os"

// fsyncDir (unix): open the directory and fsync it — flushes the
// directory's entries (create/rename) to stable storage so the rename
// writeDurableFile just performed cannot vanish after a crash.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
