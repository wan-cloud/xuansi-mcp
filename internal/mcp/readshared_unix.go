//go:build !windows

package mcp

import "os"

// readFileShared reads the whole file. On unix this is os.ReadFile —
// a plain open never blocks a concurrent rename-over (see
// readshared_windows.go for why windows needs the special form).
func readFileShared(name string) ([]byte, error) {
	return os.ReadFile(name)
}
