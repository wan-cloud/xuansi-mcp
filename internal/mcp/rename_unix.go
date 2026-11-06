//go:build !windows

package mcp

import "os"

// replaceFile renames src over dst (the atomic-publish half of
// writeDurableFile). On unix this is a plain rename: replacing an open
// file is legal, no reader can observe anything but old-or-new.
func replaceFile(src, dst string) error {
	return os.Rename(src, dst)
}
