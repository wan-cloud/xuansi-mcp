//go:build !windows

package discover

import "os"

// replaceBoardsFileOS renames src over dst (writeBoardsFile's atomic
// publish). On unix this is a plain rename: replacing an open file is
// legal, and no reader can observe anything but old-or-new.
func replaceBoardsFileOS(src, dst string) error {
	return os.Rename(src, dst)
}

// fsyncBoardsDirOS flushes the directory's entries (the rename
// writeBoardsFile just performed) so the new boards.conf cannot
// vanish after a crash, leaving the file's old-or-new guarantee whole.
func fsyncBoardsDirOS(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
