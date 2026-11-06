//go:build windows

package mcp

// fsyncDir (windows): a deliberate no-op. Windows has no POSIX
// directory fsync: os.Open gives a read-only handle and Sync
// (FlushFileBuffers) needs FILE_WRITE_DATA on it — the exact
// "Access is denied" the first CI matrix run (42158718976) hit on
// every workspace durability test. The crash-durability contract the
// unix dir-fsync provides (the rename entry itself surviving power
// loss) is carried differently on NTFS: metadata changes are journaled
// by the filesystem, and the file's own fsync in writeDurableFile
// (which DOES work on Windows — a writable handle) persists the data
// before the rename. Skipping the dir-fsync is the standard portable
// pattern (sqlite/glibc fall back the same way); what we keep is the
// whole-file atomicity half — a reader never sees a torn write on any
// platform.
func fsyncDir(dir string) error { return nil }
