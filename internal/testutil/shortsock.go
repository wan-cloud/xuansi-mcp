// Package testutil holds cross-package test helpers.
package testutil

import (
	"os"
	"testing"
)

// sunPathMax is darwin's unix-socket path limit (sun_path bytes);
// linux allows 108, windows is path-keyed and effectively unbounded —
// the smallest governs portable tests.
const sunPathMax = 104

// riskyBaseLen: when the host temp dir's path is longer than this, a
// t.TempDir() inside it (test-name-derived subdir + numeric sequence)
// plus a socket file name can overrun sunPathMax — darwin's
// /var/folders/<x>/<y>/T/ is ~52 bytes and long test names overflow
// exactly there (the T9 mac finding: bind EINVAL or helper-bind
// failures surfacing as dial timeouts).
const riskyBaseLen = 80

// shortRoot is the fallback base when the host temp dir itself is
// risky: /tmp is short everywhere we support (on darwin it symlinks
// to /private/tmp; the kernel resolves the link, and sun_path checks
// the string we pass, which stays short).
const shortRoot = "/tmp"

// ShortSockDir returns a per-test directory whose full paths stay
// inside every host's unix-socket sun_path budget: tests join their
// socket file names onto it (keep names short). On a short-temp host
// (linux /tmp, windows) it is a plain MkdirTemp under os.TempDir() —
// behavior and location unchanged; on a long-temp host (darwin's
// /var/folders) it drops to the short /tmp root so bind/dial cannot
// hit EINVAL. Removed at test cleanup.
func ShortSockDir(t testing.TB) string {
	t.Helper()
	base := os.TempDir()
	if len(base) > riskyBaseLen {
		base = shortRoot
	}
	dir, err := os.MkdirTemp(base, "xuansi-t9-")
	if err != nil {
		// The risky-base fallback must not turn into a test-killer on
		// hosts without /tmp; one retry inside the host temp dir.
		if dir, err = os.MkdirTemp("", "xuansi-t9-"); err != nil {
			t.Fatalf("testutil: short sock dir: %v", err)
		}
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
