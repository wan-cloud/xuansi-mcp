// Package testutil holds cross-package test helpers.
package testutil

import (
	"os"
	"runtime"
	"testing"
)

// sunPathBudget is this host's unix-socket path limit in bytes: the
// sockaddr_un sun_path array minus whatever the platform needs for the
// terminator. Per platform (revised after the first CI matrix run
// 42158718976, where the old flat 104-byte check was wrong in both
// directions: windows paths of 105 bytes were rejected although
// windows' AF_UNIX budget is 108, and darwin never fell back to /tmp
// because the risky-base threshold was tuned for the flat 104):
//
//	darwin:  104  (the smallest sun_path of the three)
//	linux:   107  (108-byte array minus the NUL)
//	windows: 108  (AF_UNIX sun_path, per Microsoft's afunix.h)
func sunPathBudget() int {
	switch runtime.GOOS {
	case "darwin":
		return 104
	case "windows":
		return 108
	default:
		return 107
	}
}

// nameAllowance is the socket file-name bytes ShortSockDir guarantees
// INSIDE the platform budget after its own directory. The deliberately
// long T9 probe name (46 bytes) fits with margin; real daemon/client
// socket names are far shorter.
const nameAllowance = 48

// dirCost is the most a ShortSockDir directory itself adds under the
// base: the "xuansi-t9-" prefix plus os.MkdirTemp's ≤10-digit random
// suffix, and the two path separators around them.
const dirCost = 20 + 2

// riskyBaseLen is the base-path length beyond which the host temp dir
// cannot host a ShortSockDir that still leaves nameAllowance for the
// socket name: budget − dirCost − nameAllowance. That threshold puts
// darwin's /var/folders/<x>/<y>/T (~49 bytes — the T9 mac finding,
// re-bitten by the first CI matrix run when the threshold was 80, far
// too lax for long socket names) onto the /tmp fallback, keeps linux's
// /tmp (4 bytes) as-is, and keeps the windows CI %TEMP%
// (C:\Users\RUNNER~1\AppData\Local\Temp, 38 bytes) exactly at the
// line: 38+1+20+1+46 = 106 ≤ 108.
func riskyBaseLen() int { return sunPathBudget() - dirCost - nameAllowance }

// shortRoot is the fallback base when the host temp dir itself is
// risky: /tmp is short everywhere we support (on darwin it symlinks
// to /private/tmp; the kernel resolves the link, and sun_path checks
// the string we pass, which stays short). Unix-shaped by design: the
// windows CI %TEMP% (38 bytes) fits its 108 budget with a full
// nameAllowance to spare, so windows never falls back. A hypothetical
// longer-%TEMP% windows host would retry into %TEMP% (today's
// behavior, unchanged) rather than risk creating roots on a drive it
// cannot write.
const shortRoot = "/tmp"

// ShortSockDir returns a per-test directory whose full paths stay
// inside every host's unix-socket sun_path budget: tests join their
// socket file names onto it (keep names short — the helper reserves
// nameAllowance bytes for them). On a short-temp host (linux /tmp,
// the windows CI %TEMP%) it is a plain MkdirTemp under os.TempDir() —
// behavior and location unchanged; on a long-temp host (darwin's
// /var/folders) it drops to the short /tmp root so bind/dial cannot
// hit EINVAL. Removed at test cleanup.
func ShortSockDir(t testing.TB) string {
	t.Helper()
	base := os.TempDir()
	if len(base) > riskyBaseLen() {
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
