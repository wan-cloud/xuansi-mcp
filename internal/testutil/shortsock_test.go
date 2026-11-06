package testutil

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestShortSockDirBindsLongNames pins the property the T9 mac finding
// broke: a unix socket BINDS under the returned dir even with a
// generous file name, on every host — and the dir is cleaned up. The
// budget check is the platform's own (sunPathBudget: 104 darwin / 107
// linux / 108 windows — the flat 104 rejected 105-byte windows paths
// that bind fine there, first CI matrix run finding).
func TestShortSockDirBindsLongNames(t *testing.T) {
	dir := ShortSockDir(t)
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("ShortSockDir %q is not a usable dir: %v", dir, err)
	}
	sock := filepath.Join(dir, "a-deliberately-long-socket-name-0123456789.sock")
	if budget := sunPathBudget(); len(sock) > budget {
		t.Fatalf("sock path %q is %d bytes, exceeds the %d-byte sun_path budget on %s", sock, len(sock), budget, runtime.GOOS)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("bind under ShortSockDir: %v", err)
	}
	ln.Close()
}

// TestShortSockDirReservesNameAllowance: whatever the host temp root,
// the returned dir leaves the promised nameAllowance bytes for a
// socket name inside the platform budget — the contract the risky-base
// fallback exists to keep (darwin's /var/folders roots are exactly the
// host that must fall back to /tmp).
func TestShortSockDirReservesNameAllowance(t *testing.T) {
	dir := ShortSockDir(t)
	name := strings.Repeat("n", nameAllowance-len(".sock")) + ".sock" // exactly nameAllowance bytes
	long := filepath.Join(dir, name)
	if budget := sunPathBudget(); len(long) > budget {
		t.Fatalf("dir %q + a %d-byte name = %d bytes, exceeds the %d-byte budget on %s (fallback did not fire?)",
			dir, len(name), len(long), budget, runtime.GOOS)
	}
}

// TestShortSockDirUniquePerCall: two calls give two dirs (parallel
// tests must not share a socket dir).
func TestShortSockDirUniquePerCall(t *testing.T) {
	if ShortSockDir(t) == ShortSockDir(t) {
		t.Fatal("two ShortSockDir calls returned the same dir")
	}
}
