package testutil

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

// TestShortSockDirBindsLongNames pins the property the T9 mac finding
// broke: a unix socket BINDS under the returned dir even with a
// generous file name, on every host — and the dir is cleaned up.
func TestShortSockDirBindsLongNames(t *testing.T) {
	dir := ShortSockDir(t)
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("ShortSockDir %q is not a usable dir: %v", dir, err)
	}
	sock := filepath.Join(dir, "a-deliberately-long-socket-name-0123456789.sock")
	if len(sock) > sunPathMax {
		t.Fatalf("sock path %q is %d bytes, exceeds the %d-byte sun_path budget", sock, len(sock), sunPathMax)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("bind under ShortSockDir: %v", err)
	}
	ln.Close()
}

// TestShortSockDirUniquePerCall: two calls give two dirs (parallel
// tests must not share a socket dir).
func TestShortSockDirUniquePerCall(t *testing.T) {
	if ShortSockDir(t) == ShortSockDir(t) {
		t.Fatal("two ShortSockDir calls returned the same dir")
	}
}
