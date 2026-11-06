//go:build !windows

package server

import (
	"fmt"
	"net"
	"os"
)

// sockclaim_unix.go — the bind-temp + hard-link atomic socket claim is
// supported here (linux, darwin): a unix socket IS its inode, so a
// hard link to a bound socket's file is another live name for the same
// listener — the claim protocol in bindSock builds on that (verified:
// linux in this repo's tests, darwin on the T9 machine).
const linkClaimSupported = true

// bindSockPlatform (unix): the stale-check prelude — a live socket is
// the graceful already-listening refusal; a dead socket or a
// non-socket file at the path is cleared — then the atomic claim
// (bindClaim, which itself falls back to bindDirect on link-incapable
// filesystems). Byte-for-byte the pre-seam bindSock behavior.
func bindSockPlatform(path string) (net.Listener, error) {
	if fi, err := os.Stat(path); err == nil {
		if fi.Mode()&os.ModeSocket != 0 {
			if c, derr := net.Dial("unix", path); derr == nil {
				c.Close()
				return nil, fmt.Errorf("%w: %s", ErrAlreadyListening, path)
			}
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("server: remove stale sock %s: %w", path, err)
			}
		} else {
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("server: clear non-socket path %s: %w", path, err)
			}
		}
	}
	return bindClaim(path)
}
