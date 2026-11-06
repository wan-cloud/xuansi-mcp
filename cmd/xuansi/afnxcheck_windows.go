//go:build windows

package main

// afnxcheck_windows.go — the typed AF_UNIX-support classification for
// `xuansi serve`'s bind failure (final-review minor: the check used to
// substring-match "10047" in the error text). x/sys/windows carries the
// REAL Winsock errno — unlike stdlib syscall's invented
// APPLICATION_ERROR-offset constants real Winsock calls never return
// (see internal/server's wsaeaddrinuse note) — so errors.Is matches
// the exact value net.Listen reports on a pre-1803 Windows.

import (
	"errors"

	"golang.org/x/sys/windows"
)

// isAFNXUnsupported reports whether err is WSAEAFNOSUPPORT (10047):
// this Windows build cannot speak AF_UNIX at all — the socket address
// family needs Windows 10 1803+.
func isAFNXUnsupported(err error) bool {
	return errors.Is(err, windows.WSAEAFNOSUPPORT)
}
