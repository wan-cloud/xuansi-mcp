//go:build darwin

package main

// credspass_darwin.go — the termios ioctl request codes (darwin).
// TIOCGETA/TIOCSETA verified on a real mac (2026-11-16, macOS 26.7.3
// arm64, go1.27.4 / x/sys v0.49.0): native build clean, the hidden
// read pty-tested with echo suppression confirmed.
import "golang.org/x/sys/unix"

const (
	getTermiosReq = unix.TIOCGETA
	setTermiosReq = unix.TIOCSETA
)
