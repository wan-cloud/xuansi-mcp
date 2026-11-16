//go:build darwin

package main

// credspass_darwin.go — the termios ioctl request codes (darwin).
// TIOCGETA/TIOCSETA pending verification on the first real mac build
// (the cgo serial face is unreachable from this Linux host — noted,
// not fixable here).
import "golang.org/x/sys/unix"

const (
	getTermiosReq = unix.TIOCGETA
	setTermiosReq = unix.TIOCSETA
)
