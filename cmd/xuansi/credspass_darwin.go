//go:build darwin

package main

// credspass_darwin.go — the termios ioctl request codes (darwin).
import "golang.org/x/sys/unix"

const (
	getTermiosReq = unix.TIOCGETA
	setTermiosReq = unix.TIOCSETA
)
