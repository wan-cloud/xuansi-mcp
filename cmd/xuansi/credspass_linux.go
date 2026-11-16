//go:build linux

package main

// credspass_linux.go — the termios ioctl request codes (linux).
import "golang.org/x/sys/unix"

const (
	getTermiosReq = unix.TCGETS
	setTermiosReq = unix.TCSETS
)
