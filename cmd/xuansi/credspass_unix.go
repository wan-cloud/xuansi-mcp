//go:build !windows

package main

// credspass_unix.go — the hidden-input read's unix half. On a terminal
// the psk is entered with ECHO off (termios via golang.org/x/sys — the
// approved direct dependency; x/term would need a policy ruling); on a
// pipe (scripts, tests) the read degrades to a plain line — the echo
// concern is moot off a terminal, and this is the hermetic seam.

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// readPassword prompts then reads one line, hiding the input when
// stdin is a terminal.
func readPassword(prompt string, stdout io.Writer, in *bufio.Reader) (string, error) {
	fd := int(os.Stdin.Fd())
	old, err := unix.IoctlGetTermios(fd, getTermiosReq)
	if err != nil {
		// not a terminal (a pipe or a file): plain read, no echo to turn off
		fmt.Fprint(stdout, prompt)
		line, rerr := readTrimLine(in)
		if rerr != nil {
			return "", rerr
		}
		fmt.Fprintln(stdout)
		return line, nil
	}
	hushed := *old
	hushed.Lflag &^= unix.ECHO
	if err := unix.IoctlSetTermios(fd, setTermiosReq, &hushed); err != nil {
		return "", fmt.Errorf("terminal echo off: %w", err)
	}
	defer unix.IoctlSetTermios(fd, setTermiosReq, old)

	fmt.Fprint(stdout, prompt)
	line, rerr := readTrimLine(in)
	fmt.Fprintln(stdout) // the hidden ENTER left no newline of its own
	if rerr != nil {
		return "", rerr
	}
	return line, nil
}
