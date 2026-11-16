//go:build windows

package main

// credspass_windows.go — the hidden-input read's windows half: the
// console's ENABLE_ECHO_INPUT comes off for the psk's entry (console
// mode via golang.org/x/sys/windows — the approved direct dependency).
// A non-console stdin (a pipe, tests) degrades to a plain line read.

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/windows"
) // readPassword prompts then reads one line, hiding the input when
// stdin is a console.
func readPassword(prompt string, stdout io.Writer, in *bufio.Reader) (string, error) {
	h := windows.Handle(os.Stdin.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(h, &mode); err != nil {
		// not a console: plain read, no echo to turn off
		fmt.Fprint(stdout, prompt)
		line, rerr := readTrimLine(in)
		if rerr != nil {
			return "", rerr
		}
		fmt.Fprintln(stdout)
		return line, nil
	}
	if err := windows.SetConsoleMode(h, mode&^windows.ENABLE_ECHO_INPUT); err != nil {
		return "", fmt.Errorf("console echo off: %w", err)
	}
	defer windows.SetConsoleMode(h, mode)

	fmt.Fprint(stdout, prompt)
	line, rerr := readTrimLine(in)
	fmt.Fprintln(stdout) // the hidden ENTER left no newline of its own
	if rerr != nil {
		return "", rerr
	}
	return line, nil
}
