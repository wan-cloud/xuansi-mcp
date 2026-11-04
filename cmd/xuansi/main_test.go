package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestUsageListsAllSubcommands: bare invocation prints usage to stderr,
// exits non-zero, and names every registered subcommand.
func TestUsageListsAllSubcommands(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != exitUsage {
		t.Fatalf("run(nil) exit = %d, want %d", code, exitUsage)
	}
	if stdout.Len() != 0 {
		t.Errorf("bare invocation wrote to stdout, want usage on stderr only")
	}
	for _, c := range commands {
		if !strings.Contains(stderr.String(), c.name) {
			t.Errorf("usage text missing subcommand %q", c.name)
		}
	}
}

// TestHelpExitsZero: explicit help goes to stdout with exit code 0.
func TestHelpExitsZero(t *testing.T) {
	for _, arg := range []string{"-h", "-help", "--help", "help"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{arg}, &stdout, &stderr); code != exitOK {
			t.Errorf("run(%q) exit = %d, want %d", arg, code, exitOK)
		}
		if !strings.Contains(stdout.String(), "serve") {
			t.Errorf("run(%q): help on stdout missing subcommands", arg)
		}
	}
}

// TestUnknownCommandFailsWithUsage: unknown commands exit non-zero and say so.
func TestUnknownCommandFailsWithUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"nope"}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("run([nope]) exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), `unknown command "nope"`) {
		t.Errorf("stderr does not name the unknown command: %q", stderr.String())
	}
}

// TestStubCommandsReportStub: every registered command dispatches and
// reports itself as a stub with its usage, exiting non-zero. This pins
// the dispatch table until real implementations replace the stubs.
func TestStubCommandsReportStub(t *testing.T) {
	for _, c := range commands {
		var stdout, stderr bytes.Buffer
		if code := c.run(&stdout, &stderr, nil); code != exitStub {
			t.Errorf("%s: exit = %d, want %d", c.name, code, exitStub)
		}
		if !strings.Contains(stdout.String(), "Usage: xuansi "+c.name) {
			t.Errorf("%s: usage not printed", c.name)
		}
		if !strings.Contains(stdout.String(), "stub") {
			t.Errorf("%s: does not identify itself as a stub", c.name)
		}
	}
}
