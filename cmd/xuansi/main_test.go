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
	if code := run(strings.NewReader(""), &stdout, &stderr, nil); code != exitUsage {
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
		if code := run(strings.NewReader(""), &stdout, &stderr, []string{arg}); code != exitOK {
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
	if code := run(strings.NewReader(""), &stdout, &stderr, []string{"nope"}); code != exitUsage {
		t.Fatalf("run([nope]) exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), `unknown command "nope"`) {
		t.Errorf("stderr does not name the unknown command: %q", stderr.String())
	}
}

// TestClientCommandsNeedTheirPositionals: every daemon-client command
// (all but serve) rejects a bare invocation as a usage error on stderr —
// the dispatch table stays honest now that the stubs are gone.
func TestClientCommandsNeedTheirPositionals(t *testing.T) {
	for _, c := range commands {
		if c.name == "serve" || c.name == "list" {
			continue // no positional args required
		}
		var stdin, stdout, stderr bytes.Buffer
		if code := c.run(&stdin, &stdout, &stderr, []string{"--sock", "/nonexistent/x.sock"}); code != exitUsage {
			t.Errorf("%s: bare exit = %d, want %d (stderr %q)", c.name, code, exitUsage, stderr.String())
		}
		if stderr.Len() == 0 {
			t.Errorf("%s: usage not on stderr", c.name)
		}
	}
}

// TestPerCommandHelpExitsZero: -h on each subcommand prints its usage.
func TestPerCommandHelpExitsZero(t *testing.T) {
	for _, c := range commands {
		var stdin, stdout, stderr bytes.Buffer
		if code := c.run(&stdin, &stdout, &stderr, []string{"-h"}); code != exitOK {
			t.Errorf("%s -h: exit = %d, want %d (stderr %q)", c.name, code, exitOK, stderr.String())
		}
		if stdout.Len() == 0 && stderr.Len() == 0 {
			t.Errorf("%s -h: no usage printed", c.name)
		}
	}
}
