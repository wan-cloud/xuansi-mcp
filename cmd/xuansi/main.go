// Command xuansi is the host-side daemon CLI of the xuansi hardware probe
// (懸絲): it discovers boards (USB by-id + mDNS, joined by mac6), holds
// exclusive per-board leases, and serves the internal unix-socket API that
// agents talk to. The subcommand surface is fixed; this skeleton build only
// dispatches — real logic lands task by task per the plan in README.md.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

const (
	exitOK    = 0
	exitStub  = 1 // known command, not implemented in this skeleton build
	exitUsage = 2 // bad invocation: no args, unknown command, bad flags
)

type command struct {
	name    string
	summary string
	// run executes the command with the args after the command name and
	// returns the process exit code.
	run func(stdout, stderr io.Writer, args []string) int
}

// commands is the subcommand table, in usage order. The internal/ layout
// mirrors the layering replclient → discover → lease → server (see each
// package's doc comment).
var commands = []command{
	{"serve", "run the daemon: discovery, leasing, unix-socket API", stub("serve")},
	{"list", "list boards known to the daemon (USB + mDNS, mac6-joined)", stub("list")},
	{"status", "show one board's live status", stub("status")},
	{"lease", "take the exclusive lease on one board", stub("lease")},
	{"release", "release a board lease", stub("release")},
	{"eval", "evaluate one expression on a board and print the result", stub("eval")},
	{"repl", "open an interactive REPL session with a board", stub("repl")},
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches argv (everything after the program name) and returns the
// process exit code. Writers are parameters so tests can capture output.
func run(argv []string, stdout, stderr io.Writer) int {
	if len(argv) == 0 {
		fmt.Fprint(stderr, usageText())
		return exitUsage
	}
	switch argv[0] {
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stdout, usageText())
		return exitOK
	}
	for _, c := range commands {
		if c.name == argv[0] {
			return c.run(stdout, stderr, argv[1:])
		}
	}
	fmt.Fprintf(stderr, "xuansi: unknown command %q\n\n", argv[0])
	fmt.Fprint(stderr, usageText())
	return exitUsage
}

// usageText renders the top-level help. The commands table is the single
// source of truth; the unit test asserts every subcommand is listed.
func usageText() string {
	var b strings.Builder
	b.WriteString(`xuansi — host-side daemon of the xuansi hardware probe (懸絲)

Usage:

  xuansi <command> [flags]

Commands:

`)
	for _, c := range commands {
		fmt.Fprintf(&b, "  %-8s %s\n", c.name, c.summary)
	}
	b.WriteString(`
Flags:

  Every command accepts --verbose after the command name (slog debug
  logging; default is info level, text handler on stderr). -h/--help
  shows the top-level or per-command usage.

See README.md for build, layout and dependency policy.
`)
	return b.String()
}

// stub returns the placeholder runner shared by every command in this
// skeleton build: it applies the repo-wide flag and logging conventions,
// then reports the command as not yet implemented, with its usage.
func stub(name string) func(stdout, stderr io.Writer, args []string) int {
	return func(stdout, stderr io.Writer, args []string) int {
		fs := flag.NewFlagSet(name, flag.ContinueOnError)
		fs.SetOutput(stdout)
		verbose := fs.Bool("verbose", false, "log at debug level (slog text handler)")
		fs.Usage = func() {
			fmt.Fprintf(fs.Output(), "Usage: xuansi %s [flags]\n", name)
			fs.PrintDefaults()
		}
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return exitOK
			}
			return exitUsage
		}
		setupLogging(*verbose, stderr)
		fmt.Fprintf(stdout, "xuansi %s: stub — implementation lands in a later task.\n\n", name)
		fs.Usage()
		return exitStub
	}
}

// setupLogging installs the repo-wide logging convention: slog text
// handler on stderr, info level by default, debug with --verbose.
func setupLogging(verbose bool, w io.Writer) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})))
}
