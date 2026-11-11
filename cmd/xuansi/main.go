// Command xuansi is the host-side daemon CLI of the xuansi hardware probe
// (懸絲): it discovers boards (USB by-id + mDNS, joined by mac6), holds
// exclusive per-board leases, and serves the internal unix-socket API that
// agents talk to. `xuansi serve` IS the daemon; every other subcommand is a
// client of it over the socket (contract: internal/server/proto.go).
package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

const (
	exitOK    = 0
	exitError = 1 // runtime failure: daemon down, board error, refused lease
	exitUsage = 2 // bad invocation: no args, unknown command, bad flags
)

type command struct {
	name    string
	summary string
	// run executes the command with the args after the command name and
	// returns the process exit code.
	run func(stdin io.Reader, stdout, stderr io.Writer, args []string) int
}

// commands is the subcommand table, in usage order. The internal/ layout
// mirrors the layering replclient → discover → lease → server (see each
// package's doc comment).
var commands = []command{
	{"serve", "run the daemon: discovery, leasing, unix-socket API", runServe},
	{"list", "list boards known to the daemon (USB + mDNS, mac6-joined)", runList},
	{"status", "show one board's live status", runStatus},
	{"lease", "take the exclusive lease on one board", runLease},
	{"release", "release a board lease", runRelease},
	{"eval", "evaluate one expression on a board and print the result", runEval},
	{"repl", "open a line-by-line REPL session with a board", runRepl},
	{"mcp", "serve the MCP face over stdio (agent API; zero tools yet)", runMCP},
}

func main() {
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr, os.Args[1:]))
}

// run dispatches argv (everything after the program name) and returns the
// process exit code. Writers are parameters so tests can capture output.
func run(stdin io.Reader, stdout, stderr io.Writer, argv []string) int {
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
			return c.run(stdin, stdout, stderr, argv[1:])
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
The daemon socket defaults to $XDG_RUNTIME_DIR/xuansi/daemon.sock
(fallback /tmp/xuansi-daemon.sock); --sock overrides it everywhere.

BOARD selectors are mac6 / alias (boards.conf) / hostname prefixes,
resolved by the daemon; an ambiguous or unknown selector lists the
candidate boards.

Leases are holder-scoped: ` + "`xuansi lease B --holder H`" + ` binds the
identity H; later commands re-supplying the same --holder adopt the
lease back within the grace TTL after a disconnect.

USB is a route the daemon picks honestly: a default lease stays on TCP
whenever a TCP-capable source is live, and auto-falls back to USB —
REBOOTING the board, running scripts killed — only when USB is the one
live source (the lease reply always discloses the route, the reboot,
and the note; ` + "`xuansi lease B --usb`" + ` forces it). Ops never
silently re-route: whatever controller is warm carries them.

Flags:

  Every command accepts --verbose after the command name (slog debug
  logging; default is info level, text handler on stderr). -h/--help
  shows the top-level or per-command usage.

See README.md for build, layout and dependency policy.
`)
	return b.String()
}

// fsVerbose reports whether --verbose was seen by the shared client
// flag prelude (clientFlags); set just before setupLogging runs.
var fsVerbose bool

// clientFlags installs the flags every daemon-client subcommand takes:
// --sock and --verbose.
func clientFlags(fs *flag.FlagSet) *string {
	sock := fs.String("sock", "", "daemon unix socket (default $XDG_RUNTIME_DIR/xuansi/daemon.sock, fallback /tmp/xuansi-daemon.sock)")
	fs.BoolVar(&fsVerbose, "verbose", false, "log at debug level (slog text handler)")
	return sock
}

// slogLogf adapts the daemon's printf-style trace seam onto slog.
func slogLogf(format string, args ...any) {
	slog.Info(fmt.Sprintf(format, args...))
}

// setupLogging installs the repo-wide logging convention: slog text
// handler on stderr, info level by default, debug with --verbose.
func setupLogging(verbose bool, w io.Writer) {
	fsVerbose = verbose
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})))
}
