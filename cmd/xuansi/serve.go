package main

// serve.go — `xuansi serve`: the daemon itself — discovery (USB by-id +
// mDNS, mac6-joined), boards.conf aliases, exclusive per-board leases
// with grace-TTL teardown, the startup orphan audit (report only), and
// the unix-socket API.
//
// Ops note (B15 minor, detached-serve hygiene): most serves are not
// started here — every client command auto-spawns a DETACHED serve
// (spawn.go's ensureDaemon) that outlives its spawner. THAT serve's
// life: stdout+stderr append to paths.StateDir()/serve.log (never the
// spawning client's stdio), it self-exits once the idle window passes
// with no API connections and no live captures (idle-exit defaults to
// the grace TTL), and SIGINT/SIGTERM walk the graceful shutdown (board
// links closed — scripts keep running; the socket removed). To manage
// one by hand: read serve.log, check the socket
// ($XDG_RUNTIME_DIR/xuansi/daemon.sock, fallback /tmp/xuansi-daemon.sock),
// and signal by pid.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"xuansi/internal/lease"
	"xuansi/internal/server"
)

func runServe(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := fs.String("sock", "", "daemon unix socket (default $XDG_RUNTIME_DIR/xuansi/daemon.sock, fallback /tmp/xuansi-daemon.sock)")
	graceTTL := fs.Duration("grace-ttl", lease.DefaultGraceTTL, "lease grace window after an agent disconnect (e.g. 300s, 10s)")
	idleExit := fs.Duration("idle-exit", -1*time.Nanosecond, "self-exit after D with no clients and no live captures (-1ns = follow --grace-ttl, 0 = never)")
	boardsFile := fs.String("boards-file", "", "boards.conf path (name=mac6 aliases; default user config dir xuansi/boards.conf, missing = zero-config)")
	verbose := fs.Bool("verbose", false, "log at debug level (slog text handler)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: xuansi serve [--sock S] [--grace-ttl D] [--idle-exit D] [--boards-file F] [--verbose]\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "xuansi serve: unexpected arguments: %v\n", fs.Args())
		fs.Usage()
		return exitUsage
	}
	setupLogging(*verbose, stderr)

	opts := server.Opts{
		GraceTTL:   *graceTTL,
		BoardsFile: *boardsFile,
		Log:        slogLogf,
	}
	// --idle-exit resolution: the -1ns default (any negative) follows
	// --grace-ttl; 0 opts out entirely; a positive value is the window.
	if *idleExit < 0 {
		opts.IdleExit = *graceTTL
	} else {
		opts.IdleExit = *idleExit
	}
	if *sock != "" {
		opts.SockPath = *sock
	}
	srv, err := server.New(opts)
	if err != nil {
		if errors.Is(err, server.ErrAlreadyListening) {
			fmt.Fprintf(stderr, "xuansi serve: another daemon is already listening — leaving (spawn race or double start)\n")
			return exitOK
		}
		fmt.Fprintf(stderr, "xuansi serve: %v\n", err)
		if isAFNXUnsupported(err) { // WSAEAFNOSUPPORT: AF_UNIX needs Windows 10 1803+ (typed, afnxcheck_windows.go)
			fmt.Fprint(stderr, "requires Windows 10 1803+ (AF_UNIX)\n")
		}
		return 1
	}
	if err := srv.Start(); err != nil {
		fmt.Fprintf(stderr, "xuansi serve: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "xuansi daemon serving %s (v%s, grace-ttl %s, Ctrl-C to stop)\n", srv.SockPath(), Version, *graceTTL)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Ctrl-C or the idle exit — whichever comes first. The idle branch
	// only reports; the graceful shutdown below is the same either way
	// (the server never Closes itself on idle).
	select {
	case <-ctx.Done():
	case <-srv.IdleExited():
		fmt.Fprintf(stdout, "xuansi: idle exit (no clients for %s)\n", opts.IdleExit)
	}

	fmt.Fprintf(stdout, "xuansi: shutting down (socket removed, board connections closed; board-side scripts keep running — the next start's audit lists them)\n")
	srv.Close()
	select {
	case <-srv.Done():
	case <-time.After(10 * time.Second):
		fmt.Fprintf(stderr, "xuansi: shutdown did not settle within 10s\n")
		return 1
	}
	fmt.Fprintf(stdout, "xuansi: shutdown complete\n")
	return 0
}
