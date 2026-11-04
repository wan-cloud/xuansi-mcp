package main

// serve.go — `xuansi serve`: the daemon itself — discovery (USB by-id +
// mDNS, mac6-joined), boards.conf aliases, exclusive per-board leases
// with grace-TTL teardown, the startup orphan audit (report only), and
// the unix-socket API.

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
	boardsFile := fs.String("boards-file", "", "boards.conf path (name=mac6 aliases; default user config dir xuansi/boards.conf, missing = zero-config)")
	verbose := fs.Bool("verbose", false, "log at debug level (slog text handler)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: xuansi serve [--sock S] [--grace-ttl D] [--boards-file F] [--verbose]\n")
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
	if *sock != "" {
		opts.SockPath = *sock
	}
	srv, err := server.New(opts)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi serve: %v\n", err)
		return 1
	}
	if err := srv.Start(); err != nil {
		fmt.Fprintf(stderr, "xuansi serve: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "xuansi daemon serving %s (grace-ttl %s, Ctrl-C to stop)\n", srv.SockPath(), *graceTTL)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

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
