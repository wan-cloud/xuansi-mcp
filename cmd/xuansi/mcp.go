package main

// mcp.go — `xuansi mcp`: the MCP (Model Context Protocol) face of the
// daemon, batch 6 stage C. An official-SDK MCP server over stdio whose
// tools are the daemon's internal API (via internal/apiclient),
// registered by internal/mcp — the static lease/eval/script/fs/op set
// plus the dynamic per-board tools expanded from each board's caps
// (refreshed every internal/mcp.DynRefreshDefault; boards arriving or
// leaving grow or shrink the toolset with tools/list_changed
// notifications), plus the event face: per-board
// xuansi://boards/<mac6>/events resources (subscribable, snapshot =
// the per-source event rings) and the xuansi_events_recent polling
// tool. The daemon connection is dialed EAGERLY, before the stdio
// loop, so a missing daemon fails fast with the actionable message
// instead of half-starting behind an MCP handshake; the SDK's
// StdioTransport binds the process's real stdin/stdout, and MCP
// traffic rides stdout while logs stay on stderr (slog, like every
// command).
//
// Daemon death mid-session does not exit the loop: every tool call
// rides the one connection, and once it is gone the calls fail fast
// (apiclient.ErrDead), surfacing per-call as tool errors that name the
// lost connection (internal/mcp's mapping) — the agent sees why, the
// session stays alive for stdout-less clients that can't respawn us.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/apiclient"
	xumcp "xuansi/internal/mcp"
)

// daemonVersion is the version the MCP server advertises. The daemon
// has no version scheme yet; "dev" until one exists.
const daemonVersion = "dev"

func runMCP(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "xuansi mcp: unexpected arguments: %v\n", fs.Args())
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)

	c, err := apiclient.Dial(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi mcp: %v\n", err)
		return 1
	}
	defer c.Close()

	// NewServer (not the bare SDK constructor): arms the resource-
	// subscription handlers the event face's per-board resources need
	// (internal/mcp.NewServer documents the wiring).
	srv := xumcp.NewServer(&mcp.Implementation{Name: "xuansi", Version: daemonVersion})
	stopTools := xumcp.RegisterTools(srv, c, xumcp.Options{
		Refresh: xumcp.DynRefreshDefault,
		// the refresh loop's operational notes (e.g. alias truncation,
		// B8 item 4) ride the same slog-on-stderr seam as every other
		// log line — stdout stays pure MCP traffic.
		Logf: func(format string, args ...any) { slog.Warn(fmt.Sprintf(format, args...)) },
	})
	defer stopTools()
	if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(stderr, "xuansi mcp: %v\n", err)
		return 1
	}
	return exitOK
}
