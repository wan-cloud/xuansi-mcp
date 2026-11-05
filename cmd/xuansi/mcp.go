package main

// mcp.go — `xuansi mcp`: the MCP (Model Context Protocol) face of the
// daemon, batch 6 stage C. An official-SDK MCP server over stdio whose
// tools are the daemon's internal API (via internal/apiclient),
// registered by internal/mcp — the static lease/eval/script/fs/op set
// now, with the events/websocket/capture faces to follow. The daemon
// connection is dialed EAGERLY, before the stdio loop, so a missing
// daemon fails fast with the actionable message instead of
// half-starting behind an MCP handshake; the SDK's StdioTransport
// binds the process's real stdin/stdout, and MCP traffic rides stdout
// while logs stay on stderr (slog, like every command).
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

	srv := mcp.NewServer(&mcp.Implementation{Name: "xuansi", Version: daemonVersion}, nil)
	xumcp.RegisterTools(srv, c, xumcp.Options{})
	if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(stderr, "xuansi mcp: %v\n", err)
		return 1
	}
	return exitOK
}
