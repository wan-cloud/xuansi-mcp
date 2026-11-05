package main

// mcp.go — `xuansi mcp`: the MCP (Model Context Protocol) face of the
// daemon, batch 6 stage C. This is the SKELETON: an official-SDK MCP
// server over stdio with ZERO tools for now — later batch-6 tasks wire
// the internal API (via internal/apiclient) into tools one face at a
// time. The daemon connection is dialed EAGERLY, before the stdio
// loop, so a missing daemon fails fast with the actionable message
// instead of half-starting behind an MCP handshake; the SDK's
// StdioTransport binds the process's real stdin/stdout, and MCP traffic
// rides stdout while logs stay on stderr (slog, like every command).
//
// Daemon death mid-session does not exit the loop: with zero tools
// nothing dials on demand yet, and once tools exist their Calls fail
// fast (apiclient.ErrDead) — surfacing daemon loss is the tools'
// behavior to define, not the skeleton's.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/apiclient"
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
	if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(stderr, "xuansi mcp: %v\n", err)
		return 1
	}
	return exitOK
}
