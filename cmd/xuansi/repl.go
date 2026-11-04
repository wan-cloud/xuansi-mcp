package main

// repl.go — `xuansi repl BOARD [--holder H]`: the line-by-line debug
// face. Each stdin line is one eval (statement or `return ...` chunk);
// out and the bridged value print per line; Ctrl-D (EOF) ends the
// session.
//
// Lease lifecycle: repl takes (or adopts) the lease under its holder
// first and does NOT release on exit — an exited repl is an agent-gone
// (grace) like any other disconnect, and the board state survives for
// re-entry or the TTL teardown. The exit note names the release
// command. Ctrl-C kills the CLI outright (same grace path).

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"

	"xuansi/internal/server"
)

func runRepl(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("repl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := clientFlags(fs)
	holder := fs.String("holder", "", "lease holder identity (re-supply an existing holder to adopt its lease)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintf(stderr, "Usage: xuansi repl BOARD [--holder H] [--sock S]\n\nBOARD: mac6 / alias / hostname prefix (unique)\nOne line = one eval; Ctrl-D exits.\n")
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)
	boardSel := fs.Arg(0)

	c, err := dialDaemon(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi repl: %v\n", err)
		return 1
	}
	defer c.Close()

	// take (or adopt) the lease first: every line is a mutating eval
	res, rerr, err := c.call(server.Request{Type: "lease", Board: boardSel, Holder: *holder}, nil)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi repl: %v\n", err)
		return 1
	}
	if rerr != nil {
		return failRPC(stderr, "repl", rerr)
	}
	bmac, _ := res["board"].(string)
	h, _ := res["holder"].(string)
	sess, _ := res["session"].(float64)
	fmt.Fprintf(stdout, "xuansi repl — board %s (holder=%s session=%d); one line = one eval, Ctrl-D exits\n", bmac, h, int64(sess))

	sc := bufio.NewScanner(stdin)
	sc.Buffer(make([]byte, 64*1024), 64*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		res, rerr, err := c.call(server.Request{Type: "eval", Board: bmac, Code: line}, nil)
		switch {
		case err != nil:
			fmt.Fprintf(stderr, "xuansi repl: connection lost: %v\n", err)
			return 1
		case rerr != nil:
			fmt.Fprintf(stdout, "! %s\n", rerr.Error())
		default:
			printEvalFace(stdout, res)
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(stderr, "xuansi repl: stdin: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "EOF — lease left in grace (holder=%s); release deliberately with: xuansi release %s --holder %s\n", h, bmac, h)
	return 0
}
