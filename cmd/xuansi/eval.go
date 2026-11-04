package main

// eval.go — `xuansi eval BOARD CODE [--holder H]`: one synchronous eval
// through the daemon's warm controller connection, printing the
// captured stdout and, when the board bridged a return value, its JSON
// form (encoding/json marshal of the decoded value).

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"xuansi/internal/server"
)

func runEval(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("eval", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := clientFlags(fs)
	holder := fs.String("holder", "", "lease holder identity (eval requires the lease; re-supplies adopt within grace)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 2 {
		fmt.Fprintf(stderr, "Usage: xuansi eval BOARD CODE [--holder H] [--sock S]\n\nBOARD: mac6 / alias / hostname prefix (unique)\nCODE:  a Lua chunk (return something to see the value face)\n")
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)

	c, err := dialDaemon(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi eval: %v\n", err)
		return 1
	}
	defer c.Close()
	res, rerr, err := c.call(server.Request{Type: "eval", Board: fs.Arg(0), Holder: *holder, Code: fs.Arg(1)}, nil)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi eval: %v\n", err)
		return 1
	}
	if rerr != nil {
		return failRPC(stderr, "eval", rerr)
	}
	printEvalFace(stdout, res)
	return 0
}

// printEvalFace renders one eval result: out text, then the value's
// JSON form when the board bridged one.
func printEvalFace(w io.Writer, res map[string]any) {
	if out, _ := res["out"].(string); out != "" {
		fmt.Fprint(w, out)
		if n := len(out); n == 0 || out[n-1] != '\n' {
			fmt.Fprintln(w)
		}
	}
	if v, ok := res["value"]; ok && v != nil {
		b, err := json.Marshal(v)
		if err != nil {
			fmt.Fprintf(w, "value: <%v>\n", v)
			return
		}
		fmt.Fprintf(w, "value: %s\n", b)
	}
}
