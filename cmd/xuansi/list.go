package main

// list.go — `xuansi list`: every board the daemon knows (alias?/mac6/
// sources/host/fw as plain aligned text).

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"xuansi/internal/server"
)

func runList(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "xuansi list: unexpected arguments: %v\n", fs.Args())
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)

	c, err := dialDaemon(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi list: %v\n", err)
		return 1
	}
	defer c.Close()
	res, rerr, err := c.call(server.Request{Type: "list_boards"}, nil)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi list: %v\n", err)
		return 1
	}
	if rerr != nil {
		return failRPC(stderr, "list", rerr)
	}

	tw := tabwriter.NewWriter(stdout, 2, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "ALIAS\tMAC6\tSOURCES\tHOST\tFW")
	boards, _ := res["boards"].([]any)
	for _, b := range boards {
		bm, _ := b.(map[string]any)
		alias, _ := bm["alias"].(string)
		if alias == "" {
			alias = "-"
		}
		mac6, _ := bm["mac6"].(string)
		srcs := "stale"
		if list, ok := bm["sources"].([]any); ok && len(list) > 0 {
			parts := make([]string, len(list))
			for i, s := range list {
				parts[i] = fmt.Sprint(s)
			}
			srcs = strings.Join(parts, "+")
		}
		host, _ := bm["host"].(string)
		port, _ := bm["port"].(float64)
		hp := "-"
		if host != "" {
			hp = fmt.Sprintf("%s:%d", host, int64(port))
		}
		fw, _ := bm["fw"].(string)
		if fw == "" {
			fw = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", alias, mac6, srcs, hp, fw)
	}
	tw.Flush()
	if mdnsErr, _ := res["mdns_error"].(string); mdnsErr != "" {
		fmt.Fprintf(stderr, "note: mDNS face degraded (last-known kept): %s\n", mdnsErr)
	}
	if len(boards) == 0 {
		fmt.Fprintln(stderr, "no boards known yet — the daemon refreshes discovery every 30s")
	}
	return 0
}
