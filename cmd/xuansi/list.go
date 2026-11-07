package main

// list.go — `xuansi list`: every board the daemon knows (alias?/mac6/
// sources/host/fw as plain aligned text). The HOST column shows the
// endpoint the daemon DIALS — the mDNS-resolved IP with the hostname
// in parens (T10) — not the hostname alone.

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"xuansi/internal/server"
)

// dialEndpoint renders the endpoint the daemon DIALS for one board
// (final-review minor: T10 made the browse-resolved IP the dialed
// address, so the display must show it — not just the hostname).
// The IP carries the port first, the hostname rides in parens; an
// IP-less board falls back to the hostname:port; nothing known is "-".
func dialEndpoint(host, ip string, port float64) string {
	addr := ip
	if addr == "" {
		addr = host
	}
	if addr == "" {
		return "-"
	}
	ep := fmt.Sprintf("%s:%d", addr, int64(port))
	if ip != "" && host != "" && ip != host {
		ep += " (" + host + ")"
	}
	return ep
}

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
		ip, _ := bm["ip"].(string)
		port, _ := bm["port"].(float64)
		hp := dialEndpoint(host, ip, port)
		fw, _ := bm["fw"].(string)
		fwVer, _ := bm["fw_version"].(string)
		if fw == "" {
			fw = "-"
		}
		if fwVer != "" { // B10-T8c: semver beside the build id
			fw = fwVer + "@" + fw
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
