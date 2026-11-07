package main

// status.go — `xuansi status BOARD`: one board's composed view —
// registry identity, live connection, lease state, resident scripts.

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"xuansi/internal/server"
)

func runStatus(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintf(stderr, "Usage: xuansi status BOARD [--sock S]\n\nBOARD: mac6 / alias / hostname prefix (unique)\n")
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)
	board := fs.Arg(0)

	c, err := dialDaemon(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi status: %v\n", err)
		return 1
	}
	defer c.Close()
	res, rerr, err := c.call(server.Request{Type: "board_status", Board: board}, nil)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi status: %v\n", err)
		return 1
	}
	if rerr != nil {
		return failRPC(stderr, "status", rerr)
	}

	bm, _ := res["board"].(map[string]any)
	mac6, _ := bm["mac6"].(string)
	alias, _ := bm["alias"].(string)
	name := mac6
	if alias != "" {
		name += " (" + alias + ")"
	}
	srcs := "stale (last-known)"
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
	usb, _ := bm["usb_path"].(string)
	fw, _ := bm["fw"].(string)
	fwVer, _ := bm["fw_version"].(string)
	connected := "no"
	if res["connected"] == true {
		connected = "yes"
	}
	if fw == "" {
		fw = "-"
	}
	if fwVer != "" { // B10-T8c: semver beside the build id
		fw = fwVer + " @ " + fw
	}

	fmt.Fprintf(stdout, "board:     %s\n", name)
	fmt.Fprintf(stdout, "sources:   %s\n", srcs)
	// the tcp line shows the DIALED endpoint — the mDNS-resolved IP
	// first (T10), the hostname in parens when both are known
	if host != "" || ip != "" {
		fmt.Fprintf(stdout, "tcp:       %s\n", dialEndpoint(host, ip, port))
	}
	if usb != "" {
		fmt.Fprintf(stdout, "usb:       %s\n", usb)
	}
	fmt.Fprintf(stdout, "fw:        %s\n", fw)
	fmt.Fprintf(stdout, "connected: %s\n", connected)

	if lm, ok := res["lease"].(map[string]any); ok {
		holder, _ := lm["holder"].(string)
		sess, _ := lm["session"].(float64)
		since, _ := lm["since"].(string)
		line := fmt.Sprintf("holder=%s session=%d since=%s", holder, int64(sess), since)
		if gd, _ := lm["grace_deadline"].(string); gd != "" {
			line += fmt.Sprintf(" — holder gone, grace-expiring at %s (re-supply --holder %s to adopt)", gd, holder)
		}
		fmt.Fprintf(stdout, "lease:     %s\n", line)
	} else {
		fmt.Fprintf(stdout, "lease:     (free)\n")
	}
	if se, _ := res["scripts_error"].(string); se != "" {
		fmt.Fprintf(stdout, "scripts:   (listing failed: %s)\n", se)
	} else if list, ok := res["scripts"].([]any); ok {
		names := make([]string, len(list))
		for i, s := range list {
			names[i] = fmt.Sprint(s)
		}
		if len(names) == 0 {
			fmt.Fprintf(stdout, "scripts:   (none)\n")
		} else {
			fmt.Fprintf(stdout, "scripts:   %s\n", strings.Join(names, " "))
		}
	}
	return 0
}
