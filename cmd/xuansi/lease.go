package main

// lease.go — `xuansi lease BOARD [--holder H] [--usb]` and `xuansi
// release BOARD [--holder H]`. The holder string is the lease identity
// across invocations: re-supplying the same holder within the grace
// TTL adopts the lease back without touching the board (see
// internal/server/proto.go's holder contract). Without --holder the
// daemon binds a generated per-connection id — printed, so it can be
// re-supplied. --usb is the DELIBERATE fallback route (T10 ruling):
// it reboots the board at open — never implied, never automatic.

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"xuansi/internal/server"
)

func runLease(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("lease", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := clientFlags(fs)
	holder := fs.String("holder", "", "lease holder identity (re-supply the same value to adopt within the grace TTL)")
	usb := fs.Bool("usb", false, "deliberate USB fallback route: opening the port REBOOTS the board — running scripts are killed (last resort per T10 ruling)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintf(stderr, "Usage: xuansi lease BOARD [--holder H] [--usb] [--sock S]\n\nBOARD: mac6 / alias / hostname prefix (unique)\n--usb:  deliberate USB fallback (reboots the board; TCP stays the default)\n")
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)

	c, err := dialDaemon(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi lease: %v\n", err)
		return 1
	}
	defer c.Close()
	req := server.Request{Type: "lease", Board: fs.Arg(0), Holder: *holder}
	if *usb {
		req.Transport = "usb"
	}
	res, rerr, err := c.call(req, nil)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi lease: %v\n", err)
		return 1
	}
	if rerr != nil {
		return failRPC(stderr, "lease", rerr)
	}
	sess, _ := res["session"].(float64)
	h, _ := res["holder"].(string)
	if *usb {
		fmt.Fprintf(stdout, "leased %s holder=%s session=%d transport=usb\n", fs.Arg(0), h, int64(sess))
		// the daemon's reply carries the same string (result.warning);
		// print it regardless so the --usb caller cannot miss it
		fmt.Fprintf(stdout, "WARNING: %s\n", server.USBRebootWarning)
	} else {
		fmt.Fprintf(stdout, "leased %s holder=%s session=%d\n", fs.Arg(0), h, int64(sess))
	}
	if *holder == "" {
		fmt.Fprintf(stdout, "(holder generated — re-supply `--holder %s` on later commands to keep/adopt this lease)\n", h)
	}
	return 0
}

func runRelease(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("release", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := clientFlags(fs)
	holder := fs.String("holder", "", "lease holder identity (must match the leasing holder)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fmt.Fprintf(stderr, "Usage: xuansi release BOARD [--holder H] [--sock S]\n\nBOARD: mac6 / alias / hostname prefix (unique)\n")
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)

	c, err := dialDaemon(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi release: %v\n", err)
		return 1
	}
	defer c.Close()
	res, rerr, err := c.call(server.Request{Type: "release", Board: fs.Arg(0), Holder: *holder}, nil)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi release: %v\n", err)
		return 1
	}
	if rerr != nil {
		return failRPC(stderr, "release", rerr)
	}
	h, _ := res["holder"].(string)
	fmt.Fprintf(stdout, "released %s (holder=%s; board session reset to safe state)\n", fs.Arg(0), h)
	return 0
}
