package main

// boards.go — `xuansi boards pin|unpin` (⑨, B15 §4.9): the CLI half
// of the registry's pin source — declaring a board's TCP endpoint for
// boards this host's discovery cannot route (cross-subnet mDNS
// blindness). The endpoint parses through net.SplitHostPort, so a
// bracketed IPv6 literal (`[fe80::1]:7777`) works.

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"

	"xuansi/internal/server"
)

func runBoards(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, "Usage: xuansi boards <pin|unpin> [flags] ...\n\n  pin   MAC6 IP:PORT   pin a board's TCP endpoint (registry pin source)\n  unpin MAC6           remove the pin\n")
		return exitUsage
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		fmt.Fprintf(stdout, "Usage: xuansi boards <pin|unpin> [flags] ...\n\n  pin   MAC6 IP:PORT   pin a board's TCP endpoint (registry pin source)\n  unpin MAC6           remove the pin\n\n`xuansi boards pin -h` shows the pin subcommand's own usage.\n")
		return exitOK
	case "pin":
		return runBoardsPin(stdin, stdout, stderr, args[1:])
	case "unpin":
		return runBoardsUnpin(stdin, stdout, stderr, args[1:])
	}
	fmt.Fprintf(stderr, "xuansi boards: unknown subcommand %q (want pin or unpin)\n", args[0])
	return exitUsage
}

func runBoardsPin(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("boards pin", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: xuansi boards pin MAC6 IP:PORT [--sock S]\n\nMAC6:     exact board id (6 hex chars, e.g. d75884)\nIP:PORT:  the pinned TCP endpoint (the board's REPL listener, e.g. 10.1.11.204:7777)\n")
	}
	sock := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return exitUsage
	}
	mac6 := fs.Arg(0)
	host, portStr, err := net.SplitHostPort(fs.Arg(1))
	if err != nil || host == "" {
		fmt.Fprintf(stderr, "xuansi boards pin: endpoint %q is not ip:port (want e.g. 10.1.11.204:7777)\n", fs.Arg(1))
		return exitUsage
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		fmt.Fprintf(stderr, "xuansi boards pin: port %q out of range 1..65535\n", portStr)
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)

	c, err := dialDaemon(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi boards pin: %v\n", err)
		return 1
	}
	defer c.Close()
	res, rerr, err := c.call(server.Request{Type: "boards_pin", Mac6: mac6, IP: host, Port: port}, nil)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi boards pin: %v\n", err)
		return 1
	}
	if rerr != nil {
		return failRPC(stderr, "boards pin", rerr)
	}
	pinned, _ := res["pinned"].(string)
	endpoint, _ := res["endpoint"].(string)
	fmt.Fprintf(stdout, "pinned %s → %s (registry pin source; dial priority usb > pin > mdns; in-memory — boards.conf's name=mac6@ip:port line is the durable seed)\n", pinned, endpoint)
	return 0
}

func runBoardsUnpin(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("boards unpin", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: xuansi boards unpin MAC6 [--sock S]\n\nMAC6: the pinned board's exact id\n")
	}
	sock := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return exitUsage
	}
	setupLogging(fsVerbose, stderr)

	c, err := dialDaemon(sockFrom(*sock))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi boards unpin: %v\n", err)
		return 1
	}
	defer c.Close()
	res, rerr, err := c.call(server.Request{Type: "boards_unpin", Mac6: fs.Arg(0)}, nil)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi boards unpin: %v\n", err)
		return 1
	}
	if rerr != nil {
		return failRPC(stderr, "boards unpin", rerr)
	}
	unpinned, _ := res["unpinned"].(string)
	fmt.Fprintf(stdout, "unpinned %s (pin removed; the board keeps its other live sources, a pin-only board becomes stale last-known)\n", unpinned)
	return 0
}
