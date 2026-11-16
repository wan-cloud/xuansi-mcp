package main

// creds.go — `xuansi creds` (憑證零儀式波 T2): the OWNER-side credential
// vault CLI — set (interactive, the psk entered hidden), import (the
// existing xscreds host-file format, fw tools/xscreds.py's line
// grammar), status (presence only). This is the one sanctioned way
// WiFi credentials enter the host (owner ruling 2026-11-16: the CLI,
// never an agent channel — redaction 鐵則); the device PSK is not an
// input at all: the daemon generates it on first provisioning and the
// owner provides nothing beyond WiFi.
//
// The subcommands are LOCAL: they read/write the vault dir directly
// (default paths.StateDir(), the daemon's own landing spot) and never
// dial the daemon — the vault is host state, and `creds` works with
// the daemon down. Output is masked, never material (the tests' vector).

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"xuansi/internal/creds"
	"xuansi/internal/paths"
)

func runCreds(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, "Usage: xuansi creds <set|import|status> [--dir D]  (flags BEFORE any file arg)\n\n  set     enter the WiFi credentials interactively (psk hidden)\n  import  read them from an xscreds-format file: creds import [--dir D] <file>\n  status  show what the vault holds — never the material\n\n--dir overrides the vault directory (default: the daemon state dir,\ne.g. ~/.local/state/xuansi). Credentials never cross the agent channel;\nthe device PSK is daemon-generated — nothing to enter for it.\n")
		return exitUsage
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		fmt.Fprintf(stdout, "Usage: xuansi creds <set|import|status> [--dir D]  (flags BEFORE any file arg)\n\n  set     enter the WiFi credentials interactively (psk hidden)\n  import  read them from an xscreds-format file: creds import [--dir D] <file>\n  status  show what the vault holds — never the material\n\n--dir overrides the vault directory (default: the daemon state dir,\ne.g. ~/.local/state/xuansi). Credentials never cross the agent channel;\nthe device PSK is daemon-generated — nothing to enter for it.\n")
		return exitOK
	case "set":
		return runCredsSet(stdin, stdout, stderr, args[1:])
	case "import":
		return runCredsImport(stdout, stderr, args[1:])
	case "status":
		return runCredsStatus(stdout, stderr, args[1:])
	}
	fmt.Fprintf(stderr, "xuansi creds: unknown subcommand %q (want set, import or status)\n", args[0])
	return exitUsage
}

// readTrimLine reads one trimmed line from the buffered reader (the
// trailing read error rides along with any partial line — the caller
// decides emptiness).
func readTrimLine(in *bufio.Reader) (string, error) {
	line, err := in.ReadString('\n')
	return strings.TrimSpace(line), err
}

// credsFlags installs the shared --dir flag (the vault's landing spot).
func credsFlags(fs *flag.FlagSet) *string {
	return fs.String("dir", "", "vault directory (default: the daemon state dir, e.g. ~/.local/state/xuansi)")
}

// vaultDir resolves the flag's answer, defaulting to the daemon's own
// landing spot — the same dir the daemon's provisioner reads LIVE.
func vaultDir(override string) string {
	if override != "" {
		return override
	}
	return paths.StateDir()
}

func runCredsSet(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("creds set", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: xuansi creds set [--dir D]\n\nInteractive: prompts for the WiFi ssid (shown) and psk (hidden input).\n")
	}
	dirFlag := credsFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return exitUsage
	}
	dir := vaultDir(*dirFlag)

	in := bufio.NewReader(stdin)
	fmt.Fprint(stdout, "WiFi SSID: ")
	ssid, err := in.ReadString('\n')
	if err != nil && ssid == "" {
		fmt.Fprintf(stderr, "xuansi creds set: reading ssid: %v\n", err)
		return 1
	}
	ssid = strings.TrimSpace(ssid)
	if ssid == "" {
		fmt.Fprintf(stderr, "xuansi creds set: an empty ssid clears nothing here — import or set needs a real network name\n")
		return exitUsage
	}
	psk, err := readPassword("WiFi PSK: ", stdout, in)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi creds set: reading psk: %v\n", err)
		return 1
	}
	if psk == "" {
		fmt.Fprintf(stderr, "xuansi creds set: the psk cannot be empty\n")
		return exitUsage
	}
	if err := creds.SaveWiFi(dir, ssid, psk); err != nil {
		fmt.Fprintf(stderr, "xuansi creds set: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "stored in %s — wifi ssid %q, psk %s (never printed; the daemon provisions cred-less boards automatically)\n",
		dir, ssid, creds.Mask(psk))
	return exitOK
}

func runCredsImport(stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("creds import", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: xuansi creds import [--dir D] <file>\n\n<file>: the xscreds format (SSID: name / 密碼|密码|Password|PASS|pass: secret lines).\n")
	}
	dirFlag := credsFlags(fs)
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
	path := fs.Arg(0)
	dir := vaultDir(*dirFlag)

	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi creds import: %v\n", err)
		return 1
	}
	ssid, psk, err := creds.ParseHostCreds(string(b))
	if err != nil {
		fmt.Fprintf(stderr, "xuansi creds import: %s: %v\n", path, err)
		return 1
	}
	if err := creds.SaveWiFi(dir, ssid, psk); err != nil {
		fmt.Fprintf(stderr, "xuansi creds import: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "imported from %s into %s — wifi ssid %q, psk %s (never printed; the daemon provisions cred-less boards automatically)\n",
		path, dir, ssid, creds.Mask(psk))
	return exitOK
}

func runCredsStatus(stdout, stderr io.Writer, args []string) int {
	fs := flag.NewFlagSet("creds status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: xuansi creds status [--dir D]\n\nShows what the vault holds — presence only, never the material.\n")
	}
	dirFlag := credsFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return exitUsage
	}
	dir := vaultDir(*dirFlag)
	v, err := creds.Load(dir)
	if err != nil {
		fmt.Fprintf(stderr, "xuansi creds status: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, creds.StatusText(dir, v))
	return exitOK
}
