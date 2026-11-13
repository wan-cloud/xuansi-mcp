package main

// cli_test.go — the CLI subcommands against an in-process daemon (the
// real server package over a fake board controller). Flag handling,
// output rendering, holder plumbing and exit codes are pinned here; the
// live-board smoke is results/'s job.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/replclient"
	"xuansi/internal/server"
	"xuansi/internal/testutil"
)

// cmdFake is the minimal board controller the CLI tests need (the
// server package's own fake is test-scoped there; this one stays local
// to keep the two test suites independent).
type cmdFake struct {
	mu     sync.Mutex
	calls  []string
	hellos int
}

func (f *cmdFake) Op(ctx context.Context, op string, args map[string]any) (map[string]any, error) {
	f.mu.Lock()
	f.calls = append(f.calls, op)
	f.mu.Unlock()
	switch op {
	case "hello":
		return map[string]any{"protocol_ver": int64(5), "role": "controller", "fw_build": "deadbee"}, nil
	case "eval":
		if code, _ := args["code"].(string); code == "return {ok=1}" {
			return map[string]any{"out": "hello\n", "value": map[string]any{"ok": int64(1)}}, nil
		}
		// deterministic bridge for the repl test's first line — its
		// "2" assertion previously passed only by accident, matching
		// the epoch-based session id in the banner (pre-existing flake)
		if code, _ := args["code"].(string); code == "return 1+1" {
			return map[string]any{"value": int64(2)}, nil
		}
		return map[string]any{"out": "ok"}, nil
	case "script.list":
		return map[string]any{"out": "scripts (0):"}, nil
	case "diag.pins":
		return map[string]any{"out": "pins (0):"}, nil
	case "wifi.status":
		return map[string]any{"state": "off"}, nil
	case "events.subscribe":
		return map[string]any{"source": args["source"]}, nil
	case "session.reset":
		return map[string]any{"stopped": int64(0)}, nil
	default:
		return map[string]any{}, nil
	}
}

func (f *cmdFake) Hello(observer bool, name string) (*replclient.HelloInfo, error) {
	f.mu.Lock()
	f.hellos++
	f.mu.Unlock()
	return &replclient.HelloInfo{ProtocolVer: 5, Role: "controller", FwBuild: "deadbee"}, nil
}
func (f *cmdFake) Dead() bool                                         { return false }
func (f *cmdFake) DeadReason() string                                 { return "" }
func (f *cmdFake) Close() error                                       { return nil }
func (f *cmdFake) OnEvent(p string, fn func(replclient.Event)) func() { return func() {} }
func (f *cmdFake) OnStream(sink replclient.StreamSink)                {}

// FsStreamWrite satisfies the v8 upload-lane face (B16-T7 C2);
// unscripted refusal — the cmd tests do not exercise the lane.
func (f *cmdFake) FsStreamWrite(ctx context.Context, path string, data []byte, sha256hex string) error {
	return &replclient.OpError{Op: "fs.stream_write", Class: "api",
		Msg: "upload lane not scripted (cmd tests)"}
}

// startMockDaemon serves one fake board on a temp sock; returns the path.
func startMockDaemon(t *testing.T) string {
	sock, _ := startMockDaemonUSB(t)
	return sock
}

// startMockDaemonUSB is startMockDaemon that also wires the deliberate
// USB dial seam (lease --usb) and returns the USB-side fake so tests
// can prove the route reached it.
func startMockDaemonUSB(t *testing.T) (string, *cmdFake) {
	t.Helper()
	fake := &cmdFake{}
	usbFake := &cmdFake{}
	boards := []discover.Board{{
		MAC6: "d75884", Sources: []string{discover.SourceUSB, discover.SourceMDNS},
		USBPath: "/dev/serial/by-id/fake-d75884",
		Host:    "xuansi-d75884.local", IP: "10.1.51.205", Port: 7777,
	}}
	reg := discover.NewRegistry(map[string]string{"d75884": "bench"}, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) { return boards, nil }),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return boards, nil }),
	)
	srv, err := server.New(server.Opts{
		SockPath:        testutil.ShortSockDir(t) + "/d.sock",
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				DialTCP: func() (board.Controller, error) { return fake, nil },
				DialUSB: func() (board.Controller, error) { return usbFake, nil },
			})
		},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	// let the startup audit settle so list shows the fw column
	time.Sleep(100 * time.Millisecond)
	return srv.SockPath(), usbFake
}

// TestListPrintsColumns: list renders the documented columns —
// alias?/mac6/sources/host/fw — as plain aligned text (fw appears once
// the startup audit's hello enrichment lands; polled). The HOST column
// shows the DIALED endpoint: the mDNS-resolved IP with the hostname in
// parens (T10 — the daemon dials the IP; final-review minor).
func TestListPrintsColumns(t *testing.T) {
	sock := startMockDaemon(t)
	var out, errb strings.Builder
	var got string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		out.Reset()
		errb.Reset()
		if code := run(strings.NewReader(""), &out, &errb, []string{"list", "--sock", sock}); code != 0 {
			t.Fatalf("list exit=%d stdout=%q stderr=%q", code, out.String(), errb.String())
		}
		got = out.String()
		if strings.Contains(got, "deadbee") {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, want := range []string{"bench", "d75884", "usb+mdns", "10.1.51.205:7777 (xuansi-d75884.local)", "deadbee"} {
		if !strings.Contains(got, want) {
			t.Fatalf("list output missing %q: %q", want, got)
		}
	}
}

// TestDialEndpoint pins the HOST-column renderer: IP-first (the T10
// dialed address) with the hostname in parens, hostname-only fallback,
// and "-" when neither is known.
func TestDialEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		host, ip string
		port     float64
		want     string
	}{
		{"ip and host", "xuansi-d75884.local", "10.1.51.205", 7777, "10.1.51.205:7777 (xuansi-d75884.local)"},
		{"host only", "xuansi-d75884.local", "", 7777, "xuansi-d75884.local:7777"},
		{"ip only", "", "10.1.51.205", 7777, "10.1.51.205:7777"},
		{"neither", "", "", 7777, "-"},
	}
	for _, c := range cases {
		if got := dialEndpoint(c.host, c.ip, c.port); got != c.want {
			t.Errorf("%s: dialEndpoint(%q,%q,%d) = %q, want %q", c.name, c.host, c.ip, int64(c.port), got, c.want)
		}
	}
}

// TestEvalPrintsOutAndValue: eval prints the captured out and, when the
// board bridged a value, its JSON form.
func TestEvalPrintsOutAndValue(t *testing.T) {
	sock := startMockDaemon(t)
	var out, errb strings.Builder
	if code := run(strings.NewReader(""), &out, &errb, []string{"lease", "--sock", sock, "--holder", "cli-t", "d75884"}); code != 0 {
		t.Fatalf("lease exit=%d: %q %q", code, out.String(), errb.String())
	}
	out.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"eval", "--sock", sock, "--holder", "cli-t", "d75884", "return {ok=1}"}); code != 0 {
		t.Fatalf("eval exit=%d: %q %q", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "hello") {
		t.Fatalf("eval output missing out: %q", out.String())
	}
	if !strings.Contains(out.String(), `"ok":1`) {
		t.Fatalf("eval output missing value face: %q", out.String())
	}
}

// TestEvalWithoutLeaseFails: the exclusive-lease rule surfaces as a
// class not_held error, exit 1, stderr.
func TestEvalWithoutLeaseFails(t *testing.T) {
	sock := startMockDaemon(t)
	var out, errb strings.Builder
	code := run(strings.NewReader(""), &out, &errb, []string{"eval", "--sock", sock, "d75884", "return 1"})
	if code != 1 {
		t.Fatalf("eval exit=%d, want 1", code)
	}
	if !strings.Contains(errb.String(), "not_held") {
		t.Fatalf("stderr missing not_held: %q", errb.String())
	}
}

// TestMissingArgsIsUsageError: a missing positional is a flag error —
// usage on stderr, exit 2 (the T6 review pattern).
func TestMissingArgsIsUsageError(t *testing.T) {
	sock := startMockDaemon(t)
	var out, errb strings.Builder
	code := run(strings.NewReader(""), &out, &errb, []string{"eval", "--sock", sock})
	if code != exitUsage {
		t.Fatalf("eval exit=%d, want %d", code, exitUsage)
	}
	if errb.Len() == 0 {
		t.Fatalf("usage not on stderr")
	}
	if out.Len() != 0 {
		t.Fatalf("stdout not empty: %q", out.String())
	}
}

// TestReplFeedsLinesAndExitsOnEOF: repl is the line-by-line debug face:
// each stdin line is one eval; EOF (Ctrl-D) ends the session without
// releasing (the lease follows the standard grace lifecycle).
func TestReplFeedsLinesAndExitsOnEOF(t *testing.T) {
	sock := startMockDaemon(t)
	var out, errb strings.Builder
	in := "return 1+1\nprint('hi')\n"
	code := run(strings.NewReader(in), &out, &errb, []string{"repl", "--sock", sock, "--holder", "repl-t", "d75884"})
	if code != 0 {
		t.Fatalf("repl exit=%d stdout=%q stderr=%q", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "2") {
		t.Fatalf("repl output missing first eval value: %q", out.String())
	}
	if !strings.Contains(out.String(), "ok") {
		t.Fatalf("repl output missing second eval out: %q", out.String())
	}
}

// TestLeaseReleaseRoundtrip: release by the same holder succeeds.
func TestLeaseReleaseRoundtrip(t *testing.T) {
	sock := startMockDaemon(t)
	var out, errb strings.Builder
	if code := run(strings.NewReader(""), &out, &errb, []string{"lease", "--sock", sock, "--holder", "r1", "d75884"}); code != 0 {
		t.Fatalf("lease exit=%d: %q", code, errb.String())
	}
	out.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"release", "--sock", sock, "--holder", "r1", "d75884"}); code != 0 {
		t.Fatalf("release exit=%d: %q %q", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "released") {
		t.Fatalf("release output: %q", out.String())
	}
}

// TestLeaseUSBPrintsRebootWarning: `lease --usb` (I-1) rides the
// deliberate USB route — the daemon's DialUSB seam is reached, the
// success output carries transport=usb plus the plain reboot warning —
// while a plain lease (same holder, no --usb) never mentions USB.
func TestLeaseUSBPrintsRebootWarning(t *testing.T) {
	sock, usbFake := startMockDaemonUSB(t)
	var out, errb strings.Builder

	// plain lease first: default TCP route, no USB anywhere
	if code := run(strings.NewReader(""), &out, &errb, []string{"lease", "--sock", sock, "--holder", "u1", "d75884"}); code != 0 {
		t.Fatalf("plain lease exit=%d: %q %q", code, out.String(), errb.String())
	}
	if strings.Contains(out.String(), "WARNING") || strings.Contains(out.String(), "transport=usb") {
		t.Fatalf("plain lease output mentions USB: %q", out.String())
	}
	usbFake.mu.Lock()
	usbHellos := usbFake.hellos
	usbFake.mu.Unlock()
	if usbHellos != 0 {
		t.Fatalf("plain lease reached the usb seam (hellos=%d)", usbHellos)
	}

	// then the deliberate route (same holder: re-lease is idempotent)
	out.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"lease", "--sock", sock, "--holder", "u1", "--usb", "d75884"}); code != 0 {
		t.Fatalf("lease --usb exit=%d: %q %q", code, out.String(), errb.String())
	}
	got := out.String()
	for _, want := range []string{"transport=usb", "WARNING:", "REBOOTS the board", "one connection = one device session", "running scripts were killed"} {
		if !strings.Contains(got, want) {
			t.Fatalf("lease --usb output missing %q: %q", want, got)
		}
	}
	// the deliberate route reached the USB controller (ConnectUSB +
	// the server tap both hello on it; the USB fake sees no ops —
	// a lease touches no board op)
	usbFake.mu.Lock()
	hellos := usbFake.hellos
	ops := len(usbFake.calls)
	usbFake.mu.Unlock()
	if hellos == 0 {
		t.Fatalf("usb controller hellos = 0, want ≥1 from ConnectUSB (ops=%d)", ops)
	}
}

// TestStatusPrintsBlock: status shows the composed view, the tcp line
// carrying the DIALED endpoint — the mDNS-resolved IP first, the
// hostname in parens (T10; final-review minor).
func TestStatusPrintsBlock(t *testing.T) {
	sock := startMockDaemon(t)
	var out, errb strings.Builder
	if code := run(strings.NewReader(""), &out, &errb, []string{"status", "--sock", sock, "d75884"}); code != 0 {
		t.Fatalf("status exit=%d: %q %q", code, out.String(), errb.String())
	}
	for _, want := range []string{"d75884", "bench", "connected", "tcp:       10.1.51.205:7777 (xuansi-d75884.local)"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("status output missing %q: %q", want, out.String())
		}
	}
}

// TestDaemonDownIsCleanError: no daemon → one clear stderr line, exit 1.
func TestDaemonDownIsCleanError(t *testing.T) {
	// no-op the auto-spawn seam (preflight ruling): this test pins the
	// PLAIN dial failure; with the real ensureDaemon it would spawn a
	// serve and burn the retry window first.
	orig := ensureBeforeDial
	ensureBeforeDial = func(string) error { return nil }
	t.Cleanup(func() { ensureBeforeDial = orig })

	var out, errb strings.Builder
	code := run(strings.NewReader(""), &out, &errb, []string{"list", "--sock", t.TempDir() + "/missing.sock"})
	if code != 1 {
		t.Fatalf("exit=%d, want 1", code)
	}
	if !strings.Contains(errb.String(), "daemon") {
		t.Fatalf("stderr does not mention the daemon: %q", errb.String())
	}
}

// TestServeAlreadyListeningExitsZero: a second serve against a live
// daemon's socket is the spawn-race loser (or a deliberate double
// start) — one clear line, exit 0, harmless.
func TestServeAlreadyListeningExitsZero(t *testing.T) {
	sock := startMockDaemon(t)
	var out, errb strings.Builder
	code := run(strings.NewReader(""), &out, &errb, []string{"serve", "--sock", sock})
	if code != exitOK {
		t.Fatalf("serve exit=%d, want %d (stdout %q stderr %q)", code, exitOK, out.String(), errb.String())
	}
	if !strings.Contains(errb.String(), "xuansi serve: another daemon is already listening — leaving") {
		t.Fatalf("stderr missing the leaving line: %q", errb.String())
	}
}
