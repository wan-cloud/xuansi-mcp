package main

// boards_test.go — `xuansi boards pin|unpin` (⑨, B15 §4.9): the CLI
// half of the registry's pin source, against the in-process mock
// daemon (empty discovery — the cross-subnet shape).

import (
	"context"
	"strings"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/server"
	"xuansi/internal/testutil"
)

// startPinDaemon serves an empty-discovery daemon (no board known —
// the cross-subnet bench shape: neither face ever sees the board).
func startPinDaemon(t *testing.T) string {
	t.Helper()
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	srv, err := server.New(server.Opts{
		SockPath:        testutil.ShortSockDir(t) + "/d.sock",
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				DialTCP: func() (board.Controller, error) { return &cmdFake{}, nil },
				DialUSB: func() (board.Controller, error) { return &cmdFake{}, nil },
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
	// the empty initial sweep settles
	time.Sleep(100 * time.Millisecond)
	return srv.SockPath()
}

// TestBoardsPinUnpin: pin echoes the daemon's reply, list shows the
// pin-only entry, unpin reports the removal — and a bad endpoint or
// mac6 is a usage/daemon refusal, never a crash.
func TestBoardsPinUnpin(t *testing.T) {
	sock := startPinDaemon(t)
	var out, errb strings.Builder

	if code := run(strings.NewReader(""), &out, &errb, []string{"boards", "pin", "--sock", sock, "d75884", "10.1.11.204:7777"}); code != 0 {
		t.Fatalf("boards pin exit=%d: %q %q", code, out.String(), errb.String())
	}
	for _, want := range []string{"pinned d75884", "10.1.11.204:7777"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("boards pin output missing %q: %q", want, out.String())
		}
	}

	out.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"list", "--sock", sock}); code != 0 {
		t.Fatalf("list exit=%d: %q", code, errb.String())
	}
	if !strings.Contains(out.String(), "pin") {
		t.Fatalf("list output lacks the pin source: %q", out.String())
	}

	out.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"boards", "unpin", "--sock", sock, "d75884"}); code != 0 {
		t.Fatalf("boards unpin exit=%d: %q %q", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "unpinned d75884") {
		t.Fatalf("boards unpin output: %q", out.String())
	}

	// argument shapes: pin needs mac6 + ip:port; a malformed endpoint
	// is a usage refusal client-side
	out.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"boards", "pin", "--sock", sock, "d75884"}); code != exitUsage {
		t.Fatalf("pin without endpoint exit=%d, want exitUsage", code)
	}
	out.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"boards", "pin", "--sock", sock, "d75884", "10.1.11.204"}); code != exitUsage {
		t.Fatalf("pin without port exit=%d, want exitUsage", code)
	}
	out.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"boards", "unpin", "--sock", sock}); code != exitUsage {
		t.Fatalf("unpin without mac6 exit=%d, want exitUsage", code)
	}

	// a non-mac6 id crosses to the daemon and surfaces its server-class
	// refusal with a non-zero exit
	out.Reset()
	errb.Reset()
	if code := run(strings.NewReader(""), &out, &errb, []string{"boards", "pin", "--sock", sock, "notmac", "10.1.11.204:7777"}); code != 1 {
		t.Fatalf("pin bad mac6 exit=%d, want 1", code)
	}
	if !strings.Contains(errb.String(), "mac6") {
		t.Fatalf("pin bad mac6 stderr: %q", errb.String())
	}
}
