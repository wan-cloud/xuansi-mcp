package server

// keepalive_test.go — B16 F3-②: last_seen must measure the BOARD's
// reachability, not the AGENT's activity. In run3 the seat idled
// through a ~46s wifi scan and last_seen went stale for 113s — with no
// op traffic the daemon crossed nothing, and on USB CDC (where the
// board's own probes cannot fail a detached link) a silently dead
// controller could sit warm forever. The keepalive loop services warm
// controllers on a path independent of client ops: a bounded ping
// while the link is idle keeps last_seen fresh, a TIMEOUT (the board
// busy behind a long op — a scan — never a verdict) leaves the link
// alone (§4.2: timeout ≠ dead), and a DEAD verdict closes the corpse
// so the next op redials per §4.4 — no release→re-lease ritual (③).

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/replclient"
	"xuansi/internal/testutil"
)

// boardStatusOf fetches board_status and returns (board view, connected).
func boardStatusOf(t *testing.T, c *sockClient, id int64) (map[string]any, bool) {
	t.Helper()
	rep, _ := c.call(id, map[string]any{"type": "board_status", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("board_status: %#v", rep)
	}
	res := asMap(rep["result"])
	return asMap(res["board"]), res["connected"] == true
}

// TestKeepaliveKeepsLastSeenFreshThroughIdleAndScan pins the interaction:
// idle freshness (crossings with no client ops), timeout-tolerance (a
// scan-busy ping never tears the link down), and the dead-verdict
// teardown + no-release recovery.
func TestKeepaliveKeepsLastSeenFreshThroughIdleAndScan(t *testing.T) {
	const interval = 20 * time.Millisecond
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			return []discover.Board{{
				MAC6: "d75884", Sources: []string{discover.SourceUSB},
				USBPath: "/dev/serial/by-id/fake-d75884",
			}}, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	var usbDials atomic.Int32
	var warm *fakeCtl
	sock := filepath.Join(testutil.ShortSockDir(t), "k.sock")
	srv, err := New(Opts{
		SockPath:          sock,
		Registry:          reg,
		GraceTTL:          10 * time.Second,
		RefreshInterval:   -1,
		KeepaliveInterval: interval,
		Log:               newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
				DialTCP: func() (board.Controller, error) {
					return newFakeCtl(), nil
				},
				DialUSB: func() (board.Controller, error) {
					n := usbDials.Add(1)
					f := newFakeCtl()
					if n == 1 {
						warm = f
					}
					return f, nil
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	awaitSweep(t, reg, 1)
	c := dialSock(t, sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "h1", "transport": "usb"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("usb lease: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	if usbDials.Load() != 1 {
		t.Fatalf("usb dials = %d, want 1", usbDials.Load())
	}

	// (a) idle freshness: with NO client traffic for ~5 intervals,
	// last_seen stays inside the keepalive tolerance — the loop's own
	// crossings carry it. Without the loop the age would grow past the
	// whole idle window.
	time.Sleep(5 * interval)
	st, connected := boardStatusOf(t, c, 3)
	if !connected {
		t.Fatal("connected = false over a live idle link")
	}
	if got := asInt(st["last_seen_age_ms"]); got > 60 {
		t.Fatalf("last_seen_age_ms = %d after an idle window with no client ops, want <60ms (keepalive-serviced)", got)
	}

	// (b) scan-busy tolerance: the ping times out (the board wedged
	// behind a long op — the scan shape). §4.2: a timeout is NOT a
	// death verdict; the link must stay up and the next answered ping
	// resumes the freshness.
	warm.failOnce("ping", fmt.Errorf("%w: board busy scanning", replclient.ErrOpTimeout))
	time.Sleep(3 * interval)
	st, connected = boardStatusOf(t, c, 4)
	if !connected {
		t.Fatal("a ping timeout tore the link down — §4.2 says timeout ≠ dead")
	}
	// the busy ping did not count as a crossing; the next one did
	time.Sleep(3 * interval)
	st, _ = boardStatusOf(t, c, 5)
	if got := asInt(st["last_seen_age_ms"]); got > 60 {
		t.Fatalf("last_seen_age_ms = %d after the busy window cleared, want fresh again", got)
	}

	// (c) dead verdict: the link dies under the keepalive's ping — the
	// loop closes the corpse promptly (the 113s blind spot), and the
	// NEXT OP redials per §4.4 without any release (③'s folklore gone).
	warm.dieOn("ping", nil)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, conn := boardStatusOf(t, c, 6); !conn {
			break
		}
		time.Sleep(interval)
	}
	if _, conn := boardStatusOf(t, c, 7); conn {
		t.Fatal("the dead controller was still connected after the keepalive window")
	}
	rep, _ = c.call(8, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval after the keepalive teardown: the redial must save the op WITHOUT a release — %#v", rep)
	}
	if got := usbDials.Load(); got != 2 {
		t.Fatalf("usb dials = %d, want 2 (the no-release redial)", got)
	}
}
