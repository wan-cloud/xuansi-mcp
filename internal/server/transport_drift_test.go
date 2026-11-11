package server

// transport_drift_test.go — fix round 1, Important-1: the USB→TCP
// upgrade that completes via supervisor()'s ENDPOINT-DRIFT REBUILD
// (outside any lease) must emit the board.transport event too — the
// daemon-held route fact switches usb→tcp when the rebuild closes the
// live USB link and re-arms the TCP seams, and the next default lease
// cannot report it (the fresh supervisor reads Transport()==""). The
// event is the ONLY seat-visible trace of that switch.

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/testutil"
)

// TestDriftRebuildUpgradeEmitsTransportEvent: a board whose live
// controller is the USB route (ctlUSB) drifts — its registry entry
// changes endpoint with a TCP-capable source — and the next
// supervisor() acquisition rebuilds the seams: the closed route (usb)
// and the new resolution (tcp) cross as ONE board.transport event
// {from:"usb", to:"tcp", rebooted:false} to watchers, alongside the
// §12 yield-close of the USB link. A rebuild that does NOT leave the
// USB route (tcp→tcp drift, or a usb→none drop) fires nothing.
func TestDriftRebuildUpgradeEmitsTransportEvent(t *testing.T) {
	var mu sync.Mutex
	// v1: WiFi-dead shape — usb source + endpoint fields last-known
	// (mdns source gone, so tcpDialable is false in v1... carried in
	// below via the view's Sources)
	view := discover.Board{
		MAC6: "d75884", Sources: []string{discover.SourceUSB, discover.SourceMDNS},
		USBPath: "/dev/serial/by-id/fake-d75884", Host: "xuansi-d75884.local", IP: "10.1.51.205", Port: 7777,
	}
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			mu.Lock()
			defer mu.Unlock()
			return []discover.Board{view}, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			mu.Lock()
			defer mu.Unlock()
			return []discover.Board{view}, nil
		}),
	)
	var usbFake *fakeCtl
	sock := filepath.Join(testutil.ShortSockDir(t), "d.sock")
	srv, err := New(Opts{
		SockPath:        sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		Log:             newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
				DialTCP: func() (board.Controller, error) {
					return newFakeCtl(), nil
				},
				DialUSB: func() (board.Controller, error) {
					f := newFakeCtl()
					usbFake = f
					return f, nil
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// watcher first (its warm-controller leg rides TCP; the USB
	// controller below is installed straight through the supervisor,
	// so the ONLY transport event the watcher can see is the rebuild's)
	c := dialSock(t, sock)
	rep, _ := c.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}

	// v1 acquisition + the USB route in hand (the §4.4 fallback's
	// aftermath, driven straight through the supervisor like the
	// drift test's Connect — the emission under test is the
	// supervisor()'s, not the lease's)
	b1, rerr := reg.Resolve("d75884")
	if rerr != nil {
		t.Fatalf("Resolve: %v", rerr)
	}
	db1 := srv.supervisor(b1)
	db1.connectMu.Lock()
	uctl, uerr := db1.sup.ConnectUSB(context.Background())
	db1.connectMu.Unlock()
	if uerr != nil {
		t.Fatalf("ConnectUSB: %v", uerr)
	}
	if got := db1.sup.Transport(); got != "usb" {
		t.Fatalf("v1 route = %q, want usb before the drift", got)
	}
	_ = uctl

	// drift: the board re-associated on TCP (endpoint moves .205→.206).
	// The rebuild closes the live USB link (yield) and re-arms the TCP
	// seams — the daemon-held route switches usb→tcp.
	mu.Lock()
	view.IP = "10.1.51.206"
	mu.Unlock()
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh (drift): %v", err)
	}
	b2, rerr := reg.Resolve("d75884")
	if rerr != nil {
		t.Fatalf("Resolve (drift): %v", rerr)
	}
	srv.supervisor(b2)

	// the ONE transport event: usb → tcp, no reboot (§13)
	d := transportEventOf(t, c.drainEvents(t))
	if asStr(d["from"]) != "usb" || asStr(d["to"]) != "tcp" || d["rebooted"] != false {
		t.Fatalf("transport event = %#v, want usb → tcp rebooted false (the drift-rebuild switch)", d)
	}
	if !sawObserverYield(usbFake) {
		t.Fatal("the drift rebuild closed the USB link without the §12 observer yield")
	}
	if !usbFake.Dead() {
		t.Fatal("the drift rebuild left the USB controller open")
	}

	// control: a further drift that does not leave the usb route fires
	// nothing (here: no drift at all — the same acquisition is a no-op)
	srv.supervisor(b2)
	c.recvNone()
}

// drainEvents reads every event push already queued on the connection
// (deadline-bounded) — the watcher's whole transport-event take.
func (c *sockClient) drainEvents(t *testing.T) []map[string]any {
	t.Helper()
	var events []map[string]any
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
		var m map[string]any
		if err := ReadMsg(c.conn, &m); err != nil {
			break // the short deadline expired: no more queued events
		}
		if asStr(m["type"]) == "event" {
			events = append(events, m)
		}
	}
	c.conn.SetDeadline(time.Now().Add(recvDeadline))
	return events
}

// deadOnSecondDead is the Minor-2 race window made deterministic: its
// Dead() answers false exactly once (supervisor Transport()'s read,
// which then reports the usb route) and true from the second call on
// (supervisor Client()'s read, which then answers nil) — the link died
// in the gap, so the lease's USB route is a FRESH open (an actual
// reboot), never a warm ride.
type deadOnSecondDead struct {
	*fakeCtl
	calls int32
}

func (d *deadOnSecondDead) Dead() bool { return atomic.AddInt32(&d.calls, 1) >= 2 }

// TestLeaseUSBFreshLabelAfterDeathInTheGap (fix round 1, Minor-2): a
// USB-route re-lease whose warm controller dies exactly between the
// Transport() and Client() reads must label the fresh CDC open
// honestly — rebooted:true + the reboot note — never the warm
// ride's rebooted:false.
func TestLeaseUSBFreshLabelAfterDeathInTheGap(t *testing.T) {
	var mu sync.Mutex
	dials := 0
	var warm *deadOnSecondDead
	var fresh *fakeCtl
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			return []discover.Board{{
				MAC6: "d75884", Sources: []string{discover.SourceUSB},
				USBPath: "/dev/serial/by-id/fake-d75884",
			}}, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	sock := filepath.Join(testutil.ShortSockDir(t), "d.sock")
	srv, err := New(Opts{
		SockPath:        sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		Log:             newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
				DialTCP: func() (board.Controller, error) {
					return newFakeCtl(), nil
				},
				DialUSB: func() (board.Controller, error) {
					mu.Lock()
					defer mu.Unlock()
					dials++
					if dials == 1 {
						warm = &deadOnSecondDead{fakeCtl: newFakeCtl()}
						return warm, nil
					}
					fresh = newFakeCtl()
					return fresh, nil
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	awaitSweep(t, reg, 1)
	c := dialSock(t, sock)

	// first usb lease: installs the warm controller (a real fresh open)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "u1", "transport": "usb"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("first usb lease: %#v", rep)
	}
	res := asMap(rep["result"])
	if res["rebooted"] != true {
		t.Fatalf("first lease rebooted = %#v, want true (the open)", res)
	}

	// re-lease: the warm controller dies in the Transport()→Client()
	// gap — the fresh open must be labeled rebooted:true
	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "u1", "transport": "usb"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("re-lease over the dying gap: %#v", rep)
	}
	res = asMap(rep["result"])
	if asStr(res["transport"]) != "usb" {
		t.Fatalf("re-lease transport = %#v, want usb", res)
	}
	if res["rebooted"] != true {
		t.Fatalf("re-lease rebooted = %#v, want true — the gap death makes this a FRESH open (an actual reboot), never a warm ride", res)
	}
	if asStr(res["transport_note"]) != USBTransportNote {
		t.Fatalf("re-lease transport_note = %#v\nwant  %q", res, USBTransportNote)
	}
	mu.Lock()
	n := dials
	mu.Unlock()
	if n != 2 {
		t.Fatalf("usb dials = %d, want 2 (the warm ride failed, a fresh open followed)", n)
	}
	if fresh == nil || fresh.Dead() {
		t.Fatal("the fresh controller must be live after the re-lease")
	}
	_ = warm
}
