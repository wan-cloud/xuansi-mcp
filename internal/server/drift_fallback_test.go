package server

// drift_fallback_test.go — B16 F3-①: the §4.4 transport decision must
// hold at the DRIFT seam and the cold-redial seam, not only at lease
// time. The w3t5a run3 shape: a live USB lease, one endpoint drift
// (the registry entry re-faces), the compare-and-rebuild closes the
// ONLY live link, and every later op redials TCP-only — "no TCP
// endpoint to dial" forever, saved only by release→re-lease (the seat
// folklore ③ was supposed to never need).
//
// Three states, one truth (§4.4):
//   - drift to dead-TCP + live-USB from a tcp route → the next op
//     falls back to USB (one CDC open = one reboot, disclosed via the
//     board.transport event) — no release in sight;
//   - drift with both live while riding USB → the existing upgrade
//     close + usb→tcp event (transport_drift_test.go pins that one);
//   - drift on a LIVE usb route whose USBPath is unchanged and stays
//     the decided route → the rebuild CARRIES the live controller
//     over: no close, no re-open (no gratuitous reboot), no event,
//     last_seen keeps advancing — the run3 fix.

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/testutil"
)

// driftEnv is the shared fixture: one board view behind a mutex, USB
// and TCP dial seams that count dials, and a server on a short sock.
type driftEnv struct {
	mu   sync.Mutex
	view discover.Board

	tcpDials int
	usbDials int
	vars     map[int]*fakeCtl // dial counter → the fake it returned

	reg  *discover.Registry
	srv  *Server
	sock string
}

func newDriftEnv(t *testing.T, view discover.Board, mod func(o *Opts)) *driftEnv {
	t.Helper()
	e := &driftEnv{view: view, vars: map[int]*fakeCtl{}}
	e.reg = discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			e.mu.Lock()
			defer e.mu.Unlock()
			return []discover.Board{e.view}, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			e.mu.Lock()
			defer e.mu.Unlock()
			// the mdns face answers only while the view carries the
			// mdns source — dropping it is the healthy empty sweep that
			// downgrades the entry (the registry's last-known backfill
			// then keeps Host/IP, mirroring the production flap
			// semantics the route truth must survive)
			if !hasSource(e.view, discover.SourceMDNS) {
				return nil, nil
			}
			return []discover.Board{e.view}, nil
		}),
	)
	e.sock = filepath.Join(testutil.ShortSockDir(t), "d.sock")
	o := Opts{
		SockPath:          e.sock,
		Registry:          e.reg,
		GraceTTL:          10 * time.Second,
		RefreshInterval:   -1,
		KeepaliveInterval: -1, // off: this file pins the dial/decision faces only
		Log:               newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
				DialTCP: func() (board.Controller, error) {
					// honest to the ref: a view with no TCP-capable
					// source has no TCP endpoint to dial (the
					// production seam's ErrNoDialEndpoint)
					e.mu.Lock()
					v := e.view
					e.mu.Unlock()
					if !tcpDialable(v) {
						return nil, fmt.Errorf("board %s: no TCP endpoint to dial: %w", v.MAC6, board.ErrNoDialEndpoint)
					}
					e.mu.Lock()
					defer e.mu.Unlock()
					e.tcpDials++
					f := newFakeCtl()
					e.vars[e.tcpDials] = f
					return f, nil
				},
				DialUSB: func() (board.Controller, error) {
					e.mu.Lock()
					v := e.view
					e.mu.Unlock()
					if !hasSource(v, discover.SourceUSB) || v.USBPath == "" {
						return nil, fmt.Errorf("board %s: no USB path to dial: %w", v.MAC6, board.ErrNoDialEndpoint)
					}
					e.mu.Lock()
					defer e.mu.Unlock()
					e.usbDials++
					f := newFakeCtl()
					e.vars[-e.usbDials] = f
					return f, nil
				},
			})
		},
	}
	if mod != nil {
		mod(&o)
	}
	srv, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e.srv = srv
	t.Cleanup(func() { srv.Close() })
	if err := regRefresh(e.reg); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	awaitSweep(t, e.reg, 1)
	return e
}

func regRefresh(reg *discover.Registry) error { return reg.Refresh(context.Background()) }

func (e *driftEnv) setView(fn func(b *discover.Board)) {
	e.mu.Lock()
	fn(&e.view)
	e.mu.Unlock()
	if err := regRefresh(e.reg); err != nil {
		panic(err)
	}
}

func (e *driftEnv) counts() (tcp, usb int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tcpDials, e.usbDials
}

func (e *driftEnv) usbFake(n int) *fakeCtl {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.vars[-n]
}

func (e *driftEnv) tcpFake(n int) *fakeCtl {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.vars[n]
}

// TestDriftDeadTCPFallsBackToUSB (state A, the wifi-down migration):
// a default lease rides TCP; the wifi face drops (only USB live); the
// drift rebuild closes the dead TCP corpse and the NEXT OP re-dials
// per §4.4 — the USB fallback, one reboot, disclosed as ONE
// board.transport event {from:"tcp", to:"usb", rebooted:true} — and
// the op SUCCEEDS. No release anywhere (③).
func TestDriftDeadTCPFallsBackToUSB(t *testing.T) {
	e := newDriftEnv(t, discover.Board{
		MAC6: "d75884", Sources: []string{discover.SourceUSB, discover.SourceMDNS},
		USBPath: "/dev/serial/by-id/fake-d75884", Host: "xuansi-d75884.local", IP: "10.1.51.205", Port: 7777,
	}, nil)
	c := dialSock(t, e.sock)

	// watcher first: the transport event is the fallback's disclosure
	rep, _ := c.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}

	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "h1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	if asStr(asMap(rep["result"])["transport"]) != "tcp" {
		t.Fatalf("default lease transport = %#v, want tcp (both sources live)", rep["result"])
	}
	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval over tcp: %#v", rep)
	}
	tcp, usb := e.counts()
	if tcp != 1 || usb != 0 {
		t.Fatalf("dials after the tcp leg = tcp:%d usb:%d, want 1/0", tcp, usb)
	}

	// (a) the wifi migrates .205 → .206 while still up: the endpoint
	// drifts, the rebuild closes the tcp corpse, and the SAME op's cold
	// redial lands on the fresh TCP seam — a tcp→tcp redial is no
	// switch, no event (§13).
	e.setView(func(b *discover.Board) { b.IP = "10.1.51.206" })
	rep, _ = c.call(4, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval across the .206 drift: %#v", rep)
	}
	tcp, usb = e.counts()
	if tcp != 2 || usb != 0 {
		t.Fatalf("dials after the .206 drift = tcp:%d usb:%d, want 2/0", tcp, usb)
	}
	c.recvNone() // no event: tcp → tcp redial is not a switch

	// (b) the wifi dies: the healthy empty sweep downgrades the entry
	// to usb-only (Host/IP last-known), and the tcp link dies with the
	// network — the next op's cold redial applies §4.4: ONLY USB live,
	// so the USB fallback opens (one reboot, disclosed as ONE
	// board.transport event) and the op SUCCEEDS. No release anywhere.
	e.setView(func(b *discover.Board) { b.Sources = []string{discover.SourceUSB} })
	e.tcpFake(2).markDead()
	rep, evs := c.call(5, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 3"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval after the wifi death: the §4.4 fallback must save the op without a release — %#v", rep)
	}
	tcp, usb = e.counts()
	if usb != 1 {
		t.Fatalf("usb dials after the wifi death = %d, want 1 (the fallback open)", usb)
	}
	if e.usbFake(1).Dead() {
		t.Fatal("the fallback controller must be live after the op")
	}
	d := transportEventOf(t, evs) // the event rides the same request as the op it saved
	if asStr(d["from"]) != "tcp" || asStr(d["to"]) != "usb" || d["rebooted"] != true {
		t.Fatalf("transport event = %#v, want tcp → usb rebooted:true (the fallback disclosure)", d)
	}
	c.recvNone() // exactly one event, nothing else
}

// TestDriftUSBRouteCarriedOverNoReopen (state C, the run3 replay): a
// live USB lease drifts on a face the USB link does not depend on
// (here: the self-reported port materializing, the exact run3 trigger)
// — the rebuild CARRIES the live controller into the fresh supervisor:
// no §12 yield-close, no second CDC open (no gratuitous reboot), no
// transport event, and ops keep crossing (last_seen continuity).
func TestDriftUSBRouteCarriedOverNoReopen(t *testing.T) {
	e := newDriftEnv(t, discover.Board{
		MAC6: "d75884", Sources: []string{discover.SourceUSB},
		USBPath: "/dev/serial/by-id/fake-d75884",
	}, nil)
	c := dialSock(t, e.sock)

	// lease first (the §4.4 usb route, one open), THEN watch: an
	// UNLEASED crossing keeps the TCP-only redial by design, and this
	// board has no TCP face
	rep, _ := c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "h1", "transport": "usb"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("usb lease: %#v", rep)
	}
	rep, _ = c.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}
	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval over usb: %#v", rep)
	}
	if _, usb := e.counts(); usb != 1 {
		t.Fatalf("usb dials = %d, want 1 (the lease open)", usb)
	}
	warm := e.usbFake(1)

	// run3's trigger: the board's SELF-REPORTED port materializes in
	// the registry between sweeps (EnrichEndpoint — the tap's hello/
	// wifi crossing), so the ref compare fires a rebuild while the USB
	// link is LIVE and the USBPath is unchanged.
	e.reg.EnrichEndpoint("d75884", "", 7777)
	e.setView(func(b *discover.Board) {}) // the sweep lands the enriched port

	rep, _ = c.call(4, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval across the drift: %#v", rep)
	}
	if _, usb := e.counts(); usb != 1 {
		t.Fatalf("usb dials after drift = %d, want 1 — the live link carries over, NO reopen (reopen = a gratuitous board reboot)", usb)
	}
	if warm.Dead() {
		t.Fatal("the drift rebuild closed the carried-over USB link")
	}
	// last_seen continuity across the drift (spec AC3): the crossing
	// right after the drift is fresh, not a reconnect gap
	rep, _ = c.call(5, map[string]any{"type": "board_status", "board": "d75884"})
	st := asMap(asMap(rep["result"])["board"])
	if got := asInt(st["last_seen_age_ms"]); got > 2000 {
		t.Fatalf("last_seen_age_ms = %d after the drift crossing, want fresh (<2s — the carried link crossed)", got)
	}
	if got := asStr(st["transport"]); got != "usb" {
		t.Fatalf("transport after carry-over = %q, want usb", got)
	}
	c.recvNone() // no transport event: usb → usb is not a switch
}

// TestExplicitTCPLeaseNeverFallsBackToUSB (the pin honesty): an
// explicit transport:"tcp" lease refused the CDC reboot up front —
// when TCP then dies under the lease, ops fail HONESTLY (cause "dial
// failed"); the daemon never repays the explicit choice with a silent
// USB open (proto.go's transport contract: "never a silent USB open").
func TestExplicitTCPLeaseNeverFallsBackToUSB(t *testing.T) {
	e := newDriftEnv(t, discover.Board{
		MAC6: "d75884", Sources: []string{discover.SourceUSB, discover.SourceMDNS},
		USBPath: "/dev/serial/by-id/fake-d75884", Host: "xuansi-d75884.local", IP: "10.1.51.205", Port: 7777,
	}, nil)
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "h1", "transport": "tcp"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("tcp lease: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval over tcp: %#v", rep)
	}

	// the wifi dies the same way (migrate, then the empty sweep), the
	// tcp link dies with it — the explicit "tcp" pin keeps the redial
	// honest: no USB open, the bounded dial failure surfaces verbatim
	e.setView(func(b *discover.Board) { b.IP = "10.1.51.206" })
	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval across the .206 drift: %#v", rep)
	}
	e.setView(func(b *discover.Board) { b.Sources = []string{discover.SourceUSB} })
	e.tcpFake(2).markDead()

	rep, _ = c.call(4, map[string]any{"type": "eval", "board": "d75884", "holder": "h1", "code": "return 3"})
	er := asMap(rep["error"])
	if er == nil {
		t.Fatal("explicit-tcp lease + dead TCP: the op must fail honestly, not silently reboot the board over USB")
	}
	if _, usb := e.counts(); usb != 0 {
		t.Fatalf("usb dials = %d, want 0 (the explicit tcp choice forbids the CDC open)", usb)
	}
	if asStr(er["cause"]) != "dial failed" {
		t.Fatalf("error cause = %q, want dial failed", er["cause"])
	}
}
