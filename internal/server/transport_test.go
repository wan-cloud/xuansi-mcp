package server

// transport_test.go — the §4.4 (B15) transport decision on the default
// lease: the DAEMON resolves the route from the live sources (both
// live → TCP, never a gratuitous USB reboot; only USB live → USB with
// the reboot disclosure; explicit transport param still overrides),
// the USB→TCP upgrade when TCP revives, and the synthetic board
// transport event (kind "transport", data from/to/rebooted) on every
// actual daemon-side route switch.

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

// usbOnlyBoardForRoute is the WiFi-dead shape for the §4.4 route
// states: the USB face sees the board, mDNS does not (cross-subnet or
// radio off). (Distinct from server_test.go's usbOnlyBoard — that one
// blanks Host to dodge the startup audit for the boot-identity tests.)
func usbOnlyBoardForRoute(mac6 string) discover.Board {
	return discover.Board{
		MAC6: mac6, Sources: []string{discover.SourceUSB},
		USBPath: "/dev/serial/by-id/fake-" + mac6,
	}
}

// transportEventOf digs the first board-source transport event out of
// a call()'s unsolicited pushes, failing when none arrived.
func transportEventOf(t *testing.T, events []map[string]any) map[string]any {
	t.Helper()
	for _, m := range events {
		if asStr(m["type"]) != "event" {
			continue
		}
		ev := asMap(m["event"])
		if asStr(ev["source"]) != "board" {
			continue
		}
		d := asMap(ev["data"])
		if asStr(d["kind"]) == "transport" {
			return d
		}
	}
	t.Fatalf("no board transport event in %#v", events)
	return nil
}

// TestLeaseDefaultBothLiveStaysTCP (§4.4 state 1): with both a
// TCP-capable source and USB live, the default lease resolves TCP —
// reply transport "tcp"/rebooted false, no note, the USB seam never
// touched, the first op dials the warm TCP route. Never a gratuitous
// USB reboot.
func TestLeaseDefaultBothLiveStaysTCP(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	res := asMap(rep["result"])
	if asStr(res["transport"]) != "tcp" {
		t.Fatalf("transport = %#v, want tcp (both sources live)", res)
	}
	if v, ok := res["rebooted"].(bool); !ok || v {
		t.Fatalf("rebooted = %#v, want explicit false", res)
	}
	if _, ok := res["transport_note"]; ok {
		t.Fatalf("default TCP lease carries transport_note: %#v", res)
	}
	if n := e.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d after plain lease, want 0", n)
	}

	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	if n := e.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d after plain lease + eval, want 0 — never a gratuitous USB reboot", n)
	}
}

// TestLeaseAutoFallbackUSBOnly (§4.4 states 2+3): only USB live — the
// default lease AUTO-FALLS BACK to USB: the CDC open happens at lease
// time (rebooted:true + transport_note disclosure, never silent). The
// event stream is only reachable AFTER the fallback (watch_events
// itself needs a controller — on a WiFi-dead board TCP cannot warm
// one), so the switch events observed here are the warm-reuse re-lease
// (no event — no switch) and the post-release fresh open
// (from "" → usb: the release yielded the USB controller).
func TestLeaseAutoFallbackUSBOnly(t *testing.T) {
	env := newTransportEnv(t, []discover.Board{usbOnlyBoardForRoute("d75884")}, nil)
	c := dialSock(t, env.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease (auto-fallback): %#v", rep)
	}
	res := asMap(rep["result"])
	if asStr(res["transport"]) != "usb" {
		t.Fatalf("transport = %#v, want usb (only USB live)", res)
	}
	if v, ok := res["rebooted"].(bool); !ok || !v {
		t.Fatalf("rebooted = %#v, want explicit true (the CDC open rebooted the board)", res)
	}
	if asStr(res["transport_note"]) != USBTransportNote {
		t.Fatalf("transport_note = %#v\nwant  %q", res, USBTransportNote)
	}
	if n := env.dialedUSB("d75884"); n != 1 {
		t.Fatalf("usb dials = %d, want exactly 1 (the fallback open)", n)
	}

	// events watching rides the warm USB controller (TCP never dialed —
	// the board has no TCP face at all in this state)
	rep, _ = c.call(2, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}
	if n := env.dialedTCP("d75884"); n != 0 {
		t.Fatalf("tcp dials = %d, want 0 (watch rode the warm USB controller)", n)
	}

	// re-lease (same holder, warm USB controller): no second open, no
	// second reboot, the warm note — §2's one-connection-one-session
	// means the session the first open created is still the one in hand
	rep, evs := c.call(3, map[string]any{"type": "lease", "board": "d75884", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("re-lease: %#v", rep)
	}
	res = asMap(rep["result"])
	if asStr(res["transport"]) != "usb" {
		t.Fatalf("re-lease transport = %#v, want usb", res)
	}
	if v, ok := res["rebooted"].(bool); !ok || v {
		t.Fatalf("re-lease rebooted = %#v, want false (warm USB controller reused)", res)
	}
	if asStr(res["transport_note"]) != USBWarmNote {
		t.Fatalf("re-lease transport_note = %#v\nwant  %q", res, USBWarmNote)
	}
	if n := env.dialedUSB("d75884"); n != 1 {
		t.Fatalf("usb dials = %d after re-lease, want still 1 (warm reuse)", n)
	}
	if len(evs) != 0 {
		t.Fatalf("warm re-lease fired events: %#v — no switch, no event", evs)
	}

	// ops ride the warm USB controller
	rep, _ = c.call(4, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval over usb: %#v", rep)
	}
	if n := env.dialedTCP("d75884"); n != 0 {
		t.Fatalf("tcp dials = %d under a usb lease, want 0", n)
	}

	// release yields the USB controller; the next (still USB-only)
	// default lease fresh-opens again — rebooted:true and the from ""
	// → usb switch event, now observable because the watcher is live.
	// A fresh fake models the real world's new CDC session (the yield
	// closed the old link).
	env.dmu.Lock()
	env.usbFakes["d75884"] = newFakeCtl()
	env.dmu.Unlock()
	rep, _ = c.call(5, map[string]any{"type": "release", "board": "d75884", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("release: %#v", rep)
	}
	rep, evs = c.call(6, map[string]any{"type": "lease", "board": "d75884", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("re-lease after release: %#v", rep)
	}
	res = asMap(rep["result"])
	if res["rebooted"] != true {
		t.Fatalf("post-release lease rebooted = %#v, want true (fresh open)", res)
	}
	d := transportEventOf(t, evs)
	if asStr(d["from"]) != "" || asStr(d["to"]) != "usb" || d["rebooted"] != true {
		t.Fatalf("transport event = %#v, want from \"\" → usb rebooted true (the yield cleared the controller)", d)
	}
}

// TestLeaseUpgradeOnRevivedTCP (§4.4): a board auto-fallen-back to USB
// whose TCP source later revives — the next DEFAULT lease upgrades back
// to TCP: the USB controller is yielded (§12 observer hello) and
// dropped, the reply says tcp, the board transport event carries
// usb → tcp rebooted:false (§13: redial ≠ reboot), and the first op
// dials TCP.
func TestLeaseUpgradeOnRevivedTCP(t *testing.T) {
	// phase 0: both faces see the board — the registry LEARNS the
	// endpoint from the mdns face (the usb face contributes only the
	// path), so the phase-1 downgrade leaves it as last-known
	env := newTransportEnv(t,
		[]discover.Board{usbOnlyBoardForRoute("d75884")},
		[]discover.Board{mkBoard("d75884", "xuansi-d75884.local")})
	c := dialSock(t, env.sock)

	// phase 1: the mdns face goes silent (a healthy empty sweep — the
	// source downgrades away, the endpoint fields survive last-known)
	env.setMDNS(nil)
	if err := env.reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh (mdns gone): %v", err)
	}

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease (fallback): %#v", rep)
	}
	if asStr(asMap(rep["result"])["transport"]) != "usb" {
		t.Fatalf("phase-1 transport = %#v, want usb", rep)
	}

	// the watcher rides the warm USB controller (TCP is dead in this
	// phase — it could not warm one)
	rep, _ = c.call(2, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}

	// phase 2: TCP revives — the mdns face sees the board again with
	// the SAME endpoint (no supervisor drift: the last-known fields
	// never left), one refresh lands
	env.setMDNS([]discover.Board{mkBoard("d75884", "xuansi-d75884.local")})
	if err := env.reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rep, evs := c.call(3, map[string]any{"type": "lease", "board": "d75884", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease (upgrade): %#v", rep)
	}
	res := asMap(rep["result"])
	if asStr(res["transport"]) != "tcp" {
		t.Fatalf("transport = %#v, want tcp (TCP revived — upgrade)", res)
	}
	if v, ok := res["rebooted"].(bool); !ok || v {
		t.Fatalf("rebooted = %#v, want false (redial is not a reboot, §13)", res)
	}
	d := transportEventOf(t, evs)
	if asStr(d["from"]) != "usb" || asStr(d["to"]) != "tcp" || d["rebooted"] != false {
		t.Fatalf("transport event = %#v, want usb → tcp rebooted false", d)
	}
	if !sawObserverYield(env.usbFakes["d75884"]) {
		t.Fatal("upgrade performed no §12 observer yield before closing the USB link")
	}
	if !env.usbFakes["d75884"].Dead() {
		t.Fatal("upgrade must drop the USB controller (the next touch dials TCP)")
	}
	if n := env.dialedUSB("d75884"); n != 1 {
		t.Fatalf("usb dials = %d during upgrade, want still 1 (no new open)", n)
	}

	// the USB fallback's swap closed the phase-0 audit's TCP link — a
	// real upgrade opens a FRESH socket, so model that with a fresh fake
	env.dmu.Lock()
	env.fakes["d75884"] = newFakeCtl()
	env.dmu.Unlock()

	// the first op rides the upgraded TCP route (a fresh dial beyond
	// whatever the phase-0 audit warmed)
	tcpBefore := env.dialedTCP("d75884")
	rep, _ = c.call(4, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval after upgrade: %#v", rep)
	}
	if n := env.dialedTCP("d75884"); n <= tcpBefore {
		t.Fatalf("tcp dials %d → %d — the upgrade must re-arm the TCP route", tcpBefore, n)
	}
}

// TestLeaseExplicitTCPOverridesUSBOnly: the explicit transport param
// still overrides the daemon's decision — transport:"tcp" on a
// USB-only board leases the TCP route (and fails honestly at op time
// when TCP is dead), never silently opening USB.
func TestLeaseExplicitTCPOverridesUSBOnly(t *testing.T) {
	env := newTransportEnv(t, []discover.Board{usbOnlyBoardForRoute("d75884")}, nil)
	c := dialSock(t, env.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "x1", "transport": "tcp"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease (explicit tcp): %#v", rep)
	}
	res := asMap(rep["result"])
	if asStr(res["transport"]) != "tcp" {
		t.Fatalf("transport = %#v, want tcp (explicit override)", res)
	}
	if n := env.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d under an explicit tcp lease, want 0 — no silent USB open", n)
	}
}

// TestLeaseUSBTransportReplyShape: the EXPLICIT usb lease rides the
// same §4.4 reply shape — transport usb, rebooted:true (this call
// opened the CDC port), transport_note verbatim — and fires the
// transport event like the auto path.
func TestLeaseUSBTransportReplyShape(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}
	rep, evs := c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "u1", "transport": "usb"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease usb: %#v", rep)
	}
	res := asMap(rep["result"])
	if asStr(res["transport"]) != "usb" {
		t.Fatalf("transport = %#v, want usb", res)
	}
	if v, ok := res["rebooted"].(bool); !ok || !v {
		t.Fatalf("rebooted = %#v, want true (the explicit open rebooted the board)", res)
	}
	if asStr(res["transport_note"]) != USBTransportNote {
		t.Fatalf("transport_note = %#v\nwant  %q", res, USBTransportNote)
	}
	d := transportEventOf(t, evs)
	if asStr(d["from"]) != "tcp" || asStr(d["to"]) != "usb" || d["rebooted"] != true {
		t.Fatalf("transport event = %#v, want tcp → usb rebooted true (the audit's warm TCP link was in hand)", d)
	}
}

// transportEnv is a custom-registry env for the §4.4 states the shared
// newEnv cannot build (it always feeds both faces the same board set):
// a mutable discovery pair plus the shared dial-counter plumbing.
type transportEnv struct {
	*env
	reg     *discover.Registry
	setMDNS func([]discover.Board)
}

// mutableSources is the discover tests' fakeSources shape, local here
// so the §4.4 states can flip faces mid-test.
type mutableSources struct {
	mu   sync.Mutex
	usb  []discover.Board
	mdns []discover.Board
}

func (m *mutableSources) snapshot() (usb, mdns []discover.Board) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.usb, m.mdns
}

// newTransportEnv builds a serving Server over a mutable discovery
// pair: usbBoards feeds the USB face, mdnsBoards the mDNS face (nil =
// that face sees nothing). Both faces share one fake controller per
// seam exactly like newEnvFull's plumbing.
func newTransportEnv(t *testing.T, usbBoards, mdnsBoards []discover.Board) *transportEnv {
	t.Helper()
	src := &mutableSources{usb: usbBoards, mdns: mdnsBoards}
	e := &env{
		t:         t,
		fakes:     map[string]*fakeCtl{},
		dialed:    map[string]int{},
		usbFakes:  map[string]*fakeCtl{},
		usbDialed: map[string]int{},
	}
	seed := append(append([]discover.Board{}, usbBoards...), mdnsBoards...)
	for _, b := range seed {
		e.fakes[b.MAC6] = newFakeCtl()
		e.usbFakes[b.MAC6] = newFakeCtl()
	}
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			usb, _ := src.snapshot()
			return usb, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			_, mdns := src.snapshot()
			return mdns, nil
		}),
	)
	e.sock = filepath.Join(testutil.ShortSockDir(t), "d.sock")
	srv, err := New(Opts{
		SockPath:        e.sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		Log:             newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
				DialTCP: func() (board.Controller, error) {
					e.dmu.Lock()
					e.dialed[ref.MAC6]++
					f := e.fakes[ref.MAC6]
					e.dmu.Unlock()
					if f == nil {
						return nil, fmt.Errorf("no fake for %s", ref.MAC6)
					}
					return f, nil
				},
				DialUSB: func() (board.Controller, error) {
					e.dmu.Lock()
					e.usbDialed[ref.MAC6]++
					f := e.usbFakes[ref.MAC6]
					e.dmu.Unlock()
					if f == nil {
						return nil, fmt.Errorf("no usb fake for %s", ref.MAC6)
					}
					return f, nil
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e.srv = srv
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	// one entry per mac6 (the faces may describe the same board)
	distinct := map[string]bool{}
	for _, b := range seed {
		distinct[b.MAC6] = true
	}
	awaitSweep(t, reg, len(distinct))
	return &transportEnv{env: e, reg: reg, setMDNS: func(bs []discover.Board) {
		src.mu.Lock()
		src.mdns = bs
		src.mu.Unlock()
	}}
}
