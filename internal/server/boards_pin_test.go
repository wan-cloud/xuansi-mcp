package server

// boards_pin_test.go — the ⑨ (B15 §4.9) pin face at the internal API:
// boards_pin {mac6, ip, port} → {pinned, endpoint} materializes the
// registry's pin source immediately (a still-undiscovered board
// appears, dialable through the pinned endpoint), boards_unpin {mac6}
// → {unpinned} removes exactly the pin declaration.

import (
	"context"
	"sync"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/testutil"
)

// pinEnv is an empty-discovery server (the cross-subnet bench: neither
// face ever sees the board) whose supervisor factory records the refs
// its dial seams were pinned against — the proof the pin's endpoint is
// what gets dialed.
type pinEnv struct {
	*env
	reg *discover.Registry

	mu   sync.Mutex
	refs []board.BoardRef
}

func newPinEnv(t *testing.T) *pinEnv {
	t.Helper()
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	e := &env{
		t:         t,
		fakes:     map[string]*fakeCtl{},
		dialed:    map[string]int{},
		usbFakes:  map[string]*fakeCtl{},
		usbDialed: map[string]int{},
	}
	pe := &pinEnv{env: e, reg: reg}
	e.sock = testutil.ShortSockDir(t) + "/d.sock"
	srv, err := New(Opts{
		SockPath:        e.sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		Log:             newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			pe.mu.Lock()
			pe.refs = append(pe.refs, ref)
			pe.mu.Unlock()
			e.dmu.Lock()
			e.fakes[ref.MAC6] = newFakeCtl()
			e.usbFakes[ref.MAC6] = newFakeCtl()
			e.dmu.Unlock()
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
				DialTCP: func() (board.Controller, error) {
					e.dmu.Lock()
					e.dialed[ref.MAC6]++
					f := e.fakes[ref.MAC6]
					e.dmu.Unlock()
					return f, nil
				},
				DialUSB: func() (board.Controller, error) {
					e.dmu.Lock()
					e.usbDialed[ref.MAC6]++
					f := e.usbFakes[ref.MAC6]
					e.dmu.Unlock()
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
	awaitSweep(t, reg, 0)
	return pe
}

func (p *pinEnv) recordedRefs() []board.BoardRef {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]board.BoardRef, len(p.refs))
	copy(out, p.refs)
	return out
}

// TestBoardsPinMaterializesAndDials: the pin lands immediately — no
// discovery sweep, no prior knowledge of the board — the reply echoes
// {pinned, endpoint}, list_boards shows the pin-only entry, and the
// default lease + eval ride the PINNED endpoint (the supervisor's dial
// ref carries the pin's address; nothing else ever supplied one).
func TestBoardsPinMaterializesAndDials(t *testing.T) {
	pe := newPinEnv(t)
	c := dialSock(t, pe.sock)

	rep, _ := c.call(1, map[string]any{"type": "boards_pin", "mac6": "d75884", "ip": "10.1.11.204", "port": 7777})
	if asMap(rep["error"]) != nil {
		t.Fatalf("boards_pin: %#v", rep)
	}
	res := asMap(rep["result"])
	if asStr(res["pinned"]) != "d75884" || asStr(res["endpoint"]) != "10.1.11.204:7777" {
		t.Fatalf("pin reply = %#v, want {pinned:d75884, endpoint:10.1.11.204:7777}", res)
	}

	// the entry is live immediately: listed with exactly the pin source
	rep, _ = c.call(2, map[string]any{"type": "list_boards"})
	boards := asMap(rep["result"])["boards"].([]any)
	if len(boards) != 1 {
		t.Fatalf("boards = %#v, want the pinned entry", boards)
	}
	v := asMap(boards[0])
	if asStr(v["mac6"]) != "d75884" || asStr(v["ip"]) != "10.1.11.204" || asInt(v["port"]) != 7777 {
		t.Fatalf("pinned view = %#v", v)
	}
	srcs := v["sources"].([]any)
	if len(srcs) != 1 || asStr(srcs[0]) != "pin" {
		t.Fatalf("sources = %#v, want [pin]", srcs)
	}
	if v["stale"] != false {
		t.Fatalf("a pinned board is live, not stale: %#v", v)
	}

	// default lease + eval ride the pinned endpoint
	rep, _ = c.call(3, map[string]any{"type": "lease", "board": "d75884", "holder": "p1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease over the pin: %#v", rep)
	}
	lres := asMap(rep["result"])
	if asStr(lres["transport"]) != "tcp" {
		t.Fatalf("lease transport = %#v, want tcp (the pin is a TCP-capable source)", lres)
	}
	rep, _ = c.call(4, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "holder": "p1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval over the pin: %#v", rep)
	}
	refs := pe.recordedRefs()
	if len(refs) == 0 || refs[0].IP != "10.1.11.204" || refs[0].Port != 7777 {
		t.Fatalf("supervisor refs = %+v, want the pinned 10.1.11.204:7777 dialed first", refs)
	}
}

// TestBoardsUnpinDowngrades: unpin removes the pin — the reply echoes
// {unpinned}, the pin-only entry degrades to last-known (empty sources,
// stale — honest, not deleted), and unpinning again is an idempotent
// no-op echo.
func TestBoardsUnpinDowngrades(t *testing.T) {
	pe := newPinEnv(t)
	c := dialSock(t, pe.sock)

	c.call(1, map[string]any{"type": "boards_pin", "mac6": "d75884", "ip": "10.1.11.204", "port": 7777})
	rep, _ := c.call(2, map[string]any{"type": "boards_unpin", "mac6": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("boards_unpin: %#v", rep)
	}
	if asStr(asMap(rep["result"])["unpinned"]) != "d75884" {
		t.Fatalf("unpin reply = %#v", rep)
	}

	rep, _ = c.call(3, map[string]any{"type": "list_boards"})
	boards := asMap(rep["result"])["boards"].([]any)
	if len(boards) != 1 {
		t.Fatalf("boards = %#v — the entry must stay as last-known", boards)
	}
	v := asMap(boards[0])
	if srcs, _ := v["sources"].([]any); len(srcs) != 0 {
		t.Fatalf("sources after unpin = %#v, want empty (stale last-known)", srcs)
	}
	if v["stale"] != true {
		t.Fatalf("pin-only board after unpin must be stale: %#v", v)
	}

	// idempotent-shaped: unpinning a never-pinned board echoes success
	rep, _ = c.call(4, map[string]any{"type": "boards_unpin", "mac6": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("repeat boards_unpin: %#v", rep)
	}
	if asStr(asMap(rep["result"])["unpinned"]) != "d75884" {
		t.Fatalf("repeat unpin reply = %#v", rep)
	}
}

// TestBoardsPinValidation: a non-mac6 id or an out-of-range port is a
// server-class refusal (the registry is the validator) — and a refused
// pin materializes nothing.
func TestBoardsPinValidation(t *testing.T) {
	pe := newPinEnv(t)
	c := dialSock(t, pe.sock)

	for _, req := range []map[string]any{
		{"type": "boards_pin", "mac6": "d7588", "ip": "10.1.11.204", "port": 7777},
		{"type": "boards_pin", "mac6": "zzzzzz", "ip": "10.1.11.204", "port": 7777},
		{"type": "boards_pin", "mac6": "d75884", "ip": "", "port": 7777},
		{"type": "boards_pin", "mac6": "d75884", "port": 7777},
		{"type": "boards_pin", "mac6": "d75884", "ip": "10.1.11.204", "port": 0},
		{"type": "boards_pin", "mac6": "d75884", "ip": "10.1.11.204", "port": 70000},
	} {
		rep, _ := c.call(1, req)
		er := errOf(t, rep)
		if asStr(er["class"]) != "server" {
			t.Errorf("pin %v: class = %q, want server (%#v)", req, er["class"], er)
		}
	}
	rep, _ := c.call(2, map[string]any{"type": "list_boards"})
	if boards := asMap(rep["result"])["boards"].([]any); len(boards) != 0 {
		t.Fatalf("refused pins materialized entries: %#v", boards)
	}
}
