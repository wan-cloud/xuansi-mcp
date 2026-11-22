package mcp

// tools_dynamic_test.go — the board-world refresh loop's locks: the
// 2026-11-23 bench-tool retirement pin (a refresh over api1 + api2
// boards registers NOTHING beyond the static set, while the caps cache
// still records each version), the holder-less caps crossing, the
// loop's stop discipline, and the board-list decode — all against a
// request-dependent fake daemon riding the task-8 harness (real unix
// socket, real apiclient, real MCP server↔client pair). The
// caps-driven tool expansion these tests once covered is retired
// (owner ruling); its face is xuansi_run_lua / xuansi_board_op /
// xuansi_script_start plus xuansi_abi_reference's bench_signatures.

import (
	"context"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// ---------------------------------------------------------------------------
// request-dependent fake daemon (the exact-script harness cannot
// answer refresh loops or repeated request types)

// dynFake is a scriptable stand-in daemon whose replies depend on the
// request: list_boards answers the current board set, op caps answers
// per-board caps (else a not_held refusal), eval answers a plain
// result, watch_events acks the board (or a forced refusal). Every
// request it reads is counted and (optionally) published; tests can
// push board events (§8 EventPush frames) on the same connection via
// pushEvent — serialized with the reply writes by wmu so frames never
// interleave.
type dynFake struct {
	t    *testing.T
	reqs chan server.Request // nil = counting only

	mu       sync.Mutex
	boards   []map[string]any            // list_boards result entries
	caps     map[string]map[string]any   // mac6 → caps result (nil = none served)
	watchErr map[string]*server.RPCError // mac6 → forced watch_events refusal
	counts   map[string]int              // request-type counters

	wmu  sync.Mutex // serializes frame writes (replies + pushed events)
	conn net.Conn
}

func newDynFake(t *testing.T, fd *fakeDaemon, withChan bool) *dynFake {
	df := &dynFake{
		t:        t,
		caps:     map[string]map[string]any{},
		watchErr: map[string]*server.RPCError{},
		counts:   map[string]int{},
	}
	if withChan {
		df.reqs = make(chan server.Request, 64)
	}
	fd.serve(t, df.handle)
	return df
}

func (df *dynFake) handle(conn net.Conn) {
	df.wmu.Lock()
	df.conn = conn
	df.wmu.Unlock()
	for {
		var req server.Request
		if err := server.ReadMsg(conn, &req); err != nil {
			return // client close: expected shutdown path
		}
		df.mu.Lock()
		df.counts[req.Type]++
		if df.reqs != nil {
			select {
			case df.reqs <- req:
			default:
				df.t.Errorf("dynFake: request queue overflow (%s)", req.Type)
			}
		}
		var rep *server.Reply
		switch req.Type {
		case "list_boards":
			boards := make([]any, len(df.boards))
			for i, b := range df.boards {
				boards[i] = b
			}
			rep = &server.Reply{Type: "reply", ID: req.ID,
				Result: map[string]any{"boards": boards, "mdns_error": ""}}
		case "op":
			if caps := df.caps[req.Board]; caps != nil {
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: caps}
			} else {
				rep = &server.Reply{Type: "reply", ID: req.ID, Error: &server.RPCError{
					Class: "not_held",
					Msg:   "no lease on board " + req.Board + " — lease it first"}}
			}
		case "eval":
			rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{"out": ""}}
		case "lease", "release":
			rep = &server.Reply{Type: "reply", ID: req.ID,
				Result: map[string]any{"board": req.Board, "holder": req.Holder, "session": 1.0}}
		case "watch_events":
			if rerr := df.watchErr[req.Board]; rerr != nil {
				rep = &server.Reply{Type: "reply", ID: req.ID, Error: rerr}
			} else {
				rep = &server.Reply{Type: "reply", ID: req.ID,
					Result: map[string]any{"board": req.Board}}
			}
		default:
			rep = &server.Reply{Type: "reply", ID: req.ID, Error: &server.RPCError{
				Class: "server", Msg: "dynFake: unexpected request type " + req.Type}}
		}
		df.mu.Unlock()
		df.wmu.Lock()
		err := server.WriteMsg(conn, rep)
		df.wmu.Unlock()
		if err != nil {
			df.t.Errorf("dynFake reply: %v", err)
			return
		}
	}
}

func (df *dynFake) setBoards(boards ...map[string]any) {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.boards = boards
}

func (df *dynFake) setCaps(mac6 string, caps map[string]any) {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.caps[mac6] = caps
}

func (df *dynFake) failWatch(mac6 string, rerr *server.RPCError) {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.watchErr[mac6] = rerr
}

// clearWatchFailures drops every forced watch_events refusal.
func (df *dynFake) clearWatchFailures() {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.watchErr = map[string]*server.RPCError{}
}

// pushEvent writes one §8 EventPush frame for (mac6, source) on the
// served connection — the daemon's unsolicited event half. false means
// the write failed (connection gone); frames never interleave with
// replies (wmu).
func (df *dynFake) pushEvent(mac6, source string, data map[string]any) bool {
	df.wmu.Lock()
	defer df.wmu.Unlock()
	if df.conn == nil {
		return false
	}
	return server.WriteMsg(df.conn, server.EventPush{
		Type: "event", Board: mac6, Event: server.WireEvent{Source: source, Data: data},
	}) == nil
}

func (df *dynFake) count(typ string) int {
	df.mu.Lock()
	defer df.mu.Unlock()
	return df.counts[typ]
}

// v1Caps is §8's caps schema shape with api_version 1 (values as the
// daemon's JSON decode would deliver them).
func v1Caps() map[string]any {
	ops := func(names ...string) map[string]any {
		list := make([]any, len(names))
		for i, n := range names {
			list[i] = n
		}
		return map[string]any{"ops": list}
	}
	return map[string]any{
		"fw":          "xuansi-fw v0.1.0",
		"api_version": 1.0,
		"batch":       2.0,
		"primitives": map[string]any{
			"gpio":   ops("mode", "write", "read", "on"),
			"uart":   ops("setup", "write", "read", "on"),
			"i2c":    ops("setup", "write", "read", "wreg", "rreg", "scan"),
			"spi":    ops("setup", "xfer", "write", "read"),
			"pwm":    ops("setup", "duty", "stop"),
			"adc":    ops("setup", "read", "read_mv"),
			"rmt":    ops("setup", "tx", "rx"),
			"events": ops("on", "off", "next"),
		},
	}
}

func boardEntry(mac6, alias string) map[string]any {
	return map[string]any{"mac6": mac6, "alias": alias, "host": "xuansi-" + mac6 + ".local", "fw": "v0.1.0"}
}

// listTools returns the served tool names, sorted.
func listTools(t *testing.T, e *env) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(lr.Tools))
	for _, tool := range lr.Tools {
		names = append(names, tool.Name)
	}
	sort.Strings(names)
	return names
}

// ---------------------------------------------------------------------------
// the retirement pin

// TestNoBenchDynamicToolsEverRegistered (2026-11-23 owner ruling, the
// seat-debt batch): the per-primitive bench dynamic tools AND the
// api_version notice are RETIRED — a refresh over an api_version 1
// board and an api_version 2 board registers NOTHING beyond the static
// set (no <base>_bench_* notice, no <base>_<prim>_<op> tool), while the
// loop's surviving duties hold: the caps cache records each board's
// version (driver_push's --@api gate reads it) and the refresh's caps
// crossing stays HOLDER-LESS even after a lease bound the connection
// (the task-8 grace-adopt invariant).
func TestNoBenchDynamicToolsEverRegistered(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("a1b2c3", "bench1"), boardEntry("ee0005", "future"))
	df.setCaps("a1b2c3", v1Caps())
	df.setCaps("ee0005", map[string]any{"api_version": 2.0, "primitives": map[string]any{}})
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	// bind the daemon connection with an explicit holder first — the
	// refresh's caps must not ride (or rebind) it
	if res := e.call("xuansi_lease", map[string]any{"board": "bench1", "holder": "agent-h1"}); res.IsError {
		t.Fatalf("lease failed: %s", resultText(t, res))
	}

	e.ts.refreshDynamic(context.Background())
	if req := wantReq(t, df.reqs); req.Type != "lease" {
		t.Fatalf("first request = %s, want the lease", req.Type)
	}
	if req := wantReq(t, df.reqs); req.Type != "list_boards" {
		t.Fatalf("refresh request = %s, want list_boards", req.Type)
	}
	for _, b := range []string{"a1b2c3", "ee0005"} { // boards cross in mac6 order
		req := wantReq(t, df.reqs)
		if req.Type != "op" || req.Op != "caps" || req.Board != b || req.Holder != "" {
			t.Errorf("caps request = %+v, want holder-less op caps for %s", req, b)
		}
	}

	// the served tool table is EXACTLY the static set — no dynamic tool
	// of any kind, for either the api1 or the api2 board
	names := listTools(t, e)
	want := append([]string(nil), staticToolNames...)
	sort.Strings(want)
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("the refresh registered tools beyond the static set: %v", names)
	}
	for _, name := range names {
		if strings.Contains(name, "_bench_") || strings.HasPrefix(name, "bench1_") || strings.HasPrefix(name, "future_") {
			t.Errorf("tool %s survived the bench dynamic retirement", name)
		}
	}

	// the caps cache still records each board's api_version — the fact
	// driver_push's --@api gate reads (the notice tool is gone; the
	// FACT stays)
	if v, ok := e.ts.dyn.capsFresh("a1b2c3"); !ok || v != 1 {
		t.Errorf("caps cache for a1b2c3 = (%d, %v), want (1, true)", v, ok)
	}
	if v, ok := e.ts.dyn.capsFresh("ee0005"); !ok || v != 2 {
		t.Errorf("caps cache for ee0005 = (%d, %v), want (2, true)", v, ok)
	}
}

// ---------------------------------------------------------------------------
// loop lifecycle

// TestDynamicRefreshLoopStops: the refresh loop runs on its ticker and
// STOPS dead when the RegisterTools stop func is called — no further
// daemon traffic, no leaked goroutine (the -race suite run covers the
// concurrency half).
func TestDynamicRefreshLoopStops(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, false)
	df.setBoards(boardEntry("a1b2c3", "bench1"))
	df.setCaps("a1b2c3", v1Caps())

	e := wireMCPClient(t, fd, Options{Holder: "mcp-fixed01", Refresh: 15 * time.Millisecond}, nil)

	// the loop refreshes at startup and on every tick
	deadline := time.Now().Add(2 * time.Second)
	for df.count("list_boards") < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("refresh loop made only %d list_boards calls in 2s", df.count("list_boards"))
		}
		time.Sleep(5 * time.Millisecond)
	}

	e.stop() // must return only after the loop goroutine exited
	settled := df.count("list_boards")
	time.Sleep(80 * time.Millisecond) // > 5 ticks
	if now := df.count("list_boards"); now != settled {
		t.Errorf("list_boards continued after stop: %d → %d (loop leaked)", settled, now)
	}
}

// TestParseDynBoardsChip (P3-T13): the board list decode carries the
// hello-cached chip — the push pairing's architecture source rides the
// same dynBoard the refresh feeds every face, honestly empty when the
// daemon has not crossed the board.
func TestParseDynBoardsChip(t *testing.T) {
	boards := parseDynBoards(map[string]any{"boards": []any{
		map[string]any{"mac6": "d75884", "alias": "bench1", "chip": "ESP32-S3"},
		map[string]any{"mac6": "aabbcc"},
	}})
	if len(boards) != 2 {
		t.Fatalf("boards = %+v, want the two entries", boards)
	}
	if boards[0].chip != "ESP32-S3" || boards[0].mac6 != "d75884" || boards[0].alias != "bench1" {
		t.Errorf("boards[0] = %+v, want the chip carried beside mac6/alias", boards[0])
	}
	if boards[1].chip != "" {
		t.Errorf("boards[1].chip = %q, want empty (no hello crossing)", boards[1].chip)
	}
}
