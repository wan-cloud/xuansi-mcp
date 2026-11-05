package mcp

// tools_dynamic_test.go — the dynamic per-board tool face (task 9):
// caps-driven expansion, naming (alias|mac6, collision suffixing),
// online/offline churn, the api_version gate's notice, and the
// synthesized eval shape (positional args mapping, bin base64 decode,
// optional-param omission) — all against a request-dependent fake
// daemon riding the task-8 harness (real unix socket, real apiclient,
// real MCP server↔client pair).

import (
	"context"
	"encoding/base64"
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
// per-board caps (or a forced refusal), eval answers a configurable
// hook. Every request it reads is counted and (optionally) published.
type dynFake struct {
	t    *testing.T
	reqs chan server.Request // nil = counting only

	mu        sync.Mutex
	boards    []map[string]any            // list_boards result entries
	caps      map[string]map[string]any   // mac6 → caps result (nil = none served)
	capsErr   map[string]*server.RPCError // mac6 → forced caps refusal
	evalErr   map[string]*server.RPCError // mac6 → forced eval refusal
	evalRes   map[string]map[string]any   // mac6 → eval result override
	onEval    func(server.Request)        // observer hook (assertions)
	counts    map[string]int              // request-type counters
	listCount int
}

func newDynFake(t *testing.T, fd *fakeDaemon, withChan bool) *dynFake {
	df := &dynFake{
		t:       t,
		caps:    map[string]map[string]any{},
		capsErr: map[string]*server.RPCError{},
		evalErr: map[string]*server.RPCError{},
		evalRes: map[string]map[string]any{},
		counts:  map[string]int{},
	}
	if withChan {
		df.reqs = make(chan server.Request, 64)
	}
	fd.serve(t, df.handle)
	return df
}

func (df *dynFake) handle(conn net.Conn) {
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
			if rerr := df.capsErr[req.Board]; rerr != nil {
				rep = &server.Reply{Type: "reply", ID: req.ID, Error: rerr}
			} else if caps := df.caps[req.Board]; caps != nil {
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: caps}
			} else {
				rep = &server.Reply{Type: "reply", ID: req.ID, Error: &server.RPCError{
					Class: "not_held",
					Msg:   "no lease on board " + req.Board + " — lease it first"}}
			}
		case "eval":
			if hook := df.onEval; hook != nil {
				hook(req)
			}
			if rerr := df.evalErr[req.Board]; rerr != nil {
				rep = &server.Reply{Type: "reply", ID: req.ID, Error: rerr}
			} else if res := df.evalRes[req.Board]; res != nil {
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: res}
			} else {
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{"out": ""}}
			}
		case "lease":
			rep = &server.Reply{Type: "reply", ID: req.ID,
				Result: map[string]any{"board": req.Board, "holder": req.Holder, "session": 1.0}}
		default:
			rep = &server.Reply{Type: "reply", ID: req.ID, Error: &server.RPCError{
				Class: "server", Msg: "dynFake: unexpected request type " + req.Type}}
		}
		df.mu.Unlock()
		if err := server.WriteMsg(conn, rep); err != nil {
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

func (df *dynFake) failCaps(mac6 string, rerr *server.RPCError) {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.capsErr[mac6] = rerr
}

func (df *dynFake) failEval(mac6 string, rerr *server.RPCError) {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.evalErr[mac6] = rerr
}

func (df *dynFake) setEvalResult(mac6 string, res map[string]any) {
	df.mu.Lock()
	defer df.mu.Unlock()
	df.evalRes[mac6] = res
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

func hasPrefixName(names []string, prefix string) int {
	n := 0
	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// expansion: count, naming, request shapes

// TestDynamicExpansionTwoBoards: one aliased board (bench1, mac6
// a1b2c3) and one bare-mac6 board (d75884) expand to 30 typed tools
// each (60 dynamic + the 11 static); naming is <alias|mac6>_<prim>_<op>;
// the refresh's daemon traffic is list_boards + per-board op caps
// HOLDER-LESS (the connection-binding-preserving choice).
func TestDynamicExpansionTwoBoards(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("a1b2c3", "bench1"), boardEntry("d75884", ""))
	df.setCaps("a1b2c3", v1Caps())
	df.setCaps("d75884", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	e.ts.refreshDynamic(context.Background())

	// daemon traffic: list_boards, then op caps per board in mac6 order
	req := wantReq(t, df.reqs)
	if req.Type != "list_boards" {
		t.Fatalf("first refresh request = %s, want list_boards", req.Type)
	}
	req = wantReq(t, df.reqs)
	if req.Type != "op" || req.Op != "caps" || req.Board != "a1b2c3" || req.Holder != "" {
		t.Errorf("caps request = %+v, want holder-less op caps for a1b2c3", req)
	}
	req = wantReq(t, df.reqs)
	if req.Type != "op" || req.Op != "caps" || req.Board != "d75884" || req.Holder != "" {
		t.Errorf("caps request = %+v, want holder-less op caps for d75884", req)
	}

	names := listTools(t, e)
	if len(names) != len(staticTools)+60 {
		t.Fatalf("served %d tools, want %d static + 60 dynamic", len(names), len(staticTools))
	}
	if hasPrefixName(names, "bench1_") != 30 || hasPrefixName(names, "d75884_") != 30 {
		t.Errorf("bench1_* = %d, d75884_* = %d, want 30 each", hasPrefixName(names, "bench1_"), hasPrefixName(names, "d75884_"))
	}
	for _, want := range []string{"bench1_gpio_write", "bench1_i2c_scan", "bench1_events_next", "d75884_i2c_scan", "d75884_gpio_mode", "d75884_rmt_rx"} {
		if !contains(names, want) {
			t.Errorf("tool %s missing from the expansion", want)
		}
	}
	// the static toolset is untouched by the dynamic face
	for _, s := range staticTools {
		if !contains(names, s) {
			t.Errorf("static tool %s displaced by the dynamic expansion", s)
		}
	}

	// a second refresh with the same world is a no-op (no churn)
	e.ts.refreshDynamic(context.Background())
	if again := listTools(t, e); !reflect.DeepEqual(again, names) {
		t.Errorf("second refresh changed the tool set: %d vs %d names", len(again), len(names))
	}
}

// TestDynamicAliasCollision: two boards sharing an alias — the first
// in mac6 order keeps the plain name, the later one is suffixed
// _<mac6>; both boards stay fully expanded.
func TestDynamicAliasCollision(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("aa0001", "bench1"), boardEntry("bb0002", "bench1"))
	df.setCaps("aa0001", v1Caps())
	df.setCaps("bb0002", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	e.ts.refreshDynamic(context.Background())
	names := listTools(t, e)

	mustHave := map[string]bool{"bench1_gpio_write": false, "bench1_gpio_write_bb0002": false,
		"bench1_i2c_scan": false, "bench1_i2c_scan_bb0002": false}
	for _, name := range names {
		mustHave[name] = true
	}
	for name, ok := range mustHave {
		if !ok {
			t.Errorf("collision-suffixed expansion misses %s", name)
		}
	}
	if hasPrefixName(names, "bench1_") != 60 {
		t.Errorf("bench1_* tools = %d, want 60 (30 plain + 30 suffixed)", hasPrefixName(names, "bench1_"))
	}
	// determinism: recompute and compare
	e.ts.refreshDynamic(context.Background())
	if again := listTools(t, e); !reflect.DeepEqual(again, names) {
		t.Error("refresh with an unchanged world churned the colliding names")
	}
}

// TestDynamicCapsFilter: the expansion is filtered by the caps the
// board actually advertises — a batch-1-shaped board (api_version 1,
// gpio only) gets exactly gpio's four tools, not the full v1 table.
func TestDynamicCapsFilter(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("cc0004", ""))
	df.setCaps("cc0004", map[string]any{
		"api_version": 1.0,
		"primitives": map[string]any{
			"gpio": map[string]any{"ops": []any{"mode", "write", "read", "on"}},
		},
	})
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	e.ts.refreshDynamic(context.Background())
	names := listTools(t, e)
	if got := hasPrefixName(names, "cc0004_"); got != 4 {
		t.Fatalf("cc0004_* tools = %d, want 4 (gpio only per caps)", got)
	}
	for _, want := range []string{"cc0004_gpio_mode", "cc0004_gpio_write", "cc0004_gpio_read", "cc0004_gpio_on"} {
		found := false
		for _, name := range names {
			if name == want {
				found = true
			}
		}
		if !found {
			t.Errorf("tool %s missing", want)
		}
	}
}

// ---------------------------------------------------------------------------
// the api_version gate

// TestDynamicAPIVersion2Notice: a board reporting api_version 2 gets
// ZERO per-primitive tools and exactly ONE notice tool explaining why;
// calling the notice answers locally (zero daemon contact beyond the
// refresh itself).
func TestDynamicAPIVersion2Notice(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("ee0005", "future"))
	df.setCaps("ee0005", map[string]any{"api_version": 2.0, "primitives": map[string]any{}})
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	e.ts.refreshDynamic(context.Background())
	wantReq(t, df.reqs) // list_boards
	wantReq(t, df.reqs) // op caps

	names := listTools(t, e)
	if got := hasPrefixName(names, "future_"); got != 1 {
		t.Fatalf("future_* tools = %d, want exactly the single notice", got)
	}
	if !contains(names, "future_bench_unsupported") {
		t.Fatalf("notice tool future_bench_unsupported missing: %v", names)
	}

	res := e.call("future_bench_unsupported", nil)
	if res.IsError {
		t.Fatalf("notice call errored: %s", resultText(t, res))
	}
	m := structured(t, res)
	if m["board"] != "ee0005" || m["api_version"] != 2.0 {
		t.Errorf("notice result = %v, want board ee0005 api_version 2", m)
	}
	if s, _ := m["notice"].(string); !strings.Contains(s, "api_version") {
		t.Errorf("notice text = %q, want it to name the api_version gate", s)
	}
	noContact(t, df.reqs) // the notice answered locally
}

func contains(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// churn

// TestDynamicBoardDisappearsAndStaleCall: a board leaving list_boards
// loses its tools on the next refresh; a call that races the refresh
// (board gone, tool still registered) surfaces the daemon's no_board
// refusal verbatim through the shared error path.
func TestDynamicBoardDisappearsAndStaleCall(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("a1b2c3", "bench1"), boardEntry("d75884", ""))
	df.setCaps("a1b2c3", v1Caps())
	df.setCaps("d75884", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	e.ts.refreshDynamic(context.Background())
	if got := hasPrefixName(listTools(t, e), "d75884_"); got != 30 {
		t.Fatalf("d75884_* = %d before churn, want 30", got)
	}

	// the stale call FIRST: board gone daemon-side, tool still live
	df.failEval("d75884", &server.RPCError{
		Class: "no_board",
		Msg:   "board d75884 — gone from the registry (no candidate shares that prefix)"})
	res := e.call("d75884_i2c_scan", map[string]any{"port": 0})
	if !res.IsError {
		t.Fatalf("stale call did not surface as a tool error: %+v", res)
	}
	want := "no_board: board d75884 — gone from the registry (no candidate shares that prefix)"
	if got := resultText(t, res); got != want {
		t.Errorf("stale-call error text = %q, want exactly %q", got, want)
	}

	// then the refresh: the vanished board's tools are withdrawn
	df.setBoards(boardEntry("a1b2c3", "bench1"))
	e.ts.refreshDynamic(context.Background())
	names := listTools(t, e)
	if got := hasPrefixName(names, "d75884_"); got != 0 {
		t.Errorf("d75884_* = %d after the board left list_boards, want 0", got)
	}
	if got := hasPrefixName(names, "bench1_"); got != 30 {
		t.Errorf("bench1_* = %d after the other board left, want 30 (untouched)", got)
	}
}

// TestDynamicCapsFailureIsSticky: a caps failure (not_held — the ops
// face's lease gate) for a board STILL PRESENT keeps its already
// expanded tools; only leaving list_boards withdraws them.
func TestDynamicCapsFailureIsSticky(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("a1b2c3", "bench1"))
	df.setCaps("a1b2c3", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	e.ts.refreshDynamic(context.Background())
	before := listTools(t, e)

	df.failCaps("a1b2c3", &server.RPCError{Class: "not_held", Msg: "no lease on board a1b2c3 — lease it first"})
	e.ts.refreshDynamic(context.Background())
	if after := listTools(t, e); !reflect.DeepEqual(after, before) {
		t.Error("a not_held caps failure churned an already-expanded board's tools (must be sticky)")
	}

	df.setBoards() // board leaves the registry entirely
	e.ts.refreshDynamic(context.Background())
	if got := hasPrefixName(listTools(t, e), "bench1_"); got != 0 {
		t.Errorf("bench1_* = %d after the board left list_boards, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// the synthesized eval

// TestDynamicSynthEvalShape: a dynamic call crosses as ONE eval with
// the fixed code string and the positional args array — params in
// signature order, bin params base64-decoded to raw bytes, optional
// params omitted shortening the array, the holder riding the
// connection identity exactly like the static tools.
func TestDynamicSynthEvalShape(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("a1b2c3", "bench1"))
	df.setCaps("a1b2c3", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})
	e.ts.refreshDynamic(context.Background())
	wantReq(t, df.reqs) // list_boards
	wantReq(t, df.reqs) // caps

	// int + bin (base64 → raw bytes); result value bridges back
	df.setEvalResult("a1b2c3", map[string]any{"out": "wrote 2", "value": 2.0})
	res := e.call("bench1_uart_write", map[string]any{"port": 2, "data_b64": base64.StdEncoding.EncodeToString([]byte{0x01, 0x02})})
	if res.IsError {
		t.Fatalf("uart_write failed: %s", resultText(t, res))
	}
	req := wantReq(t, df.reqs)
	if req.Type != "eval" || req.Board != "a1b2c3" || req.Holder != "mcp-fixed01" {
		t.Errorf("uart_write request = %+v, want eval/a1b2c3/mcp-fixed01 (board by mac6, holder like the static face)", req)
	}
	if req.Code != "return bench.uart.write(ARGS[1], ARGS[2])" {
		t.Errorf("uart_write code = %q, want the fixed synthesis", req.Code)
	}
	if want := []any{2.0, "\x01\x02"}; !reflect.DeepEqual(req.Args, want) {
		t.Errorf("uart_write args = %#v, want %#v (positional, bin decoded to raw bytes)", req.Args, want)
	}
	if m := structured(t, res); m["out"] != "wrote 2" || m["value"] != 2.0 {
		t.Errorf("uart_write result = %v, want out+value bridged", m)
	}

	// optional trailing param omitted → shorter array, code keeps all slots
	res = e.call("bench1_uart_read", map[string]any{"port": 1, "max": 10})
	if res.IsError {
		t.Fatalf("uart_read failed: %s", resultText(t, res))
	}
	req = wantReq(t, df.reqs)
	if req.Code != "return bench.uart.read(ARGS[1], ARGS[2], ARGS[3])" {
		t.Errorf("uart_read code = %q, want the full-slot synthesis", req.Code)
	}
	if want := []any{1.0, 10.0}; !reflect.DeepEqual(req.Args, want) {
		t.Errorf("uart_read args = %#v, want %#v (optional timeout_ms omitted → shorter array)", req.Args, want)
	}

	// enum + table params cross positionally as string/object
	e.call("bench1_gpio_mode", map[string]any{"pin": 2, "mode": "out"})
	req = wantReq(t, df.reqs)
	if want := []any{2.0, "out"}; !reflect.DeepEqual(req.Args, want) {
		t.Errorf("gpio_mode args = %#v, want %#v", req.Args, want)
	}
	e.call("bench1_uart_setup", map[string]any{"port": 1, "cfg": map[string]any{"baud": 115200}})
	req = wantReq(t, df.reqs)
	if want := []any{1.0, map[string]any{"baud": 115200.0}}; !reflect.DeepEqual(req.Args, want) {
		t.Errorf("uart_setup args = %#v, want %#v", req.Args, want)
	}

	// the events polling variant: fn omitted (the API's fn|nil)
	e.call("bench1_events_on", map[string]any{"source": "uart1.rx"})
	req = wantReq(t, df.reqs)
	if req.Code != "return bench.events.on(ARGS[1], ARGS[2])" {
		t.Errorf("events_on code = %q", req.Code)
	}
	if want := []any{"uart1.rx"}; !reflect.DeepEqual(req.Args, want) {
		t.Errorf("events_on args = %#v, want the single-source array (fn omitted → polling variant)", req.Args)
	}
}

// TestDynamicLocalRefusals: a missing required param and an
// undecodable bin are refused CLIENT-SIDE (api class, no daemon
// contact) — the positional mapping must never silently shift.
func TestDynamicLocalRefusals(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("a1b2c3", "bench1"))
	df.setCaps("a1b2c3", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})
	e.ts.refreshDynamic(context.Background())
	wantReq(t, df.reqs) // list_boards
	wantReq(t, df.reqs) // caps

	res := e.call("bench1_gpio_write", map[string]any{"pin": 2})
	if !res.IsError {
		t.Fatalf("missing required param did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); !strings.HasPrefix(got, "api: bench1_gpio_write: missing required parameter") || !strings.Contains(got, "level") {
		t.Errorf("missing-param error = %q, want api class naming level", got)
	}

	res = e.call("bench1_uart_write", map[string]any{"port": 1, "data_b64": "!!not-base64!!"})
	if !res.IsError {
		t.Fatalf("undecodable bin did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "api: bench1_uart_write") || !strings.Contains(got, "base64") {
		t.Errorf("bad-base64 error = %q, want api class naming base64", got)
	}
	noContact(t, df.reqs) // both refusals were local
}

// TestDynamicHolderRidesTheBinding: after an explicit holder binds the
// daemon connection (the grace-adopt entry), dynamic calls ride the
// BOUND holder, and the refresh's caps stays holder-less.
func TestDynamicHolderRidesTheBinding(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, true)
	df.setBoards(boardEntry("a1b2c3", "bench1"))
	df.setCaps("a1b2c3", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	if res := e.call("xuansi_lease", map[string]any{"board": "bench1", "holder": "agent-h1"}); res.IsError {
		t.Fatalf("lease failed: %s", resultText(t, res))
	}
	e.ts.refreshDynamic(context.Background())
	if req := wantReq(t, df.reqs); req.Holder != "agent-h1" { // the lease
		t.Fatalf("lease holder = %q", req.Holder)
	}
	wantReq(t, df.reqs)                               // list_boards
	if req := wantReq(t, df.reqs); req.Holder != "" { // caps — must stay holder-less
		t.Errorf("refresh caps carried holder %q — it must ride the connection binding instead", req.Holder)
	}

	res := e.call("bench1_gpio_write", map[string]any{"pin": 2, "level": 1})
	if res.IsError {
		t.Fatalf("dynamic call failed: %s", resultText(t, res))
	}
	if req := wantReq(t, df.reqs); req.Holder != "agent-h1" {
		t.Errorf("dynamic call holder = %q, want the bound agent-h1 (exactly like the static face)", req.Holder)
	}
}

// TestDynamicCapsVersionFlipMidSession: a board that STAYS in
// list_boards while its caps changes story must have its toolset
// follow the caps — api_version 1→2 withdraws all 30 prim tools
// leaving exactly the notice, 2→1 restores them, and a primitive
// shrink under the same version withdraws the dropped prims' tools.
// Review round 1: the withdraw switch only fired on board-absent or
// changed-id, so a successful-but-smaller caps kept the stale tools
// alongside the notice.
func TestDynamicCapsVersionFlipMidSession(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, false)
	df.setBoards(boardEntry("a1b2c3", "bench1"))
	df.setCaps("a1b2c3", v1Caps())
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01"})

	e.ts.refreshDynamic(context.Background())
	names := listTools(t, e)
	if got := hasPrefixName(names, "bench1_"); got != 30 {
		t.Fatalf("bench1_* = %d after v1 caps, want 30", got)
	}
	if contains(names, "bench1_bench_unsupported") {
		t.Fatal("notice present under api_version 1")
	}

	// the board never leaves list_boards; its caps flips to v2
	df.setCaps("a1b2c3", map[string]any{"api_version": 2.0, "primitives": map[string]any{}})
	e.ts.refreshDynamic(context.Background())
	if names = listTools(t, e); hasPrefixName(names, "bench1_") != 1 || !contains(names, "bench1_bench_unsupported") {
		t.Fatalf("after 1→2: bench1_* = %d (%v), want exactly the notice — stale prim tools must withdraw", hasPrefixName(names, "bench1_"), names)
	}

	// and back: 2→1 restores the 30 typed tools, notice gone
	df.setCaps("a1b2c3", v1Caps())
	e.ts.refreshDynamic(context.Background())
	if names = listTools(t, e); contains(names, "bench1_bench_unsupported") || hasPrefixName(names, "bench1_") != 30 {
		t.Fatalf("after 2→1: bench1_* = %d (notice present: %v), want the 30 prim tools back", hasPrefixName(names, "bench1_"), contains(names, "bench1_bench_unsupported"))
	}

	// a primitive shrink without a version change withdraws the
	// dropped prims' tools too (gpio-only caps → gpio-only tools)
	df.setCaps("a1b2c3", map[string]any{
		"api_version": 1.0,
		"primitives": map[string]any{
			"gpio": map[string]any{"ops": []any{"mode", "write", "read", "on"}},
		},
	})
	e.ts.refreshDynamic(context.Background())
	if names = listTools(t, e); hasPrefixName(names, "bench1_") != 4 {
		t.Fatalf("after caps shrink: bench1_* = %d, want 4 (gpio only)", hasPrefixName(names, "bench1_"))
	}
}

// ---------------------------------------------------------------------------
// notification + loop lifecycle

// TestDynamicToolsListChangedNotification: expansion and withdrawal
// both notify tools/list_changed (the go-sdk's built-in session
// broadcast, riding the server's advertised ListChanged capability).
func TestDynamicToolsListChangedNotification(t *testing.T) {
	fd := newFakeDaemon(t)
	df := newDynFake(t, fd, false)
	df.setBoards(boardEntry("a1b2c3", "bench1"))
	df.setCaps("a1b2c3", v1Caps())

	notif := make(chan struct{}, 4)
	e := wireMCPClient(t, fd, Options{Holder: "mcp-fixed01"}, &mcpsdk.ClientOptions{
		ToolListChangedHandler: func(context.Context, *mcpsdk.ToolListChangedRequest) {
			select {
			case notif <- struct{}{}:
			default:
			}
		},
	})

	e.ts.refreshDynamic(context.Background())
	select {
	case <-notif:
	case <-time.After(2 * time.Second):
		t.Fatal("expansion did not notify tools/list_changed")
	}

	df.setBoards()
	e.ts.refreshDynamic(context.Background())
	select {
	case <-notif:
	case <-time.After(2 * time.Second):
		t.Fatal("withdrawal did not notify tools/list_changed")
	}
}

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
