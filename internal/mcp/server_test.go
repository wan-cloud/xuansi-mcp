package mcp

// server_test.go — the static tool face tested end-to-end: a fake
// daemon over a real unix socket (the internal/apiclient harness
// pattern), a real apiclient.Client, and a REAL MCP server↔client pair
// over the go-sdk's in-memory transport — so every test exercises the
// exact wiring `xuansi mcp` serves: SDK schema inference, typed
// handlers, tool-error packing. The fake daemon asserts the
// server.Request each tool actually sends.

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/apiclient"
	"xuansi/internal/server"
	"xuansi/internal/testutil"
)

// ---------------------------------------------------------------------------
// fake daemon harness (mirrors internal/apiclient/client_test.go)

// fakeDaemon is a scriptable stand-in daemon: one accepted connection
// per test, handed to a handler closure speaking the framed-JSON
// contract (internal/server/proto.go).
type fakeDaemon struct {
	ln   net.Listener
	path string
	wg   sync.WaitGroup
}

func newFakeDaemon(t *testing.T) *fakeDaemon {
	t.Helper()
	path := filepath.Join(testutil.ShortSockDir(t), "d.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("fake daemon listen: %v", err)
	}
	fd := &fakeDaemon{ln: ln, path: path}
	t.Cleanup(func() { ln.Close(); fd.wg.Wait() })
	return fd
}

// serve accepts exactly one connection and runs h on it.
func (fd *fakeDaemon) serve(t *testing.T, h func(conn net.Conn)) {
	t.Helper()
	fd.wg.Add(1)
	go func() {
		defer fd.wg.Done()
		conn, err := fd.ln.Accept()
		if err != nil {
			return
		}
		fd.wg.Add(1)
		go func() {
			defer fd.wg.Done()
			defer conn.Close()
			h(conn)
		}()
	}()
}

// step is one scripted reply: a result, a structured refusal, or an
// abrupt connection death (no reply at all).
type step struct {
	result map[string]any
	rerr   *server.RPCError
	death  bool
}

// serveScript answers each request from the script in order and
// publishes every request it reads on the returned channel (buffered;
// under-receiving surfaces as a timeout, over-receiving as an
// "unexpected request" error — the zero-contact proof rides on that).
func (fd *fakeDaemon) serveScript(t *testing.T, script []step) chan server.Request {
	t.Helper()
	reqs := make(chan server.Request, 16)
	fd.serve(t, func(conn net.Conn) {
		for i := 0; ; i++ {
			var req server.Request
			if err := server.ReadMsg(conn, &req); err != nil {
				return // client close / broken pipe: expected shutdown path
			}
			select {
			case reqs <- req:
			default:
				t.Errorf("fake daemon: unexpected extra request %+v", req)
			}
			if i >= len(script) {
				t.Errorf("fake daemon: request %d (%s) beyond the %d-step script", i, req.Type, len(script))
				return
			}
			st := script[i]
			if st.death {
				conn.Close() // abrupt death, no reply
				return
			}
			rep := &server.Reply{Type: "reply", ID: req.ID, Result: st.result}
			if st.rerr != nil {
				rep = &server.Reply{Type: "reply", ID: req.ID, Error: st.rerr}
			}
			if err := server.WriteMsg(conn, rep); err != nil {
				t.Errorf("fake daemon reply: %v", err)
			}
		}
	})
	return reqs
}

// ---------------------------------------------------------------------------
// MCP environment: fake daemon ← apiclient ← MCP server ⇄ MCP client

// env is one wired test environment over the in-memory MCP transport.
type env struct {
	t    *testing.T
	c    *apiclient.Client
	ts   *toolset // the registered toolset (tests drive its dynamic face)
	stop func()   // the dynamic refresh loop's stop func (no-op when off)
	ss   *mcpsdk.ServerSession
	cl   *mcpsdk.ClientSession
}

// wireMCPClient wires the MCP half (apiclient ← server ⇄ in-memory
// client) against a caller-owned fake daemon, with caller-supplied
// client options (e.g. a ToolListChangedHandler). The toolset is built
// explicitly (not via RegisterTools) so tests hold it for direct
// refreshDynamic drives; wireMCP is the nil-client-opts convenience.
func wireMCPClient(t *testing.T, fd *fakeDaemon, opts Options, copts *mcpsdk.ClientOptions) *env {
	t.Helper()
	c, err := apiclient.Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(&mcpsdk.Implementation{Name: "xuansi", Version: "test"})
	ts := &toolset{c: c, holders: newHolderState(opts.Holder), ws: newWSState(opts.WorkspaceRoot), logf: opts.Logf}
	ts.register(srv)
	ts.dyn = newDynState(srv)
	stop := ts.startDynamic(srv, opts.Refresh)

	ctx := context.Background()
	ct, st := mcpsdk.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0"}, copts).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, c: c, ts: ts, stop: stop, ss: ss, cl: cl}
	t.Cleanup(func() {
		stop()          // the refresh loop goes down before the connection it rides
		ts.stopEvents() // the event face's feed + notifier (no-op for the loop's own stop)
		cl.Close()
		ss.Close()
		c.Close()
	})
	return e
}

// wireMCP wires the MCP half against a caller-owned fake daemon — for
// scripted daemons beyond serveScript's fixed reply queue (e.g.
// request-dependent behavior).
func wireMCP(t *testing.T, fd *fakeDaemon, opts Options) *env {
	return wireMCPClient(t, fd, opts, nil)
}

// newEnv wires a full environment against a scripted fake daemon.
func newEnv(t *testing.T, script []step, opts Options) (*env, chan server.Request) {
	t.Helper()
	fd := newFakeDaemon(t)
	reqs := fd.serveScript(t, script)
	return wireMCP(t, fd, opts), reqs
}

// call invokes one tool; tool errors (IsError results) are part of the
// contract, so they return as results rather than failing here.
func (e *env) call(name string, args map[string]any) *mcpsdk.CallToolResult {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := e.cl.CallTool(ctx, &mcpsdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		e.t.Fatalf("call %s: %v", name, err)
	}
	return res
}

// callAsync invokes one tool on its own goroutine — concurrent-call
// scenarios, observed under a timeout via waitResult.
func (e *env) callAsync(name string, args map[string]any) chan *mcpsdk.CallToolResult {
	out := make(chan *mcpsdk.CallToolResult, 1)
	go func() { out <- e.call(name, args) }()
	return out
}

// waitResult receives one async tool result within 5s or fails.
func waitResult(t *testing.T, ch <-chan *mcpsdk.CallToolResult, what string) *mcpsdk.CallToolResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// resultText returns the first text content of a tool result.
func resultText(t *testing.T, res *mcpsdk.CallToolResult) string {
	t.Helper()
	for _, c := range res.Content {
		if tc, ok := c.(*mcpsdk.TextContent); ok {
			return tc.Text
		}
	}
	t.Fatalf("result carries no text content: %+v", res.Content)
	return ""
}

// structured returns the structured content as a JSON object map.
func structured(t *testing.T, res *mcpsdk.CallToolResult) map[string]any {
	t.Helper()
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content = %#v, want a JSON object", res.StructuredContent)
	}
	return m
}

// wantReq receives the next request the daemon saw, or fails.
func wantReq(t *testing.T, reqs chan server.Request) server.Request {
	t.Helper()
	select {
	case r := <-reqs:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the daemon to receive a request")
		return server.Request{}
	}
}

// noContact asserts the daemon saw nothing — the zero-contact proof
// for client-side refusals (bounded quiet window, apiclient-suite
// style).
func noContact(t *testing.T, reqs chan server.Request) {
	t.Helper()
	select {
	case r := <-reqs:
		t.Fatalf("daemon was contacted despite a client-side refusal: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
}

// staticTools is GONE (B8 item 7): the test-side hand copy of the
// static tool list drifted every time a face extended the contract
// (its count prose said 17 when the list was 20). Both pins —
// TestRegistersExactStaticToolset here and TestStaticToolNamesSingleSource
// (tools_static_test.go) — now derive from the PRODUCTION
// staticToolNames (tools_static.go), the same lockstep-pinned list the
// register calls keep; adding a tool without the list fails loudly.

// ---------------------------------------------------------------------------
// registration, holder identity, error mapping

// TestRegistersExactStaticToolset: the served tool list is exactly the
// production staticToolNames list (task 8's daemon-surface set, since
// extended by task 10's events_recent, task 11's workspace tools, task
// 12's capture, B8's fs_rm/driver_pull/session_reset — the count
// drifts BY DESIGN as each face extends the same static contract, so
// the pin is the shared list, never a hand-copied count: drift fails
// here and again in TestStaticToolNamesSingleSource), each with a
// non-empty agent-facing description, and the inferred schemas mark
// the required/optional split the handlers rely on (board required,
// holder/transport optional).
func TestRegistersExactStaticToolset(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	desc := map[string]string{}
	for _, tool := range lr.Tools {
		got = append(got, tool.Name)
		desc[tool.Name] = tool.Description
		if tool.Description == "" {
			t.Errorf("tool %s has an empty description", tool.Name)
		}
	}
	sort.Strings(got)
	want := append([]string(nil), staticToolNames...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("served tools = %v, want exactly %v", got, want)
	}

	byName := map[string]map[string]any{}
	for _, tool := range lr.Tools {
		m, ok := tool.InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("tool %s input schema = %#v, want a JSON object", tool.Name, tool.InputSchema)
		}
		byName[tool.Name] = m
	}
	required := func(schema map[string]any) map[string]bool {
		out := map[string]bool{}
		if r, ok := schema["required"].([]any); ok {
			for _, x := range r {
				if s, ok := x.(string); ok {
					out[s] = true
				}
			}
		}
		return out
	}
	if r := required(byName["xuansi_board_status"]); !r["board"] || r["holder"] {
		t.Errorf("board_status required = %v, want board required and no holder field", r)
	}
	if r := required(byName["xuansi_lease"]); !r["board"] || r["holder"] || r["transport"] {
		t.Errorf("lease required = %v, want only board (holder/transport optional)", r)
	}
	if r := required(byName["xuansi_run_lua"]); !r["board"] || !r["code"] || r["args"] || r["holder"] {
		t.Errorf("run_lua required = %v, want board+code only", r)
	}
	if r := required(byName["xuansi_events_recent"]); !r["board"] || r["source_prefix"] || r["limit"] {
		t.Errorf("events_recent required = %v, want only board (source_prefix/limit optional)", r)
	}
	if r := required(byName["xuansi_ws_put"]); !r["name"] || !r["version"] || !r["content"] {
		t.Errorf("ws_put required = %v, want name+version+content", r)
	}
	if r := required(byName["xuansi_driver_push"]); !r["board"] || !r["name"] || r["version"] || r["holder"] {
		t.Errorf("driver_push required = %v, want board+name only (version/holder optional)", r)
	}
	// the polling tool carries the events face's arming precondition
	// (final review): gpio.N/uartN.rx/stdout/net flow only after the
	// source is armed board-side, and xuansi_capture is this face's
	// in-contract arming path.
	if d := desc["xuansi_events_recent"]; !strings.Contains(d, "armed") || !strings.Contains(d, "xuansi_capture") {
		t.Errorf("events_recent description = %q, want the arming precondition naming xuansi_capture", d)
	}
}

// TestStaticToolsDeCamerad (B12-P2 T6): the camera face is retired —
// tools/list serves exactly 26 static tools (24 since the retirement;
// B15 W1.T2 appended the ⑨ pin pair) and none of them is a camera
// tool. This is the seat-facing pin (an agent reads tools/list, not Go
// tables): a camera tool reappearing fails here even if
// staticToolNames were internally consistent with it.
func TestStaticToolsDeCamerad(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	if len(lr.Tools) != 26 {
		names := make([]string, len(lr.Tools))
		for i, tool := range lr.Tools {
			names[i] = tool.Name
		}
		t.Fatalf("tools/list serves %d tools, want exactly 26: %v", len(lr.Tools), names)
	}
	for _, tool := range lr.Tools {
		if strings.Contains(tool.Name, "camera") {
			t.Errorf("tool %s survived the camera retirement", tool.Name)
		}
	}
}

// TestListBoardsBootIdentity (B12-P2 T6): the daemon's boot-identity
// fields cross the MCP mapping — a list_boards entry carrying
// boot_epoch/reset_reason/last_seen answers with all three (B12: the
// agent seat could not see a board's boot identity at all).
func TestListBoardsBootIdentity(t *testing.T) {
	e, _ := newEnv(t, []step{{result: map[string]any{
		"boards": []any{map[string]any{
			"mac6": "d75884", "alias": "bench1",
			"sources": []any{"usb", "mdns"}, "host": "xuansi-d75884.local",
			"port": 7777.0, "fw": "dcaa9ae", "fw_version": "0.2.0",
			"boot_epoch":   8.0,
			"reset_reason": "usb",
			"last_seen":    "2026-11-09T12:34:56Z",
		}},
	}}}, Options{})
	res := e.call("xuansi_list_boards", map[string]any{})
	if res.IsError {
		t.Fatalf("list_boards failed: %s", resultText(t, res))
	}
	m := structured(t, res)
	boards, _ := m["boards"].([]any)
	if len(boards) != 1 {
		t.Fatalf("boards = %#v, want the one board", m["boards"])
	}
	b0, _ := boards[0].(map[string]any)
	if b0["boot_epoch"] != 8.0 {
		t.Errorf("boot_epoch = %#v, want 8", b0["boot_epoch"])
	}
	if b0["reset_reason"] != "usb" {
		t.Errorf("reset_reason = %#v, want usb", b0["reset_reason"])
	}
	if b0["last_seen"] != "2026-11-09T12:34:56Z" {
		t.Errorf("last_seen = %#v, want the daemon's RFC3339 crossing time", b0["last_seen"])
	}
}

// TestListBoardsChipColumn (P3-T13): the daemon board list's chip
// column (hello-cached) crosses the mapping — the pairing decision's
// architecture source must be VISIBLE at the seat, honestly absent
// before the first hello crossing.
func TestListBoardsChipColumn(t *testing.T) {
	e, _ := newEnv(t, []step{{result: map[string]any{
		"boards": []any{
			map[string]any{
				"mac6": "d75884", "alias": "bench1",
				"sources": []any{"usb"}, "chip": "ESP32-S3", "fw": "dcaa9ae",
			},
			map[string]any{"mac6": "aabbcc", "sources": []any{"mdns"}},
		},
	}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_list_boards", nil)
	if res.IsError {
		t.Fatalf("list_boards failed: %s", resultText(t, res))
	}
	m := structured(t, res)
	boards, _ := m["boards"].([]any)
	if len(boards) != 2 {
		t.Fatalf("boards = %#v, want the two-board list", m["boards"])
	}
	b0, _ := boards[0].(map[string]any)
	if b0["chip"] != "ESP32-S3" {
		t.Errorf("board[0].chip = %#v, want ESP32-S3 (the hello-cached chip)", b0["chip"])
	}
	b1, _ := boards[1].(map[string]any)
	if _, ok := b1["chip"]; ok {
		t.Errorf("board[1].chip = %#v, want honest absence (no hello crossing yet)", b1["chip"])
	}
}

// TestDefaultHolderFormat: a zero Options generates a per-process
// holder "mcp-<8 hex>" — short, unique per process.
func TestDefaultHolderFormat(t *testing.T) {
	re := regexp.MustCompile(`^mcp-[0-9a-f]{8}$`)
	h1 := newHolderState("")
	if !re.MatchString(h1.def) {
		t.Errorf("generated holder %q does not match mcp-<8 hex>", h1.def)
	}
	h2 := newHolderState("")
	if h1.def == h2.def {
		t.Errorf("two generated holders collided: %s", h1.def)
	}
	if got := (newHolderState("pinned")).def; got != "pinned" {
		t.Errorf("pinned holder = %q, want \"pinned\"", got)
	}
}

// TestHolderEffectiveAndSettle: the effective-holder ladder (explicit
// > bound > default) and the settle mirror of the daemon's per-
// connection binding (first holder binds; a refused conflicting holder
// leaves it untouched).
func TestHolderEffectiveAndSettle(t *testing.T) {
	h := newHolderState("mcp-dflt")
	if got := h.effective(""); got != "mcp-dflt" {
		t.Errorf("effective(empty) = %q, want the default", got)
	}
	if got := h.effective("agent-1"); got != "agent-1" {
		t.Errorf("effective(explicit) = %q, want the explicit holder", got)
	}
	h.settle("mcp-dflt") // first reply binds the default
	if got := h.effective(""); got != "mcp-dflt" {
		t.Errorf("effective after settle = %q, want mcp-dflt", got)
	}
	h.settle("agent-1") // a conflicting holder was refused daemon-side: binding unchanged
	if got := h.effective(""); got != "mcp-dflt" {
		t.Errorf("effective after conflicting settle = %q, want the unchanged binding mcp-dflt", got)
	}

	fresh := newHolderState("mcp-dflt")
	fresh.settle("agent-1") // the FIRST holder the daemon saw binds
	if got := fresh.effective(""); got != "agent-1" {
		t.Errorf("effective after first-settle = %q, want agent-1", got)
	}
}

// TestHolderRefusalNeverSettles (review round 1, unit pin): a
// class-"holder" refusal means the daemon ALREADY had a different
// binding and never bound the sent holder — callHeld must not settle
// the mirror on it. Pre-fix, this pins the loser as the process
// identity while the mirror is still empty.
func TestHolderRefusalNeverSettles(t *testing.T) {
	fd := newFakeDaemon(t)
	fd.serveScript(t, []step{{rerr: &server.RPCError{
		Class: "holder",
		Msg:   `connection bound to holder "agent-x", request says "agent-y"`,
	}}})
	c, err := apiclient.Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ts := &toolset{c: c, holders: newHolderState("mcp-dflt")}
	_, callErr := ts.callHeld(context.Background(), server.Request{Type: "lease", Board: "d75884", Holder: "agent-y"})
	var rerr *server.RPCError
	if !errors.As(callErr, &rerr) || rerr.Class != "holder" {
		t.Fatalf("callHeld error = %v, want the holder-class refusal", callErr)
	}
	if got := ts.holders.effective(""); got != "mcp-dflt" {
		t.Errorf("effective after a holder refusal = %q, want the untouched default mcp-dflt (refused holders never settle)", got)
	}
}

// TestConcurrentFirstHoldersRideTheWinner (review round 1, the race
// driven end-to-end): two concurrent first calls with different
// holders — the daemon binds whichever lands first (mirroring its
// bindHolder: first holder binds, different ones refused) and refuses
// the other. Whatever order the replies PROCESS client-side, a later
// default-holder call must ride the WINNER — the daemon's actual
// binding. Pre-fix, the loser's refusal processing before the
// winner's success poisoned the mirror with the loser's holder, and
// every subsequent default call failed with class "holder" until
// process restart; post-fix every interleaving converges on the
// winner, so this test is deterministic.
func TestConcurrentFirstHoldersRideTheWinner(t *testing.T) {
	fd := newFakeDaemon(t)
	reqs := make(chan server.Request, 8)
	fd.serve(t, func(conn net.Conn) {
		var bound string // the daemon's binding: the first holder it saw
		for {
			var req server.Request
			if err := server.ReadMsg(conn, &req); err != nil {
				return // client close: expected shutdown path
			}
			select {
			case reqs <- req:
			default:
				t.Errorf("fake daemon: unexpected extra request %+v", req)
			}
			var rep *server.Reply
			switch {
			case bound == "":
				bound = req.Holder // bindHolder: the first holder binds
				rep = &server.Reply{Type: "reply", ID: req.ID,
					Result: map[string]any{"board": "d75884", "holder": bound, "session": 1.0}}
			case req.Holder != bound:
				rep = &server.Reply{Type: "reply", ID: req.ID,
					Error: &server.RPCError{Class: "holder",
						Msg: fmt.Sprintf("connection bound to holder %q, request says %q", bound, req.Holder)}}
			default:
				rep = &server.Reply{Type: "reply", ID: req.ID,
					Result: map[string]any{"board": "d75884", "holder": bound}}
			}
			if err := server.WriteMsg(conn, rep); err != nil {
				t.Errorf("fake daemon reply: %v", err)
				return
			}
		}
	})
	e := wireMCP(t, fd, Options{Holder: "mcp-dflt"})

	outX := e.callAsync("xuansi_lease", map[string]any{"board": "d75884", "holder": "agent-x"})
	outY := e.callAsync("xuansi_lease", map[string]any{"board": "d75884", "holder": "agent-y"})
	resX := waitResult(t, outX, "the agent-x lease result")
	resY := waitResult(t, outY, "the agent-y lease result")

	winner, refusals := "", 0
	for _, res := range []*mcpsdk.CallToolResult{resX, resY} {
		if res.IsError {
			refusals++
			if got := resultText(t, res); !strings.HasPrefix(got, "holder: connection bound to holder") {
				t.Errorf("refusal text = %q, want the daemon's holder-class refusal", got)
			}
			continue
		}
		if h, _ := structured(t, res)["holder"].(string); h != "" {
			winner = h
		}
	}
	if refusals != 1 || winner == "" {
		t.Fatalf("want exactly one lease success + one holder refusal, got winner=%q refusals=%d", winner, refusals)
	}

	// the regression pin: the later DEFAULT-holder call rides the
	// winner (the daemon's actual binding) — under the poisoned mirror
	// it carried the refused loser and drew a second holder-class
	// refusal
	res := e.call("xuansi_release", map[string]any{"board": "d75884"})
	if res.IsError {
		t.Fatalf("post-race release failed (mirror poisoned?): %s", resultText(t, res))
	}
	wantReq(t, reqs) // lease winner
	wantReq(t, reqs) // lease loser
	if req := wantReq(t, reqs); req.Holder != winner {
		t.Errorf("post-race release holder = %q, want the winner %q", req.Holder, winner)
	}
}

// TestLeaseUSBWarningVerbatim: the USB-transport lease carries the
// §4.4 fields — transport, rebooted, and the daemon's reboot
// disclosure crossing into the result verbatim.
func TestLeaseUSBWarningVerbatim(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"board": "d75884", "holder": "mcp-fixed01", "session": 7.0,
		"transport": "usb", "rebooted": true, "transport_note": server.USBTransportNote,
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_lease", map[string]any{"board": "d75884", "transport": "usb"})
	if res.IsError {
		t.Fatalf("lease failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "lease" || req.Board != "d75884" || req.Transport != "usb" || req.Holder != "mcp-fixed01" {
		t.Errorf("lease request = %+v, want lease/d75884/usb/mcp-fixed01", req)
	}
	m := structured(t, res)
	if m["transport_note"] != server.USBTransportNote {
		t.Errorf("transport_note = %v, want the daemon's verbatim reboot disclosure", m["transport_note"])
	}
	if m["rebooted"] != true {
		t.Errorf("rebooted = %v, want true", m["rebooted"])
	}
	if m["transport"] != "usb" || m["session"] != 7.0 {
		t.Errorf("lease result = %v, want transport usb session 7", m)
	}
}

// TestLeaseHolderOverrideReachesDaemon: an explicit holder input wins
// over the process default; omitted fields (transport) stay empty.
func TestLeaseHolderOverrideReachesDaemon(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"board": "d75884", "holder": "agent-s1", "session": 2.0,
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_lease", map[string]any{"board": "d75884", "holder": "agent-s1"})
	if res.IsError {
		t.Fatalf("lease failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Holder != "agent-s1" {
		t.Errorf("holder reached daemon as %q, want the explicit agent-s1", req.Holder)
	}
	if req.Transport != "" {
		t.Errorf("transport = %q, want empty when omitted", req.Transport)
	}
	if m := structured(t, res); m["holder"] != "agent-s1" {
		t.Errorf("holder in result = %v, want agent-s1", m["holder"])
	}
}

// TestHolderOverrideSticksForLaterCalls: after an explicit holder
// binds the connection (the grace-adopt entry move), plain later calls
// ride the BOUND holder — not the process default, which the daemon
// would refuse with class "holder".
func TestHolderOverrideSticksForLaterCalls(t *testing.T) {
	e, reqs := newEnv(t, []step{
		{result: map[string]any{"board": "d75884", "holder": "agent-s1", "session": 2.0}},
		{result: map[string]any{"out": "ok"}},
	}, Options{Holder: "mcp-fixed01"})

	if res := e.call("xuansi_lease", map[string]any{"board": "d75884", "holder": "agent-s1"}); res.IsError {
		t.Fatalf("lease failed: %s", resultText(t, res))
	}
	wantReq(t, reqs) // the lease

	if res := e.call("xuansi_run_lua", map[string]any{"board": "d75884", "code": "return 1"}); res.IsError {
		t.Fatalf("run_lua after holder override failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "eval" || req.Holder != "agent-s1" {
		t.Errorf("post-override eval request = %+v, want holder agent-s1 (the bound holder)", req)
	}
}

// TestHeldErrorTextVerbatim: a daemon refusal crosses as a tool error
// whose text is EXACTLY "<class>: <msg>" — no wrapping, class intact.
func TestHeldErrorTextVerbatim(t *testing.T) {
	msg := "board d75884 leased by e1 since 2026-11-05T00:00:00Z (grace until 00:02:00Z; re-supply holder e1 to adopt)"
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{Class: "held", Msg: msg, Holder: "e1"}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_lease", map[string]any{"board": "d75884"})
	if !res.IsError {
		t.Fatalf("held lease did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); got != "held: "+msg {
		t.Errorf("error text = %q, want exactly %q", got, "held: "+msg)
	}
}

// TestBoardErrorTextVerbatim: a board-side class (with its byte
// suffix) crosses verbatim too — the class-transparent promise.
func TestBoardErrorTextVerbatim(t *testing.T) {
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{
		Class: "rt", Msg: "attempt to index a nil value at byte 42",
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_run_lua", map[string]any{"board": "d75884", "code": "x()"})
	if !res.IsError {
		t.Fatalf("board rt error did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); got != "rt: attempt to index a nil value at byte 42" {
		t.Errorf("error text = %q, want the verbatim board line", got)
	}
}

// TestDaemonDeathIsToolErrorNamingConnectionLoss: a dead daemon
// (transport-level failure, not a refusal) still surfaces as a tool
// error, worded so an agent can tell connection loss from refusal.
func TestDaemonDeathIsToolErrorNamingConnectionLoss(t *testing.T) {
	e, _ := newEnv(t, []step{{death: true}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_list_boards", nil)
	if !res.IsError {
		t.Fatalf("daemon death did not surface as a tool error: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "daemon connection lost") {
		t.Errorf("error text = %q, want it to name daemon connection loss", got)
	}
}

// TestFsWriteAboveCapNowCrosses: a decoded payload above the board's
// 16384B single-op cap CROSSES to the daemon whole (B10-T1: the former
// client-side refusal is gone — the daemon chunks it onto the board);
// exactly 16384B still crosses too.
func TestFsWriteAboveCapNowCrosses(t *testing.T) {
	for _, n := range []int{16384, 16385, 40000} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte('a' + i%26)
		}
		e, reqs := newEnv(t, []step{{result: map[string]any{"size": float64(n)}}}, Options{Holder: "mcp-fixed01"})

		res := e.call("xuansi_fs_write", map[string]any{
			"board": "d75884", "path": "/big.bin",
			"data_b64": base64Std(t, data),
		})
		if res.IsError {
			t.Fatalf("n=%d: fs_write failed: %s", n, resultText(t, res))
		}
		req := wantReq(t, reqs)
		if req.Type != "fs_write" || len(req.Data) != n {
			t.Fatalf("n=%d: daemon saw %d bytes (type %s), want the whole payload", n, len(req.Data), req.Type)
		}
	}
}

// TestFsWriteInvalidBase64RefusedLocally: a data_b64 that does not
// decode is a local api-class refusal — no daemon contact.
func TestFsWriteInvalidBase64RefusedLocally(t *testing.T) {
	e, reqs := newEnv(t, nil, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_fs_write", map[string]any{
		"board": "d75884", "path": "/f.txt", "data_b64": "!!not-base64!!",
	})
	if !res.IsError {
		t.Fatalf("undecodable fs_write did not surface as a tool error: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api: fs_write /f.txt", "base64"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	noContact(t, reqs)
}

// ---------------------------------------------------------------------------
// budget_ms domain validation + frames decode on the static eval face
// (B12-P2 T5): out-of-domain budgets are refused client-side with zero
// daemon contact; valid budgets forward on server.Request.BudgetMs;
// eval replies' frames/frames_dropped decode into the typed out.
// ---------------------------------------------------------------------------

// TestRunLuaBudgetMsDomainAndForward: budget_ms is validated on the MCP
// face (1..30000, the board's own wire domain; anything else —
// negative, over-domain — is refused BEFORE any daemon contact, so the
// int-wrap corner T4's review flagged can never reach the wire); valid
// budgets forward on the request; omitted/0 rides as 0 (the session
// budget, key absent).
func TestRunLuaBudgetMsDomainAndForward(t *testing.T) {
	e, reqs := newEnv(t, []step{
		{result: map[string]any{"out": "ok"}}, // budget_ms 8000
		{result: map[string]any{"out": "ok"}}, // budget_ms 30000 (domain edge)
		{result: map[string]any{"out": "ok"}}, // omitted
		{result: map[string]any{"out": "ok"}}, // budget_ms 0 (rides omitted)
	}, Options{})

	for _, bad := range []float64{-1, 30001, 1 << 40} {
		res := e.call("xuansi_run_lua", map[string]any{"board": "d75884", "code": "return 1", "budget_ms": bad})
		if !res.IsError {
			t.Fatalf("budget_ms %v: accepted, want a refusal", bad)
		}
		if txt := resultText(t, res); !strings.Contains(txt, "budget_ms") || !strings.Contains(txt, "1..30000") {
			t.Fatalf("budget_ms %v refusal = %q, want it naming budget_ms and the 1..30000 domain", bad, txt)
		}
	}
	select {
	case r := <-reqs:
		t.Fatalf("an out-of-domain budget reached the daemon: %+v", r)
	case <-time.After(150 * time.Millisecond):
	}

	for _, ms := range []int64{8000, 30000, 0, 0} {
		args := map[string]any{"board": "d75884", "code": "return 1"}
		if ms != 0 {
			args["budget_ms"] = float64(ms)
		}
		res := e.call("xuansi_run_lua", args)
		if res.IsError {
			t.Fatalf("budget_ms %d: %s", ms, resultText(t, res))
		}
		req := wantReq(t, reqs)
		if req.Type != "eval" || req.BudgetMs != ms {
			t.Fatalf("daemon request = %s budget %d, want eval budget %d", req.Type, req.BudgetMs, ms)
		}
	}
}

// TestSubmitBudgetMsDomainAndForward: the same MCP-face domain gate and
// forwarding on the async half.
func TestSubmitBudgetMsDomainAndForward(t *testing.T) {
	e, reqs := newEnv(t, []step{
		{result: map[string]any{"tag": "j1", "queued": true}},
	}, Options{})

	res := e.call("xuansi_submit", map[string]any{"board": "d75884", "code": "shot()", "tag": "j1", "budget_ms": float64(-5)})
	if !res.IsError || !strings.Contains(resultText(t, res), "budget_ms") {
		t.Fatalf("negative budget on submit = %+v, want a refusal naming budget_ms", res)
	}
	select {
	case r := <-reqs:
		t.Fatalf("out-of-domain submit budget reached the daemon: %+v", r)
	case <-time.After(150 * time.Millisecond):
	}

	res = e.call("xuansi_submit", map[string]any{"board": "d75884", "code": "shot()", "tag": "j1", "budget_ms": float64(12000)})
	if res.IsError {
		t.Fatalf("valid submit budget: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "submit" || req.BudgetMs != 12000 {
		t.Fatalf("daemon request = %s budget %d, want submit budget 12000", req.Type, req.BudgetMs)
	}
}

// TestRunLuaDecodesFrames: the eval reply's frames array and drop count
// decode into the typed out (ch/name/data_b64 per frame).
func TestRunLuaDecodesFrames(t *testing.T) {
	jpeg := []byte{0xFF, 0xD8, 7, 7, 0xFF, 0xD9}
	e, _ := newEnv(t, []step{{result: map[string]any{
		"out":            "ok",
		"frames":         []any{map[string]any{"ch": float64(2), "name": "jpge", "data_b64": base64.StdEncoding.EncodeToString(jpeg)}},
		"frames_dropped": float64(3),
	}}}, Options{})

	res := e.call("xuansi_run_lua", map[string]any{"board": "d75884", "code": "shot()"})
	if res.IsError {
		t.Fatalf("run_lua: %s", resultText(t, res))
	}
	m := structured(t, res)
	frames, ok := m["frames"].([]any)
	if !ok || len(frames) != 1 {
		t.Fatalf("frames = %#v, want one entry", m["frames"])
	}
	fm, _ := frames[0].(map[string]any)
	if fm["ch"] != float64(2) || fm["name"] != "jpge" {
		t.Fatalf("frame meta = %#v, want ch=2 name=jpge", fm)
	}
	got, err := base64.StdEncoding.DecodeString(fmt.Sprint(fm["data_b64"]))
	if err != nil || !bytes.Equal(got, jpeg) {
		t.Fatalf("data_b64 decode = %x (err %v), want %x", got, err, jpeg)
	}
	if m["frames_dropped"] != float64(3) {
		t.Fatalf("frames_dropped = %#v, want 3", m["frames_dropped"])
	}
}
