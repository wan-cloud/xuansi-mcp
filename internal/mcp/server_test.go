package mcp

// server_test.go — the static tool face tested end-to-end: a fake
// daemon over a real unix socket (the internal/apiclient harness
// pattern), a real apiclient.Client, and a REAL MCP server↔client pair
// over the go-sdk's in-memory transport — so every test exercises the
// exact wiring `xuansi mcp` serves: SDK schema inference, typed
// handlers, tool-error packing. The fake daemon asserts the
// server.Request each tool actually sends.

import (
	"context"
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
	path := filepath.Join(t.TempDir(), "d.sock")
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
	t  *testing.T
	c  *apiclient.Client
	ss *mcpsdk.ServerSession
	cl *mcpsdk.ClientSession
}

// wireMCP wires the MCP half (apiclient ← server ⇄ in-memory client)
// against a caller-owned fake daemon — for scripted daemons beyond
// serveScript's fixed reply queue (e.g. request-dependent behavior).
func wireMCP(t *testing.T, fd *fakeDaemon, opts Options) *env {
	t.Helper()
	c, err := apiclient.Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "xuansi", Version: "test"}, nil)
	RegisterTools(srv, c, opts)

	ctx := context.Background()
	ct, st := mcpsdk.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, c: c, ss: ss, cl: cl}
	t.Cleanup(func() {
		cl.Close()
		ss.Close()
		c.Close()
	})
	return e
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

// staticTools is the exact task-8 toolset — no events/ws/capture
// tools (those land with tasks 10-12).
var staticTools = []string{
	"xuansi_list_boards",
	"xuansi_board_status",
	"xuansi_lease",
	"xuansi_release",
	"xuansi_run_lua",
	"xuansi_submit",
	"xuansi_script_start",
	"xuansi_script_stop",
	"xuansi_fs_write",
	"xuansi_fs_read",
	"xuansi_board_op",
}

// ---------------------------------------------------------------------------
// registration, holder identity, error mapping

// TestRegistersExactStaticToolset: the served tool list is exactly the
// 11 static tools, each with a non-empty agent-facing description, and
// the inferred schemas mark the required/optional split the handlers
// rely on (board required, holder/transport optional).
func TestRegistersExactStaticToolset(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, tool := range lr.Tools {
		got = append(got, tool.Name)
		if tool.Description == "" {
			t.Errorf("tool %s has an empty description", tool.Name)
		}
	}
	sort.Strings(got)
	want := append([]string(nil), staticTools...)
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
// transport field, and the daemon's reboot warning crosses into the
// result verbatim.
func TestLeaseUSBWarningVerbatim(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"board": "d75884", "holder": "mcp-fixed01", "session": 7.0,
		"transport": "usb", "warning": server.USBRebootWarning,
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
	if m["warning"] != server.USBRebootWarning {
		t.Errorf("warning = %v, want the daemon's verbatim reboot warning", m["warning"])
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

// TestFsWriteClientSideCapRefusal: a decoded payload above the 16384B
// single-file cap is refused HERE — api class, message mirroring the
// daemon's own refusal shape — with ZERO daemon contact; the boundary
// itself (exactly 16384B) crosses normally.
func TestFsWriteClientSideCapRefusal(t *testing.T) {
	e, reqs := newEnv(t, nil, Options{Holder: "mcp-fixed01"})

	data := make([]byte, fsWriteMax+1)
	for i := range data {
		data[i] = byte('a' + i%26)
	}
	res := e.call("xuansi_fs_write", map[string]any{
		"board": "d75884", "path": "/big.bin",
		"data_b64": base64Std(t, data),
	})
	if !res.IsError {
		t.Fatalf("oversized fs_write did not surface as a tool error: %+v", res)
	}
	want := "api: fs_write /big.bin: 16385 bytes exceeds the 16384B single-file cap (refused client-side)"
	if got := resultText(t, res); got != want {
		t.Errorf("error text = %q, want exactly %q", got, want)
	}
	noContact(t, reqs) // the refusal never reached the daemon
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
