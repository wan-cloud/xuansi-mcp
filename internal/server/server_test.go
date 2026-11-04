package server

// server_test.go — the task-11 unit face: the daemon's unix-socket API
// against a scripted fake board controller (the board.Controller seam
// SupOpts.DialTCP injects). The framing, the three message shapes, id
// pairing, the lease gating (holder semantics: bind / adopt /
// agent-gone), error-class preservation from the board, event fan-out,
// and graceful shutdown are all pinned here; the live-board behavior is
// the HIL/smoke's job (results/2026-11-05-b5-t11-*.log).

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/lease"
	"xuansi/internal/replclient"
)

// ---------------------------------------------------------------------------
// fakeCtl — a scripted board.Controller (the Supervisor dial seam).
// ---------------------------------------------------------------------------

type fakeCtl struct {
	mu       sync.Mutex
	dead     bool
	ops      map[string]func(args map[string]any) (map[string]any, *replclient.OpError)
	calls    []mockCall
	handlers map[int]evHandler
	nextHID  int
	hellos   int
}

type mockCall struct {
	op   string
	args map[string]any
}

type evHandler struct {
	prefix string
	fn     func(replclient.Event)
}

func newFakeCtl() *fakeCtl {
	f := &fakeCtl{
		ops:      map[string]func(args map[string]any) (map[string]any, *replclient.OpError){},
		handlers: map[int]evHandler{},
	}
	f.ops["hello"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": int64(3), "role": "controller", "fw": "xuansi-fw v0.1.0",
			"fw_build": "deadbee", "fs": true,
		}, nil
	}
	f.ops["script.list"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"out": "scripts (1): orphan"}, nil
	}
	f.ops["events.subscribe"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"source": args["source"]}, nil
	}
	f.ops["diag.pins"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"out": "pins (0):"}, nil
	}
	f.ops["wifi.status"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"state": "off"}, nil
	}
	f.ops["session.reset"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"stopped": int64(0)}, nil
	}
	f.ops["eval"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"out": "ok"}, nil
	}
	f.ops["submit"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"tag": args["tag"], "queued": true}, nil
	}
	f.ops["script.start"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"name": args["name"]}, nil
	}
	f.ops["script.stop"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{}, nil
	}
	f.ops["fs.write"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		data, _ := args["data"].([]byte)
		return map[string]any{"size": int64(len(data))}, nil
	}
	f.ops["fs.read"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"data": []byte("print(1)")}, nil
	}
	return f
}

// on scripts one op's reply.
func (f *fakeCtl) on(op string, fn func(args map[string]any) (map[string]any, *replclient.OpError)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops[op] = fn
}

// got returns the op calls so far, in order.
func (f *fakeCtl) got() []mockCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]mockCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// push delivers one board event to the prefix subscribers (the tap).
func (f *fakeCtl) push(source string, data map[string]any) {
	f.mu.Lock()
	hs := make([]evHandler, 0, len(f.handlers))
	for _, h := range f.handlers {
		hs = append(hs, h)
	}
	f.mu.Unlock()
	for _, h := range hs {
		if strings.HasPrefix(source, h.prefix) {
			h.fn(replclient.Event{Source: source, Data: data})
		}
	}
}

func (f *fakeCtl) Op(ctx context.Context, op string, args map[string]any) (map[string]any, error) {
	f.mu.Lock()
	if f.dead {
		f.mu.Unlock()
		return nil, fmt.Errorf("%w: fake closed", replclient.ErrDead)
	}
	f.calls = append(f.calls, mockCall{op: op, args: args})
	h := f.ops[op]
	f.mu.Unlock()
	if h == nil {
		return nil, &replclient.OpError{Op: op, Class: "api", Msg: "unknown op " + op}
	}
	res, berr := h(args)
	if berr != nil {
		return nil, berr
	}
	return res, nil
}

func (f *fakeCtl) Hello(observer bool, name string) (*replclient.HelloInfo, error) {
	_, err := f.Op(context.Background(), "hello", map[string]any{"client": name, "observer": observer})
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.hellos++
	f.mu.Unlock()
	return &replclient.HelloInfo{
		ProtocolVer: 3, Role: "controller", Fw: "xuansi-fw v0.1.0", FwBuild: "deadbee",
	}, nil
}

func (f *fakeCtl) Dead() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.dead }
func (f *fakeCtl) DeadReason() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead {
		return "fake closed"
	}
	return ""
}
func (f *fakeCtl) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dead = true
	return nil
}

func (f *fakeCtl) OnEvent(sourcePrefix string, fn func(replclient.Event)) (cancel func()) {
	f.mu.Lock()
	id := f.nextHID
	f.nextHID++
	f.handlers[id] = evHandler{prefix: sourcePrefix, fn: fn}
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		delete(f.handlers, id)
		f.mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// test environment: a Server over injected discovery + fake controllers.
// ---------------------------------------------------------------------------

type env struct {
	t      *testing.T
	srv    *Server
	sock   string
	fakes  map[string]*fakeCtl
	dialed map[string]int
}

// newEnv builds a serving Server over boards (usb+mdns merged view) with
// one shared fake controller per board. The daemon lifecycle (initial
// refresh + startup audit) runs for real.
func newEnv(t *testing.T, aliases map[string]string, boards ...discover.Board) *env {
	return newEnvTTL(t, 10*time.Second, aliases, boards...)
}

// newEnvTTL is newEnv with a custom lease grace TTL (the boundary tests
// run the real expiry path with a short window).
func newEnvTTL(t *testing.T, ttl time.Duration, aliases map[string]string, boards ...discover.Board) *env {
	t.Helper()
	e := &env{
		t:      t,
		fakes:  map[string]*fakeCtl{},
		dialed: map[string]int{},
	}
	for _, b := range boards {
		e.fakes[b.MAC6] = newFakeCtl()
	}
	var dmu sync.Mutex
	reg := discover.NewRegistry(aliases,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			return boards, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			return boards, nil
		}),
	)
	e.sock = filepath.Join(t.TempDir(), "d.sock")
	srv, err := New(Opts{
		SockPath:        e.sock,
		Registry:        reg,
		GraceTTL:        ttl, // HIL-short by default; no test waits it out
		RefreshInterval: -1,  // no background loop; the initial sweep still runs
		Log:             func(format string, args ...any) { t.Logf("srv: "+format, args...) },
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: func(format string, args ...any) { t.Logf("sup: "+format, args...) },
				DialTCP: func() (board.Controller, error) {
					dmu.Lock()
					e.dialed[ref.MAC6]++
					f := e.fakes[ref.MAC6]
					dmu.Unlock()
					if f == nil {
						return nil, fmt.Errorf("no fake for %s", ref.MAC6)
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
	return e
}

// mkBoard builds a TCP-dialable discovered board view.
func mkBoard(mac6, host string) discover.Board {
	return discover.Board{
		MAC6: mac6, Sources: []string{discover.SourceUSB, discover.SourceMDNS},
		USBPath: "/dev/serial/by-id/fake-" + mac6, Host: host, Port: 7777,
	}
}

// sockClient is a raw API client end for the tests.
type sockClient struct {
	t    *testing.T
	conn net.Conn
}

func dialSock(t *testing.T, path string) *sockClient {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	t.Cleanup(func() { conn.Close() })
	return &sockClient{t: t, conn: conn}
}

// send writes one framed JSON message.
func (c *sockClient) send(v any) {
	c.t.Helper()
	if err := WriteMsg(c.conn, v); err != nil {
		c.t.Fatalf("send: %v", err)
	}
}

// recv reads one framed JSON message into a map (deadline-bounded).
func (c *sockClient) recv() map[string]any {
	c.t.Helper()
	c.conn.SetDeadline(time.Now().Add(4 * time.Second))
	var m map[string]any
	if err := ReadMsg(c.conn, &m); err != nil {
		c.t.Fatalf("recv: %v", err)
	}
	return m
}

// recvNone asserts no message arrives within a short window.
func (c *sockClient) recvNone() {
	c.t.Helper()
	c.conn.SetDeadline(time.Now().Add(250 * time.Millisecond))
	var m map[string]any
	if err := ReadMsg(c.conn, &m); err == nil {
		c.t.Fatalf("unexpected message: %#v", m)
	}
}

// call sends one request and waits for the reply with the same id
// (events on the wire are skipped and returned alongside).
func (c *sockClient) call(id int64, req map[string]any) (reply map[string]any, events []map[string]any) {
	c.t.Helper()
	req["id"] = id
	c.send(req)
	for {
		m := c.recv()
		if m["type"] == "event" {
			events = append(events, m)
			continue
		}
		if got := asInt(m["id"]); got != id {
			c.t.Fatalf("reply id = %v, want %d (%#v)", m["id"], id, m)
		}
		return m, events
	}
}

func asInt(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	}
	return -1
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asStr(v any) string {
	s, _ := v.(string)
	return s
}

// errOf digs {class,msg} out of a reply, failing when the reply is not
// an error.
func errOf(t *testing.T, reply map[string]any) map[string]any {
	t.Helper()
	e := asMap(reply["error"])
	if e == nil {
		t.Fatalf("expected error reply, got %#v", reply)
	}
	return e
}

// ---------------------------------------------------------------------------
// proto unit: framing + the three message shapes.
// ---------------------------------------------------------------------------

// TestProtoFramingRoundTrip: every message is [u32 BE len][JSON]; the
// prefix counts exactly the payload bytes; replies, requests and events
// all survive a round-trip with their shape intact.
func TestProtoFramingRoundTrip(t *testing.T) {
	var buf memoryBuffer
	in := &Reply{Type: "reply", ID: 7, Result: map[string]any{"x": 1, "ok": true}}
	if err := WriteMsg(&buf, in); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	raw := buf.Bytes()
	if len(raw) < 4 {
		t.Fatalf("short frame: %v", raw)
	}
	if n := binary.BigEndian.Uint32(raw[:4]); int(n) != len(raw)-4 {
		t.Fatalf("len prefix = %d, payload = %d", n, len(raw)-4)
	}
	var out Reply
	if err := ReadMsg(&buf, &out); err != nil {
		t.Fatalf("ReadMsg: %v", err)
	}
	if out.Type != "reply" || out.ID != 7 {
		t.Fatalf("round-trip = %+v", out)
	}
	res := asMap(out.Result)
	if asInt(res["x"]) != 1 || res["ok"] != true {
		t.Fatalf("result = %#v", out.Result)
	}

	// request decode: the documented request keys.
	var req Request
	j := `{"type":"eval","id":3,"board":"d75884","holder":"h1","code":"return 1","args":[1,2]}`
	if err := json.Unmarshal([]byte(j), &req); err != nil {
		t.Fatalf("request json: %v", err)
	}
	if req.Type != "eval" || req.ID != 3 || req.Board != "d75884" || req.Holder != "h1" || req.Code != "return 1" {
		t.Fatalf("request = %+v", req)
	}
	if len(req.Args.([]any)) != 2 { //nolint:typecheck // documented any-typed field
		t.Fatalf("args = %#v", req.Args)
	}

	// event push shape: {"type":"event","board",event{source,data}}
	ev := EventPush{Type: "event", Board: "d75884", Event: WireEvent{Source: "gpio.4", Data: map[string]any{"edge": "rising"}}}
	p, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	for _, want := range []string{`"type":"event"`, `"board":"d75884"`, `"source":"gpio.4"`, `"edge":"rising"`} {
		if !strings.Contains(string(p), want) {
			t.Fatalf("event json %s missing %s", p, want)
		}
	}
}

// memoryBuffer is a byte buffer satisfying io.ReadWriter.
type memoryBuffer struct {
	data []byte
}

func (m *memoryBuffer) Write(p []byte) (int, error) {
	// account for the length prefix written before the payload
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *memoryBuffer) Read(p []byte) (int, error) {
	if len(m.data) == 0 {
		return 0, os.ErrDeadlineExceeded // never reached in these tests
	}
	n := copy(p, m.data)
	m.data = m.data[n:]
	return n, nil
}

func (m *memoryBuffer) Bytes() []byte { return m.data }

// TestProtoRejectsOversize: a declared length above MaxMsgBytes is a
// protocol violation, and a truncated payload surfaces as an error
// rather than a silent hang.
func TestProtoRejectsOversize(t *testing.T) {
	one := memoryBuffer{}
	big := strings.Repeat("x", MaxMsgBytes+1)
	if err := WriteMsg(&one, map[string]any{"data": big}); err == nil {
		t.Fatalf("oversize write accepted")
	}

	// hand-rolled oversized prefix
	c := memoryBuffer{data: []byte{0xFF, 0xFF, 0xFF, 0xFF}}
	var m map[string]any
	if err := ReadMsg(&c, &m); err == nil {
		t.Fatalf("oversize read accepted")
	}

	// truncated: prefix says 10 bytes, only 2 follow
	c2 := memoryBuffer{data: append([]byte{0, 0, 0, 10}, []byte("{}")...)}
	if err := ReadMsg(&c2, &m); err == nil {
		t.Fatalf("truncated read accepted")
	}
}

// ---------------------------------------------------------------------------
// e2e over the unix socket.
// ---------------------------------------------------------------------------

// TestListBoardsShape: list_boards returns the registry view — mac6,
// alias, sources, host, fw (from the enriched hello — the async startup
// audit fills it; polled) — and the mdns health marker.
func TestListBoardsShape(t *testing.T) {
	aliases := map[string]string{"d75884": "bench"}
	e := newEnv(t, aliases, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	var b map[string]any
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rep, _ := c.call(1, map[string]any{"type": "list_boards"})
		if asMap(rep["error"]) != nil {
			t.Fatalf("list_boards error: %#v", rep)
		}
		res := asMap(rep["result"])
		boards, _ := res["boards"].([]any)
		if len(boards) != 1 {
			t.Fatalf("boards = %#v", res)
		}
		b = asMap(boards[0])
		if asStr(b["fw"]) != "" {
			break // audit enriched the board
		}
		time.Sleep(10 * time.Millisecond)
	}
	if asStr(b["mac6"]) != "d75884" || asStr(b["alias"]) != "bench" {
		t.Fatalf("board = %#v", b)
	}
	srcs, _ := b["sources"].([]any)
	if len(srcs) != 2 || asStr(srcs[0]) != "usb" || asStr(srcs[1]) != "mdns" {
		t.Fatalf("sources = %#v", b["sources"])
	}
	if asStr(b["host"]) != "xuansi-d75884.local" || asInt(b["port"]) != 7777 {
		t.Fatalf("host/port = %#v", b)
	}
	if asStr(b["fw"]) == "" {
		t.Fatalf("fw not enriched within deadline: %#v", b)
	}
}

// TestReplyIDPairingUnderConcurrency: two requests in flight on one
// connection each get the reply carrying their own id.
func TestReplyIDPairingUnderConcurrency(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	// lease first so both evals are legal
	rep, _ := c.call(100, map[string]any{"type": "lease", "board": "d75884", "holder": "pair"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	var wg sync.WaitGroup
	type outcome struct {
		id   int64
		code string
	}
	out := make(chan outcome, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := int64(200 + i)
			code := fmt.Sprintf("return %d", i)
			rep, _ := c.call(id, map[string]any{"type": "eval", "board": "d75884", "code": code})
			if asMap(rep["error"]) != nil {
				t.Errorf("eval %d: %#v", id, rep)
			}
			if asInt(rep["id"]) != id {
				t.Errorf("reply id = %v, want %d", rep["id"], id)
			}
			out <- outcome{id: asInt(rep["id"]), code: code}
		}(i)
	}
	wg.Wait()
	close(out)
	seen := map[int64]bool{}
	for o := range out {
		if seen[o.id] {
			t.Fatalf("duplicate reply id %d", o.id)
		}
		seen[o.id] = true
	}
}

// TestEvalRequiresLease: eval is a mutating op — without a lease held by
// this connection's holder it refuses with class not_held.
func TestEvalRequiresLease(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "eval", "board": "d75884", "code": "return 1"})
	e1 := errOf(t, rep)
	if asStr(e1["class"]) != "not_held" {
		t.Fatalf("class = %q, want not_held (%#v)", e1["class"], e1)
	}
}

// TestLeaseEvalValueFaceAndReleaseTeardown: the full holder flow —
// lease (session granted), eval (out + bridged value), release (board
// session.reset of the holder namespace AND the eval namespace 0, per
// the T10 teardown ruling).
func TestLeaseEvalValueFaceAndReleaseTeardown(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "smoke"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	res := asMap(rep["result"])
	sess := asInt(res["session"])
	if sess <= 0 {
		t.Fatalf("session = %v", res["session"])
	}
	if asStr(res["holder"]) != "smoke" {
		t.Fatalf("holder = %#v", res)
	}

	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		if asStr(args["code"]) != "return {ok=1}" {
			return nil, &replclient.OpError{Op: "eval", Class: "rt", Msg: "wrong code"}
		}
		return map[string]any{"out": "hello\n", "value": map[string]any{"ok": int64(1)}}, nil
	})
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return {ok=1}"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	res = asMap(rep["result"])
	if asStr(res["out"]) != "hello\n" {
		t.Fatalf("out = %#v", res)
	}
	v := asMap(res["value"])
	if asInt(v["ok"]) != 1 {
		t.Fatalf("value = %#v", res["value"])
	}

	rep, _ = c.call(3, map[string]any{"type": "release", "board": "d75884", "holder": "smoke"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("release: %#v", rep)
	}

	// teardown: session.reset{holder session} then session.reset{0}
	var resets []int64
	deadline := time.Now().Add(2 * time.Second)
	for len(resets) < 2 && time.Now().Before(deadline) {
		resets = resets[:0]
		for _, call := range e.fakes["d75884"].got() {
			if call.op == "session.reset" {
				resets = append(resets, asInt(call.args["session"]))
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(resets) != 2 || resets[0] != sess || resets[1] != 0 {
		t.Fatalf("session.reset calls = %v, want [%d 0]", resets, sess)
	}
}

// TestEvalBoardErrorClassPreserved: a board error reply crosses the API
// with its fw class and msg verbatim (including ` at byte N`).
func TestEvalBoardErrorClassPreserved(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "e1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return nil, &replclient.OpError{Op: "eval", Class: "compile", Msg: "syntax error near '}' at byte 12"}
	})
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return {"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "compile" {
		t.Fatalf("class = %q, want compile", er["class"])
	}
	if !strings.Contains(asStr(er["msg"]), " at byte 12") {
		t.Fatalf("msg = %q, want byte-offset suffix preserved", er["msg"])
	}
}

// TestDisconnectMovesLeaseToGrace: a socket disconnect is the agent-gone
// signal — the holder's live lease enters grace (board state kept).
func TestDisconnectMovesLeaseToGrace(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "gone-1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	c.conn.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, ls := range e.srv.Leases().Snapshot() {
			if ls.Board == "d75884" && !ls.GraceDeadline.IsZero() {
				return // in grace: agent gone, board state kept
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("lease never entered grace after disconnect: %+v", e.srv.Leases().Snapshot())
}

// TestHolderAdoptAcrossConnections: adopt-within-TTL — a NEW connection
// re-supplying the same holder takes the grace lease back live and may
// eval immediately.
func TestHolderAdoptAcrossConnections(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c1 := dialSock(t, e.sock)
	rep, _ := c1.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "agent-9"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	c1.conn.Close()
	// wait for grace
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := false
		for _, ls := range e.srv.Leases().Snapshot() {
			if ls.Board == "d75884" && !ls.GraceDeadline.IsZero() {
				ok = true
			}
		}
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	c2 := dialSock(t, e.sock)
	rep, _ = c2.call(2, map[string]any{"type": "eval", "board": "d75884", "holder": "agent-9", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("adopt-eval: %#v", rep)
	}
	for _, ls := range e.srv.Leases().Snapshot() {
		if ls.Board == "d75884" {
			if ls.Holder != "agent-9" || !ls.GraceDeadline.IsZero() {
				t.Fatalf("lease after adopt = %+v, want live agent-9", ls)
			}
		}
	}
}

// TestEvalForeignHolderRefused: a connection whose holder is NOT the
// lease holder gets the held error with the holder's identity.
func TestEvalForeignHolderRefused(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c1 := dialSock(t, e.sock)
	rep, _ := c1.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "owner-a"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	c2 := dialSock(t, e.sock)
	rep, _ = c2.call(2, map[string]any{"type": "eval", "board": "d75884", "holder": "owner-b", "code": "return 1"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "held" {
		t.Fatalf("class = %q, want held (%#v)", er["class"], er)
	}
	if asStr(er["holder"]) != "owner-a" {
		t.Fatalf("error holder = %q, want owner-a", er["holder"])
	}
}

// TestReleaseWrongHolder: release by anyone else refuses not_held.
func TestReleaseWrongHolder(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c1 := dialSock(t, e.sock)
	rep, _ := c1.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "owner-a"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	c2 := dialSock(t, e.sock)
	rep, _ = c2.call(2, map[string]any{"type": "release", "board": "d75884", "holder": "owner-b"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("class = %q, want not_held", er["class"])
	}
}

// TestHolderConflictOnOneConnection: once a connection bound an explicit
// holder, a different explicit holder on the same connection is a
// protocol error.
func TestHolderConflictOnOneConnection(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "h1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "holder": "h2", "code": "return 1"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "holder" {
		t.Fatalf("class = %q, want holder", er["class"])
	}
}

// TestWatchEventsFanOut: watch_events subscribes the CONNECTION to the
// board's event stream; a board event reaches every watcher and nobody
// else, as {"type":"event","board",event}.
func TestWatchEventsFanOut(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w1 := dialSock(t, e.sock)
	w2 := dialSock(t, e.sock)
	other := dialSock(t, e.sock)

	rep, _ := w1.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch 1: %#v", rep)
	}
	rep, _ = w2.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch 2: %#v", rep)
	}
	rep, _ = other.call(1, map[string]any{"type": "list_boards"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("list: %#v", rep)
	}

	e.fakes["d75884"].push("gpio.4", map[string]any{"edge": "rising", "level": int64(1)})

	for _, w := range []*sockClient{w1, w2} {
		ev := w.recv()
		if asStr(ev["type"]) != "event" || asStr(ev["board"]) != "d75884" {
			t.Fatalf("event push = %#v", ev)
		}
		inner := asMap(ev["event"])
		if asStr(inner["source"]) != "gpio.4" || asStr(asMap(inner["data"])["edge"]) != "rising" {
			t.Fatalf("event = %#v", inner)
		}
	}
	other.recvNone()

	// watch_events with a source also subscribes that source board-side.
	rep, _ = w1.call(2, map[string]any{"type": "watch_events", "board": "d75884", "source": "stdout"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch source: %#v", rep)
	}
	found := false
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "events.subscribe" && asStr(call.args["source"]) == "stdout" {
			found = true
		}
	}
	if !found {
		t.Fatalf("events.subscribe(stdout) never sent: %+v", e.fakes["d75884"].got())
	}

	// disconnect one watcher: no more delivery to it (unsubscribe on
	// disconnect), the other keeps receiving.
	w1.conn.Close()
	time.Sleep(100 * time.Millisecond)
	e.fakes["d75884"].push("gpio.4", map[string]any{"edge": "falling", "level": int64(0)})
	ev := w2.recv()
	if asStr(asMap(asMap(ev["event"])["data"])["edge"]) != "falling" {
		t.Fatalf("second event = %#v", ev)
	}
}

// TestBoardStatusShape: status composes the registry view, the lease
// state, and the live script listing once a connection is warm.
func TestBoardStatusShape(t *testing.T) {
	aliases := map[string]string{"d75884": "bench"}
	e := newEnv(t, aliases, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	// free board first: no lease (the startup audit has already warmed
	// the controller connection, so connected is true from the start)
	rep, _ := c.call(1, map[string]any{"type": "board_status", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("status: %#v", rep)
	}
	res := asMap(rep["result"])
	if res["lease"] != nil {
		t.Fatalf("lease = %#v, want null", res["lease"])
	}
	if res["connected"] != true {
		t.Fatalf("connected = %#v, want true (startup audit warmed it)", res["connected"])
	}

	// lease + eval warms the controller; status then sees it all
	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "st"})
	_ = rep
	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	rep, _ = c.call(4, map[string]any{"type": "board_status", "board": "d75884"})
	res = asMap(rep["result"])
	if res["connected"] != true {
		t.Fatalf("connected = %#v, want true", res["connected"])
	}
	leaseM := asMap(res["lease"])
	if asStr(leaseM["holder"]) != "st" {
		t.Fatalf("lease = %#v", leaseM)
	}
	scripts, _ := res["scripts"].([]any)
	if len(scripts) != 1 || asStr(scripts[0]) != "orphan" {
		t.Fatalf("scripts = %#v", res["scripts"])
	}
	b := asMap(res["board"])
	if asStr(b["alias"]) != "bench" || asStr(b["fw"]) == "" {
		t.Fatalf("board view = %#v", b)
	}
}

// TestSelectorErrors: resolve failures cross as structured errors —
// no match → no_board, ambiguity → ambiguous with candidates listed.
func TestSelectorErrors(t *testing.T) {
	e := newEnv(t, nil,
		mkBoard("d75884", "xuansi-d75884.local"),
		mkBoard("d75800", "xuansi-d75800.local"),
	)
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "board_status", "board": "zzzzzz"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "no_board" {
		t.Fatalf("class = %q, want no_board", er["class"])
	}

	rep, _ = c.call(2, map[string]any{"type": "board_status", "board": "d7"})
	er = errOf(t, rep)
	if asStr(er["class"]) != "ambiguous" {
		t.Fatalf("class = %q, want ambiguous (%#v)", er["class"], er)
	}
	if !strings.Contains(asStr(er["msg"]), "d75884") || !strings.Contains(asStr(er["msg"]), "d75800") {
		t.Fatalf("ambiguous msg does not list candidates: %q", er["msg"])
	}
}

// TestScriptStartCarriesLeaseSession: the eval ruling (T10) — agent
// work runs under script.start with the LEASE's session id, not the
// default namespace 1.
func TestScriptStartCarriesLeaseSession(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "s1"})
	sess := asInt(asMap(rep["result"])["session"])

	rep, _ = c.call(2, map[string]any{
		"type": "script_start", "board": "d75884", "name": "blink", "code": "while true do end",
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("script_start: %#v", rep)
	}
	var got *mockCall
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "script.start" {
			cc := call
			got = &cc
		}
	}
	if got == nil {
		t.Fatalf("script.start never sent: %+v", e.fakes["d75884"].got())
	}
	if asInt(got.args["session"]) != sess {
		t.Fatalf("script.start session = %v, want lease session %d", got.args["session"], sess)
	}
}

// TestScriptStartRequiresLease: like eval — mutating ops are
// holder-gated.
func TestScriptStartRequiresLease(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "script_start", "board": "d75884", "name": "x", "code": "y"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("class = %q, want not_held", er["class"])
	}
}

// TestFsWriteCapAndFsRead: the 16KB single-file cap is refused
// daemon-side; fs_read is read-only (no lease needed) and returns the
// board's bytes.
func TestFsWriteCapAndFsRead(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "fs"})
	_ = rep

	big := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 16385)))
	rep, _ = c.call(2, map[string]any{"type": "fs_write", "board": "d75884", "path": "/lua/big.lua", "data": big})
	er := errOf(t, rep)
	if asStr(er["class"]) != "api" {
		t.Fatalf("class = %q, want api (%#v)", er["class"], er)
	}

	rep, _ = c.call(3, map[string]any{"type": "fs_read", "board": "d75884", "path": "/lua/a.lua"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("fs_read: %#v", rep)
	}
}

// TestSubmitPassthrough: submit acks with the board's result and needs
// the lease.
func TestSubmitPassthrough(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "submit", "board": "d75884", "code": "return 1", "tag": "j1"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("class = %q, want not_held", er["class"])
	}
	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "sub"})
	_ = rep
	e.fakes["d75884"].on("submit", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"tag": asStr(args["tag"]), "queued": true}, nil
	})
	rep, _ = c.call(3, map[string]any{"type": "submit", "board": "d75884", "code": "return 1", "tag": "j1"})
	res := asMap(rep["result"])
	if asStr(res["tag"]) != "j1" || res["queued"] != true {
		t.Fatalf("submit result = %#v", res)
	}
}

// TestEvalArgsNumbersNormalizedToIntegers: JSON decodes every number to
// float64, but the board's v3 args bridge accepts integers — a raw
// pass-through would reach the fw as msgpack floats and be rejected
// ("protocol: args: malformed", found by the b5-full HIL S3). The daemon
// must convert integral float64s (the only way an integer can arrive over
// JSON) back to int64; genuine fractions pass through untouched.
func TestEvalArgsNumbersNormalizedToIntegers(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "n"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	var got map[string]any
	var gotArr []any
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		got, _ = args["args"].(map[string]any)
		gotArr, _ = args["args"].(map[string]any)["arr"].([]any)
		return map[string]any{"out": ""}, nil
	})
	rep, _ = c.call(2, map[string]any{
		"type": "eval", "board": "d75884", "code": "return ARGS",
		"args": map[string]any{"n": float64(7), "f": 2.5, "arr": []any{float64(1), float64(-2)}},
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	if got == nil {
		t.Fatalf("args not captured")
	}
	if v, ok := got["n"].(int64); !ok || v != 7 {
		t.Fatalf("integral float64 7 must cross as int64, got %#v", got["n"])
	}
	if _, ok := got["f"].(float64); !ok {
		t.Fatalf("fraction 2.5 must stay float64, got %#v", got["f"])
	}
	if len(gotArr) != 2 {
		t.Fatalf("array args = %#v", got["arr"])
	}
	if v, ok := gotArr[0].(int64); !ok || v != 1 {
		t.Fatalf("array integral 1 must cross as int64, got %#v", gotArr[0])
	}
	if v, ok := gotArr[1].(int64); !ok || v != -2 {
		t.Fatalf("array integral -2 must cross as int64, got %#v", gotArr[1])
	}
}

// TestMultiBoardIndependentLeases: one connection, two boards — each
// board gets its own lease and its own controller connection.
func TestMultiBoardIndependentLeases(t *testing.T) {
	e := newEnv(t, nil,
		mkBoard("d75884", "xuansi-d75884.local"),
		mkBoard("aabbcc", "xuansi-aabbcc.local"),
	)
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "multi"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease 1: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "aabbcc", "holder": "multi"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease 2: %#v", rep)
	}
	for i, b := range []string{"d75884", "aabbcc"} {
		rep, _ = c.call(int64(10+i), map[string]any{"type": "eval", "board": b, "code": "return 1"})
		if asMap(rep["error"]) != nil {
			t.Fatalf("eval %s: %#v", b, rep)
		}
	}
	if len(e.srv.Leases().Snapshot()) != 2 {
		t.Fatalf("leases = %+v, want 2", e.srv.Leases().Snapshot())
	}
}

// TestUnknownRequestType: an unknown request type is a structured
// server error, not a dropped connection.
func TestUnknownRequestType(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "dance", "board": "d75884"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "server" {
		t.Fatalf("class = %q, want server", er["class"])
	}
	// the connection stays usable
	rep, _ = c.call(2, map[string]any{"type": "list_boards"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("list after unknown: %#v", rep)
	}
}

// TestMalformedJSONFrame: a frame whose payload is not JSON keeps the
// framing in sync — the connection gets a server-class error reply and
// stays usable; only the broken sender is affected.
func TestMalformedJSONFrame(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	bad := dialSock(t, e.sock)
	good := dialSock(t, e.sock)

	// frame whose payload is not JSON
	payload := []byte("not json at all")
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(payload)))
	bad.conn.Write(append(hdr, payload...))

	// the connection gets a server-class error reply (id 0) and stays
	// framed — the next real request lands normally
	er := bad.recv()
	if asStr(er["type"]) != "reply" || asStr(asMap(er["error"])["class"]) != "server" {
		t.Fatalf("garbage reply = %#v", er)
	}

	rep, _ := bad.call(1, map[string]any{"type": "list_boards"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("connection did not survive garbage: %#v", rep)
	}

	rep, _ = good.call(1, map[string]any{"type": "list_boards"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("peer unaffected by garbage: %#v", rep)
	}
}

// ---------------------------------------------------------------------------
// lifecycle.
// ---------------------------------------------------------------------------

// TestShutdownGraceful: Close removes the socket file, stops the
// listener, ends every server goroutine (Done closes) and is
// idempotent.
func TestShutdownGraceful(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "bye"})
	_ = rep

	e.srv.Close()
	e.srv.Close() // idempotent

	if _, err := os.Stat(e.sock); !os.IsNotExist(err) {
		t.Fatalf("sock file %s not removed (stat err=%v)", e.sock, err)
	}
	if _, err := net.Dial("unix", e.sock); err == nil {
		t.Fatalf("sock still dialable after Close")
	}
	select {
	case <-e.srv.Done():
	case <-time.After(3 * time.Second):
		t.Fatalf("server goroutines did not exit within 3s of Close")
	}
	// the board connections are closed too
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e.fakes["d75884"].Dead() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("board controller not closed at shutdown")
}

// TestNewRefusesWhenSockAlive: a second daemon on a live socket refuses
// to start (the dial probe says another daemon owns it).
func TestNewRefusesWhenSockAlive(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	if _, err := New(Opts{SockPath: e.sock, RefreshInterval: -1}); err == nil {
		t.Fatalf("second daemon accepted on a live socket")
	}
}

// TestStaleSockFileRemoved: a leftover socket file (dead daemon) is
// removed and owned anew.
func TestStaleSockFileRemoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stale.sock")
	if err := os.WriteFile(path, []byte("corpse"), 0o600); err != nil {
		t.Fatalf("write stale: %v", err)
	}
	reg := discover.NewRegistry(nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	srv, err := New(Opts{SockPath: path, Registry: reg, RefreshInterval: -1})
	if err != nil {
		t.Fatalf("New over stale sock: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Close()
	if _, err := net.Dial("unix", path); err != nil {
		t.Fatalf("fresh sock not dialable: %v", err)
	}
}

// TestDefaultSockPathFallback documents the no-XDG fallback.
func TestDefaultSockPath(t *testing.T) {
	if !strings.HasSuffix(DefaultSockPath(), ".sock") {
		t.Fatalf("DefaultSockPath = %q", DefaultSockPath())
	}
}

// ---------------------------------------------------------------------------
// grace-expiry boundary (review I-1): the gate must refuse, never
// proceed ungated, when the adopt fails.
// ---------------------------------------------------------------------------

// schedClock is a clock whose Now() returns a scripted sequence (last
// value repeats) once armed; before arming it returns a stable epoch.
// After(d) never fires. With the manager's reaper closed, the read
// sequence is deterministic — this pins the exact Snapshot-not-due →
// Adopt-due window.
type schedClock struct {
	mu   sync.Mutex
	arm  bool
	plan []time.Time
	i    int
}

func (c *schedClock) Arm(plan []time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.plan, c.i, c.arm = plan, 0, true
}

func (c *schedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.arm || len(c.plan) == 0 {
		return time.Unix(1700000000, 0)
	}
	t := c.plan[c.i]
	if c.i < len(c.plan)-1 {
		c.i++
	}
	return t
}

func (c *schedClock) After(d time.Duration) <-chan time.Time { return make(chan time.Time) }

// TestEnsureLiveLeaseAdoptFalseGatesOps: at the grace-expiry boundary
// the Snapshot shows a still-adoptable grace lease, the clock crosses
// the deadline, and Adopt() lazily expires + tears down and returns
// false — the gate must treat that as not_held instead of letting the
// op proceed ungated (review I-1).
func TestEnsureLiveLeaseAdoptFalseGatesOps(t *testing.T) {
	clk := &schedClock{}
	m := lease.NewManager(lease.Policy{GraceTTL: 100 * time.Millisecond, Clock: clk})
	m.Close() // reaper out: the read sequence is ours alone

	s := &Server{leases: m, logf: func(format string, args ...any) { t.Logf("srv: "+format, args...) }}
	if err := m.Acquire("d75884", "edge"); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	m.OnAgentGone("edge") // grace, deadline = T0+100ms

	// post-arm reads: gate's Snapshot → T0 (grace, not due, entry shown),
	// then Adopt's lazy-expiry check → T0+150ms (deadline T0+100ms
	// crossed: the manager CAS's the entry to tearing and runs the
	// teardown synchronously INSIDE Adopt, then reports false)
	T0 := time.Unix(1700000000, 0)
	clk.Arm([]time.Time{T0, T0.Add(150 * time.Millisecond)})

	err := s.ensureLiveLease(nil, &Request{Holder: "edge"}, "d75884")
	if err == nil {
		t.Fatalf("gate passed an op whose adopt failed at the expiry boundary — ungated proceed (review I-1)")
	}
	rerr, ok := err.(*RPCError)
	if !ok {
		t.Fatalf("error type = %T, want *RPCError", err)
	}
	if rerr.Class != "not_held" {
		t.Fatalf("class = %q, want not_held", rerr.Class)
	}
	// pins the adopt-false branch specifically (the fall-through path
	// says "no lease on board ... lease it first")
	if !strings.Contains(rerr.Msg, "grace TTL expired") {
		t.Fatalf("msg = %q, want the grace-expired adopt refusal", rerr.Msg)
	}
	// the lease is gone either way — a fresh lease is required
	if got := m.Snapshot(); len(got) != 0 {
		t.Fatalf("leases after boundary = %+v, want none", got)
	}
}

// TestEvalAfterGraceExpiryRefused: end-to-end over the socket with a
// short real TTL — once the grace expired (and the teardown ran), a
// re-supplied holder gets not_held, never a silent proceed.
func TestEvalAfterGraceExpiryRefused(t *testing.T) {
	e := newEnvTTL(t, 60*time.Millisecond, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c1 := dialSock(t, e.sock)
	rep, _ := c1.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "gone-e2e"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	c1.conn.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(e.srv.Leases().Snapshot()) == 0 {
			break // expired + torn down
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := e.srv.Leases().Snapshot(); len(got) != 0 {
		t.Fatalf("lease never expired: %+v", got)
	}

	c2 := dialSock(t, e.sock)
	rep, _ = c2.call(2, map[string]any{"type": "eval", "board": "d75884", "holder": "gone-e2e", "code": "return 1"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("class = %q, want not_held (%#v)", er["class"], er)
	}
}

// stepClock is a manually-advanced frozen clock whose After always
// returns one shared channel — the test fires it to drive the reaper.
type stepClock struct {
	mu   sync.Mutex
	now  time.Time
	fire chan time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *stepClock) After(d time.Duration) <-chan time.Time { return c.fire }

// TestEnsureLiveLeaseTeardownWindowGated: while the expiry teardown is
// on the wire the entry is still visible in Snapshot but carries NO
// grace deadline — it must not pass the gate as a live lease (the
// sibling of the I-1 window; Manager.Session is the tearing probe).
func TestEnsureLiveLeaseTeardownWindowGated(t *testing.T) {
	clk := &stepClock{now: time.Unix(1700000000, 0), fire: make(chan time.Time, 1)}
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	m := lease.NewManager(lease.Policy{
		GraceTTL: 10 * time.Second, Clock: clk,
		OnExpiry: func(ctx context.Context, board, holder string, sessions []int) error {
			close(blocked)
			<-unblock // hold the teardown: the entry sits mid-teardown
			return nil
		},
	})
	t.Cleanup(func() {
		select {
		case <-unblock:
		default:
			close(unblock)
		}
		m.Close()
	})

	s := &Server{leases: m, logf: func(format string, args ...any) { t.Logf("srv: "+format, args...) }}
	if err := m.Acquire("d75884", "t1"); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	m.OnAgentGone("t1")

	clk.Advance(11 * time.Second) // past the deadline
	clk.fire <- time.Time{}       // wake the reaper → expiry teardown
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatalf("teardown never started")
	}

	// mid-teardown: Snapshot still lists the entry (no grace deadline),
	// but the gate must refuse.
	err := s.ensureLiveLease(nil, &Request{Holder: "t1"}, "d75884")
	if err == nil {
		t.Fatalf("gate passed a mid-teardown lease as live — ungated proceed")
	}
	rerr, ok := err.(*RPCError)
	if !ok || rerr.Class != "not_held" {
		t.Fatalf("err = %#v, want not_held RPCError", err)
	}

	close(unblock)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(m.Snapshot()) == 0 {
			return // teardown completed, entry removed
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("entry never removed after teardown: %+v", m.Snapshot())
}
