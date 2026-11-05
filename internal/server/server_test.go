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
	"math"
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
	t         *testing.T
	srv       *Server
	sock      string
	fakes     map[string]*fakeCtl
	dialed    map[string]int
	usbFakes  map[string]*fakeCtl
	usbDialed map[string]int

	// dmu guards the dial counters (daemon goroutines write them).
	dmu sync.Mutex
}

// dialedTCP / dialedUSB read the dial counters race-free.
func (e *env) dialedTCP(mac6 string) int {
	e.dmu.Lock()
	defer e.dmu.Unlock()
	return e.dialed[mac6]
}

func (e *env) dialedUSB(mac6 string) int {
	e.dmu.Lock()
	defer e.dmu.Unlock()
	return e.usbDialed[mac6]
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
		t:         t,
		fakes:     map[string]*fakeCtl{},
		dialed:    map[string]int{},
		usbFakes:  map[string]*fakeCtl{},
		usbDialed: map[string]int{},
	}
	for _, b := range boards {
		e.fakes[b.MAC6] = newFakeCtl()
		e.usbFakes[b.MAC6] = newFakeCtl()
	}
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

// TestLeaseUSBTransport: lease{transport:"usb"} is the deliberate USB
// route (I-1): it reaches Supervisor.ConnectUSB (the DialUSB seam),
// the reply carries transport:"usb" plus the exact USBRebootWarning,
// the TCP seam is NOT touched by the lease itself, and subsequent ops
// ride the warm USB controller.
func TestLeaseUSBTransport(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	// let the startup audit's TCP dial settle so the before/after
	// counts below are stable
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.dialedTCP("d75884") == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	tcpBefore := e.dialedTCP("d75884")

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "u1", "transport": "usb"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease usb: %#v", rep)
	}
	res := asMap(rep["result"])
	if asInt(res["session"]) <= 0 {
		t.Fatalf("session = %#v", res)
	}
	if asStr(res["transport"]) != "usb" {
		t.Fatalf("transport = %#v, want usb", res)
	}
	if asStr(res["warning"]) != USBRebootWarning {
		t.Fatalf("warning = %#v\nwant  %q", res, USBRebootWarning)
	}

	// the deliberate route hit the USB seam exactly once…
	if n := e.dialedUSB("d75884"); n != 1 {
		t.Fatalf("usb dials = %d, want 1", n)
	}
	// …and ConnectUSB said hello on the USB controller
	usbHellos := 0
	for _, call := range e.usbFakes["d75884"].got() {
		if call.op == "hello" {
			usbHellos++
		}
	}
	if usbHellos == 0 {
		t.Fatal("ConnectUSB never said hello on the USB controller")
	}

	// the lease itself dialed no TCP, and a later eval reuses the warm
	// USB controller (no new dial of either seam)
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval over usb controller: %#v", rep)
	}
	if n := e.dialedTCP("d75884"); n != tcpBefore {
		t.Fatalf("tcp dials %d → %d during usb lease + eval: the USB route must not ride TCP", tcpBefore, n)
	}
	if n := e.dialedUSB("d75884"); n != 1 {
		t.Fatalf("usb dials = %d after eval, want still 1 (warm reuse)", n)
	}
}

// TestLeaseDefaultPathUnchanged: a plain lease (no transport) keeps the
// default TCP semantics — no warning keys in the reply, the USB seam
// never touched, the first op dials the warm TCP route.
func TestLeaseDefaultPathUnchanged(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	res := asMap(rep["result"])
	if _, ok := res["transport"]; ok {
		t.Fatalf("default lease reply carries transport: %#v", res)
	}
	if _, ok := res["warning"]; ok {
		t.Fatalf("default lease reply carries warning: %#v", res)
	}
	if n := e.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d after plain lease, want 0", n)
	}

	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	if n := e.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d after plain lease + eval, want 0 — no silent fallback", n)
	}
}

// TestLeaseUnknownTransport: a bogus transport value is a structured
// server-class error, not a silent default.
func TestLeaseUnknownTransport(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "transport": "can"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "server" {
		t.Fatalf("class = %q, want server (%#v)", er["class"], er)
	}
	if !strings.Contains(asStr(er["msg"]), "transport") {
		t.Fatalf("msg = %q, want it to name the transport", asStr(er["msg"]))
	}
	if n := e.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d, want 0", n)
	}
}

// TestFailedTCPDialNeverFallsBackToUSB: the never-auto-fallback rule —
// a board whose TCP dial fails answers unreachable and the USB seam
// stays untouched (USB happens ONLY through lease transport:"usb").
func TestFailedTCPDialNeverFallsBackToUSB(t *testing.T) {
	var mu sync.Mutex
	usbDials := 0
	usbFake := newFakeCtl()
	reg := discover.NewRegistry(nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			return []discover.Board{mkBoard("d75884", "xuansi-d75884.local")}, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			return []discover.Board{mkBoard("d75884", "xuansi-d75884.local")}, nil
		}),
	)
	sock := filepath.Join(t.TempDir(), "d.sock")
	srv, err := New(Opts{
		SockPath:        sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		RequestTimeout:  400 * time.Millisecond, // bound the dial retry loop
		AuditTimeout:    200 * time.Millisecond, // the audit's dead-TCP retries must not pin connectMu
		Log:             func(format string, args ...any) { t.Logf("srv: "+format, args...) },
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: func(format string, args ...any) { t.Logf("sup: "+format, args...) },
				DialTCP: func() (board.Controller, error) {
					return nil, fmt.Errorf("wifi-dead bench: tcp dial refused")
				},
				DialUSB: func() (board.Controller, error) {
					mu.Lock()
					usbDials++
					mu.Unlock()
					return usbFake, nil
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

	c := dialSock(t, sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "w1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease (host-side, default route): %#v", rep)
	}
	// the first board-touching op rides the dead TCP route → unreachable
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "holder": "w1"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "unreachable" {
		t.Fatalf("class = %q, want unreachable (%#v)", er["class"], er)
	}
	mu.Lock()
	n := usbDials
	mu.Unlock()
	if n != 0 {
		t.Fatalf("usb dials = %d after a failed TCP route, want 0 — no silent fallback, ever", n)
	}
}

// sawObserverYield reports whether the fake saw a hello with
// observer=true — and (ordered proof) that it ANSWERED: fakeCtl.Op
// refuses ops on a closed link, so a recorded yield hello must have
// landed before Close.
func sawObserverYield(f *fakeCtl) bool {
	for _, call := range f.got() {
		if call.op == "hello" {
			if ob, _ := call.args["observer"].(bool); ob {
				return true
			}
		}
	}
	return false
}

// TestReleaseUSBLeaseYieldsThenTCPLeaseTakesOver (§12 讓位): releasing
// a USB-transport lease yields the controller slot (observer hello)
// BEFORE closing the CDC link — the board cannot see a CDC close — so
// the USB controller is dropped and the very next (default TCP) lease
// takes the controller with no headless-reopen ritual.
func TestReleaseUSBLeaseYieldsThenTCPLeaseTakesOver(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "u1", "transport": "usb"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease usb: %#v", rep)
	}
	tcpDuringUSB := e.dialedTCP("d75884")

	rep, _ = c.call(2, map[string]any{"type": "release", "board": "d75884", "holder": "u1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("release: %#v", rep)
	}

	// yield-then-close on the USB controller, in that order
	if !sawObserverYield(e.usbFakes["d75884"]) {
		t.Fatal("release performed no §12 observer yield before closing the USB link")
	}
	if !e.usbFakes["d75884"].Dead() {
		t.Fatal("release must drop the USB controller (the next touch dials TCP)")
	}

	// the USB lease's swap closed the old TCP link (§12 close); a real
	// redial opens a FRESH socket — model that with a fresh fake
	e.dmu.Lock()
	e.fakes["d75884"] = newFakeCtl()
	e.dmu.Unlock()

	// the next lease — default transport, from a FRESH client — rides
	// TCP into the freed slot and answers immediately
	c2 := dialSock(t, e.sock)
	rep, _ = c2.call(3, map[string]any{"type": "lease", "board": "d75884", "holder": "u2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("tcp lease after usb release: %#v", rep)
	}
	rep, _ = c2.call(4, map[string]any{"type": "eval", "board": "d75884", "code": "return 2", "holder": "u2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval over tcp: %#v", rep)
	}
	if n := e.dialedTCP("d75884"); n <= tcpDuringUSB {
		t.Fatalf("tcp dials %d → %d — the post-release lease must dial TCP", tcpDuringUSB, n)
	}
	if !e.usbFakes["d75884"].Dead() {
		t.Fatal("the USB controller must stay dropped")
	}
}

// TestReleaseTCPLeaseUnchanged: the TCP release path gains NOTHING —
// no observer yield (§12: a TCP close releases the slot by itself) and
// the warm TCP controller stays connected across release.
func TestReleaseTCPLeaseUnchanged(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	tcpDialed := e.dialedTCP("d75884")

	rep, _ = c.call(3, map[string]any{"type": "release", "board": "d75884", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("release: %#v", rep)
	}
	if sawObserverYield(e.fakes["d75884"]) {
		t.Fatal("TCP release performed a pointless yield — §12 frees the slot on close alone")
	}
	if e.fakes["d75884"].Dead() {
		t.Fatal("TCP release must keep the warm controller (current behavior)")
	}

	// re-lease + eval reuses the warm TCP link: no new dial (same
	// holder — the connection is bound to it)
	rep, _ = c.call(4, map[string]any{"type": "lease", "board": "d75884", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("re-lease: %#v", rep)
	}
	rep, _ = c.call(5, map[string]any{"type": "eval", "board": "d75884", "code": "return 2", "holder": "d1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval after re-lease: %#v", rep)
	}
	if n := e.dialedTCP("d75884"); n != tcpDialed {
		t.Fatalf("tcp dials %d → %d — the warm TCP controller must be reused across release", tcpDialed, n)
	}
}

// TestShutdownYieldsUSBController: daemon shutdown while holding a live
// USB controller yields the §12 slot before closing the CDC link
// (srv.Close → supervisor.Close → yieldAndClose).
func TestShutdownYieldsUSBController(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "u1", "transport": "usb"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease usb: %#v", rep)
	}
	e.srv.Close() // idempotent with the cleanup's Close

	if !sawObserverYield(e.usbFakes["d75884"]) {
		t.Fatal("shutdown performed no §12 observer yield before closing the USB link")
	}
	if !e.usbFakes["d75884"].Dead() {
		t.Fatal("shutdown must close the USB controller")
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
// JSON) back to int64. Non-integral or ≥2^53 floats are refused
// daemon-side before any board contact (TestEvalArgsValidation).
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
		"args": map[string]any{"n": float64(7), "arr": []any{float64(1), float64(-2)}},
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

// TestEvalArgsValidation: the fw args bridge is int-only (v3 §4) — the
// daemon refuses the float shapes that cannot cross honestly BEFORE any
// board contact (class api, the brief's exact message naming the
// offending path), while integral float64s (JSON's only integer shape)
// convert to int64 so they msgpack-encode as ints; -2^63, the msgpack
// int64 floor, crosses exactly.
func TestEvalArgsValidation(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "arg"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	boardEvals := 0
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		boardEvals++
		return map[string]any{"out": ""}, nil
	})

	// rejections: class api, the exact message, the board untouched
	for i, tc := range []struct {
		name string
		args any
		want string
	}{
		{"fraction", map[string]any{"x": 0.5}, "eval args: x: non-integer or ≥2^53 number"},
		{"2^53 boundary (corruptible literal)", map[string]any{"x": 9007199254740993}, "eval args: x: non-integer or ≥2^53 number"},
		{"array leaf beyond 2^53", map[string]any{"a": []any{float64(1), 1e300}}, "eval args: a[1]: non-integer or ≥2^53 number"},
	} {
		rep, _ := c.call(int64(10+i), map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "args": tc.args})
		er := errOf(t, rep)
		if asStr(er["class"]) != "api" {
			t.Fatalf("%s: class = %q, want api (%#v)", tc.name, er["class"], er)
		}
		if asStr(er["msg"]) != tc.want {
			t.Fatalf("%s: msg = %q, want %q", tc.name, er["msg"], tc.want)
		}
		if boardEvals != 0 {
			t.Fatalf("%s reached the board (%d eval calls)", tc.name, boardEvals)
		}
	}

	// submit validates with the same message
	rep, _ = c.call(20, map[string]any{"type": "submit", "board": "d75884", "code": "return 1", "tag": "t1", "args": map[string]any{"x": 0.5}})
	er := errOf(t, rep)
	if asStr(er["class"]) != "api" || asStr(er["msg"]) != "eval args: x: non-integer or ≥2^53 number" {
		t.Fatalf("submit validation: %#v", er)
	}
	if boardEvals != 0 {
		t.Fatal("submit validation reached the board")
	}

	// integral crossings: the board receives int64 (msgpack int), the
	// int64 floor included
	var gotX any
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		m, _ := args["args"].(map[string]any)
		gotX = m["x"]
		return map[string]any{"out": ""}, nil
	})
	for i, x := range []float64{3.0, float64(math.MinInt64)} {
		rep, _ := c.call(int64(30+i), map[string]any{"type": "eval", "board": "d75884", "code": "return ARGS", "args": map[string]any{"x": x}})
		if asMap(rep["error"]) != nil {
			t.Fatalf("eval %v: %#v", x, rep)
		}
		v, ok := gotX.(int64)
		if !ok {
			t.Fatalf("%v crossed as %T (%#v), want int64 (msgpack int)", x, gotX, gotX)
		}
		if want := int64(x); v != want {
			t.Fatalf("%v crossed as %d, want %d", x, v, want)
		}
	}
}

// TestOpPassthroughAllowlist: the ops face — op forwards an allowlisted
// board op over the warm controller under eval's lease gate (uniformly:
// even the read-only diag/caps ops need the lease), the board's result
// crosses as the reply result, and a board error's class/msg pass
// through verbatim like eval's.
func TestOpPassthroughAllowlist(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	// uniform lease gate: even caps needs the lease first
	rep, _ := c.call(1, map[string]any{"type": "op", "board": "d75884", "op": "caps"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("no-lease class = %q, want not_held (%#v)", er["class"], er)
	}

	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "op1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	e.fakes["d75884"].on("budget.set", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"budget_ms": args["budget_ms"]}, nil
	})
	rep, _ = c.call(3, map[string]any{
		"type": "op", "board": "d75884", "op": "budget.set",
		"args": map[string]any{"budget_ms": float64(5000)},
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op budget.set: %#v", rep)
	}
	if asInt(asMap(rep["result"])["budget_ms"]) != 5000 {
		t.Fatalf("op result = %#v", rep["result"])
	}
	sent := false
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "budget.set" {
			sent = true
			if asInt(call.args["budget_ms"]) != 5000 {
				t.Fatalf("forwarded args = %#v", call.args)
			}
		}
	}
	if !sent {
		t.Fatal("op budget.set never reached the board")
	}

	// a board error crosses with its class/msg verbatim
	e.fakes["d75884"].on("wifi.setup", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return nil, &replclient.OpError{Op: "wifi.setup", Class: "api", Msg: "wifi.setup: ssid missing at byte 3"}
	})
	rep, _ = c.call(4, map[string]any{
		"type": "op", "board": "d75884", "op": "wifi.setup",
		"args": map[string]any{"ssid": ""},
	})
	er = errOf(t, rep)
	if asStr(er["class"]) != "api" || !strings.Contains(asStr(er["msg"]), "ssid missing at byte 3") {
		t.Fatalf("board error not verbatim: %#v", er)
	}
}

// TestOpAllowlistRefusal: anything off the allowlist — other diag.*,
// the reserved faces (eval/submit/fs.*/script.*/session.*), unknown or
// empty ops — is a daemon-side "server" class refusal naming the op
// and the ops-face allowlist; the board is never contacted for it.
func TestOpAllowlistRefusal(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "op2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	offList := []string{"diag.corrupt", "eval", "submit", "fs.write", "fs.read", "script.start", "session.reset", "budget.blast", ""}
	for i, op := range offList {
		rep, _ := c.call(int64(10+i), map[string]any{"type": "op", "board": "d75884", "op": op})
		er := errOf(t, rep)
		if asStr(er["class"]) != "server" {
			t.Fatalf("op %q: class = %q, want server (%#v)", op, er["class"], er)
		}
		msg := asStr(er["msg"])
		if !strings.Contains(msg, fmt.Sprintf("%q", op)) {
			t.Fatalf("op %q: msg does not name the op: %q", op, msg)
		}
		if !strings.Contains(msg, "allowlist") {
			t.Fatalf("op %q: msg does not name the allowlist: %q", op, msg)
		}
	}
	// none of the refused ops reached the board
	for _, call := range e.fakes["d75884"].got() {
		for _, op := range offList {
			if call.op == op {
				t.Fatalf("off-allowlist op %q reached the board", op)
			}
		}
	}
}

// TestOutboundQueueEvictionPolicy pins the delivery policy on one
// connection's queue: a full queue drops an incoming EVENT
// (fire-and-forget, unchanged) but a REPLY is never dropped — the
// oldest queued event is evicted to make room.
func TestOutboundQueueEvictionPolicy(t *testing.T) {
	s := &Server{logf: func(format string, args ...any) {}}
	cc := &clientConn{srv: s, outbound: make(chan any, outboundQ)}
	for i := 0; i < outboundQ; i++ {
		cc.send(EventPush{Type: "event", Board: "b", Event: WireEvent{Source: "gpio.4", Data: map[string]any{"seq": int64(i)}}})
	}

	// a full queue drops an incoming event, displacing nothing
	if !cc.send(EventPush{Type: "event", Board: "b", Event: WireEvent{Source: "gpio.4", Data: map[string]any{"seq": int64(outboundQ)}}}) {
		t.Fatal("event send reported connection gone")
	}
	if len(cc.outbound) != outboundQ {
		t.Fatalf("queue = %d, want still %d (an event must not displace anything)", len(cc.outbound), outboundQ)
	}

	// a reply evicts the OLDEST queued event and is itself queued
	if !cc.send(&Reply{Type: "reply", ID: 42}) {
		t.Fatal("reply send reported connection gone")
	}
	if len(cc.outbound) != outboundQ {
		t.Fatalf("queue = %d, want %d (one event evicted, the reply queued)", len(cc.outbound), outboundQ)
	}
	first := <-cc.outbound
	if fe, ok := first.(EventPush); !ok || asInt(fe.Event.Data["seq"]) != 1 {
		t.Fatalf("front = %#v, want the seq-1 event — seq 0 must be the evicted one", first)
	}
	var last any
	for len(cc.outbound) > 0 {
		last = <-cc.outbound
	}
	if r, ok := last.(*Reply); !ok || r.ID != 42 {
		t.Fatalf("back = %#v, want reply 42 — replies are never dropped", last)
	}
}

// TestFullQueueReplyDelivered: the delivery policy end to end — a
// watcher that stops reading first plugs the kernel socket buffer
// (256-KiB pad events, far past any sndbuf/rcvbuf) so the writer
// goroutine stalls and the 256-slot queue itself fills; the stalled
// watcher's request still gets its reply, events beyond the cap drop,
// and the connection stays live afterwards.
func TestFullQueueReplyDelivered(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	// the initial discovery sweep may still be mid-flight — poll until
	// the board is resolvable before watching (TestListBoardsShape's
	// pattern)
	deadline := time.Now().Add(3 * time.Second)
	for asMap(rep["error"]) != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		rep, _ = c.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	}
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}

	pad := strings.Repeat("x", 256<<10)
	for i := 0; i < 96; i++ {
		e.fakes["d75884"].push("gpio.4", map[string]any{"pad": pad})
	}
	for i := 1; i <= 600; i++ {
		e.fakes["d75884"].push("gpio.4", map[string]any{"edge": "rising", "seq": int64(i)})
	}

	// the stalled watcher's request still gets its reply — the oldest
	// queued event was evicted for it (delivery policy, proto.go)
	rep, events := c.call(2, map[string]any{"type": "list_boards"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("reply over a full queue: %#v", rep)
	}
	if len(events) > 96+outboundQ {
		t.Fatalf("received %d events, want ≤ %d — overflow must drop", len(events), 96+outboundQ)
	}

	// the connection is alive and delivering afterwards
	e.fakes["d75884"].push("gpio.4", map[string]any{"edge": "rising", "seq": int64(601)})
	ev := c.recv()
	if asInt(asMap(asMap(ev["event"])["data"])["seq"]) != 601 {
		t.Fatalf("post-reply event = %#v", ev)
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
