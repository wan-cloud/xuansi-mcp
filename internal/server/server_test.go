package server

// server_test.go — the task-11 unit face: the daemon's unix-socket API
// against a scripted fake board controller (the board.Controller seam
// SupOpts.DialTCP injects). The framing, the three message shapes, id
// pairing, the lease gating (holder semantics: bind / adopt /
// agent-gone), error-class preservation from the board, event fan-out,
// and graceful shutdown are all pinned here; the live-board behavior is
// the HIL/smoke's job (results/2026-11-05-b5-t11-*.log).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/lease"
	"xuansi/internal/replclient"
	"xuansi/internal/testutil"
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
	sink     replclient.StreamSink // the connection's current 0x02 sink (camera scripting drives it synchronously)
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
			"protocol_ver": int64(5), "role": "controller", "fw": "xuansi-fw v0.1.0",
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

// markDead flips the liveness flag (a scripted link death under a warm
// controller — the redial paths' trigger).
func (f *fakeCtl) markDead() {
	f.mu.Lock()
	f.dead = true
	f.mu.Unlock()
}

// OnStream stores the connection's 0x02 sink (nil clears).
func (f *fakeCtl) OnStream(sink replclient.StreamSink) {
	f.mu.Lock()
	f.sink = sink
	f.mu.Unlock()
}

// sinkSnapshot reads the current sink.
func (f *fakeCtl) sinkSnapshot() replclient.StreamSink {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sink
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
	res, err := f.Op(context.Background(), "hello", map[string]any{"client": name, "observer": observer})
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.hellos++
	f.mu.Unlock()
	// B10-T8b: the hello info now derives from the SCRIPTED op reply
	// (epoch/reset_reason/fw_build must be scriptable per test) with
	// the same defaults the old fixed struct carried.
	h := &replclient.HelloInfo{
		ProtocolVer: 5, Role: "controller", Fw: "xuansi-fw v0.1.0", FwBuild: "deadbee",
	}
	if v, ok := res["protocol_ver"].(int64); ok {
		h.ProtocolVer = v
	}
	if v, ok := res["role"].(string); ok {
		h.Role = v
	}
	if v, ok := res["fw_build"].(string); ok {
		h.FwBuild = v
	}
	if v, ok := res["reset"].(string); ok {
		h.Reset = v
	}
	if v, ok := res["reset_reason"].(string); ok {
		h.ResetReason = v
	}
	if v, ok := res["boot_epoch"].(int64); ok && v >= 0 && v <= math.MaxUint32 {
		h.BootEpoch = uint32(v)
	}
	if h.ResetReason == "" {
		h.ResetReason = h.Reset
	}
	return h, nil
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
	return newEnvFull(t, ttl, 0, aliases, boards...)
}

// newEnvIdle is newEnv with the idle-exit window set (0 = disabled; the
// idle-exit tests inject their own timer seam via afterFunc).
func newEnvIdle(t *testing.T, idle time.Duration, aliases map[string]string, boards ...discover.Board) *env {
	return newEnvFull(t, 10*time.Second, idle, aliases, boards...)
}

// newTestLogf builds the tests' server-side Log seam: daemon
// goroutines (conn drops, lease reaper teardowns, refresh loop) log
// through it, and any of them may still be running when the test body
// returns and cleanups start — a conn drop triggered by a cleanup's
// conn.Close, say. A bare t.Logf there is a data race against
// testing.tRunner's own state, which the testing package deliberately
// leaves unsynchronized; on this suite it surfaced as the intermittent
// -race "WARNING: DATA RACE ... newEnvFull.func3" flake (~1 run in 4,
// reproduced on clean HEAD under repetition — the loaded-hammer
// ambiguity the final review chased). The seam buffers under a mutex
// instead, and the cleanup dumps the buffer through t.Logf from the
// TEST goroutine itself (always legal) when the test failed or under
// -v — failure evidence kept, live streaming traded away, race gone.
func newTestLogf(t *testing.T, prefix string) func(format string, args ...any) {
	var mu sync.Mutex
	var lines []string
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		if !t.Failed() && !testing.Verbose() {
			return
		}
		for _, l := range lines {
			t.Logf("%s", l)
		}
	})
	return func(format string, args ...any) {
		mu.Lock()
		lines = append(lines, prefix+fmt.Sprintf(format, args...))
		mu.Unlock()
	}
}

// newEnvFull is the shared env builder: a custom grace TTL plus the
// idle-exit window (0 = the disabled default every older test relies on).
func newEnvFull(t *testing.T, ttl, idle time.Duration, aliases map[string]string, boards ...discover.Board) *env {
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
	e.sock = filepath.Join(testutil.ShortSockDir(t), "d.sock")
	srvLog := newTestLogf(t, "srv: ")
	supLog := newTestLogf(t, "sup: ")
	srv, err := New(Opts{
		SockPath:        e.sock,
		Registry:        reg,
		GraceTTL:        ttl,  // HIL-short by default; no test waits it out
		IdleExit:        idle, // 0 = disabled unless an idle-exit test sets it
		RefreshInterval: -1,   // no background loop; the initial sweep still runs
		Log:             srvLog,
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: supLog,
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
	awaitSweep(t, srv.reg, len(boards))
	return e
}

// sweepWait bounds awaitSweep and names its bound in the failure
// message (the message cannot lie if the bound is ever tuned).
const sweepWait = 2 * time.Second

// awaitSweep waits bounded for the initial discovery sweep to land its
// boards. The sweep runs ASYNC inside Start (the startupAudit
// goroutine — the production shape, deliberately untouched): a test's
// first resolve racing it reads an EMPTY registry — the no_board
// "(none known)" startup lottery (~1/3 full-suite runs under load,
// hitting a different first-lease test each time). Every env helper
// waits here instead; want 0 (a zero-board env, empty by design)
// returns immediately, and a sweep that genuinely cannot land its
// boards inside the bound fails loudly instead of flaking downstream.
func awaitSweep(t *testing.T, reg *discover.Registry, want int) {
	t.Helper()
	deadline := time.Now().Add(sweepWait)
	for len(reg.Snapshot()) < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(reg.Snapshot()); got < want {
		t.Fatalf("initial discovery sweep never landed its board(s): registry has %d of %d after %s", got, want, sweepWait)
	}
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

// recvDeadline bounds one recv() wait. Generous (15s, final-review
// minor): the old 4s produced one ambiguous timeout during the
// reviewer's loaded-machine -race hammer — with the whole suite
// stressing the box a legal reply can take seconds, and the deadline
// exists to fail a hung test today, not to police latency.
var recvDeadline = 15 * time.Second

// recv reads one framed JSON message into a map (deadline-bounded).
func (c *sockClient) recv() map[string]any {
	c.t.Helper()
	c.conn.SetDeadline(time.Now().Add(recvDeadline))
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
// (unsolicited pushes — events, capture_done — on the wire are skipped
// and returned alongside; capture tests that assert pushes use the
// send+collect pattern instead).
func (c *sockClient) call(id int64, req map[string]any) (reply map[string]any, events []map[string]any) {
	c.t.Helper()
	req["id"] = id
	c.send(req)
	for {
		m := c.recv()
		if m["type"] == "event" || m["type"] == "capture_done" {
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
// connection each get the reply carrying their own id. The replies may
// legally complete in EITHER order — the server runs each request's
// handler independently and the writer serializes bytes, not
// completion order — so the two requests are pipelined (both sent
// before either reply is read) and replies are paired centrally by id.
// The old shape (two concurrent send+read call()s, each reader
// assuming its own reply arrives first) was the long-known
// cross-platform flake.
func TestReplyIDPairingUnderConcurrency(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	// lease first so both evals are legal
	rep, _ := c.call(100, map[string]any{"type": "lease", "board": "d75884", "holder": "pair"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	// pipeline: both requests outstanding at once (sends are framed
	// one Write each — the wire is not a parallel-write surface)
	for i := 0; i < 2; i++ {
		c.send(map[string]any{
			"type": "eval", "board": "d75884",
			"code": fmt.Sprintf("return %d", i),
			"id":   int64(200 + i),
		})
	}

	replies := map[int64]map[string]any{}
	for len(replies) < 2 {
		m := c.recv()
		if m["type"] == "event" || m["type"] == "capture_done" {
			t.Fatalf("unexpected push during eval pairing: %#v", m)
		}
		id := asInt(m["id"])
		if id != 200 && id != 201 {
			t.Fatalf("unexpected reply id %d: %#v", id, m)
		}
		if _, dup := replies[id]; dup {
			t.Fatalf("duplicate reply id %d", id)
		}
		replies[id] = m
	}
	for i := 0; i < 2; i++ {
		id := int64(200 + i)
		if asMap(replies[id]["error"]) != nil {
			t.Errorf("eval %d: %#v", id, replies[id])
		}
		if got := asInt(replies[id]["id"]); got != id {
			t.Errorf("reply id = %v, want %d", got, id)
		}
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

// TestCloseWaitsForInflightConnect: Close's sup.Close must run under
// connectMu. A connect parked mid-flight (here: the lease USB route,
// blocked on its controller's first hello while hLease holds
// connectMu) finishes after Close started; when sup.Close runs outside
// the critical section it sees no controller yet (ConnectUSB installs
// it only after the hello) and the connect's fresh controller link is
// never closed — a re-dial racing the close leaks one extra controller
// connection. With sup.Close inside the critical section, Close waits
// for the connect and reclaims what it installed.
func TestCloseWaitsForInflightConnect(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	// let the startup audit's TCP dial settle (same idiom as
	// TestLeaseUSBTransport) so the lease connect below is the only
	// traffic on this board's supervisor
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.dialedTCP("d75884") == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	// gate: the USB controller's FIRST hello blocks — parking ConnectUSB
	// (and hLease's connectMu critical section) mid-connect. Later
	// hellos (the §12 yield) pass through once the gate is open.
	usb := e.usbFakes["d75884"]
	release := make(chan struct{})
	usb.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		<-release
		return map[string]any{
			"protocol_ver": int64(5), "role": "controller", "fw": "xuansi-fw v0.1.0",
			"fw_build": "deadbee", "fs": true,
		}, nil
	})

	c.send(map[string]any{"type": "lease", "board": "d75884", "holder": "u1", "transport": "usb"})
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.dialedUSB("d75884") == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if n := e.dialedUSB("d75884"); n != 1 {
		t.Fatalf("usb dials = %d, want 1 — lease never reached the USB seam", n)
	}
	// the dial happened; give the hello a beat to park in the gate so
	// the racing Close below cannot win by scheduling luck
	time.Sleep(20 * time.Millisecond)

	// Close races the in-flight connect: it runs on its own goroutine
	// because with sup.Close under connectMu it WAITS for the parked
	// connect.
	closed := make(chan struct{})
	go func() {
		e.srv.Close()
		close(closed)
	}()
	// Barrier: the sock file is removed by Close strictly AFTER the
	// sups loop, i.e. after the sup.Close decision point. When it
	// disappears while the connect is still parked, sup.Close has
	// already run (and seen no controller — ConnectUSB installs one
	// only after its hello). With sup.Close under connectMu the file
	// cannot disappear yet, so this poll just times out and opens the
	// gate with Close still parked behind connectMu.
	deadline = time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(e.sock); os.IsNotExist(err) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// the connect finishes and installs the fresh controller
	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatalf("Close did not return within 5s of the connect finishing")
	}
	select {
	case <-e.srv.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("server goroutines did not finish within 5s of Close+release")
	}
	if !usb.Dead() {
		t.Fatalf("USB controller left alive after Close — sup.Close ran outside connectMu and missed the racing connect's fresh controller (leaked connection)")
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
	sock := filepath.Join(testutil.ShortSockDir(t), "d.sock")
	srv, err := New(Opts{
		SockPath:        sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		RequestTimeout:  400 * time.Millisecond, // bound the dial retry loop
		AuditTimeout:    200 * time.Millisecond, // the audit's dead-TCP retries must not pin connectMu
		Log:             newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
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
	awaitSweep(t, reg, 1) // the sweep races this test's first lease — wait it out

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

// TestFsWriteLeaseGateAndFsRead: fs_write is lease-gated; fs_read is
// read-only (no lease needed). The 16KB cap refusal is GONE (B10-T1 —
// bigger writes ride the chunked transport, pinned in
// fs_chunked_test.go).
func TestFsWriteLeaseGateAndFsRead(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	newChunkedFake(t, e, "d75884") // fs_read rides fs.read_part since B10-T1
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "fs_write", "board": "d75884", "path": "/lua/x.lua", "data": base64.StdEncoding.EncodeToString([]byte("x"))})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("class = %q, want not_held", er["class"])
	}

	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "fs"})
	_ = rep
	rep, _ = c.call(3, map[string]any{"type": "fs_write", "board": "d75884", "path": "/lua/x.lua", "data": base64.StdEncoding.EncodeToString([]byte("x"))})
	if asMap(rep["error"]) != nil {
		t.Fatalf("small fs_write: %#v", rep)
	}

	rep, _ = c.call(4, map[string]any{"type": "fs_read", "board": "d75884", "path": "/lua/x.lua"})
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
// board contact (class api, the exact message naming the offending
// path), while integral float64s (JSON's only integer shape) convert to
// int64 so they msgpack-encode as ints. The ±2^53 guard is
// sign-symmetric (controller ruling: JSON-authored integers beyond
// ±2^53 may have already lost precision client-side): ±2^53 and
// everything beyond — the int64 floor -2^63 included — reject; the
// largest passing integers are ±(2^53-1).
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
		{"fraction", map[string]any{"x": 0.5}, "eval args: x: non-integer or |v|≥2^53 number"},
		{"+2^53 (corruptible literal 2^53+1)", map[string]any{"x": 9007199254740993}, "eval args: x: non-integer or |v|≥2^53 number"},
		{"+2^53 exactly", map[string]any{"x": 9007199254740992}, "eval args: x: non-integer or |v|≥2^53 number"},
		{"-2^53 exactly", map[string]any{"x": -9007199254740992}, "eval args: x: non-integer or |v|≥2^53 number"},
		{"-2^53-2 (corruptible literal)", map[string]any{"x": -9007199254740994}, "eval args: x: non-integer or |v|≥2^53 number"},
		{"-2^63 (int64 floor, beyond the guard)", map[string]any{"x": float64(math.MinInt64)}, "eval args: x: non-integer or |v|≥2^53 number"},
		{"array leaf beyond 2^53", map[string]any{"a": []any{float64(1), 1e300}}, "eval args: a[1]: non-integer or |v|≥2^53 number"},
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
	if asStr(er["class"]) != "api" || asStr(er["msg"]) != "eval args: x: non-integer or |v|≥2^53 number" {
		t.Fatalf("submit validation: %#v", er)
	}
	if boardEvals != 0 {
		t.Fatal("submit validation reached the board")
	}

	// integral crossings: the board receives int64 (msgpack int), the
	// ±(2^53-1) boundaries included
	var gotX any
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		m, _ := args["args"].(map[string]any)
		gotX = m["x"]
		return map[string]any{"out": ""}, nil
	})
	for i, x := range []float64{3.0, 9007199254740991, -9007199254740991} {
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
// through verbatim like eval's. args run through the shared numeric
// walk (controller ruling): JSON integers cross as msgpack ints.
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
			if v, ok := call.args["budget_ms"].(int64); !ok || v != 5000 {
				t.Fatalf("forwarded args = %#v, want budget_ms int64 5000 (msgpack int)", call.args)
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

// TestOpBudgetGetTranslation: budget.get has no wire op of its own
// (the frozen §4 table has only budget.set, which refuses its no-keys
// form), so the ops face reads caps — whose limits block reflects the
// runtime budgets — and projects the {eval_ms, event_ms} pair it
// promises. The board must never see a budget.get op (b6 full-HIL
// finding: the unit fake used to accept the raw name the daemon sent).
func TestOpBudgetGetTranslation(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "opg"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	e.fakes["d75884"].on("caps", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"api_version": float64(1),
			"limits":      map[string]any{"eval_budget_ms": float64(3000), "event_budget_ms": float64(50)},
		}, nil
	})
	rep, _ = c.call(2, map[string]any{"type": "op", "board": "d75884", "op": "budget.get"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op budget.get: %#v", rep)
	}
	res := asMap(rep["result"])
	if asInt(res["eval_ms"]) != 3000 || asInt(res["event_ms"]) != 50 {
		t.Fatalf("budget.get result = %#v, want the projected applied pair", rep["result"])
	}
	sawCaps := false
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "budget.get" {
			t.Fatal("budget.get crossed to the board verbatim — the wire has no such op")
		}
		if call.op == "caps" {
			sawCaps = true
		}
	}
	if !sawCaps {
		t.Fatal("the caps read behind budget.get never reached the board")
	}
}

// TestOpBudgetGetNilLimitsGuard (B7 minors item 3): budget.get's caps
// projection must not paper over an off-contract caps reply. Pinned
// pre-fix behavior (red): a caps reply WITHOUT the limits block
// answered {"eval_ms": null, "event_ms": null} — silent JSON nulls
// reading as "budgets are null" — and a PARTIAL limits block projected
// nulls per missing key. Guard (chosen per the pin): a missing limits
// BLOCK is a structured api-class refusal (the fw's caps always
// carries one — its absence is drift the caller must see, not
// misread); a missing INDIVIDUAL key inside a present limits block
// stays honest as an ABSENT projection key (the board did not report
// it; null would claim it reported nothing).
func TestOpBudgetGetNilLimitsGuard(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "opg2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	// caps WITHOUT the limits block: refusal, not silent nulls
	e.fakes["d75884"].on("caps", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"api_version": float64(1), "primitives": map[string]any{}}, nil
	})
	rep, _ = c.call(2, map[string]any{"type": "op", "board": "d75884", "op": "budget.get"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "api" || !strings.Contains(asStr(er["msg"]), "limits") {
		t.Fatalf("caps without limits: error = %#v, want an api-class refusal naming the limits block", er)
	}

	// PARTIAL limits: the present key projects, the missing key is ABSENT
	e.fakes["d75884"].on("caps", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"api_version": float64(1),
			"limits":      map[string]any{"eval_budget_ms": float64(3000)},
		}, nil
	})
	rep, _ = c.call(3, map[string]any{"type": "op", "board": "d75884", "op": "budget.get"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("partial-limits budget.get: %#v", rep)
	}
	res := asMap(rep["result"])
	if asInt(res["eval_ms"]) != 3000 {
		t.Errorf("partial-limits result = %#v, want eval_ms 3000", res)
	}
	if v, has := res["event_ms"]; has {
		t.Errorf("partial-limits result carries event_ms=%v, want the unreported key ABSENT", v)
	}
}

// TestOpArgsValidation: op args run through the SAME numeric walk as
// eval/submit (controller ruling — the motivating case: op budget.set
// {"eval_ms":3000} would otherwise encode 3000 as a msgpack float the
// fw refuses). JSON integers reach the board as int64; non-integral or
// ±2^53 numbers reject daemon-side with the api class under the
// "op args:" prefix, board untouched; strings cross unchanged.
func TestOpArgsValidation(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "oparg"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	// the ruling's motivating case: budget.set's JSON integers cross as
	// msgpack ints (a raw float64 would be refused by the fw bridge)
	var gotEvalMs any
	e.fakes["d75884"].on("budget.set", func(args map[string]any) (map[string]any, *replclient.OpError) {
		gotEvalMs = args["eval_ms"]
		return map[string]any{"eval_ms": args["eval_ms"]}, nil
	})
	rep, _ = c.call(2, map[string]any{
		"type": "op", "board": "d75884", "op": "budget.set",
		"args": map[string]any{"eval_ms": float64(3000)},
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op budget.set: %#v", rep)
	}
	if v, ok := gotEvalMs.(int64); !ok || v != 3000 {
		t.Fatalf("eval_ms crossed as %#v, want int64 3000 (msgpack int)", gotEvalMs)
	}

	// strings (wifi.setup's ssid) cross the walk untouched
	var gotSsid any
	e.fakes["d75884"].on("wifi.setup", func(args map[string]any) (map[string]any, *replclient.OpError) {
		gotSsid = args["ssid"]
		return map[string]any{"state": "joining"}, nil
	})
	rep, _ = c.call(3, map[string]any{
		"type": "op", "board": "d75884", "op": "wifi.setup",
		"args": map[string]any{"ssid": "bench-iot"},
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op wifi.setup: %#v", rep)
	}
	if gotSsid != "bench-iot" {
		t.Fatalf("ssid crossed as %#v, want the string verbatim", gotSsid)
	}

	// rejects: api class under the "op args:" prefix, board untouched
	callsBefore := len(e.fakes["d75884"].got())
	for i, tc := range []struct {
		name string
		args any
		want string
	}{
		{"fraction", map[string]any{"x": 0.5}, "op args: x: non-integer or |v|≥2^53 number"},
		{"+2^53", map[string]any{"x": 9007199254740992}, "op args: x: non-integer or |v|≥2^53 number"},
		{"-2^53-2", map[string]any{"x": -9007199254740994}, "op args: x: non-integer or |v|≥2^53 number"},
		{"array leaf", map[string]any{"a": []any{float64(1), 1e300}}, "op args: a[1]: non-integer or |v|≥2^53 number"},
	} {
		rep, _ := c.call(int64(10+i), map[string]any{"type": "op", "board": "d75884", "op": "budget.set", "args": tc.args})
		er := errOf(t, rep)
		if asStr(er["class"]) != "api" {
			t.Fatalf("%s: class = %q, want api (%#v)", tc.name, er["class"], er)
		}
		if asStr(er["msg"]) != tc.want {
			t.Fatalf("%s: msg = %q, want %q", tc.name, er["msg"], tc.want)
		}
	}
	if got := len(e.fakes["d75884"].got()); got != callsBefore {
		t.Fatalf("rejected op args reached the board (%d → %d calls)", callsBefore, got)
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

// opsFaceAllowlistV1 is the ops face's frozen allowlist, transcribed
// independently of the opAllowlist table (benchschema-style): the
// single-source test below fails when the table, the hOp refusal's
// rendered list, or either MCP description (pinned separately in
// internal/mcp) drifts from it. diag.i2cslave joined with b7 — the
// driver-pipeline loopback's on-board fixture toggle; camera.capture/
// status/off joined with b9 (the camera face — capture's 0x02 stream
// is consumed by the dedicated camera_shot request; through op it
// returns the metadata reply and the stream is dropped daemon-side).
var opsFaceAllowlistV1 = []string{
	"budget.get", "budget.set", "camera.capture", "camera.off",
	"camera.status", "caps", "diag.heapinfo", "diag.i2cslave",
	"diag.pins", "diag.stats", "log.read", "wifi.off", "wifi.setup",
	"wifi.status",
}

// TestOpAllowlistSingleSource: the ops face's allowlist is single-source
// — opAllowlist (server.go) is the only truth; the hOp refusal's op list
// renders from it (AllowlistedOps) and the MCP xuansi_board_op
// descriptions quote the same set (pinned in internal/mcp). This fails
// when the table drifts from the frozen contract or a listing stops
// matching the table — an op added to one but not the other.
func TestOpAllowlistSingleSource(t *testing.T) {
	keys := make([]string, 0, len(opAllowlist))
	for op := range opAllowlist {
		keys = append(keys, op)
	}
	sort.Strings(keys)
	if got, want := strings.Join(keys, ", "), strings.Join(opsFaceAllowlistV1, ", "); got != want {
		t.Fatalf("opAllowlist = [%s], want the frozen %d-op set [%s]", got, len(opsFaceAllowlistV1), want)
	}

	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "op", "board": "d75884", "op": "diag.corrupt"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "server" {
		t.Fatalf("off-allowlist class = %q, want server (%#v)", er["class"], er)
	}
	msg := asStr(er["msg"])
	i := strings.Index(msg, "allowlist (")
	j := strings.Index(msg, ") —")
	if i < 0 || j < i {
		t.Fatalf("refusal does not render the allowlist list: %q", msg)
	}
	got := strings.Split(msg[i+len("allowlist ("):j], ", ")
	if g, w := strings.Join(got, ", "), strings.Join(opsFaceAllowlistV1, ", "); g != w {
		t.Fatalf("refusal lists [%s], want the table's [%s]", g, w)
	}
}

// TestOpDiagI2CSlaveForwarded: diag.i2cslave — the board's internal I2C
// echo-slave loopback fixture toggle ({mode:-1..1} = status/off/on), the
// b7 driver-pipeline loopback's sparring partner — is allowlisted like
// every other op: no special-casing, the same uniform lease gate, args
// through the shared numeric walk (mode crosses as a msgpack int), and
// the board's reply as the result.
func TestOpDiagI2CSlaveForwarded(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	// the uniform lease gate applies — diag.i2cslave is not special
	rep, _ := c.call(1, map[string]any{"type": "op", "board": "d75884", "op": "diag.i2cslave"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("no-lease class = %q, want not_held (%#v)", er["class"], er)
	}

	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "i2cslv"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	var gotMode any
	e.fakes["d75884"].on("diag.i2cslave", func(args map[string]any) (map[string]any, *replclient.OpError) {
		gotMode = args["mode"]
		return map[string]any{"mode": args["mode"], "state": "on"}, nil
	})
	rep, _ = c.call(3, map[string]any{
		"type": "op", "board": "d75884", "op": "diag.i2cslave",
		"args": map[string]any{"mode": float64(1)},
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op diag.i2cslave: %#v", rep)
	}
	if v, ok := gotMode.(int64); !ok || v != 1 {
		t.Fatalf("mode crossed as %#v, want int64 1 (msgpack int)", gotMode)
	}
	res := asMap(rep["result"])
	if asInt(res["mode"]) != 1 || asStr(res["state"]) != "on" {
		t.Fatalf("diag.i2cslave result = %#v, want the board's reply", rep["result"])
	}
}

// scriptCamera wires the fake's camera.capture: two SOI/EOI frames
// streamed as ch=1 two-chunk sequences through the connection's
// registered sink BEFORE the reply (the fw's frames-then-reply order).
// gap drops frame 0's second chunk — a daemon-side reassembly loss the
// board's reply cannot know about (it still reports frames=2).
func scriptCamera(f *fakeCtl, gap bool) {
	frames := [][]byte{
		{0xFF, 0xD8, 1, 2, 3, 0xFF, 0xD9},
		{0xFF, 0xD8, 4, 5, 6, 7, 0xFF, 0xD9},
	}
	f.on("camera.capture", func(args map[string]any) (map[string]any, *replclient.OpError) {
		sink := f.sinkSnapshot()
		total := int64(0)
		for i, fr := range frames {
			half := len(fr) / 2
			sink(1, uint32(7+i), 0, 2, fr[:half])
			if !(gap && i == 0) {
				sink(1, uint32(7+i), 1, 2, fr[half:])
			}
			total += int64(len(fr))
		}
		return map[string]any{
			"frames": int64(2), "bytes": total, "w": int64(640), "h": int64(480),
			"format": "JPEG", "sensor": "GC2145", "seq0": int64(7),
		}, nil
	})
}

// TestCameraShotLandsFiles: the dedicated camera_shot request — lease
// gate, args shaping, per-frame JPEG landing under the captures root
// (O_EXCL-unique <epoch>-<mac6>-camera[-N].jpg, one file per frame,
// sha256/bytes/w/h per file), and the gap path: a lost chunk drops its
// frame from files and counts in gaps while the call still succeeds.
func TestCameraShotLandsFiles(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	e.srv.mu.Lock()
	e.srv.captureRoot = t.TempDir()
	e.srv.mu.Unlock()
	c := dialSock(t, e.sock)

	// the lease gate applies — camera.capture is not ro
	rep, _ := c.call(1, map[string]any{"type": "camera_shot", "board": "d75884"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("no-lease class = %q, want not_held (%#v)", er["class"], er)
	}

	// daemon-side validation, before any board contact
	rep, _ = c.call(2, map[string]any{"type": "camera_shot", "board": "d75884", "framesize": "SVGA"})
	er = errOf(t, rep)
	if asStr(er["class"]) != "api" || !strings.Contains(asStr(er["msg"]), "QVGA") || !strings.Contains(asStr(er["msg"]), "VGA") {
		t.Fatalf("bad framesize = %s: %q, want api naming QVGA and VGA", er["class"], asStr(er["msg"]))
	}
	rep, _ = c.call(3, map[string]any{"type": "camera_shot", "board": "d75884", "frames": float64(5)})
	er = errOf(t, rep)
	if asStr(er["class"]) != "api" || !strings.Contains(asStr(er["msg"]), "1..4") {
		t.Fatalf("bad frames = %s: %q, want api naming the 1..4 range", er["class"], asStr(er["msg"]))
	}
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "camera.capture" {
			t.Fatal("a refused camera_shot reached the board")
		}
	}

	rep, _ = c.call(4, map[string]any{"type": "lease", "board": "d75884", "holder": "camshot"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	scriptCamera(e.fakes["d75884"], false)
	rep, _ = c.call(5, map[string]any{"type": "camera_shot", "board": "d75884", "frames": float64(2), "framesize": "VGA"})
	res := asMap(rep["result"])
	if res == nil {
		t.Fatalf("camera_shot: %#v", rep)
	}
	files, _ := res["files"].([]any)
	if len(files) != 2 {
		t.Fatalf("files = %d entries, want 2 (%#v)", len(files), res)
	}
	if g := asInt(res["gaps"]); g != 0 {
		t.Fatalf("gaps = %d, want 0", g)
	}
	if s := asStr(res["sensor"]); s != "GC2145" {
		t.Fatalf("sensor = %q, want GC2145", s)
	}
	wantBody := [][]byte{
		{0xFF, 0xD8, 1, 2, 3, 0xFF, 0xD9},
		{0xFF, 0xD8, 4, 5, 6, 7, 0xFF, 0xD9},
	}
	root := ""
	e.srv.mu.Lock()
	root = e.srv.captureRoot
	e.srv.mu.Unlock()
	for i, fe := range files {
		fm, _ := fe.(map[string]any)
		path := asStr(fm["path"])
		wantName := fmt.Sprintf("%d-d75884-camera-%d.jpg", time.Now().Unix(), i+1)
		if filepath.Base(path) != wantName {
			t.Fatalf("file[%d] name = %q, want %q", i, filepath.Base(path), wantName)
		}
		if !strings.HasPrefix(path, root) {
			t.Fatalf("file[%d] outside the captures root: %q (root %q)", i, path, root)
		}
		body, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("file[%d] read: %v", i, rerr)
		}
		if !bytes.Equal(body, wantBody[i]) {
			t.Fatalf("file[%d] body = %v, want %v", i, body, wantBody[i])
		}
		sum := sha256.Sum256(wantBody[i])
		if asStr(fm["sha256"]) != fmt.Sprintf("%x", sum) {
			t.Fatalf("file[%d] sha256 = %q, want %x", i, asStr(fm["sha256"]), sum)
		}
		if asInt(fm["bytes"]) != int64(len(wantBody[i])) || asInt(fm["w"]) != 640 || asInt(fm["h"]) != 480 {
			t.Fatalf("file[%d] bytes/w/h = %d/%d/%d", i, asInt(fm["bytes"]), asInt(fm["w"]), asInt(fm["h"]))
		}
	}

	// args crossed as msgpack ints / verbatim strings
	var camArgs map[string]any
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "camera.capture" {
			camArgs = call.args
		}
	}
	if camArgs == nil {
		t.Fatal("camera.capture never reached the board")
	}
	if v, ok := camArgs["frames"].(int64); !ok || v != 2 {
		t.Fatalf("args[frames] = %#v, want int64 2", camArgs["frames"])
	}
	if v, _ := camArgs["framesize"].(string); v != "VGA" {
		t.Fatalf("args[framesize] = %#v, want VGA", camArgs["framesize"])
	}

	// the gap path: frame 0's chunk loss drops that file, counts once,
	// and the call still succeeds with what landed
	scriptCamera(e.fakes["d75884"], true)
	rep, _ = c.call(6, map[string]any{"type": "camera_shot", "board": "d75884"})
	res = asMap(rep["result"])
	if res == nil {
		t.Fatalf("gap camera_shot: %#v", rep)
	}
	files, _ = res["files"].([]any)
	if len(files) != 1 || asInt(res["gaps"]) != 1 {
		t.Fatalf("gap path: %d files, gaps=%d, want 1 and 1", len(files), asInt(res["gaps"]))
	}
	fm, _ := files[0].(map[string]any)
	body, _ := os.ReadFile(asStr(fm["path"]))
	if !bytes.Equal(body, wantBody[1]) {
		t.Fatalf("gap path landed frame = %v, want the intact frame 1", body)
	}
}

// TestCameraShotRedialsDeadLink: a dead link under the warm controller
// costs nothing — the next camera_shot redials (fresh TCP, no reboot),
// and the retried call's stream sink lands on the FRESH connection
// before its op (the per-connection sink ruling; a redial's fresh link
// starts with no sink).
func TestCameraShotRedialsDeadLink(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	e.srv.mu.Lock()
	e.srv.captureRoot = t.TempDir()
	e.srv.mu.Unlock()
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "camredial"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	scriptCamera(e.fakes["d75884"], false)
	rep, _ = c.call(2, map[string]any{"type": "camera_shot", "board": "d75884"})
	if asMap(rep["result"]) == nil {
		t.Fatalf("warm-up camera_shot: %#v", rep)
	}
	if e.dialed["d75884"] != 1 {
		t.Fatalf("dials = %d, want 1 before the death", e.dialed["d75884"])
	}

	// the link dies; a FRESH fake answers the redial with a working
	// camera (the sink must register on IT, not the corpse)
	fresh := newFakeCtl()
	scriptCamera(fresh, false)
	e.fakes["d75884"].markDead()
	e.dmu.Lock()
	e.fakes["d75884"] = fresh
	e.dmu.Unlock()

	rep, _ = c.call(3, map[string]any{"type": "camera_shot", "board": "d75884"})
	res := asMap(rep["result"])
	if res == nil {
		t.Fatalf("post-death camera_shot: %#v", rep)
	}
	files, _ := res["files"].([]any)
	if len(files) != 2 || asInt(res["gaps"]) != 0 {
		t.Fatalf("post-death files=%d gaps=%d, want 2 and 0 (the fresh link's stream consumed)", len(files), asInt(res["gaps"]))
	}
	if e.dialed["d75884"] != 2 {
		t.Fatalf("dials = %d, want exactly one redial", e.dialed["d75884"])
	}
}

// TestOpCameraCaptureForwarded: camera.capture crosses the ops face
// like every allowlisted op — uniform lease gate, args through the
// shared walk — under the camera mutex hold (its 0x02 chunks ride the
// warm connection with no sink registered on this path, so nothing
// lands and nothing interleaves; the board's metadata reply is the
// result). The mutex is the b9-review minor-4 hardening: a concurrent
// camera_shot must never share its connection with op-path chunks.
func TestOpCameraCaptureForwarded(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "op", "board": "d75884", "op": "camera.capture"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "not_held" {
		t.Fatalf("no-lease class = %q, want not_held (%#v)", er["class"], er)
	}

	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "camop"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	var gotFrames any
	e.fakes["d75884"].on("camera.capture", func(args map[string]any) (map[string]any, *replclient.OpError) {
		gotFrames = args["frames"]
		return map[string]any{"frames": args["frames"], "bytes": int64(1234), "w": int64(640), "h": int64(480), "sensor": "GC2145"}, nil
	})
	rep, _ = c.call(3, map[string]any{
		"type": "op", "board": "d75884", "op": "camera.capture",
		"args": map[string]any{"frames": float64(2)},
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op camera.capture: %#v", rep)
	}
	if v, ok := gotFrames.(int64); !ok || v != 2 {
		t.Fatalf("args[frames] crossed as %#v, want int64 2", gotFrames)
	}
	res := asMap(rep["result"])
	if asStr(res["sensor"]) != "GC2145" || asInt(res["frames"]) != 2 {
		t.Fatalf("camera.capture result = %#v, want the board's metadata reply", rep["result"])
	}
}

// TestOutboundQueueEvictionPolicy pins the delivery policy on one
// connection's queue: a full queue drops an incoming EVENT
// (fire-and-forget, unchanged) but a REPLY is never dropped — the
// oldest queued event is evicted to make room, with no reply lost.
// (The all-replies wedge is TestWedgedReplyQueueBounded's job.)
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
	if cc.repliesLost != 0 {
		t.Fatalf("repliesLost = %d, want 0 — the event-eviction path loses no replies", cc.repliesLost)
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

	// after the drain a plain send enqueues without any eviction
	if !cc.send(&Reply{Type: "reply", ID: 43}) {
		t.Fatal("post-drain send reported connection gone")
	}
	if cc.repliesLost != 0 {
		t.Fatalf("repliesLost = %d, want 0 — a drained queue must not take the last-resort path", cc.repliesLost)
	}
}

// TestWedgedReplyQueueBounded pins review round 1's wedge: a client
// that parks 256 replies (pipelined requests, stopped reading) and is
// handed one more reply must not block — the old fallback stalled
// forever holding wmu. The bounded wait expires, the OLDEST queued
// reply is evicted — logged and counted — and the new reply takes the
// slot.
func TestWedgedReplyQueueBounded(t *testing.T) {
	s := &Server{logf: func(format string, args ...any) {}}
	cc := &clientConn{srv: s, outbound: make(chan any, outboundQ), replyWait: 5 * time.Millisecond}
	for i := 0; i < outboundQ; i++ {
		cc.outbound <- &Reply{Type: "reply", ID: int64(1000 + i)}
	}

	done := make(chan bool, 1)
	go func() { done <- cc.send(&Reply{Type: "reply", ID: 4242}) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("send reported connection gone")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reply send wedged — the fallback must be bounded, never a stall")
	}

	if cc.repliesLost != 1 {
		t.Fatalf("repliesLost = %d, want 1 — the eviction must be accounted", cc.repliesLost)
	}
	if len(cc.outbound) != outboundQ {
		t.Fatalf("queue = %d, want %d (one evicted, ours queued)", len(cc.outbound), outboundQ)
	}
	front := <-cc.outbound
	if r, ok := front.(*Reply); !ok || r.ID != 1001 {
		t.Fatalf("front = %#v, want reply 1001 — 1000 is the accounted eviction", front)
	}
	var last any
	for len(cc.outbound) > 0 {
		last = <-cc.outbound
	}
	if r, ok := last.(*Reply); !ok || r.ID != 4242 {
		t.Fatalf("back = %#v, want reply 4242 — the new reply must be queued", last)
	}

	// after the drain a plain send enqueues directly (no wait, no loss)
	if !cc.send(&Reply{Type: "reply", ID: 4243}) {
		t.Fatal("post-wedge send reported gone")
	}
	if cc.repliesLost != 1 {
		t.Fatalf("repliesLost = %d, want still 1 — a drained queue must not evict", cc.repliesLost)
	}
}

// TestWedgedConnectionDoesNotStallDaemon: review round 1's daemon-wide
// freeze — a pipelining client fills its queue with replies and stops
// reading; under the old fallback its reader then blocked HOLDING wmu,
// so the next event broadcast for it blocked inside sendLocked under
// srv.mu and froze the whole daemon. Now: the event push returns (the
// event drops on the full queue), other connections are served
// throughout, and when the wedged client resumes it finds replies
// 2..257 — reply 1 the logged, accounted eviction.
func TestWedgedConnectionDoesNotStallDaemon(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	wedged := dialSock(t, e.sock)
	other := dialSock(t, e.sock)

	// wedged subscribes (events must target it), then plugs the kernel
	// socket buffer with pad events so the writer stalls and the
	// 256-slot queue itself fills
	rep, _ := wedged.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	deadline := time.Now().Add(3 * time.Second)
	for asMap(rep["error"]) != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		rep, _ = wedged.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	}
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}
	pad := strings.Repeat("x", 256<<10)
	for i := 0; i < 96; i++ {
		e.fakes["d75884"].push("gpio.4", map[string]any{"pad": pad})
	}
	for i := 0; i < 300; i++ {
		e.fakes["d75884"].push("gpio.4", map[string]any{"edge": "rising", "seq": int64(i)})
	}

	// pipeline 257 requests without reading: the replies evict the 256
	// queued events, and reply #257 meets the all-replies wedge — the
	// bounded wait, with wmu released
	for i := 1; i <= outboundQ+1; i++ {
		wedged.send(map[string]any{"type": "list_boards", "id": int64(i)})
	}
	time.Sleep(300 * time.Millisecond) // the wedge's bounded wait is entered

	// an event to the wedged watcher must not hang the broadcaster:
	// under the old code this push blocked inside sendLocked holding
	// srv.mu — the goroutine froze mid-push
	pushed := make(chan struct{})
	go func() {
		e.fakes["d75884"].push("gpio.4", map[string]any{"edge": "rising", "seq": int64(999)})
		close(pushed)
	}()
	select {
	case <-pushed:
	case <-time.After(2 * time.Second):
		t.Fatal("event push stalled — srv.mu frozen by the wedged connection")
	}

	// another connection is served throughout (watch_events takes
	// srv.mu — exactly what the old freeze pinned)
	rep, _ = other.call(2, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("other connection starved: %#v", rep)
	}

	// past the bounded wait: the oldest queued reply (id 1) is evicted
	// — logged and counted — and reply #257 queues
	time.Sleep(1300 * time.Millisecond)
	rep, _ = other.call(3, map[string]any{"type": "list_boards"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("daemon stalled after the eviction: %#v", rep)
	}

	// the wedged client resumes: 256 replies survive — ids 2..257
	ids := make([]int64, 0, outboundQ)
	for len(ids) < outboundQ {
		m := wedged.recv()
		if asStr(m["type"]) == "event" {
			continue // the pad/small events that preceded the replies
		}
		ids = append(ids, asInt(m["id"]))
	}
	for i, id := range ids {
		if want := int64(2 + i); id != want {
			t.Fatalf("reply %d = id %d, want %d — reply 1 is the accounted eviction", i, id, want)
		}
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

// TestNewAlreadyListeningSentinel: New against a sock a live listener
// owns classifies the refusal as ErrAlreadyListening — the sentinel
// serve.go keys its spawn-race exit-0 on. A raw listener is enough:
// the probe only dials, it never speaks the protocol.
func TestNewAlreadyListeningSentinel(t *testing.T) {
	dir := testutil.ShortSockDir(t) // darwin's 104-byte sun_path: t.TempDir() overflows (T9 mac)
	path := filepath.Join(dir, "live.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	reg := discover.NewRegistry(nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	_, err = New(Opts{SockPath: path, Registry: reg, RefreshInterval: -1})
	if !errors.Is(err, ErrAlreadyListening) {
		t.Fatalf("New error = %v, want ErrAlreadyListening in the chain", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error does not name the socket path: %v", err)
	}
}

// TestStaleSockFileRemoved: a leftover socket file (dead daemon) is
// removed and owned anew.
func TestStaleSockFileRemoved(t *testing.T) {
	dir := testutil.ShortSockDir(t) // darwin's 104-byte sun_path: t.TempDir() + the claim scratch overflows (T9 mac)
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

	s := &Server{leases: m, logf: newTestLogf(t, "srv: ")}
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

	s := &Server{leases: m, logf: newTestLogf(t, "srv: ")}
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

// ---------------------------------------------------------------------------
// idle exit (Opts.IdleExit): the last-client self-exit state machine.
// ---------------------------------------------------------------------------

// idleSeam replaces the package afterFunc seam: every arming is RECORDED
// (closure + the window it asked for) and the returned timer is inert —
// the test fires the captured closure by hand, so "the timer went off"
// happens on the test's cue, deterministically, never the scheduler's.
type idleSeam struct {
	mu sync.Mutex
	fn func()
	d  time.Duration
	n  int // arms so far (each arm is a fresh afterFunc call)
}

// installIdleSeam swaps afterFunc for the recorder and restores it at
// test end.
func installIdleSeam(t *testing.T) *idleSeam {
	t.Helper()
	s := &idleSeam{}
	prev := afterFunc
	afterFunc = func(d time.Duration, fn func()) *time.Timer {
		s.mu.Lock()
		s.fn, s.d, s.n = fn, d, s.n+1
		s.mu.Unlock()
		tm := time.NewTimer(time.Hour) // inert stand-in: Stop() disarms cleanly
		tm.Stop()
		return tm
	}
	t.Cleanup(func() { afterFunc = prev })
	return s
}

// armed returns the latest arming's closure and window (nil when never
// armed) plus the arm count so far.
func (s *idleSeam) armed() (fn func(), d time.Duration, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fn, s.d, s.n
}

// waitConns polls the live-connection count under the server lock until
// it reaches want (bounded; loud on timeout). Observing the count under
// the lock is also the arm barrier: the conn-delete hook arms INSIDE the
// same critical section that made the count zero.
func waitConns(t *testing.T, srv *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		n := len(srv.conns)
		srv.mu.Unlock()
		if n == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("live conn count never reached %d", want)
}

// assertNoIdleExit asserts the idle-exit channel stays open for the
// given window (the never-triggers cases).
func assertNoIdleExit(t *testing.T, srv *Server, wait time.Duration) {
	t.Helper()
	select {
	case <-srv.IdleExited():
		t.Fatalf("IdleExited() closed, want it still open (no idle exit)")
	case <-time.After(wait):
	}
}

// TestIdleExitFiresWhenLastConnLeaves: with two live connections, the
// FIRST departure arms nothing; the LAST one arms the timer for the FULL
// IdleExit window, and its firing (no conns, no captures) closes
// IdleExited() exactly once.
func TestIdleExitFiresWhenLastConnLeaves(t *testing.T) {
	seam := installIdleSeam(t)
	e := newEnvIdle(t, 50*time.Millisecond, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c1 := dialSock(t, e.sock)
	c2 := dialSock(t, e.sock)
	waitConns(t, e.srv, 2)

	c1.conn.Close()
	waitConns(t, e.srv, 1)
	if fn, _, n := seam.armed(); fn != nil || n != 0 {
		t.Fatal("idle timer armed while a connection still lives")
	}

	c2.conn.Close()
	waitConns(t, e.srv, 0)
	fn, d, n := seam.armed()
	if fn == nil || n != 1 {
		t.Fatal("the last disconnect never armed the idle timer")
	}
	if d != 50*time.Millisecond {
		t.Fatalf("armed window = %s, want the FULL IdleExit 50ms", d)
	}

	fn() // the timer goes off
	select {
	case <-e.srv.IdleExited():
	case <-time.After(2 * time.Second):
		t.Fatal("IdleExited() never closed after the window fired with no clients")
	}
}

// TestIdleExitKeptAliveByRemainingConn: closing one of two connections
// never arms (let alone fires) the idle exit — the survivor holds the
// daemon open.
func TestIdleExitKeptAliveByRemainingConn(t *testing.T) {
	seam := installIdleSeam(t)
	e := newEnvIdle(t, 50*time.Millisecond, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c1 := dialSock(t, e.sock)
	c2 := dialSock(t, e.sock)
	waitConns(t, e.srv, 2)

	c1.conn.Close()
	waitConns(t, e.srv, 1)
	if fn, _, n := seam.armed(); fn != nil || n != 0 {
		t.Fatal("idle timer armed while a connection still lives")
	}
	// the survivor is still served — the daemon is not winding down
	rep, _ := c2.call(9, map[string]any{"type": "list_boards"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("survivor's request refused: %#v", rep)
	}
	assertNoIdleExit(t, e.srv, 150*time.Millisecond)
}

// TestIdleExitDisarmedByReconnect: a reconnection inside the window
// disarms the armed timer — and even the already-spent closure racing in
// after the disarm (Stop() returned false) must be refused by the
// locked re-check, not close the channel.
func TestIdleExitDisarmedByReconnect(t *testing.T) {
	seam := installIdleSeam(t)
	e := newEnvIdle(t, 50*time.Millisecond, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c1 := dialSock(t, e.sock)
	waitConns(t, e.srv, 1)
	c1.conn.Close()
	waitConns(t, e.srv, 0)
	fn, _, n := seam.armed()
	if fn == nil || n != 1 {
		t.Fatal("the last disconnect never armed the idle timer")
	}

	c2 := dialSock(t, e.sock) // the reconnect disarms the pending window
	waitConns(t, e.srv, 1)
	fn() // the stale closure fires anyway (the Stop()-returned-false race)
	assertNoIdleExit(t, e.srv, 150*time.Millisecond)
	// the reconnected client is still served — no shutdown happened
	rep, _ := c2.call(9, map[string]any{"type": "list_boards"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("reconnected client's request refused: %#v", rep)
	}
}

// TestIdleExitDisabledByZero: IdleExit=0 disables the whole face — no
// arming ever happens, and the channel stays open past the point where
// an armed window would long have fired.
func TestIdleExitDisabledByZero(t *testing.T) {
	seam := installIdleSeam(t)
	e := newEnvIdle(t, 0, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	waitConns(t, e.srv, 1)
	c.conn.Close()
	waitConns(t, e.srv, 0)
	if fn, _, n := seam.armed(); fn != nil || n != 0 {
		t.Fatal("IdleExit=0 armed a timer — zero must disable the face entirely")
	}
	assertNoIdleExit(t, e.srv, 150*time.Millisecond)
}

// TestIdleExitBlockedByLiveCapture: a live capture session outlives the
// clients — the armed timer's firing is REFUSED while the capture runs,
// and the capture's own ending re-arms the window; only then does the
// next firing close the channel.
func TestIdleExitBlockedByLiveCapture(t *testing.T) {
	seam := installIdleSeam(t)
	e := newEnvIdle(t, 50*time.Millisecond, nil, mkBoard("d75884", "192.168.7.1"))
	e.srv.mu.Lock()
	e.srv.captureRoot = t.TempDir()
	e.srv.mu.Unlock()
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "h1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	res := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "gpio.1", "max_seconds": 1})
	capID := asStr(res["capture_id"])
	if capID == "" {
		t.Fatalf("capture_start result = %#v", res)
	}

	// every client leaves while the capture is live
	c.conn.Close()
	waitConns(t, e.srv, 0)
	fn1, _, n := seam.armed()
	if fn1 == nil || n != 1 {
		t.Fatal("the last disconnect never armed the idle timer")
	}
	fn1() // fires inside the capture's lifetime → must NOT trigger
	assertNoIdleExit(t, e.srv, 150*time.Millisecond)

	// the capture ends on its own deadline (no client asked) → re-arm
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, n := seam.armed(); n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	fn2, _, n := seam.armed()
	if fn2 == nil || n < 2 {
		t.Fatal("the capture's ending never re-armed the idle timer")
	}
	fn2() // fires again, now with no conns AND no captures → trigger
	select {
	case <-e.srv.IdleExited():
	case <-time.After(2 * time.Second):
		t.Fatal("IdleExited() never closed after the post-capture window fired")
	}
}

// TestSupervisorRefCarriesIP: T10 — the daemon's warm-controller dial
// chain must carry the browse-resolved IP from the registry into the
// BoardRef (BoardRef.TCPAddr dials it first; CGO=0 cannot resolve
// .local). The default wiring is asserted here so the IP cannot be
// dropped between discover and board without a test failing.
func TestSupervisorRefCarriesIP(t *testing.T) {
	reg := discover.NewRegistry(nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) { return nil, nil }),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			return []discover.Board{{
				MAC6: "d75884", Sources: []string{discover.SourceMDNS},
				Host: "xuansi-d75884.local", IP: "10.1.51.205", Port: 7777,
			}}, nil
		}),
	)
	var gotRef board.BoardRef
	srv, err := New(Opts{
		SockPath: filepath.Join(testutil.ShortSockDir(t), "d.sock"),
		Registry: reg,
		GraceTTL: time.Second,
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			gotRef = ref
			return board.NewSupervisor(ref, board.SupOpts{})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer srv.Close()
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	b, rerr := reg.Resolve("d75884")
	if rerr != nil {
		t.Fatalf("Resolve: %v", rerr)
	}
	srv.supervisor(b) // the lazy per-board construction the board paths ride
	if gotRef.IP != "10.1.51.205" || gotRef.Host != "xuansi-d75884.local" || gotRef.Port != 7777 {
		t.Errorf("BoardRef = %#v, want IP 10.1.51.205 carried from the registry entry", gotRef)
	}
	if addr := gotRef.TCPAddr(); addr != "10.1.51.205:7777" {
		t.Errorf("BoardRef.TCPAddr() = %q, want 10.1.51.205:7777 (IP first)", addr)
	}
}

// TestBindSockConcurrentRaceGracefulLosers: the spawn-race contract at
// the bind level (task-2's promise, pinned after a task-10 HIL rerun
// caught the narrow window): N servers binding the same sock path at
// once leave exactly ONE listener, and EVERY loser gets the graceful
// ErrAlreadyListening — including a loser whose stale-check ran before
// the winner created the file (the stat-miss → bind-EADDRINUSE window
// that used to die with a bare "address already in use" exit-1). The
// hammer repeats the race; the property holds in every interleaving
// with the fix and is violated in some without it.
func TestBindSockConcurrentRaceGracefulLosers(t *testing.T) {
	const rounds = 30
	for r := 0; r < rounds; r++ {
		path := filepath.Join(testutil.ShortSockDir(t), "race.sock")
		const n = 6
		res := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ln, err := bindSock(path)
				if err == nil {
					t.Cleanup(func() { ln.Close() })
				}
				res[i] = err
			}(i)
		}
		wg.Wait()
		winners, graceful, bare := 0, 0, 0
		for _, err := range res {
			switch {
			case err == nil:
				winners++
			case errors.Is(err, ErrAlreadyListening):
				graceful++
			default:
				bare++
				t.Logf("round %d: non-graceful loser: %v", r, err)
			}
		}
		if winners != 1 || bare != 0 || graceful != n-1 {
			t.Fatalf("round %d: winners=%d graceful=%d bare=%d, want 1/%d/0", r, winners, graceful, bare, n-1)
		}
	}
}

// TestIsAddrInUseClassification pins the bind-conflict classifier for
// ALL THREE supported platforms' errnos directly (the hammer test
// above is the behavioral coverage, but it can only ever fire the host
// platform's form): Linux's EADDRINUSE, Windows' WSAEADDRINUSE 10048,
// and darwin's EEXIST — each raw and wrapped in the exact
// net.OpError → os.SyscallError chain net.Listen produces — must
// classify as a conflict (the T9 findings were the WSA and EEXIST
// forms dying bare). Near-miss errnos and non-socket errors must NOT
// match, or a genuine failure would be misread as a lost race.
func TestIsAddrInUseClassification(t *testing.T) {
	// listenChain rebuilds the error shape net.Listen returns on a
	// failed bind: *net.OpError wrapping *os.SyscallError wrapping the
	// raw errno (the Windows T9 evidence text "listen unix <path>:
	// bind: Only one usage of each socket address ..." is exactly
	// this chain around Errno(10048)).
	listenChain := func(raw syscall.Errno) error {
		return &net.OpError{
			Op: "listen", Net: "unix",
			Err: os.NewSyscallError("bind", raw),
		}
	}
	conflicts := map[string]error{
		"linux EADDRINUSE (raw)":              syscall.EADDRINUSE,
		"linux EADDRINUSE (listen wrap)":      listenChain(syscall.EADDRINUSE),
		"windows WSAEADDRINUSE (raw)":         wsaeaddrinuse,
		"windows WSAEADDRINUSE (listen wrap)": listenChain(wsaeaddrinuse),
		"darwin EEXIST (raw)":                 syscall.EEXIST,
		"darwin EEXIST (listen wrap)":         listenChain(syscall.EEXIST),
	}
	for name, err := range conflicts {
		if !isAddrInUse(err) {
			t.Errorf("isAddrInUse(%s) = false, want true — a spawn-race loser in this dress would die bare instead of re-probing", name)
		}
	}
	others := map[string]error{
		"plain error":            errors.New("server: listen C:\\x.sock: something else"),
		"wrapped non-conflict":   fmt.Errorf("listen: %w", os.NewSyscallError("bind", syscall.EACCES)),
		"ECONNREFUSED (raw)":     syscall.ECONNREFUSED,
		"ECONNREFUSED (wrapped)": listenChain(syscall.ECONNREFUSED),
	}
	for name, err := range others {
		if isAddrInUse(err) {
			t.Errorf("isAddrInUse(%s) = true, want false — not every listen failure is a lost race", name)
		}
	}
}

// TestHardLinkedSocketNameAnswers pins the OS capability bindClaim
// builds on: a hard link to a bound unix socket's file is another live
// name for the SAME listener — connects through the linked name land
// in the accept queue even after the original name is dropped. A unix
// socket is its inode. (Verified on linux here; the controller runs
// it on darwin. On a host where socket hard-links do not work the
// test skips — bindClaim's own fallback covers that shape.)
func TestHardLinkedSocketNameAnswers(t *testing.T) {
	dir := testutil.ShortSockDir(t)
	claim := filepath.Join(dir, "c.sock")
	target := filepath.Join(dir, "t.sock")

	ln, err := net.Listen("unix", claim)
	if err != nil {
		t.Fatalf("listen claim: %v", err)
	}
	defer ln.Close()
	if err := os.Link(claim, target); err != nil {
		t.Skipf("socket hard-links unsupported on this host: %v", err)
	}
	os.Remove(claim)

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	c, err := net.Dial("unix", target) // the LINKED name, original dropped
	if err != nil {
		t.Fatalf("dial through the linked name: %v", err)
	}
	c.Close()
	ac, ok := <-accepted
	if ok {
		ac.Close()
	} else {
		t.Fatal("connect through the linked name never reached the accept queue")
	}
}

// TestBindSockClaimLeavesNoScratchNames: a successful bindSock leaves
// ONLY the target socket in its directory — no .claim-* scratch names
// — and the target really answers (the claim linked an armed
// listener). Runs on every platform: windows never claims (direct
// bind), so the no-scratch property holds trivially there.
func TestBindSockClaimLeavesNoScratchNames(t *testing.T) {
	dir := testutil.ShortSockDir(t)
	target := filepath.Join(dir, "d.sock")

	ln, err := bindSock(target)
	if err != nil {
		t.Fatalf("bindSock: %v", err)
	}
	defer ln.Close()

	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, de := range des {
		if strings.HasPrefix(de.Name(), "d.sock.c-") {
			t.Fatalf("scratch claim name %q survived the bind", de.Name())
		}
	}
	c, err := net.Dial("unix", target)
	if err != nil {
		t.Fatalf("dial the claimed target: %v", err)
	}
	c.Close()
}

// TestBindSockDeadSocketFileIsStale: a socket file nobody listens on
// (a crashed daemon's leftover — made by closing a listener without
// unlinking) is stale: bindSock removes it and claims the path.
func TestBindSockDeadSocketFileIsStale(t *testing.T) {
	dir := testutil.ShortSockDir(t)
	target := filepath.Join(dir, "dead.sock")

	ln, err := net.Listen("unix", target)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ul, ok := ln.(*net.UnixListener)
	if !ok {
		t.Fatalf("listener is %T", ln)
	}
	ul.SetUnlinkOnClose(false) // the crashed-daemon shape: file stays
	ln.Close()

	ln2, err := bindSock(target)
	if err != nil {
		t.Fatalf("bindSock over a dead sock file: %v", err)
	}
	defer ln2.Close()
	c, err := net.Dial("unix", target)
	if err != nil {
		t.Fatalf("dial after re-claim: %v", err)
	}
	c.Close()
}

// TestBindSockSweepsDeadClaimScratch: the bind-time scratch sweep
// (final-review minor) — a racer killed between its scratch bind and
// the link leaks a dead path+".c-XXXXXXXX" file forever, so the NEXT
// bindSock at that path collects it: probe-dial each scratch sibling,
// remove the dead socket (and any regular file wearing the name),
// leave a LIVE scratch (an active racer's armed listener) strictly
// alone, and never touch the main sock path or non-matching names.
func TestBindSockSweepsDeadClaimScratch(t *testing.T) {
	if !linkClaimSupported {
		t.Skip("link-claim unsupported here: bindSock never creates scratch names")
	}
	dir := testutil.ShortSockDir(t)
	target := filepath.Join(dir, "sweep.sock")

	// a dead scratch SOCKET: the crashed-racer shape (file stays after
	// the listener is gone — the SetUnlinkOnClose trick above)
	deadSock := target + ".c-0000dead"
	dln, err := net.Listen("unix", deadSock)
	if err != nil {
		t.Fatalf("listen dead scratch: %v", err)
	}
	ul, ok := dln.(*net.UnixListener)
	if !ok {
		t.Fatalf("listener is %T", dln)
	}
	ul.SetUnlinkOnClose(false)
	dln.Close()

	// a regular file wearing a scratch name: garbage too
	deadFile := target + ".c-0000beef"
	if err := os.WriteFile(deadFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("write scratch file: %v", err)
	}

	// a LIVE scratch: an active racer's armed listener — must survive
	liveScratch := target + ".c-0000cafe"
	lln, err := net.Listen("unix", liveScratch)
	if err != nil {
		t.Fatalf("listen live scratch: %v", err)
	}
	defer lln.Close()

	// near-miss names: not the scratch shape, never swept
	keepNear := []string{target + ".c-abc", target + ".c-123456789", target + ".c-zzzzzzzz", filepath.Join(dir, "other.sock")}
	for _, n := range keepNear {
		if err := os.WriteFile(n, []byte("x"), 0o600); err != nil {
			t.Fatalf("write near-miss %s: %v", n, err)
		}
	}

	ln, err := bindSock(target)
	if err != nil {
		t.Fatalf("bindSock: %v", err)
	}
	defer ln.Close()

	if _, err := os.Stat(deadSock); !os.IsNotExist(err) {
		t.Errorf("dead scratch socket %q survived the sweep (err=%v)", deadSock, err)
	}
	if _, err := os.Stat(deadFile); !os.IsNotExist(err) {
		t.Errorf("regular scratch file %q survived the sweep (err=%v)", deadFile, err)
	}
	if _, err := os.Stat(liveScratch); err != nil {
		t.Fatalf("LIVE scratch %q was swept — an active racer's scratch must be left alone: %v", liveScratch, err)
	}
	for _, n := range keepNear {
		if _, err := os.Stat(n); err != nil {
			t.Errorf("non-scratch name %q was touched by the sweep: %v", n, err)
		}
	}
	// and the claim itself still answers (the sweep broke nothing)
	c, err := net.Dial("unix", target)
	if err != nil {
		t.Fatalf("dial the claimed target: %v", err)
	}
	c.Close()
}
