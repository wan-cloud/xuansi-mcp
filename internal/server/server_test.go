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
	sink     replclient.StreamSink // the connection's current 0x02 sink (frames-face scripting drives it synchronously)
	dieOnOp  string                // the half-open death script (dieOn): this op's next call fails ErrDead
	onDie    func()                // …after running this hook
	gen      int                   // test bookkeeping: which dial-generation this fake belongs to (supervisor rebuild tests)
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

// dieOn scripts the half-open variant (§13): the named op's NEXT call
// discovers a dead link even though the dial and hello saw it alive —
// the exact ErrDead shape boardOpStream's one-redial retry exists for.
// One-shot; fn (optional) runs just before the failure so a test can
// script the world the death leaves behind (e.g. install the next
// dial's controller).
func (f *fakeCtl) dieOn(op string, fn func()) {
	f.mu.Lock()
	f.dieOnOp, f.onDie = op, fn
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
	if op == f.dieOnOp {
		hook := f.onDie
		f.dieOnOp, f.onDie = "", nil
		f.mu.Unlock()
		if hook != nil {
			hook()
		}
		return nil, fmt.Errorf("%w: half-open link discovered mid-op", replclient.ErrDead)
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
	if v, ok := res["fw_version"].(string); ok { // B10-T8c
		h.FwVersion = v
	}
	if v, ok := res["reset"].(string); ok {
		h.Reset = v
	}
	if v, ok := res["reset_reason"].(string); ok {
		h.ResetReason = v
	}
	if v, ok := res["chip"].(string); ok { // P3-T13: chip rides the sighting (parseHello's twin)
		h.Chip = v
	}
	if v, ok := res["boot_epoch"].(int64); ok && v >= 0 && v <= math.MaxUint32 {
		h.BootEpoch = uint32(v)
	}
	// v6 ring (B12-P2 T6): strings in wire order; absent key leaves the
	// non-nil empty slice (T4's normalization)
	h.ResetHistory = []string{}
	if rh, ok := res["reset_history"].([]any); ok {
		for _, v := range rh {
			if s, ok := v.(string); ok {
				h.ResetHistory = append(h.ResetHistory, s)
			}
		}
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

// TestEvalBudgetMsForwards: eval/submit's per-call budget_ms (§9.6,
// protocol v6) crosses the daemon untouched into the board op args —
// key present when sent, absent when omitted — so the board runs this
// call under the override and the reply wait derives from it
// (replclient side).
func TestEvalBudgetMsForwards(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "h1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"out": "ok"}, nil
	})

	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1", "budget_ms": 8000})
	if asMap(rep["error"]) != nil {
		t.Fatalf("budgeted eval: %#v", rep)
	}
	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "code": "return 2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("plain eval: %#v", rep)
	}

	var evals []mockCall
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "eval" {
			evals = append(evals, call)
		}
	}
	if len(evals) != 2 {
		t.Fatalf("board eval calls = %d, want 2", len(evals))
	}
	if evals[0].args["budget_ms"] != int64(8000) {
		t.Fatalf("budgeted eval board args = %#v, want budget_ms int64(8000)", evals[0].args)
	}
	if _, ok := evals[1].args["budget_ms"]; ok {
		t.Fatalf("plain eval must not carry budget_ms: %#v", evals[1].args)
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
// driver-pipeline loopback's on-board fixture toggle; the camera trio
// left with the camera face's retirement (B12-P2 T6: the fw keeps its
// camera ops, the daemon no longer forwards them).
var opsFaceAllowlistV1 = []string{
	"budget.get", "budget.set",
	"caps", "diag.heapinfo", "diag.i2cslave",
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

// rebootEventOf reads the watcher's next push as a board.reboot event
// and returns its data map (the synthesis tests' shared reader).
func rebootEventOf(t *testing.T, w *sockClient) map[string]any {
	t.Helper()
	ev := w.recv()
	if asStr(ev["type"]) != "event" || asStr(ev["board"]) != "d75884" {
		t.Fatalf("event push = %#v, want a d75884 event", ev)
	}
	inner := asMap(ev["event"])
	if asStr(inner["source"]) != "board" {
		t.Fatalf("event source = %#v, want board", inner)
	}
	data := asMap(inner["data"])
	if asStr(data["kind"]) != "board.reboot" {
		t.Fatalf("event kind = %#v, want board.reboot", data)
	}
	return data
}

// reasonsOf reads data["reasons"] as a string slice, failing when the
// key is absent (callers that want absence check the key themselves).
func reasonsOf(t *testing.T, data map[string]any) []string {
	t.Helper()
	raw, ok := data["reasons"].([]any)
	if !ok {
		t.Fatalf("reasons = %#v, want an array", data["reasons"])
	}
	out := make([]string, len(raw))
	for i, v := range raw {
		out[i] = asStr(v)
	}
	return out
}

// usbOnlyBoard keeps the startup audit away: the audit only touches
// mDNS boards WITH a host, and the env's fake discovery sources merge
// mdns+host back onto every entry — so the lever that actually holds
// is the empty Host. The boot-identity tests need a clean daemon-side
// bootSeen, and the audit's own tap would silently baseline epoch 0
// under them the moment Start races ahead.
func usbOnlyBoard(mac6, host string) discover.Board {
	b := mkBoard(mac6, host)
	b.Host = ""
	return b
}

// TestBoardRebootReasonsSynthesis (B12-P2 T6): an epoch delta > 1
// loses the intermediate boots' causes unless the event carries them —
// the board.reboot synthesis reads the fw's v6 reset_history ring
// (HelloInfo.ResetHistory, wire order oldest→newest, its final entry
// this boot's reason): delta > 1 → reasons = the history's tail delta
// entries (oldest→newest, the last == reset_reason), capped at the
// ring's 8 with reasons_partial flagging the cut; delta ≤ 1 or an
// empty history (pre-v6 fw, T4's non-nil-empty normalization) → the
// reasons key is absent, honestly. Each changed-epoch sighting pair
// fires exactly ONE board.reboot. The sightings are driven straight
// into noteBootSighting (the tap hello's landing point — the test
// env's dial seam hands every dial the same fake pointer, so a second
// tap never re-hellos; the synthesis under test is this function's).
func TestBoardRebootReasonsSynthesis(t *testing.T) {
	sight := func(e *env, b discover.Board, epoch uint32, reason string, history []string) {
		t.Helper()
		db := e.srv.supervisor(b)
		e.srv.noteBootSighting(b.MAC6, db, &replclient.HelloInfo{
			ProtocolVer: 6, FwBuild: "dcaa9ae", FwVersion: "0.2.0",
			BootEpoch: epoch, ResetReason: reason, ResetHistory: history,
		})
	}
	// hWatchEvents itself warms the controller and taps it — the
	// watch's reply landing means the tap hello (the fake's default:
	// no boot_epoch → baseline epoch 0) has already sighted. Each
	// subtest's single sight() then fires 0→N, delta = N, with the
	// watcher guaranteed subscribed before the event.
	watch := func(t *testing.T, e *env) *sockClient {
		t.Helper()
		w := dialSock(t, e.sock)
		rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
		if asMap(rep["error"]) != nil {
			t.Fatalf("watch: %#v", rep)
		}
		return w
	}

	t.Run("delta 3 tail", func(t *testing.T) {
		b := usbOnlyBoard("d75884", "xuansi-d75884.local")
		e := newEnv(t, nil, b)
		w := watch(t, e)
		sight(e, b, 3, "usb", []string{"power-on", "usb", "software", "panic", "usb"})
		data := rebootEventOf(t, w)
		if asInt(data["prev_epoch"]) != 0 || asInt(data["new_epoch"]) != 3 {
			t.Fatalf("epoch pair = %v→%v, want 0→3", data["prev_epoch"], data["new_epoch"])
		}
		if asStr(data["reset_reason"]) != "usb" {
			t.Fatalf("reset_reason = %q, want usb (the last boot's cause)", data["reset_reason"])
		}
		got := reasonsOf(t, data)
		want := []string{"software", "panic", "usb"} // the tail 3, oldest→newest
		if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
			t.Fatalf("reasons = %v, want %v (tail delta entries, oldest→newest)", got, want)
		}
		if _, ok := data["reasons_partial"]; ok {
			t.Fatalf("reasons_partial = %#v, want absent (delta 3 ≤ the ring's 8)", data["reasons_partial"])
		}
		if _, ok := data["gap_ms"]; !ok {
			t.Fatal("gap_ms missing from the reboot event")
		}
		w.recvNone() // one reboot, one event — nothing else fired
	})

	t.Run("delta 1 no reasons", func(t *testing.T) {
		b := usbOnlyBoard("d75884", "xuansi-d75884.local")
		e := newEnv(t, nil, b)
		w := watch(t, e)
		sight(e, b, 1, "power-on", []string{"usb", "power-on"})
		data := rebootEventOf(t, w)
		if asInt(data["new_epoch"]) != 1 {
			t.Fatalf("new_epoch = %v, want 1", data["new_epoch"])
		}
		if _, ok := data["reasons"]; ok {
			t.Fatalf("reasons = %v, want the key absent (delta 1: no lost intermediate to recover)", data["reasons"])
		}
		if _, ok := data["reasons_partial"]; ok {
			t.Fatalf("reasons_partial = %#v, want absent with reasons", data["reasons_partial"])
		}
	})

	t.Run("delta 10 partial eight", func(t *testing.T) {
		b := usbOnlyBoard("d75884", "xuansi-d75884.local")
		e := newEnv(t, nil, b)
		w := watch(t, e)
		hist := []string{"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"} // the fw ring is last-8
		sight(e, b, 10, "r8", hist)
		data := rebootEventOf(t, w)
		got := reasonsOf(t, data)
		if len(got) != 8 {
			t.Fatalf("reasons = %d entries, want the ring's full 8", len(got))
		}
		for i, r := range hist {
			if got[i] != r {
				t.Fatalf("reasons = %v, want %v (all 8, oldest→newest)", got, hist)
			}
		}
		if partial, ok := data["reasons_partial"].(bool); !ok || !partial {
			t.Fatalf("reasons_partial = %#v, want true (delta 10 > the 8 the ring kept)", data["reasons_partial"])
		}
	})

	t.Run("old fw empty history", func(t *testing.T) {
		b := usbOnlyBoard("d75884", "xuansi-d75884.local")
		e := newEnv(t, nil, b)
		w := watch(t, e)
		sight(e, b, 3, "usb", []string{}) // pre-v6 fw: T4 normalizes the absent key to non-nil empty
		data := rebootEventOf(t, w)
		if _, ok := data["reasons"]; ok {
			t.Fatalf("reasons = %v, want the key absent (no history to read)", data["reasons"])
		}
		if _, ok := data["reasons_partial"]; ok {
			t.Fatalf("reasons_partial = %#v, want absent with reasons", data["reasons_partial"])
		}
		if asStr(data["reset_reason"]) != "usb" {
			t.Fatalf("reset_reason = %q, want usb (the last boot's cause still crosses)", data["reset_reason"])
		}
	})
}

// TestCameraShotRequestRetired (B12-P2 T6): the camera_shot request
// type is gone from the internal API — a client sending one gets the
// generic unknown-type refusal (server class) and the board is never
// contacted.
func TestCameraShotRequestRetired(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "camera_shot", "board": "d75884"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "server" || !strings.Contains(asStr(er["msg"]), "unknown request type") {
		t.Fatalf("retired camera_shot = %s: %q, want the server unknown-type refusal", asStr(er["class"]), asStr(er["msg"]))
	}
	for _, call := range e.fakes["d75884"].got() {
		if call.op == "camera.capture" {
			t.Fatal("a retired camera_shot reached the board")
		}
	}
}

// TestBoardViewBootIdentity (B12-P2 T6): list_boards/board_status
// carry the board's boot identity — boot_epoch/reset_reason from the
// daemon's last hello sighting (the registry's enriched view, where
// fw/fw_version already live) and last_seen, the daemon's most recent
// protocol crossing (the tap hello, then every op reply). Before any
// crossing all three are absent — honest, there is nothing to report.
func TestBoardViewBootIdentity(t *testing.T) {
	b := usbOnlyBoard("d75884", "xuansi-d75884.local")
	e := newEnv(t, nil, b)
	c := dialSock(t, e.sock)

	// pre-crossing: no boot fields on an untouched board
	rep, _ := c.call(1, map[string]any{"type": "list_boards"})
	boards, _ := asMap(rep["result"])["boards"].([]any)
	if len(boards) != 1 {
		t.Fatalf("boards = %#v, want the one board", rep["result"])
	}
	b0, _ := boards[0].(map[string]any)
	for _, k := range []string{"boot_epoch", "reset_reason", "last_seen", "chip"} {
		if _, ok := b0[k]; ok {
			t.Fatalf("pre-crossing board view carries %q = %#v, want honest absence", k, b0[k])
		}
	}

	// a v6 hello crossing lands the identity: the tap hello enriches
	// the registry AND sights the boot (P3-T13: chip rides the same
	// sighting — the push pairing's architecture source)
	e.fakes["d75884"].on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": int64(6), "role": "controller", "fw": "xuansi-fw v0.2.0",
			"fw_build": "dcaa9ae", "fw_version": "0.2.0", "fs": true,
			"chip":          "ESP32-S3",
			"boot_epoch":    int64(8),
			"reset_reason":  "usb",
			"reset_history": []any{"power-on", "usb"},
		}, nil
	})
	rep, _ = c.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "bootid"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	crossed := time.Now()
	rep, _ = c.call(3, map[string]any{"type": "op", "board": "d75884", "op": "wifi.status"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op wifi.status: %#v", rep)
	}

	checkView := func(view map[string]any, label string) {
		t.Helper()
		if asStr(view["chip"]) != "ESP32-S3" {
			t.Fatalf("%s chip = %#v, want ESP32-S3 (the sighting's chip)", label, view["chip"])
		}
		if asInt(view["boot_epoch"]) != 8 {
			t.Fatalf("%s boot_epoch = %#v, want 8 (the sighting's epoch)", label, view["boot_epoch"])
		}
		if asStr(view["reset_reason"]) != "usb" {
			t.Fatalf("%s reset_reason = %q, want usb", label, view["reset_reason"])
		}
		seen, perr := time.Parse(time.RFC3339, asStr(view["last_seen"]))
		if perr != nil {
			t.Fatalf("%s last_seen = %q, not RFC3339: %v", label, view["last_seen"], perr)
		}
		if seen.Before(crossed.Add(-2*time.Second)) || time.Now().Before(seen) {
			t.Fatalf("%s last_seen = %v, want a timestamp at the crossing (±clock skew)", label, seen)
		}
	}
	rep, _ = c.call(4, map[string]any{"type": "list_boards"})
	boards, _ = asMap(rep["result"])["boards"].([]any)
	b0, _ = boards[0].(map[string]any)
	checkView(b0, "list_boards")

	rep, _ = c.call(5, map[string]any{"type": "board_status", "board": "d75884"})
	checkView(asMap(asMap(rep["result"])["board"]), "board_status")
}

// TestLastSeenAdvancesOnPollCrossings (T6 fix round 1): last_seen's
// claimed width — "the daemon's most recent protocol crossing (hello
// or op reply)" — must hold on the POLLING paths an agent actually
// rides: board_status's script.list, watch_events's board-side
// events.subscribe, and capture_start's. Each leaves last_seen
// strictly advanced. The daemon-side state field is the witness (the
// RFC3339 wire truncates to whole seconds — two sub-second crossings
// would read equal there; noteBoardContact updates it at nanosecond
// resolution).
func TestLastSeenAdvancesOnPollCrossings(t *testing.T) {
	b := usbOnlyBoard("d75884", "xuansi-d75884.local")
	e := newEnv(t, nil, b)
	c := dialSock(t, e.sock)
	e.srv.mu.Lock()
	e.srv.captureRoot = t.TempDir()
	e.srv.mu.Unlock()

	lastSeen := func() time.Time {
		t.Helper()
		e.srv.mu.Lock()
		defer e.srv.mu.Unlock()
		db := e.srv.sups["d75884"]
		if db == nil {
			t.Fatal("no daemon-side board state")
		}
		return db.lastSeen
	}
	// settle: a hair between crossings so a coarse clock cannot stamp
	// two legs identically (this host is nanosecond-precise; the sleep
	// is the portability margin)
	settle := func() { time.Sleep(5 * time.Millisecond) }

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "poller"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "op", "board": "d75884", "op": "wifi.status"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op wifi.status (warm connect + tap hello): %#v", rep)
	}
	t0 := lastSeen()
	if t0.IsZero() {
		t.Fatal("no hello sighting recorded after the first crossing")
	}

	// ① board_status's own script.list — the reply the field's own
	// tool elicits must advance it
	settle()
	rep, _ = c.call(3, map[string]any{"type": "board_status", "board": "d75884"})
	if asMap(rep["error"]) != nil || asMap(rep["result"])["scripts"] == nil {
		t.Fatalf("board_status: %#v", rep)
	}
	if t1 := lastSeen(); !t1.After(t0) {
		t.Fatalf("last_seen did not advance over board_status's script.list: %v → %v", t0, t1)
	}

	// ② watch_events's board-side events.subscribe
	settle()
	rep, _ = c.call(4, map[string]any{"type": "watch_events", "board": "d75884", "source": "stdout"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}
	if t2 := lastSeen(); !t2.After(t0) {
		t.Fatalf("last_seen did not advance over watch_events's events.subscribe: %v → %v", t0, t2)
	}

	// ③ capture_start's board-side events.subscribe
	settle()
	rep, _ = c.call(5, map[string]any{"type": "capture_start", "board": "d75884", "source": "gpio.4"})
	res := asMap(rep["result"])
	if res == nil {
		t.Fatalf("capture_start: %#v", rep)
	}
	if t3 := lastSeen(); !t3.After(t0) {
		t.Fatalf("last_seen did not advance over capture_start's events.subscribe: %v → %v", t0, t3)
	}
	rep, _ = c.call(6, map[string]any{"type": "capture_stop", "capture_id": asStr(res["capture_id"])})
	if asMap(rep["error"]) != nil {
		t.Fatalf("capture_stop: %#v", rep)
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

// TestSupervisorRebuildsOnEndpointDrift: Bug 1 (B15 §4.10) — the
// per-board supervisor pins its dial seams (DialTCP's endpoint,
// DialUSB's path) at construction from a BoardRef snapshot. When the
// registry's CURRENT snapshot drifts (the board's IP changed on
// re-association, or a USB-only first appearance later gains its mDNS
// endpoint), the next supervisor() acquisition must rebuild the seams
// against the fresh ref — same daemonBoard (state: frames hub, boot
// baseline, last_seen preserved), the stale controller corpse CLOSED,
// and callers unchanged. No drift, no rebuild: the warm controller
// rides on untouched.
func TestSupervisorRebuildsOnEndpointDrift(t *testing.T) {
	var mu sync.Mutex
	view := discover.Board{ // the registry's canned answer, mutated below
		MAC6: "d75884", Sources: []string{discover.SourceUSB, discover.SourceMDNS},
		USBPath: "/dev/serial/by-id/fake-d75884", Host: "xuansi-d75884.local", Port: 7777,
	}
	reg := discover.NewRegistry(nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			mu.Lock()
			defer mu.Unlock()
			return []discover.Board{view}, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			mu.Lock()
			defer mu.Unlock()
			return []discover.Board{view}, nil
		}),
	)
	var refs []board.BoardRef
	gen := 0
	sock := filepath.Join(testutil.ShortSockDir(t), "d.sock")
	srv, err := New(Opts{
		SockPath:        sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		Log:             newTestLogf(t, "srv: "),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			mu.Lock()
			defer mu.Unlock()
			refs = append(refs, ref)
			gen++
			g := gen
			return board.NewSupervisor(ref, board.SupOpts{
				DialTCP: func() (board.Controller, error) {
					f := newFakeCtl()
					f.gen = g
					return f, nil
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// v1: IP 10.1.51.205 — the supervisor builds once and warms its
	// controller (generation-1 fake goes live).
	b1, rerr := reg.Resolve("d75884")
	if rerr != nil {
		t.Fatalf("Resolve: %v", rerr)
	}
	b1.IP = "10.1.51.205"
	db1 := srv.supervisor(b1)
	if len(refs) != 1 || refs[0].IP != "10.1.51.205" {
		t.Fatalf("refs = %#v, want one ref carrying IP .205", refs)
	}
	db1.connectMu.Lock()
	ctl1, cerr := db1.sup.Connect(context.Background())
	db1.connectMu.Unlock()
	if cerr != nil {
		t.Fatalf("Connect v1: %v", cerr)
	}
	gen1Fake, ok := ctl1.(*fakeCtl)
	if !ok || gen1Fake.gen != 1 {
		t.Fatalf("warm controller = %#v, want the generation-1 fake", ctl1)
	}

	// drift: the board re-associated — IP .206 now. The next acquisition
	// rebuilds the dial seams on the SAME daemonBoard and closes the
	// generation-1 corpse.
	mu.Lock()
	view.IP = "10.1.51.206"
	mu.Unlock()
	if err := reg.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh (drift): %v", err)
	}
	b2, rerr := reg.Resolve("d75884")
	if rerr != nil {
		t.Fatalf("Resolve (drift): %v", rerr)
	}
	db2 := srv.supervisor(b2)
	if db2 != db1 {
		t.Fatal("drift rebuild swapped the daemonBoard — per-board server state (frames hub, boot baseline, last_seen) must survive")
	}
	if len(refs) != 2 || refs[1].IP != "10.1.51.206" {
		t.Fatalf("refs = %#v, want the rebuild against IP .206", refs)
	}
	if !gen1Fake.Dead() {
		t.Fatal("the stale generation-1 controller was left open — the rebuild must close the corpse")
	}
	if c := db2.sup.Client(); c != nil {
		t.Fatalf("post-rebuild controller = %#v, want nil (fresh seams, cold)", c)
	}

	// no further drift: another acquisition reuses the rebuilt seams.
	db3 := srv.supervisor(b2)
	if len(refs) != 2 {
		t.Fatalf("refs = %#v, want no rebuild without drift", refs)
	}
	if db3 != db2 {
		t.Fatal("no-drift acquisition must return the same daemonBoard")
	}
	// the rebuilt seam actually dials the new generation.
	db3.connectMu.Lock()
	ctl2, cerr := db3.sup.Connect(context.Background())
	db3.connectMu.Unlock()
	if cerr != nil {
		t.Fatalf("Connect v2: %v", cerr)
	}
	if f, ok := ctl2.(*fakeCtl); !ok || f.gen != 2 {
		t.Fatalf("post-drift controller = %#v, want the generation-2 fake", ctl2)
	}
}

// TestOpWifiSelfReportEnrichesEndpoint: Bug 2② — wifi.status/wifi.setup
// replies carry the board's own ip/port; the ops face writes them back
// onto the registry entry, so the next list_boards dials the
// self-reported address (a USB-only first appearance gains its TCP
// endpoint without waiting for the next mDNS sweep to resolve one).
func TestOpWifiSelfReportEnrichesEndpoint(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	e.fakes["d75884"].on("wifi.status", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"state": "up", "ssid": "bench", "ip": "10.1.51.205", "port": int64(7777)}, nil
	})

	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "w1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "op", "board": "d75884", "op": "wifi.status", "holder": "w1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("op wifi.status: %#v", rep)
	}
	// the registry entry now carries the self-reported endpoint
	for _, b := range e.srv.reg.Snapshot() {
		if b.MAC6 != "d75884" {
			continue
		}
		if b.IP != "10.1.51.205" || b.Port != 7777 {
			t.Fatalf("registry endpoint after wifi.status = ip %q port %d, want the self-reported 10.1.51.205:7777", b.IP, b.Port)
		}
	}
	// and the board view surfaces it (list_boards' ip field)
	rep, _ = c.call(3, map[string]any{"type": "list_boards"})
	boards, _ := rep["result"].(map[string]any)["boards"].([]any)
	if len(boards) != 1 {
		t.Fatalf("list_boards: %#v", rep)
	}
	if asStr(asMap(boards[0])["ip"]) != "10.1.51.205" {
		t.Errorf("list_boards ip = %q, want the self-reported address", asStr(asMap(boards[0])["ip"]))
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

// ---------------------------------------------------------------------------
// run_lua/submit frames primitive (B12-P2 T5, spec §5.6): eval-window
// stream frames ride the reply; pending-job frames ride the job event.
// ---------------------------------------------------------------------------

// stampStreamChunk builds one ch=2 module chunk exactly the way the
// fw's stream_send host-stamp writes it (xs_wasm.c): every chunk of a
// sequence carries [u8 name_len][name][data part].
func stampStreamChunk(name string, part []byte) []byte {
	out := make([]byte, 0, 1+len(name)+len(part))
	out = append(out, byte(len(name)))
	out = append(out, name...)
	return append(out, part...)
}

// scriptEvalStream wires the fake's eval op: it drives the
// connection's current sink with one COMPLETE single-chunk ch=2 frame
// per payload (frames-then-reply, the fw's wire order) before
// answering — the module-streaming eval shape.
func scriptEvalStream(f *fakeCtl, name string, payloads ...[]byte) {
	f.on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		sink := f.sinkSnapshot()
		for i, p := range payloads {
			sink(2, uint32(i+1), 0, 1, stampStreamChunk(name, p))
		}
		return map[string]any{"out": "ok"}, nil
	})
}

// TestEvalReplyCarriesStreamFrames (case ①): a ch=2 frame that
// completes inside the eval window rides the reply as
// frames[0]{ch,name,data_b64} with the host-stamped module name parsed
// off and the payload byte-exact; nothing dropped, no frames_dropped
// key.
func TestEvalReplyCarriesStreamFrames(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "fr1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	jpeg := []byte{0xFF, 0xD8, 1, 2, 3, 0xFF, 0xD9}
	scriptEvalStream(e.fakes["d75884"], "jpge", jpeg)

	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "jpge.shot()"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	res := asMap(rep["result"])
	if asStr(res["out"]) != "ok" {
		t.Fatalf("out = %#v", res["out"])
	}
	frames, ok := res["frames"].([]any)
	if !ok || len(frames) != 1 {
		t.Fatalf("frames = %#v, want one entry", res["frames"])
	}
	fm := asMap(frames[0])
	if asInt(fm["ch"]) != 2 || asStr(fm["name"]) != "jpge" {
		t.Fatalf("frame meta = %#v, want ch=2 name=jpge", fm)
	}
	got, err := base64.StdEncoding.DecodeString(asStr(fm["data_b64"]))
	if err != nil || !bytes.Equal(got, jpeg) {
		t.Fatalf("data_b64 decodes to %x (err %v), want %x", got, err, jpeg)
	}
	if _, ok := res["frames_dropped"]; ok {
		t.Fatalf("frames_dropped present with nothing dropped: %#v", res)
	}
}

// TestEvalFramesSixteenBound (case ②): the 16-frame bound truncates the
// reply's frames and counts the remainder in frames_dropped.
func TestEvalFramesSixteenBound(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "fr2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	payloads := make([][]byte, 20)
	for i := range payloads {
		payloads[i] = []byte{byte('a' + i)}
	}
	scriptEvalStream(e.fakes["d75884"], "jpge", payloads...)

	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "burst()"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	res := asMap(rep["result"])
	frames, _ := res["frames"].([]any)
	if len(frames) != board.FramesMax {
		t.Fatalf("frames = %d, want %d", len(frames), board.FramesMax)
	}
	if asInt(res["frames_dropped"]) != 4 {
		t.Fatalf("frames_dropped = %#v, want 4", res["frames_dropped"])
	}
	last := asMap(frames[len(frames)-1])
	got, _ := base64.StdEncoding.DecodeString(asStr(last["data_b64"]))
	if !bytes.Equal(got, []byte{'a' + 15}) {
		t.Fatalf("last kept frame = %q, want the 16th (p)", got)
	}
}

// TestEvalFramesByteBound (case ③): crossing the 1 MiB cumulative
// payload bound truncates — the fitting frame rides the reply, the
// overflow counts in frames_dropped.
func TestEvalFramesByteBound(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "fr3"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	first := bytes.Repeat([]byte{1}, 560*1024)
	second := bytes.Repeat([]byte{2}, 560*1024)
	scriptEvalStream(e.fakes["d75884"], "jpge", first, second)

	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "fat()"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	res := asMap(rep["result"])
	frames, _ := res["frames"].([]any)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1 (the fitting one)", len(frames))
	}
	got, _ := base64.StdEncoding.DecodeString(asStr(asMap(frames[0])["data_b64"]))
	if !bytes.Equal(got, first) {
		t.Fatalf("kept frame is not the first payload (len %d)", len(got))
	}
	if asInt(res["frames_dropped"]) != 1 {
		t.Fatalf("frames_dropped = %#v, want 1", res["frames_dropped"])
	}
}

// TestEvalFramesChunkedOutOfOrder (case ④): one frame split across
// chunks whose idx order scrambles reassembles as ONE reply frame with
// the parts concatenated in idx order — the T2 reassembler semantics
// riding the eval window.
func TestEvalFramesChunkedOutOfOrder(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "fr4"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		sink := e.fakes["d75884"].sinkSnapshot()
		sink(2, 5, 1, 3, stampStreamChunk("jpge", []byte("B")))
		sink(2, 5, 0, 3, stampStreamChunk("jpge", []byte("A")))
		sink(2, 5, 2, 3, stampStreamChunk("jpge", []byte("C")))
		return map[string]any{"out": "ok"}, nil
	})

	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "split()"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	res := asMap(rep["result"])
	frames, _ := res["frames"].([]any)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want exactly 1", len(frames))
	}
	fm := asMap(frames[0])
	if asStr(fm["name"]) != "jpge" {
		t.Fatalf("name = %q, want jpge", fm["name"])
	}
	got, _ := base64.StdEncoding.DecodeString(asStr(fm["data_b64"]))
	if !bytes.Equal(got, []byte("ABC")) {
		t.Fatalf("reassembled = %q, want ABC", got)
	}
	if _, ok := res["frames_dropped"]; ok {
		t.Fatalf("frames_dropped present with nothing dropped: %#v", res)
	}
}

// TestEvalNoFramesReplyShapeUnchanged (case ⑤ — the highest bar): an
// eval whose window saw no stream frames answers with a reply
// byte-equivalent to the pre-frames shape — no frames key, no
// frames_dropped key (existing callers see zero difference).
func TestEvalNoFramesReplyShapeUnchanged(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "fr5"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}
	b, err := json.Marshal(asMap(rep["result"]))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"out":"ok"}` {
		t.Fatalf("no-frames eval reply = %s, want the exact pre-frames shape {\"out\":\"ok\"}", b)
	}

	// the value face stays byte-stable too
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"out": "hello\n", "value": map[string]any{"ok": int64(1)}}, nil
	})
	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "code": "return {ok=1}"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("value eval: %#v", rep)
	}
	b, err = json.Marshal(asMap(rep["result"]))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"out":"hello\n","value":{"ok":1}}` {
		t.Fatalf("no-frames value eval reply = %s, want the exact pre-frames shape", b)
	}
}

// TestEvalErrorReplyCarriesNoFrames: a board error mid-stream still
// answers as the plain error reply — frames never ride error replies.
func TestEvalErrorReplyCarriesNoFrames(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "fr6"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	e.fakes["d75884"].on("eval", func(args map[string]any) (map[string]any, *replclient.OpError) {
		sink := e.fakes["d75884"].sinkSnapshot()
		sink(2, 1, 0, 1, stampStreamChunk("jpge", []byte("x")))
		return nil, &replclient.OpError{Op: "eval", Class: "rt", Msg: "boom"}
	})
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "err()"})
	er := errOf(t, rep)
	if asStr(er["class"]) != "rt" || asStr(er["msg"]) != "boom" {
		t.Fatalf("error = %#v, want the board's rt/boom verbatim", er)
	}
}

// TestSubmitJobEventCarriesFrames: submit acks immediately with the
// board's {tag, queued} byte-unchanged; the job's stream frames buffer
// while it is pending and ride its job event's data (frames +
// frames_dropped); a job that streams nothing leaves the event data
// byte-unchanged.
func TestSubmitJobEventCarriesFrames(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch: %#v", rep)
	}
	rep, _ = w.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "fr7"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	rep, _ = w.call(3, map[string]any{"type": "submit", "board": "d75884", "code": "shot()", "tag": "j1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("submit: %#v", rep)
	}
	b, err := json.Marshal(asMap(rep["result"]))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"queued":true,"tag":"j1"}` {
		t.Fatalf("submit ack = %s, want the exact pre-frames ack shape", b)
	}

	// the job streams on the warm connection, then its event lands
	f := e.fakes["d75884"]
	jpeg := []byte{0xFF, 0xD8, 9, 9, 0xFF, 0xD9}
	sink := f.sinkSnapshot()
	sink(2, 1, 0, 1, stampStreamChunk("jpge", jpeg))
	sink(2, 2, 0, 1, stampStreamChunk("jpge", jpeg))
	f.push("job", map[string]any{"tag": "j1", "ok": true, "out": "done"})

	ev := w.recv()
	if asStr(ev["type"]) != "event" || asStr(ev["board"]) != "d75884" {
		t.Fatalf("event push = %#v", ev)
	}
	inner := asMap(ev["event"])
	if asStr(inner["source"]) != "job" {
		t.Fatalf("source = %q, want job", inner["source"])
	}
	data := asMap(inner["data"])
	frames, ok := data["frames"].([]any)
	if !ok || len(frames) != 2 {
		t.Fatalf("job event frames = %#v, want two entries", data["frames"])
	}
	fm := asMap(frames[0])
	if asInt(fm["ch"]) != 2 || asStr(fm["name"]) != "jpge" {
		t.Fatalf("frame meta = %#v, want ch=2 name=jpge", fm)
	}
	got, derr := base64.StdEncoding.DecodeString(asStr(fm["data_b64"]))
	if derr != nil || !bytes.Equal(got, jpeg) {
		t.Fatalf("data_b64 = %x (err %v), want %x", got, derr, jpeg)
	}

	// a job that streams nothing leaves its event data byte-unchanged
	f.push("job", map[string]any{"tag": "j2", "ok": true, "out": "quiet"})
	ev = w.recv()
	data = asMap(asMap(ev["event"])["data"])
	b, err = json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"ok":true,"out":"quiet","tag":"j2"}` {
		t.Fatalf("no-stream job event data = %s, want the exact pre-frames shape", b)
	}
}

// TestSubmitRedialKeepsFrameWindow (final-review Important-1): a submit
// whose link dies mid-op retries on a redialed controller. The tap's
// controller-identity jobReset correctly orphans the OLD link's pending
// windows — but the retrying submit's window is not one of them: its
// job is about to be acked on the NEW link. The window must survive the
// redial, or the job's stream chunks pass the sink uncollected and its
// event drains an empty buffer — frames silently lost with no
// frames_dropped, the honest-drops violation. Here: warm link dies →
// redial lands a half-open controller → submit discovers the death →
// one-redial retry → healthy controller acks → the job streams on THAT
// connection → its event carries the frames.
func TestSubmitRedialKeepsFrameWindow(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch: %#v", rep)
	}
	rep, _ = w.call(2, map[string]any{"type": "lease", "board": "d75884", "holder": "fr7"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	// the warm link dies; the next dial returns a half-open controller
	// whose FIRST submit discovers the death, and the dial after that a
	// healthy one — dead link → ErrDead retry → redial, scripted with
	// no races
	e.fakes["d75884"].markDead()
	healthy := newFakeCtl()
	half := newFakeCtl()
	half.dieOn("submit", func() {
		e.dmu.Lock()
		e.fakes["d75884"] = healthy
		e.dmu.Unlock()
	})
	e.dmu.Lock()
	e.fakes["d75884"] = half
	e.dmu.Unlock()

	tcpBefore := e.dialedTCP("d75884")
	rep, _ = w.call(3, map[string]any{"type": "submit", "board": "d75884", "code": "shot()", "tag": "j1", "holder": "fr7"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("submit after redial: %#v", rep)
	}
	b, err := json.Marshal(asMap(rep["result"]))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"queued":true,"tag":"j1"}` {
		t.Fatalf("submit ack = %s, want the exact pre-frames ack shape", b)
	}
	if n := e.dialedTCP("d75884") - tcpBefore; n < 2 {
		t.Fatalf("tcp dials since death = %d, want ≥2 (the half-open link, then the redial)", n)
	}
	sawSubmit := false
	for _, c := range healthy.got() {
		if c.op == "submit" {
			sawSubmit = true
		}
	}
	if !sawSubmit {
		t.Fatal("the retried submit never rode the healthy controller")
	}

	// the job streams on the REDIALED connection, then its event lands
	jpeg := []byte{0xFF, 0xD8, 9, 9, 0xFF, 0xD9}
	sink := healthy.sinkSnapshot()
	sink(2, 1, 0, 1, stampStreamChunk("jpge", jpeg))
	healthy.push("job", map[string]any{"tag": "j1", "ok": true, "out": "done"})

	ev := w.recv()
	if asStr(ev["type"]) != "event" {
		t.Fatalf("event push = %#v", ev)
	}
	data := asMap(asMap(ev["event"])["data"])
	frames, ok := data["frames"].([]any)
	if !ok || len(frames) != 1 {
		t.Fatalf("job event data = %#v — the redial silently swallowed the frame window (honest-drops violation)", data)
	}
	fm := asMap(frames[0])
	if asInt(fm["ch"]) != 2 || asStr(fm["name"]) != "jpge" {
		t.Fatalf("frame meta = %#v, want ch=2 name=jpge", fm)
	}
	got, derr := base64.StdEncoding.DecodeString(asStr(fm["data_b64"]))
	if derr != nil || !bytes.Equal(got, jpeg) {
		t.Fatalf("data_b64 = %x (err %v), want %x", got, derr, jpeg)
	}
}
