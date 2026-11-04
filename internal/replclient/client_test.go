package replclient

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestOpTimeoutsTable: the §6-derived silence table — immediate queries
// 2s, fs.* 10s, eval/submit 5s (budget domain 0..4000ms + 1s), script.*
// 5s; diag.wdttest is deliberately absent (it never replies by design,
// §6 — the caller sees the default timeout expire and reconnects);
// unknown ops fall back to the 5s default.
func TestOpTimeoutsTable(t *testing.T) {
	for _, op := range []string{"hello", "ping", "caps", "log.read", "wifi.status"} {
		if OpTimeouts[op] != 2*time.Second {
			t.Errorf("OpTimeouts[%q] = %v, want 2s", op, OpTimeouts[op])
		}
	}
	for _, entry := range []struct {
		key  string
		want time.Duration
	}{
		{"fs.*", 10 * time.Second},
		{"eval", 5 * time.Second},
		{"submit", 5 * time.Second},
		{"script.*", 5 * time.Second},
	} {
		if OpTimeouts[entry.key] != entry.want {
			t.Errorf("OpTimeouts[%q] = %v, want %v", entry.key, OpTimeouts[entry.key], entry.want)
		}
	}
	if _, ok := OpTimeouts["diag.wdttest"]; ok {
		t.Error(`OpTimeouts["diag.wdttest"] must stay explicitly skipped (never replies, §6)`)
	}
	for _, tc := range []struct {
		op   string
		want time.Duration
	}{
		{"hello", 2 * time.Second},
		{"fs.write", 10 * time.Second},    // fs.* family
		{"fs.read", 10 * time.Second},     // fs.* family
		{"script.start", 5 * time.Second}, // script.* family
		{"budget.set", 5 * time.Second},   // default
		{"diag.stats", 5 * time.Second},   // default
	} {
		if got := opTimeout(tc.op); got != tc.want {
			t.Errorf("opTimeout(%q) = %v, want %v", tc.op, got, tc.want)
		}
	}
}

// TestHelloParsesAndCachesCaps: hello round-trips the §2 full picture,
// reflects the granted role, and caches caps (hello.caps is the caps
// op's dict — the daemon never sends a separate caps op).
func TestHelloParsesAndCachesCaps(t *testing.T) {
	c, b := newTestClient(t)

	h, err := c.Hello(false, "daemon-test")
	if err != nil {
		t.Fatalf("hello: %v", err)
	}
	if h.ProtocolVer != 3 || h.APIVersion != 1 {
		t.Fatalf("ver/api = %d/%d, want 3/1", h.ProtocolVer, h.APIVersion)
	}
	if h.Role != "controller" {
		t.Fatalf("role = %q, want controller", h.Role)
	}
	if h.Fw != "xuansi-fw v0.1.0" || h.FwBuild != "deadbee" || h.Chip != "ESP32-S3" || h.Lua != "Lua 5.4.9" || h.Idf != "v5.5.5" {
		t.Fatalf("identity fields wrong: %#v", h)
	}
	if h.Reset != "usb" || !h.Fs {
		t.Fatalf("reset/fs = %q/%v", h.Reset, h.Fs)
	}
	if h.Net["port"] != int64(7777) || h.Heap["psram_free_kb"] != int64(7859) {
		t.Fatalf("net/heap snapshots wrong: %#v / %#v", h.Net, h.Heap)
	}
	if caps := c.Caps(); !reflect.DeepEqual(caps, h.Caps) || caps == nil {
		t.Fatalf("caps cache = %#v, want hello caps %#v", caps, h.Caps)
	}

	reqs := b.got()
	if len(reqs) != 1 || reqs[0].op != "hello" {
		t.Fatalf("board saw %#v", reqs)
	}
	if reqs[0].args["client"] != "daemon-test" {
		t.Fatalf("hello args = %#v", reqs[0].args)
	}
	if _, ok := reqs[0].args["observer"]; ok {
		t.Error("non-observer hello must not send the observer key")
	}

	h2, err := c.Hello(true, "daemon-test")
	if err != nil {
		t.Fatalf("observer hello: %v", err)
	}
	if h2.Role != "observer" {
		t.Fatalf("observer hello role = %q, want observer", h2.Role)
	}
	if reqs := b.got(); len(reqs) != 2 || reqs[1].args["observer"] != true {
		t.Fatalf("observer hello request args = %#v", reqs)
	}
}

// TestPingOp: a plain op round-trips its result map.
func TestPingOp(t *testing.T) {
	c, _ := newTestClient(t)
	res, err := c.Op(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if res["uptime_us"] != int64(123456) {
		t.Fatalf("ping result = %#v", res)
	}
}

// TestEvalPassesCodeAndArgs: eval carries the code key always and the
// args key only when non-nil (v3: args becomes the Lua global ARGS);
// out and the bridged value come back.
func TestEvalPassesCodeAndArgs(t *testing.T) {
	c, b := newTestClient(t)
	seen := make(chan map[string]any, 2)
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		seen <- args
		return map[string]any{"out": "ran", "value": []any{"a", int64(1)}}, nil
	})

	out, value, err := c.Eval(context.Background(), "return ARGS[1]", []any{"a", int64(1)})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if out != "ran" {
		t.Fatalf("out = %q, want ran", out)
	}
	if !reflect.DeepEqual(value, []any{"a", int64(1)}) {
		t.Fatalf("value = %#v", value)
	}
	select {
	case a := <-seen:
		if a["code"] != "return ARGS[1]" {
			t.Fatalf("eval request code = %#v", a["code"])
		}
		if !reflect.DeepEqual(a["args"], []any{"a", int64(1)}) {
			t.Fatalf("eval request args = %#v", a["args"])
		}
	default:
		t.Fatal("eval handler did not run")
	}

	if _, _, err := c.Eval(context.Background(), "return 1", nil); err != nil {
		t.Fatalf("args-less eval: %v", err)
	}
	select {
	case a := <-seen:
		if _, ok := a["args"]; ok {
			t.Fatal("args key must be absent from the request when nil")
		}
	default:
		t.Fatal("second eval handler did not run")
	}
}

// TestEvalErrorPreservesClassAndByteOffset: a board error reply surfaces
// as *OpError with class and msg verbatim — including the ` at byte N`
// suffix (§3).
func TestEvalErrorPreservesClassAndByteOffset(t *testing.T) {
	c, b := newTestClient(t)
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		return nil, &boardError{class: "rt", msg: "cannot bridge float at byte 3"}
	})
	out, _, err := c.Eval(context.Background(), "return 3.0", nil)
	if out != "" {
		t.Fatalf("out = %q, want empty on error", out)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("err = %v (%T), want *OpError", err, err)
	}
	if oe.Class != "rt" || oe.Msg != "cannot bridge float at byte 3" {
		t.Fatalf("OpError = %q/%q", oe.Class, oe.Msg)
	}
	if !strings.HasSuffix(oe.Msg, " at byte 3") {
		t.Fatalf("byte-offset suffix not preserved: %q", oe.Msg)
	}
	if !strings.Contains(err.Error(), "eval: error rt:") {
		t.Fatalf("error text = %q", err.Error())
	}
}

// TestProtocolErrorWithIDIsNotFatal: a protocol-class error addressed to
// the request's id (e.g. an observer violation, §5) is an ordinary
// error reply — only the nil-id protocol frame kills the connection
// (§7).
func TestProtocolErrorWithIDIsNotFatal(t *testing.T) {
	c, b := newTestClient(t)
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		return nil, &boardError{class: "protocol", msg: "observer not allowed: eval"}
	})
	_, err := c.Op(context.Background(), "eval", map[string]any{"code": "x"})
	var oe *OpError
	if !errors.As(err, &oe) || oe.Class != "protocol" {
		t.Fatalf("err = %v, want protocol OpError", err)
	}
	if c.Dead() {
		t.Fatal("id-addressed protocol error must not kill the connection")
	}
	if _, err := c.Op(context.Background(), "ping", nil); err != nil {
		t.Fatalf("connection unusable after protocol error reply: %v", err)
	}
}

// TestSubmitAckAndJobEvent: submit returns on the ack; the result
// arrives later as a job event on the same connection, deliverable via
// OnEvent("job") (§8).
func TestSubmitAckAndJobEvent(t *testing.T) {
	c, b := newTestClient(t)
	jobs := make(chan Event, 1)
	cancel := c.OnEvent("job", func(e Event) { jobs <- e })
	defer cancel()

	b.setJob(map[string]any{"tag": "j1", "ok": true, "out": "done"})
	if err := c.Submit(context.Background(), "return 1", "j1"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	select {
	case e := <-jobs:
		if e.Source != "job" {
			t.Fatalf("job event source = %q", e.Source)
		}
		if e.Data["tag"] != "j1" || e.Data["ok"] != true || e.Data["out"] != "done" {
			t.Fatalf("job event data = %#v", e.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job event not delivered after the submit ack")
	}
}

// TestOnEventPrefixDelivery: prefix subscribers get only matching
// sources; cancel stops delivery.
func TestOnEventPrefixDelivery(t *testing.T) {
	c, b := newTestClient(t)
	gpio := make(chan Event, 4)
	cancel := c.OnEvent("gpio.", func(e Event) { gpio <- e })
	defer cancel()

	b.pushEvent("gpio.5", map[string]any{"edge": "rising", "level": int64(1)})
	b.pushEvent("uart1.rx", map[string]any{"data": []byte{1, 2}})
	b.pushEvent("gpio.6", map[string]any{"edge": "falling", "level": int64(0)})

	e := recvEvent(t, gpio)
	if e.Source != "gpio.5" || e.Data["edge"] != "rising" || e.Data["level"] != int64(1) {
		t.Fatalf("first gpio event = %#v", e)
	}
	e = recvEvent(t, gpio)
	if e.Source != "gpio.6" || e.Data["edge"] != "falling" {
		t.Fatalf("second gpio event = %#v", e)
	}

	cancel()
	b.pushEvent("gpio.7", map[string]any{"edge": "rising"})
	select {
	case e := <-gpio:
		t.Fatalf("event delivered after cancel: %#v", e)
	case <-time.After(150 * time.Millisecond):
	}
}

// TestOpTimeoutAndRecovery: a silent board times the op out with a
// deadline error; the connection stays usable and later ops route
// correctly (the timed-out id leaves no stale slot).
func TestOpTimeoutAndRecovery(t *testing.T) {
	c, b := newTestClient(t)
	old := OpTimeouts["ping"]
	OpTimeouts["ping"] = 80 * time.Millisecond
	defer func() { OpTimeouts["ping"] = old }()
	b.muteAll(true)

	start := time.Now()
	_, err := c.Op(context.Background(), "ping", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timeout took %v, want ~80ms", elapsed)
	}
	if c.Dead() {
		t.Fatal("a silent board times the op out but does not kill the connection")
	}

	b.muteAll(false)
	res, err := c.Op(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("post-timeout ping: %v", err)
	}
	if res["uptime_us"] != int64(123456) {
		t.Fatalf("post-timeout ping result = %#v", res)
	}
}

// TestOpContextCancel: caller cancellation aborts the wait immediately.
func TestOpContextCancel(t *testing.T) {
	c, b := newTestClient(t)
	b.muteAll(true)
	defer b.muteAll(false)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := c.Op(ctx, "ping", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestConcurrentOpsOutOfOrder: two ops in flight; the board answers the
// later one first (the earlier op's reply is held back) — each Op still
// pairs with its own reply (§6: ops queue, replies may reorder).
func TestConcurrentOpsOutOfOrder(t *testing.T) {
	c, b := newTestClient(t)
	b.hold("eval")
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		return map[string]any{"out": "slow"}, nil
	})
	type opResult struct {
		out string
		err error
	}
	results := make(chan opResult, 1)
	go func() {
		res, err := c.Op(context.Background(), "eval", map[string]any{"code": "sleep(1)"})
		if err != nil {
			results <- opResult{err: err}
			return
		}
		out, _ := res["out"].(string)
		results <- opResult{out: out}
	}()
	waitFor(t, func() bool {
		reqs := b.got()
		return len(reqs) > 0 && reqs[0].op == "eval"
	})

	if _, err := c.Op(context.Background(), "ping", nil); err != nil {
		t.Fatalf("ping while eval reply is held: %v", err)
	}
	b.flushHeld()

	select {
	case r := <-results:
		if r.err != nil {
			t.Fatalf("held eval op: %v", r.err)
		}
		if r.out != "slow" {
			t.Fatalf("held eval result = %q, want slow", r.out)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("held reply not routed back to its op")
	}
}

// TestFatalProtocolFrameKillsClient: the board's nil-id protocol error
// frame (its CS_DEAD notice, §7) fails the pending op, marks the client
// dead with a protocol reason, and fast-fails later ops.
func TestFatalProtocolFrameKillsClient(t *testing.T) {
	c, b := newTestClient(t)
	opErr := make(chan error, 1)
	b.hold("eval") // keep the reply pending so the fatal frame arrives first
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		return map[string]any{"out": "never"}, nil
	})
	go func() {
		_, err := c.Op(context.Background(), "eval", map[string]any{"code": "1"})
		opErr <- err
	}()
	waitFor(t, func() bool { return len(b.got()) > 0 })

	b.goFatal("bad frame type 0x02")
	select {
	case err := <-opErr:
		if !errors.Is(err, ErrDead) {
			t.Fatalf("pending op err = %v, want ErrDead", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending op not failed by the fatal frame")
	}
	waitFor(t, c.Dead)
	if reason := c.DeadReason(); !strings.Contains(reason, "protocol") {
		t.Fatalf("dead reason = %q, want it to mention protocol", reason)
	}

	start := time.Now()
	if _, err := c.Op(context.Background(), "ping", nil); !errors.Is(err, ErrDead) {
		t.Fatalf("post-dead op err = %v, want ErrDead", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("post-dead op took %v, want fast-fail", elapsed)
	}
}

// TestOversizeFrameFromBoardKills: a length prefix above MAX_FRAME in
// the board's own stream is a fatal desync (§7) — the client marks the
// connection dead and fails pending ops.
func TestOversizeFrameFromBoardKills(t *testing.T) {
	c, b := newTestClient(t)
	b.muteAll(true)
	opErr := make(chan error, 1)
	go func() {
		_, err := c.Op(context.Background(), "ping", nil)
		opErr <- err
	}()
	waitFor(t, func() bool { return len(b.got()) > 0 })

	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], maxFrame+1) // 65537 > 65536
	hdr[4] = 0x01
	b.sendRaw(hdr[:])

	select {
	case err := <-opErr:
		if !errors.Is(err, ErrDead) {
			t.Fatalf("pending op err = %v, want ErrDead", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversize frame did not fail the pending op")
	}
	waitFor(t, c.Dead)
	if reason := c.DeadReason(); !strings.Contains(reason, "65537") {
		t.Fatalf("dead reason = %q, want it to name 65537", reason)
	}
}

// TestZeroLenFramesTolerated: the board prefixing replies with len==0
// empty frames (§1) loses no frames — hello and ops still round-trip.
func TestZeroLenFramesTolerated(t *testing.T) {
	c, b := newTestClient(t)
	b.setZeroLen(true)

	h, err := c.Hello(false, "z")
	if err != nil {
		t.Fatalf("hello: %v", err)
	}
	if h.ProtocolVer != 3 {
		t.Fatalf("hello ver = %d", h.ProtocolVer)
	}
	if _, err := c.Op(context.Background(), "ping", nil); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

// TestFragmentedBoardWrites: the board delivering frames in 3-byte
// fragments — splitting the 5-byte header across writes (§9) — still
// round-trips hello and eval, nested values intact.
func TestFragmentedBoardWrites(t *testing.T) {
	c, b := newTestClient(t)
	b.setFrag(3)

	h, err := c.Hello(false, "f")
	if err != nil {
		t.Fatalf("hello: %v", err)
	}
	if h.ProtocolVer != 3 || h.Role != "controller" {
		t.Fatalf("hello = ver %d role %q", h.ProtocolVer, h.Role)
	}

	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		return map[string]any{"out": "chunked", "value": map[string]any{"k": []any{int64(1), int64(2)}}}, nil
	})
	out, value, err := c.Eval(context.Background(), "return {k={1,2}}", nil)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if out != "chunked" {
		t.Fatalf("out = %q", out)
	}
	if !reflect.DeepEqual(value, map[string]any{"k": []any{int64(1), int64(2)}}) {
		t.Fatalf("value = %#v", value)
	}
}

// TestBoardCloseFailsPendingOps: the board end closing (EOF) fails
// pending ops and marks the client dead.
func TestBoardCloseFailsPendingOps(t *testing.T) {
	c, b := newTestClient(t)
	b.muteAll(true)
	opErr := make(chan error, 1)
	go func() {
		_, err := c.Op(context.Background(), "ping", nil)
		opErr <- err
	}()
	waitFor(t, func() bool { return len(b.got()) > 0 })

	b.close()
	select {
	case err := <-opErr:
		if !errors.Is(err, ErrDead) {
			t.Fatalf("pending op err = %v, want ErrDead", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("board EOF did not fail the pending op")
	}
	waitFor(t, c.Dead)
}

// TestReplySlotsReleased (I-1 regression): every Op exit — success,
// timeout, context cancellation — releases its reply slot; the Python
// oracle does this with `finally: del replies[k]` on all paths. 50
// sequential successful ops plus one timed-out and one cancelled op
// must leave the router's table empty (ids are monotonic, so a leaked
// slot is a permanent leak).
func TestReplySlotsReleased(t *testing.T) {
	c, b := newTestClient(t)
	for i := 0; i < 50; i++ {
		if _, err := c.Op(context.Background(), "ping", nil); err != nil {
			t.Fatalf("ping %d: %v", i, err)
		}
	}

	// timeout exit
	old := OpTimeouts["ping"]
	OpTimeouts["ping"] = 50 * time.Millisecond
	b.muteAll(true)
	if _, err := c.Op(context.Background(), "ping", nil); !errors.Is(err, context.DeadlineExceeded) {
		b.muteAll(false)
		OpTimeouts["ping"] = old
		t.Fatalf("muted ping err = %v, want context.DeadlineExceeded", err)
	}
	b.muteAll(false)
	OpTimeouts["ping"] = old

	// context-cancel exit (slot registered, then cancelled mid-wait)
	b.muteAll(true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	if _, err := c.Op(ctx, "ping", nil); !errors.Is(err, context.Canceled) {
		b.muteAll(false)
		t.Fatalf("cancelled ping err = %v, want context.Canceled", err)
	}
	b.muteAll(false)

	c.rt.mu.Lock()
	n := len(c.rt.replies)
	c.rt.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d reply slots leaked across op exits", n)
	}
}

// eofLink is a Link whose Read can deliver the final bytes together
// with io.EOF — the io.Reader-legal shape a transport hit at unplug
// time may produce.
type eofLink struct {
	mu   sync.Mutex
	cond *sync.Cond
	buf  []byte
	eof  bool
}

func newEOFLink() *eofLink {
	l := &eofLink{}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// feed queues bytes for delivery; eofAtEnd makes the read that drains
// the last queued byte return io.EOF together with the data.
func (l *eofLink) feed(p []byte, eofAtEnd bool) {
	l.mu.Lock()
	l.buf = append(l.buf, p...)
	l.eof = eofAtEnd
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *eofLink) Read(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.buf) == 0 && !l.eof {
		l.cond.Wait()
	}
	n := copy(p, l.buf)
	l.buf = l.buf[n:]
	var err error
	if len(l.buf) == 0 && l.eof {
		err = io.EOF
	}
	return n, err
}

func (l *eofLink) Write(p []byte) (int, error) { return len(p), nil }

func (l *eofLink) Close() error {
	l.mu.Lock()
	l.eof = true
	l.cond.Broadcast()
	l.mu.Unlock()
	return nil
}

func (l *eofLink) Kind() string { return "tcp" }

// TestReaderContractFinalChunkWithError (I-2 regression): a Read
// returning (n>0, io.EOF) — legal per the io.Reader contract — must not
// lose the bytes it carried: a complete reply frame arriving in that
// final chunk is delivered to its waiter, and only after the buffered
// frames are exhausted does the connection die of EOF.
func TestReaderContractFinalChunkWithError(t *testing.T) {
	l := newEOFLink()
	c := NewClient(l)
	t.Cleanup(func() { c.Close() })

	payload, err := encodeMessage(map[string]any{
		"op":     "reply",
		"id":     int64(1),
		"result": map[string]any{"uptime_us": int64(1)},
	})
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	f, err := frame(payload)
	if err != nil {
		t.Fatalf("frame: %v", err)
	}
	l.feed(f, true) // the whole reply frame in one (n>0, io.EOF) read

	res, err := c.Op(context.Background(), "ping", nil) // first op → id 1
	if err != nil {
		t.Fatalf("op: %v — reply lost to a (n>0, EOF) read", err)
	}
	if res["uptime_us"] != int64(1) {
		t.Fatalf("reply result = %#v", res)
	}
	waitFor(t, c.Dead)
	if reason := c.DeadReason(); !strings.Contains(reason, "closed") && !strings.Contains(reason, "EOF") {
		t.Fatalf("dead reason = %q, want it to name the EOF/close", reason)
	}
}

// marks the client dead as locally closed, and is safe to repeat.
// TestCloseIsCleanAndIdempotent: Close stops the router goroutine,
// marks the client dead as locally closed, and is safe to repeat.
func TestCloseIsCleanAndIdempotent(t *testing.T) {
	c, _ := newTestClient(t)
	if _, err := c.Hello(false, "closer"); err != nil {
		t.Fatalf("hello: %v", err)
	}
	done := c.rt.done

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("router goroutine leaked past Close")
	}
	if !c.Dead() || c.DeadReason() != "client closed" {
		t.Fatalf("dead = %v/%q, want true/client closed", c.Dead(), c.DeadReason())
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
