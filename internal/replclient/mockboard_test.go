package replclient

// mockBoard is the scripted fake board for the client tests: it owns one
// end of an in-memory byte pipe, speaks the real frame layer (this
// package's own codec and reader), parses requests and replies per a
// per-op script. Tests can additionally tell it to deliver frames in
// fragments (splitting frame headers across writes, §9), to prefix
// replies with len==0 empty frames (§1 tolerance), to go silent, to park
// one op's replies for out-of-order delivery, to emit the fatal §7
// sequence — exactly one protocol error frame with id=nil followed by
// permanent silence, which is what the real board does after a
// frame-layer violation (e.g. a host→board 0x02 type byte — data frames
// are board→host only) — and to burst 0x02 data frames between the ctrl
// frames, in order or with injected chunk gaps.

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// pipeLink adapts one end of a net.Pipe to the Link interface.
type pipeLink struct {
	net.Conn
}

// Kind reports "tcp": the pipe behaves like the stream endpoint.
func (pipeLink) Kind() string { return "tcp" }

// boardError scripts an error reply {class, msg} (§3).
type boardError struct {
	class string
	msg   string
}

// mockRequest is one request the board received.
type mockRequest struct {
	op   string
	id   any
	args map[string]any
}

type mockBoard struct {
	conn net.Conn // board end of the pipe
	Link Link     // client end, handed to NewClient

	wmu sync.Mutex // serializes writes to the pipe

	mu       sync.Mutex
	handlers map[string]func(args map[string]any) (map[string]any, *boardError)
	requests []mockRequest
	hello    map[string]any   // default hello result (role granted per request)
	job      map[string]any   // when set: pushed as a job event after each submit ack
	holdOp   string           // replies for this op are parked until flushHeld
	held     []map[string]any // parked replies (out-of-order scripting)
	frag     int              // deliver frames in n-byte fragments (0 = whole)
	zeroLen  bool             // prefix replies with a len==0 empty frame (§1)
	silent   bool             // consume requests, answer nothing
	dead     bool             // fatal emitted: CS_DEAD, input discarded (§7)
}

// newMockBoard starts a fresh mock board and closes it on cleanup.
func newMockBoard(t *testing.T) *mockBoard {
	t.Helper()
	a, b := net.Pipe()
	mb := startMockBoard(t, b)
	mb.Link = pipeLink{a}
	return mb
}

// serveMockBoardOver runs the mock board protocol over an arbitrary
// connection (e.g. a TCP conn accepted by a test listener, so transport
// dialers can be exercised end-to-end). The caller owns the client end.
func serveMockBoardOver(t *testing.T, conn net.Conn) *mockBoard {
	t.Helper()
	return startMockBoard(t, conn)
}

// startMockBoard wires the default script around one end of a
// connection and serves until cleanup.
func startMockBoard(t *testing.T, conn net.Conn) *mockBoard {
	t.Helper()
	mb := &mockBoard{
		conn:     conn,
		handlers: map[string]func(args map[string]any) (map[string]any, *boardError){},
		hello:    defaultHello(),
	}
	mb.handlers["hello"] = mb.helloHandler
	mb.handlers["ping"] = func(args map[string]any) (map[string]any, *boardError) {
		return map[string]any{"uptime_us": int64(123456)}, nil
	}
	mb.handlers["submit"] = func(args map[string]any) (map[string]any, *boardError) {
		tag, _ := args["tag"].(string)
		return map[string]any{"tag": tag, "queued": true}, nil
	}
	go mb.serve()
	t.Cleanup(func() { mb.close() })
	return mb
}

// defaultHello is a realistic protocol_ver=5 hello result (§2 full
// picture; ver 4 = the 0x02 data-frame overlay, B9); the role key is
// granted per request by helloHandler.
func defaultHello() map[string]any {
	return map[string]any{
		"protocol_ver": int64(5),
		"api_version":  int64(1),
		"fw":           "xuansi-fw v0.1.0",
		"fw_build":     "deadbee",
		"fw_version":   "0.2.0",
		"idf":          "v5.5.5",
		"chip":         "ESP32-S3",
		"lua":          "Lua 5.4.9",
		"reset":        "usb",
		"fs":           true,
		"net":          map[string]any{"state": "off", "ssid": "", "ip": "", "port": int64(7777)},
		"heap": map[string]any{
			"internal_free_kb":    int64(170),
			"internal_largest_kb": int64(113),
			"psram_free_kb":       int64(7859),
			"psram_largest_kb":    int64(7840),
		},
		"gc": map[string]any{"mode": "gen", "pause": int64(200), "stepmul": int64(100)},
		"caps": map[string]any{
			"ops":    []any{"hello", "ping", "eval", "fs.write", "fs.read"},
			"limits": map[string]any{"file_max": int64(16384), "frame_max": int64(65536)},
		},
	}
}

// helloHandler grants the requested role (single-connection mock: no
// arbitration contention, §12).
func (b *mockBoard) helloHandler(args map[string]any) (map[string]any, *boardError) {
	b.mu.Lock()
	res := make(map[string]any, len(b.hello)+1)
	for k, v := range b.hello {
		res[k] = v
	}
	b.mu.Unlock()
	role := "controller"
	if obs, _ := args["observer"].(bool); obs {
		role = "observer"
	}
	res["role"] = role
	return res, nil
}

// serve is the board's frame loop: parse each request, log it, reply per
// script. A frame-layer violation from the client triggers the §7 fatal
// sequence; after it, input is drained and discarded.
func (b *mockBoard) serve() {
	fr := newFrameReader(b.conn)
	for {
		f, err := fr.next()
		if err != nil {
			if !errors.Is(err, errFatal) {
				return // pipe closed — normal teardown
			}
			b.goFatal(err.Error())
			b.drain()
			return
		}
		if f.Type == frameTypeData {
			// Data frames are board→host only: from the host a 0x02 is
			// still a fatal violation (§7) — play the real board.
			b.goFatal("frame type 0x02 from host")
			b.drain()
			return
		}
		b.mu.Lock()
		if b.dead {
			b.mu.Unlock()
			continue // CS_DEAD: input silently discarded
		}
		b.mu.Unlock()
		msg, err := decodeMessage(f.Payload)
		if err != nil {
			continue // framed garbage: drop, stay framed
		}
		op, _ := msg["op"].(string)
		args, _ := msg["args"].(map[string]any)
		b.mu.Lock()
		b.requests = append(b.requests, mockRequest{op: op, id: msg["id"], args: args})
		silent, holdOp := b.silent, b.holdOp
		h := b.handlers[op]
		job := b.job
		b.mu.Unlock()
		if silent {
			continue
		}
		var res map[string]any
		var berr *boardError
		if h != nil {
			res, berr = h(args)
		} else {
			berr = &boardError{class: "api", msg: "unknown op " + op}
		}
		reply := map[string]any{"op": "reply", "id": msg["id"]}
		if berr != nil {
			reply["error"] = map[string]any{"class": berr.class, "msg": berr.msg}
		} else {
			reply["result"] = res
		}
		if op == holdOp {
			b.mu.Lock()
			b.held = append(b.held, reply)
			b.mu.Unlock()
			continue
		}
		b.send(reply)
		if op == "submit" && berr == nil && job != nil {
			b.pushEvent("job", job)
		}
	}
}

// drain consumes and discards input until the pipe closes (the board's
// post-fatal behavior, §7).
func (b *mockBoard) drain() {
	buf := make([]byte, 4096)
	for {
		if _, err := b.conn.Read(buf); err != nil {
			return
		}
	}
}

// send writes one framed ctrl message, honoring the fragmentation and
// zero-len knobs.
func (b *mockBoard) send(msg map[string]any) {
	payload, err := encodeMessage(msg)
	if err != nil {
		return
	}
	f, err := frame(payload)
	if err != nil {
		return
	}
	b.writeWire(f)
}

// sendData writes one 0x02 data frame as the board (board→host only),
// through the same fragmentation and zero-len knobs as send — so
// interleaved data/ctrl bursts, split deliveries, and gap scenarios
// can all be scripted alike.
func (b *mockBoard) sendData(ch byte, seq uint32, idx, total uint16, payload []byte) {
	b.writeWire(dataFrame(ch, seq, idx, total, payload))
}

// writeWire writes whole frame bytes, honoring the fragmentation and
// zero-len knobs.
func (b *mockBoard) writeWire(f []byte) {
	b.mu.Lock()
	frag, zero := b.frag, b.zeroLen
	b.mu.Unlock()
	b.wmu.Lock()
	defer b.wmu.Unlock()
	if zero {
		if _, err := b.conn.Write([]byte{0, 0, 0, 0}); err != nil {
			return
		}
	}
	if frag <= 0 {
		b.conn.Write(f)
		return
	}
	for len(f) > 0 {
		n := frag
		if n > len(f) {
			n = len(f)
		}
		if _, err := b.conn.Write(f[:n]); err != nil {
			return
		}
		f = f[n:]
	}
}

// pushEvent delivers an event frame as the board (§8 shape).
func (b *mockBoard) pushEvent(source string, data map[string]any) {
	b.send(map[string]any{
		"op":    "event",
		"id":    int64(0),
		"event": map[string]any{"source": source, "data": data},
	})
}

// goFatal emits the §7 fatal sequence: exactly one protocol error frame
// with id=nil, then permanent silence.
func (b *mockBoard) goFatal(msg string) {
	b.mu.Lock()
	if b.dead {
		b.mu.Unlock()
		return
	}
	b.dead = true
	b.mu.Unlock()
	b.send(map[string]any{
		"op":    "reply",
		"id":    nil,
		"error": map[string]any{"class": "protocol", "msg": msg},
	})
}

// sendRaw writes bytes as the board without framing (malformed probes:
// oversize length prefix, reserved type byte).
func (b *mockBoard) sendRaw(p []byte) {
	b.wmu.Lock()
	defer b.wmu.Unlock()
	b.conn.Write(p)
}

// close ends the board side; the client end sees EOF.
func (b *mockBoard) close() {
	b.conn.Close()
}

// on scripts the reply for op, replacing the default.
func (b *mockBoard) on(op string, fn func(args map[string]any) (map[string]any, *boardError)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[op] = fn
}

// got returns the requests received so far, in order.
func (b *mockBoard) got() []mockRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]mockRequest, len(b.requests))
	copy(out, b.requests)
	return out
}

// setFrag delivers frames in n-byte chunks, splitting frame headers
// across writes (§9).
func (b *mockBoard) setFrag(n int) {
	b.mu.Lock()
	b.frag = n
	b.mu.Unlock()
}

// setZeroLen prefixes replies with empty len==0 frames (§1 tolerance).
func (b *mockBoard) setZeroLen(on bool) {
	b.mu.Lock()
	b.zeroLen = on
	b.mu.Unlock()
}

// muteAll makes the board consume requests without answering (silence
// scripting for timeout tests).
func (b *mockBoard) muteAll(on bool) {
	b.mu.Lock()
	b.silent = on
	b.mu.Unlock()
}

// setJob scripts the job event pushed after each successful submit ack.
func (b *mockBoard) setJob(data map[string]any) {
	b.mu.Lock()
	b.job = data
	b.mu.Unlock()
}

// hold parks every reply for op until flushHeld sends them — scripting
// out-of-order replies.
func (b *mockBoard) hold(op string) {
	b.mu.Lock()
	b.holdOp = op
	b.mu.Unlock()
}

// flushHeld sends the parked replies.
func (b *mockBoard) flushHeld() {
	b.mu.Lock()
	held := b.held
	b.held = nil
	b.mu.Unlock()
	for _, m := range held {
		b.send(m)
	}
}

// waitFor polls cond until it holds or a 2s deadline passes.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// recvEvent receives one event with a timeout.
func recvEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an event")
		return Event{}
	}
}

// newTestClient wires a Client to a fresh mock board; both are closed on
// cleanup.
func newTestClient(t *testing.T) (*Client, *mockBoard) {
	t.Helper()
	b := newMockBoard(t)
	c := NewClient(b.Link)
	t.Cleanup(func() { c.Close() })
	return c, b
}

// TestMockBoardDefaultScript pins the mock's built-in script: hello
// grants the requested role, ping answers, a scripted eval round-trips
// and a submit ack is followed by the scripted job event — over a real
// Client, so the test infrastructure itself is under test.
func TestMockBoardDefaultScript(t *testing.T) {
	c, b := newTestClient(t)
	ctx := context.Background()

	h, err := c.Hello(false, "sanity")
	if err != nil {
		t.Fatalf("hello: %v", err)
	}
	if h.Role != "controller" || h.ProtocolVer != 5 {
		t.Fatalf("hello = role %q ver %d, want controller/5", h.Role, h.ProtocolVer)
	}
	res, err := c.Op(ctx, "ping", nil)
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if res["uptime_us"] != int64(123456) {
		t.Fatalf("ping result = %#v", res)
	}

	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		return map[string]any{"out": "ok", "value": int64(7)}, nil
	})
	out, value, err := c.Eval(ctx, "return 7", nil)
	if err != nil || out != "ok" || value != int64(7) {
		t.Fatalf("eval = (%q, %#v, %v)", out, value, err)
	}

	jobs := make(chan Event, 1)
	cancel := c.OnEvent("job", func(e Event) { jobs <- e })
	defer cancel()
	b.setJob(map[string]any{"tag": "j1", "ok": true, "out": "done"})
	if err := c.Submit(ctx, "return 1", "j1"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	select {
	case e := <-jobs:
		if e.Source != "job" || e.Data["tag"] != "j1" || e.Data["ok"] != true {
			t.Fatalf("job event = %#v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job event not delivered after submit ack")
	}
}
