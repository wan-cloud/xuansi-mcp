package apiclient

// client_test.go — the persistent client against an in-process fake
// daemon: a unix listener speaking the framed JSON contract
// (internal/server/proto.go), scripted per test for reply order, event
// pushes and death. Every wait is timeout-guarded: no assertion sleeps
// on acceptance.

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/server"
)

// ---------------------------------------------------------------------------
// fake daemon harness

// fakeDaemon is a scriptable stand-in daemon: one accepted connection
// per test, handed to a handler closure that reads requests and writes
// replies/events/death in whatever order the test needs.
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

// readReq reads one request frame off the fake's side. Any read end
// (EOF from the client's Close, or a broken pipe) returns a zero
// Request, ending the handler loop silently: every client-side
// expectation is timeout-guarded, so a script that dies mid-way
// surfaces as a clear "timed out waiting for" failure, not as noise
// from the expected shutdown path.
func readReq(t *testing.T, conn net.Conn) server.Request {
	t.Helper()
	var req server.Request
	if err := server.ReadMsg(conn, &req); err != nil {
		return server.Request{}
	}
	return req
}

// writeReply replies to id with a result map.
func writeReply(t *testing.T, conn net.Conn, id int64, result map[string]any) {
	t.Helper()
	if err := server.WriteMsg(conn, &server.Reply{Type: "reply", ID: id, Result: result}); err != nil {
		t.Errorf("fake daemon reply: %v", err)
	}
}

// writeReplyErr replies to id with a structured error.
func writeReplyErr(t *testing.T, conn net.Conn, id int64, rerr *server.RPCError) {
	t.Helper()
	if err := server.WriteMsg(conn, &server.Reply{Type: "reply", ID: id, Error: rerr}); err != nil {
		t.Errorf("fake daemon error reply: %v", err)
	}
}

// pushEvent writes one unsolicited event push.
func pushEvent(t *testing.T, conn net.Conn, board, source string, data map[string]any) {
	t.Helper()
	ev := server.EventPush{Type: "event", Board: board, Event: server.WireEvent{Source: source, Data: data}}
	if err := server.WriteMsg(conn, &ev); err != nil {
		t.Errorf("fake daemon push: %v", err)
	}
}

// ---------------------------------------------------------------------------
// helpers

type callOut struct {
	res  map[string]any
	rerr *server.RPCError
	err  error
}

// callAsync runs one Call on its own goroutine; the buffered channel
// makes the outcome observable under a timeout.
func callAsync(c *Client, req server.Request) chan callOut {
	out := make(chan callOut, 1)
	go func() {
		res, rerr, err := c.Call(context.Background(), req)
		out <- callOut{res: res, rerr: rerr, err: err}
	}()
	return out
}

// waitRecv receives one value within 5s or fails the test.
func waitRecv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// ---------------------------------------------------------------------------
// tests

// TestDialUnreachableActionableError: a missing daemon gets the same
// actionable message the CLI prints, naming the path.
func TestDialUnreachableActionableError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sock")
	_, err := Dial(path)
	if err == nil {
		t.Fatal("Dial on a missing socket succeeded")
	}
	for _, want := range []string{
		"cannot reach the daemon at " + path,
		"is `xuansi serve` running?",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err.Error(), want)
		}
	}
}

// TestCallPairsReplyByID: sequential calls over the one connection each
// get their own reply's result.
func TestCallPairsReplyByID(t *testing.T) {
	fd := newFakeDaemon(t)
	fd.serve(t, func(conn net.Conn) {
		for {
			req := readReq(t, conn)
			if req.Type == "" {
				return
			}
			writeReply(t, conn, req.ID, map[string]any{"echo": req.Type, "board": req.Board})
		}
	})
	c, err := Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	res, rerr, err := c.Call(context.Background(), server.Request{Type: "list_boards"})
	if err != nil || rerr != nil {
		t.Fatalf("call 1: err=%v rerr=%v", err, rerr)
	}
	if res["echo"] != "list_boards" {
		t.Errorf("call 1 result = %v, want echo list_boards", res)
	}

	res, rerr, err = c.Call(context.Background(), server.Request{Type: "board_status", Board: "d75884"})
	if err != nil || rerr != nil {
		t.Fatalf("call 2: err=%v rerr=%v", err, rerr)
	}
	if res["echo"] != "board_status" || res["board"] != "d75884" {
		t.Errorf("call 2 result = %v, want echoed board_status/d75884", res)
	}
}

// TestCallDaemonErrorBecomesRPCError: an error reply crosses as a
// non-nil *server.RPCError with nil error, fields preserved.
func TestCallDaemonErrorBecomesRPCError(t *testing.T) {
	fd := newFakeDaemon(t)
	fd.serve(t, func(conn net.Conn) {
		for {
			req := readReq(t, conn)
			if req.Type == "" {
				return
			}
			writeReplyErr(t, conn, req.ID, &server.RPCError{
				Class: "held", Msg: "held by e1", Holder: "e1", Since: "2026-11-05T00:00:00Z",
			})
		}
	})
	c, err := Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	res, rerr, err := c.Call(context.Background(), server.Request{Type: "lease", Board: "d75884"})
	if err != nil {
		t.Fatalf("error reply must not be a Go error: %v", err)
	}
	if res != nil {
		t.Errorf("result = %v, want nil", res)
	}
	if rerr == nil {
		t.Fatal("rerr = nil, want the structured held error")
	}
	if rerr.Class != "held" || rerr.Holder != "e1" {
		t.Errorf("rerr = %+v, want class held holder e1", rerr)
	}
}

// TestOutOfOrderReplies: replies that arrive swapped still pair by id —
// each caller sees its own result, never the other's.
func TestOutOfOrderReplies(t *testing.T) {
	fd := newFakeDaemon(t)
	fd.serve(t, func(conn net.Conn) {
		first := readReq(t, conn)
		second := readReq(t, conn)
		if first.Type == "" || second.Type == "" {
			return
		}
		// answer the second arrival first, then the first
		writeReply(t, conn, second.ID, map[string]any{"board": second.Board})
		writeReply(t, conn, first.ID, map[string]any{"board": first.Board})
	})

	c, err := Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	outA := callAsync(c, server.Request{Type: "board_status", Board: "aaaaaa"})
	outB := callAsync(c, server.Request{Type: "board_status", Board: "bbbbbb"})
	a := waitRecv(t, outA, "call A under swapped replies")
	b := waitRecv(t, outB, "call B under swapped replies")
	for _, o := range []struct {
		name string
		got  callOut
		want string
	}{
		{"A", a, "aaaaaa"},
		{"B", b, "bbbbbb"},
	} {
		if o.got.err != nil || o.got.rerr != nil {
			t.Errorf("call %s: err=%v rerr=%v", o.name, o.got.err, o.got.rerr)
			continue
		}
		if o.got.res["board"] != o.want {
			t.Errorf("call %s got board %v, want %s (crossed pairing)", o.name, o.got.res["board"], o.want)
		}
	}
}

// TestEventFanOutTwoHandlersAndCancel: every registered handler sees
// every push; a cancelled handler sees nothing after its cancel.
func TestEventFanOutTwoHandlersAndCancel(t *testing.T) {
	fd := newFakeDaemon(t)
	fd.serve(t, func(conn net.Conn) {
		// barrier 1: one request round before the first pushes, so
		// registration has surely happened client-side
		req := readReq(t, conn)
		if req.Type == "" {
			return
		}
		writeReply(t, conn, req.ID, map[string]any{"ok": true})
		pushEvent(t, conn, "d75884", "gpio.4", map[string]any{"v": 1.0})
		pushEvent(t, conn, "d75884", "stdout", map[string]any{"line": "hi"})
		// barrier 2: one more round before the post-cancel push
		req = readReq(t, conn)
		if req.Type == "" {
			return
		}
		writeReply(t, conn, req.ID, map[string]any{"ok": true})
		pushEvent(t, conn, "d75884", "gpio.4", map[string]any{"v": 0.0})
		// park: keep the connection open for the quiet-period check
		for {
			if readReq(t, conn).Type == "" {
				return
			}
		}
	})

	c, err := Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	got1 := make(chan server.EventPush, 8)
	got2 := make(chan server.EventPush, 8)
	cancel1 := c.OnEvent(func(ev server.EventPush) { got1 <- ev })
	cancel2 := c.OnEvent(func(ev server.EventPush) { got2 <- ev })
	defer cancel2()

	if _, _, err := c.Call(context.Background(), server.Request{Type: "watch_events", Board: "d75884"}); err != nil {
		t.Fatal(err)
	}
	first := waitRecv(t, got1, "handler 1 first event")
	if first.Board != "d75884" || first.Event.Source != "gpio.4" || first.Event.Data["v"] != 1.0 {
		t.Errorf("event decode = %+v, want board d75884 gpio.4 v=1", first)
	}
	waitRecv(t, got1, "handler 1 second event")
	for i := 0; i < 2; i++ {
		waitRecv(t, got2, "handler 2 event")
	}

	cancel1()
	if _, _, err := c.Call(context.Background(), server.Request{Type: "ping"}); err != nil {
		t.Fatal(err)
	}
	third := waitRecv(t, got2, "handler 2 post-cancel event")
	if third.Event.Data["v"] != 0.0 {
		t.Errorf("post-cancel event = %+v, want v=0", third)
	}
	// quiet period: handler 1 must stay silent now (bounded negative
	// check — the only sleep in this suite, and it asserts absence)
	select {
	case ev := <-got1:
		t.Errorf("cancelled handler 1 received %+v", ev)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestSlowHandlerNeverBlocksReads: a handler that never returns must
// not stall the read loop — the next call still completes on time.
func TestSlowHandlerNeverBlocksReads(t *testing.T) {
	fd := newFakeDaemon(t)
	fd.serve(t, func(conn net.Conn) {
		req := readReq(t, conn)
		if req.Type == "" {
			return
		}
		writeReply(t, conn, req.ID, map[string]any{"ok": true})
		pushEvent(t, conn, "d75884", "gpio.4", map[string]any{"v": 1.0})
		req = readReq(t, conn)
		if req.Type == "" {
			return
		}
		writeReply(t, conn, req.ID, map[string]any{"ok": true})
		for {
			if readReq(t, conn).Type == "" {
				return
			}
		}
	})

	c, err := Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cancel := c.OnEvent(func(ev server.EventPush) {
		started <- struct{}{}
		<-release // this handler never returns until the test ends
	})
	defer cancel()

	if _, _, err := c.Call(context.Background(), server.Request{Type: "watch_events"}); err != nil {
		t.Fatal(err)
	}
	waitRecv(t, started, "the stuck handler took the event")

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Call(context.Background(), server.Request{Type: "list_boards"})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("call under a stuck handler failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read loop stalled on a slow handler: call did not complete")
	}
}

// TestSocketDeathFailsInFlightCallsImmediately: an abrupt socket kill
// returns every in-flight call at once (not at any timeout) and later
// calls fail fast.
func TestSocketDeathFailsInFlightCallsImmediately(t *testing.T) {
	fd := newFakeDaemon(t)
	gone := make(chan struct{})
	fd.serve(t, func(conn net.Conn) {
		if readReq(t, conn).Type == "" {
			return
		}
		conn.Close() // abrupt death, no reply
		fd.ln.Close()
		close(gone)
	})

	c, err := Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	out := callAsync(c, server.Request{Type: "board_status", Board: "d75884"}) // reply never comes
	<-gone

	select {
	case r := <-out:
		if r.err == nil || !errors.Is(r.err, ErrDead) {
			t.Fatalf("in-flight call error = %v, want ErrDead", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight call did not return after socket death")
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Call(context.Background(), server.Request{Type: "list_boards"})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, ErrDead) {
			t.Errorf("post-death call error = %v, want ErrDead (fail fast)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-death call blocked instead of failing fast")
	}
}

// TestCloseWakesInFlightAndIsIdempotent: Close returns every in-flight
// call immediately, and a second Close is a harmless no-op.
func TestCloseWakesInFlightAndIsIdempotent(t *testing.T) {
	fd := newFakeDaemon(t)
	sawReq := make(chan struct{}, 4)
	fd.serve(t, func(conn net.Conn) {
		for { // reads but never replies
			req := readReq(t, conn)
			if req.Type == "" {
				return
			}
			sawReq <- struct{}{}
		}
	})

	c, err := Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}

	out := callAsync(c, server.Request{Type: "board_status", Board: "d75884"})
	<-sawReq // the request landed daemon-side before Close

	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	select {
	case r := <-out:
		if r.err == nil || !errors.Is(r.err, ErrDead) {
			t.Fatalf("in-flight call error = %v, want ErrDead after Close", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wake the in-flight call")
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestContextCancelKeepsClientUsable: a cancelled call returns
// ctx.Err() promptly and leaves the connection healthy for the next
// call (its id is simply never answered; no cross-talk).
func TestContextCancelKeepsClientUsable(t *testing.T) {
	fd := newFakeDaemon(t)
	fd.serve(t, func(conn net.Conn) {
		readReq(t, conn) // the cancelled one: hold it, never reply
		for {
			req := readReq(t, conn)
			if req.Type == "" {
				return
			}
			writeReply(t, conn, req.ID, map[string]any{"echo": req.Type})
		}
	})

	c, err := Dial(fd.path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _, err = c.Call(ctx, server.Request{Type: "board_status", Board: "d75884"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled call error = %v, want context.DeadlineExceeded", err)
	}

	res, rerr, err := c.Call(context.Background(), server.Request{Type: "list_boards"})
	if err != nil || rerr != nil {
		t.Fatalf("post-cancel call: err=%v rerr=%v", err, rerr)
	}
	if res["echo"] != "list_boards" {
		t.Errorf("post-cancel result = %v, want echo list_boards", res)
	}
}
