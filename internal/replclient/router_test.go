package replclient

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// newRouterPipe wires a router to one end of an in-memory pipe; the test
// holds the board end and writes frames by hand.
func newRouterPipe(t *testing.T, onDead func(string)) (*router, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	rt := newRouter(pipeLink{a}, onDead)
	rt.start()
	t.Cleanup(func() {
		rt.close()
		b.Close()
	})
	return rt, b
}

// boardSend writes one framed message from the board end.
func boardSend(t *testing.T, conn net.Conn, msg map[string]any) {
	t.Helper()
	payload, err := encodeMessage(msg)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	f, err := frame(payload)
	if err != nil {
		t.Fatalf("frame: %v", err)
	}
	if _, err := conn.Write(f); err != nil {
		t.Fatalf("board write: %v", err)
	}
}

// recvReply receives one routed reply with a timeout.
func recvReply(t *testing.T, ch <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a routed reply")
		return nil
	}
}

// TestRouterPairsRepliesByID: replies are routed by id and matched
// correctly when they arrive out of order (§6: a ping queued behind a
// long eval replies late).
func TestRouterPairsRepliesByID(t *testing.T) {
	rt, conn := newRouterPipe(t, func(string) {})
	ch1 := rt.register(1)
	ch2 := rt.register(2)
	ch3 := rt.register(3)

	for _, id := range []int64{2, 3, 1} {
		boardSend(t, conn, map[string]any{
			"op":     "reply",
			"id":     id,
			"result": map[string]any{"n": id},
		})
	}
	for _, want := range []int64{2, 3, 1} {
		var ch <-chan map[string]any
		switch want {
		case 1:
			ch = ch1
		case 2:
			ch = ch2
		case 3:
			ch = ch3
		}
		m := recvReply(t, ch)
		if m["id"] != want {
			t.Fatalf("reply id = %#v, want %d", m["id"], want)
		}
		if res, _ := m["result"].(map[string]any); res["n"] != want {
			t.Fatalf("reply result = %#v, want n=%d", m["result"], want)
		}
	}
}

// TestRouterStringIDDoesNotCollide: id 1 (int) and id "1" (string) are
// distinct — a string-id reply must not satisfy an int-id waiter (the
// int|str echo rule, §3).
func TestRouterStringIDDoesNotCollide(t *testing.T) {
	rt, conn := newRouterPipe(t, func(string) {})
	ch := rt.register(1)

	boardSend(t, conn, map[string]any{
		"op":     "reply",
		"id":     "1",
		"result": map[string]any{"who": "string"},
	})
	select {
	case m := <-ch:
		t.Fatalf("string-id reply delivered to int-id waiter: %#v", m)
	case <-time.After(100 * time.Millisecond):
	}

	boardSend(t, conn, map[string]any{
		"op":     "reply",
		"id":     int64(1),
		"result": map[string]any{"who": "int"},
	})
	m := recvReply(t, ch)
	if res, _ := m["result"].(map[string]any); res["who"] != "int" {
		t.Fatalf("reply result = %#v", m["result"])
	}
}

// TestRouterDropsUnknownID: replies for unregistered ids are dropped
// without stalling the loop — later frames still flow.
func TestRouterDropsUnknownID(t *testing.T) {
	rt, conn := newRouterPipe(t, func(string) {})
	ch := rt.register(7)

	boardSend(t, conn, map[string]any{"op": "reply", "id": int64(99)})
	boardSend(t, conn, map[string]any{"op": "reply", "id": int64(7), "result": map[string]any{"ok": true}})
	m := recvReply(t, ch)
	if res, _ := m["result"].(map[string]any); res["ok"] != true {
		t.Fatalf("reply after unknown-id drop = %#v", m)
	}
}

// TestRouterEventPrefixDispatch: events fan out to prefix subscribers
// only, in arrival order; cancel stops delivery for that subscriber.
func TestRouterEventPrefixDispatch(t *testing.T) {
	rt, conn := newRouterPipe(t, func(string) {})
	gpio := make(chan Event, 4)
	cancel := rt.onEvent("gpio.", func(e Event) { gpio <- e })
	all := make(chan Event, 4)
	cancelAll := rt.onEvent("", func(e Event) { all <- e })
	defer cancelAll()

	boardSend(t, conn, map[string]any{
		"op":    "event",
		"id":    int64(0),
		"event": map[string]any{"source": "gpio.5", "data": map[string]any{"edge": "rising"}},
	})
	boardSend(t, conn, map[string]any{
		"op":    "event",
		"id":    int64(0),
		"event": map[string]any{"source": "uart1.rx", "data": map[string]any{"data": []byte{1, 2}}},
	})
	boardSend(t, conn, map[string]any{
		"op":    "event",
		"id":    int64(0),
		"event": map[string]any{"source": "gpio.6", "data": map[string]any{"edge": "falling"}},
	})

	e := recvEvent(t, gpio)
	if e.Source != "gpio.5" {
		t.Fatalf("first gpio event = %q, want gpio.5", e.Source)
	}
	e = recvEvent(t, gpio)
	if e.Source != "gpio.6" {
		t.Fatalf("second gpio event = %q, want gpio.6", e.Source)
	}
	if got := recvEvent(t, all).Source; got != "gpio.5" {
		t.Fatalf("catch-all first event = %q, want gpio.5", got)
	}
	if got := recvEvent(t, all).Source; got != "uart1.rx" {
		t.Fatalf("catch-all second event = %q, want uart1.rx", got)
	}
	if got := recvEvent(t, all).Source; got != "gpio.6" {
		t.Fatalf("catch-all third event = %q, want gpio.6", got)
	}

	cancel()
	boardSend(t, conn, map[string]any{
		"op":    "event",
		"id":    int64(0),
		"event": map[string]any{"source": "gpio.7", "data": map[string]any{"edge": "rising"}},
	})
	if got := recvEvent(t, all).Source; got != "gpio.7" {
		t.Fatalf("catch-all after cancel = %q, want gpio.7", got)
	}
	select {
	case e := <-gpio:
		t.Fatalf("event delivered after cancel: %#v", e)
	case <-time.After(150 * time.Millisecond):
	}
}

// TestRouterFatalNilID: the board's fatal notice — exactly one protocol
// error frame with id=nil (§7) — marks the connection dead and ends the
// read loop; pending waiters are never satisfied by it.
func TestRouterFatalNilID(t *testing.T) {
	var mu sync.Mutex
	dead := make(chan string, 1)
	rt, conn := newRouterPipe(t, func(reason string) {
		mu.Lock()
		defer mu.Unlock()
		select {
		case dead <- reason:
		default:
		}
	})
	ch := rt.register(1)

	boardSend(t, conn, map[string]any{
		"op":    "reply",
		"id":    nil,
		"error": map[string]any{"class": "protocol", "msg": "bad type 0x02"},
	})
	select {
	case reason := <-dead:
		if !strings.Contains(reason, "protocol") {
			t.Fatalf("dead reason = %q, want it to mention protocol", reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fatal nil-id frame did not mark the connection dead")
	}
	select {
	case m := <-ch:
		t.Fatalf("fatal frame delivered as a reply: %#v", m)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-rt.done:
	case <-time.After(2 * time.Second):
		t.Fatal("router loop did not exit after the fatal frame")
	}
}

// TestRouterLinkEOF: the board end closing surfaces as a dead mark with
// a read-failure reason.
func TestRouterLinkEOF(t *testing.T) {
	dead := make(chan string, 1)
	a, b := net.Pipe()
	rt := newRouter(pipeLink{a}, func(r string) {
		select {
		case dead <- r:
		default:
		}
	})
	rt.start()
	b.Close()
	select {
	case reason := <-dead:
		if reason == "" {
			t.Fatal("dead reason empty on link EOF")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("EOF on the link did not mark the connection dead")
	}
	rt.close()
}

// TestRouterCloseStopsLoop: close unblocks the reader and the loop exits
// promptly — no goroutine leak.
func TestRouterCloseStopsLoop(t *testing.T) {
	rt, _ := newRouterPipe(t, func(string) {})
	if err := rt.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-rt.done:
	case <-time.After(2 * time.Second):
		t.Fatal("router loop leaked past close")
	}
}
