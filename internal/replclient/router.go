package replclient

import (
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

// router is the background read loop: it reassembles frames off the
// link, decodes them, and dispatches replies to the pending op waiters
// (paired by id — int and string ids never collide), events to prefix
// subscribers, and fatal conditions to the client's dead mark. It never
// blocks on delivery: reply channels are buffered and unknown ids are
// dropped, so a slow or vanished waiter cannot stall the loop.

// replyBuf is one id's reply buffer: the upload lane's ack burst (see
// register).
const replyBuf = uplinkWindowTCP + 2

type router struct {
	link   Link
	reader *frameReader
	onDead func(reason string) // the client's markDead; only its first call lands

	mu       sync.Mutex
	replies  map[any]chan map[string]any
	handlers map[int]eventHandler
	nextHID  int
	sink     StreamSink // fed every parsed 0x02 chunk; nil drops them

	closeOnce sync.Once
	closeErr  error
	done      chan struct{} // closed when the loop exits
}

type eventHandler struct {
	prefix string
	fn     func(Event)
}

func newRouter(link Link, onDead func(string)) *router {
	return &router{
		link:     link,
		reader:   newFrameReader(link),
		onDead:   onDead,
		replies:  make(map[any]chan map[string]any),
		handlers: make(map[int]eventHandler),
		done:     make(chan struct{}),
	}
}

// start launches the read loop.
func (r *router) start() { go r.run() }

// run reads until the stream ends: link close or failure, a fatal frame
// error (§7), or the board's nil-id protocol frame (its CS_DEAD notice
// — the board goes silent right after, so there is nothing left to
// read). Data frames never end the loop: they detour to the stream
// sink while ctrl frames keep the reply/event path.
func (r *router) run() {
	defer close(r.done)
	for {
		f, err := r.reader.next()
		if err != nil {
			switch {
			case errors.Is(err, errFatal):
				r.onDead(err.Error())
			case errors.Is(err, io.EOF):
				r.onDead("link closed by board")
			default:
				r.onDead("link read failed: " + err.Error())
			}
			return
		}
		if f.Type == frameTypeData {
			r.dispatchStream(f)
			continue
		}
		msg, err := decodeMessage(f.Payload)
		if err != nil {
			continue // malformed payload: drop the frame, sync is intact
		}
		op, _ := msg["op"].(string)
		switch op {
		case "reply":
			if r.dispatchReply(msg) {
				return
			}
		case "event":
			r.dispatchEvent(msg)
		}
	}
}

// dispatchStream hands one data frame to the stream sink as a parsed
// chunk. An unparseable sub-header or an unset sink drops the frame —
// sync is intact either way, so neither is fatal.
func (r *router) dispatchStream(f Frame) {
	ch, seq, idx, total, payload, ok := parseStreamChunk(f.Payload)
	if !ok {
		return
	}
	r.mu.Lock()
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink(ch, seq, idx, total, payload)
	}
}

// dispatchReply routes one reply to its waiter. A reply without an id
// is the §7 fatal notice — id=nil appears only on the board's one-shot
// protocol error frame — and ends the stream. Returns true when the
// loop must stop.
func (r *router) dispatchReply(msg map[string]any) bool {
	id, ok := msg["id"]
	if !ok || id == nil {
		reason := "board sent a fatal protocol error (id nil)"
		if e, ok := msg["error"].(map[string]any); ok {
			if m, _ := e["msg"].(string); m != "" {
				reason = "fatal protocol error: " + m
			}
		}
		r.onDead(reason)
		return true
	}
	r.mu.Lock()
	ch := r.replies[id]
	r.mu.Unlock()
	if ch != nil {
		select {
		case ch <- msg: // replyBuf: several replies may queue for one id
			// (the v8 upload lane's per-frame acks bunch behind a
			// windowed pump — T7-④) without the router ever blocking
			// on an abandoned waiter.
		default: // the overflow valve: a board spamming one id past the
			// buffer drops (the waiter's next read rightly fails fast)
		}
	}
	return false
}

// dispatchEvent fans one event out to every subscriber whose prefix
// matches the source (§8 sources: gpio.N, uartN.rx, stdout, net — plus
// job, the submit result direct-delivery "source"). Handlers run on the
// router goroutine: they must be quick and must not call back into the
// client's blocking API.
func (r *router) dispatchEvent(msg map[string]any) {
	ev, _ := msg["event"].(map[string]any)
	src, _ := ev["source"].(string)
	var data map[string]any
	if d, ok := ev["data"].(map[string]any); ok {
		data = d
	}
	e := Event{Source: src, Data: data}

	r.mu.Lock()
	handlers := make([]eventHandler, 0, len(r.handlers))
	for _, h := range r.handlers {
		handlers = append(handlers, h)
	}
	r.mu.Unlock()
	for _, h := range handlers {
		if strings.HasPrefix(src, h.prefix) {
			h.fn(e)
		}
	}
}

// register reserves the reply slot for id before the request is written
// (the reply can race the write's return). The buffer holds the upload
// lane's ack burst: a windowed pump (uplinkWindowTCP frames in flight)
// can leave up to that many un-read acks plus the commit queued behind
// one id while it processes the first — buffered(1) would DROP them
// (the router never blocks on a waiter), and a dropped ack fails the
// upload. Strict one-reply ops never notice the capacity.
func (r *router) register(id int64) <-chan map[string]any {
	ch := make(chan map[string]any, replyBuf)
	r.mu.Lock()
	r.replies[id] = ch
	r.mu.Unlock()
	return ch
}

// unregister releases the reply slot (timeout, cancellation, write
// failure); a late reply for the id is then dropped.
func (r *router) unregister(id int64) {
	r.mu.Lock()
	delete(r.replies, id)
	r.mu.Unlock()
}

// onEvent adds a prefix subscription and returns its cancel.
func (r *router) onEvent(prefix string, fn func(Event)) (cancel func()) {
	r.mu.Lock()
	id := r.nextHID
	r.nextHID++
	r.handlers[id] = eventHandler{prefix: prefix, fn: fn}
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.handlers, id)
		r.mu.Unlock()
	}
}

// onStream sets the sink for 0x02 data frames (nil drops them). The
// swap is atomic: the new sink sees the next frame onward.
func (r *router) onStream(sink StreamSink) {
	r.mu.Lock()
	r.sink = sink
	r.mu.Unlock()
}

// close shuts the link down — unblocking the read loop — and waits for
// the loop to exit. Idempotent.
func (r *router) close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.link.Close()
	})
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		// The transport did not unblock its reader; the loop is left to
		// exit on its own. Every real Link (pipe, serial, TCP) unblocks
		// on Close, so this indicates a transport bug, not client state.
	}
	return r.closeErr
}
