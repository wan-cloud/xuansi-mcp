// Package apiclient is the persistent client half of the daemon's
// internal API (the contract: internal/server/proto.go). One long-lived
// unix-socket connection, one background read goroutine: replies are
// paired to concurrent Calls by id, event pushes fan out to OnEvent
// handlers. Unlike cmd/xuansi's one-shot rpcClient (a fresh connection
// per CLI invocation), this client sits inside a long-lived process —
// the MCP face (`xuansi mcp`) — so holder binding, event subscriptions
// and pipelined calls all survive across requests on one connection.
//
// # Death semantics
//
// The connection dying (socket closed, read error, EOF, or Close)
// seals the client at once: every in-flight Call returns immediately
// with an error wrapping ErrDead (never at its context deadline), all
// event handlers stop, and later Calls fail fast with the same error.
// errors.Is(err, ErrDead) is the "retrying in place is pointless —
// dial a fresh client" signal. A request write that fails while the
// connection is still healthy (an over-size payload refused before any
// bytes moved) is returned as a plain error and does NOT kill the
// client; a transport-level write failure (EPIPE after daemon death)
// is reclassified by the read loop within moments.
//
// # Event delivery
//
// Each OnEvent handler runs on its own goroutine fed by a
// handlerQueueDepth-deep queue, so the read loop NEVER blocks on a
// slow handler (mirroring the daemon's own delivery policy, fire-
// and-forget events). A handler that falls handlerQueueDepth pushes
// behind drops newly arriving events until it catches up — it never
// stalls the connection and never slows other handlers.
//
// Unknown message types (the capture_done push documented in the
// contract but landing with T12) are read and ignored for now.
package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"xuansi/internal/server"
)

// ErrDead is wrapped into every error returned once the connection
// became unusable (socket death, read error, EOF, Close).
var ErrDead = errors.New("apiclient: connection dead")

// handlerQueueDepth bounds one handler's event queue — the same depth
// as the daemon's per-connection outbound queue (proto.go's delivery
// policy), so both ends drop at the same order of consumer slowness.
const handlerQueueDepth = 256

// Client is one persistent framed-JSON connection to the daemon. Safe
// for concurrent use by multiple goroutines.
type Client struct {
	conn net.Conn
	path string

	nextID atomic.Int64

	wmu sync.Mutex // serializes request writes on the conn

	mu      sync.Mutex
	pending map[int64]chan *server.Reply // in-flight calls by id
	regs    map[int64]*eventReg          // live event subscriptions
	nextReg int64
	dead    error // non-nil once sealed; wraps ErrDead

	closeOnce sync.Once
	closeErr  error
	done      chan struct{} // closed when the read loop exits
}

// eventReg is one OnEvent subscription: its own delivery goroutine
// draining queue, stopped by closing quit.
type eventReg struct {
	queue chan server.EventPush
	quit  chan struct{}
}

// Dial connects to the daemon's unix socket and starts the background
// read loop. An unreachable daemon gets the same actionable message
// the CLI prints (mirror of cmd/xuansi/client.go).
func Dial(path string) (*Client, error) {
	conn, err := net.Dial("unix", path)
	if err != nil {
		return nil, fmt.Errorf("cannot reach the daemon at %s (is `xuansi serve` running?): %w", path, err)
	}
	c := &Client{
		conn:    conn,
		path:    path,
		pending: make(map[int64]chan *server.Reply),
		regs:    make(map[int64]*eventReg),
		done:    make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// readLoop is the single reader: frames off the conn, replies to their
// waiter (unknown ids are dropped — a late reply after cancellation),
// events to every subscriber, anything else ignored. The first read
// failure or EOF seals the client.
func (c *Client) readLoop() {
	defer close(c.done)
	defer c.closeConn()
	for {
		payload, err := server.ReadFrame(c.conn)
		if err != nil {
			c.markDead(fmt.Errorf("%w: read failed: %v", ErrDead, err))
			return
		}
		var rep server.Reply
		if err := json.Unmarshal(payload, &rep); err != nil {
			continue // garbage payload: framing is self-syncing, drop it
		}
		switch rep.Type {
		case "reply":
			c.routeReply(&rep)
		case "event":
			var ev server.EventPush
			if err := json.Unmarshal(payload, &ev); err != nil {
				continue
			}
			c.fanOut(ev)
		default:
			// capture_done lands with T12; unknown types are ignored
		}
	}
}

// routeReply hands one reply to its waiter. Exactly one reply per id
// on a buffered(1) channel, so this never blocks; a reply nobody
// waits for (cancelled call) is dropped.
func (c *Client) routeReply(rep *server.Reply) {
	c.mu.Lock()
	ch := c.pending[rep.ID]
	delete(c.pending, rep.ID)
	c.mu.Unlock()
	if ch != nil {
		ch <- rep
	}
}

// fanOut enqueues one event on every live handler's queue —
// non-blocking by construction (see the delivery policy above).
func (c *Client) fanOut(ev server.EventPush) {
	c.mu.Lock()
	regs := make([]*eventReg, 0, len(c.regs))
	for _, r := range c.regs {
		regs = append(regs, r)
	}
	c.mu.Unlock()
	for _, r := range regs {
		select {
		case r.queue <- ev:
		default: // this handler is handlerQueueDepth behind: drop
		}
	}
}

// Call sends one request and waits for the reply carrying its id. The
// daemon's structured error reply crosses as a non-nil
// *server.RPCError with nil error; transport-level trouble (death,
// cancellation) crosses as a plain error. Safe for concurrent use.
func (c *Client) Call(ctx context.Context, req server.Request) (map[string]any, *server.RPCError, error) {
	c.mu.Lock()
	if c.dead != nil { // fail fast: no write, no wait
		err := c.dead
		c.mu.Unlock()
		return nil, nil, err
	}
	id := c.nextID.Add(1)
	req.ID = id
	ch := make(chan *server.Reply, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	c.wmu.Lock()
	werr := server.WriteMsg(c.conn, &req)
	c.wmu.Unlock()
	if werr != nil {
		c.mu.Lock()
		delete(c.pending, id)
		dead := c.dead
		c.mu.Unlock()
		if dead != nil {
			// the connection died under the write; report that, not the EPIPE
			return nil, nil, dead
		}
		return nil, nil, fmt.Errorf("apiclient: write request: %w", werr)
	}

	select {
	case rep := <-ch:
		if rep == nil { // nil is the death wake-up, never a reply
			return nil, nil, c.deadReason()
		}
		if rep.Error != nil {
			return nil, rep.Error, nil
		}
		res, _ := rep.Result.(map[string]any)
		return res, nil, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id) // its late reply, if any, is dropped
		c.mu.Unlock()
		return nil, nil, ctx.Err()
	}
}

// OnEvent subscribes fn to every event push. Each handler runs on its
// own goroutine with a handlerQueueDepth-deep queue: the read loop
// never blocks on fn, however slow; a handler more than
// handlerQueueDepth events behind silently drops new ones (see the
// delivery policy in the package comment). The returned cancel stops
// delivery and is safe to call more than once.
func (c *Client) OnEvent(fn func(server.EventPush)) (cancel func()) {
	r := &eventReg{
		queue: make(chan server.EventPush, handlerQueueDepth),
		quit:  make(chan struct{}),
	}
	c.mu.Lock()
	if c.dead != nil {
		c.mu.Unlock()
		return func() {} // dead: no event will ever arrive
	}
	id := c.nextReg
	c.nextReg++
	c.regs[id] = r
	c.mu.Unlock()

	go func() {
		for {
			select {
			case ev := <-r.queue:
				fn(ev)
			case <-r.quit:
				return
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			delete(c.regs, id)
			c.mu.Unlock()
			close(r.quit)
		})
	}
}

// Close tears the connection down: in-flight Calls return immediately
// with errors wrapping ErrDead, handlers stop, later Calls fail fast.
// Idempotent; the first close error is the one reported.
func (c *Client) Close() error {
	c.markDead(fmt.Errorf("%w: closed by the caller", ErrDead))
	c.closeConn()
	<-c.done // the read loop has surely exited
	return c.closeErr
}

// markDead seals the client (first reason wins): records why, wakes
// every in-flight Call with the nil death sentinel, stops every event
// handler.
func (c *Client) markDead(reason error) {
	c.mu.Lock()
	if c.dead != nil {
		c.mu.Unlock()
		return
	}
	c.dead = reason
	pending := c.pending
	c.pending = make(map[int64]chan *server.Reply)
	regs := c.regs
	c.regs = make(map[int64]*eventReg)
	c.mu.Unlock()

	for _, ch := range pending {
		ch <- nil // buffered(1) and id-exclusive: never blocks, never a real reply
	}
	for _, r := range regs {
		close(r.quit)
	}
}

// deadReason returns the recorded death error, if any.
func (c *Client) deadReason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead
}

// closeConn closes the conn exactly once (read-loop exit and Close
// both funnel here; whichever runs first unblocks the other).
func (c *Client) closeConn() {
	c.closeOnce.Do(func() {
		c.closeErr = c.conn.Close()
	})
}
