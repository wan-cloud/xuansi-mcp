package replclient

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Event is one pushed board event: Source is the §8 source name
// (gpio.N, uartN.rx, stdout, net — or job for submit results) and Data
// its payload map.
type Event struct {
	Source string
	Data   map[string]any
}

// OpTimeouts bounds the wait for a reply per op, derived from the §6
// silence table of the frozen protocol: immediate queries (hello, ping,
// caps, log.read, wifi.status) get 2s; file I/O (fs.*) gets 10s (a 16KB
// read measures ~99ms — generous margin); eval and the submit ack get
// 5s — the eval budget domain 0..4000ms (§6) plus 1s of margin; script.*
// ops get 5s. Ops not listed fall back to defaultOpTimeout. diag.wdttest
// is deliberately absent: it never replies by design (§6) — a caller
// that sends it sees the default timeout expire and reconnects.
var OpTimeouts = map[string]time.Duration{
	"hello":       2 * time.Second,
	"ping":        2 * time.Second,
	"caps":        2 * time.Second,
	"log.read":    2 * time.Second,
	"wifi.status": 2 * time.Second,

	"fs.*": 10 * time.Second,

	"eval":   5 * time.Second, // eval budget ≤ 4000ms (§6) + 1s
	"submit": 5 * time.Second, // ack only; the result comes as a job event

	"script.*": 5 * time.Second,

	// camera.capture (B10-T8 fix round): the in-op framesize switch now
	// carries the full off→settle(1s)→prewarm(2s)→init cycle + up to
	// frames=4 software-JPEG encodes (~0.75s each at VGA) — the measured
	// switch-burst worst case is ~5.3s (b9 attempt log: QVGA→VGA switch
	// + 2×VGA encode), over the old default 5s. 12s bounds it with the
	// 4-frame margin. First-shot latency unchanged (~3.2s, well under).
	"camera.capture": 12 * time.Second,
}

// defaultOpTimeout bounds any op not named in OpTimeouts (diag.* and
// future ops): 5s, the conn-task TWDT ceiling with margin (§6).
const defaultOpTimeout = 5 * time.Second

// opTimeout resolves an op's wait bound: exact entry, then the family
// entry ("<prefix>.*"), then the default.
func opTimeout(op string) time.Duration {
	if d, ok := OpTimeouts[op]; ok {
		return d
	}
	if i := strings.IndexByte(op, '.'); i > 0 {
		if d, ok := OpTimeouts[op[:i+1]+"*"]; ok {
			return d
		}
	}
	return defaultOpTimeout
}

// budgetWaitMargin is the daemon-side slack added on top of a per-call
// eval budget when deriving the reply wait (§9.6, F-2): room for the
// board's epilogue after the budget itself expires plus the result
// bridge and reply transit. ONE place — keep in lockstep with the fw's
// budget domain (1..30000ms, protocol v6), never a second ceiling.
const budgetWaitMargin = 2 * time.Second

// opTimeoutFor resolves an op's wait bound the way Op uses it: a
// per-call budget_ms key in the args (eval/submit only, §9.6 — 1..30000,
// this call's override) derives budget+budgetWaitMargin, retiring the
// fixed table entry for the keyed call; every other call — key absent,
// budget 0, any other op — gets the §6 table value unchanged.
func opTimeoutFor(op string, args map[string]any) time.Duration {
	if op == "eval" || op == "submit" {
		if ms := budgetMsOf(args); ms > 0 {
			return time.Duration(ms)*time.Millisecond + budgetWaitMargin
		}
	}
	return opTimeout(op)
}

// budgetMsOf extracts the args' budget_ms override as whole
// milliseconds (0 = absent/non-numeric). In-process callers set int
// (Eval/Submit) or int64 (the server forwarding Request.BudgetMs);
// float64 covers any JSON-decoded face.
func budgetMsOf(args map[string]any) int64 {
	switch v := args["budget_ms"].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}

// Client is one frame-protocol connection to a board over a Link.
// Transports come from DialUSB/DialTCP and carry a redial route, so a
// dead client has a Reconnect path back to a live one. When the
// connection dies — a fatal frame error (§7), the board's nil-id
// protocol frame, a link failure, or Close — Dead reports it and the
// caller rebuilds (Reconnect, or a fresh dial).
//
// Concurrency: Op/Eval/Submit/Hello may be called from multiple
// goroutines; frame writes are serialized and replies are paired by id,
// so out-of-order replies (§6: ops queue behind a long eval) still
// route to their own caller. Event handlers run on the read goroutine.
type Client struct {
	link Link
	rt   *router

	wmu sync.Mutex // serializes link writes: frames must never interleave

	deadOnce sync.Once
	deadCh   chan struct{} // closed exactly once; wakes every pending op

	mu         sync.Mutex
	dead       bool
	deadReason string
	nextID     int64
	caps       map[string]any // cached by Hello (§2: hello.caps == caps op dict)

	redial    redialRoute // set by DialUSB/DialTCP; Reconnect's route to a successor
	helloWant bool        // last Hello's requested role, echoed on Reconnect
	helloName string      // last Hello's client name, echoed on Reconnect
}

// NewClient wraps a Link and starts the background read router.
func NewClient(link Link) *Client {
	c := &Client{
		link:   link,
		deadCh: make(chan struct{}),
	}
	c.rt = newRouter(link, c.markDead)
	c.rt.start()
	return c
}

// Op sends one request and waits for the reply carrying the same id.
// It returns the reply's result map; a board error reply comes back as
// an *OpError with class and msg verbatim (including ` at byte N`,
// §3). The wait ends at whichever comes first: the caller's context,
// the §6-derived per-op timeout (OpTimeouts), or the connection dying.
func (c *Client) Op(ctx context.Context, op string, args map[string]any) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("replclient: %s: %w", op, err)
	}
	if err := c.deadErr(op); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.mu.Unlock()

	payload, err := encodeRequest(op, id, args)
	if err != nil {
		return nil, fmt.Errorf("replclient: encode %s: %w", op, err)
	}
	f, err := frame(payload)
	if err != nil {
		return nil, fmt.Errorf("replclient: %s: %w", op, err)
	}

	ch := c.rt.register(id)   // before the write: the reply can race it
	defer c.rt.unregister(id) // oracle's `finally: del replies[k]`: every exit releases the slot
	if _, err := c.write(f); err != nil {
		c.markDead("link write failed: " + err.Error())
		return nil, fmt.Errorf("%w: write %s: %v", ErrDead, op, err)
	}

	timer := time.NewTimer(opTimeoutFor(op, args))
	defer timer.Stop()
	select {
	case msg := <-ch:
		return replyResult(op, msg)
	case <-timer.C:
		select { // the reply may have landed as the timer fired
		case msg := <-ch:
			return replyResult(op, msg)
		default:
		}
		return nil, fmt.Errorf("replclient: timeout waiting reply for %s id=%d: %w", op, id, context.DeadlineExceeded)
	case <-ctx.Done():
		return nil, fmt.Errorf("replclient: %s: %w", op, ctx.Err())
	case <-c.deadCh:
		return nil, c.deadErr(op)
	}
}

// replyResult unpacks a reply: an error reply becomes an *OpError,
// otherwise the result map is returned (nil when absent).
func replyResult(op string, msg map[string]any) (map[string]any, error) {
	if e, ok := msg["error"].(map[string]any); ok {
		class, _ := e["class"].(string)
		emsg, _ := e["msg"].(string)
		return nil, &OpError{Op: op, Class: class, Msg: emsg}
	}
	res, _ := msg["result"].(map[string]any)
	return res, nil
}

// write sends one whole frame; concurrent Ops must never interleave
// bytes on the link.
func (c *Client) write(f []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	n, err := c.link.Write(f)
	if err != nil {
		return n, err
	}
	if n != len(f) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

// Eval runs a Lua chunk synchronously (op eval): out is the captured
// stdout text and value the first return value when the board could
// bridge one (v3: the value key is present only then, nil otherwise).
// args, when non-nil, is sent as the request's args key and becomes the
// Lua global ARGS during the evaluation (v3). budgetMs, when > 0, is
// sent as the per-call budget_ms override (§9.6, protocol v6 — domain
// 1..30000, this evaluation only; 0 = the session budget, wire key
// absent) and the reply wait derives from it: budget+margin instead of
// the §6 table's 5s (opTimeoutFor).
func (c *Client) Eval(ctx context.Context, code string, args any, budgetMs int) (out string, value any, err error) {
	a := map[string]any{"code": code}
	if args != nil {
		a["args"] = args
	}
	if budgetMs > 0 {
		a["budget_ms"] = budgetMs
	}
	res, err := c.Op(ctx, "eval", a)
	if err != nil {
		return "", nil, err
	}
	out, _ = res["out"].(string)
	value = res["value"]
	return out, value, nil
}

// Submit queues an asynchronous evaluation (op submit): it returns on
// the ack — as soon as the job is queued — and the result arrives later
// as a job event on this same connection; receive it with
// OnEvent("job", ...) (§8: job results are direct-delivered to the
// submitting connection and are not a subscribable source). budgetMs
// carries the same per-call budget_ms override as Eval (the board runs
// the job under it; the ack itself is immediate, so the derived wait
// only widens the ceiling).
func (c *Client) Submit(ctx context.Context, code, tag string, budgetMs int) error {
	a := map[string]any{"code": code, "tag": tag}
	if budgetMs > 0 {
		a["budget_ms"] = budgetMs
	}
	_, err := c.Op(ctx, "submit", a)
	return err
}

// OnEvent subscribes fn to every event whose source starts with
// sourcePrefix (e.g. "gpio.", "uart1.rx", "stdout", "net", "job"). The
// returned cancel unregisters the subscription.
func (c *Client) OnEvent(sourcePrefix string, fn func(Event)) (cancel func()) {
	return c.rt.onEvent(sourcePrefix, fn)
}

// OnStream sets the sink receiving every 0x02 data chunk (parsed
// sub-header plus raw payload) off this connection. The sink runs on
// the read goroutine — the OnEvent discipline: be quick, no blocking
// calls back into the client. A nil sink drops data frames. The
// setting lives on this connection's router only: re-register on any
// Reconnect successor.
func (c *Client) OnStream(sink StreamSink) {
	c.rt.onStream(sink)
}

// Dead reports whether the connection became unusable: a fatal frame
// error (§7), the board's nil-id protocol frame, a link failure, or a
// local Close. The first reason stands.
func (c *Client) Dead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead
}

// DeadReason returns the recorded cause of death, or "" while live.
func (c *Client) DeadReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadReason
}

// SendRaw writes raw bytes to the link outside the frame writer. It
// exists for protocol-fault HIL: the §7 desync probe injects a frame
// header with the reserved 0x02 type byte ([0 0 0 1 0x02]) and watches
// the board's fatal nil-id reply kill the client. The daemon never
// calls it.
func (c *Client) SendRaw(p []byte) error {
	if err := c.deadErr("sendraw"); err != nil {
		return err
	}
	if _, err := c.write(p); err != nil {
		c.markDead("link write failed: " + err.Error())
		return fmt.Errorf("%w: sendraw: %v", ErrDead, err)
	}
	return nil
}

// Close shuts down the read router and the link, waking every pending
// op. Safe to call more than once.
func (c *Client) Close() error {
	c.markDead("client closed")
	return c.rt.close()
}

// markDead records the first fatal condition and wakes every pending
// op; later calls are no-ops.
func (c *Client) markDead(reason string) {
	c.deadOnce.Do(func() {
		c.mu.Lock()
		c.dead = true
		c.deadReason = reason
		c.mu.Unlock()
		close(c.deadCh)
	})
}

// deadErr returns the dead-connection error for what (an op name or
// phase), or nil while the connection is live.
func (c *Client) deadErr(what string) error {
	if !c.Dead() {
		return nil
	}
	return fmt.Errorf("%w (%s): %s", ErrDead, c.DeadReason(), what)
}
