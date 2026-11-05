package server

// server.go — the daemon side of the internal API: the unix listener,
// the per-connection loops (one reader dispatching requests, one writer
// serializing replies + event pushes), the lease gating with holder
// semantics (bind / adopt / agent-gone), the per-board event taps that
// fan board events out to watchers, and the daemon lifecycle (initial
// discovery sweep + startup orphan audit, refresh loop, graceful
// Close). The wire contract lives in proto.go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/lease"
	"xuansi/internal/paths"
	"xuansi/internal/replclient"
)

// Defaults (overridable through Opts; see also proto.go's contract).
const (
	defaultRefreshInterval = 30 * time.Second // discovery re-sweep cadence
	defaultAuditTimeout    = 20 * time.Second // per-board startup orphan audit bound
	defaultTeardownTimeout = 30 * time.Second // board-side lease teardown bound
	defaultRequestTimeout  = 60 * time.Second // one request's total budget
	// fsFileMax mirrors the fw single-file cap (XS_FILE_MAX = 16384B,
	// caps limits.file_max); fs_write is refused daemon-side above it.
	fsFileMax = 16384
	// outboundQ is the per-connection event/reply buffer. A full queue
	// drops an incoming EVENT (fire-and-forget, fw §8 style); a REPLY
	// evicts the oldest queued event — or, in the all-replies wedge of
	// a pipelining client that stopped reading, waits boundedly and
	// then evicts the oldest queued reply, logged and counted (the
	// delivery policy, proto.go). Nothing ever stalls under wmu.
	outboundQ = 256
	// defaultReplyWait bounds how long a reply waits for a wedged
	// all-replies queue to drain before the logged oldest-reply
	// eviction; replyWaitPoll is the wmu-RELEASED poll cadence during
	// that wait (the writer drains without wmu, so the wait is safe).
	defaultReplyWait = 1 * time.Second
	replyWaitPoll    = 2 * time.Millisecond
)

// errUnreachable wraps dial/hello failures for the error taxonomy.
var errUnreachable = errors.New("board unreachable")

// afterFunc is the idle-exit timer seam (production: time.AfterFunc;
// tests swap it to capture and fire the arming deterministically).
var afterFunc = time.AfterFunc

// ErrAlreadyListening is bindSock's spawn-race sentinel: the dial probe
// found a live daemon owning the socket. The loser of a launcher spawn
// race (or a deliberate double start) is a harmless no-op — serve.go
// keys its exit-0 "leaving" path on this; the wrapped error still
// names the socket path.
var ErrAlreadyListening = errors.New("server: another daemon is already listening")

// USBRebootWarning is the honest label the deliberate USB-transport
// lease route must state everywhere it succeeds — in the API reply's
// result (key "warning"), in the daemon log, and on the CLI's stdout
// (T10 ruling: USB is the WiFi-dead bench's LAST resort; opening the
// CDC port reboots the board, §2, killing every running script).
const USBRebootWarning = "USB transport: opening the port REBOOTS the board — running scripts are killed (last resort per T10 ruling)"

// DefaultSockPath is the daemon socket: $XDG_RUNTIME_DIR/xuansi/daemon.sock,
// falling back to /tmp/xuansi-daemon.sock when XDG_RUNTIME_DIR is unset
// (e.g. a systemd unit without the env, or a manual run). Resolution
// lives in internal/paths (per-OS; this delegation is byte-identical
// on Linux).
func DefaultSockPath() string {
	return paths.SockPath()
}

// defaultBoardsFile is the zero-config boards.conf location.
func defaultBoardsFile() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "xuansi", "boards.conf")
}

// Opts parameterize the Server; zero values get the defaults above.
type Opts struct {
	SockPath string // default DefaultSockPath()
	GraceTTL time.Duration
	// BoardsFile feeds the alias map when Registry is nil (default:
	// user config dir xuansi/boards.conf; missing file = zero-config).
	BoardsFile string
	// Registry injects a pre-built registry (tests); nil = built from
	// BoardsFile.
	Registry *discover.Registry
	// RefreshInterval: 0 → 30s; < 0 disables the background loop (the
	// initial sweep still runs).
	RefreshInterval time.Duration
	AuditTimeout    time.Duration
	TeardownTimeout time.Duration
	RequestTimeout  time.Duration
	Log             func(format string, args ...any)
	// NewSupervisor is the per-board supervisor factory (default:
	// board.NewSupervisor with the Log seam; tests inject dial fakes).
	NewSupervisor func(ref board.BoardRef) *board.Supervisor
	// CaptureRoot pins where capture files land (default:
	// $XDG_DATA_HOME/xuansi/captures — capturesRoot; tests inject a
	// temp root).
	CaptureRoot string
	// IdleExit, when positive, is the self-exit window: the LAST API
	// connection's departure arms a timer for the FULL duration, any new
	// connection disarms it, and a firing with no connections AND no
	// live captures closes IdleExited() — the serve main loop then walks
	// its existing graceful shutdown path (the server never Closes
	// itself). A live capture at fire time stands the timer down; the
	// capture's own ending re-arms it. 0 = disabled (the default).
	IdleExit time.Duration
}

// Server is the daemon's API face. New binds the socket; Start serves;
// Close shuts down gracefully (socket removed, board connections
// closed, every goroutine accounted by Done).
type Server struct {
	opts Opts
	logf func(format string, args ...any)

	reg    *discover.Registry
	leases *lease.Manager
	ln     net.Listener

	sockPath string
	ownSock  bool // we created the sock file → Close removes it

	mu         sync.Mutex
	sups       map[string]*daemonBoard             // mac6 → supervisor wrapper
	conns      map[*clientConn]struct{}            // live API connections
	watchers   map[string]map[*clientConn]struct{} // mac6 → subscribed conns
	connSeq    int64
	captures   map[string]*captureSession // capture_id → live capture (capture.go)
	captureSeq int64                      // capture_id assignment, under mu
	// captureRoot / captureOpen are the capture face's filesystem
	// seams: the root defaults to capturesRoot() (Opts.CaptureRoot
	// injects a temp root in tests) and the opener defaults to
	// osCreateCapture (O_CREAT|O_EXCL — never truncates an existing
	// file; tests swap a failing writer). Both read under mu.
	captureRoot string
	captureOpen func(path string) (io.WriteCloser, error)

	// idle-exit state (all under mu; Opts.IdleExit > 0 enables): the
	// conn-register hook disarms, the last conn-delete / capture-remove
	// arms, and the fire closure re-verifies emptiness under the lock
	// before the one-line log + close-once idleCh.
	idleExit   time.Duration
	idleTimer  *time.Timer
	idleCh     chan struct{}
	idleFired  bool // idleCh closed — never triggers again
	idleHalted bool // Close started — the idle exit stands down

	auditCancel context.CancelFunc

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
	done      chan struct{}
}

// daemonBoard couples a board Supervisor with the server-side connect
// serialization: Supervisor.Connect dials outside its own lock, so two
// concurrent cold-touch calls would double-dial the board into two
// controller connections — and §12 arbitration then forces the loser
// to observer forever (no preemption), a self-inflicted slot_held.
// EVERY server path that can make the supervisor connect (request ops
// via warmController, the startup audit's Resync, board_status's
// ScriptList, the lease teardown) passes through connectMu.
type daemonBoard struct {
	sup *board.Supervisor

	connectMu sync.Mutex
	// tapped is the controller the event tap is installed on (interface
	// identity — production dynamic types are pointers, comparable). A
	// redial swaps the controller; the next server-side touch re-taps.
	tapped board.Controller
}

// clientConn is one API connection: reader + writer goroutines over the
// framed JSON stream, plus the bound lease holder.
type clientConn struct {
	srv      *Server
	conn     net.Conn
	outbound chan any

	mu          sync.Mutex
	holder      string
	holderBound bool // holder came from a request (not generated)

	wmu   sync.Mutex
	wdead bool // outbound closed; further sends are no-ops
	dropO sync.Once

	// replyWait bounds the send()-side wait in the all-replies wedge
	// before the logged oldest-reply eviction (handleConn sets the
	// default; tests inject shorter). repliesLost counts replies lost
	// to that last resort (wmu-guarded; logged — never silent).
	replyWait   time.Duration
	repliesLost int64
}

// New builds the Server and binds the unix socket. A sock file left by
// a dead daemon is probed (a successful dial means a live daemon owns
// it → refuse) and removed otherwise.
func New(o Opts) (*Server, error) {
	if o.Log == nil {
		o.Log = func(string, ...any) {}
	}
	if o.GraceTTL <= 0 {
		o.GraceTTL = lease.DefaultGraceTTL
	}
	if o.RefreshInterval == 0 {
		o.RefreshInterval = defaultRefreshInterval
	}
	if o.AuditTimeout <= 0 {
		o.AuditTimeout = defaultAuditTimeout
	}
	if o.TeardownTimeout <= 0 {
		o.TeardownTimeout = defaultTeardownTimeout
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = defaultRequestTimeout
	}
	if o.NewSupervisor == nil {
		o.NewSupervisor = func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{Log: o.Log})
		}
	}
	if o.CaptureRoot == "" {
		o.CaptureRoot = capturesRoot()
	}
	if o.IdleExit < 0 {
		o.IdleExit = 0 // negative is not a window; only 0 disables and positives arm
	}

	reg := o.Registry
	if reg == nil {
		path := o.BoardsFile
		if path == "" {
			path = defaultBoardsFile()
		}
		aliases, err := discover.LoadBoardsFile(path)
		if err != nil {
			return nil, fmt.Errorf("server: boards file: %w", err)
		}
		reg = discover.NewRegistry(aliases)
	}

	if o.SockPath == "" {
		o.SockPath = DefaultSockPath()
	}
	ln, err := bindSock(o.SockPath)
	if err != nil {
		return nil, err
	}

	s := &Server{
		opts:        o,
		logf:        o.Log,
		reg:         reg,
		ln:          ln,
		sockPath:    o.SockPath,
		ownSock:     true,
		sups:        map[string]*daemonBoard{},
		conns:       map[*clientConn]struct{}{},
		watchers:    map[string]map[*clientConn]struct{}{},
		captures:    map[string]*captureSession{},
		captureRoot: o.CaptureRoot,
		captureOpen: osCreateCapture,
		idleExit:    o.IdleExit,
		idleCh:      make(chan struct{}),
		closed:      make(chan struct{}),
		done:        make(chan struct{}),
	}
	s.leases = lease.NewManager(lease.Policy{
		GraceTTL: o.GraceTTL,
		Log:      o.Log,
		OnExpiry: s.leaseTeardown,
	})
	return s, nil
}

// bindSock probes for a live daemon, clears a stale sock file, and
// listens.
func bindSock(path string) (net.Listener, error) {
	if fi, err := os.Stat(path); err == nil {
		if fi.Mode()&os.ModeSocket != 0 {
			if c, derr := net.Dial("unix", path); derr == nil {
				c.Close()
				return nil, fmt.Errorf("%w: %s", ErrAlreadyListening, path)
			}
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("server: remove stale sock %s: %w", path, err)
			}
		} else {
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("server: clear non-socket path %s: %w", path, err)
			}
		}
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("server: mkdir %s: %w", dir, err)
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("server: listen %s: %w", path, err)
	}
	return ln, nil
}

// Start launches the serving loops: accept, the initial discovery sweep
// + startup orphan audit (report only — nothing killed, the T10
// ruling), and the refresh loop when enabled.
func (s *Server) Start() error {
	var actx context.Context
	actx, s.auditCancel = context.WithCancel(context.Background())

	s.wg.Add(2)
	go s.acceptLoop()
	go s.startupAudit(actx)

	if s.opts.RefreshInterval > 0 {
		s.wg.Add(1)
		go s.refreshLoop(actx)
	}
	s.logf("server: serving %s (grace_ttl=%s)", s.sockPath, s.opts.GraceTTL)
	return nil
}

// Addr is the bound listener address (the unix sock path).
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// SockPath is the daemon socket path.
func (s *Server) SockPath() string { return s.sockPath }

// Leases exposes the lease manager (observation; the API routes all
// mutations through it).
func (s *Server) Leases() *lease.Manager { return s.leases }

// Done closes once every server goroutine has exited after Close.
func (s *Server) Done() <-chan struct{} { return s.done }

// IdleExited closes (exactly once) when the idle-exit window fired with
// no API connections and no live captures — Opts.IdleExit > 0 enables
// the face. The server NEVER Closes itself on idle: serve's main loop
// selects on this and walks its existing graceful shutdown path.
func (s *Server) IdleExited() <-chan struct{} { return s.idleCh }

// idleArmLocked arms the idle-exit timer for the FULL window (caller
// holds mu and has verified no live conns). Live captures do not block
// the arming: the fire closure re-checks them under the lock — a
// capture alive at fire time stands the timer down, and the capture's
// own ending re-arms (captureEnd's hook).
func (s *Server) idleArmLocked() {
	if s.idleExit <= 0 || s.idleFired || s.idleHalted || s.idleTimer != nil {
		return
	}
	s.idleTimer = afterFunc(s.idleExit, s.idleFire)
}

// idleFire is the armed timer's closure: re-verify under the lock that
// the daemon is truly idle (a client may have re-registered after the
// arming; a capture may outlive the window) and only then trigger — one
// log line and the close-once idleCh. Anything else stands the timer
// down silently; whichever ending re-empties the daemon re-arms it.
func (s *Server) idleFire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idleTimer = nil // this arming is spent either way
	if s.idleExit <= 0 || s.idleFired || s.idleHalted {
		return
	}
	if len(s.conns) > 0 || len(s.captures) > 0 {
		return // a client returned or a capture outlives the window
	}
	s.idleFired = true
	s.logf("idle exit: no clients for %s", s.idleExit)
	close(s.idleCh)
}

// Close shuts the daemon down gracefully: stop accepting, drop every
// API connection (agent-gone), stop the lease reaper, close every
// board controller connection (TCP close releases the board's
// controller slot — scripts keep running; the next daemon's startup
// audit reports them), and remove the sock file. Idempotent.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.auditCancel != nil {
			s.auditCancel()
		}
		s.ln.Close()

		// the idle exit stands down for good, BEFORE any ending below
		// can re-arm it: stopCapturesShutdown's capture endings and the
		// conn drops both hook into idleArmLocked, and a window armed
		// mid-shutdown would log a bogus "idle exit" on its way out
		s.mu.Lock()
		s.idleHalted = true
		if s.idleTimer != nil {
			s.idleTimer.Stop()
			s.idleTimer = nil
		}
		s.mu.Unlock()

		// live captures end deliberately at shutdown (files closed,
		// done pushes "stopped" — best effort toward connections that
		// drop moments later; proto.go's capture contract)
		s.stopCapturesShutdown()

		s.mu.Lock()
		conns := make([]*clientConn, 0, len(s.conns))
		for cc := range s.conns {
			conns = append(conns, cc)
		}
		sups := make([]*daemonBoard, 0, len(s.sups))
		for _, db := range s.sups {
			sups = append(sups, db)
		}
		s.mu.Unlock()
		for _, cc := range conns {
			cc.drop()
		}
		s.leases.Close()
		for _, db := range sups {
			// under connectMu: an in-flight connect (warmController /
			// lease USB / teardown) finishes first, so the controller it
			// installs is the one sup.Close reclaims — outside the
			// critical section a re-dial racing the close leaks one
			// extra controller connection (benign at exit, still wrong).
			db.connectMu.Lock()
			db.sup.Close()
			db.connectMu.Unlock()
		}
		if s.ownSock {
			os.Remove(s.sockPath)
		}
		go func() {
			s.wg.Wait()
			close(s.done)
		}()
	})
}

// ---------------------------------------------------------------------------
// loops
// ---------------------------------------------------------------------------

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
			}
			if isTemporary(err) {
				continue
			}
			s.logf("server: accept: %v", err)
			return
		}
		s.handleConn(c)
	}
}

// isTemporary reports accept errors worth retrying.
func isTemporary(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Temporary()
}

// handleConn wires one accepted connection: registers it, launches the
// writer and reader goroutines.
func (s *Server) handleConn(c net.Conn) {
	s.mu.Lock()
	s.connSeq++
	cc := &clientConn{
		srv:       s,
		conn:      c,
		outbound:  make(chan any, outboundQ),
		holder:    fmt.Sprintf("conn-%d", s.connSeq),
		replyWait: defaultReplyWait,
	}
	s.conns[cc] = struct{}{}
	// idle-exit hook: a live connection disarms the pending window (a
	// client returned; the fire closure re-checks under this same lock,
	// so a Stop()-too-late race is still refused)
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
	s.mu.Unlock()

	s.wg.Add(2)
	go cc.writeLoop()
	go cc.readLoop()
}

// writeLoop serializes every outbound message onto the connection.
func (cc *clientConn) writeLoop() {
	defer cc.srv.wg.Done()
	for m := range cc.outbound {
		if err := WriteMsg(cc.conn, m); err != nil {
			break // dead peer: the reader's error path drops the conn
		}
	}
	cc.conn.Close()
}

// readLoop dispatches requests until the stream ends. A frame-layer
// violation (oversize) ends the connection; a payload that is not JSON
// keeps the framing in sync and gets a server-class error reply.
func (cc *clientConn) readLoop() {
	defer cc.srv.wg.Done()
	s := cc.srv
	for {
		payload, err := ReadFrame(cc.conn)
		if err != nil {
			s.dropConn(cc)
			return
		}
		var req Request
		if jerr := json.Unmarshal(payload, &req); jerr != nil {
			if !cc.send(&Reply{Type: "reply", Error: &RPCError{Class: "server", Msg: "malformed JSON payload: " + jerr.Error()}}) {
				return
			}
			continue
		}
		reply := s.process(cc, &req)
		if !cc.send(reply) {
			return
		}
	}
}

// dropConn is the one-way teardown of a connection: unsubscribe from
// every board's events, signal agent-gone for its holder, stop the
// writer. Idempotent per connection.
func (s *Server) dropConn(cc *clientConn) { cc.drop() }

func (cc *clientConn) drop() {
	cc.dropO.Do(func() {
		s := cc.srv
		s.mu.Lock()
		delete(s.conns, cc)
		for mac6, set := range s.watchers {
			delete(set, cc)
			if len(set) == 0 {
				delete(s.watchers, mac6)
			}
		}
		// idle-exit hook: the LAST departure arms the window (captures
		// do not block arming — the fire closure re-checks them under
		// this lock and a live capture stands it down instead)
		if len(s.conns) == 0 {
			s.idleArmLocked()
		}
		s.mu.Unlock()

		holder := cc.holder_()
		s.leases.OnAgentGone(holder)
		s.logf("server: conn dropped (holder=%s)", holder)

		cc.wmu.Lock()
		cc.wdead = true
		close(cc.outbound)
		cc.wmu.Unlock()
		cc.conn.Close()
	})
}

// send queues one message; false means the connection is gone and the
// reader should stop. Never panics after drop (flag + close share wmu).
// A reply that meets a full all-replies queue (a pipelining client
// that stopped reading — review round 1's wedge) does NOT block: it
// waits up to replyWait for the writer to drain a slot, with wmu
// RELEASED between polls (broadcast and every other connection keep
// flowing; only this connection's reader pauses), and only then takes
// the last resort — evicting the oldest queued reply, logged and
// counted. Never a stall, never a silent drop.
func (cc *clientConn) send(m any) bool {
	wait := cc.replyWait
	if wait <= 0 {
		wait = defaultReplyWait
	}
	deadline := time.Now().Add(wait)
	for {
		cc.wmu.Lock()
		res := cc.enqueueLocked(m)
		if res != qWedged {
			cc.wmu.Unlock()
			return res == qEnqueued
		}
		cc.wmu.Unlock()
		if time.Now().After(deadline) {
			cc.wmu.Lock()
			defer cc.wmu.Unlock()
			return cc.lastResortEvictEnqueueLocked(m)
		}
		time.Sleep(replyWaitPoll) // wmu released: the writer (and only it) may drain
	}
}

// sendLocked is send() for callers already holding srv.mu (both
// serialize on cc.wmu; the server lock only guards the watcher set).
// It NEVER waits: events drop themselves on a full queue, and a reply
// reaching the wedge here (broadcast never sends one — belt and
// braces) is dropped with accounting rather than stall the daemon
// under srv.mu.
func (cc *clientConn) sendLocked(m any) bool {
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	switch cc.enqueueLocked(m) {
	case qGone:
		return false
	case qWedged:
		cc.repliesLost++
		cc.srv.logf("server: outbound queue wedged (holder=%s) — message dropped under srv.mu rather than stall the daemon (reply lost #%d)", cc.holder_(), cc.repliesLost)
		return true
	default:
		return true
	}
}

// enqueueResult is the non-blocking enqueue outcome.
type enqueueResult int

const (
	qEnqueued enqueueResult = iota // queued (or an event dropped per policy)
	qWedged                        // full queue, no event to evict — caller decides; never resolve it here
	qGone                          // connection is dead
)

// enqueueLocked queues one outbound message (caller holds wmu). The
// delivery policy (proto.go): events are fire-and-forget — an event
// meeting a full queue drops itself (fw §8 tap semantics — a slow
// agent must not stall the board's read loop); a reply meeting a full
// queue evicts the oldest QUEUED EVENT and never blocks. The one shape
// enqueueLocked refuses to resolve here is a full queue holding NO
// events — a pipelining client that parked 256+ replies and stopped
// reading (send() waits boundedly for that one; sendLocked accounts
// and drops) — because every resolution available under wmu is either
// a block or a reply eviction, and those are the caller's call.
func (cc *clientConn) enqueueLocked(m any) enqueueResult {
	if cc.wdead {
		return qGone
	}
	for {
		select {
		case cc.outbound <- m:
			return qEnqueued
		default:
		}
		if _, isEvent := m.(EventPush); isEvent {
			cc.srv.logf("server: outbound queue full (holder=%s) — event dropped", cc.holder_())
			return qEnqueued // per policy: done, nothing further to do
		}
		if cc.evictOldestEventLocked() {
			cc.srv.logf("server: outbound queue full (holder=%s) — oldest queued event evicted for the reply (replies are never silently dropped)", cc.holder_())
			continue // the freed slot is ours (only wmu holders enqueue): retry
		}
		return qWedged
	}
}

// lastResortEvictEnqueueLocked ends the bounded reply wait: the queue
// is still full of replies and the writer has not drained a slot. The
// oldest queued message goes — logged and counted, never silent — and
// ours takes the slot. Strictly non-blocking: the caller holds wmu,
// the popped slot is refilled by us (the only enqueuer under wmu), and
// the writer only ever receives.
func (cc *clientConn) lastResortEvictEnqueueLocked(m any) bool {
	if cc.wdead {
		return false
	}
	select {
	case old := <-cc.outbound:
		if _, isEvent := old.(EventPush); isEvent {
			cc.srv.logf("server: outbound queue (holder=%s) — oldest queued event evicted after the bounded wait", cc.holder_())
		} else {
			cc.repliesLost++
			cc.srv.logf("server: outbound queue wedged (holder=%s) — oldest queued reply (id %v) evicted after %s (reply lost #%d)", cc.holder_(), replyID(old), cc.replyWait, cc.repliesLost)
		}
		cc.outbound <- m // the slot we just freed: cannot block under wmu
		return true
	default:
		// the writer drained during the wait — a plain enqueue fits
		select {
		case cc.outbound <- m:
			return true
		default:
			// unreachable under wmu (only wmu holders enqueue, and we
			// hold it); account the loss loudly rather than stall
			cc.repliesLost++
			cc.srv.logf("server: outbound queue (holder=%s) — reply dropped in the unreachable full-after-drain shape (reply lost #%d)", cc.holder_(), cc.repliesLost)
			return true
		}
	}
}

// replyID digs the id out of a queued reply for the eviction log.
func replyID(m any) any {
	if r, ok := m.(*Reply); ok {
		return r.ID
	}
	return "?"
}

// evictOldestEventLocked removes the OLDEST queued event from the
// outbound queue (caller holds wmu), reporting whether one was found.
// Every receive is non-blocking — the writer goroutine also receives
// from the queue and may drain it concurrently, so a bare receive
// could block under wmu (review round 1). Queued replies encountered
// on the way rotate to the back — preserved, never dropped.
func (cc *clientConn) evictOldestEventLocked() bool {
	n := len(cc.outbound)
	for i := 0; i < n; i++ {
		select {
		case m := <-cc.outbound:
			if _, isEvent := m.(EventPush); isEvent {
				return true
			}
			cc.outbound <- m // the slot just freed: cannot block under wmu
		default:
			return false // the writer drained it: nothing to evict
		}
	}
	return false
}

// holder_ reads the bound holder.
func (cc *clientConn) holder_() string {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.holder
}

// bindHolder applies the holder rules: the first request carrying a
// holder binds the connection; a conflicting one is a protocol error.
func (cc *clientConn) bindHolder(h string) *RPCError {
	if h == "" {
		return nil
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.holderBound && cc.holder != h {
		return &RPCError{Class: "holder", Msg: fmt.Sprintf("connection bound to holder %q, request says %q", cc.holder, h)}
	}
	if !cc.holderBound {
		cc.holder = h
		cc.holderBound = true
	}
	return nil
}

// ---------------------------------------------------------------------------
// request dispatch
// ---------------------------------------------------------------------------

// process runs one request to its reply.
func (s *Server) process(cc *clientConn, req *Request) *Reply {
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.RequestTimeout)
	defer cancel()

	if herr := cc.bindHolder(req.Holder); herr != nil {
		return &Reply{Type: "reply", ID: req.ID, Error: herr}
	}

	var result any
	var err error
	switch req.Type {
	case "list_boards":
		result, err = s.hListBoards()
	case "board_status":
		result, err = s.hBoardStatus(ctx, req)
	case "lease":
		result, err = s.hLease(ctx, cc, req)
	case "release":
		result, err = s.hRelease(cc, req)
	case "eval":
		result, err = s.hEval(ctx, cc, req, false)
	case "submit":
		result, err = s.hEval(ctx, cc, req, true)
	case "script_start":
		result, err = s.hScriptStart(ctx, cc, req)
	case "script_stop":
		result, err = s.hScriptStop(ctx, cc, req)
	case "fs_write":
		result, err = s.hFsWrite(ctx, cc, req)
	case "fs_read":
		result, err = s.hFsRead(ctx, req)
	case "fs_rm":
		result, err = s.hFsRm(ctx, cc, req)
	case "session_reset":
		result, err = s.hSessionReset(ctx, cc, req)
	case "op":
		result, err = s.hOp(ctx, cc, req)
	case "watch_events":
		result, err = s.hWatchEvents(ctx, cc, req)
	case "capture_start":
		result, err = s.hCaptureStart(ctx, cc, req)
	case "capture_stop":
		result, err = s.hCaptureStop(req)
	default:
		err = &RPCError{Class: "server", Msg: "unknown request type " + strconv.Quote(req.Type)}
	}
	if err != nil {
		return &Reply{Type: "reply", ID: req.ID, Error: toRPC(err)}
	}
	return &Reply{Type: "reply", ID: req.ID, Result: result}
}

// toRPC maps an error into the wire taxonomy (proto.go): board classes
// verbatim, lease/resolve/holder/unreachable classes structured.
func toRPC(err error) *RPCError {
	var operr *replclient.OpError
	if errors.As(err, &operr) {
		return &RPCError{Class: operr.Class, Msg: operr.Msg}
	}
	var held *lease.ErrHeld
	if errors.As(err, &held) {
		e := &RPCError{Class: "held", Msg: held.Error(), Holder: held.Holder, Since: held.Since.Format(time.RFC3339)}
		if held.GraceExpiring() {
			e.GraceExpiry = held.GraceExpiry.Format(time.RFC3339)
		}
		return e
	}
	var rpc *RPCError
	if errors.As(err, &rpc) {
		return rpc
	}
	switch {
	case errors.Is(err, lease.ErrNotHeld):
		return &RPCError{Class: "not_held", Msg: err.Error()}
	case errors.Is(err, lease.ErrTeardownInProgress):
		return &RPCError{Class: "teardown", Msg: err.Error()}
	case errors.Is(err, board.ErrControllerSlotHeld):
		return &RPCError{Class: "slot_held", Msg: err.Error()}
	case errors.Is(err, errUnreachable):
		return &RPCError{Class: "unreachable", Msg: err.Error()}
	default:
		return &RPCError{Class: "server", Msg: err.Error()}
	}
}

// resolve maps a selector to one board, classifying resolve failures.
func (s *Server) resolve(selector string) (discover.Board, *RPCError) {
	b, err := s.reg.Resolve(selector)
	if err != nil {
		class := "no_board"
		if strings.Contains(err.Error(), "ambiguous") {
			class = "ambiguous"
		}
		return discover.Board{}, &RPCError{Class: class, Msg: err.Error()}
	}
	return b, nil
}

// boardView renders one registry entry for list/status replies.
func boardView(b discover.Board) map[string]any {
	v := map[string]any{
		"mac6":     b.MAC6,
		"alias":    b.Alias,
		"sources":  b.Sources,
		"host":     b.Host,
		"port":     b.Port,
		"usb_path": b.USBPath,
	}
	fw := ""
	if b.Hello != nil {
		fw = b.Hello.FwBuild
	}
	v["fw"] = fw
	return v
}

func (s *Server) hListBoards() (any, error) {
	boards := s.reg.Snapshot()
	out := make([]any, 0, len(boards))
	for _, b := range boards {
		out = append(out, boardView(b))
	}
	return map[string]any{"boards": out, "mdns_error": s.reg.MDNSError()}, nil
}

func (s *Server) hBoardStatus(ctx context.Context, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	res := map[string]any{"board": boardView(b), "connected": false, "lease": nil}

	var db *daemonBoard
	s.mu.Lock()
	db = s.sups[b.MAC6]
	s.mu.Unlock()
	if db != nil {
		if ctl := db.sup.Client(); ctl != nil {
			res["connected"] = true
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			db.connectMu.Lock()
			scripts, serr := db.sup.ScriptList(sctx)
			db.connectMu.Unlock()
			cancel()
			if serr != nil {
				res["scripts_error"] = serr.Error()
			} else {
				res["scripts"] = scripts
			}
		}
	}
	for _, ls := range s.leases.Snapshot() {
		if ls.Board != b.MAC6 {
			continue
		}
		lm := map[string]any{
			"holder": ls.Holder, "session": ls.Session,
			"since": ls.Since.Format(time.RFC3339),
		}
		if !ls.GraceDeadline.IsZero() {
			lm["grace_deadline"] = ls.GraceDeadline.Format(time.RFC3339)
		}
		res["lease"] = lm
	}
	return res, nil
}

// hLease serves lease. The default route never touches the board: the
// lease is host-side state and the first op dials the warm TCP
// controller. transport:"usb" is the DELIBERATE fallback (proto.go's
// transport contract): the refusal checks run first (a board leased
// by someone else must not be rebooted), then the controller is
// established over USB — opening the CDC port REBOOTS the board —
// and both the reply result and the daemon log carry
// USBRebootWarning. A USB open that fails leaves the lease held
// host-side (same as a TCP lease on an unreachable board: release or
// retry; the next op redials per its own transport rules). Never a
// silent fallback: a failed TCP dial stays unreachable — USB only
// ever happens through this explicit field.
func (s *Server) hLease(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	switch req.Transport {
	case "", "tcp": // the default warm route
	case "usb":
	default:
		return nil, &RPCError{Class: "server",
			Msg: fmt.Sprintf("unknown transport %q (lease accepts \"usb\"; default is tcp and never falls back)", req.Transport)}
	}
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	holder := connHolder(cc, req)
	if err := s.leases.Acquire(b.MAC6, holder); err != nil {
		return nil, err
	}
	sess, _ := s.leases.Session(b.MAC6)
	res := map[string]any{"board": b.MAC6, "holder": holder, "session": sess}
	if req.Transport == "usb" {
		db := s.supervisor(b)
		db.connectMu.Lock() // deliberate: held across ConnectUSB (~5s bounded — drain + hello); connects and Close serialize behind it
		ctl, cerr := db.sup.ConnectUSB(ctx)
		db.connectMu.Unlock()
		if cerr != nil {
			return nil, fmt.Errorf("%w: usb lease: %v", errUnreachable, cerr)
		}
		s.tap(b.MAC6, db, ctl)
		res["transport"] = "usb"
		res["warning"] = USBRebootWarning
		s.logf("server: board %s leased over USB by holder %s — %s", b.MAC6, holder, USBRebootWarning)
	}
	return res, nil
}

func (s *Server) hRelease(cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	holder := connHolder(cc, req)
	if err := s.leases.Release(b.MAC6, holder); err != nil {
		return nil, err
	}
	// a USB-transport lease leaves a USB controller warm; the board
	// cannot see a CDC close, so releasing yields the §12 slot
	// (observer hello) and drops the link — the next lease dials TCP
	// into a free slot, no headless-reopen ritual. A TCP controller is
	// left warm exactly as before (§12: its close releases the slot by
	// itself — nothing to yield).
	db := s.supervisor(b)
	db.connectMu.Lock()
	yielded := db.sup.YieldUSBController()
	db.connectMu.Unlock()
	if yielded {
		s.logf("server: board %s: USB controller yielded on release (§12 讓位) — next lease dials TCP", b.MAC6)
	}
	return map[string]any{"board": b.MAC6, "holder": holder}, nil
}

// connHolder returns the effective holder for a request: the request's
// explicit holder (already bound to the connection by process) or the
// connection's generated id.
func connHolder(cc *clientConn, req *Request) string {
	if req.Holder != "" {
		return req.Holder
	}
	return cc.holder_()
}

// ensureLiveLease is the holder gate for mutating board ops: the board
// must be leased by THIS holder — a grace lease of the same holder is
// adopted back live first (the adopt-within-TTL rule, proto.go). At the
// grace-expiry boundary the adopt itself can fail (the manager expires
// the due lease and runs the teardown synchronously inside Adopt) —
// that MUST refuse, not proceed (review I-1); a mid-teardown entry
// (visible in Snapshot without a grace deadline) refuses the same way,
// probed via Session (the only tearing-visible API).
func (s *Server) ensureLiveLease(cc *clientConn, req *Request, mac6 string) error {
	h := connHolder(cc, req)
	for _, ls := range s.leases.Snapshot() {
		if ls.Board != mac6 {
			continue
		}
		if ls.Holder != h {
			err := &lease.ErrHeld{Board: ls.Board, Holder: ls.Holder, Since: ls.Since}
			if !ls.GraceDeadline.IsZero() {
				err.GraceExpiry = ls.GraceDeadline
			}
			return err
		}
		if !ls.GraceDeadline.IsZero() {
			if !s.leases.Adopt(h) {
				return &RPCError{Class: "not_held",
					Msg: fmt.Sprintf("grace TTL expired on board %s (holder %s) — the lease was torn down at adopt; lease it fresh", mac6, h)}
			}
			s.logf("server: holder %s adopted board %s back live", h, mac6)
		}
		if _, ok := s.leases.Session(mac6); !ok {
			return &RPCError{Class: "not_held",
				Msg: fmt.Sprintf("lease on board %s is mid-teardown (holder %s) — retry shortly or lease it fresh", mac6, h)}
		}
		return nil
	}
	return &RPCError{Class: "not_held", Msg: fmt.Sprintf("no lease on board %s — lease it first (holder %s)", mac6, h)}
}

// hEval serves eval and submit (async=true) through the warm controller
// connection, lease-gated.
func (s *Server) hEval(ctx context.Context, cc *clientConn, req *Request, async bool) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	args := map[string]any{"code": req.Code}
	if req.Args != nil {
		v, verr := validateArgs("eval args", req.Args)
		if verr != nil {
			return nil, verr
		}
		args["args"] = v
	}
	op := "eval"
	if async {
		op = "submit"
		args["tag"] = req.Tag
	}
	res, err := s.boardOp(ctx, b, op, args)
	if err != nil {
		return nil, err
	}
	if async {
		return res, nil // board ack {tag, queued}
	}
	out := map[string]any{"out": res["out"]}
	if v, ok := res["value"]; ok && v != nil {
		out["value"] = v
	}
	return out, nil
}

// validateArgs walks the eval/submit/op args tree and repairs the
// JSON→msgpack number shape (found by the b5-full HIL S3): every JSON
// number decodes to float64, but the board's v3 args bridge (§4)
// accepts integers only — an integral float64 must cross as int64.
// The two float shapes that cannot cross honestly are refused
// daemon-side, before any board contact, naming the offending path
// (e.g. a.b[2]) under the calling face's prefix ("eval args:" /
// "op args:"):
//
//   - non-integral floats (the fw would refuse them anyway — this
//     error names the exact leaf instead of the fw's whole-args msg);
//   - floats at or beyond ±2^53 (controller ruling: the guard exists
//     because JSON-authored integers beyond ±2^53 may have already
//     lost precision client-side before the daemon sees them — the
//     hazard is sign-symmetric, so the rule is too; 9007199254740993
//     would silently cross as 9007199254740992). The largest integers
//     that cross are ±(2^53-1).
//
// map/array/bin (and any non-float scalar — wifi.setup's strings
// included) pass through untouched — bin/ext stay the fw bridge's
// verdict to give, not the daemon's.
func validateArgs(prefix string, v any) (any, *RPCError) {
	return validateArgsPath(prefix, v, "")
}

func validateArgsPath(prefix string, v any, path string) (any, *RPCError) {
	switch x := v.(type) {
	case float64:
		if x != math.Trunc(x) || x >= 1<<53 || x <= -(1<<53) {
			if path == "" {
				path = "args"
			}
			return nil, &RPCError{Class: "api",
				Msg: fmt.Sprintf("%s: %s: non-integer or |v|≥2^53 number", prefix, path)}
		}
		return int64(x), nil
	case map[string]any:
		for k, e := range x {
			kp := path + "." + k
			if path == "" {
				kp = k
			}
			ce, verr := validateArgsPath(prefix, e, kp)
			if verr != nil {
				return nil, verr
			}
			x[k] = ce
		}
		return x, nil
	case []any:
		for i, e := range x {
			ce, verr := validateArgsPath(prefix, e, fmt.Sprintf("%s[%d]", path, i))
			if verr != nil {
				return nil, verr
			}
			x[i] = ce
		}
		return x, nil
	default:
		return v, nil
	}
}

// opAllowlist is the ops face: the board ops an agent may reach
// DIRECTLY through the op passthrough request type. Deliberately
// narrow — the bench surface batch 6's MCP agents need (budget, wifi,
// log, caps, diagnostics) plus diag.i2cslave, the board's internal
// I2C echo-slave loopback fixture toggle ({mode:-1..1} = status/off/
// on) the b7 driver-pipeline loopback rides. eval/submit/fs.*/
// script.*/session.* have their own request types and are NOT
// reachable through op; every other diag.* stays internal.
var opAllowlist = map[string]bool{
	"budget.get": true, "budget.set": true,
	"wifi.status": true, "wifi.setup": true, "wifi.off": true,
	"log.read":  true,
	"caps":      true,
	"diag.pins": true, "diag.heapinfo": true, "diag.stats": true,
	"diag.i2cslave": true,
}

// AllowlistedOps renders the ops face's allowlisted op names, sorted —
// the single-source view every allowlist listing (the hOp refusal's op
// list below, the MCP xuansi_board_op descriptions) quotes, so the
// table stays the only truth and no copy can drift silently.
func AllowlistedOps() []string {
	ops := make([]string, 0, len(opAllowlist))
	for op := range opAllowlist {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	return ops
}

// hOp serves the ops face: an allowlisted board op forwarded over the
// warm controller under eval's lease gate — uniformly for ALL
// allowlisted ops (even read-only diag/caps; the gate is the ops
// face's shape, not each op's). args cross through the same numeric
// walk as eval/submit (controller ruling: JSON integers must msgpack
// as ints — budget.set's counters would otherwise reach the fw as
// floats and be refused), refusing non-integral / ±2^53 numbers
// daemon-side under the "op args:" prefix; every other value shape
// crosses verbatim (the board is the authority), and board error
// class/msg pass through like eval's. An off-allowlist op is a
// daemon-side "server" refusal naming the op.
func (s *Server) hOp(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	if !opAllowlist[req.Op] {
		return nil, &RPCError{Class: "server",
			Msg: fmt.Sprintf("op %q is outside the ops-face allowlist (%s) — eval/submit/fs/script/session keep their own request types",
				req.Op, strings.Join(AllowlistedOps(), ", "))}
	}
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	var args map[string]any
	if req.Args != nil {
		m, ok := req.Args.(map[string]any)
		if !ok {
			return nil, &RPCError{Class: "server",
				Msg: fmt.Sprintf("op %s: args must be a JSON object of the op's parameters", req.Op)}
		}
		v, verr := validateArgs("op args", m)
		if verr != nil {
			return nil, verr
		}
		args = v.(map[string]any)
	}
	// budget.get has no wire op of its own: the frozen §4 table has only
	// budget.set — and it REFUSES its no-keys form ("missing
	// eval_ms/event_ms"), so there is no getter to forward. The frozen
	// read path for the runtime budgets is caps' limits block (「limits
	// 反映 runtime 當前值」 per the bench API page), so budget.get reads
	// caps and projects the {eval_ms, event_ms} pair the ops face
	// promises. Found by the b6 full HIL — the unit fake used to accept
	// the raw name the daemon sent.
	//
	// nil-limits guard (B7 minors item 3): a caps reply WITHOUT a
	// limits block is off-contract (every fw caps carries one) — refuse
	// api-class rather than project silent JSON nulls that read as
	// "budgets are null". A limits block missing an INDIVIDUAL key
	// projects that key as ABSENT (the board did not report it; a null
	// would claim it reported nothing).
	if req.Op == "budget.get" {
		res, err := s.boardOp(ctx, b, "caps", nil)
		if err != nil {
			return nil, err
		}
		lim, ok := res["limits"].(map[string]any)
		if !ok {
			return nil, &RPCError{Class: "api",
				Msg: fmt.Sprintf("budget.get: board %s caps reply carries no limits block — cannot read the runtime budgets (fw off-contract?); re-run op caps directly to inspect the reply", b.MAC6)}
		}
		out := map[string]any{}
		if v, ok := lim["eval_budget_ms"]; ok {
			out["eval_ms"] = v
		}
		if v, ok := lim["event_budget_ms"]; ok {
			out["event_ms"] = v
		}
		return out, nil
	}
	return s.boardOp(ctx, b, req.Op, args)
}

func (s *Server) hScriptStart(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	sess, ok := s.leases.Session(b.MAC6)
	if !ok {
		return nil, &RPCError{Class: "not_held", Msg: "lease vanished before script_start"}
	}
	args := map[string]any{"name": req.Name, "code": req.Code, "session": sess}
	return s.boardOp(ctx, b, "script.start", args)
}

func (s *Server) hScriptStop(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	return s.boardOp(ctx, b, "script.stop", map[string]any{"name": req.Name})
}

func (s *Server) hFsWrite(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	if len(req.Data) > fsFileMax {
		return nil, &RPCError{
			Class: "api",
			Msg:   fmt.Sprintf("fs_write %s: %d bytes exceeds the %dB single-file cap (refused daemon-side)", req.Path, len(req.Data), fsFileMax),
		}
	}
	return s.boardOp(ctx, b, "fs.write", map[string]any{"path": req.Path, "data": req.Data})
}

func (s *Server) hFsRead(ctx context.Context, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	return s.boardOp(ctx, b, "fs.read", map[string]any{"path": req.Path})
}

// hFsRm removes one board file (op fs.rm) under fs_write's lease gate
// — the removal path the fs face lacked until B8 (the b7 full HIL had
// to clean /drv over an out-of-pipeline direct REPL, honestly labeled).
// The board's error class/msg cross verbatim: a missing file is the
// board's own api not-found, never reworded daemon-side.
func (s *Server) hFsRm(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	return s.boardOp(ctx, b, "fs.rm", map[string]any{"path": req.Path})
}

// hSessionReset releases one board session's scripts and pins (op
// session.reset) under eval's lease gate — the dedicated path the b7
// S4b detour (release-lease → re-lease) worked around: bare evals
// claim pins in namespace 0 (owner _eval, shared across evals), which
// no ops-face entry could reach because session.* is deliberately
// keep-out of the op allowlist (batch-6 ruling — this request type is
// the alternative, not an allowlist exception). Session omitted = 0;
// the board's {stopped:n} reply and error class/msg cross verbatim.
// The lease itself is untouched (its teardown keeps its own sweep).
func (s *Server) hSessionReset(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	return s.boardOp(ctx, b, "session.reset", map[string]any{"session": req.Session})
}

func (s *Server) hWatchEvents(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	ctl, err := s.warmController(ctx, b)
	if err != nil {
		return nil, err
	}
	if req.Source != "" {
		if _, err := ctl.Op(ctx, "events.subscribe", map[string]any{"source": req.Source}); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	set := s.watchers[b.MAC6]
	if set == nil {
		set = map[*clientConn]struct{}{}
		s.watchers[b.MAC6] = set
	}
	set[cc] = struct{}{}
	s.mu.Unlock()
	return map[string]any{"board": b.MAC6}, nil
}

// ---------------------------------------------------------------------------
// board plumbing: supervisors, warm controller, taps, board ops
// ---------------------------------------------------------------------------

// lookupExact finds a registry entry by full mac6.
func (s *Server) lookupExact(mac6 string) (discover.Board, bool) {
	for _, b := range s.reg.Snapshot() {
		if b.MAC6 == mac6 {
			return b, true
		}
	}
	return discover.Board{}, false
}

// supervisor returns (creating on demand) the board's supervisor
// wrapper.
func (s *Server) supervisor(b discover.Board) *daemonBoard {
	s.mu.Lock()
	defer s.mu.Unlock()
	if db, ok := s.sups[b.MAC6]; ok {
		return db
	}
	db := &daemonBoard{}
	db.sup = s.opts.NewSupervisor(board.BoardRef{
		MAC6:    b.MAC6,
		Host:    b.Host,
		Port:    b.Port,
		USBPath: b.USBPath,
	})
	s.sups[b.MAC6] = db
	return db
}

// warmController connects (or reuses) the board's warm TCP controller
// and installs the event tap + hello enrichment on it.
func (s *Server) warmController(ctx context.Context, b discover.Board) (board.Controller, error) {
	db := s.supervisor(b)
	db.connectMu.Lock()
	defer db.connectMu.Unlock()
	ctl, err := db.sup.Connect(ctx)
	if err != nil {
		if errors.Is(err, board.ErrControllerSlotHeld) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", errUnreachable, err)
	}
	s.tap(b.MAC6, db, ctl)
	return ctl, nil
}

// tap installs the all-sources event tap on a NEW controller (identity
// check under the server lock) and enriches the registry with its hello
// identity (the fw column of list/status). A redial swaps controllers:
// the next server-side touch re-taps — events inside the redial window
// are not forwarded (honest limitation, fw pushes them only to the dead
// connection anyway).
func (s *Server) tap(mac6 string, db *daemonBoard, ctl board.Controller) {
	s.mu.Lock()
	already := db.tapped == ctl
	if !already {
		db.tapped = ctl
	}
	s.mu.Unlock()
	if already {
		return
	}
	ctl.OnEvent("", func(ev replclient.Event) {
		s.broadcast(mac6, ev)
	})
	if h, err := ctl.Hello(false, "xuansi-daemon"); err == nil {
		s.reg.Enrich(mac6, h)
	}
}

// broadcast fans one board event out to the board's watchers.
// Fire-and-forget: a watcher whose queue is full misses the event
// (logged), mirroring the fw's own tap semantics (§8) — a slow agent
// must not stall the board's read loop.
func (s *Server) broadcast(mac6 string, ev replclient.Event) {
	msg := EventPush{Type: "event", Board: mac6, Event: WireEvent{Source: ev.Source, Data: ev.Data}}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.feedCapturesLocked(mac6, ev) // the capture face rides the same tap
	for cc := range s.watchers[mac6] {
		if !cc.sendLocked(msg) {
			s.logf("server: event to holder %s dropped (connection gone)", cc.holder_())
		}
	}
}

// boardOp runs one op over the warm controller connection, redialing
// once when the link died under it (§13: redial ≠ reboot).
func (s *Server) boardOp(ctx context.Context, b discover.Board, op string, args map[string]any) (map[string]any, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		ctl, err := s.warmController(ctx, b)
		if err != nil {
			return nil, err
		}
		res, err := ctl.Op(ctx, op, args)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if !errors.Is(err, replclient.ErrDead) {
			return nil, err // board error (OpError) or caller ctx — no retry
		}
		s.logf("server: board %s: op %s hit a dead link (%v) — redialing once", b.MAC6, op, err)
		db := s.supervisor(b)
		db.connectMu.Lock()
		db.sup.Close() // drop the corpse so the next Connect redials
		db.connectMu.Unlock()
	}
	return nil, fmt.Errorf("board %s: %s after redial: %w", b.MAC6, op, lastErr)
}

// leaseTeardown is the lease manager's board-side action, bounded so a
// vanished board cannot pin the releasing API call forever (the error
// is recorded honestly in the lease trace; the lease still ends). The
// supervisor touch is connect-serialized with every other board path —
// a teardown racing a warm connect must not double-dial the board into
// two controller connections (§12 arbitration).
func (s *Server) leaseTeardown(ctx context.Context, mac6, holder string, sessions []int) error {
	b, ok := s.lookupExact(mac6)
	if !ok {
		return fmt.Errorf("board %s no longer in the registry", mac6)
	}
	tctx, cancel := context.WithTimeout(ctx, s.opts.TeardownTimeout)
	defer cancel()
	db := s.supervisor(b)
	db.connectMu.Lock()
	defer db.connectMu.Unlock()
	return db.sup.LeaseTeardown(tctx, mac6, holder, sessions)
}

// ---------------------------------------------------------------------------
// daemon lifecycle helpers: startup audit + refresh loop
// ---------------------------------------------------------------------------

// startupAudit runs the initial discovery sweep and the T10-ruling
// orphan audit: every TCP-dialable board is connected, script.list +
// diag.pins + wifi.status'd against an empty baseline, and REPORTED —
// never killed (單人紀律 — the operator decides). USB is never dialed
// here: opening the CDC port reboots the board (§2), which is not a
// startup side effect anyone wants.
func (s *Server) startupAudit(ctx context.Context) {
	defer s.wg.Done()
	if err := s.reg.Refresh(ctx); err != nil {
		s.logf("server: initial discovery sweep failed: %v", err)
	} else {
		s.logf("server: initial discovery sweep: %d board(s)", len(s.reg.Snapshot()))
	}
	for _, b := range s.reg.Snapshot() {
		if !hasSource(b, discover.SourceMDNS) || b.Host == "" {
			continue // no TCP face to audit without rebooting
		}
		select {
		case <-s.closed:
			return
		case <-ctx.Done():
			return
		default:
		}
		actx, cancel := context.WithTimeout(ctx, s.opts.AuditTimeout)
		db := s.supervisor(b)
		db.connectMu.Lock()
		rep, err := db.sup.Resync(actx)
		db.connectMu.Unlock()
		cancel()
		if err != nil {
			s.logf("server: startup audit board %s: %v", b.MAC6, err)
			continue
		}
		s.logf("server: startup audit board %s: cold=%v scripts=%v pins=%d wifi_state=%v (orphans listed, not killed)",
			b.MAC6, rep.ColdStart, rep.Scripts, len(rep.Pins), rep.Wifi["state"])
		if ctl := db.sup.Client(); ctl != nil {
			s.tap(b.MAC6, db, ctl)
		}
	}
}

// refreshLoop keeps the registry fresh.
func (s *Server) refreshLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.opts.RefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.reg.Refresh(ctx); err != nil {
				s.logf("server: discovery refresh failed: %v", err)
			}
		}
	}
}

// hasSource reports whether a board's live sources include src.
func hasSource(b discover.Board, src string) bool {
	for _, x := range b.Sources {
		if x == src {
			return true
		}
	}
	return false
}
