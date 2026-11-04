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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/lease"
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
	outboundQ = 256 // per-connection event/reply buffer; overflow drops (fire-and-forget, fw §8 style)
)

// errUnreachable wraps dial/hello failures for the error taxonomy.
var errUnreachable = errors.New("board unreachable")

// DefaultSockPath is the daemon socket: $XDG_RUNTIME_DIR/xuansi/daemon.sock,
// falling back to /tmp/xuansi-daemon.sock when XDG_RUNTIME_DIR is unset
// (e.g. a systemd unit without the env, or a manual run).
func DefaultSockPath() string {
	if x := os.Getenv("XDG_RUNTIME_DIR"); x != "" {
		return filepath.Join(x, "xuansi", "daemon.sock")
	}
	return "/tmp/xuansi-daemon.sock"
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

	mu       sync.Mutex
	sups     map[string]*daemonBoard             // mac6 → supervisor wrapper
	conns    map[*clientConn]struct{}            // live API connections
	watchers map[string]map[*clientConn]struct{} // mac6 → subscribed conns
	connSeq  int64

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
		opts:     o,
		logf:     o.Log,
		reg:      reg,
		ln:       ln,
		sockPath: o.SockPath,
		ownSock:  true,
		sups:     map[string]*daemonBoard{},
		conns:    map[*clientConn]struct{}{},
		watchers: map[string]map[*clientConn]struct{}{},
		closed:   make(chan struct{}),
		done:     make(chan struct{}),
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
				return nil, fmt.Errorf("server: %s: another daemon is already listening", path)
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
			db.sup.Close()
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
		srv:      s,
		conn:     c,
		outbound: make(chan any, outboundQ),
		holder:   fmt.Sprintf("conn-%d", s.connSeq),
	}
	s.conns[cc] = struct{}{}
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
func (cc *clientConn) send(m any) bool {
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	if cc.wdead {
		return false
	}
	select {
	case cc.outbound <- m:
		return true
	default:
		s := cc.srv
		s.logf("server: outbound queue full (holder=%s) — message dropped", cc.holder_())
		return true
	}
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
		result, err = s.hLease(cc, req)
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
	case "watch_events":
		result, err = s.hWatchEvents(ctx, cc, req)
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

func (s *Server) hLease(cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	holder := connHolder(cc, req)
	if err := s.leases.Acquire(b.MAC6, holder); err != nil {
		return nil, err
	}
	sess, _ := s.leases.Session(b.MAC6)
	return map[string]any{"board": b.MAC6, "holder": holder, "session": sess}, nil
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
		args["args"] = req.Args
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
	for cc := range s.watchers[mac6] {
		if !cc.sendLocked(msg) {
			s.logf("server: event to holder %s dropped (connection gone)", cc.holder_())
		}
	}
}

// sendLocked is send() for callers already holding srv.mu (both
// serialize on cc.wmu; the server lock only guards the watcher set).
func (cc *clientConn) sendLocked(m any) bool {
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	if cc.wdead {
		return false
	}
	select {
	case cc.outbound <- m:
		return true
	default:
		s := cc.srv
		s.logf("server: outbound queue full (holder=%s) — event dropped", cc.holder_())
		return true
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
