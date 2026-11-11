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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	// fsFileMax is the daemon's chunk size on the board's chunked fs
	// transport (B15 §4.5①: the 16KB→64KB wave). The fw's fs-layer cap
	// XS_FILE_MAX is 65526B, but the WIRE-realizable single-op max —
	// W1-T3 measured on the v7 build d42a07d — is 65482B for fs.write
	// and ~65472B for fs.write_part (op framing overhead 43–53B), so
	// the daemon chunks at the write_part-safe bound 65472: one size
	// serves both the single-shot fs.write and every fs.write_part
	// (this is the protocol-page number). One fs.write / fs.write_part
	// / fs.read_part carries at most this many bytes; fs_write payloads
	// above it (and every VERIFIED write, sha256 key present — B15
	// §4.5②) ride fs.write_part × N + one sha256-verifying
	// fs.write_commit, transparent to internal-API clients (the request
	// type and reply shape are unchanged).
	fsFileMax = 65472
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

// errSourceGone is the fast-fail refusal's sentinel (B15 §4.1②): the
// registry's CURRENT snapshot carries no live source for the board —
// every discovery face lost it (unplugged, off-WiFi; the entry is
// last-known only). A board-touching op refused with it dialed NOTHING.
var errSourceGone = errors.New("no live discovery source (all sources gone — last-known entry only; refresh discovery, check cable/WiFi)")

// boardErr marks a board-touching failure with the board it belongs to,
// so toRPC can attach the §4.1 pathology fields (cause +
// last_seen_age_ms). It renders as the wrapped error and unwraps to it;
// the wrap site is the transport chokepoints (warmController, the op
// stream paths), never the board-answered OpErrors.
type boardErr struct {
	mac6 string
	err  error
}

func (e *boardErr) Error() string { return e.err.Error() }
func (e *boardErr) Unwrap() error { return e.err }

// boardFail wraps err (nil-safe) as mac6's board-touching failure.
func (s *Server) boardFail(mac6 string, err error) error {
	if err == nil {
		return nil
	}
	var be *boardErr
	if errors.As(err, &be) {
		return err // already wrapped — never double-wrap
	}
	return &boardErr{mac6: mac6, err: err}
}

// lastSeenAgeMS reports milliseconds since the daemon's last protocol
// crossing with the board (-1 when it never crossed one, honestly —
// a board never touched has no age to report, only absence).
func (s *Server) lastSeenAgeMS(mac6 string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, ok := s.sups[mac6]
	if !ok || db.lastSeen.IsZero() {
		return -1
	}
	return time.Since(db.lastSeen).Milliseconds()
}

// boardOpCause maps a board-touching failure onto the four-value cause
// taxonomy (B15 §5): source gone / dial failed / link dead /
// arbitration. Timeouts and other link-crossing failures that fit none
// of the first three report "link dead" — the op could not complete
// over the link and the daemon will re-establish it; the msg carries
// the precise story.
func boardOpCause(err error) string {
	switch {
	case errors.Is(err, errSourceGone):
		return "source gone"
	case errors.Is(err, board.ErrControllerSlotHeld):
		return "arbitration"
	case errors.Is(err, replclient.ErrDead):
		return "link dead"
	case errors.Is(err, errUnreachable):
		return "dial failed"
	default:
		return "link dead"
	}
}

// afterFunc is the idle-exit timer seam (production: time.AfterFunc;
// tests swap it to capture and fire the arming deterministically).
var afterFunc = time.AfterFunc

// ErrAlreadyListening is bindSock's spawn-race sentinel: the dial probe
// found a live daemon owning the socket. The loser of a launcher spawn
// race (or a deliberate double start) is a harmless no-op — serve.go
// keys its exit-0 "leaving" path on this; the wrapped error still
// names the socket path.
var ErrAlreadyListening = errors.New("server: another daemon is already listening")

// USBTransportNote is the §4.4 (B15) disclosure carried by every lease
// that OPENED the USB route — the daemon's auto-fallback (only USB
// live) or the explicit transport:"usb": one connection = one device
// session, so the open rebooted the board. It rides the lease reply
// (key "transport_note"), the daemon log, and the CLI's stdout — the
// anti-silent-reboot rule (§2 反目標).
const USBTransportNote = "USB route: opening the CDC port REBOOTS the board (one connection = one device session) — running scripts were killed; when a TCP source is live again the next default lease upgrades back to TCP"

// USBWarmNote is the §4.4 disclosure for a USB-route lease that found
// the USB controller ALREADY warm: no new open, no second reboot — the
// board keeps the device session the first open created.
const USBWarmNote = "USB route (already connected): riding the existing CDC session — this lease did not reboot the board again"

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
	// ref is the BoardRef snapshot sup's dial seams were pinned against.
	// supervisor() compares it with the CURRENT registry snapshot on
	// every acquisition: any endpoint drift (Host/IP/Port/USBPath)
	// rebuilds the seams — Bug 1 (B15 §4.10), a USB-only first
	// appearance used to keep a permanent "no TCP endpoint to dial"
	// seam until daemon restart.
	ref board.BoardRef

	connectMu sync.Mutex
	// tapped is the controller the event tap is installed on (interface
	// identity — production dynamic types are pointers, comparable). A
	// redial swaps the controller; the next server-side touch re-taps.
	tapped board.Controller
	// frames is the board's 0x02 consumer fan-out (B12-P2 T5): the
	// connection's sink slot is single, so the per-call collectors of
	// overlapping eval windows and the pending-job buffer below all
	// ride this one hub, asserted on the link around every streaming
	// op. jobBuf buffers stream frames while submitted jobs are
	// pending (jobsPending > 0); each job event drains it onto the
	// event's data (spec §5.6 — submit is the same primitive, its
	// window ends at the event, not the ack). jobsMu guards
	// jobsPending and jobGen; the buffer's own lock guards its state.
	frames      *board.FrameHub
	jobBuf      *board.FrameCollector
	jobsMu      sync.Mutex
	jobsPending int
	// jobGen counts controller-identity resets (jobReset): a pending
	// window opened at generation g is only alive while jobGen == g —
	// a submit retrying across a redial re-arms its window when it
	// sees the generation move (jobEnsure, the final-review honest-
	// drops fix: the retry's job is acked on the NEW link, so the
	// reset must not swallow its window).
	jobGen uint64
	// the job_wait ledger (B15 §4.3), all under jobsMu: every tag
	// submitted through this daemon (pending) plus the completion
	// events the tap delivered (done), FIFO-bounded so a long-lived
	// daemon cannot grow it without end. jobWake is the broadcast
	// channel job_wait parks on — closed and replaced on every
	// delivery, so all waiters wake at once.
	jobs    map[string]*jobEntry
	jobFIFO []string
	jobWake chan struct{}
	// bootSeen is the last hello sighting's boot identity (B10-T8b):
	// a CHANGED boot_epoch on the next sighting means the board
	// rebooted while we were not connected (panic auto-reboot included
	// — epoch change is epoch change regardless of cause) → the
	// board.reboot event. lastSeen (B12-P2 T6) is the daemon's most
	// recent protocol crossing with the board (hello sighting or op
	// reply) — list_boards/board_status's last_seen. Both guarded by
	// the Server mu (like tapped), in-memory only (v1 ruling): a
	// daemon restart re-baselines silently — first sighting after
	// restart never fires.
	bootSeen *bootSight
	lastSeen time.Time
}

// bootSight is one hello sighting's boot identity (B10-T8b): the fw's
// NVS-persisted epoch, the fw build fingerprint, and when the daemon
// saw it (gap_ms of the reboot event spans last-seen → re-detected).
// B10-T8c adds fwVersion (semver; empty on older fw) for the
// fw_version_prev/fw_version event pair.
type bootSight struct {
	epoch     uint32
	fwBuild   string
	fwVersion string
	at        time.Time
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
		aliases, pins, err := discover.LoadBoardsFile(path)
		if err != nil {
			return nil, fmt.Errorf("server: boards file: %w", err)
		}
		reg = discover.NewRegistry(aliases, pins)
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

// wsaeaddrinuse is Windows' bind-conflict errno (WSAEADDRINUSE 10048,
// text "Only one usage of each socket address (protocol/network
// address/port) is normally permitted."). Go's syscall.EADDRINUSE on
// Windows is an invented APPLICATION_ERROR-offset value real Winsock
// calls never return (there is no stdlib errno mapping for it), so the
// race-loser classification below must know the WSA form too — the
// T9 Windows real-machine finding. On Linux the constant is inert: the
// kernel never reports an errno that high.
const wsaeaddrinuse = syscall.Errno(10048)

// isAddrInUse reports whether err is a bind conflict in any supported
// platform's dress — the errno family a spawn-race loser's bind fails
// with when the winner created the socket file between the loser's
// stat and its bind: Linux EADDRINUSE, Windows WSAEADDRINUSE (10048),
// darwin EEXIST ("file exists"). Matching EEXIST everywhere is safe:
// Linux bind conflicts report EADDRINUSE, so EEXIST cannot
// false-positive there; on darwin it IS the conflict form (the T9 mac
// finding — unclassified losers died bare instead of re-probing into
// the graceful exit). All sides of the spawn race — winner and loser —
// must classify the same conflict so the loser re-probes into the
// graceful ErrAlreadyListening exit instead of a bare exit-1.
func isAddrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE) ||
		errors.Is(err, wsaeaddrinuse) ||
		errors.Is(err, syscall.EEXIST)
}

// claimSeq makes every racer's claim name unique inside one process
// (the hammer test's goroutines share a pid); combined with the pid it
// is unique across processes on the machine.
var claimSeq atomic.Uint64

// bindSock probes for a live daemon, clears a stale sock file, and
// claims the listen path — ATOMICALLY on linux/darwin (bindClaim:
// bind-temp + link), under a transition lockfile on windows
// (sockclaim_windows.go: the link protocol is not expressible there,
// and its remove-stale TOCTOU is real — CI disproved the "residual,
// theoretical" call). The platform seam is bindSockPlatform.
func bindSock(path string) (net.Listener, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("server: mkdir %s: %w", dir, err)
		}
	}
	return bindSockPlatform(path)
}

// bindDirect is the plain bind: net.Listen, and on a bind-conflict
// errno (isAddrInUse — the three-platform family) re-probe before
// erroring: a live daemon is the graceful already-listening refusal;
// a dead file is stale, clear it and bind once more. Windows's whole
// path (linkClaimSupported is false there), and the fallback for any
// link-incapable filesystem on linux/darwin.
func bindDirect(path string) (net.Listener, error) {
	ln, err := net.Listen("unix", path)
	if err == nil {
		return ln, nil
	}
	if isAddrInUse(err) {
		if c, derr := net.Dial("unix", path); derr == nil {
			c.Close()
			return nil, fmt.Errorf("%w: %s", ErrAlreadyListening, path)
		}
		if rerr := os.Remove(path); rerr == nil {
			if ln2, lerr := net.Listen("unix", path); lerr == nil {
				return ln2, nil
			}
		}
	}
	return nil, fmt.Errorf("server: listen %s: %w", path, err)
}

// bindClaim claims the socket path atomically (linux/darwin): bind a
// UNIQUE scratch name beside the target and fully arm it (bind+listen
// both done — the listener answers connects), and only then
// os.Link(scratch, target): the target name materializes for an
// already-listening socket, exactly once — link fails EEXIST if
// another racer's claim landed first.
//
// WHY (the darwin TOCTOU, T9 live finding): with a direct bind the
// socket FILE exists from the bind while listen() may not be armed
// yet; a racing prober's dial on darwin gets ECONNREFUSED inside that
// window, misjudges the socket stale, REMOVES the winner's file and
// binds its own — two winners, the first orphaned on an unlinked
// inode, unreachable forever (and with no connection, never
// idle-exits). Linux hides the window (connect succeeds against the
// backlog), but the defect was portable; only the exposure was
// darwin's. A unix socket is its inode, so the listener keeps
// answering on the linked target after the scratch name is dropped
// (pinned by TestHardLinkedSocketNameAnswers).
//
// EEXIST (lost the link race): re-probe the target — alive is the
// graceful ErrAlreadyListening loser path; dead is stale garbage,
// removed, claim retried (bounded — three lost races in a row is a
// live adversary, not a race). Any other link error (a filesystem
// that refuses socket hard-links) falls back to bindDirect, whose
// conflict classification handles the race the old way. The scratch
// suffix costs ~11 bytes of sun_path headroom beside the target —
// keep target paths comfortably under the 104-byte darwin limit (the
// tests do, via testutil.ShortSockDir).
func bindClaim(path string) (net.Listener, error) {
	sweepClaimScratch(path)
	for attempt := 0; attempt < 3; attempt++ {
		// scratch suffix ".c-" + 8 hex ≈ 11 bytes of sun_path headroom:
		// pid<<10 | seq packed into 32 bits (processes diverge on pid,
		// racers in one process on seq — wraps after 1024 claims, and a
		// collision just costs one EADDRINUSE retry below)
		claim := fmt.Sprintf("%s.c-%08x", path, uint64(os.Getpid())<<10|uint64(claimSeq.Add(1)&((1<<10)-1)))
		ln, err := net.Listen("unix", claim)
		if err != nil {
			if isAddrInUse(err) && attempt < 2 {
				continue // scratch-name collision: draw a fresh name
			}
			return nil, fmt.Errorf("server: listen claim %s: %w", claim, err)
		}
		lerr := os.Link(claim, path)
		if lerr == nil {
			// the scratch name has done its job; the target answers now
			os.Remove(claim)
			return ln, nil
		}
		ln.Close() // also drops the scratch name
		if !errors.Is(lerr, syscall.EEXIST) {
			// socket hard-links unavailable here: the direct bind
			return bindDirect(path)
		}
		// lost the link race — the target exists; probe it
		if c, derr := net.Dial("unix", path); derr == nil {
			c.Close()
			return nil, fmt.Errorf("%w: %s", ErrAlreadyListening, path)
		}
		if rerr := os.Remove(path); rerr != nil && !os.IsNotExist(rerr) {
			return nil, fmt.Errorf("server: remove stale sock %s: %w", path, rerr)
		}
	}
	return nil, fmt.Errorf("server: claim %s: lost the link race 3 times", path)
}

// isClaimScratch reports whether name is one of target's claim-scratch
// names: exactly target + ".c-" + 8 hex digits (bindClaim's shape —
// the pid<<10|seq packing above). Anything else — the target itself, a
// short/long/malformed suffix, a different prefix — is not sweepable.
func isClaimScratch(target, name string) bool {
	suffix, ok := strings.CutPrefix(name, target+".c-")
	if !ok || len(suffix) != 8 {
		return false
	}
	for i := 0; i < len(suffix); i++ {
		c := suffix[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// sweepClaimScratch removes DEAD claim-scratch siblings of path: a
// racer killed between its scratch bind and the link (or before the
// post-link scratch remove) leaks its path+".c-XXXXXXXX" socket file
// forever — this bind-time sweep is the only garbage collector that
// name shape has (final-review minor). Liveness is a probe-dial: a
// scratch that CONNECTS is an active racer's armed listener and is
// left strictly alone; a socket that refuses the dial, or a regular
// file wearing the name, is garbage and removed. The main sock path
// never matches the scratch shape, and nothing else in the directory
// is touched. The tiny bind-but-not-yet-listening window inside a
// live racer's net.Listen can cost it its scratch file — its link
// then fails ENOENT, which bindClaim routes to bindDirect, the exact
// pre-claim conflict handling: degraded, never orphaned.
func sweepClaimScratch(path string) {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return // the MkdirAll/bind below surfaces a real problem
	}
	base := filepath.Base(path)
	for _, e := range entries {
		if !isClaimScratch(base, e.Name()) {
			continue
		}
		scratch := filepath.Join(dir, e.Name())
		if fi, ierr := e.Info(); ierr == nil && fi.Mode()&os.ModeSocket != 0 {
			if c, derr := net.Dial("unix", scratch); derr == nil {
				c.Close()
				continue // an active racer still holds this scratch
			}
		}
		os.Remove(scratch)
	}
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
	case "job_wait":
		result, err = s.hJobWait(ctx, req)
	case "capture_start":
		result, err = s.hCaptureStart(ctx, cc, req)
	case "capture_stop":
		result, err = s.hCaptureStop(req)
	case "boards_pin":
		result, err = s.hBoardsPin(req)
	case "boards_unpin":
		result, err = s.hBoardsUnpin(req)
	default:
		err = &RPCError{Class: "server", Msg: "unknown request type " + strconv.Quote(req.Type)}
	}
	if err != nil {
		return &Reply{Type: "reply", ID: req.ID, Error: s.toRPC(err)}
	}
	return &Reply{Type: "reply", ID: req.ID, Result: result}
}

// toRPC maps an error into the wire taxonomy (proto.go): board classes
// verbatim, lease/resolve/holder/unreachable classes structured. A
// board-touching failure (boardErr) additionally carries the §4.1
// pathology fields: cause (four-value taxonomy) and last_seen_age_ms.
func (s *Server) toRPC(err error) *RPCError {
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
		// a pre-shaped RPCError crosses as-is (its class is deliberate —
		// the api-class daemon refusals); the pathology fields still
		// attach when a boardErr wraps it.
		var be *boardErr
		if errors.As(err, &be) {
			age := s.lastSeenAgeMS(be.mac6)
			rpc.Cause = boardOpCause(err)
			rpc.LastSeenAgeMS = &age
		}
		return rpc
	}
	var e *RPCError
	switch {
	case errors.Is(err, lease.ErrNotHeld):
		e = &RPCError{Class: "not_held", Msg: err.Error()}
	case errors.Is(err, lease.ErrTeardownInProgress):
		e = &RPCError{Class: "teardown", Msg: err.Error()}
	case errors.Is(err, board.ErrControllerSlotHeld):
		e = &RPCError{Class: "slot_held", Msg: err.Error()}
	case errors.Is(err, errUnreachable):
		e = &RPCError{Class: "unreachable", Msg: err.Error()}
	default:
		e = &RPCError{Class: "server", Msg: err.Error()}
	}
	var be *boardErr
	if errors.As(err, &be) {
		age := s.lastSeenAgeMS(be.mac6)
		e.Cause = boardOpCause(err)
		e.LastSeenAgeMS = &age
	}
	return e
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

// staleAfter is the staleness window (B15 §4.1⑥): a board with no live
// discovery source whose last protocol crossing is older than this (or
// never happened) reports stale:true — last-known only, nothing
// dialable. A live source or a fresh crossing keeps the board current.
const staleAfter = 30 * time.Second

// boardView renders one registry entry for list/status replies. The
// boot-identity fields (B12-P2 T6) ride the same enriched hello the
// fw/fw_version pair does — boot_epoch/reset_reason appear once the
// daemon has sighted the board, absent before (honest: nothing to
// report), epoch 0 = pre-b9 fw without the counter. last_seen is the
// daemon's most recent protocol crossing (tap hello or op reply),
// from the daemon-side board state. B15 §4.1⑥ adds the explicit
// freshness face: transport (the current ACTUAL route, "" when never
// connected), last_seen_age_ms (numeric; -1 = never crossed), and
// stale (no live source AND no crossing within staleAfter —
// never-crossed counts as infinitely old).
func (s *Server) boardView(b discover.Board) map[string]any {
	v := map[string]any{
		"mac6":     b.MAC6,
		"alias":    b.Alias,
		"sources":  b.Sources,
		"host":     b.Host,
		"ip":       b.IP, // browse-resolved A/AAAA — the address the daemon dials first
		"port":     b.Port,
		"usb_path": b.USBPath,
	}
	fw, fwVer := "", ""
	if b.Hello != nil {
		fw = b.Hello.FwBuild
		fwVer = b.Hello.FwVersion // B10-T8c: semver beside the build id (esp_app_desc face)
		v["boot_epoch"] = int64(b.Hello.BootEpoch)
		v["reset_reason"] = b.Hello.ResetReason
		if b.Hello.Chip != "" { // P3-T13: the pairing source rides the same sighting
			v["chip"] = b.Hello.Chip
		}
	}
	v["fw"] = fw
	v["fw_version"] = fwVer
	s.mu.Lock()
	db := s.sups[b.MAC6]
	var last time.Time
	var transport string
	if db != nil {
		last = db.lastSeen
		transport = db.sup.Transport()
	}
	s.mu.Unlock()
	age := int64(-1)
	if !last.IsZero() {
		age = time.Since(last).Milliseconds()
	}
	v["transport"] = transport
	v["last_seen_age_ms"] = age
	v["stale"] = len(b.Sources) == 0 && (last.IsZero() || time.Since(last) >= staleAfter)
	if !last.IsZero() {
		v["last_seen"] = last.UTC().Format(time.RFC3339)
	}
	return v
}

func (s *Server) hListBoards() (any, error) {
	boards := s.reg.Snapshot()
	out := make([]any, 0, len(boards))
	for _, b := range boards {
		out = append(out, s.boardView(b))
	}
	return map[string]any{"boards": out, "mdns_error": s.reg.MDNSError()}, nil
}

func (s *Server) hBoardStatus(ctx context.Context, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	res := map[string]any{"board": s.boardView(b), "connected": false, "lease": nil}

	var db *daemonBoard
	s.mu.Lock()
	db = s.sups[b.MAC6]
	s.mu.Unlock()
	if db != nil {
		// connectMu also covers the Client() read itself: a concurrent
		// endpoint-drift rebuild swaps db.sup under this same lock, so an
		// unlocked read would race it.
		db.connectMu.Lock()
		if ctl := db.sup.Client(); ctl != nil {
			res["connected"] = true
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			scripts, serr := db.sup.ScriptList(sctx)
			db.connectMu.Unlock()
			cancel()
			if serr != nil {
				res["scripts_error"] = serr.Error()
			} else {
				res["scripts"] = scripts
				s.noteBoardContact(b.MAC6) // script.list's reply is a protocol crossing (last_seen)
			}
		} else {
			db.connectMu.Unlock()
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

// tcpDialable reports whether b carries a live TCP-capable source with
// a dialable endpoint — the pin/mdns face (⑨'s pin counts here: a
// pinned cross-subnet board's route IS the pin). §4.4's route decision
// turns on this.
func tcpDialable(b discover.Board) bool {
	if !hasSource(b, discover.SourcePin) && !hasSource(b, discover.SourceMDNS) {
		return false
	}
	return b.Port != 0 && (b.IP != "" || b.Host != "")
}

// noteTransport (B15 §4.4) broadcasts the synthetic board transport
// event — the switch notification, following the board.reboot
// precedent (same "board" source, kind inside data): data carries
// {kind:"transport", from, to, rebooted}. from is the route in hand
// before the switch ("" when the daemon held no live controller);
// rebooted is true only when the switch OPENED the CDC port (to=usb,
// §2 one-connection-one-session). Fired on every actual switch of a
// daemon-held controller link — a lease taking the USB route with a
// fresh open (auto-fallback or explicit), the USB→TCP upgrade at a
// default lease (TCP revived, endpoint unchanged), and the
// endpoint-drift seam rebuild closing a live USB route with TCP
// capable in the fresh resolution (supervisor()'s rebuild branch —
// the upgrade completing outside any lease). Never on a same-route
// re-lease, a release/shutdown yield (the route drops to none, not to
// another route), or a mere redial (§13: redial ≠ reboot).
func (s *Server) noteTransport(mac6, from, to string, rebooted bool) {
	if from == to {
		return
	}
	s.logf("server: board %s transport %q → %q (rebooted=%v) — board.transport event", mac6, from, to, rebooted)
	s.broadcast(mac6, replclient.Event{Source: "board", Data: map[string]any{
		"kind":     "transport",
		"from":     from,
		"to":       to,
		"rebooted": rebooted,
	}})
}

// hLease serves lease with the §4.4 (B15) transport decision. The
// explicit transport param still overrides; OMITTED, the DAEMON
// resolves the route from the live sources:
//
//	TCP-capable source live (pin/mdns)  → tcp  — never a gratuitous
//	                                      USB reboot, whatever USB's
//	                                      state; the lease itself does
//	                                      not touch the board.
//	only USB live                       → usb  — the auto-fallback:
//	                                      the CDC open happens AT
//	                                      LEASE TIME, rebooting the
//	                                      board, disclosed in the
//	                                      reply (rebooted:true +
//	                                      transport_note) — never
//	                                      silent.
//	no live source at all               → tcp  — the host-side lease
//	                                      of an unreachable board
//	                                      (ops refuse fast with
//	                                      cause "source gone").
//
// A default lease that finds a warm USB controller with TCP now live
// upgrades back to TCP: the §12 yield, the board.transport event
// (usb→tcp, no reboot — §13), the next op dials TCP. The refusal
// checks run before any USB open (a board leased by someone else must
// not be rebooted); a lease whose USB open then fails stays held
// host-side (release or retry). Warm-controller reuse stays
// transport-agnostic: a USB-route lease with the USB controller
// already warm rides it without a second open (rebooted:false, the
// warm note).
func (s *Server) hLease(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	switch req.Transport {
	case "", "tcp", "usb":
	default:
		return nil, &RPCError{Class: "server",
			Msg: fmt.Sprintf("unknown transport %q (lease accepts \"tcp\" or \"usb\"; omitted = the daemon decides: TCP when a TCP-capable source is live, USB — with the reboot disclosure — when only USB is)", req.Transport)}
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
	res := map[string]any{"board": b.MAC6, "holder": holder, "session": sess, "rebooted": false}

	usbLive := hasSource(b, discover.SourceUSB) && b.USBPath != ""
	tcpLive := tcpDialable(b)
	route := req.Transport
	if route == "" {
		route = "tcp"
		if !tcpLive && usbLive {
			route = "usb" // §4.4 auto-fallback: only USB live
		}
	}

	if route != "usb" {
		res["transport"] = "tcp"
		// §4.4 upgrade: a warm USB controller with TCP now live — a
		// default (or explicit-tcp) lease yields the §12 slot and
		// re-arms the TCP route. No board touch: the next op dials.
		db := s.supervisor(b)
		db.connectMu.Lock()
		if prev := db.sup.Transport(); prev == "usb" && tcpLive {
			if db.sup.YieldUSBController() {
				s.logf("server: board %s: TCP source live again — USB controller yielded, route upgraded to TCP (§4.4)", b.MAC6)
				s.noteTransport(b.MAC6, "usb", "tcp", false)
			}
		}
		db.connectMu.Unlock()
		return res, nil
	}

	// route == usb (explicit or the auto-fallback)
	if !usbLive {
		// fast-fail (§4.1②): the registry's current snapshot shows no
		// live USB source — refuse with zero dial (cause "source gone")
		return nil, s.boardFail(b.MAC6, fmt.Errorf("board %s: usb lease: %w", b.MAC6, errSourceGone))
	}
	db := s.supervisor(b)
	db.connectMu.Lock() // deliberate: held across ConnectUSB (~5s bounded — drain + hello); connects and Close serialize behind it
	prev := db.sup.Transport()
	var ctl board.Controller
	if prev == "usb" {
		ctl = db.sup.Client() // warm USB: ride the existing session — no second open, no second reboot
	}
	// fresh labels whether THIS call opens the CDC port — the branch
	// the reply rides (fix round 1, Minor-2): if Dead() flipped in the
	// Transport()→Client() gap, Client() answers nil and the fresh
	// open below is an ACTUAL reboot; labeling it from the stale
	// prev=="usb" reading would have said rebooted:false.
	fresh := ctl == nil
	var cerr error
	if fresh {
		ctl, cerr = db.sup.ConnectUSB(ctx)
	}
	db.connectMu.Unlock()
	if cerr != nil {
		return nil, s.boardFail(b.MAC6, fmt.Errorf("%w: usb lease: %v", errUnreachable, cerr))
	}
	s.tap(b.MAC6, db, ctl)
	res["transport"] = "usb"
	if !fresh {
		res["transport_note"] = USBWarmNote
		s.logf("server: board %s leased over the warm USB controller by holder %s — no new open", b.MAC6, holder)
		return res, nil
	}
	res["rebooted"] = true
	res["transport_note"] = USBTransportNote
	s.noteTransport(b.MAC6, prev, "usb", true)
	s.logf("server: board %s leased over USB by holder %s — %s", b.MAC6, holder, USBTransportNote)
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

// hBoardsPin serves boards_pin (⑨, B15 §4.9): declare the registry's
// pin source for mac6 at ip:port — validated and applied by the
// registry (immediate effect, survives refreshes). No lease, no board
// contact: the pin is daemon-side routing state; the NEXT lease/op
// dials the endpoint. The reply echoes {pinned, endpoint} from the
// NORMALIZED values (the registry stores trimmed/lowercased — the
// echoed endpoint is exactly what was pinned; JoinHostPort renders a
// v6 address bracketed).
func (s *Server) hBoardsPin(req *Request) (any, error) {
	mac6 := strings.ToLower(strings.TrimSpace(req.Mac6))
	ip := strings.TrimSpace(req.IP)
	if err := s.reg.Pin(mac6, ip, req.Port); err != nil {
		return nil, &RPCError{Class: "server", Msg: err.Error()}
	}
	ep := net.JoinHostPort(ip, strconv.Itoa(req.Port))
	s.logf("server: boards_pin %s → %s (registry pin source; dial priority usb > pin > mdns)", mac6, ep)
	return map[string]any{"pinned": mac6, "endpoint": ep}, nil
}

// hBoardsUnpin serves boards_unpin (⑨): remove exactly the pin
// declaration — idempotent-shaped (unpinning a never-pinned mac6
// echoes success); the board keeps every other live source. No lease,
// no board contact.
func (s *Server) hBoardsUnpin(req *Request) (any, error) {
	mac6 := strings.ToLower(strings.TrimSpace(req.Mac6))
	if !discover.IsMac6(mac6) {
		return nil, &RPCError{Class: "server",
			Msg: fmt.Sprintf("boards_unpin: %q is not a mac6 (6 lowercase hex chars)", mac6)}
	}
	if s.reg.Unpin(mac6) {
		s.logf("server: boards_unpin %s (pin removed; other live sources kept)", mac6)
	}
	return map[string]any{"unpinned": mac6}, nil
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
	if req.BudgetMs > 0 {
		args["budget_ms"] = req.BudgetMs // per-call override, §9.6 — this key only
	}
	op := "eval"
	if async {
		op = "submit"
		args["tag"] = req.Tag
	}
	db := s.supervisor(b)
	if async {
		// The pending-job frame window opens BEFORE the op: the board
		// may start the job the moment it writes the ack, and the
		// router dispatches stream chunks in arrival order — opening
		// after the ack reply would race the job's first chunks. The
		// window ends at the job event (tap drains jobBuf onto it).
		// Across a redial the tap's jobReset retires the window (it
		// must — the old link's OTHER jobs are orphans); the ensure
		// closure re-arms THIS submit's window right before every
		// attempt's op, wherever the redial landed (final-review
		// Important-1: the retry acks on the new link, its frames are
		// ours to report — never silently dropped).
		gen := db.jobSubmit()
		db.jobTagSubmit(req.Tag) // the job_wait ledger tracks the tag from its submit (§4.3)
		ensure := func() {
			gen = db.jobEnsure(gen)
			db.jobTagSubmit(req.Tag) // a redial's jobReset dropped it; the retry re-records
		}
		res, err := s.boardOpStream(ctx, b, op, args, db, nil, ensure)
		if err != nil {
			db.jobRollback() // never queued: close the window
			db.jobTagRollback(req.Tag)
			return nil, err
		}
		return res, nil // board ack {tag, queued} — byte-unchanged
	}
	// The eval window: a per-call collector rides the board's frame
	// hub for exactly this op; whatever 0x02 frames complete inside
	// the window ride the reply (spec §5.6) — absent when none did.
	col := board.NewFrameCollector()
	remove := db.frames.Add(col.Sink)
	defer remove()
	res, err := s.boardOpStream(ctx, b, op, args, db, col, nil)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"out": res["out"]}
	if v, ok := res["value"]; ok && v != nil {
		out["value"] = v
	}
	if snap := col.Snapshot(); len(snap.Frames) > 0 || snap.Dropped > 0 {
		if frames := wireFrames(snap); len(frames) > 0 {
			out["frames"] = frames
		}
		if snap.Dropped > 0 {
			out["frames_dropped"] = int64(snap.Dropped)
		}
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
// on) the b7 driver-pipeline loopback rides. The fw's stills trio
// left with the stills face's retirement (B12-P2 T6): still on the
// board, no longer forwarded. eval/submit/fs.*/script.*/session.*
// have their own request types and are NOT reachable through op;
// every other diag.* stays internal.
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
	res, err := s.boardOp(ctx, b, req.Op, args)
	if err != nil {
		return nil, err
	}
	// Bug 2② (B15 §4.9③): wifi.setup/wifi.status replies self-report the
	// board's ip/port — write them back onto the registry entry (the one
	// address source guaranteed current; discovery may have resolved
	// nothing, or a stale address from before a re-association).
	if req.Op == "wifi.setup" || req.Op == "wifi.status" {
		ip, _ := res["ip"].(string)
		port, _ := resU64(res, "port")
		if ip != "" || port > 0 {
			s.reg.EnrichEndpoint(b.MAC6, ip, int(port))
			if ip != "" && ip != b.IP {
				s.logf("server: board %s self-reported endpoint %s:%d (wifi op reply) — registry updated", b.MAC6, ip, port)
			}
		}
	}
	return res, nil
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

// hFsWrite writes a whole file to the board under the lease. At or
// below the chunk cap (65472B) with no sha256 key it is verbatim
// fs.write — the pre-B10 wire, zero change. Everything else rides the
// chunked transport: fs.write_part per 65472B chunk + one fs.write_commit
// carrying the payload's sha256 (the board hashes chunk-by-chunk as it
// writes and verifies at commit; the rename that lands the file is
// atomic). B15 §4.5②: a PRESENT sha256 key (the push path's
// verification — the post-push byte read-back is retired) switches the
// write to the verified path for ANY size: even a small payload rides
// write_part + commit, and the commit verdict IS the verification. A
// malformed sha (not 64 hex) is refused with ZERO board contact — the
// fw would refuse it at commit, after the whole flash write had already
// crossed; refusing here saves the cycle. A dead link under the stream
// redials ONCE and restarts at off=0 — the .part reopen truncates and
// the board-side hash ledger restarts, so a restart is always
// idempotent (§13 redial ≠ reboot). The reply is the commit's {size} —
// the same shape fs.write returns, so internal-API clients see no
// difference (B10-T1).
func (s *Server) hFsWrite(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	if req.Sha256 != "" && !sha256HexRe.MatchString(req.Sha256) {
		return nil, &RPCError{Class: "api",
			Msg: fmt.Sprintf("fs_write %s: sha256 must be 64 hex chars (got %q) — refused with zero board contact; the fw's fs.write_commit would refuse it only AFTER the flash write crossed", req.Path, req.Sha256)}
	}
	if req.Sha256 == "" && len(req.Data) <= fsFileMax {
		return s.boardOp(ctx, b, "fs.write", map[string]any{"path": req.Path, "data": req.Data})
	}
	shaHex := req.Sha256
	if shaHex == "" {
		sum := sha256.Sum256(req.Data)
		shaHex = hex.EncodeToString(sum[:])
	}
	var res map[string]any
	err := s.withController(ctx, b, func(ctl board.Controller) error {
		for off := 0; off < len(req.Data); off += fsFileMax {
			end := off + fsFileMax
			if end > len(req.Data) {
				end = len(req.Data)
			}
			if _, err := s.opSameLink(ctx, b, ctl, "fs.write_part", map[string]any{
				"path": req.Path, "off": off, "data": req.Data[off:end],
			}, true, nil); err != nil {
				return err
			}
		}
		r, err := s.opSameLink(ctx, b, ctl, "fs.write_commit", map[string]any{
			"path": req.Path, "expected_len": len(req.Data), "sha256": shaHex,
		}, true, nil)
		if err != nil {
			return err
		}
		res = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// sha256HexRe gates the fs_write verified path's caller-supplied digest
// (exactly what the fw's own commit gate demands — xs_fs.c: "sha256
// must be 64 hex chars").
var sha256HexRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// hFsRead reads a whole board file back through the v5 chunked read
// face: fs.read_part windows of 16384B assembled until eof (a file at
// or below the cap completes in one call; bigger files loop — the
// frozen fs.read op still refuses them). The reply shape is unchanged
// ({data: base64}); a missing file surfaces as the board's own api
// error. The stream redials once on a dead link and restarts at off=0
// (reads are stateless on the board, so a restart is trivially safe).
func (s *Server) hFsRead(ctx context.Context, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	var out []byte
	err := s.withController(ctx, b, func(ctl board.Controller) error {
		out = out[:0] // a redial attempt restarts at off=0 — drop any partial
		// prefix the dead attempt appended (fix round 1: without this the
		// retry would duplicate the whole file onto the partial prefix and
		// return the corruption as success)
		var off uint32
		for {
			res, err := s.opSameLink(ctx, b, ctl, "fs.read_part", map[string]any{
				"path": req.Path, "off": int(off), "len": fsFileMax,
			}, true, nil)
			if err != nil {
				return err
			}
			data, _ := res["data"].([]byte)
			total, _ := resU64(res, "total")
			eof, _ := res["eof"].(bool)
			out = append(out, data...)
			off += uint32(len(data))
			if eof || len(data) == 0 || (total > 0 && uint64(off) >= total) {
				return nil
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"data": out}, nil
}

// resU64 reads an integral result field across the fake (int/int64) and
// the msgpack-decoded (int64) shapes.
func resU64(res map[string]any, key string) (uint64, bool) {
	switch v := res[key].(type) {
	case int64:
		return uint64(v), v >= 0
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	}
	return 0, false
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
// Negative sessions are refused daemon-side (whole-branch final
// review, Critical): -1 is the board's INTERNAL op-face pin sentinel
// (XS_PIN_SESSION_OPFACE — the on-board device face's 14 slots), never
// a user namespace; crossing it would sweep those slots while the
// device stays logically active, reopening dual-drive. The fw refuses
// it too (belt on both sides); refusing here keeps the daemon from
// ever putting the request on the wire.
func (s *Server) hSessionReset(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	if req.Session < 0 {
		return nil, &RPCError{Class: "api",
			Msg: fmt.Sprintf("session_reset: session %d refused — -1 is the board's internal op-face pin sentinel (never a user namespace); session must be 0 or positive", req.Session)}
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
		s.noteBoardContact(b.MAC6) // the subscribe's reply is a protocol crossing (last_seen)
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
// wrapper. Bug 1 fix (B15 §4.10): each acquisition compares the
// CURRENT registry snapshot's endpoint fields against the ref the
// supervisor's dial seams were pinned to — any drift (Host/IP/Port/
// USBPath) transparently rebuilds the seams and closes the stale
// controller corpse. Callers are unchanged; the daemonBoard (frames
// hub, boot baseline, last_seen) survives the rebuild, and a warm
// connection on an UNCHANGED ref is reused as before. The swap runs
// under connectMu so an in-flight Connect on the old seams finishes
// first and the corpse Close is exact (no orphaned live link).
func (s *Server) supervisor(b discover.Board) *daemonBoard {
	ref := board.BoardRef{
		MAC6:    b.MAC6,
		Host:    b.Host,
		IP:      b.IP, // browse-resolved address — TCPAddr dials it first (CGO=0 cannot resolve .local)
		Port:    b.Port,
		USBPath: b.USBPath,
	}
	s.mu.Lock()
	if db, ok := s.sups[b.MAC6]; ok {
		if db.ref == ref {
			s.mu.Unlock()
			return db
		}
		s.mu.Unlock()
		// Endpoint drift: rebuild under connectMu (serializes with the
		// in-flight connects every other board path holds it for), then
		// re-verify under s.mu — a racing rebuild may have landed while
		// this goroutine waited. connectMu→s.mu matches the existing
		// warmController→tap ordering, so no lock-cycle opens here.
		db.connectMu.Lock()
		var old *board.Supervisor
		s.mu.Lock()
		if db.ref != ref {
			old = db.sup
			db.ref = ref
			db.sup = s.opts.NewSupervisor(ref)
			s.logf("server: board %s endpoint drift (host=%q ip=%q port=%d usb=%q) — supervisor dial seams rebuilt, stale link closed",
				b.MAC6, ref.Host, ref.IP, ref.Port, ref.USBPath)
		}
		s.mu.Unlock()
		// oldRoute is read OUTSIDE s.mu (still under connectMu, which
		// serializes every controller-touching path on this board) —
		// the supervisor's own short-lived mutex must not nest inside
		// the server lock. Before the Close below nils the controller.
		oldRoute := ""
		if old != nil {
			oldRoute = old.Transport()
		}
		db.connectMu.Unlock()
		if old != nil {
			old.Close() // drop the corpse: TCP close releases the board's controller slot (§12)
			// Fix round 1 (Important-1): a rebuild that closes a live USB
			// route while the fresh resolution is TCP-capable IS the
			// USB→TCP upgrade — the §4.4 switch completing OUTSIDE any
			// lease (TCP revived WITH an endpoint change; the no-drift
			// revive upgrades at the next default lease instead). The
			// next lease cannot report it (the fresh supervisor reads
			// Transport()==""), so this is the event's only emission site.
			if oldRoute == "usb" && tcpDialable(b) {
				s.logf("server: board %s: drift rebuild closed the USB route with TCP live — route upgraded to TCP (§4.4)", b.MAC6)
				s.noteTransport(b.MAC6, "usb", "tcp", false)
			}
		}
		return db
	}
	db := &daemonBoard{ref: ref}
	db.sup = s.opts.NewSupervisor(ref)
	// The frames face (B12-P2 T5): the hub's own sink is what gets
	// asserted on a controller (boardOpStream); the pending-job buffer
	// is its one long-lived consumer, gated by jobsPending — chunks
	// outside a submitted-job window pass uncollected and uncounted
	// (they are not ours to report). Board-side every ch=2 emitter
	// runs in the one serial VM task, so a pending window's chunks are
	// exactly that job's.
	db.frames = board.NewFrameHub()
	db.jobBuf = board.NewFrameCollector()
	db.frames.Add(db.jobSink)
	s.sups[b.MAC6] = db
	s.mu.Unlock()
	return db
}

// jobSink is the pending-job buffer's hub consumer: chunks count only
// while a submitted job is outstanding. Runs on the router goroutine
// (quick, no client callbacks — the collector is mutex-only).
func (db *daemonBoard) jobSink(ch byte, seq uint32, idx, total uint16, payload []byte) {
	if !db.jobActive() {
		return
	}
	db.jobBuf.Sink(ch, seq, idx, total, payload)
}

// jobActive reports whether any submitted job is outstanding.
func (db *daemonBoard) jobActive() bool {
	db.jobsMu.Lock()
	defer db.jobsMu.Unlock()
	return db.jobsPending > 0
}

// jobSubmit opens the pending-job frame window BEFORE the submit op is
// issued (the board may start the job — and streaming — the moment the
// ack is written; opening after the ack would race the first chunks).
// It returns the window's generation: a controller identity change
// (jobReset, below) retires every window of an older generation.
func (db *daemonBoard) jobSubmit() uint64 {
	db.jobsMu.Lock()
	defer db.jobsMu.Unlock()
	db.jobsPending++
	return db.jobGen
}

// jobEnsure re-arms a window a controller reset may have retired: a
// no-op while the generation has not moved, a reopen (pending++) when
// it has. boardOpStream calls it immediately before EVERY submit
// attempt's op, so the window is provably open across the op whichever
// way the redial landed — the reset that orphans the old link's jobs
// must not swallow THIS submit's window: its retry is acked on the new
// link, its job's frames and drops are ours to report (honest drops).
func (db *daemonBoard) jobEnsure(gen uint64) uint64 {
	db.jobsMu.Lock()
	defer db.jobsMu.Unlock()
	if db.jobGen == gen {
		return gen
	}
	db.jobsPending++ // reopen on the current generation
	return db.jobGen
}

// jobRollback closes a window whose submit never queued (op error).
func (db *daemonBoard) jobRollback() {
	db.jobsMu.Lock()
	if db.jobsPending > 0 {
		db.jobsPending--
	}
	db.jobsMu.Unlock()
}

// jobReset drops every pending window and the buffer (a controller
// identity change: the board direct-delivers job events to the
// SUBMITTING connection — xs_repl.c — so a redial orphans the old
// connection's pending jobs board-side; their events never arrive).
// The generation bump is what a retrying submit's jobEnsure looks for:
// the retry rides the NEW controller, so its window re-opens while the
// old link's other windows stay retired. B15 §4.3: the job_wait ledger
// drops its PENDING entries the same way — the daemon can never hear
// from those jobs again, so a wait on one refuses as unknown-tag
// honestly (a retrying submit re-records its tag through the ensure
// closure before its op re-crosses).
func (db *daemonBoard) jobReset() {
	db.jobsMu.Lock()
	db.jobsPending = 0
	db.jobGen++
	if db.jobs != nil {
		kept := make([]string, 0, len(db.jobFIFO))
		for _, tag := range db.jobFIFO {
			if e, ok := db.jobs[tag]; ok && !e.done {
				delete(db.jobs, tag)
				continue
			}
			kept = append(kept, tag)
		}
		db.jobFIFO = kept
	}
	db.wakeJobsLocked()
	db.jobsMu.Unlock()
	db.jobBuf.Reset()
}

// jobEntry is one tag's job_wait ledger record.
type jobEntry struct {
	done bool
	data map[string]any // the job event's data (tag/ok/out/value | class/msg)
}

// jobLedgerMax bounds the ledger's DONE entries (FIFO eviction
// oldest-first). Pending entries are never evicted — only jobReset
// retires them, and they are bounded by real outstanding submits.
const jobLedgerMax = 256

// wakeJobsLocked closes the current wake channel (every parked waiter
// wakes and re-checks its tag) and arms a fresh one. Caller holds jobsMu.
func (db *daemonBoard) wakeJobsLocked() {
	if db.jobWake != nil {
		close(db.jobWake)
	}
	db.jobWake = make(chan struct{})
}

// jobTagSubmit records a tag pending at the submit chokepoint (hEval;
// the ensure closure re-records it after a redial's jobReset). An
// existing PENDING entry is a no-op (the re-record); an existing DONE
// entry resets to pending — a caller REUSING a tag for a new submit
// invalidates the old answer by their own hand (the wait must not
// answer job #2 with job #1's result).
func (db *daemonBoard) jobTagSubmit(tag string) {
	if tag == "" {
		return
	}
	db.jobsMu.Lock()
	defer db.jobsMu.Unlock()
	if db.jobs == nil {
		db.jobs = map[string]*jobEntry{}
	}
	if e, ok := db.jobs[tag]; ok {
		if !e.done {
			return // already pending (the ensure re-record)
		}
		e.done, e.data = false, nil // tag reuse: a fresh submit retires the old answer
		return
	}
	db.jobs[tag] = &jobEntry{}
	db.jobFIFO = append(db.jobFIFO, tag)
}

// jobTagRollback removes a tag whose submit never queued (op error).
func (db *daemonBoard) jobTagRollback(tag string) {
	db.jobsMu.Lock()
	defer db.jobsMu.Unlock()
	if e, ok := db.jobs[tag]; ok && !e.done {
		delete(db.jobs, tag)
		for i, t := range db.jobFIFO {
			if t == tag {
				db.jobFIFO = append(db.jobFIFO[:i], db.jobFIFO[i+1:]...)
				break
			}
		}
	}
}

// jobDeliver records one job completion event off the tap (called AFTER
// drainJobFrames enriched the data with the job's buffered frames —
// value/out/class/msg are the keys the wait surfaces). A tag with no
// pending record (a redial-orphaned submit that actually queued, a job
// from an earlier daemon generation) is recorded done anyway: the event
// is the truth, and a wait on it deserves the answer.
func (db *daemonBoard) jobDeliver(ev *replclient.Event) {
	tag, _ := ev.Data["tag"].(string)
	if tag == "" {
		return // a tagless job event is off-contract; nothing to key on
	}
	db.jobsMu.Lock()
	defer db.jobsMu.Unlock()
	if db.jobs == nil {
		db.jobs = map[string]*jobEntry{}
	}
	e, ok := db.jobs[tag]
	if !ok {
		e = &jobEntry{}
		db.jobs[tag] = e
		db.jobFIFO = append(db.jobFIFO, tag)
	}
	e.done, e.data = true, ev.Data
	// FIFO-bound the done entries (oldest-first; a pending head blocks
	// eviction — pending is bounded by real outstanding submits)
	for len(db.jobFIFO) > jobLedgerMax {
		head := db.jobFIFO[0]
		if he, hk := db.jobs[head]; !hk || !he.done {
			break
		}
		delete(db.jobs, head)
		db.jobFIFO = db.jobFIFO[1:]
	}
	db.wakeJobsLocked()
}

// jobWait parks on the ledger for one tag's completion: known-and-done
// answers immediately (the agent that waited after the fact); pending
// parks on the wake channel until the tap delivers, the timeout burns,
// or the request ctx dies; unknown tag is an api refusal (jobs are
// tracked from their submit through THIS daemon — the board
// direct-delivers job events to the submitting connection, so a foreign
// connection's jobs are invisible, honestly). The timeout answers
// (nil, false, nil) — a RESULT, never an error.
func (db *daemonBoard) jobWait(ctx context.Context, tag string, timeout time.Duration) (map[string]any, bool, error) {
	unknown := &RPCError{Class: "api",
		Msg: fmt.Sprintf("job_wait: no job tagged %q is known to this daemon — jobs are tracked from their submit through this daemon (a job submitted on another connection delivers its event there, not here)", tag)}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	db.jobsMu.Lock()
	e, known := db.jobs[tag]
	wake := db.jobWake
	db.jobsMu.Unlock()
	if !known {
		return nil, false, unknown
	}
	if e.done {
		return e.data, true, nil
	}
	for {
		select {
		case <-wake:
		case <-timer.C:
			return nil, false, nil
		case <-ctx.Done():
			return nil, false, nil // the request ctx died; the reply goes nowhere anyway
		}
		db.jobsMu.Lock()
		e, known = db.jobs[tag]
		wake = db.jobWake
		db.jobsMu.Unlock()
		if !known { // jobReset orphaned it mid-wait
			return nil, false, unknown
		}
		if e.done {
			return e.data, true, nil
		}
	}
}

// jobCleanupHint is the coaching every dead-job answer carries (B15
// §5): the cleanup ritual in kill order — loaded instances stay
// loaded, claimed pins stay claimed, and session_reset releases CLAIMS
// but never instances.
const jobCleanupHint = "job died mid-run — clean up before retrying: " +
	"(1) wasm instances it loaded STAY loaded (memory stays admitted): unload each with eval bench.wasm.unload(name); " +
	"(2) pins it claimed stay claimed (ns-0 pins survive the job): release them with xuansi_session_reset; " +
	"(3) xuansi_session_reset releases CLAIMS (pins/scripts/streams) but does NOT unload wasm instances — unload and reset are separate steps"

// hJobWait serves job_wait (B15 §4.3): park on the daemon's own event
// tap for the job's completion event. Read-only, lease-free, and it
// never dials the board — the ledger only ever holds jobs this daemon
// submitted, so there is always a tapped controller behind a waitable
// tag. timeout_ms is re-gated here (1..55000, under the 60s request
// ceiling) because the internal API is public beyond the MCP face.
func (s *Server) hJobWait(ctx context.Context, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if req.TimeoutMS < 1 || req.TimeoutMS > 55000 {
		return nil, &RPCError{Class: "api",
			Msg: fmt.Sprintf("job_wait: timeout_ms %d out of domain 1..55000 (the wait must answer inside the daemon's 60s per-request ceiling)", req.TimeoutMS)}
	}
	start := time.Now()
	db := s.supervisor(b)
	data, done, err := db.jobWait(ctx, req.Tag, time.Duration(req.TimeoutMS)*time.Millisecond)
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		return nil, err
	}
	if !done {
		return map[string]any{"done": false, "elapsed_ms": elapsed}, nil
	}
	out := map[string]any{"done": true, "elapsed_ms": elapsed}
	if ok, _ := data["ok"].(bool); ok {
		if v, present := data["value"]; present && v != nil {
			out["value"] = v
		}
	} else {
		class, _ := data["class"].(string)
		msg, _ := data["msg"].(string)
		out["error"] = map[string]any{"class": class, "msg": msg, "hint": jobCleanupHint}
	}
	return out, nil
}

// drainJobFrames ends one pending window at a job event: the buffered
// frames ride the event's data (frames/frames_dropped, the same shape
// the eval reply carries — absent when nothing was buffered). Called
// from the tap on the router goroutine.
func (db *daemonBoard) drainJobFrames(ev *replclient.Event) {
	db.jobsMu.Lock()
	if db.jobsPending > 0 {
		db.jobsPending--
	}
	db.jobsMu.Unlock()
	snap := db.jobBuf.Drain()
	if len(snap.Frames) == 0 && snap.Dropped == 0 {
		return
	}
	if ev.Data == nil {
		ev.Data = map[string]any{}
	}
	if frames := wireFrames(snap); len(frames) > 0 {
		ev.Data["frames"] = frames
	}
	if snap.Dropped > 0 {
		ev.Data["frames_dropped"] = int64(snap.Dropped)
	}
}

// wireFrames renders one snapshot as the wire shape both the eval
// reply and the job event carry (spec §5.6): [{ch, name, data_b64}].
func wireFrames(snap board.FrameSnapshot) []any {
	out := make([]any, 0, len(snap.Frames))
	for _, f := range snap.Frames {
		out = append(out, map[string]any{
			"ch":       int64(f.Ch),
			"name":     f.Name,
			"data_b64": base64.StdEncoding.EncodeToString(f.Data),
		})
	}
	return out
}

// warmController connects (or reuses) the board's warm TCP controller
// and installs the event tap + hello enrichment on it. B15 §4.1②: when
// a dial would be needed (no live controller), the reachability
// precheck runs FIRST — a board with no live discovery source is
// refused with ZERO dial (cause "source gone"), ending the 60–120s
// pathological spin an unplugged board used to cost every op.
func (s *Server) warmController(ctx context.Context, b discover.Board) (board.Controller, error) {
	db := s.supervisor(b)
	db.connectMu.Lock()
	defer db.connectMu.Unlock()
	if db.sup.Client() == nil {
		if err := s.preflightSource(b.MAC6); err != nil {
			return nil, s.boardFail(b.MAC6, err)
		}
	}
	ctl, err := db.sup.Connect(ctx)
	if err != nil {
		if errors.Is(err, board.ErrControllerSlotHeld) {
			return nil, s.boardFail(b.MAC6, err)
		}
		return nil, s.boardFail(b.MAC6, fmt.Errorf("%w: %v", errUnreachable, err))
	}
	s.tap(b.MAC6, db, ctl)
	return ctl, nil
}

// preflightSource is the board-touching ops' reachability precheck: the
// registry's CURRENT snapshot must show at least one live source for
// the board (usb and/or mdns). A warm controller short-circuits the
// caller's need for this — an established link is its own evidence the
// board is reachable. This is the fast half of fast-fail: the refusal
// carries class unreachable + cause "source gone" and dials nothing;
// the slow half (a live source whose endpoint does not answer) is the
// bounded dialLoop.
func (s *Server) preflightSource(mac6 string) error {
	if cur, ok := s.lookupExact(mac6); ok && len(cur.Sources) > 0 {
		return nil
	}
	return fmt.Errorf("%w: board %s: %w", errUnreachable, mac6, errSourceGone)
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
	// A controller identity change orphans the old link's pending jobs
	// board-side (job events are direct-delivered to the SUBMITTING
	// connection — xs_repl.c): drop their windows and the buffer.
	db.jobReset()
	ctl.OnEvent("", func(ev replclient.Event) {
		if ev.Source == "job" {
			db.drainJobFrames(&ev) // the pending window's frames ride the event (§5.6)
			db.jobDeliver(&ev)     // the job_wait ledger records the completion (§4.3)
		}
		s.broadcast(mac6, ev)
	})
	if h, err := ctl.Hello(false, "xuansi-daemon"); err == nil {
		s.reg.Enrich(mac6, h)
		s.noteBootSighting(mac6, db, h)
	}
}

// noteBootSighting (B10-T8b): every tapped hello is a boot sighting.
// A previously-seen board whose boot_epoch CHANGED rebooted while we
// were away — panic auto-reboot included, cause-agnostic by design
// (the fw's epoch is the arbiter) — so one synthetic "board" event
// (kind board.reboot) rides the same broadcast the board's own events
// take, reaching every watcher (MCP events ring included). Rules:
//   - first-ever sighting = baseline only, never an event (no past to
//     compare against);
//   - same epoch on a redial = a mere reconnect (redial ≠ reboot §13)
//     — no event, but the sighting time refreshes (last-seen moves);
//   - epoch 0 (older fw without boot_epoch) is "unknown": 0→0 never
//     fires; 0→N (fw upgrade flash, which reboots) does — honest, the
//     fw build pair in the payload says why;
//   - a changed fw build adds fw_build_prev/fw_build to the payload
//     (and a changed fw version the fw_version_prev/fw_version pair,
//     B10-T8c — both follow the same on-change-only pattern);
//   - baseline is in-memory only (v1): daemon restart = silent
//     re-baseline (the board registry refresh gives continuity; no new
//     durable state by ruling).
func (s *Server) noteBootSighting(mac6 string, db *daemonBoard, h *replclient.HelloInfo) {
	now := time.Now()
	s.mu.Lock()
	prev := db.bootSeen
	db.bootSeen = &bootSight{epoch: h.BootEpoch, fwBuild: h.FwBuild, fwVersion: h.FwVersion, at: now}
	db.lastSeen = now
	s.mu.Unlock()
	if prev == nil || prev.epoch == h.BootEpoch {
		return
	}
	data := map[string]any{
		"kind":         "board.reboot",
		"board":        mac6,
		"prev_epoch":   int64(prev.epoch),
		"new_epoch":    int64(h.BootEpoch),
		"reset_reason": h.ResetReason,
		"gap_ms":       now.Sub(prev.at).Milliseconds(),
	}
	if h.FwBuild != prev.fwBuild {
		data["fw_build_prev"] = prev.fwBuild
		data["fw_build"] = h.FwBuild
	}
	if h.FwVersion != prev.fwVersion { // B10-T8c: same pattern — pair only on change (reflash across a wave bump)
		data["fw_version_prev"] = prev.fwVersion
		data["fw_version"] = h.FwVersion
	}
	// reasons (B12-P2 T6): an epoch delta > 1 means boots whose causes
	// the single reset_reason cannot carry — the v6 fw's NVS last-8
	// ring (hello reset_history, oldest→newest, its final entry this
	// boot's reason) fills them in: the history's TAIL delta entries,
	// oldest→newest, so reasons[last] == reset_reason always. Capped
	// at 8 (the ring's depth; a shorter-than-delta ring means the same
	// thing) with reasons_partial flagging the cut. delta ≤ 1 or an
	// empty ring (pre-v6 fw): the key stays absent, honestly.
	if delta := int64(h.BootEpoch) - int64(prev.epoch); delta > 1 && len(h.ResetHistory) > 0 {
		n := delta
		if n > int64(len(h.ResetHistory)) {
			n = int64(len(h.ResetHistory))
		}
		if n > 8 {
			n = 8
		}
		reasons := make([]string, n)
		copy(reasons, h.ResetHistory[int64(len(h.ResetHistory))-n:])
		data["reasons"] = reasons
		if n < delta {
			data["reasons_partial"] = true
		}
	}
	s.logf("server: board %s rebooted (boot_epoch %d → %d, reset=%s, gap=%dms) — board.reboot event",
		mac6, prev.epoch, h.BootEpoch, h.ResetReason, now.Sub(prev.at).Milliseconds())
	s.broadcast(mac6, replclient.Event{Source: "board", Data: data})
}

// noteBoardContact records the daemon's most recent protocol crossing
// (a successful op reply) on the board's daemon-side state — the
// last_seen field's op leg. ONE helper, every direct-crossing call
// site: boardOpStream (eval/submit/op/budget.get...), withController
// (fs_write/fs_read chunked), hBoardStatus's ScriptList,
// hWatchEvents's and hCaptureStart's events.subscribe, and
// leaseTeardown's session sweeps — alongside noteBootSighting, which
// carries the hello leg (the tap hello). Read-only lookup: a board
// with no supervisor entry has never been crossed, so there is
// nothing to record.
func (s *Server) noteBoardContact(mac6 string) {
	s.mu.Lock()
	if db, ok := s.sups[mac6]; ok {
		db.lastSeen = time.Now()
	}
	s.mu.Unlock()
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
	return s.boardOpStream(ctx, b, op, args, nil, nil, nil)
}

// boardOpStream is boardOp with an optional stream-frames window
// (B12-P2 T5): a non-nil db gets the board's frame hub asserted on the
// connection for the op — the connection's sink slot is single
// (router swap semantics, set-only), and the hub is what that slot
// should carry while an eval/submit window is open, so overlapping
// windows share one fan-out instead of clobbering each other — and a
// non-nil collector rides the hub for exactly the call, RESET between
// attempts: the redial retry re-executes the op board-side, and the
// dead attempt's partial sequences must not graft onto the retry's
// frames (a nondeterministic payload would reassemble corrupt
// otherwise). A non-nil ensure runs immediately before EVERY attempt's
// op (after warmController, so any identity-change jobReset the redial
// triggered has already landed): the submit path re-arms its pending
// window there — a redial's reset must retire the old link's windows
// without swallowing THIS submit's (final-review Important-1). A
// successful reply is a protocol crossing — the board's last_seen
// advances (B12-P2 T6). §4.2 (B15): a reply timeout on a live link
// resends the SAME op once on the SAME connection for the idempotent
// faces (eval/fs/op-family — never submit/script.start, which must not
// run twice board-side); only a link-DEAD verdict tears down and
// redials, and transport failures carry the §4.1 pathology fields
// (cause + last_seen_age_ms).
func (s *Server) boardOpStream(ctx context.Context, b discover.Board, op string, args map[string]any, db *daemonBoard, col *board.FrameCollector, ensure func()) (map[string]any, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 && col != nil {
			col.Reset()
		}
		ctl, err := s.warmController(ctx, b)
		if err != nil {
			return nil, err // already boardFail-annotated (preflight/dial/arbitration)
		}
		if db != nil {
			ctl.OnStream(db.frames.Sink) // assert the hub for this window
		}
		if ensure != nil {
			ensure() // re-arm a pending-job window a redial's jobReset retired
		}
		res, err := s.opSameLink(ctx, b, ctl, op, args, idempotentOpFace(op), func() {
			if col != nil {
				col.Reset() // the resend re-executes board-side: partial frames must not graft
			}
		})
		if err == nil {
			s.noteBoardContact(b.MAC6)
			return res, nil
		}
		lastErr = err
		var oe *replclient.OpError
		if errors.As(err, &oe) {
			return nil, err // the board ANSWERED with an error — verbatim, no annotation
		}
		if !errors.Is(err, replclient.ErrDead) {
			// a timeout or ctx crossing the link — pathology fields, no redial
			return nil, s.boardFail(b.MAC6, err)
		}
		s.logf("server: board %s: op %s hit a dead link (%v) — redialing once", b.MAC6, op, err)
		dead := s.supervisor(b) // same per-board state as db when db != nil
		dead.connectMu.Lock()
		dead.sup.Close() // drop the corpse so the next Connect redials
		dead.connectMu.Unlock()
	}
	return nil, s.boardFail(b.MAC6, fmt.Errorf("board %s: %s after redial: %w", b.MAC6, op, lastErr))
}

// idempotentOpFace reports whether op is safe to RE-SEND after a
// reply timeout on a live link (§4.2): eval/fs/op-family ops are —
// they either answer or re-answer deterministically — while submit and
// script.start are NOT (a resent submit could queue the job twice; a
// resent script.start races the name registration). Those report the
// timeout honestly instead.
func idempotentOpFace(op string) bool {
	return op != "submit" && op != "script.start"
}

// opSameLink runs one ctl.Op with the §4.2 same-link retry: a reply
// timeout with the controller still live (the frame lost in transit —
// the link itself is fine) resends the SAME op once on the SAME
// connection. between (optional) runs between the attempts so callers
// can reset state the resend re-executes (the eval frame collector).
func (s *Server) opSameLink(ctx context.Context, b discover.Board, ctl board.Controller, op string, args map[string]any, retryable bool, between func()) (map[string]any, error) {
	res, err := ctl.Op(ctx, op, args)
	if err == nil || !retryable || !board.SameLinkRetry(err, ctl) {
		return res, err
	}
	s.logf("server: board %s: op %s reply timed out with the link alive — resending once on the same link (idempotent face, §4.2)", b.MAC6, op)
	if between != nil {
		between()
	}
	return ctl.Op(ctx, op, args)
}

// withController runs fn over the board's warm controller, redialing
// ONCE when the link died under it (§13: redial ≠ reboot — boardOp's
// multi-op sibling for the v5 chunked streams, B10-T1). fn must restart
// its own board-side stream on the retry: the chunked writers restart
// at off=0 (the .part reopen truncates and the board-side hash ledger
// restarts — idempotent), reads restart at off=0 trivially.
func (s *Server) withController(ctx context.Context, b discover.Board, fn func(ctl board.Controller) error) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		ctl, err := s.warmController(ctx, b)
		if err != nil {
			return err
		}
		err = fn(ctl)
		if err == nil {
			s.noteBoardContact(b.MAC6)
			return nil
		}
		lastErr = err
		var oe *replclient.OpError
		if errors.As(err, &oe) {
			return err // the board ANSWERED with an error — verbatim
		}
		if !errors.Is(err, replclient.ErrDead) {
			// a timeout or ctx crossing the link — pathology fields, no retry
			return s.boardFail(b.MAC6, err)
		}
		s.logf("server: board %s: chunked stream hit a dead link (%v) — redialing once, stream restarts at off=0", b.MAC6, err)
		db := s.supervisor(b)
		db.connectMu.Lock()
		db.sup.Close() // drop the corpse so the next Connect redials
		db.connectMu.Unlock()
	}
	return s.boardFail(b.MAC6, fmt.Errorf("board %s: chunked stream after redial: %w", b.MAC6, lastErr))
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
	err := db.sup.LeaseTeardown(tctx, mac6, holder, sessions)
	if err == nil {
		s.noteBoardContact(mac6) // the teardown sweeps' replies are protocol crossings (last_seen)
	}
	return err
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
