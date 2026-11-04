// Package lease — see doc.go for the package contract.
//
// This file: the Manager (exclusive per-board leases, grace on agent
// disconnect, adopt-within-TTL) and its time seam.
package lease

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Clock is the time seam: Now reads the current instant, After returns
// a channel receiving once d has elapsed (d ≤ 0 fires immediately —
// mirroring time.After). Production uses RealClock; unit tests inject a
// manual clock so TTL boundaries are exercised at sub-millisecond
// precision with zero real waiting.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// RealClock is the production Clock over the time package.
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// DefaultGraceTTL is the owner's 2026-11-04 ruling default: an agent
// that drops gets 300s of grace before the daemon tears the board side
// down (HIL runs use 10s).
const DefaultGraceTTL = 300 * time.Second

// ErrNotHeld: Release/Session asked about a board+holder pair that does
// not match the current lease (wrong holder, no lease, or already gone).
var ErrNotHeld = errors.New("lease: board not held by that holder")

// ErrTeardownInProgress: the board's lease is mid-teardown (reset ops on
// the wire). A would-be acquirer retries a moment later — the board
// becomes free only after the teardown completed, which is exactly the
// ordering the ruling demands (safe state before any new holder).
var ErrTeardownInProgress = errors.New("lease: lease teardown in progress")

// ErrHeld reports an Acquire refusal: Board is held by Holder since
// Since. When the holder's agent has dropped and the lease is counting
// down its grace TTL, GraceExpiry carries the deadline and
// GraceExpiring() is true ("grace-expiring" marker, ruling 2026-11-04):
// the caller learns the board MAY free itself soon, but may not take it
// over now.
type ErrHeld struct {
	Board       string
	Holder      string
	Since       time.Time
	GraceExpiry time.Time // zero while the holder is live
}

// GraceExpiring reports whether the current holder's agent is gone and
// the lease is counting down to expiry.
func (e *ErrHeld) GraceExpiring() bool { return !e.GraceExpiry.IsZero() }

func (e *ErrHeld) Error() string {
	s := fmt.Sprintf("lease: board %s held by %s since %s",
		e.Board, e.Holder, e.Since.Format(time.RFC3339))
	if e.GraceExpiring() {
		s += fmt.Sprintf(" — holder gone, grace-expiring at %s", e.GraceExpiry.Format(time.RFC3339))
	}
	return s
}

// TeardownFunc executes the board-side teardown the Manager requests at
// lease end: for every session namespace in sessions (the holder's own
// numeric session first, then the eval namespace 0 — see session.go),
// session.reset{session} on that board. The Supervisor implements it
// (board.Supervisor.LeaseTeardown); unit tests record invocations. err
// is surfaced in the trace but never blocks the host-side release: a
// board that cannot be reached stays torn-down-pending and is reported
// by the next resync instead of pinning the lease forever.
type TeardownFunc func(ctx context.Context, board, holder string, sessions []int) error

// Policy parameterizes a Manager. NewManager fills the defaults:
// GraceTTL=DefaultGraceTTL (ruling) and Clock=RealClock. The expiry
// teardown is UNCONDITIONAL — the owner's 2026-11-04 ruling pins
// reset-on-expiry (daemon 主動 session.reset＋lease 釋放), so batch 5
// exposes no knob for it; a zero-value Policy{Clock:…, OnExpiry:…}
// tears down on expiry exactly like a fully-specified one.
type Policy struct {
	GraceTTL time.Duration                    // grace window after agent-gone (default 300s)
	Clock    Clock                            // time seam (default RealClock)
	Log      func(format string, args ...any) // ring-log trace seam
	OnExpiry TeardownFunc                     // board-side teardown executor
}

// leaseState is the lifecycle of one lease entry.
type leaseState int

const (
	stateLive    leaseState = iota // holder's agent connected
	stateGrace                     // agent gone, TTL counting down (board state kept)
	stateTearing                   // teardown claimed the entry (reset ops may be on the wire)
)

// leaseEntry is one board's lease. Field access is under Manager.mu.
type leaseEntry struct {
	board    string
	holder   string
	session  int
	since    time.Time
	state    leaseState
	deadline time.Time // grace deadline; zero unless state == stateGrace
}

// Manager grants exclusive per-board leases and drives the grace-TTL
// teardown (owner ruling 2026-11-04):
//
//   - Acquire is exclusive: a second holder gets ErrHeld (grace-expiring
//     marker when the lease is counting down).
//   - OnAgentGone moves every live lease of that holder into grace —
//     board-side state is KEPT (scripts run on, pins stay claimed).
//   - Adopt(holder) within the TTL re-adopts without touching the board;
//     at/after the deadline the Manager itself runs the teardown
//     (session.reset of the holder's namespace + the eval namespace 0)
//     and releases the lease — exactly once per lease.
//   - Release runs the same teardown deliberately (design doc §teardown:
//     agent-drop, TTL and release are all teardown paths).
//
// Expiry is driven from two converging paths — the background reaper
// and lazy expiry inside every API call — both funneled through
// expireDueLocked's compare-and-set, so the board action runs exactly
// once per lease and the board only becomes free after it finished.
type Manager struct {
	policy Policy
	clock  Clock
	logf   func(format string, args ...any)

	mu     sync.Mutex
	leases map[string]*leaseEntry // board mac6 → lease
	alloc  *sessionAlloc

	wake   chan struct{} // nudges the reaper after deadline changes
	closed chan struct{}
	done   chan struct{} // reaper exited

	opCtx    context.Context // teardown callbacks run under it
	opCancel context.CancelFunc
}

// NewManager builds a Manager around p, filling Policy defaults, and
// starts the expiry reaper. Close stops it.
func NewManager(p Policy) *Manager {
	if p.GraceTTL <= 0 {
		p.GraceTTL = DefaultGraceTTL
	}
	if p.Clock == nil {
		p.Clock = RealClock{}
	}
	m := &Manager{
		policy: p,
		clock:  p.Clock,
		leases: map[string]*leaseEntry{},
		alloc:  newSessionAlloc(p.Clock),
		wake:   make(chan struct{}, 1),
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	m.opCtx, m.opCancel = context.WithCancel(context.Background())
	if p.Log != nil {
		m.logf = p.Log
	} else {
		m.logf = func(string, ...any) {}
	}
	go m.reap()
	return m
}

// Acquire grants holder the exclusive lease on board, allocating a
// fresh session namespace for it (see session.go). Re-acquiring by the
// same holder is idempotent; re-entering by the GRACE holder within the
// TTL adopts (board state kept). A board held by someone else refuses
// with *ErrHeld; a board mid-teardown refuses with ErrTeardownInProgress.
func (m *Manager) Acquire(board, holder string) error {
	for {
		m.mu.Lock()
		if acts := m.expireDueLocked(m.clock.Now()); len(acts) > 0 {
			m.mu.Unlock()
			m.runTeardown(acts)
			continue
		}
		e, ok := m.leases[board]
		if !ok {
			sess, err := m.alloc.alloc()
			if err != nil {
				m.mu.Unlock()
				return fmt.Errorf("lease: acquire %s: %w", board, err)
			}
			m.leases[board] = &leaseEntry{
				board: board, holder: holder, session: sess, since: m.clock.Now(), state: stateLive,
			}
			m.mu.Unlock()
			m.logf("lease: grant board=%s holder=%s session=%d", board, holder, sess)
			return nil
		}
		if e.holder == holder {
			if e.state == stateGrace {
				if m.clock.Now().Before(e.deadline) {
					// the grace holder walked back in through the front
					// door — same adopt semantics as Adopt
					e.state = stateLive
					e.deadline = time.Time{}
					m.mu.Unlock()
					m.logf("lease: adopt board=%s holder=%s session=%d (board state kept)", board, holder, e.session)
					return nil
				}
				m.mu.Unlock()
				continue // due — the loop's expire path claims it
			}
			m.mu.Unlock()
			return nil // live + same holder: idempotent
		}
		if e.state == stateTearing {
			m.mu.Unlock()
			return fmt.Errorf("lease: acquire %s by %s: %w", board, holder, ErrTeardownInProgress)
		}
		err := &ErrHeld{Board: board, Holder: e.holder, Since: e.since}
		if e.state == stateGrace {
			err.GraceExpiry = e.deadline
		}
		m.mu.Unlock()
		return err
	}
}

// Release ends holder's lease on board deliberately: the same board-side
// teardown as TTL expiry runs (holder namespace + eval namespace 0),
// then the board is free. Wrong holder or unknown board → ErrNotHeld.
func (m *Manager) Release(board, holder string) error {
	for {
		m.mu.Lock()
		if acts := m.expireDueLocked(m.clock.Now()); len(acts) > 0 {
			m.mu.Unlock()
			m.runTeardown(acts)
			continue
		}
		e, ok := m.leases[board]
		if !ok || e.holder != holder {
			m.mu.Unlock()
			return fmt.Errorf("lease: release %s by %s: %w", board, holder, ErrNotHeld)
		}
		if e.state == stateTearing {
			m.mu.Unlock()
			return fmt.Errorf("lease: release %s by %s: %w", board, holder, ErrTeardownInProgress)
		}
		e.state = stateTearing // claim for the deliberate teardown
		act := teardownAction{entry: e, reason: "released"}
		m.mu.Unlock()
		m.runTeardown([]teardownAction{act})
		return nil
	}
}

// OnAgentGone moves every live lease held by holder into grace: the
// deadline is now+GraceTTL, board-side state is kept. Idempotent — a
// second call (double disconnect) never extends the first deadline.
func (m *Manager) OnAgentGone(holder string) {
	m.mu.Lock()
	now := m.clock.Now()
	n := 0
	var firstBoard string
	var firstDL string
	for _, e := range m.leases {
		if e.holder == holder && e.state == stateLive {
			e.state = stateGrace
			e.deadline = now.Add(m.policy.GraceTTL)
			if n == 0 || e.board < firstBoard {
				firstBoard = e.board
				firstDL = e.deadline.Format(time.RFC3339) // read under the lock: Adopt may rewrite it the moment we let go
			}
			n++
		}
	}
	if n > 0 {
		m.wakeLocked()
	}
	m.mu.Unlock()
	if n > 0 {
		m.logf("lease: agent-gone holder=%s boards=%d grace_deadline=%s ttl=%s (board state kept)",
			holder, n, firstDL, m.policy.GraceTTL)
	}
}

// Adopt takes the holder back after a reconnect: every grace lease of
// holder whose deadline has not passed returns to live WITHOUT touching
// the board (scripts keep running, pins stay claimed — the ruling's
// 接手不重置). It reports whether holder now holds at least one live
// lease (an already-live lease counts — repeated Adopts are idempotent).
// After the deadline the report is false: the lease is gone and the
// holder must Acquire fresh.
func (m *Manager) Adopt(holder string) bool {
	for {
		m.mu.Lock()
		if acts := m.expireDueLocked(m.clock.Now()); len(acts) > 0 {
			m.mu.Unlock()
			m.runTeardown(acts)
			continue
		}
		live := false
		adopted := 0
		for _, e := range m.leases {
			if e.holder != holder {
				continue
			}
			if e.state == stateLive {
				live = true
			}
			if e.state == stateGrace && m.clock.Now().Before(e.deadline) {
				e.state = stateLive
				e.deadline = time.Time{}
				live = true
				adopted++
				m.logf("lease: adopt board=%s holder=%s session=%d (board state kept — no reset)",
					e.board, holder, e.session)
			}
		}
		m.mu.Unlock()
		_ = adopted
		return live
	}
}

// Session returns the numeric board session namespace of the board's
// current lease (live or grace). ok=false when the board is free.
func (m *Manager) Session(board string) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.leases[board]
	if !ok || e.state == stateTearing {
		return 0, false
	}
	return e.session, true
}

// LeaseState is one lease as observed by Snapshot.
type LeaseState struct {
	Board         string
	Holder        string
	Session       int
	Since         time.Time
	GraceDeadline time.Time // zero when the holder is live
}

// Snapshot lists all leases sorted by board, first expiring any grace
// lease whose TTL has passed (the same lazy-expiry path every API call
// takes, so the view never shows a lease the reaper is due to tear down).
func (m *Manager) Snapshot() []LeaseState {
	for {
		m.mu.Lock()
		if acts := m.expireDueLocked(m.clock.Now()); len(acts) > 0 {
			m.mu.Unlock()
			m.runTeardown(acts)
			continue
		}
		out := make([]LeaseState, 0, len(m.leases))
		for _, e := range m.leases {
			ls := LeaseState{Board: e.board, Holder: e.holder, Session: e.session, Since: e.since}
			if e.state == stateGrace {
				ls.GraceDeadline = e.deadline
			}
			out = append(out, ls)
		}
		m.mu.Unlock()
		sort.Slice(out, func(i, j int) bool { return out[i].Board < out[j].Board })
		return out
	}
}

// Close stops the reaper and cancels in-flight teardown callbacks. A
// Manager is not usable after Close.
func (m *Manager) Close() {
	select {
	case <-m.closed:
		return
	default:
	}
	close(m.closed)
	m.opCancel()
	m.wakeReaper()
	<-m.done
}

// wakeLocked nudges the reaper (caller holds mu).
func (m *Manager) wakeLocked() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// wakeReaper nudges the reaper (no lock held required).
func (m *Manager) wakeReaper() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// reap is the expiry loop: sleep until the earliest grace deadline (or a
// nudge, or Close), then expire what is due. Stale timers are harmless —
// a spurious wake just recomputes. After arming the wake-up timer the
// loop pumps once more: a clock that jumped past the deadline between
// the computation and the arming would otherwise arm a stale timer
// (fake clocks freeze; real clocks merely drift) — the extra pump
// catches the jump deterministically.
func (m *Manager) reap() {
	defer close(m.done)
	pump := func() {
		m.mu.Lock()
		acts := m.expireDueLocked(m.clock.Now())
		m.mu.Unlock()
		if len(acts) > 0 {
			m.runTeardown(acts)
		}
	}
	for {
		pump()

		m.mu.Lock()
		now := m.clock.Now()
		next := time.Duration(1<<62 - 1)
		armed := false
		for _, e := range m.leases {
			if e.state == stateGrace && now.Before(e.deadline) {
				if d := e.deadline.Sub(now); d < next {
					next = d
					armed = true
				}
			}
		}
		m.mu.Unlock()

		var timer <-chan time.Time
		if armed {
			timer = m.clock.After(next)
		}
		pump() // clock may have jumped past the deadline mid-arming

		select {
		case <-m.closed:
			return
		case <-m.wake:
		case <-timer:
		}
	}
}
