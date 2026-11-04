package lease

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// fakeClock is the manual Clock: Advance fires every registered timer
// whose deadline has passed (non-blocking sends; After(d≤0) fires at
// registration). All ordering is deterministic — no real waits.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := fakeTimer{at: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	if !t.at.After(c.now) {
		t.ch <- c.now
	}
	return t.ch
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if !t.at.After(c.now) {
			select {
			case t.ch <- c.now:
			default:
			}
		}
	}
}

// recorder is the TeardownFunc test double: it records every
// session.reset namespace per board in order and can be gated by the
// test to hold a teardown mid-flight.
type recorder struct {
	mu      sync.Mutex
	calls   []recCall
	enter   chan struct{} // signaled per call (non-blocking)
	blockCh chan struct{} // when non-nil, each call waits for its close
	retErr  error
}

type recCall struct {
	board    string
	holder   string
	sessions []int
}

func newRecorder() *recorder {
	return &recorder{enter: make(chan struct{}, 64)}
}

func (r *recorder) gate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blockCh = make(chan struct{})
}

func (r *recorder) ungate() {
	r.mu.Lock()
	ch := r.blockCh
	r.blockCh = nil
	r.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (r *recorder) fn(ctx context.Context, board, holder string, sessions []int) error {
	r.mu.Lock()
	r.calls = append(r.calls, recCall{board, holder, append([]int(nil), sessions...)})
	block := r.blockCh
	r.mu.Unlock()
	select {
	case r.enter <- struct{}{}:
	default:
	}
	if block != nil {
		<-block // gated by the test
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retErr
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recorder) sessions() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return nil
	}
	return r.calls[0].sessions
}

func (r *recorder) failWith(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retErr = err
}

// waitFor spins (Gosched, never sleep) until cond holds or the bound
// panics — deterministic single-machine scheduling.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 5_000_000; i++ {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("waitFor: condition not reached")
}

// newTestManager wires a Manager around a fake clock + recorder.
func newTestManager(ttl time.Duration, rec *recorder) (*Manager, *fakeClock) {
	fc := newFakeClock()
	m := NewManager(Policy{
		GraceTTL: ttl,
		Clock:    fc,
		OnExpiry: rec.fn,
		Log: func(format string, args ...any) {
			fmt.Printf("    [lease-log] "+format+"\n", args...)
		},
	})
	return m, fc
}

// logCapture builds a Policy.Log that records lines.
func logCapture() (fn func(string, ...any), get func() []string) {
	var mu sync.Mutex
	var lines []string
	fn = func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(format, args...))
	}
	get = func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), lines...)
	}
	return fn, get
}

func TestAcquireGrantsFreshSessionIDs(t *testing.T) {
	m, fc := newTestManager(time.Minute, newRecorder())
	defer m.Close()

	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	s1, ok := m.Session("d75884")
	if !ok || s1 <= 0 {
		t.Fatalf("session after acquire = %d,%v — want fresh >0", s1, ok)
	}
	base := fc.Now().Unix()
	if int64(s1) <= base || int64(s1) > base+1_000_000 {
		t.Fatalf("session %d not based on daemon epoch %d", s1, base)
	}
	// same holder re-acquire: idempotent, same session
	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatalf("idempotent re-acquire: %v", err)
	}
	if s2, _ := m.Session("d75884"); s2 != s1 {
		t.Fatalf("re-acquire changed session %d → %d", s1, s2)
	}
	// second board: fresh, distinct session
	if err := m.Acquire("aa0001", "agent-a"); err != nil {
		t.Fatalf("second board acquire: %v", err)
	}
	if s3, _ := m.Session("aa0001"); s3 == s1 || s3 == 0 {
		t.Fatalf("second board session %d must be fresh (≠%d, ≠0)", s3, s1)
	}
	// after release the next acquire NEVER reuses the id
	if err := m.Release("d75884", "agent-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := m.Acquire("d75884", "agent-b"); err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	if s4, _ := m.Session("d75884"); s4 == s1 {
		t.Fatalf("session id reused across leases (%d)", s4)
	}
}

func TestAcquireExclusiveThirdPartyErrHeld(t *testing.T) {
	m, _ := newTestManager(time.Minute, newRecorder())
	defer m.Close()
	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatal(err)
	}
	err := m.Acquire("d75884", "agent-b")
	var eh *ErrHeld
	if !errors.As(err, &eh) {
		t.Fatalf("want *ErrHeld, got %v", err)
	}
	if eh.Holder != "agent-a" || eh.Board != "d75884" || eh.Since.IsZero() {
		t.Fatalf("ErrHeld fields: %+v", eh)
	}
	if eh.GraceExpiring() {
		t.Fatalf("live holder must not be marked grace-expiring: %+v", eh)
	}
}

func TestOnAgentGoneGraceDeadlineNotExtendedOnDoubleDisconnect(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatal(err)
	}
	fc.Advance(2 * time.Second)
	m.OnAgentGone("agent-a")
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].GraceDeadline.IsZero() {
		t.Fatalf("no grace deadline after agent-gone: %+v", snap)
	}
	want := snap[0].GraceDeadline
	// double disconnect: same tick, later tick — deadline must not move
	m.OnAgentGone("agent-a")
	fc.Advance(3 * time.Second)
	m.OnAgentGone("agent-a")
	snap = m.Snapshot()
	if len(snap) != 1 || !snap[0].GraceDeadline.Equal(want) {
		t.Fatalf("double-disconnect extended deadline: want %v got %+v", want, snap)
	}
	// expiry fires at the ORIGINAL deadline, exactly once
	fc.Advance(7 * time.Second) // 2+3+7 = 12s — exactly the deadline: due
	waitFor(t, func() bool { return rec.count() == 1 })
	if got := rec.sessions(); len(got) != 2 || got[0] <= 0 || got[1] != 0 {
		t.Fatalf("teardown namespaces = %v, want [holderSession, 0]", got)
	}
	waitFor(t, func() bool { return len(m.Snapshot()) == 0 })
}

func TestGracePeriodThirdPartyAcquireErrHeldGraceExpiring(t *testing.T) {
	m, _ := newTestManager(10*time.Second, newRecorder())
	defer m.Close()
	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatal(err)
	}
	m.OnAgentGone("agent-a")
	err := m.Acquire("d75884", "agent-b")
	var eh *ErrHeld
	if !errors.As(err, &eh) {
		t.Fatalf("grace acquire must refuse with *ErrHeld, got %v", err)
	}
	if !eh.GraceExpiring() {
		t.Fatalf("grace refusal must carry the grace-expiring marker: %+v", eh)
	}
	if eh.GraceExpiry.IsZero() {
		t.Fatalf("grace refusal must expose the deadline: %+v", eh)
	}
}

func TestAdoptWithinTTLKeepsBoardState(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatal(err)
	}
	sess, _ := m.Session("d75884")
	m.OnAgentGone("agent-a")
	fc.Advance(10*time.Second - time.Millisecond) // 1ms before the deadline
	if !m.Adopt("agent-a") {
		t.Fatal("adopt 1ms before deadline must succeed")
	}
	snap := m.Snapshot()
	if len(snap) != 1 || !snap[0].GraceDeadline.IsZero() {
		t.Fatalf("adopt must return the lease to live: %+v", snap)
	}
	if s2, _ := m.Session("d75884"); s2 != sess {
		t.Fatalf("adopt must keep the session namespace (%d → %d)", sess, s2)
	}
	if rec.count() != 0 {
		t.Fatalf("adopt must NOT touch the board (teardown calls=%d)", rec.count())
	}
	// repeated adopts are idempotent
	if !m.Adopt("agent-a") {
		t.Fatal("second adopt (already live) must report true")
	}
	if rec.count() != 0 {
		t.Fatalf("idempotent adopt ran teardown (%d)", rec.count())
	}
}

func TestAdoptAfterTTLFailsAndTeardownRanExactlyOnce(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatal(err)
	}
	m.OnAgentGone("agent-a")
	fc.Advance(10*time.Second + time.Millisecond) // 1ms past the deadline
	if m.Adopt("agent-a") {
		t.Fatal("adopt after deadline must fail")
	}
	waitFor(t, func() bool { return rec.count() == 1 })
	waitFor(t, func() bool { return len(m.Snapshot()) == 0 })
	// board is free for a third party now
	if err := m.Acquire("d75884", "agent-b"); err != nil {
		t.Fatalf("board must be free after teardown: %v", err)
	}
	fc.Advance(time.Second) // any re-armed timers fire; nothing more is due
	waitFor(t, func() bool { return m.idleNoGraceNoTearing() })
	if rec.count() != 1 {
		t.Fatalf("teardown ran %d times — exactly-once violated", rec.count())
	}
}

func TestAdoptAtExactDeadlineIsExpired(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	m.OnAgentGone("agent-a")
	fc.Advance(10 * time.Second) // exactly at the deadline
	if m.Adopt("agent-a") {
		t.Fatal("adopt AT the deadline must fail (expiry owns the boundary)")
	}
	waitFor(t, func() bool { return rec.count() == 1 })
}

func TestReaperFiresWithoutAPICall(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	m.OnAgentGone("agent-a")
	fc.Advance(12 * time.Second)
	waitFor(t, func() bool { return rec.count() == 1 && len(m.Snapshot()) == 0 })
	if _, ok := m.Session("d75884"); ok {
		t.Fatal("session must be gone after reaper expiry")
	}
}

func TestExpiryExactlyOnceLazyAndReaper(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	m.OnAgentGone("agent-a")
	fc.Advance(11 * time.Second)
	// The LAZY path: a third-party Acquire lands while the reaper may
	// already be mid-teardown (its fired timer won the race) — the lazy
	// path then observes ErrTeardownInProgress and retries, per the
	// busy-window contract; whichever path claims it, the board action
	// runs exactly once.
	acquired := false
	for i := 0; i < 1_000_000 && !acquired; i++ {
		switch err := m.Acquire("d75884", "agent-b"); {
		case err == nil:
			acquired = true
		case errors.Is(err, ErrTeardownInProgress):
			waitForTick()
		default:
			t.Fatalf("lazy-expiry acquire: %v", err)
		}
	}
	if !acquired {
		t.Fatal("lazy-expiry acquire never succeeded")
	}
	fc.Advance(time.Second)
	waitFor(t, func() bool { return m.idleNoGraceNoTearing() })
	if rec.count() != 1 {
		t.Fatalf("teardown calls=%d — lazy+reaper double fire", rec.count())
	}
}

func TestPolicyZeroValueStillTearsDownOnExpiry(t *testing.T) {
	// The owner ruling pins reset-on-expiry: a zero-value Policy (only
	// the test seams filled — no GraceTTL, no flags) must still run the
	// board teardown when the TTL passes. The knob does not exist.
	rec := newRecorder()
	fc := newFakeClock()
	m := NewManager(Policy{Clock: fc, OnExpiry: rec.fn})
	defer m.Close()
	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatal(err)
	}
	// GraceTTL defaulted to DefaultGraceTTL (300s)
	snap := m.Snapshot()
	m.OnAgentGone("agent-a")
	snap = m.Snapshot()
	if len(snap) != 1 || !snap[0].GraceDeadline.Equal(fc.Now().Add(DefaultGraceTTL)) {
		t.Fatalf("zero-value Policy must default GraceTTL to %v: %+v", DefaultGraceTTL, snap)
	}
	fc.Advance(DefaultGraceTTL + time.Second)
	waitFor(t, func() bool { return rec.count() == 1 && len(m.Snapshot()) == 0 })
	if got := rec.sessions(); len(got) != 2 || got[0] <= 0 || got[1] != 0 {
		t.Fatalf("zero-value Policy teardown namespaces = %v, want [holderSession, 0]", got)
	}
}

func TestReleaseWrongHolderRefused(t *testing.T) {
	rec := newRecorder()
	m, _ := newTestManager(time.Minute, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	if err := m.Release("d75884", "agent-b"); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("wrong-holder release = %v, want ErrNotHeld", err)
	}
	if err := m.Release("aa0001", "agent-a"); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("unknown-board release = %v, want ErrNotHeld", err)
	}
	if rec.count() != 0 {
		t.Fatalf("refused release must not tear down (calls=%d)", rec.count())
	}
}

func TestManagerCloseStopsReaper(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	m.Acquire("d75884", "agent-a")
	m.OnAgentGone("agent-a")
	fc.Advance(2 * time.Second) // not yet due
	m.Close()
	fc.Advance(20 * time.Second) // would be due — but the Manager is closed
	if rec.count() != 0 {
		t.Fatalf("closed manager ran teardown (calls=%d)", rec.count())
	}
}

// idleNoGraceNoTearing reports whether no lease is pending work (grace
// counting or tearing): used by tests to observe the reaper having
// settled. Test-only accessor.
func (m *Manager) idleNoGraceNoTearing() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.leases {
		if e.state == stateGrace || e.state == stateTearing {
			return false
		}
	}
	return true
}
