package lease

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestTeardownNamespacesIncludeEvalSweep(t *testing.T) {
	got := teardownNamespaces(1792000000)
	if len(got) != 2 || got[0] != 1792000000 || got[1] != 0 {
		t.Fatalf("namespaces = %v, want [1792000000 0] (holder namespace, then the eval sweep)", got)
	}
}

func TestSessionAllocFreshPositiveAndEpochBased(t *testing.T) {
	fc := newFakeClock()
	a := newSessionAlloc(fc)
	base := fc.Now().Unix()
	seen := map[int]bool{}
	for i := 0; i < 100; i++ {
		s, err := a.alloc()
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		if s <= 0 {
			t.Fatalf("alloc %d = %d — must be >0 (0 is the eval namespace)", i, s)
		}
		if seen[s] {
			t.Fatalf("session %d reused", s)
		}
		seen[s] = true
		if int64(s) <= base || int64(s) > base+1000 {
			t.Fatalf("session %d not epoch-based (%d)", s, base)
		}
	}
}

func TestExpiryRunsResetsHolderThenEvalSweep(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	sess, _ := m.Session("d75884")
	m.OnAgentGone("agent-a")
	fc.Advance(11 * time.Second)
	waitFor(t, func() bool { return rec.count() == 1 })
	c := rec.callsSnapshot()[0]
	if c.board != "d75884" || c.holder != "agent-a" {
		t.Fatalf("action identity: %+v", c)
	}
	if len(c.sessions) != 2 || c.sessions[0] != sess || c.sessions[1] != 0 {
		t.Fatalf("expiry sessions = %v, want [%d 0]", c.sessions, sess)
	}
}

func TestExpiryTraceLine(t *testing.T) {
	logf, get := logCapture()
	fc := newFakeClock()
	rec := newRecorder()
	m := NewManager(Policy{GraceTTL: 10 * time.Second, Clock: fc, OnExpiry: rec.fn, Log: logf})
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	sess, _ := m.Session("d75884")
	m.OnAgentGone("agent-a")
	fc.Advance(11 * time.Second)
	waitFor(t, func() bool { return len(m.Snapshot()) == 0 })
	var trace string
	for _, l := range get() {
		if strings.Contains(l, "teardown reason=expired") {
			trace = l
		}
	}
	if trace == "" {
		t.Fatalf("no expired teardown trace in %v", get())
	}
	for _, want := range []string{
		"board=d75884", "holder=agent-a",
		fmt.Sprintf("session=%d", sess),
		fmt.Sprintf("sessions=[%d 0]", sess),
	} {
		if !strings.Contains(trace, want) {
			t.Fatalf("trace %q missing %q", trace, want)
		}
	}
	if !strings.Contains(trace, "err=<nil>") {
		t.Fatalf("trace must record teardown outcome: %q", trace)
	}
}

func TestReleaseRunsSameTeardownAsExpiry(t *testing.T) {
	rec := newRecorder()
	m, _ := newTestManager(time.Minute, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	sess, _ := m.Session("d75884")
	if err := m.Release("d75884", "agent-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if rec.count() != 1 {
		t.Fatalf("release must run the board teardown (calls=%d)", rec.count())
	}
	c := rec.callsSnapshot()[0]
	if len(c.sessions) != 2 || c.sessions[0] != sess || c.sessions[1] != 0 {
		t.Fatalf("release sessions = %v, want [%d 0]", c.sessions, sess)
	}
	if _, ok := m.Session("d75884"); ok {
		t.Fatal("lease must be gone after release")
	}
}

func TestTeardownErrorStillReleasesLease(t *testing.T) {
	rec := newRecorder()
	rec.failWith(errors.New("board unreachable"))
	logf, get := logCapture()
	fc := newFakeClock()
	m := NewManager(Policy{GraceTTL: 10 * time.Second, Clock: fc, OnExpiry: rec.fn, Log: logf})
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	m.OnAgentGone("agent-a")
	fc.Advance(11 * time.Second)
	waitFor(t, func() bool { return len(m.Snapshot()) == 0 })
	if err := m.Acquire("d75884", "agent-b"); err != nil {
		t.Fatalf("a failed teardown must not pin the lease: %v", err)
	}
	found := false
	for _, l := range get() {
		if strings.Contains(l, "teardown reason=expired") && strings.Contains(l, "board unreachable") {
			found = true
		}
	}
	if !found {
		t.Fatalf("teardown failure not surfaced in the trace: %v", get())
	}
}

func TestAcquireDuringTeardownIsBusyThenFree(t *testing.T) {
	rec := newRecorder()
	rec.gate() // hold the teardown mid-flight
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	m.OnAgentGone("agent-a")
	fc.Advance(11 * time.Second)
	waitFor(t, func() bool { return rec.count() == 1 }) // teardown entered, gated
	err := m.Acquire("d75884", "agent-b")
	if !errors.Is(err, ErrTeardownInProgress) {
		t.Fatalf("acquire during teardown = %v, want ErrTeardownInProgress", err)
	}
	rec.ungate()
	// after the teardown completes the board is free — ordering proof
	// (free only AFTER the board action finished)
	tryAcquire := func() bool {
		for i := 0; i < 1_000_000; i++ {
			if err := m.Acquire("d75884", "agent-b"); err == nil {
				return true
			} else if !errors.Is(err, ErrTeardownInProgress) {
				t.Fatalf("unexpected acquire error: %v", err)
				return false
			}
			waitForTick()
		}
		return false
	}
	if !tryAcquire() {
		t.Fatal("board never became free after teardown")
	}
}

// waitForTick yields once without sleeping (busy-wait companion).
func waitForTick() { time.Sleep(0) }

func TestAcquireDuringGraceBySameHolderAdopts(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	sess, _ := m.Session("d75884")
	m.OnAgentGone("agent-a")
	fc.Advance(5 * time.Second)
	// the grace holder re-entering via Acquire = adopt (same reconnection
	// semantics through the front door)
	if err := m.Acquire("d75884", "agent-a"); err != nil {
		t.Fatalf("grace-holder re-acquire: %v", err)
	}
	if s2, _ := m.Session("d75884"); s2 != sess {
		t.Fatalf("adopt via acquire changed session %d → %d", sess, s2)
	}
	if rec.count() != 0 {
		t.Fatalf("adopt via acquire touched the board (calls=%d)", rec.count())
	}
}

func TestSessionGoneAfterTeardown(t *testing.T) {
	rec := newRecorder()
	m, fc := newTestManager(10*time.Second, rec)
	defer m.Close()
	m.Acquire("d75884", "agent-a")
	m.OnAgentGone("agent-a")
	fc.Advance(11 * time.Second)
	waitFor(t, func() bool { return rec.count() == 1 && len(m.Snapshot()) == 0 })
	if _, ok := m.Session("d75884"); ok {
		t.Fatal("Session must report gone after expiry teardown")
	}
}

// callsSnapshot copies the recorded calls.
func (r *recorder) callsSnapshot() []recCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recCall(nil), r.calls...)
}

var _ = context.Background
