// session.go — the numeric board session namespaces the daemon hands
// out and tears down (fw §4: session is a numeric namespace;
// script.start{session?:int=1}, session.reset{session:int} releases
// that session's scripts AND pins).
//
// THE EVAL RULING (resolved on the live board, b310378, 2026-11-05 —
// evidence results/2026-11-05-b5-t10-teardown.log): pins claimed
// through bare eval are session-scoped too, in the SHARED namespace 0
// (fw xs_script.c: "console eval 屬 session 0"; owner name "_eval").
// session.reset{session:N} with N≠0 does NOT release them;
// session.reset{session:0} does. Therefore:
//
//   - every lease gets its own fresh session id (>0, never reused
//     within a daemon life, based on the daemon's start epoch — a
//     restarted daemon never collides with prior-era orphans it chose
//     not to kill);
//   - agent work that claims pins should run under script.start with
//     the agent's session id (the daemon's own API enforces this where
//     it can);
//   - lease teardown resets BOTH the holder's namespace and namespace 0
//     (teardownNamespaces). The sweep of 0 is safe because the lease is
//     exclusive and observers cannot eval (ro op matrix): at teardown
//     time namespace 0 holds only the outgoing holder's eval residue —
//     or older orphans, which the sweep then legitimately clears as the
//     daemon owns the board's safe state. This is what makes
//     "teardown ends with pins in safe state" true even for eval claims.
package lease

import (
	"fmt"
	"math"
)

// teardownNamespaces is the ordered session.reset list for ending the
// lease on session: the holder's own namespace first (stops its
// scripts, releases its pins), then namespace 0 (the eval sweep — see
// the file comment). Order is pinned: holder namespace before the
// shared sweep, so the trace reads cause→sweep.
func teardownNamespaces(session int) []int {
	return []int{session, 0}
}

// sessionAlloc hands out the fresh-per-lease session ids: base+1,
// base+2, … where base is the daemon's start epoch (Clock.Now().Unix()
// — positive by construction, int32-safe until 2038, distinct across
// daemon restarts). Zero is never handed out (it is the eval namespace).
type sessionAlloc struct {
	next int64
}

// newSessionAlloc bases the allocator on the clock's current epoch
// second.
func newSessionAlloc(c Clock) *sessionAlloc {
	base := c.Now().Unix()
	if base < 1 {
		base = 1 // clocks without an epoch (test fixtures): any stable base
	}
	return &sessionAlloc{next: base}
}

// alloc returns the next session id, or an error when the int32 session
// arg domain (fw rejects out-of-range) is exhausted — a practically
// unreachable 68-year corner, refused honestly rather than wrapped.
func (a *sessionAlloc) alloc() (int, error) {
	if a.next >= math.MaxInt32 {
		return 0, fmt.Errorf("session id space exhausted (int32 domain)")
	}
	a.next++
	return int(a.next), nil
}
