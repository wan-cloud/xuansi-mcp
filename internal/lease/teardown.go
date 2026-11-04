// teardown.go — the expiry engine: which leases are due, the
// exactly-once claim of a due entry, the board-side action, and the
// ring-log trace lines the ruling demands (逾期動作＝session.reset＋
// lease 釋放＋ring log 痕跡).
package lease

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// teardownAction is one due lease claimed for teardown (state already
// CAS'd to stateTearing — the exactly-once claim; whoever holds the
// action runs it and removes the entry).
type teardownAction struct {
	entry  *leaseEntry
	reason string // "expired" | "released"
}

// teardownTrace renders the pinned ring-log line. err==nil reads
// "err=<nil>"; an error is included verbatim — a failed board action
// still releases the lease host-side (the residue becomes the next
// resync's honest report, never a stuck lease).
func teardownTrace(reason, board, holder string, session int, sessions []int, err error, took time.Duration) string {
	return fmt.Sprintf("lease: teardown reason=%s board=%s holder=%s session=%d sessions=%v err=%v took=%s",
		reason, board, holder, session, sessions, err, took.Round(time.Millisecond))
}

// runTeardown executes actions outside Manager.mu: for each, the
// board-side TeardownFunc (unconditionally — the ruling pins
// reset-on-expiry and release is a deliberate teardown path of the same
// shape; a caller that wants pure host-side leasing simply leaves
// OnExpiry nil), then the entry is removed and the reaper nudged. A
// TeardownFunc error never resurrects the lease.
func (m *Manager) runTeardown(acts []teardownAction) {
	for _, act := range acts {
		e := act.entry
		sessions := teardownNamespaces(e.session)
		start := m.clock.Now()
		var err error
		if fn := m.policy.OnExpiry; fn != nil {
			err = fn(m.opCtx, e.board, e.holder, sessions)
		}
		m.logf("%s", teardownTrace(act.reason, e.board, e.holder, e.session, sessions, err, m.clock.Now().Sub(start)))

		m.mu.Lock()
		if cur, ok := m.leases[e.board]; ok && cur == e {
			delete(m.leases, e.board)
		}
		m.mu.Unlock()
		m.wakeReaper() // deadline set changed
	}
}

// expireDueLocked claims every grace entry whose deadline has passed
// (now ≥ deadline) by CAS to stateTearing and returns the actions
// sorted by board. Callers run them with the lock released. A lease
// adopted at now < deadline is NOT due (strict boundary: adopt valid
// strictly before the deadline, expiry at/after it).
func (m *Manager) expireDueLocked(now time.Time) []teardownAction {
	var acts []teardownAction
	for _, e := range m.leases {
		if e.state == stateGrace && !now.Before(e.deadline) {
			e.state = stateTearing
			acts = append(acts, teardownAction{entry: e, reason: "expired"})
		}
	}
	sort.Slice(acts, func(i, j int) bool { return acts[i].entry.board < acts[j].entry.board })
	return acts
}

// ensureCtx is the context teardown callbacks run under (canceled by
// Close so a shutting-down daemon does not hang on board I/O).
func (m *Manager) ensureCtx() context.Context { return m.opCtx }
