package mcp

// ring.go — the event face's ring machinery (batch 6 task 10, design
// §8): per-(board, source) bounded rings of WHOLE events, fed by ONE
// apiclient OnEvent handler (resources.go). Every entry carries the
// full §8 event — source, data map, receive timestamp — and eviction
// drops the OLDEST WHOLE entry per source when a ring crosses ringCap:
// an entry is either fully present or fully gone, never torn, and the
// per-source dropped count keeps the eviction observable ("丟棄計數可
// 查"). Ordering inside a source is arrival order (newest last); the
// merged view across sources (recent) sorts by the per-board ARRIVAL
// SEQUENCE stamped at append — never by the receive timestamp: on
// Windows the monotonic clock ticks ~15.6ms, so a burst can land
// entirely inside one tick, and a timestamp sort of equal-Ts entries
// degenerates to map-iteration concatenation order, misordering the
// merged view (first CI matrix run finding, 2026-11-06). The arrival
// counter is assigned under the owning eventState's mutex, so it is
// the true feed order on every platform.

import (
	"sort"
	"strings"
	"time"
)

// ringCap bounds one (board, source) ring: the last 256 events per
// source, the same order of magnitude as the daemon's and apiclient's
// per-connection event queues — an agent polling events_recent at a
// sane cadence never loses ground, and a flood costs at most 256 ×
// sources entries per board.
const ringCap = 256

// ringEvent is one whole §8 event as the rings (and every face that
// reads them — the resource snapshot, events_recent) present it.
type ringEvent struct {
	Source string         `json:"source"`
	Data   map[string]any `json:"data,omitempty"`
	Ts     time.Time      `json:"ts"` // when this process received the push (display; ordering rides arr)

	// arr is this event's per-board arrival sequence, stamped by
	// boardRing.append under the owning eventState's mutex (unexported,
	// so it never crosses the JSON wire). recent() orders the merged
	// cross-source view by it — the receive timestamp cannot do that
	// job on platforms with a coarse monotonic clock (Windows: a burst
	// can share one tick, and equal-Ts entries would fall back to map
	// iteration order — see the file comment).
	arr int64
}

// boardRing is one board's rings, keyed by source. All access is
// serialized by the owning eventState's mutex (one lock for the map
// and the rings — event volume is low and the operations are copies).
type boardRing struct {
	events  map[string][]ringEvent // source → ring, newest last (arrival order)
	dropped map[string]int         // source → whole events evicted by overflow
	arrSeq  int64                  // per-board arrival counter feeding ringEvent.arr
}

func newBoardRing() *boardRing {
	return &boardRing{
		events:  map[string][]ringEvent{},
		dropped: map[string]int{},
	}
}

// append stores one event in its source's ring, stamping the per-board
// arrival sequence, and evicts the oldest WHOLE entries beyond ringCap
// (counted in dropped).
func (r *boardRing) append(ev ringEvent) {
	r.arrSeq++
	ev.arr = r.arrSeq
	ring := append(r.events[ev.Source], ev)
	if over := len(ring) - ringCap; over > 0 {
		ring = ring[over:]
		r.dropped[ev.Source] += over
	}
	r.events[ev.Source] = ring
}

// sources returns the board's source names, sorted (deterministic
// snapshots).
func (r *boardRing) sources() []string {
	out := make([]string, 0, len(r.events))
	for src := range r.events {
		out = append(out, src)
	}
	sort.Strings(out)
	return out
}

// ringSource is one source's leg of the resource snapshot JSON.
type ringSource struct {
	Source  string      `json:"source"`
	Dropped int         `json:"dropped"`
	Events  []ringEvent `json:"events"`
}

// boardSnapshot is a board's whole ring state as the resource content:
// per-source, newest-last, with the eviction counts.
type boardSnapshot struct {
	Board   string       `json:"board"`
	RingCap int          `json:"ring_cap"`
	Sources []ringSource `json:"sources"`
}

// snapshot renders the board's rings for the resource read.
func (r *boardRing) snapshot(board string) boardSnapshot {
	snap := boardSnapshot{Board: board, RingCap: ringCap, Sources: []ringSource{}}
	for _, src := range r.sources() {
		events := r.events[src]
		cp := make([]ringEvent, len(events))
		copy(cp, events)
		snap.Sources = append(snap.Sources, ringSource{Source: src, Dropped: r.dropped[src], Events: cp})
	}
	return snap
}

// recent merges every source whose name carries prefix ("" = all) into
// one newest-last view, capped at limit (when limit > 0), reporting
// the per-source dropped counts of the sources it merged and whether
// the cap cut events. Entries are copied — callers can hold them
// without racing later appends.
func (r *boardRing) recent(prefix string, limit int) (events []ringEvent, dropped map[string]int, truncated bool) {
	merged := []ringEvent{}
	dropped = map[string]int{}
	for src, ring := range r.events {
		if !sourceMatches(src, prefix) {
			continue
		}
		merged = append(merged, ring...)
		dropped[src] = r.dropped[src]
	}
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].arr < merged[j].arr })
	if limit > 0 && len(merged) > limit {
		merged = merged[len(merged)-limit:]
		truncated = true
	}
	return merged, dropped, truncated
}

// sourceMatches is the prefix filter ("gpio" matches gpio.4 and
// gpio.5; "uart1.rx" matches exactly; "" matches everything).
func sourceMatches(source, prefix string) bool {
	return strings.HasPrefix(source, prefix)
}
