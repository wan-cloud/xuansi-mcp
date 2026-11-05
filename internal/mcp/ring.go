package mcp

// ring.go — the event face's ring machinery (batch 6 task 10, design
// §8): per-(board, source) bounded rings of WHOLE events, fed by ONE
// apiclient OnEvent handler (resources.go). Every entry carries the
// full §8 event — source, data map, receive timestamp — and eviction
// drops the OLDEST WHOLE entry per source when a ring crosses ringCap:
// an entry is either fully present or fully gone, never torn, and the
// per-source dropped count keeps the eviction observable ("丟棄計數可
// 查"). Ordering inside a source is arrival order (newest last); the
// merged view across sources (recent) sorts by receive timestamp,
// stable, so equal-timestamp events keep arrival order too.

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
	Ts     time.Time      `json:"ts"` // when this process received the push
}

// boardRing is one board's rings, keyed by source. All access is
// serialized by the owning eventState's mutex (one lock for the map
// and the rings — event volume is low and the operations are copies).
type boardRing struct {
	events  map[string][]ringEvent // source → ring, newest last (arrival order)
	dropped map[string]int         // source → whole events evicted by overflow
}

func newBoardRing() *boardRing {
	return &boardRing{
		events:  map[string][]ringEvent{},
		dropped: map[string]int{},
	}
}

// append stores one event in its source's ring, evicting the oldest
// WHOLE entries beyond ringCap (counted in dropped).
func (r *boardRing) append(ev ringEvent) {
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
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].Ts.Before(merged[j].Ts) })
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
