package mcp

// ring_test.go — white-box regression tests for the ring machinery's
// ordering contract. resources_test.go covers the rings end-to-end;
// this file pins the one shape e2e cannot manufacture on a fast clock:
// the merged cross-source view when every event carries the SAME
// receive timestamp (a Windows daemon reality — the monotonic clock
// there ticks ~15.6ms, so a burst can land inside one tick; first CI
// matrix run finding, 2026-11-06: recent() then degenerated to
// map-iteration concatenation order and misordered the merged view).

import (
	"testing"
	"time"
)

// TestRingRecentEqualTsKeepsArrivalOrderAcrossSources: entries sharing
// one timestamp (the coarse-clock burst shape) still merge in TRUE
// arrival order, not per-source blocks in map-iteration order. This is
// the exact regression the first windows matrix run caught live
// (newest-last violated, limited view [5 1 3]).
func TestRingRecentEqualTsKeepsArrivalOrderAcrossSources(t *testing.T) {
	r := newBoardRing()
	tick := time.Now() // every event in one "clock tick"
	pushes := []struct {
		source string
		seq    int
	}{
		{"gpio.4", 1}, {"uart1.rx", 2}, {"gpio.4", 3},
		{"stdout", 4}, {"gpio.5", 5}, {"uart1.rx", 6},
	}
	for _, p := range pushes {
		r.append(ringEvent{Source: p.source, Data: map[string]any{"seq": float64(p.seq)}, Ts: tick})
	}

	// The merged view must be arrival order (seq 1..6) no matter how the
	// events map iterates — run the read repeatedly so a single lucky
	// map order cannot pass the test by chance.
	for i := 0; i < 50; i++ {
		events, _, _ := r.recent("", 0)
		if len(events) != len(pushes) {
			t.Fatalf("merged view holds %d events, want %d", len(events), len(pushes))
		}
		for j, ev := range events {
			if got := ev.Data["seq"].(float64); got != float64(j+1) {
				t.Fatalf("read %d: merged position %d = seq %v, want %d (arrival order must survive equal timestamps)", i, j, got, j+1)
			}
		}
	}

	// The limit cut keeps the NEWEST tail by arrival: limit 3 → seqs
	// [4 5 6] with truncated flagged — the second assertion the windows
	// run tripped.
	events, _, truncated := r.recent("", 3)
	if !truncated {
		t.Errorf("truncated not flagged with limit 3 over 6 events")
	}
	for j, ev := range events {
		if got := ev.Data["seq"].(float64); got != float64(j+4) {
			t.Fatalf("limited view position %d = seq %v, want %d (newest three by arrival)", j, got, j+4)
		}
	}
}

// TestRingArrivalSeqSurvivesEviction: eviction drops the oldest whole
// events but the arrival sequence keeps increasing — the merged order
// of the survivors stays true arrival order even across ring wraps —
// and the cap is PER SOURCE: two interleaved sources each under ringCap
// hold everything even though the merged view crosses ringCap.
func TestRingArrivalSeqSurvivesEviction(t *testing.T) {
	r := newBoardRing()
	tick := time.Now()

	// interleaved, each source under ringCap: nothing evicted, merged
	// view legitimately above ringCap
	total := ringCap + 10
	for i := 0; i < total; i++ {
		src := "gpio.4"
		if i%2 == 1 {
			src = "uart1.rx"
		}
		r.append(ringEvent{Source: src, Data: map[string]any{"seq": float64(i)}, Ts: tick})
	}
	events, dropped, _ := r.recent("", 0)
	if len(events) != total {
		t.Fatalf("interleaved merged view = %d events, want all %d (the cap is per source, not per board)", len(events), total)
	}
	if got := dropped["gpio.4"] + dropped["uart1.rx"]; got != 0 {
		t.Errorf("dropped = %d with both sources under ringCap, want 0", got)
	}
	for j, ev := range events {
		if want := float64(j); ev.Data["seq"].(float64) != want {
			t.Fatalf("merged position %d = seq %v, want %v (arrival order)", j, ev.Data["seq"], want)
		}
	}

	// single source across the wrap: oldest evicted, survivors in
	// arrival order, dropped honest
	r2 := newBoardRing()
	solo := 3 * ringCap
	for i := 0; i < solo; i++ {
		r2.append(ringEvent{Source: "gpio.4", Data: map[string]any{"seq": float64(i)}, Ts: tick})
	}
	events, dropped, _ = r2.recent("", 0)
	if len(events) != ringCap {
		t.Fatalf("post-eviction merged view = %d events, want %d", len(events), ringCap)
	}
	if got := dropped["gpio.4"]; got != solo-ringCap {
		t.Errorf("dropped = %d, want %d", got, solo-ringCap)
	}
	for j, ev := range events {
		if want := float64(solo - ringCap + j); ev.Data["seq"].(float64) != want {
			t.Fatalf("merged position %d = seq %v, want %v (oldest evicted in arrival order)", j, ev.Data["seq"], want)
		}
	}
}
