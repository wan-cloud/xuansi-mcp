package la

// activity_test.go — la_activity (LA 增強波 A): the per-mode binning
// goldens (edge transitions, samples popcount edges, listen chunk
// arrivals), the derived width, the bin-cap refusal coach, and the
// window.

import (
	"strings"
	"testing"
)

func TestActivityEdgeGolden(t *testing.T) {
	ref := fixtureRef(t, "edgepwm")
	got, err := Activity(ref, 1, 0, 0, 0)
	if err != nil {
		t.Fatalf("Activity: %v", err)
	}
	// 20ms span at 1ms bins: bin0 holds the lone +250µs transition,
	// bins 1..19 hold two each (the ms boundary + the +250µs after it)
	if got.Mode != "edge" || got.BinNS != 1_000_000 || len(got.Bins) != 20 {
		t.Fatalf("echo = mode %s bin %dns n %d", got.Mode, got.BinNS, len(got.Bins))
	}
	if got.NEdges != 39 {
		t.Fatalf("n_edges = %d, want 39 (the 40-run stream's transitions)", got.NEdges)
	}
	if got.Bins[0].Edges != 1 || got.Bins[0].Ratio != 0.5 {
		t.Errorf("bin 0 = %+v, want 1 edge at ratio 0.5", got.Bins[0])
	}
	for i := 1; i < 20; i++ {
		if got.Bins[i].Edges != 2 || got.Bins[i].Ratio != 1.0 {
			t.Fatalf("bin %d = %+v, want 2 edges at ratio 1.0", i, got.Bins[i])
		}
	}
	if got.Bins[0].FromNS != 4_000_000_000 || got.Bins[0].ToNS != 4_001_000_000 {
		t.Errorf("bin 0 window = [%d,%d)", got.Bins[0].FromNS, got.Bins[0].ToNS)
	}
	if got.BusiestNS == nil || *got.BusiestNS != 4_001_000_000 {
		t.Errorf("busiest = %v, want the FIRST 2-edge bin at 4.001e9", got.BusiestNS)
	}
	if got.Truncated {
		t.Error("truncated on an in-bounds ask")
	}

	// the derived width: omitted bin_ms → the span fills ≤256 bins
	got, err = Activity(ref, 0, 0, 0, 0)
	if err != nil || got.BinNS < 78_125 || len(got.Bins) > 256 {
		t.Fatalf("derived = bin %dns n %d err %v, want ≥78125ns ≤256 bins", got.BinNS, len(got.Bins), err)
	}

	// the window: [4.002e9, 4.004e9) → bins 2..3 of the 1ms grid
	got, err = Activity(ref, 1, 4_002_000_000, 4_004_000_000, 0)
	if err != nil || len(got.Bins) != 2 || got.Bins[0].FromNS != 4_002_000_000 {
		t.Fatalf("windowed = n %d first %+v err %v", len(got.Bins), got.Bins[0], err)
	}
}

func TestActivitySamplesPopcount(t *testing.T) {
	ref := fixtureRef(t, "sampwm")
	got, err := Activity(ref, 1, 0, 0, 0)
	if err != nil {
		t.Fatalf("Activity samples: %v", err)
	}
	// the 80µs span folds into ONE 1ms bin; every flip tick flips one
	// channel (D1 lags D0 by a tick) → 30 edges
	if len(got.Bins) != 1 || got.NEdges != 30 || got.Bins[0].Edges != 30 {
		t.Fatalf("bins = %+v n_edges %d, want 1 bin of 30 popcount edges", got.Bins, got.NEdges)
	}
	// a same-tick two-channel flip counts twice: splice a byte that
	// flips both at once (0x00 → 0x03)
	two := synthRef(t, Meta{Mode: "samples", RateHz: 1_000_000, T0NS: 9}, []byte{0x00, 0x00, 0x03, 0x03})
	got, err = Activity(two, 1, 0, 0, 0)
	if err != nil || got.NEdges != 2 {
		t.Fatalf("two-channel tick = n_edges %d err %v, want 2 (popcount)", got.NEdges, err)
	}
}

func TestActivityListenChunks(t *testing.T) {
	stream := append(append(append(
		encChunk(0, []byte{0x01}),
		encChunk(5_000_000, []byte{0x02})...),
		encChunk(10_000_000, []byte{0x03})...),
		encChunk(15_000_000, []byte{0x04})...)
	ref := synthRef(t, Meta{CapID: "l1", Board: "b", Mode: "listen", Baud: 9600, IdleSyms: 4}, stream)
	got, err := Activity(ref, 5, 0, 0, 0)
	if err != nil {
		t.Fatalf("Activity listen: %v", err)
	}
	if got.Mode != "listen" || got.NEdges != 4 {
		t.Fatalf("listen = mode %s n %d, want 4 chunk arrivals", got.Mode, got.NEdges)
	}
	// the last bin stretches to the final chunk's byte-time end
	// (start 15ms + 1 sym 1.041666ms); every bin carries its chunk's
	// byte count
	if len(got.Bins) != 4 {
		t.Fatalf("bins = %d, want 4", len(got.Bins))
	}
	for i, b := range got.Bins {
		if b.Edges != 1 || b.Bytes != 1 {
			t.Errorf("bin %d = %+v, want 1 chunk / 1 byte", i, b)
		}
	}
	if last := got.Bins[3]; last.FromNS != 15_000_000 || last.ToNS != 16_041_666 {
		t.Errorf("last bin = [%d,%d), want [15e6, 16041666)", last.FromNS, last.ToNS)
	}
}

func TestActivityNegatives(t *testing.T) {
	pwm := fixtureRef(t, "edgepwm")
	// bin_ms too fine for the cap: 20ms at 1ms needs 20 bins — fine;
	// at bin_ms=1 the ask fits, so refuse via a window that forces it:
	// 20ms window at bin_ms=1 = 20 bins ok — use max_bins 8 to force
	_, err := Activity(pwm, 1, 0, 0, 8)
	if err == nil || !strings.Contains(err.Error(), "bin_ms 1 over") || !strings.Contains(err.Error(), "≥ 3 ms") {
		t.Errorf("cap refusal = %v, want the min-bin_ms coach (≥3ms at 8 bins over 20ms)", err)
	}
	// an empty window refuses honestly
	if _, err := Activity(pwm, 1, 9_000_000_000, 9_500_000_000, 0); err == nil || !strings.Contains(err.Error(), "empty against the capture's axis") {
		t.Errorf("empty window = %v, want the axis coach", err)
	}
	// an empty capture refuses
	empty := synthRef(t, Meta{Mode: "samples", RateHz: 1_000_000, T0NS: 9}, nil)
	if _, err := Activity(empty, 1, 0, 0, 0); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty capture = %v, want the empty coach", err)
	}
	// max_bins clamps at the hard cap: 1024 bins of a fine derived ask
	got, err := Activity(pwm, 0, 0, 0, 100_000)
	if err != nil || len(got.Bins) > 1024 {
		t.Errorf("oversized max_bins = n %d err %v, want ≤1024", len(got.Bins), err)
	}
}
