package la

// align_test.go — la_align (LA 增強波 A): the two-capture alignment —
// the self-align identity (lag 0), the known-shift golden (an exact
// +123456ns t0 offset answers exactly 123456), the drift estimate on
// a +1000ppm capture, and the honest refusals (aperiodic, cross-board,
// cross-mode, pin sets).

import (
	"strings"
	"testing"
)

// shiftedEdge clones the fixture's run stream under a new meta: t0
// moved by dNS (and optionally every run width scaled by ppm).
func shiftedEdge(t *testing.T, id string, t0 int64, dNS int64, ppm int) CaptureRef {
	t.Helper()
	src := fixtureRef(t, "edgepwm")
	runs, _, err := levelRuns(src)
	if err != nil {
		t.Fatal(err)
	}
	var stream []byte
	for i, r := range runs {
		d := r.WidthNS
		if ppm != 0 {
			d = d + uint64(int64(d)*int64(ppm)/1_000_000)
			_ = i
		}
		stream = append(stream, encRun(r.Level, d)...)
	}
	meta := edgeMeta(id, []int{40}, 4_000_000_000+dNS)
	return CaptureRef{ID: id, DataPath: writeTemp(t, stream), Meta: meta}
}

func TestAlignSelfIsIdentity(t *testing.T) {
	ref := fixtureRef(t, "edgepwm")
	got, err := Align(ref, ref)
	if err != nil {
		t.Fatalf("Align self: %v", err)
	}
	if got.LagNS != 0 || got.Matched != 39 || got.NA != 39 || got.NB != 39 {
		t.Fatalf("self = lag %d matched %d (%d/%d), want 0/39", got.LagNS, got.Matched, got.NA, got.NB)
	}
	if got.PeriodANS != 250_000 || got.PeriodBNS != 250_000 || got.TolNS != 31_250 {
		t.Fatalf("beats = a %d b %d tol %d, want 250000/250000/31250", got.PeriodANS, got.PeriodBNS, got.TolNS)
	}
	if got.DriftPPM == nil || *got.DriftPPM != 0 {
		t.Fatalf("drift = %v, want the exact zero", got.DriftPPM)
	}
	for _, s := range got.Segments {
		if s.OffsetNS != 0 {
			t.Errorf("segment %+v, want zero offsets on the identity", s)
		}
	}
}

func TestAlignKnownShiftGolden(t *testing.T) {
	a := fixtureRef(t, "edgepwm")
	b := shiftedEdge(t, "shifted", 4_000_123_456, 123_456, 0)
	got, err := Align(a, b)
	if err != nil {
		t.Fatalf("Align shift: %v", err)
	}
	// every pair sits exactly +123456ns over — the refined lag is exact
	if got.LagNS != 123_456 {
		t.Fatalf("lag = %d, want the exact 123456", got.LagNS)
	}
	if got.Matched != 39 {
		t.Fatalf("matched = %d, want all 39", got.Matched)
	}
	if got.DriftPPM == nil || *got.DriftPPM != 0 {
		t.Fatalf("drift = %v, want zero (a pure shift)", got.DriftPPM)
	}
}

func TestAlignDriftEstimate(t *testing.T) {
	// B runs 1000ppm slow (every width ×1.001): the events walk
	// +1ns/period past A — the segment offsets grow linearly and the
	// drift estimate lands on ~1000ppm
	a := fixtureRef(t, "edgepwm")
	b := shiftedEdge(t, "slow", 4_000_000_000, 0, 1000)
	got, err := Align(a, b)
	if err != nil {
		t.Fatalf("Align drift: %v", err)
	}
	if got.Matched < 35 {
		t.Fatalf("matched = %d, the 1000ppm walk stays inside the band for 20ms", got.Matched)
	}
	if got.DriftPPM == nil {
		t.Fatal("drift absent on a two-segment match")
	}
	if d := *got.DriftPPM; d < 800 || d > 1200 {
		t.Fatalf("drift = %v ppm, want ≈1000", d)
	}
	// the segments' offsets walk ~1ns/period, centered on the mean by
	// the lag refinement: early ≈ −8.5µs, late ≈ +8.3µs
	first, last := got.Segments[0], got.Segments[len(got.Segments)-1]
	if first.OffsetNS < -10500 || first.OffsetNS > -6500 {
		t.Errorf("first offset = %v, want ≈-8500", first.OffsetNS)
	}
	if last.OffsetNS < 6500 || last.OffsetNS > 10500 {
		t.Errorf("last offset = %v, want ≈+8300", last.OffsetNS)
	}
}

func TestAlignRefusals(t *testing.T) {
	pwm := fixtureRef(t, "edgepwm")
	// cross-board (the Diff precedent)
	other := synthEdge(t, Meta{CapID: "xboard", Board: "d75884", Mode: "edge",
		Pins: []int{40}, ResHz: 80_000_000, T0NS: 4_000_000_000}, encRuns(pwmRuns(20, 250_000, 750_000)...))
	if _, err := Align(pwm, other); err == nil || !strings.Contains(err.Error(), "different boards") {
		t.Errorf("cross-board: err = %v, want the same-board coach", err)
	}
	// unequal pin sets refuse
	twoPin := synthEdge(t, edgeMeta("two", []int{2, 3}, 4_000_000_000), encRuns(pwmRuns(20, 250_000, 750_000)...))
	if _, err := Align(pwm, twoPin); err == nil || !strings.Contains(err.Error(), "pin sets") {
		t.Errorf("pin sets: err = %v, want the pin-set coach", err)
	}
	// listen refuses the level domain
	mod := fixtureRef(t, "modbus")
	if _, err := Align(pwm, mod); err == nil || !strings.Contains(err.Error(), "LEVEL") {
		t.Errorf("listen B: err = %v, want the LEVEL coach", err)
	}
	// aperiodic: random-ish gaps never vote a beat
	var ragged []srun
	cur := 0
	widths := []uint64{1_000, 137_000, 999_999, 42_000, 777_777, 314_159, 271_828, 100_000, 654_321, 111_111}
	for _, w := range widths {
		for i := 0; i < 3; i++ {
			ragged = append(ragged, srun{cur, w})
			cur ^= 1
		}
	}
	aper := synthEdge(t, edgeMeta("aper", []int{5}, 9_000), encRuns(ragged...))
	if _, err := Align(aper, aper); err == nil || !strings.Contains(err.Error(), "no dominant beat") {
		t.Errorf("aperiodic: err = %v, want the beat coach", err)
	}
	// too few events
	tiny := synthEdge(t, edgeMeta("tiny", []int{5}, 9_000), encRuns(srun{1, 100}, srun{0, 100}, srun{1, 100}))
	if _, err := Align(tiny, tiny); err == nil || !strings.Contains(err.Error(), "≥4 events") {
		t.Errorf("tiny: err = %v, want the events coach", err)
	}
	// a beat-matched but CLUSTERED B (6 runs inside 1.25ms, parked
	// off-phase in A's 20ms): the phase is real and aligns exactly —
	// the reply states it with matched=5 (B's whole event set), not a
	// refusal; the raw-residual locality guard is what kept the other
	// 34 A-events out of the account. lag_ns rides the NEAREST-PHASE
	// reading: +234567 wraps to −15433 (one beat back)
	clipped := synthEdge(t, edgeMeta("clip", []int{40}, 4_001_234_567), encRuns(
		srun{1, 250_000}, srun{0, 250_000}, srun{1, 250_000},
		srun{0, 250_000}, srun{1, 250_000}, srun{0, 250_000}))
	got, err := Align(pwm, clipped)
	if err != nil {
		t.Fatalf("clustered B: %v", err)
	}
	if got.LagNS != 234_567-250_000 || got.Matched != 3 {
		t.Fatalf("clustered B = lag %d matched %d, want the nearest-phase −15433 over the 3 A-events inside B's window", got.LagNS, got.Matched)
	}
	if len(got.Segments) != 1 || got.DriftPPM != nil {
		t.Fatalf("clustered segments = %+v drift %v, want one segment / no drift claim", got.Segments, got.DriftPPM)
	}

	// a NON-DIVISOR beat (300µs against A's 250µs) scatters the
	// phases across the fold: the votes spread thin, the residuals
	// scatter — the honest band refusal
	misBeat := synthEdge(t, edgeMeta("mis", []int{40}, 4_000_000_000), encRuns(pwmRuns(20, 75_000, 225_000)...))
	if _, err := Align(pwm, misBeat); err == nil || !strings.Contains(err.Error(), "no alignment within") {
		t.Errorf("mis-beat pair: err = %v, want the band coach", err)
	}
}
