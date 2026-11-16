package la

// bits_test.go — la_bits (LA 增強波 A): the universal decoder's
// goldens — the edgenrz NRZ stream (hand-derived bit string, the
// violation account), the sampwm samples read, the explicit-rate and
// domain refusals, and the truncation bound.

import (
	"strings"
	"testing"
)

func TestBitsNRZGolden(t *testing.T) {
	ref := fixtureRef(t, "edgenrz")
	got, err := Bits(ref, 0, 0)
	if err != nil {
		t.Fatalf("Bits: %v", err)
	}
	// the unit votes 100µs; the grid anchors at the FIRST transition
	// (+100µs) — run 0's leading '1' is the arm artifact and never a
	// bit (the ws2812 precedent), so the read starts one bit in
	if got.RateNS != 100_000 || !got.RateDerived {
		t.Fatalf("rate = %dns derived %v, want 100000/true", got.RateNS, got.RateDerived)
	}
	if got.FirstTNS != 6_000_100_000 {
		t.Fatalf("first_t_ns = %d, want 6.0001e9", got.FirstTNS)
	}
	want := "0110010" + strings.Repeat("10110010", 3) + "1011"
	if got.NBits != 35 || len(got.Segments) != 1 || got.Segments[0].Bits != want {
		t.Fatalf("bits = %q (n %d), want %q (35)", got.Segments[0].Bits, got.NBits, want)
	}
	// the violation account: the 140µs tail run (1.4 units — off-grid
	// by 40ns > 25% band) and the 30µs glitch (sub-unit); the clean
	// 310µs run (rounds to 3 units, 10ns off) does NOT count
	if got.Violations != 2 {
		t.Fatalf("violations = %d, want 2 (H140 + L30)", got.Violations)
	}
	if got.FirstViolationNS == nil || *got.FirstViolationNS != 6_003_200_000 {
		t.Fatalf("first_violation = %v, want 6.0032e9 (the H140 at +3.2ms)", got.FirstViolationNS)
	}
	if got.Truncated {
		t.Error("truncated on a 35-bit read")
	}
}

func TestBitsExplicitRateAndSamples(t *testing.T) {
	// sampwm ch0: every channel run is 5000ns — the derived grid reads
	// the square wave "1010…1"; an explicit rate echoes rate_derived=false
	ref := fixtureRef(t, "sampwm")
	got, err := Bits(ref, 0, 0)
	if err != nil {
		t.Fatalf("Bits samples: %v", err)
	}
	if got.RateNS != 5_000 || !got.RateDerived || got.NBits != 15 ||
		got.Segments[0].Bits != "101010101010101" || got.Violations != 0 {
		t.Fatalf("samples bits = %+v, want 5000ns derived / 15 clean alternating bits", got)
	}
	got, err = Bits(ref, 0, 200_000) // 200k bits/s = 5µs, the same grid
	if err != nil || got.RateDerived || got.RateNS != 5_000 {
		t.Fatalf("explicit rate = %+v err %v, want rate_ns 5000 not derived", got, err)
	}
	// ch1 (the delayed copy) anchors at its own first transition (+6µs)
	// and reads 14 alternating bits from there
	got, err = Bits(ref, 1, 0)
	if err != nil || got.FirstTNS != 7_000_006_000 || got.NBits != 14 ||
		got.Segments[0].Bits != "10101010101010" {
		t.Fatalf("ch1 bits = %+v err %v, want 14 bits from +6µs", got, err)
	}
}

func TestBitsSegmentBoundAndTruncation(t *testing.T) {
	// a long square wave at an explicit 1ms… no: rate 2000 bits/s =
	// 500µs unit over 3000 H/L periods → 5999 bits, over the 4096
	// cap: sixteen full segments listed, the cut flagged
	runs := pwmRuns(3000, 500_000, 500_000)
	ref := synthEdge(t, edgeMeta("long", []int{7}, 5_000_000_000), encRuns(runs...))
	got, err := Bits(ref, 0, 2000)
	if err != nil {
		t.Fatalf("Bits long: %v", err)
	}
	if got.NBits != 5999 || !got.Truncated || len(got.Segments) != 16 {
		t.Fatalf("long = n %d trunc %v segs %d, want 5999/true/16 (the 4096-bit cap)", got.NBits, got.Truncated, len(got.Segments))
	}
	total := 0
	for _, s := range got.Segments {
		if len(s.Bits) != 256 {
			t.Fatalf("segment %d = %d bits, want 256", s.I, len(s.Bits))
		}
		total += len(s.Bits)
	}
	if total != 4096 {
		t.Fatalf("listed bits = %d, want the 4096 cap", total)
	}
	if got.Segments[1].TNS != 5_000_500_000+256*500_000 {
		t.Errorf("segment 1 t_ns = %d, want anchor+256×500µs", got.Segments[1].TNS)
	}
}

func TestBitsNegatives(t *testing.T) {
	mod := fixtureRef(t, "modbus")
	if _, err := Bits(mod, 0, 0); err == nil || !strings.Contains(err.Error(), "LEVEL") {
		t.Errorf("listen: err = %v, want the LEVEL coach", err)
	}
	ref := fixtureRef(t, "edgenrz")
	if _, err := Bits(ref, 0, 2_000_000_000); err == nil || !strings.Contains(err.Error(), "1..1000000000") {
		t.Errorf("rate too fine: err = %v, want the 1ns-grid coach", err)
	}
	sam := fixtureRef(t, "sampwm")
	if _, err := Bits(sam, 0, 40_000_000); err == nil || !strings.Contains(err.Error(), "rate_hz 1000000") {
		t.Errorf("rate over sampling: err = %v, want the rate_hz coach", err)
	}
	if _, err := Bits(sam, 8, 0); err == nil || !strings.Contains(err.Error(), "0..7") {
		t.Errorf("ch 8: err = %v, want the channel coach", err)
	}
	// a static line never anchors a grid
	flat := synthEdge(t, edgeMeta("flat", []int{5}, 9), encRuns(srun{1, 5_000_000}))
	if _, err := Bits(flat, 0, 0); err == nil || !strings.Contains(err.Error(), "never transitions") {
		t.Errorf("static line: err = %v, want the no-anchor coach", err)
	}
	// one flip only (run 0 + one run): no runS past run 0 to vote…
	// one transition exists (anchor ok) but no widths past it? the
	// two-run case still votes one width — assert it refuses on the
	// degenerate span instead when it must
	twoRun := synthEdge(t, edgeMeta("two", []int{5}, 9), encRuns(srun{1, 1_000}, srun{0, 1_000}))
	if _, err := Bits(twoRun, 0, 0); err == nil {
		// a 1µs unit over a 1µs span reads one bit — legitimate; the
		// refusal case is the zero-width degenerate, not this
		if got, gerr := Bits(twoRun, 0, 0); gerr == nil && got.NBits != 1 {
			t.Errorf("two-run bits = %+v, want a single honest bit", got)
		}
	}
}
