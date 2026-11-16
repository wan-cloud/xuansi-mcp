package la

// glitch_eye_test.go — la_glitch + la_eye (LA 增強波 A): the outlier
// and fold goldens over the committed fixtures (hand-derived from the
// frozen streams), the ratio domain, and the honest zeros.

import (
	"strings"
	"testing"
)

func TestGlitchNRZGolden(t *testing.T) {
	ref := fixtureRef(t, "edgenrz")
	got, err := Glitch(ref, 0, 0)
	if err != nil {
		t.Fatalf("Glitch: %v", err)
	}
	// the unit votes 100µs (16 of 26 runs); threshold 50µs catches
	// exactly the 30µs tear; the 140µs violation is NOT a glitch (it
	// is la_bits' off-grid account, a different question)
	if got.ModeNS != 100_000 || got.Ratio != 0.5 || got.ThresholdNS != 50_000 {
		t.Fatalf("echo = mode %d ratio %v thr %d, want 100000/0.5/50000", got.ModeNS, got.Ratio, got.ThresholdNS)
	}
	if got.Count != 1 || len(got.Rows) != 1 || got.Truncated {
		t.Fatalf("count = %d rows %d trunc %v, want the single L30", got.Count, len(got.Rows), got.Truncated)
	}
	r := got.Rows[0]
	if r.RunI != 25 || r.TNS != 6_003_340_000 || r.Level != 0 || r.DurNS != 30_000 {
		t.Fatalf("glitch row = %+v, want {run 25, t 6.00334e9, 0, 30000}", r)
	}

	// a tighter ratio keeps it (threshold 30µs — the 30µs tear sits AT
	// it and passes); a wider one lists it
	got, err = Glitch(ref, 0, 0.3)
	if err != nil || got.Count != 0 || len(got.Rows) != 0 {
		t.Fatalf("ratio 0.3 = count %d err %v, want the honest zero", got.Count, err)
	}
	// an explicit ratio echoes
	got, err = Glitch(ref, 0, 1)
	if err != nil || got.Ratio != 1 || got.ThresholdNS != 100_000 {
		t.Fatalf("ratio 1 = %+v err %v, want threshold == the mode", got, err)
	}
}

func TestGlitchCleanPWMAndRefusals(t *testing.T) {
	// edgepwm: the lows outnumber 20:19 → the mode is the 750µs low;
	// the 19 HIGH runs then sit under the 375µs threshold — the
	// honest listing (the threshold rides the MODAL run: an
	// asymmetric duty's minority phase reads as outlier, stated in
	// the Desc)
	ref := fixtureRef(t, "edgepwm")
	got, err := Glitch(ref, 0, 0)
	if err != nil || got.ModeNS != 750_000 || got.ThresholdNS != 375_000 {
		t.Fatalf("pwm glitch = %+v err %v, want mode 750000 thr 375000", got, err)
	}
	if got.Count != 19 || len(got.Rows) != 19 {
		t.Fatalf("pwm glitch count = %d, want the 19 minority highs", got.Count)
	}
	if got.Rows[0].RunI != 2 || got.Rows[0].DurNS != 250_000 {
		t.Fatalf("row 0 = %+v, want {run 2, 250000}", got.Rows[0])
	}
	// sampwm ch0: every run at the mode — the honest zero
	sam := fixtureRef(t, "sampwm")
	got, err = Glitch(sam, 0, 0)
	if err != nil || got.ModeNS != 5_000 || got.Count != 0 {
		t.Fatalf("sampwm glitch = %+v err %v, want mode 5000 / zero", got, err)
	}
	// domain + guards
	if _, err := Glitch(ref, 0, 1.5); err == nil || !strings.Contains(err.Error(), "(0, 1]") {
		t.Errorf("ratio 1.5: err = %v, want the ratio domain coach", err)
	}
	if _, err := Glitch(ref, 0, -0.1); err == nil || !strings.Contains(err.Error(), "(0, 1]") {
		t.Errorf("ratio -0.1: err = %v, want the ratio domain coach", err)
	}
	if _, err := Glitch(fixtureRef(t, "modbus"), 0, 0); err == nil || !strings.Contains(err.Error(), "LEVEL") {
		t.Errorf("listen: err = %v, want the LEVEL coach", err)
	}
	// a static line has no unit to vote
	flat := synthEdge(t, edgeMeta("flat", []int{5}, 9), encRuns(srun{1, 5_000_000}))
	if _, err := Glitch(flat, 0, 0); err == nil || !strings.Contains(err.Error(), "no signal runs") {
		t.Errorf("static: err = %v, want the no-runs coach", err)
	}
	// the row bound: 300 sub-threshold tears (40ns) against a 4µs
	// majority → count 300, rows bounded at 256 with the flag
	var runs []srun
	runs = append(runs, srun{1, 10_000})
	for i := 0; i < 300; i++ {
		runs = append(runs, srun{0, 4_000}, srun{1, 40})
	}
	runs = append(runs, srun{0, 4_000}) // the majority tips past the tie
	burst := synthEdge(t, edgeMeta("burst", []int{5}, 9), encRuns(runs...))
	got, err = Glitch(burst, 0, 0)
	if err != nil || got.ModeNS != 4_000 || got.Count != 300 || len(got.Rows) != 256 || !got.Truncated {
		t.Fatalf("bound = mode %d count %d rows %d trunc %v, want 4000/300/256/true", got.ModeNS, got.Count, len(got.Rows), got.Truncated)
	}
}

func TestEyePWMGolden(t *testing.T) {
	ref := fixtureRef(t, "edgepwm")
	got, err := Eye(ref, 0)
	if err != nil {
		t.Fatalf("Eye: %v", err)
	}
	// 38 consecutive pairs over the 39 signal runs fold cleanly at the
	// exact 1ms period: pp spreads zero, openings exactly the duty widths
	if got.PeriodNS != 1_000_000 || got.NFold != 38 {
		t.Fatalf("fold = period %d n %d, want 1ms × 38 pairs", got.PeriodNS, got.NFold)
	}
	if *got.MinHighNS != 250_000 || *got.MaxHighNS != 250_000 || *got.HighPPNS != 0 {
		t.Errorf("high = %d..%d pp %d, want 250000 flat", *got.MinHighNS, *got.MaxHighNS, *got.HighPPNS)
	}
	if *got.MinLowNS != 750_000 || *got.LowPPNS != 0 {
		t.Errorf("low = %d..%d pp %d, want 750000 flat", *got.MinLowNS, *got.MaxLowNS, *got.LowPPNS)
	}
}

func TestEyeJitterAndSamples(t *testing.T) {
	// the edgenrz fold: 24 consecutive pairs over the 25 signal runs
	// vote {200×12, 300×8, 400×4} — the modal pair span is 200µs (the
	// honest vote, not the intuitive 300: NRZ pairs are unequal)
	ref := fixtureRef(t, "edgenrz")
	got, err := Eye(ref, 0)
	if err != nil {
		t.Fatalf("Eye nrz: %v", err)
	}
	if got.PeriodNS != 200_000 || got.NFold != 25 {
		t.Fatalf("fold = period %d n %d, want 200000 × 25 pairs", got.PeriodNS, got.NFold)
	}
	// sig runs past run 0: highs {100×15? }: the highs are
	// 100,200,100,100,200,… plus the tail 140,310; the lows
	// 100,200,…,30. min_high = 100000, max_high = 310000 → pp 210000;
	// min_low = 30000 (the tear), max_low 200000
	if *got.MinHighNS != 100_000 || *got.MaxHighNS != 310_000 || *got.HighPPNS != 210_000 {
		t.Errorf("high = %d..%d pp %d, want 100000..310000 pp 210000", *got.MinHighNS, *got.MaxHighNS, *got.HighPPNS)
	}
	if *got.MinLowNS != 30_000 || *got.MaxLowNS != 200_000 || *got.LowPPNS != 170_000 {
		t.Errorf("low = %d..%d pp %d, want 30000..200000 pp 170000", *got.MinLowNS, *got.MaxLowNS, *got.LowPPNS)
	}

	// samples: sampwm ch0 folds at 10µs, clean
	sam := fixtureRef(t, "sampwm")
	got, err = Eye(sam, 0)
	if err != nil || got.PeriodNS != 10_000 || got.NFold != 14 ||
		*got.MinHighNS != 5_000 || *got.LowPPNS != 0 {
		t.Fatalf("sampwm eye = %+v err %v, want 10µs fold clean", got, err)
	}
	// a no-fold capture refuses; listen refuses with the domain coach
	twoRun := synthEdge(t, edgeMeta("two", []int{5}, 9), encRuns(srun{1, 1000}, srun{0, 1000}))
	if _, err := Eye(twoRun, 0); err == nil || !strings.Contains(err.Error(), "one full alternation") {
		t.Errorf("two-run: err = %v, want the fold coach", err)
	}
	if _, err := Eye(fixtureRef(t, "modbus"), 0); err == nil || !strings.Contains(err.Error(), "LEVEL") {
		t.Errorf("listen: err = %v, want the LEVEL coach", err)
	}
}
