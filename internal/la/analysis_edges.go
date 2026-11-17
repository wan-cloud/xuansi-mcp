package la

// analysis_edges.go — the L1 edge-wave analysis seats (T11): the six
// frozen functions over T10 edge captures. One shared model: the run
// stream read whole (ReadRuns — bounded by the ring, ≤4096KB ≈ 466k
// runs), absolute time = meta.t0_ns + Σ preceding dur (the fw's
// frozen single-source derivation), every reply bounded with
// truncated flags, never silent, never unbounded.

import (
	"fmt"
	"sort"
)

// The view bounds (constraint 10): rows cut at a cap, the cut
// flagged; the hard cap clamps an oversized ask.
const (
	// rowsDefault bounds Edges/Runs when max is omitted.
	rowsDefault = 256
	// rowsMax is the hard row cap (an oversized max clamps to it, no
	// flag — the ask fit under the bound).
	rowsMax = 4096
	// histBinsDefault / histBinsMax bound the histogram's bin count.
	histBinsDefault = 32
	histBinsMax     = 256
	// diffTolNS is the frozen run-comparison tolerance: 2µs absorbs
	// both accuracy bands of the engine (tick-direct 12.5ns runs and
	// the task-clock-rebuilt runs past the 409.59µs frame threshold,
	// µs-scale — the T10 HIL measured sub-µs error on both) while
	// still catching any real protocol divergence (protocol timing
	// units are ≥ tens of µs).
	diffTolNS = 2_000
)

// guardEdge refuses the seats the edge analysis cannot read: the
// capture must be an edge capture with its timing facts (res_hz and
// t0_ns — the absolute axis derives from t0; a meta without it is a
// pull that lost its ledger, and the honest fix is a re-pull).
func guardEdge(ref CaptureRef) error {
	switch ref.Meta.Mode {
	case "edge":
	case "listen", "samples":
		return fmt.Errorf("la: capture %s is mode %q — the edges/runs/hist/measure/diff/decode_wire seats read EDGE captures (arm with la_capture mode \"edge\")", ref.ID, ref.Meta.Mode)
	default:
		return fmt.Errorf("la: capture %s has unknown mode %q", ref.ID, ref.Meta.Mode)
	}
	if ref.Meta.ResHz <= 0 {
		return fmt.Errorf("la: capture %s lacks res_hz — timing cannot derive (re-pull the object)", ref.ID)
	}
	if ref.Meta.T0NS == 0 {
		return fmt.Errorf("la: capture %s lacks t0_ns (the fw ledger did not land with it) — the absolute axis cannot derive; re-pull the object", ref.ID)
	}
	return nil
}

// Edge is one transition row: the instant the run starts, the level
// after it, and how long that level held.
type Edge struct {
	TNS   uint64 `json:"t_ns"`
	Level int    `json:"level"`
	DurNS uint64 `json:"dur_ns"`
}

// EdgesResult is the bounded transition view.
type EdgesResult struct {
	Edges     []Edge `json:"edges"`
	T0NS      int64  `json:"t0_ns"`
	NRuns     int    `json:"n_runs"`
	Truncated bool   `json:"truncated,omitempty"`
}

// RunRow is one run-length row with its whole-capture index.
type RunRow struct {
	I     int    `json:"i"`
	TNS   uint64 `json:"t_ns"`
	Level int    `json:"level"`
	DurNS uint64 `json:"dur_ns"`
}

// RunsResult is the bounded run-length view.
type RunsResult struct {
	Runs      []RunRow `json:"runs"`
	T0NS      int64    `json:"t0_ns"`
	NRuns     int      `json:"n_runs"`
	SpanNS    uint64   `json:"span_ns"`
	Truncated bool     `json:"truncated,omitempty"`
}

// HistBin is one populated bin (half-open [lo, hi); the top bin
// closed at the sample max).
type HistBin struct {
	LoNS uint64 `json:"lo_ns"`
	HiNS uint64 `json:"hi_ns"`
	N    int    `json:"n"`
}

// HistResult is the deterministic statistics reply.
type HistResult struct {
	Kind     string    `json:"kind"`
	Bins     []HistBin `json:"bins"`
	ModeNS   uint64    `json:"mode_ns,omitempty"`
	EstBaud  float64   `json:"est_baud,omitempty"`
	Duty     *float64  `json:"duty,omitempty"`
	NSamples int       `json:"n_samples"`
}

// MeasurePoint names one instant: an absolute t_ns, or the start of
// run edge_i (the Runs view's index).
type MeasurePoint struct {
	TNS   *uint64 `json:"t_ns,omitempty"`
	EdgeI *int    `json:"edge_i,omitempty"`
}

// MeasureResult is the two-point measurement.
type MeasureResult struct {
	DtNS   int64    `json:"dt_ns"`
	FreqHz *float64 `json:"freq_hz,omitempty"`
	Duty   *float64 `json:"duty,omitempty"`
	Note   string   `json:"note,omitempty"`
}

// DiffResult is the two-capture comparison.
type DiffResult struct {
	Same         bool    `json:"same"`
	FirstDiffTNS *uint64 `json:"first_diff_t_ns,omitempty"`
	RunIndex     *int    `json:"run_index,omitempty"`
	RunIndexB    *int    `json:"run_index_b,omitempty"`
	Why          string  `json:"why,omitempty"`
	NRunsA       int     `json:"n_runs_a"`
	NRunsB       int     `json:"n_runs_b"`
	MatchedNS    uint64  `json:"matched_ns"`
	TolNS        uint64  `json:"tol_ns"`
}

// loadRuns reads the whole run stream and answers it on the absolute
// axis (t0 + Σ preceding dur) with the whole-capture span.
func loadRuns(ref CaptureRef) (runs []Run, abs []uint64, span uint64, t0 uint64, err error) {
	runs, err = ReadRuns(ref.DataPath)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	t0 = uint64(ref.Meta.T0NS)
	abs = make([]uint64, len(runs))
	for i, r := range runs {
		abs[i] = t0 + r.StartNS
	}
	if len(runs) > 0 {
		span = runs[len(runs)-1].StartNS + runs[len(runs)-1].WidthNS
	}
	return runs, abs, span, t0, nil
}

// inWindow answers whether absolute t sits in [from, to) — 0 on a
// side means open (the whole axis that direction).
func inWindow(t, from, to uint64) bool {
	if from != 0 && t < from {
		return false
	}
	if to != 0 && t >= to {
		return false
	}
	return true
}

// clampRows bounds the row ask: omitted → rowsDefault, over the hard
// cap → the cap.
func clampRows(max int) int {
	if max <= 0 {
		return rowsDefault
	}
	if max > rowsMax {
		return rowsMax
	}
	return max
}

// Edges answers the bounded TRANSITION view: one row per run — the
// instant it starts (the transition into it), the level after it, and
// how long that level held. The first row is the capture's opening
// level at t0 (not a transition — the reply's t0_ns says so). The
// window from/to bounds by ABSOLUTE t_ns ([from, to), 0 = open); max
// bounds the rows (default 256, hard cap 4096, the cut flagged).
func Edges(ref CaptureRef, fromNS, toNS uint64, max int) (EdgesResult, error) {
	if err := guardEdge(ref); err != nil {
		return EdgesResult{}, err
	}
	runs, abs, _, _, err := loadRuns(ref)
	if err != nil {
		return EdgesResult{}, err
	}
	res := EdgesResult{T0NS: ref.Meta.T0NS, NRuns: len(runs)}
	cap := clampRows(max)
	for i, r := range runs {
		if !inWindow(abs[i], fromNS, toNS) {
			continue
		}
		if len(res.Edges) == cap {
			res.Truncated = true
			break
		}
		res.Edges = append(res.Edges, Edge{TNS: abs[i], Level: r.Level, DurNS: r.WidthNS})
	}
	return res, nil
}

// Runs answers the bounded RUN-LENGTH view: the same stream with each
// row's whole-capture index (the coordinate la_measure's edge_i and
// la_diff's run_index speak) and the whole-capture account (n_runs,
// span_ns) riding along. Same window/bounds semantics as Edges.
func Runs(ref CaptureRef, fromNS, toNS uint64, max int) (RunsResult, error) {
	if err := guardEdge(ref); err != nil {
		return RunsResult{}, err
	}
	runs, abs, span, _, err := loadRuns(ref)
	if err != nil {
		return RunsResult{}, err
	}
	res := RunsResult{T0NS: ref.Meta.T0NS, NRuns: len(runs), SpanNS: span}
	cap := clampRows(max)
	for i, r := range runs {
		if !inWindow(abs[i], fromNS, toNS) {
			continue
		}
		if len(res.Runs) == cap {
			res.Truncated = true
			break
		}
		res.Runs = append(res.Runs, RunRow{I: i, TNS: abs[i], Level: r.Level, DurNS: r.WidthNS})
	}
	return res, nil
}

// Hist answers the deterministic duration statistics of one capture:
// a histogram of PULSE widths (every run) or PERIOD spans (each
// adjacent run pair, dur[i]+dur[i+1] — one full cycle of an
// alternating signal regardless of phase), the modal duration
// (mode_ns — the median sample of the modal bin, ties to the lower
// bin), its inverse (est_baud = 1e9/mode_ns: the symbol-rate estimate
// when the modal pulse is one symbol, the frequency itself for period
// kind), and duty for single-pin captures (Σ high / Σ all). Bins
// carry only the populated ranges, ascending; bins param 2..256
// (default 32). All deterministic — no guessing past the data.
func Hist(ref CaptureRef, kind string, bins int) (HistResult, error) {
	if err := guardEdge(ref); err != nil {
		return HistResult{}, err
	}
	if kind != "pulse" && kind != "period" {
		return HistResult{}, fmt.Errorf("la: hist kind %q — want \"pulse\" (every run's width) or \"period\" (each adjacent run pair's span, dur[i]+dur[i+1])", kind)
	}
	if bins == 0 {
		bins = histBinsDefault
	}
	if bins < 2 || bins > histBinsMax {
		return HistResult{}, fmt.Errorf("la: hist bins %d out of domain 2..%d", bins, histBinsMax)
	}
	runs, _, _, _, err := loadRuns(ref)
	if err != nil {
		return HistResult{}, err
	}
	res := HistResult{Kind: kind}

	// single-pin duty: the mask is the line (0|1)
	single := len(ref.Meta.Pins) == 1
	if single {
		var hi, all uint64
		for _, r := range runs {
			all += r.WidthNS
			if r.Level == 1 {
				hi += r.WidthNS
			}
		}
		if all > 0 {
			d := float64(hi) / float64(all)
			res.Duty = &d
		}
	}

	if kind == "pulse" {
		samples := make([]uint64, len(runs))
		for i, r := range runs {
			samples[i] = r.WidthNS
		}
		res.NSamples = len(samples)
		res.Bins, res.ModeNS, res.EstBaud = histBinsOf(samples, bins)
	} else {
		if len(runs) < 2 {
			return res, nil // no pair exists — the honest zero
		}
		samples := make([]uint64, len(runs)-1)
		for i := range samples {
			samples[i] = runs[i].WidthNS + runs[i+1].WidthNS
		}
		res.NSamples = len(samples)
		res.Bins, res.ModeNS, res.EstBaud = histBinsOf(samples, bins)
	}
	return res, nil
}

// histBinsOf bins the samples over [min, max] (equal-width bins,
// half-open [lo, hi) with the top bin closed), answers the populated
// bins ascending, the modal duration (the modal bin's median sample,
// ties to the lower bin), and its inverse. A degenerate range (all
// equal) answers one exact bin. Empty input answers nothing.
func histBinsOf(samples []uint64, bins int) (out []HistBin, modeNS uint64, estBaud float64) {
	if len(samples) == 0 {
		return nil, 0, 0
	}
	lo, hi := samples[0], samples[0]
	for _, s := range samples[1:] {
		if s < lo {
			lo = s
		}
		if s > hi {
			hi = s
		}
	}
	if hi == lo {
		out = []HistBin{{LoNS: lo, HiNS: hi + 1, N: len(samples)}}
		return out, lo, 1e9 / float64(lo)
	}
	width := (hi - lo + uint64(bins) - 1) / uint64(bins) // ceil: every sample lands
	counts := make([]int, bins)
	for _, s := range samples {
		b := int((s - lo) / width)
		if b >= bins {
			b = bins - 1
		}
		counts[b]++
	}
	best, bestN := 0, -1
	for i, n := range counts {
		if n > bestN { // strict: ties keep the LOWER bin
			best, bestN = i, n
		}
		if n > 0 {
			out = append(out, HistBin{LoNS: lo + uint64(i)*width, HiNS: lo + uint64(i+1)*width, N: n})
		}
	}
	// the modal bin's median sample (sorted, lower middle on even)
	var in []uint64
	bLo, bHi := lo+uint64(best)*width, lo+uint64(best+1)*width
	for _, s := range samples {
		if s >= bLo && (s < bHi || (best == bins-1 && s <= hi)) {
			in = append(in, s)
		}
	}
	sort.Slice(in, func(i, j int) bool { return in[i] < in[j] })
	modeNS = in[(len(in)-1)/2]
	return out, modeNS, 1e9 / float64(modeNS)
}

// Measure answers the two-point measurement: dt_ns = t_b − t_a
// (signed), freq_hz = 1e9/dt_ns when positive, and duty = the
// fraction of [a, b) the single-pin line spent high (runs clipped to
// the window pro-rata — mid-run points are honest). A point is an
// absolute t_ns or the start of run edge_i (the Runs view's index);
// exactly one form per point. Reversed or equal points answer dt
// with freq/duty absent and a note.
func Measure(ref CaptureRef, a, b MeasurePoint) (MeasureResult, error) {
	if err := guardEdge(ref); err != nil {
		return MeasureResult{}, err
	}
	runs, abs, _, _, err := loadRuns(ref)
	if err != nil {
		return MeasureResult{}, err
	}
	resolve := func(p MeasurePoint, name string) (uint64, error) {
		if (p.TNS != nil && p.EdgeI != nil) || (p.TNS == nil && p.EdgeI == nil) {
			return 0, fmt.Errorf("la: measure point %s — exactly one of t_ns (absolute) or edge_i (the Runs view's run index) is required", name)
		}
		if p.TNS != nil {
			return *p.TNS, nil
		}
		if *p.EdgeI < 0 || *p.EdgeI >= len(runs) {
			return 0, fmt.Errorf("la: measure point %s edge_i %d out of range 0..%d (the capture's run count)", name, *p.EdgeI, len(runs)-1)
		}
		return abs[*p.EdgeI], nil
	}
	ta, err := resolve(a, "a")
	if err != nil {
		return MeasureResult{}, err
	}
	tb, err := resolve(b, "b")
	if err != nil {
		return MeasureResult{}, err
	}
	res := MeasureResult{DtNS: int64(tb) - int64(ta)}
	if res.DtNS <= 0 {
		res.Note = "dt_ns ≤ 0: freq/duty are defined on a forward window (b after a)"
		return res, nil
	}
	f := 1e9 / float64(res.DtNS)
	res.FreqHz = &f

	if len(ref.Meta.Pins) != 1 {
		return res, nil // masks are not one line — duty is a single-pin fact
	}
	var hi uint64
	for i, r := range runs {
		s, e := abs[i], abs[i]+r.WidthNS
		if e <= ta || s >= tb {
			continue
		}
		// clip the run to [ta, tb)
		if s < ta {
			s = ta
		}
		if e > tb {
			e = tb
		}
		if r.Level == 1 {
			hi += e - s
		}
	}
	// Window honesty (T11 minor; head side added minors wave — the
	// tail note was alone and the ask can overhang BOTH ends): the
	// ask [ta,tb) may exceed the captured extent on either side — the
	// numerator counts only the captured window while dt_ns spans the
	// full ask, so duty reads LOW on whichever side overhangs (the
	// uncaptured span counts as zero-high). One merged note names the
	// capture's measured extent and each overhang; silence means the
	// window sat inside the capture.
	if len(runs) == 0 {
		res.Note = "capture has no runs — duty covers nothing (freq_hz still measures the two points)"
	} else {
		first, last := abs[0], abs[len(runs)-1]+runs[len(runs)-1].WidthNS
		var sides []string
		if ta < first {
			sides = append(sides, fmt.Sprintf("start %dns is before the first edge %dns (the uncaptured head reads as low)", ta, first))
		}
		if tb > last {
			sides = append(sides, fmt.Sprintf("end %dns is past the capture's last edge %dns (the uncaptured tail reads as low)", tb, last))
		}
		switch len(sides) {
		case 2:
			res.Note = fmt.Sprintf("window exceeds the captured extent on both sides — duty covers the captured extent [%d,%d]ns only, so the fraction under-reports: %s; %s", first, last, sides[0], sides[1])
		case 1:
			res.Note = fmt.Sprintf("window exceeds the captured extent — duty covers the captured extent [%d,%d]ns only, so the fraction under-reports: %s", first, last, sides[0])
		}
	}
	d := float64(hi) / float64(res.DtNS)
	res.Duty = &d
	return res, nil
}

// Diff compares two edge captures of one signal: a SEQUENCE walk
// over the runs, first-edge aligned (each capture's run 0 is the
// pre-history its arm opened on — its LEVEL is signal and compares,
// its WIDTH is an arm artifact and never does). Run i of A pairs
// with run i of B: masks must match, then widths within the frozen
// 2µs tolerance (both accuracy bands of the engine — the Desc states
// the resolution boundary). The reply names the first divergence
// (why: level|width|length — length = one stream ended with signal
// left; run_index may then equal the stream's own run count) or
// same=true with the matched span. Same-board only: the engines'
// timing must come from one clock. Both single-pin captures always
// compare (two views of one line); multi-pin captures compare iff
// their pin sets are equal (mask bits must mean the same lines).
func Diff(a, b CaptureRef) (DiffResult, error) {
	if err := guardEdge(a); err != nil {
		return DiffResult{}, err
	}
	if err := guardEdge(b); err != nil {
		return DiffResult{}, err
	}
	if a.Meta.Board != b.Meta.Board {
		return DiffResult{}, fmt.Errorf("la: diff refuses captures from different boards (%s vs %s) — the comparison rides one board's clock; capture both views on the same board (e.g. two edge captures of one transmission)", a.Meta.Board, b.Meta.Board)
	}
	single := len(a.Meta.Pins) == 1 && len(b.Meta.Pins) == 1
	if !single && !intSlicesEqual(a.Meta.Pins, b.Meta.Pins) {
		return DiffResult{}, fmt.Errorf("la: diff refuses pin sets %v vs %v — multi-pin masks compare only when the mask bits mean the same lines (same pins, same order); single-pin captures always compare", a.Meta.Pins, b.Meta.Pins)
	}
	runsA, absA, _, _, err := loadRuns(a)
	if err != nil {
		return DiffResult{}, err
	}
	runsB, _, _, _, err := loadRuns(b)
	if err != nil {
		return DiffResult{}, err
	}
	res := DiffResult{NRunsA: len(runsA), NRunsB: len(runsB), TolNS: diffTolNS}
	if len(runsA) == 0 || len(runsB) == 0 {
		return DiffResult{}, fmt.Errorf("la: diff refuses a capture with no runs (A %d, B %d) — nothing to compare", len(runsA), len(runsB))
	}

	// run 0: the level is signal; the width is the arm's artifact.
	if runsA[0].Level != runsB[0].Level {
		i := 0
		res.Same, res.RunIndex, res.RunIndexB, res.Why = false, &i, &i, "level"
		t := absA[0]
		res.FirstDiffTNS = &t
		return res, nil
	}

	var matched uint64
	for i := 1; ; i++ {
		endA, endB := i >= len(runsA), i >= len(runsB)
		if endA && endB {
			res.Same, res.MatchedNS = true, matched
			return res, nil
		}
		if endA || endB {
			// one stream ended with the other still carrying signal
			res.Same, res.RunIndex, res.RunIndexB, res.Why = false, &i, &i, "length"
			t := absA[len(runsA)-1] + runsA[len(runsA)-1].WidthNS
			if !endA {
				t = absA[i]
			}
			res.FirstDiffTNS = &t
			res.MatchedNS = matched
			return res, nil
		}
		ra, rb := runsA[i], runsB[i]
		if ra.Level != rb.Level {
			res.Same, res.RunIndex, res.RunIndexB, res.Why = false, &i, &i, "level"
			t := absA[i]
			res.FirstDiffTNS = &t
			res.MatchedNS = matched
			return res, nil
		}
		d := ra.WidthNS - rb.WidthNS
		if rb.WidthNS > ra.WidthNS {
			d = rb.WidthNS - ra.WidthNS
		}
		if d > diffTolNS {
			res.Same, res.RunIndex, res.RunIndexB, res.Why = false, &i, &i, "width"
			t := absA[i]
			res.FirstDiffTNS = &t
			res.MatchedNS = matched
			return res, nil
		}
		matched += ra.WidthNS
	}
}

// intSlicesEqual answers ordered equality (pin order IS the mask's
// bit order — order matters).
func intSlicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
