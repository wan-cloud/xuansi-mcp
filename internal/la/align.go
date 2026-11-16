package la

// align.go — la_align (LA 增強波 A): the two-capture alignment seat —
// do two captures of one board share a beat, at what phase, and how
// far does that phase drift across the span? Same-board only (the
// Diff precedent: one clock), listen refused (the level domain). The
// algorithm, stated once: each side's beat = the majority-vote modal
// gap between its events (mask-run boundaries — run 0's t0 is the arm
// instant, never an event); the event lists cross-correlate on a
// circular axis of one beat (bounded bins, every pair one vote —
// deterministic integer counting); the winning phase refines against
// the NEAREST-event pairs; the matched pairs then fold into equal
// time segments whose mean offsets carry the drift. lag_ns is the
// NEAREST-PHASE alignment — inside ±half the beat; whole beats the
// two arms add are invisible from the streams alone (stated, not
// guessed past).

import (
	"fmt"
	"sort"
)

// The align bounds.
const (
	alignEventsMax = 4096 // per-side event cap (the head; stated in the reply)
	alignPhaseBins = 512  // the circular correlation's bin count
	alignSegments  = 8    // the drift account's segment count
)

// AlignSegment is one equal span of capture A's axis: the matched
// pairs whose a-event fell inside it, and their mean offset (b − a −
// lag, wrapped to ±half the beat — the phase drift at that instant).
type AlignSegment struct {
	FromNS   uint64  `json:"from_ns"`
	N        int     `json:"n"`
	OffsetNS float64 `json:"offset_ns"`
}

// AlignResult is the reply. Matched is the pair count the account
// sits on; TolNS the match band (beat/8); DriftPPM present iff two or
// more segments matched — the linear drift estimate across their span
// (honest limit: a drift past ±half the beat wraps, so the estimate
// saturates there).
type AlignResult struct {
	PeriodANS uint64         `json:"period_a_ns"`
	PeriodBNS uint64         `json:"period_b_ns"`
	LagNS     int64          `json:"lag_ns"`
	TolNS     uint64         `json:"tol_ns"`
	Matched   int            `json:"matched"`
	NA        int            `json:"n_events_a"`
	NB        int            `json:"n_events_b"`
	DriftPPM  *float64       `json:"drift_ppm,omitempty"`
	Segments  []AlignSegment `json:"segments"`
}

// Align compares two level captures of one board. Both single-pin
// captures always compare (two views of one line); multi-pin captures
// compare iff their pin sets are equal (the Diff rule — mask bits
// must mean the same lines).
func Align(a, b CaptureRef) (AlignResult, error) {
	if err := guardLevel(a); err != nil {
		return AlignResult{}, err
	}
	if err := guardLevel(b); err != nil {
		return AlignResult{}, err
	}
	if a.Meta.Board != b.Meta.Board {
		return AlignResult{}, fmt.Errorf("la: align refuses captures from different boards (%s vs %s) — the alignment rides one board's clock; capture both views on the same board (e.g. two edge captures of one transmission)", a.Meta.Board, b.Meta.Board)
	}
	single := len(a.Meta.Pins) == 1 && len(b.Meta.Pins) == 1
	if !single && !intSlicesEqual(a.Meta.Pins, b.Meta.Pins) {
		return AlignResult{}, fmt.Errorf("la: align refuses pin sets %v vs %v — multi-pin masks compare only when the mask bits mean the same lines (same pins, same order); single-pin captures always compare", a.Meta.Pins, b.Meta.Pins)
	}

	evA, err := alignEvents(a)
	if err != nil {
		return AlignResult{}, err
	}
	evB, err := alignEvents(b)
	if err != nil {
		return AlignResult{}, err
	}
	res := AlignResult{NA: len(evA), NB: len(evB)}
	if len(evA) < 4 || len(evB) < 4 {
		return AlignResult{}, fmt.Errorf("la: align needs ≥4 events per side (A %d, B %d) — too few transitions to vote a beat", len(evA), len(evB))
	}

	pa, okA := modalBeat(evA)
	pb, okB := modalBeat(evB)
	if !okA || !okB {
		return AlignResult{}, fmt.Errorf("la: align refuses — no dominant beat (the modal event gap must hold ≥25%% of the gaps; A periodic %v, B %v): the alignment rides a repeating rhythm, and these captures do not carry one", okA, okB)
	}
	res.PeriodANS, res.PeriodBNS = pa, pb

	// the circular correlation: every pair votes its (b − a) mod beat
	// into the phase bins; the winning bin's center is the seed lag.
	P := int64(pa)
	nBins := int64(alignPhaseBins)
	if P < nBins {
		nBins = P // a beat under 512ns: 1ns bins
	}
	votes := make([]int, nBins)
	for _, ai := range evA {
		for _, bj := range evB {
			d := (bj - ai) % P
			if d < 0 {
				d += P
			}
			votes[int(d*nBins/P)]++
		}
	}
	best, bestN := 0, -1
	for k, v := range votes {
		if v > bestN { // strict: ties keep the LOWER bin
			best, bestN = k, v
		}
	}
	step := P / nBins
	lag := int64(best)*step + step/2
	tol := P / 8
	res.TolNS = uint64(tol)

	// the significance gate: the winning phase's vote density must
	// beat uniform scatter 5× — a random pair cloud puts ~total/nBins
	// votes in every bin and no alignment exists to find.
	if total := len(evA) * len(evB); bestN*int(nBins) < 5*total {
		return AlignResult{}, fmt.Errorf("la: align refuses — no dominant phase (the best of %d bins holds %d of %d pair votes; a real alignment concentrates them): the captures share no beat", nBins, bestN, total)
	}

	// the refinement: pair each a with its NEAREST b (around a+lag),
	// keep the pairs inside the band, settle the lag on their mean
	// residual — one pass to vote, one to hold the settled account.
	// The second gate rides the matched set: a real alignment's
	// residuals CLUSTER (≥3/4 inside the inner half-band); a chance
	// pairing scatters uniformly across ±tol and refuses.
	matched := matchPairs(evA, evB, lag, P, tol)
	tight := 0
	for _, m := range matched {
		if m.r >= -tol/2 && m.r <= tol/2 {
			tight++
		}
	}
	if len(matched) < 2 || tight*4 < len(matched)*3 {
		return AlignResult{}, fmt.Errorf("la: align found no alignment within the %dns band (matched %d of %d A-events, %d tightly) — the captures do not share the beat (period_a %dns, period_b %dns)", tol, len(matched), len(evA), tight, pa, pb)
	}
	var sum int64
	for _, m := range matched {
		sum += m.r
	}
	lag += sum / int64(len(matched))

	// the settled account: re-match at the refined lag (UNWRAPPED —
	// the true offset can sit a whole beat from the wrapped reading,
	// and the raw-residual match must not cross that seam), fold into
	// the eight equal segments of A's axis.
	matched = matchPairs(evA, evB, lag, P, tol)
	res.Matched = len(matched)
	res.LagNS = wrapC(lag, P) // the reply rides the nearest-phase reading

	spanA := evA[len(evA)-1] - evA[0]
	segLen := spanA / alignSegments
	if segLen == 0 {
		segLen = 1
	}
	var nSeg [alignSegments]int
	var sumSeg [alignSegments]int64
	for _, m := range matched {
		k := int((m.a - evA[0]) / segLen)
		if k >= alignSegments {
			k = alignSegments - 1
		}
		nSeg[k]++
		sumSeg[k] += m.r
	}
	var first, last = -1, -1
	for k := 0; k < alignSegments; k++ {
		if nSeg[k] == 0 {
			continue
		}
		off := float64(sumSeg[k]) / float64(nSeg[k])
		res.Segments = append(res.Segments, AlignSegment{
			FromNS: uint64(evA[0] + int64(k)*segLen), N: nSeg[k], OffsetNS: off})
		if first < 0 {
			first = k
		}
		last = k
	}
	if first >= 0 && last > first {
		c1 := float64(evA[0]) + (float64(first)+0.5)*float64(segLen)
		c2 := float64(evA[0]) + (float64(last)+0.5)*float64(segLen)
		if c2 > c1 {
			o1 := float64(sumSeg[first]) / float64(nSeg[first])
			o2 := float64(sumSeg[last]) / float64(nSeg[last])
			d := (o2 - o1) / (c2 - c1) * 1e6
			res.DriftPPM = &d
		}
	}
	return res, nil
}

// alignEvents answers the capture's event instants (absolute ns):
// every mask-run boundary past run 0 (run 0's start is the arm
// instant t0, never a signal transition), capped at alignEventsMax
// (the head — the reply's n_events_a/b carry the used count).
func alignEvents(ref CaptureRef) ([]int64, error) {
	runs, t0, err := levelRuns(ref)
	if err != nil {
		return nil, err
	}
	n := len(runs) - 1
	if n > alignEventsMax {
		n = alignEventsMax
	}
	out := make([]int64, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, int64(t0+runs[i].StartNS))
	}
	return out, nil
}

// modalBeat answers the majority-vote modal gap between consecutive
// events, with the significance verdict: the modal bin must hold
// ≥25% of the gaps (and ≥4 gaps must exist) for the beat to be real.
func modalBeat(ev []int64) (uint64, bool) {
	if len(ev) < 4 {
		return 0, false
	}
	gaps := make([]uint64, len(ev)-1)
	for i := range gaps {
		if ev[i+1] <= ev[i] {
			return 0, false // a non-forward pair: no honest gap axis
		}
		gaps[i] = uint64(ev[i+1] - ev[i])
	}
	mode, ok := modalWidth(gaps)
	if !ok {
		return 0, false
	}
	bins, _, _ := histBinsOf(gaps, histBinsDefault)
	nModal := 0
	for _, b := range bins {
		if mode >= b.LoNS && mode < b.HiNS {
			nModal = b.N
			break
		}
	}
	if nModal*4 < len(gaps) {
		return 0, false
	}
	return mode, true
}

// alignPair is one matched event pair: the a-event's instant and the
// centered residual (b − a − lag).
type alignPair struct {
	a, r int64
}

// matchPairs pairs each a-event with its NEAREST b-event around
// a+lag, keeping the pairs whose RAW residual (b − a − lag, NO wrap)
// sits inside the band — the raw difference is the locality guard: a
// far-away b wraps into any phase, so wrapping before the tolerance
// test would match events tens of milliseconds apart (the wave-A
// red: the clustered-B case matched all 39). The residual only wraps
// (to ±P/2) where a value legitimately rides the fold, never here.
func matchPairs(evA, evB []int64, lag, P, tol int64) []alignPair {
	var out []alignPair
	for _, ai := range evA {
		j := nearestIdx(evB, ai+lag)
		r := evB[j] - ai - lag
		if r >= -tol && r <= tol {
			out = append(out, alignPair{ai, r})
		}
	}
	return out
}

// nearestIdx answers ev's index whose value is nearest to target
// (ev ascending; ties to the lower).
func nearestIdx(ev []int64, target int64) int {
	j := sort.Search(len(ev), func(i int) bool { return ev[i] >= target })
	if j == 0 {
		return 0
	}
	if j == len(ev) {
		return len(ev) - 1
	}
	if target-ev[j-1] <= ev[j]-target {
		return j - 1
	}
	return j
}

// wrapC wraps v into [−P/2, P/2) (the centered residue).
func wrapC(v, P int64) int64 {
	v %= P
	if v < -P/2 {
		v += P
	} else if v >= P/2 {
		v -= P
	}
	return v
}
