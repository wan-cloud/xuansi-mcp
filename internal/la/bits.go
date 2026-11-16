package la

// bits.go — la_bits (LA 增強波 A): the UNIVERSAL DECODER — one
// channel's waveform resampled onto a bit grid, answered as a bit
// string the LLM reads directly (self-converging: frame the bits,
// find the protocol yourself; the seat computes every NUMBER, you
// never measure). The grid's rate is the run-width majority vote (the
// existing hist machinery via modalWidth — no second estimator) or an
// explicit rate; every run that does not land on a whole grid cell is
// a VIOLATION, counted, with the first one's instant named.

import (
	"fmt"
	"strings"
)

// The bits bounds: segment length and the whole-reply bit cap.
const (
	bitsSegLen  = 256
	bitsMaxBits = 4096
	bitsRateMax = 1_000_000_000 // 1ns grid floor — no finer rate exists
)

// BitsSegment is one bounded run of the bit string: the segment's
// start instant and its ≤256 bits.
type BitsSegment struct {
	I    int    `json:"i"`
	TNS  uint64 `json:"t_ns"`
	Bits string `json:"bits"`
}

// BitsResult is the bounded reply. RateNS is the grid unit actually
// applied (rate_derived=true means it came from the run-width
// majority vote, not an ask); NBits carries the FULL bit count — the
// segments are the bounded head (truncated flags the cut). The grid
// anchors at the channel's FIRST TRANSITION (run 0 is the arm
// artifact: its width is pre-history, never a bit, and it is exempt
// from the violation account — Diff's precedent).
type BitsResult struct {
	Ch               int           `json:"ch"`
	RateNS           uint64        `json:"rate_ns"`
	RateDerived      bool          `json:"rate_derived"`
	FirstTNS         uint64        `json:"first_t_ns"`
	NBits            int           `json:"n_bits"`
	Segments         []BitsSegment `json:"segments"`
	Violations       int           `json:"violations"`
	FirstViolationNS *uint64       `json:"first_violation_t_ns,omitempty"`
	Truncated        bool          `json:"truncated,omitempty"`
}

// Bits resamples one channel onto the bit grid. rate (bits/s)
// omitted → the majority-vote derivation; given → the exact grid
// (refused beyond 1e9 — the integer-ns floor — and on samples beyond
// the capture's own rate_hz: bits finer than the sampling cannot
// exist).
func Bits(ref CaptureRef, ch int, rate int) (BitsResult, error) {
	runs, t0, err := levelRunsChecked(ref, ch)
	if err != nil {
		return BitsResult{}, err
	}
	cr := channelRuns(runs, t0, ch)
	if len(cr) < 2 {
		return BitsResult{}, fmt.Errorf("la: capture %s ch %d never transitions — no edge to anchor the bit grid (a static line carries no bits)", ref.ID, ch)
	}

	var rateNS uint64
	derived := false
	if rate > 0 {
		if rate > bitsRateMax {
			return BitsResult{}, fmt.Errorf("la: bits rate %d — the grid is integer nanoseconds, so the domain is 1..%d bits/s", rate, bitsRateMax)
		}
		if ref.Meta.Mode == "samples" && rate > ref.Meta.RateHz {
			return BitsResult{}, fmt.Errorf("la: bits rate %d is finer than the capture's own sampling (rate_hz %d) — bits the sampler cannot resolve do not exist; ask at or below the capture's rate", rate, ref.Meta.RateHz)
		}
		rateNS = uint64(1_000_000_000 / rate)
	} else {
		// the majority vote over the signal's own run widths (run 0 —
		// the arm artifact — never votes)
		widths := make([]uint64, len(cr)-1)
		for i := 1; i < len(cr); i++ {
			widths[i-1] = cr[i].DurNS
		}
		mode, ok := modalWidth(widths)
		if !ok {
			return BitsResult{}, fmt.Errorf("la: capture %s ch %d has no run widths to vote a rate from — give rate explicitly", ref.ID, ch)
		}
		rateNS, derived = mode, true
	}
	if rateNS == 0 {
		return BitsResult{}, fmt.Errorf("la: bits rate %d is too fine for the 1ns grid", rate)
	}

	// the violation account: every run (past run 0) must sit on whole
	// grid cells — width ≈ k×rateNS within rateNS/4 (the sub-cell
	// residual a clean NRZ never carries; the 25% band is stated).
	tol := rateNS / 4
	res := BitsResult{Ch: ch, RateNS: rateNS, RateDerived: derived, FirstTNS: cr[1].TNS}
	for i := 1; i < len(cr); i++ {
		k := (cr[i].DurNS + rateNS/2) / rateNS // round
		diff := uint64(0)
		switch {
		case k < 1:
			diff = cr[i].DurNS // sub-unit: the whole width is the residual
		default:
			if want := k * rateNS; cr[i].DurNS > want {
				diff = cr[i].DurNS - want
			} else {
				diff = want - cr[i].DurNS
			}
		}
		if k < 1 || diff > tol {
			res.Violations++
			if res.FirstViolationNS == nil {
				t := cr[i].TNS
				res.FirstViolationNS = &t
			}
		}
	}

	// the grid: anchored at the first transition, one bit per cell,
	// sampled at the cell midpoint (level-hold — a mid past the last
	// run reads the held level).
	anchor := cr[1].TNS
	end := cr[len(cr)-1].TNS + cr[len(cr)-1].DurNS
	if end <= anchor {
		return BitsResult{}, fmt.Errorf("la: capture %s ch %d has a degenerate span — no whole bit cell fits", ref.ID, ch)
	}
	nBitsFull := int((end - anchor) / rateNS)
	res.NBits = nBitsFull
	nOut := nBitsFull
	if nOut > bitsMaxBits {
		nOut = bitsMaxBits
		res.Truncated = true
	}

	var sb strings.Builder
	ri := 0 // the run walk's cursor
	for k := 0; k < nOut; k++ {
		mid := anchor + uint64(k)*rateNS + rateNS/2
		for ri < len(cr)-1 && cr[ri].TNS+cr[ri].DurNS <= mid {
			ri++
		}
		sb.WriteByte(byte('0' + cr[ri].Level))
		if sb.Len() == bitsSegLen {
			i := len(res.Segments)
			res.Segments = append(res.Segments, BitsSegment{
				I: i, TNS: anchor + uint64(i)*bitsSegLen*rateNS, Bits: sb.String()})
			sb.Reset()
		}
	}
	if sb.Len() > 0 {
		i := len(res.Segments)
		res.Segments = append(res.Segments, BitsSegment{
			I: i, TNS: anchor + uint64(i)*bitsSegLen*rateNS, Bits: sb.String()})
	}
	return res, nil
}
