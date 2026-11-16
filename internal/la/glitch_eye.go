package la

// glitch_eye.go — la_glitch + la_eye (LA 增強波 A): the run-quality
// seats over the channel's own majority-vote unit. Glitch lists the
// outlier runs SHORTER than modal_run × ratio (the sub-unit slivers a
// clean protocol never emits — bounce, coupling, tear). Eye folds the
// alternation by its main period and answers the eye numbers as pure
// integers (the LLM measures nothing: min-high, min-low, the
// high-width peak-to-peak spread, the fold count).

import (
	"fmt"
)

// GlitchRow is one outlier run: the mask-run index that opened it
// (la_runs' coordinate), its absolute start, level, width.
type GlitchRow struct {
	RunI  int    `json:"run_i"`
	TNS   uint64 `json:"t_ns"`
	Level int    `json:"level"`
	DurNS uint64 `json:"dur_ns"`
}

// GlitchResult is the bounded reply: Count carries the FULL outlier
// count, Rows the bounded head (256 rows, the cut flagged).
type GlitchResult struct {
	Ch          int         `json:"ch"`
	ModeNS      uint64      `json:"modal_run_ns"`
	Ratio       float64     `json:"ratio"`
	ThresholdNS uint64      `json:"threshold_ns"`
	Count       int         `json:"count"`
	Rows        []GlitchRow `json:"rows"`
	Truncated   bool        `json:"truncated,omitempty"`
}

// glitchRatioDefault is the outlier threshold's default multiplier
// (a run shorter than half the modal unit).
const glitchRatioDefault = 0.5

// Glitch answers the sub-unit outlier account. ratio (0 < r ≤ 1,
// default 0.5) scales the threshold; the modal unit is the channel's
// run-width majority vote (run 0 — the arm artifact — neither votes
// nor lists). A capture whose every run sits at the mode answers the
// honest zero.
func Glitch(ref CaptureRef, ch int, ratio float64) (GlitchResult, error) {
	if ratio == 0 {
		ratio = glitchRatioDefault
	}
	if ratio <= 0 || ratio > 1 {
		return GlitchResult{}, fmt.Errorf("la: glitch ratio %v out of domain (0, 1] — the threshold is modal_run × ratio (default %v)", ratio, glitchRatioDefault)
	}
	runs, t0, err := levelRunsChecked(ref, ch)
	if err != nil {
		return GlitchResult{}, err
	}
	cr := channelRuns(runs, t0, ch)
	if len(cr) < 2 {
		return GlitchResult{}, fmt.Errorf("la: capture %s ch %d carries no signal runs — no modal unit to threshold against", ref.ID, ch)
	}
	widths := make([]uint64, len(cr)-1)
	for i := 1; i < len(cr); i++ {
		widths[i-1] = cr[i].DurNS
	}
	mode, ok := modalWidth(widths)
	if !ok {
		return GlitchResult{}, fmt.Errorf("la: capture %s ch %d has no run widths to vote a modal unit from", ref.ID, ch)
	}
	threshold := uint64(float64(mode) * ratio)
	if threshold == 0 {
		threshold = 1 // a sub-ns mode: everything at or above 1ns passes
	}
	res := GlitchResult{Ch: ch, ModeNS: mode, Ratio: ratio, ThresholdNS: threshold}
	for i := 1; i < len(cr); i++ {
		if cr[i].DurNS >= threshold {
			continue
		}
		res.Count++
		if len(res.Rows) < rowsDefault {
			res.Rows = append(res.Rows, GlitchRow{RunI: cr[i].RunI, TNS: cr[i].TNS, Level: cr[i].Level, DurNS: cr[i].DurNS})
		}
	}
	res.Truncated = res.Count > len(res.Rows)
	return res, nil
}

// EyeResult is the folded account — every number computed here, none
// measured downstream.
type EyeResult struct {
	Ch        int     `json:"ch"`
	PeriodNS  uint64  `json:"period_ns"` // the fold axis: the majority-vote modal period (adjacent run-pair spans)
	NFold     int     `json:"n_fold"`    // the folded sample count (the pair count the statistics sit on)
	MinHighNS *uint64 `json:"min_high_ns,omitempty"`
	MaxHighNS *uint64 `json:"max_high_ns,omitempty"`
	HighPPNS  *uint64 `json:"high_pp_ns,omitempty"` // the high-width peak-to-peak spread — the eye's vertical jitter
	MinLowNS  *uint64 `json:"min_low_ns,omitempty"`
	MaxLowNS  *uint64 `json:"max_low_ns,omitempty"`
	LowPPNS   *uint64 `json:"low_pp_ns,omitempty"`
}

// Eye folds one channel's alternation by its main period: the period
// is the majority vote over ADJACENT run-pair spans (one full cycle
// regardless of phase — the hist "period" discipline), the widths'
// extrema the eye openings, the spread the jitter. Runs past run 0
// only (the arm artifact never folds). A line that never reaches one
// of the levels answers that level's columns absent — stated, not
// zero-filled.
func Eye(ref CaptureRef, ch int) (EyeResult, error) {
	runs, t0, err := levelRunsChecked(ref, ch)
	if err != nil {
		return EyeResult{}, err
	}
	cr := channelRuns(runs, t0, ch)
	if len(cr) < 3 { // run 0 + at least one full high/low pair
		return EyeResult{}, fmt.Errorf("la: capture %s ch %d carries fewer than one full alternation — nothing to fold (need ≥2 flips)", ref.ID, ch)
	}
	sig := cr[1:]
	spans := make([]uint64, len(sig)-1)
	for i := range spans {
		spans[i] = sig[i].DurNS + sig[i+1].DurNS
	}
	period, ok := modalWidth(spans)
	if !ok {
		return EyeResult{}, fmt.Errorf("la: capture %s ch %d has no period spans to vote a fold axis from", ref.ID, ch)
	}
	res := EyeResult{Ch: ch, PeriodNS: period, NFold: len(spans)}
	var minH, maxH, minL, maxL uint64
	var haveH, haveL bool
	for _, r := range sig {
		if r.Level == 1 {
			if !haveH || r.DurNS < minH {
				minH = r.DurNS
			}
			if !haveH || r.DurNS > maxH {
				maxH = r.DurNS
			}
			haveH = true
		} else {
			if !haveL || r.DurNS < minL {
				minL = r.DurNS
			}
			if !haveL || r.DurNS > maxL {
				maxL = r.DurNS
			}
			haveL = true
		}
	}
	if haveH {
		pp := maxH - minH
		res.MinHighNS, res.MaxHighNS, res.HighPPNS = &minH, &maxH, &pp
	}
	if haveL {
		pp := maxL - minL
		res.MinLowNS, res.MaxLowNS, res.LowPPNS = &minL, &maxL, &pp
	}
	return res, nil
}
