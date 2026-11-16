package la

// levels.go — the LA wave-2 shared LEVEL domain (2026-11-17, LA 增強波 A):
// the one representation the second-batch analysis seats (search/
// activity/bits/adjacent/glitch/eye/align/export-vcd) read — MASK RUNS,
// the same stream the edge engine records ({u8 mask, dur_ns}), with the
// samples capture derived into it (consecutive equal sample bytes fold
// into one mask run at the tick width). One representation, one set of
// mode guards: listen stays the BYTE domain (its seats are la_frames/
// la_repeat/la_decode_uart) and refuses here with the coach.

import (
	"fmt"
)

// guardLevel refuses the seats the level domain cannot read: the
// capture must be edge or samples with its timing facts (edge:
// res_hz + t0_ns; samples: rate_hz + t0_ns — the absolute axis
// derives from t0; a meta without them is a pull that lost its
// ledger, and the honest fix is a re-pull).
func guardLevel(ref CaptureRef) error {
	switch ref.Meta.Mode {
	case "edge":
		if ref.Meta.ResHz <= 0 {
			return fmt.Errorf("la: capture %s lacks res_hz — timing cannot derive (re-pull the object)", ref.ID)
		}
	case "samples":
		if ref.Meta.RateHz <= 0 {
			return fmt.Errorf("la: capture %s lacks rate_hz — the sample axis cannot derive (re-pull the object)", ref.ID)
		}
	case "listen":
		return fmt.Errorf("la: capture %s is mode %q — this seat reads the LEVEL domain (edge or samples captures: run-length levels over time); the listen capture is a BYTE stream (its seats are la_frames/la_repeat/la_decode_uart)", ref.ID, ref.Meta.Mode)
	default:
		return fmt.Errorf("la: capture %s has unknown mode %q", ref.ID, ref.Meta.Mode)
	}
	if ref.Meta.T0NS == 0 {
		return fmt.Errorf("la: capture %s lacks t0_ns (the fw ledger did not land with it) — the absolute axis cannot derive; re-pull the object", ref.ID)
	}
	return nil
}

// levelRuns answers the capture's MASK-RUN stream on the T0-RELATIVE
// axis (run 0 starts at 0 — loadRuns' shape, so every downstream seat
// adds t0 for absolute times exactly like the edge seats) plus the
// absolute origin. Edge: the run records verbatim. Samples: the tick
// stream folded — consecutive equal sample bytes become one run
// {mask = the byte, dur = ticks × tick_ns} (a same-tick multi-channel
// change is one mask boundary, the popcount of the diff is the
// per-channel edge count — activity's own account).
func levelRuns(ref CaptureRef) (runs []Run, t0 uint64, err error) {
	if ref.Meta.Mode == "samples" {
		samples, serr := ReadSamples(ref.DataPath)
		if serr != nil {
			return nil, 0, serr
		}
		tick := tickOf(ref.Meta.RateHz)
		for i := 0; i < len(samples); {
			j := i + 1
			for j < len(samples) && samples[j] == samples[i] {
				j++
			}
			runs = append(runs, Run{Level: int(samples[i]),
				StartNS: uint64(i) * tick, WidthNS: uint64(j-i) * tick})
			i = j
		}
		return runs, uint64(ref.Meta.T0NS), nil
	}
	runs, err = ReadRuns(ref.DataPath)
	if err != nil {
		return nil, 0, err
	}
	return runs, uint64(ref.Meta.T0NS), nil
}

// channelCount answers the capture's channel count: edge = the armed
// pins (mask bit i = pins[i]), samples = the fixed 8-channel DVP bus.
func channelCount(ref CaptureRef) int {
	if ref.Meta.Mode == "samples" {
		return 8
	}
	return len(ref.Meta.Pins)
}

// resolveCh validates one channel selector against the capture's
// channel count. Omitted (0) is a VALID channel (D0 / the first armed
// pin) — the seat's reply echoes ch so the read line is never a guess;
// out of range refuses with the coach naming the domain.
func resolveCh(ref CaptureRef, ch int) error {
	n := channelCount(ref)
	if ch < 0 || ch >= n {
		return fmt.Errorf("la: capture %s: ch %d out of range 0..%d (%s)", ref.ID, ch, n-1, chWord(ref))
	}
	return nil
}

// chWord names the channel axis in coach messages.
func chWord(ref CaptureRef) string {
	if ref.Meta.Mode == "samples" {
		return "D0..D7 = the sample byte's bits"
	}
	return "ch i = pins[i], the armed pins in order"
}

// chRun is one CHANNEL run: the span one channel held its level,
// carried on the mask-run grid — RunI is the mask run whose boundary
// OPENED it (the coordinate la_runs/la_measure's run_index speaks, so
// a hit cites back into the existing views), TNS its absolute start.
type chRun struct {
	RunI  int
	TNS   uint64
	Level int
	DurNS uint64
}

// channelRuns extracts one channel's run sequence from the mask runs:
// consecutive mask runs holding the same bit merge (a neighbour's
// transition never splits this channel's level). Run 0 rides along —
// its width is the arm artifact (the level held since arm; Diff's
// precedent: never a timing fact) and the seats that measure widths
// exempt it, each stating so.
func channelRuns(runs []Run, t0 uint64, ch int) []chRun {
	var out []chRun
	for i, r := range runs {
		lv := (r.Level >> uint(ch)) & 1
		if len(out) > 0 && out[len(out)-1].Level == lv {
			out[len(out)-1].DurNS += r.WidthNS
			continue
		}
		out = append(out, chRun{RunI: i, TNS: t0 + r.StartNS, Level: lv, DurNS: r.WidthNS})
	}
	return out
}

// modalWidth answers the modal duration of the given widths (the
// existing hist machinery — no second implementation) and whether a
// mode exists at all. The majority vote of wave 2: bits' rate
// derivation, glitch's threshold, adjacent's beat window and align's
// period estimate all ride THIS one function.
func modalWidth(widths []uint64) (uint64, bool) {
	if len(widths) == 0 {
		return 0, false
	}
	_, modeNS, _ := histBinsOf(widths, histBinsDefault)
	if modeNS == 0 {
		return 0, false
	}
	return modeNS, true
}
