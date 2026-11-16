package la

// adjacent_test.go — la_adjacent (LA 增強波 A): the cross-channel
// accompaniment goldens over sampwm (the one-tick-delayed copy), the
// derived window (the subject's own beat ×2), and the refusals.

import (
	"strings"
	"testing"
)

func TestAdjacentSamplesGolden(t *testing.T) {
	ref := fixtureRef(t, "sampwm")
	// D0 flips every 5 ticks → modal gap 5000ns → window 10000ns; D1
	// (the delayed copy) flips 1 tick after every D0 flip → every
	// flip accompanied → count 15, ratio 1.0; the six idle channels
	// never accompany
	got, err := Adjacent(ref, 0, 0)
	if err != nil {
		t.Fatalf("Adjacent: %v", err)
	}
	if got.Ch != 0 || got.WindowNS != 10_000 || got.Flips != 15 {
		t.Fatalf("echo = ch %d window %d flips %d, want 0/10000/15", got.Ch, got.WindowNS, got.Flips)
	}
	if len(got.Peers) != 7 {
		t.Fatalf("peers = %d, want 7", len(got.Peers))
	}
	top := got.Peers[0]
	if top.Ch != 1 || top.Count != 15 || top.Ratio != 1.0 {
		t.Fatalf("top peer = %+v, want ch1 15/1.0", top)
	}
	for _, p := range got.Peers[1:] {
		if p.Count != 0 || p.Ratio != 0 {
			t.Errorf("idle peer %+v, want zeros", p)
		}
	}
	// the deterministic order: count desc, then ch asc over the ties
	for i := 2; i < len(got.Peers); i++ {
		if got.Peers[i-1].Ch > got.Peers[i].Ch {
			t.Errorf("tie order broke at %d: %+v", i, got.Peers)
		}
	}
}

func TestAdjacentEdgeMultiPin(t *testing.T) {
	// two pins: pin1 answers pin0's rises 1µs in (a handshake shape)
	var runs []srun
	emit := func(v0, v1 int, dur uint64) {
		runs = append(runs, srun{v0 | v1<<1, dur})
	}
	for i := 0; i < 6; i++ {
		emit(1, 0, 1000) // ch0 high, ch1 low
		emit(1, 1, 200)  // ch1 answers high
		emit(0, 1, 1000)
		emit(0, 0, 7800) // both low to the next round
	}
	ref := synthEdge(t, edgeMeta("hand", []int{2, 3}, 1_000), encRuns(runs...))
	got, err := Adjacent(ref, 0, 0)
	if err != nil {
		t.Fatalf("Adjacent edge: %v", err)
	}
	// ch0's runs fold to H1200/L8800 per 10ms round → 11 flips whose
	// gaps alternate 8800/1200 five-and-five — the TIE breaks to the
	// LOWER bin (the frozen hist rule, pinned here) → modal gap 1200,
	// window 2400; every ch0 flip still has ch1's answer (±1000ns or
	// exact) inside → all 11 accompanied
	if got.Flips != 11 || got.Peers[0].Ch != 1 || got.Peers[0].Count != 11 || got.Peers[0].Ratio != 1.0 {
		t.Fatalf("ch0 = flips %d peer %+v, want 11 flips all accompanied", got.Flips, got.Peers[0])
	}
	if got.WindowNS != 2_400 {
		t.Fatalf("window = %d, want 2×1200 (the tie→lower modal gap)", got.WindowNS)
	}
}

func TestAdjacentWindowOverrideAndRefusals(t *testing.T) {
	ref := fixtureRef(t, "sampwm")
	got, err := Adjacent(ref, 0, 1500) // tight: only the +1-tick copy lands
	if err != nil || got.WindowNS != 1500 || got.Peers[0].Count != 15 {
		t.Fatalf("tight window = %+v err %v, want the copy still inside", got, err)
	}
	// single-channel captures refuse the cross-channel fact
	pwm := fixtureRef(t, "edgepwm")
	if _, err := Adjacent(pwm, 0, 0); err == nil || !strings.Contains(err.Error(), "≥2") {
		t.Errorf("1-pin: err = %v, want the ≥2 channels coach", err)
	}
	// a subject that never flips cannot derive a window (bit0 rides
	// constant-0 while bit1 moves)
	flat := synthRef(t, Meta{Mode: "samples", RateHz: 1_000_000, T0NS: 9}, []byte{0x04, 0x04, 0x06, 0x04})
	if _, err := Adjacent(flat, 0, 0); err == nil || !strings.Contains(err.Error(), "give window_ns") {
		t.Errorf("no-beat subject: err = %v, want the window_ns coach", err)
	}
	// the listen capture refuses with the level-domain coach
	mod := fixtureRef(t, "modbus")
	if _, err := Adjacent(mod, 0, 0); err == nil || !strings.Contains(err.Error(), "LEVEL") {
		t.Errorf("listen: err = %v, want the LEVEL coach", err)
	}
	// ch domain
	if _, err := Adjacent(ref, 8, 0); err == nil || !strings.Contains(err.Error(), "0..7") {
		t.Errorf("ch 8: err = %v, want the channel coach", err)
	}
}
