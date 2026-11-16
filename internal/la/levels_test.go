package la

// levels_test.go — the wave-A level domain (2026-11-17): the samples→
// mask-runs derivation (tick-exact golden over the sampwm fixture),
// the channel-run extraction with mask-run indices, and the channel
// selector's domain guards.

import (
	"strings"
	"testing"
)

func TestLevelRunsDerivesSamples(t *testing.T) {
	ref := fixtureRef(t, "sampwm")
	runs, t0, err := levelRuns(ref)
	if err != nil {
		t.Fatalf("levelRuns: %v", err)
	}
	if t0 != 7_000_000_000 {
		t.Fatalf("t0 = %d, want 7e9", t0)
	}
	if len(runs) != 31 {
		t.Fatalf("runs = %d, want 31 (the sampwm fold)", len(runs))
	}
	want := []Run{
		{0x00, 0, 5000}, {0x01, 5000, 1000}, {0x03, 6000, 4000}, {0x02, 10000, 1000},
		{0x00, 11000, 4000}, {0x01, 15000, 1000}, {0x03, 16000, 4000}, {0x02, 20000, 1000},
	}
	for i, w := range want {
		if runs[i] != w {
			t.Errorf("run %d = %+v, want %+v", i, runs[i], w)
		}
	}
	// the tail: the last period ends inside the high block (no 02)
	last := runs[len(runs)-1]
	if last.Level != 0x03 || last.StartNS != 76_000 || last.WidthNS != 4000 {
		t.Errorf("tail run = %+v, want {0x03, 76000, 4000}", last)
	}
}

func TestLevelRunsEdgePassthrough(t *testing.T) {
	ref := fixtureRef(t, "edgepwm")
	runs, t0, err := levelRuns(ref)
	if err != nil {
		t.Fatalf("levelRuns: %v", err)
	}
	if t0 != 4_000_000_000 || len(runs) != 40 || runs[0] != (Run{1, 0, 250_000}) {
		t.Fatalf("edge passthrough = t0 %d n %d r0 %+v", t0, len(runs), runs[0])
	}
}

func TestChannelRunsCarryMaskIndices(t *testing.T) {
	// a 2-pin mask stream where pin B's transitions split pin A's
	// runs — A's channel runs must merge over them, each citing the
	// mask run that opened it
	ref := synthEdge(t, edgeMeta("m2", []int{2, 3}, 1_000), encRuns(
		srun{0b00, 100}, srun{0b01, 50}, srun{0b11, 100}, srun{0b10, 40}, srun{0b11, 60},
	))
	runs, t0, err := levelRuns(ref)
	if err != nil {
		t.Fatal(err)
	}
	a := channelRuns(runs, t0, 0)
	if len(a) != 4 { // 0b10's bit0 is 0: ch0 splits at it, ch1 doesn't
		t.Fatalf("ch0 runs = %d (%+v), want 4", len(a), a)
	}
	if a[0].RunI != 0 || a[0].Level != 0 || a[0].DurNS != 100 { // the 00 lead
		t.Errorf("ch0 run 0 = %+v, want {RunI 0, 0, 100}", a[0])
	}
	if a[1].RunI != 1 || a[1].Level != 1 || a[1].DurNS != 150 { // 01,11
		t.Errorf("ch0 run 1 = %+v, want {RunI 1, 1, 150}", a[1])
	}
	if a[2].RunI != 3 || a[2].Level != 0 || a[2].DurNS != 40 { // 10 (bit0 fell)
		t.Errorf("ch0 run 2 = %+v, want {RunI 3, 0, 40}", a[2])
	}
	if a[3].RunI != 4 || a[3].DurNS != 60 {
		t.Errorf("ch0 run 3 = %+v, want {RunI 4, …60}", a[3])
	}
	b := channelRuns(runs, t0, 1)
	if len(b) != 2 || b[0].DurNS != 150 || b[1].DurNS != 200 { // ch1 runs through 10 (bit1 high)
		t.Fatalf("ch1 runs = %+v, want 2 runs 150/200", b)
	}
	if b[1].RunI != 2 { // ch1's high opened at mask run 2 (0b11)
		t.Errorf("ch1 run 1 = %+v, want RunI 2", b[1])
	}
}

func TestGuardLevelAndResolveCh(t *testing.T) {
	listen := synthRef(t, Meta{Mode: "listen", Baud: 9600, Pin: 3}, encChunk(1, []byte("x")))
	for name, fn := range map[string]func(CaptureRef) error{
		"levels": func(r CaptureRef) error { _, _, err := levelRunsChecked(r, 0); return err },
	} {
		err := fn(listen)
		if err == nil || !strings.Contains(err.Error(), "LEVEL") {
			t.Errorf("%s on listen: err = %v, want the LEVEL-domain coach", name, err)
		}
	}
	// a listen capture missing t0/baud facts still refuses on the facts
	noT0 := synthEdge(t, Meta{CapID: "x", Board: "b", Mode: "edge", Pins: []int{2}, ResHz: 80_000_000}, encRuns(srun{1, 5}))
	if _, _, err := levelRunsChecked(noT0, 0); err == nil || !strings.Contains(err.Error(), "t0_ns") {
		t.Errorf("t0-less err = %v, want the t0_ns coach", err)
	}
	sampNoRate := synthRef(t, Meta{Mode: "samples", T0NS: 5}, []byte{0, 1})
	if _, _, err := levelRunsChecked(sampNoRate, 0); err == nil || !strings.Contains(err.Error(), "rate_hz") {
		t.Errorf("rate-less err = %v, want the rate_hz coach", err)
	}
	// the channel domain: single-pin 0..0, samples 0..7
	pwm := fixtureRef(t, "edgepwm")
	if err := resolveCh(pwm, 1); err == nil || !strings.Contains(err.Error(), "0..0") {
		t.Errorf("resolveCh(1) on 1-pin = %v, want the 0..0 coach", err)
	}
	sam := fixtureRef(t, "sampwm")
	if err := resolveCh(sam, 8); err == nil || !strings.Contains(err.Error(), "0..7") {
		t.Errorf("resolveCh(8) on samples = %v, want the 0..7 coach", err)
	}
	if err := resolveCh(sam, 7); err != nil {
		t.Errorf("resolveCh(7) on samples = %v, want ok (D7 exists)", err)
	}
}
