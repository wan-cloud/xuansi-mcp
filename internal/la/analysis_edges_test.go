package la

// analysis_edges_test.go — the L1 edge-wave analysis seats (T11): the
// six functions over SYNTHETIC run streams (the frozen T10 format
// {u8 mask, u64 dur_ns LE} written byte-exact, the encRun golden
// encoder). The fixtures are the protocols the exam probes: a NEC
// carrier frame (the classic ir_nec shape: 13/13µs halves, lead
// 9ms/4.5ms, bits 560/560|1690), a demodulated baseband view, a PWM
// duty pair, WS2812 and 1-Wire wire views (decode_wire_test.go), and
// the diff trio (same / width-divergence / cross-mode views).

import (
	"encoding/binary"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// the run-stream golden encoder + edge synth refs
// ---------------------------------------------------------------------------

// srun is one synthetic run: the mask (single-pin = 0|1) and its ns
// width.
type srun struct {
	mask int
	dur  uint64
}

// encRun encodes one frozen-format edge record: {u8 mask, u64 dur_ns
// LE} — the T10 ring stream, test-side golden encoder.
func encRun(mask int, dur uint64) []byte {
	b := make([]byte, 9)
	b[0] = byte(mask)
	binary.LittleEndian.PutUint64(b[1:], dur)
	return b
}

func encRuns(runs ...srun) []byte {
	out := make([]byte, 0, 9*len(runs))
	for _, r := range runs {
		out = append(out, encRun(r.mask, r.dur)...)
	}
	return out
}

// synthEdge builds an in-test edge capture: a raw run stream against
// an edge meta (the analysis reads DataPath + Meta only).
func synthEdge(t *testing.T, m Meta, stream []byte) CaptureRef {
	t.Helper()
	if m.Mode != "edge" {
		t.Fatalf("synthEdge: mode %q", m.Mode)
	}
	return CaptureRef{ID: m.CapID, DataPath: writeTemp(t, stream), Meta: m}
}

func edgeMeta(id string, pins []int, t0 int64) Meta {
	return Meta{CapID: id, Board: "97c1ac", Mode: "edge", Pins: pins,
		ResHz: 80_000_000, T0NS: t0, Reason: "stopped"}
}

// ---------------------------------------------------------------------------
// the synthetic protocol fixtures (the exam's protocols, frozen shapes)
// ---------------------------------------------------------------------------

const (
	necHalfNS  = 13_000 // the classic's 13/13µs carrier halves (26µs period ≈ 38.46kHz)
	necLeadUS  = 9_000_000
	necSpaceUS = 4_500_000
	necMarkUS  = 560_000
	necBit0NS  = 560_000
	necBit1NS  = 1_690_000
)

// necCarrierRuns builds the RAW-CARRIER view of one NEC frame — the
// classic ir_nec_frame shape (mark = LED on = line LOW, idle/space =
// HIGH): a mark of ≥us becomes ceil(us/26µs) alternating half-cycles,
// a space one HIGH run; the mark's trailing HIGH half merges with the
// following space (the run stream never carries two equal-level
// neighbors). payload is 4 bytes MSB-first; idleNS is the pre-frame
// HIGH the capture opened on.
func necCarrierRuns(payload [4]byte, idleNS uint64) []srun {
	var seg []srun
	mark := func(us uint64) {
		cycles := (us + 2*necHalfNS - 1) / (2 * necHalfNS)
		for i := uint64(0); i < cycles; i++ {
			seg = append(seg, srun{0, necHalfNS}, srun{1, necHalfNS})
		}
	}
	space := func(us uint64) { seg = append(seg, srun{1, us}) }
	seg = append(seg, srun{1, idleNS})
	mark(necLeadUS)
	space(necSpaceUS)
	for _, b := range payload {
		for i := 7; i >= 0; i-- {
			mark(necMarkUS)
			if b&(1<<uint(i)) != 0 {
				space(necBit1NS)
			} else {
				space(necBit0NS)
			}
		}
	}
	mark(necMarkUS)
	space(1_000_000) // post-frame idle tail
	return mergeRuns(seg)
}

// necBaseRuns builds the DEMODULATED baseband view of the same frame
// (what a real IR head outputs: burst = LOW envelope, gap = HIGH).
func necBaseRuns(payload [4]byte, idleNS uint64) []srun {
	var seg []srun
	seg = append(seg, srun{1, idleNS}, srun{0, necLeadUS}, srun{1, necSpaceUS})
	for _, b := range payload {
		for i := 7; i >= 0; i-- {
			seg = append(seg, srun{0, necMarkUS})
			if b&(1<<uint(i)) != 0 {
				seg = append(seg, srun{1, necBit1NS})
			} else {
				seg = append(seg, srun{1, necBit0NS})
			}
		}
	}
	seg = append(seg, srun{0, necMarkUS}, srun{1, 1_000_000})
	return mergeRuns(seg)
}

// mergeRuns folds equal-mask neighbors (the fw's ring never carries
// two same-level runs back to back — a boundary is a level change).
func mergeRuns(in []srun) []srun {
	var out []srun
	for _, r := range in {
		if len(out) > 0 && out[len(out)-1].mask == r.mask {
			out[len(out)-1].dur += r.dur
			continue
		}
		out = append(out, r)
	}
	return out
}

// pwmRuns builds a clean PWM capture: high/low duty periods, opening
// on a full HIGH run (no pre-history artifact).
func pwmRuns(periods int, hiNS, loNS uint64) []srun {
	out := make([]srun, 0, 2*periods)
	for i := 0; i < periods; i++ {
		out = append(out, srun{1, hiNS}, srun{0, loNS})
	}
	return out
}

// ---------------------------------------------------------------------------
// ReadRuns (the T5-frozen signature, T10's format)
// ---------------------------------------------------------------------------

func TestReadRunsGoldenAndTorn(t *testing.T) {
	stream := encRuns(srun{1, 200_000}, srun{0, 13000}, srun{1, 4_513_000})
	runs, err := ReadRuns(writeTemp(t, stream))
	if err != nil {
		t.Fatalf("ReadRuns: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("runs = %d, want 3", len(runs))
	}
	// StartNS rides the T0-RELATIVE axis (Σ preceding dur — run 0 starts
	// at 0; the analysis seats add meta.t0_ns for the absolute axis).
	want := []Run{{1, 0, 200_000}, {0, 200_000, 13000}, {1, 213_000, 4_513_000}}
	for i, w := range want {
		if runs[i] != w {
			t.Errorf("run %d = %+v, want %+v", i, runs[i], w)
		}
	}
	if _, err := ReadRuns(writeTemp(t, nil)); err != nil || len(runs) != 3 {
		t.Fatalf("empty stream: runs=%v err=%v, want zero runs no error", runs, err)
	}

	// torn tails and trailing garbage name the byte offset
	for name, cut := range map[string]int{"torn record": 4, "torn tail": len(stream) - 1} {
		_, err := ReadRuns(writeTemp(t, stream[:cut]))
		if err == nil || !strings.Contains(err.Error(), "torn") {
			t.Errorf("%s: err = %v, want a torn error", name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Edges / Runs — the bounded views
// ---------------------------------------------------------------------------

// The PWM golden: 25% duty, 20 periods of [H 250µs, L 750µs].
func pwmEdgeRef(t *testing.T) CaptureRef {
	return synthEdge(t, edgeMeta("pwm25", []int{40}, 4_000_000_000), encRuns(pwmRuns(20, 250_000, 750_000)...))
}

func TestEdgesWindowAndBounds(t *testing.T) {
	ref := pwmEdgeRef(t)
	got, err := Edges(ref, 0, 0, 0)
	if err != nil {
		t.Fatalf("Edges: %v", err)
	}
	if got.T0NS != 4_000_000_000 || got.NRuns != 40 || got.Truncated {
		t.Fatalf("echo = %+v, want t0 4e9 / 40 runs / no cut", got)
	}
	// default max bounds the list at 256 rows
	if len(got.Edges) != 40 { // 40 < 256: whole
		t.Fatalf("rows = %d, want 40", len(got.Edges))
	}
	e0 := got.Edges[0]
	if e0.TNS != 4_000_000_000 || e0.Level != 1 || e0.DurNS != 250_000 {
		t.Errorf("edge 0 = %+v, want {4e9, 1, 250000}", e0)
	}
	e1 := got.Edges[1]
	if e1.TNS != 4_000_250_000 || e1.Level != 0 || e1.DurNS != 750_000 {
		t.Errorf("edge 1 = %+v, want {4.00025e9, 0, 750000}", e1)
	}

	// absolute-time window [t0+1ms, t0+3ms): runs 2..5 (run 6 starts
	// exactly at to — excluded, the window is [from, to)).
	got, err = Edges(ref, 4_001_000_000, 4_003_000_000, 0)
	if err != nil {
		t.Fatalf("Edges window: %v", err)
	}
	if len(got.Edges) != 4 || got.Edges[0].TNS != 4_001_000_000 || got.Edges[3].Level != 0 {
		t.Fatalf("window rows = %+v, want the 4 runs starting at +1ms", got.Edges)
	}

	// max truncates with the flag
	got, err = Edges(ref, 0, 0, 2)
	if err != nil || len(got.Edges) != 2 || !got.Truncated {
		t.Fatalf("max=2 → rows=%d truncated=%v err=%v, want 2/true", len(got.Edges), got.Truncated, err)
	}
	// max above the hard cap clamps
	got, err = Edges(ref, 0, 0, 1_000_000)
	if err != nil || len(got.Edges) != 40 || got.Truncated {
		t.Fatalf("oversized max → rows=%d truncated=%v, want 40/false (clamp, no flag)", len(got.Edges), got.Truncated)
	}
	// from past the end: the honest empty view
	got, err = Edges(ref, 5_000_000_000, 0, 0)
	if err != nil || len(got.Edges) != 0 || got.Truncated || got.NRuns != 40 {
		t.Fatalf("past-end window = %+v err=%v, want empty rows, honest echo", got, err)
	}
}

func TestRunsViewCarriesIndicesAndSpan(t *testing.T) {
	ref := pwmEdgeRef(t)
	got, err := Runs(ref, 0, 0, 3)
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	if len(got.Runs) != 3 || got.Truncated != true {
		t.Fatalf("rows=%d truncated=%v, want 3/true", len(got.Runs), got.Truncated)
	}
	r := got.Runs[2]
	if r.I != 2 || r.TNS != 4_001_000_000 || r.Level != 1 || r.DurNS != 250_000 {
		t.Errorf("run row 2 = %+v, want {2, 4.000002e9, 0, 750000}", r)
	}
	// the whole-capture account rides along: 20 periods of 1ms
	if got.NRuns != 40 || got.SpanNS != 20_000_000 {
		t.Errorf("account = n_runs %d span %d, want 40 / 20000000", got.NRuns, got.SpanNS)
	}
}

// The mode guards: the edge seats refuse listen captures with the
// coach; the listen seats still refuse edge captures (T5's pin).
func TestEdgeSeatsModeGuards(t *testing.T) {
	listen := synthRef(t, Meta{Mode: "listen", Baud: 9600, Pin: 3}, encChunk(1, []byte("x")))
	for name, fn := range map[string]func(CaptureRef) error{
		"edges": func(r CaptureRef) error { _, err := Edges(r, 0, 0, 0); return err },
		"runs":  func(r CaptureRef) error { _, err := Runs(r, 0, 0, 0); return err },
		"hist":  func(r CaptureRef) error { _, err := Hist(r, "pulse", 0); return err },
		"measure": func(r CaptureRef) error {
			i := 0
			_, err := Measure(r, MeasurePoint{EdgeI: &i}, MeasurePoint{EdgeI: &i})
			return err
		},
		"diff-a": func(r CaptureRef) error { _, err := Diff(r, r); return err },
		"wire":   func(r CaptureRef) error { _, err := DecodeWire(r, "onewire"); return err },
	} {
		err := fn(listen)
		if err == nil || !strings.Contains(err.Error(), "edge") {
			t.Errorf("%s on a listen capture: err = %v, want the edge-mode coach", name, err)
		}
	}
	// a meta missing t0_ns (the pre-T11 pull shape) refuses honestly
	noT0 := synthEdge(t, Meta{CapID: "x", Board: "b", Mode: "edge", Pins: []int{2}, ResHz: 80_000_000}, encRuns(srun{1, 5}))
	if _, err := Edges(noT0, 0, 0, 0); err == nil || !strings.Contains(err.Error(), "t0_ns") {
		t.Errorf("t0-less meta err = %v, want the t0_ns coach", err)
	}
	// Frames still refuses edge captures with the listen-seat coach
	if _, err := Frames(noT0, 0, 0); err == nil || !strings.Contains(err.Error(), "LISTEN") {
		t.Errorf("Frames on edge err = %v, want the LISTEN-captures coach", err)
	}
}

// ---------------------------------------------------------------------------
// Hist — the deterministic statistics
// ---------------------------------------------------------------------------

func TestHistPulseAndPeriod(t *testing.T) {
	ref := pwmEdgeRef(t)

	// pulse kind: two equal clusters (20×250µs, 20×750µs); the tie
	// breaks to the LOWER bin and mode_ns is the modal bin's median
	// sample — the exact 250000 (a clean signal answers exactly).
	got, err := Hist(ref, "pulse", 32)
	if err != nil {
		t.Fatalf("Hist pulse: %v", err)
	}
	if got.Kind != "pulse" || got.NSamples != 40 {
		t.Fatalf("echo = %+v, want pulse / 40 samples", got)
	}
	if got.ModeNS != 250_000 {
		t.Errorf("mode_ns = %d, want 250000 (tie → lower cluster, median exact)", got.ModeNS)
	}
	if got.EstBaud != 1e9/250_000 {
		t.Errorf("est_baud = %v, want 4000 (1e9/mode_ns)", got.EstBaud)
	}
	if got.Duty == nil || *got.Duty != 0.25 {
		t.Errorf("duty = %v, want 0.25", got.Duty)
	}
	// both clusters present, ascending lo
	if len(got.Bins) != 2 || got.Bins[0].N != 20 || got.Bins[1].N != 20 ||
		got.Bins[0].LoNS != 250_000 || got.Bins[1].LoNS != 734_375 {
		t.Errorf("bins = %+v, want the two clean clusters ascending (bin 31 of 32 tops at 750000)", got.Bins)
	}

	// period kind: one cluster at 1ms (Σ adjacent run pair), the PWM
	// frequency itself as est_baud — the coherent reading for PWM.
	got, err = Hist(ref, "period", 8)
	if err != nil {
		t.Fatalf("Hist period: %v", err)
	}
	if got.NSamples != 39 || got.ModeNS != 1_000_000 || got.EstBaud != 1000 {
		t.Errorf("period = %+v, want 38 samples / mode 1ms / est_baud 1000", got)
	}

	// domain refusals
	if _, err := Hist(ref, "junk", 0); err == nil || !strings.Contains(err.Error(), "kind") {
		t.Errorf("junk kind err = %v, want the kind refusal", err)
	}
	if _, err := Hist(ref, "pulse", 1); err == nil || !strings.Contains(err.Error(), "bins") {
		t.Errorf("bins=1 err = %v, want the bins refusal", err)
	}

	// multi-pin captures carry masks, not one line: duty is absent
	multi := synthEdge(t, edgeMeta("multi", []int{2, 39}, 1), encRuns(srun{0b01, 250_000}, srun{0b11, 750_000}))
	mgot, err := Hist(multi, "pulse", 4)
	if err != nil || mgot.Duty != nil {
		t.Errorf("multi-pin duty = %v err=%v, want absent (masks are not one line)", mgot.Duty, err)
	}
}

// ---------------------------------------------------------------------------
// Measure — two points
// ---------------------------------------------------------------------------

func TestMeasurePoints(t *testing.T) {
	ref := pwmEdgeRef(t)

	// edge_i pair: runs 0 → 4 = exactly two periods
	i0, i4 := 0, 4
	got, err := Measure(ref, MeasurePoint{EdgeI: &i0}, MeasurePoint{EdgeI: &i4})
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	if got.DtNS != 2_000_000 || got.FreqHz == nil || *got.FreqHz != 500 || got.Duty == nil || *got.Duty != 0.25 {
		t.Fatalf("measure = %+v, want dt 2ms / 500Hz / duty .25", got)
	}

	// t_ns points MID-RUN: the window clips runs pro-rata (duty stays
	// exact on a periodic signal: +125µs and +1.125ms are both mid-high)
	t1, t2 := uint64(4_000_125_000), uint64(4_001_125_000)
	got, err = Measure(ref, MeasurePoint{TNS: &t1}, MeasurePoint{TNS: &t2})
	if err != nil {
		t.Fatalf("Measure t_ns: %v", err)
	}
	if got.DtNS != 1_000_000 || *got.FreqHz != 1000 || *got.Duty != 0.25 {
		t.Fatalf("mid-run measure = %+v, want dt 1ms / 1000Hz / duty .25 (clipped)", got)
	}

	// reversed points: dt negative, freq/duty absent with the note
	got, err = Measure(ref, MeasurePoint{EdgeI: &i4}, MeasurePoint{EdgeI: &i0})
	if err != nil || got.DtNS != -2_000_000 || got.FreqHz != nil || got.Duty != nil || got.Note == "" {
		t.Fatalf("reversed = %+v err=%v, want dt −2ms / absents / note", got, err)
	}

	// domain: edge_i out of range; both fields; neither field
	i9 := 99
	if _, err := Measure(ref, MeasurePoint{EdgeI: &i9}, MeasurePoint{EdgeI: &i0}); err == nil || !strings.Contains(err.Error(), "edge_i") {
		t.Errorf("out-of-range err = %v, want the edge_i coach", err)
	}
	if _, err := Measure(ref, MeasurePoint{EdgeI: &i0, TNS: &t1}, MeasurePoint{EdgeI: &i0}); err == nil {
		t.Errorf("double-specified point accepted")
	}
	if _, err := Measure(ref, MeasurePoint{}, MeasurePoint{EdgeI: &i0}); err == nil {
		t.Errorf("empty point accepted")
	}
}

// ---------------------------------------------------------------------------
// Diff — the sequence walk (first-edge aligned)
// ---------------------------------------------------------------------------

func diffRefs(t *testing.T) (a, b, jitter, drift CaptureRef) {
	t.Helper()
	payload := [4]byte{0x00, 0xff, 0x00, 0xff}
	base := necCarrierRuns(payload, 200_000)
	a = synthEdge(t, edgeMeta("necA", []int{2}, 5_000_000_000), encRuns(base...))

	// jittered second view: same waveform, +37/−45ns per run, one pair
	// +1500ns apart — all inside tol (2000ns); idle differs (350µs).
	var j []srun
	for i, r := range base {
		d := r.dur
		if i%2 == 1 {
			d += 37
		} else {
			d -= 45
		}
		j = append(j, srun{r.mask, d})
	}
	j[0] = srun{1, 350_000} // the arm artifact: never compared
	j[7] = srun{base[7].mask, base[7].dur + 1500}
	jitter = synthEdge(t, edgeMeta("necB", []int{39}, 9_100_000_000), encRuns(j...))

	// drift: run 5 stretched past tol — divergence at 5
	d5 := append([]srun(nil), base...)
	d5[5] = srun{d5[5].mask, d5[5].dur + 5000}
	drift = synthEdge(t, edgeMeta("necC", []int{2}, 5_500_000_000), encRuns(d5...))
	b = jitter
	return
}

func TestDiffSameAndDivergence(t *testing.T) {
	a, b, _, drift := diffRefs(t)

	got, err := Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if !got.Same || got.Why != "" || got.RunIndex != nil {
		t.Fatalf("same-view diff = %+v, want same over the frame", got)
	}
	if got.NRunsA != got.NRunsB || got.TolNS != 2000 {
		t.Errorf("account = %+v", got)
	}
	// matched_ns = the whole post-alignment span (Σ dur[1..] of A)
	var span uint64
	for _, r := range necCarrierRuns([4]byte{0x00, 0xff, 0x00, 0xff}, 200_000)[1:] {
		span += r.dur
	}
	if got.MatchedNS != span {
		t.Errorf("matched_ns = %d, want %d", got.MatchedNS, span)
	}

	// width drift at run 5: first_diff_t = t0A + Σdur[0..4], why=width
	got, err = Diff(a, drift)
	if err != nil {
		t.Fatalf("Diff drift: %v", err)
	}
	if got.Same || got.Why != "width" || got.RunIndex == nil || *got.RunIndex != 5 || got.RunIndexB == nil || *got.RunIndexB != 5 {
		t.Fatalf("drift diff = %+v, want why=width at index 5 both", got)
	}
	var expectT uint64
	for _, r := range necCarrierRuns([4]byte{0x00, 0xff, 0x00, 0xff}, 200_000)[:5] {
		expectT += r.dur
	}
	if got.FirstDiffTNS == nil || *got.FirstDiffTNS != 5_000_000_000+expectT {
		t.Errorf("first_diff_t = %v, want %d (absolute, capture A)", got.FirstDiffTNS, 5_000_000_000+expectT)
	}
}

// The two-VIEW case the exam probes: carrier vs demodulated baseband
// of the same frame — the lead burst's first carrier half-cycle is
// where the views genuinely disagree (width), run 1 past the aligned
// first edge.
func TestDiffCarrierVsBaseDivergesAtLead(t *testing.T) {
	payload := [4]byte{0x00, 0xff, 0x00, 0xff}
	a := synthEdge(t, edgeMeta("car", []int{2}, 5_000_000_000), encRuns(necCarrierRuns(payload, 200_000)...))
	b := synthEdge(t, edgeMeta("base", []int{39}, 5_000_000_000), encRuns(necBaseRuns(payload, 200_000)...))
	got, err := Diff(a, b)
	if err != nil {
		t.Fatalf("Diff views: %v", err)
	}
	if got.Same || got.Why != "width" || got.RunIndex == nil || *got.RunIndex != 1 {
		t.Fatalf("views diff = %+v, want why=width at run 1 (13µs vs 9ms lead)", got)
	}
}

// Length divergence: one capture longer than the other after a common
// prefix — run_index rides past the shorter stream's end (honest
// marker, stated in the Desc).
func TestDiffLengthCase(t *testing.T) {
	short := encRuns(pwmRuns(4, 250_000, 750_000)...)
	long := encRuns(pwmRuns(6, 250_000, 750_000)...)
	a := synthEdge(t, edgeMeta("s", []int{2}, 1_000_000_000), short)
	b := synthEdge(t, edgeMeta("l", []int{2}, 2_000_000_000), long)
	got, err := Diff(a, b)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if got.Same || got.Why != "length" || got.RunIndex == nil || *got.RunIndex != 8 || *got.RunIndexB != 8 {
		t.Fatalf("length diff = %+v, want why=length at index 8 (past A's end)", got)
	}
	if got.NRunsA != 8 || got.NRunsB != 12 || got.MatchedNS != 3_750_000 {
		t.Errorf("length account = %+v", got)
	}
}

// The frozen refusals: cross-board (the coach names the constraint),
// and pin-set mismatch on multi-pin captures (masks not comparable).
func TestDiffRefusals(t *testing.T) {
	a, b, _, _ := diffRefs(t)
	cross := synthEdge(t, Meta{CapID: "x", Board: "d75884", Mode: "edge", Pins: []int{2}, ResHz: 80_000_000, T0NS: 1}, encRuns(srun{1, 5}))
	if _, err := Diff(a, cross); err == nil || !strings.Contains(err.Error(), "same board") {
		t.Errorf("cross-board err = %v, want the same-board coach", err)
	}
	_ = b
	multiA := synthEdge(t, edgeMeta("mA", []int{2, 39}, 1), encRuns(srun{0b01, 5}, srun{0b11, 5}))
	multiB := synthEdge(t, edgeMeta("mB", []int{4, 40}, 1), encRuns(srun{0b01, 5}, srun{0b11, 5}))
	if _, err := Diff(multiA, multiB); err == nil || !strings.Contains(err.Error(), "pins") {
		t.Errorf("pin-set mismatch err = %v, want the pins coach", err)
	}
	// equal pin sets ARE comparable (mask bits mean the same lines)
	multiC := synthEdge(t, edgeMeta("mC", []int{2, 39}, 1), encRuns(srun{0b01, 5}, srun{0b11, 5}))
	if got, err := Diff(multiA, multiC); err != nil || !got.Same {
		t.Errorf("equal pin sets diff = %+v err=%v, want same", got, err)
	}
	// empty capture refuses honestly
	empty := synthEdge(t, edgeMeta("e", []int{2}, 1), nil)
	if _, err := Diff(a, empty); err == nil || !strings.Contains(err.Error(), "no runs") {
		t.Errorf("empty diff err = %v, want the no-runs refusal", err)
	}
}
