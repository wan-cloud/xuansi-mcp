package la

// decode_wire_test.go — the wire decoders over synthetic run views:
// WS2812 frames (T0H/T1H around the 450ns threshold + the reset gap)
// and 1-Wire reset/presence + write slots (the frozen thresholds).

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// WS2812
// ---------------------------------------------------------------------------

// wsBits encodes one bit as the (high, low) run pair.
func wsBits(out []srun, bit int) []srun {
	if bit == 1 {
		return append(out, srun{1, 900}, srun{0, 350})
	}
	return append(out, srun{1, 350}, srun{0, 900})
}

func ws2812Stream(bytes ...byte) []srun {
	var out []srun
	out = append(out, srun{0, 100_000}) // idle low before the frame
	for _, b := range bytes {
		for i := 7; i >= 0; i-- {
			out = wsBits(out, int(b>>uint(i)&1))
		}
	}
	out = append(out, srun{0, 300_000}) // the reset/latch gap
	return out
}

func TestDecodeWireWS2812(t *testing.T) {
	// two LEDs, wire order G,R,B per LED
	ref := synthEdge(t, edgeMeta("ws", []int{6}, 8_000_000_000),
		encRuns(ws2812Stream(0x12, 0x34, 0x56, 0xab, 0xcd, 0xef)...))
	got, err := DecodeWire(ref, "ws2812")
	if err != nil {
		t.Fatalf("DecodeWire ws2812: %v", err)
	}
	if got.Proto != "ws2812" || len(got.Txns) != 1 || got.Truncated {
		t.Fatalf("reply = %+v, want one frame", got)
	}
	tx := got.Txns[0]
	if tx.TNS != 8_000_100_000 { // the first bit's high run (after the idle)
		t.Errorf("t_ns = %d, want 8.0001e9 (first bit, idle excluded)", tx.TNS)
	}
	if tx.Kind != "frame" || tx.NBits != 48 || tx.NBytes != 6 || tx.NLEDs != 2 || tx.RemainderBits != 0 {
		t.Errorf("counts = %+v, want frame/48/6/2/0", tx)
	}
	if tx.Bytes != "123456abcdef" {
		t.Errorf("bytes = %q, want the wire-order GRB hex", tx.Bytes)
	}
	if tx.Latched == nil || !*tx.Latched {
		t.Errorf("latched = %v, want true (the reset gap closed it)", tx.Latched)
	}

	// a frame with a torn LED count (a mid-frame capture end): the
	// remainder is honest, latched=false (no reset gap seen)
	var torn []srun
	torn = append(torn, srun{0, 100_000})
	torn = wsBits(torn, 1) // 1 bit only
	ref2 := synthEdge(t, edgeMeta("ws2", []int{6}, 8_000_000_000), encRuns(torn...))
	got, err = DecodeWire(ref2, "ws2812")
	if err != nil {
		t.Fatalf("DecodeWire torn: %v", err)
	}
	if len(got.Txns) != 1 || got.Txns[0].NBits != 1 || got.Txns[0].NLEDs != 0 || got.Txns[0].RemainderBits != 1 {
		t.Fatalf("torn = %+v, want 1 bit / 0 LEDs / remainder 1", got.Txns)
	}
	if got.Txns[0].Latched == nil || *got.Txns[0].Latched {
		t.Errorf("torn latched = %v, want false", got.Txns[0].Latched)
	}
}

// ---------------------------------------------------------------------------
// 1-Wire
// ---------------------------------------------------------------------------

// onewireStream: idle high, reset 600µs low, 30µs release, presence
// 90µs low, 40µs recovery, then the ROM command 0x33 LSB-first
// (bit 1 = 8µs low + 70µs recovery; bit 0 = 65µs low + 10µs).
func onewireStream(bits ...int) []srun {
	out := []srun{{1, 200_000}, {0, 600_000}, {1, 30_000}, {0, 90_000}, {1, 40_000}}
	for _, b := range bits {
		if b == 1 {
			out = append(out, srun{0, 8_000}, srun{1, 70_000})
		} else {
			out = append(out, srun{0, 65_000}, srun{1, 10_000})
		}
	}
	out = append(out, srun{1, 1_000_000}) // idle tail
	return out
}

func TestDecodeWireOneWire(t *testing.T) {
	// 0x33 = 0b00110011, LSB first → 1,1,0,0,1,1,0,0
	ref := synthEdge(t, edgeMeta("ow", []int{5}, 2_000_000_000),
		encRuns(onewireStream(1, 1, 0, 0, 1, 1, 0, 0)...))
	got, err := DecodeWire(ref, "onewire")
	if err != nil {
		t.Fatalf("DecodeWire onewire: %v", err)
	}
	if len(got.Txns) != 2 || got.Truncated {
		t.Fatalf("txns = %+v, want reset + one bytes row", got)
	}
	rst, byt := got.Txns[0], got.Txns[1]
	if rst.Kind != "reset" || rst.TNS != 2_000_200_000 || rst.Presence == nil || !*rst.Presence {
		t.Errorf("reset txn = %+v, want reset at +200µs with presence", rst)
	}
	if byt.Kind != "bytes" || byt.NBits != 8 || byt.Bytes != "33" {
		t.Errorf("bytes txn = %+v, want 8 bits assembling 0x33 LSB-first", byt)
	}

	// no presence: the release is followed by a short/high-only gap
	noPres := []srun{{1, 200_000}, {0, 600_000}, {1, 200_000}, {0, 65_000}, {1, 1_000_000}}
	ref2 := synthEdge(t, edgeMeta("ow2", []int{5}, 2_000_000_000), encRuns(noPres...))
	got, err = DecodeWire(ref2, "onewire")
	if err != nil {
		t.Fatalf("DecodeWire no-presence: %v", err)
	}
	// the 65µs low at +800µs from release start (200µs after release,
	// past the 120µs presence window) is a write-0 slot, not presence
	if got.Txns[0].Presence == nil || *got.Txns[0].Presence {
		t.Errorf("presence = %v, want false (the 65µs low is a slot, not a presence)", got.Txns[0].Presence)
	}
	if len(got.Txns) != 2 || got.Txns[1].NBits != 1 || got.Txns[1].Bytes != "" {
		t.Errorf("slot row = %+v, want 1 bit (a byte never completed — bytes empty, n_bits honest)", got.Txns)
	}
}

// The run-0 doctrine, onewire case (the T11-review deferred exposure):
// run 0's WIDTH is an arm artifact and never signal. A line armed
// MID-SLOT makes run 0 a short LOW whose width (<32µs) would assemble a
// phantom 1-bit ahead of the real transaction — unreachable via a
// normal idle-high arm, but the same one-decoder-over fix WS2812 took
// in fix round 1. Run 0 must never open a row, never be a bit, never
// reset-detect on its arm-truncated width.
func TestDecodeWireOneWireRun0ArmArtifact(t *testing.T) {
	// arm mid-slot: a 20µs LOW run 0 (arm-truncated write-1 slot tail),
	// then the real traffic — reset + 0x0A's eight slots (LSB first).
	stream := append([]srun{{0, 20_000}}, onewireStream(0, 1, 0, 1, 0, 0, 0, 0)...)
	ref := synthEdge(t, edgeMeta("owr0", []int{5}, 2_000_000_000), encRuns(stream...))
	got, err := DecodeWire(ref, "onewire")
	if err != nil {
		t.Fatalf("DecodeWire onewire run-0: %v", err)
	}
	if len(got.Txns) != 2 || got.Txns[0].Kind != "reset" || got.Txns[1].Kind != "bytes" {
		t.Fatalf("txns = %+v, want reset + one bytes row (no run-0 phantom)", got)
	}
	if got.Txns[1].NBits != 8 || got.Txns[1].Bytes != "0a" {
		t.Errorf("bytes txn = %+v, want exactly the 8 real slots assembling 0x0a (the arm-artifact run 0 contributes nothing)", got.Txns[1])
	}
	// a reset-shaped run 0 (line low at arm THROUGH a reset): the width
	// is arm-truncated and must not open a reset row either — the first
	// honest row is the presence-windowed slot traffic after it.
}

// The domain guards: unknown proto, and multi-pin captures refuse (a
// wire protocol rides ONE line).
func TestDecodeWireGuards(t *testing.T) {
	ref := synthEdge(t, edgeMeta("g", []int{5}, 1), encRuns(srun{1, 5}))
	if _, err := DecodeWire(ref, "nec"); err == nil || !strings.Contains(err.Error(), "proto") {
		t.Errorf("junk proto err = %v, want the proto refusal", err)
	}
	multi := synthEdge(t, edgeMeta("gm", []int{2, 39}, 1), encRuns(srun{1, 5}))
	if _, err := DecodeWire(multi, "ws2812"); err == nil || !strings.Contains(err.Error(), "ONE line") {
		t.Errorf("multi-pin err = %v, want the ONE-line refusal", err)
	}
}

// The run-0 doctrine (the one Diff established this wave): run 0's
// WIDTH is an arm artifact — the time from arm to the first edge — and
// never carries signal. WS2812 lines that sit HIGH at arm make this
// load-bearing for the decoder: a HIGH run 0 must never become a bit.
//
// Case A — armed on idle-high, the frame's reset low follows: run 0
// (the high arm-gap) must NOT emit a phantom 1-bit latched frame
// before the real one.
func TestDecodeWireWS2812Run0ArmGapHigh(t *testing.T) {
	var out []srun
	out = append(out, srun{1, 5_000_000}) // the HIGH arm gap (idle-high line)
	out = append(out, srun{0, 300_000})   // the frame's reset low
	for _, b := range []byte{0x12, 0x34, 0x56, 0xab, 0xcd, 0xef} {
		for i := 7; i >= 0; i-- {
			out = wsBits(out, int(b>>uint(i)&1))
		}
	}
	out = append(out, srun{0, 300_000}) // the latch gap
	ref := synthEdge(t, edgeMeta("wsA", []int{6}, 8_000_000_000), encRuns(out...))

	got, err := DecodeWire(ref, "ws2812")
	if err != nil {
		t.Fatalf("DecodeWire case A: %v", err)
	}
	if len(got.Txns) != 1 {
		t.Fatalf("case A txns = %d (%+v), want 1 — run 0's arm-gap high must not open a phantom frame", len(got.Txns), got.Txns)
	}
	tx := got.Txns[0]
	if tx.NBits != 48 || tx.Bytes != "123456abcdef" || tx.Latched == nil || !*tx.Latched {
		t.Errorf("case A frame = %+v, want the real 48-bit frame latched", tx)
	}
}

// Case B — the arm lands so the idle-high RUN 0 MERGES with the first
// bit's high (no edge between them): run 0's width is the artifact
// and the first bit is honestly UNKNOWABLE — skipped, not guessed.
// The frame's first true bit here is 0 (0x12's MSB); run 0's ms-scale
// width would force it to 1 without the exclusion.
func TestDecodeWireWS2812Run0MergedWithFirstBit(t *testing.T) {
	var out []srun
	// run 0 = 5ms idle-high fused with the first bit's T0H (350ns):
	// the engine sees ONE high run — its width is meaningless as a bit
	out = append(out, srun{1, 5_000_350})
	out = append(out, srun{0, 900}) // the first bit's T0L
	for _, b := range []byte{0x12, 0x34, 0x56, 0xab, 0xcd, 0xef} {
		for i := 7; i >= 0; i-- {
			if b == 0x12 && i == 7 {
				continue // the first bit already rode run 0's merge
			}
			out = wsBits(out, int(b>>uint(i)&1))
		}
	}
	out = append(out, srun{0, 300_000})
	ref := synthEdge(t, edgeMeta("wsB", []int{6}, 8_000_000_000), encRuns(out...))

	got, err := DecodeWire(ref, "ws2812")
	if err != nil {
		t.Fatalf("DecodeWire case B: %v", err)
	}
	if len(got.Txns) != 1 {
		t.Fatalf("case B txns = %d, want 1", len(got.Txns))
	}
	tx := got.Txns[0]
	if tx.NBits != 47 || tx.NLEDs != 1 || tx.RemainderBits != 23 {
		t.Errorf("case B counts = %+v, want 47 bits / 1 LED / remainder 23 (the first bit is honestly gone)", tx)
	}
	// the surviving bits are the frame's bits 1..47, MSB-first per byte
	if tx.Bytes != "2468ad579b" {
		t.Errorf("case B bytes = %q, want the shifted assembly 2468ad579b", tx.Bytes)
	}
	if tx.Latched == nil || !*tx.Latched {
		t.Errorf("case B latched = %v, want true (the reset gap closed it)", tx.Latched)
	}
}
