package la

import (
	"strings"
	"testing"
)

// The 7E1 golden: frame 1's bytes all satisfy even parity over their 8
// bits (0x00/0x41/0xE1/0xFF — popcounts 0/2/4/8); frame 2 carries one
// deliberately broken byte (0x80, popcount 1). Under an E claim the
// frames answer true/false; under an O claim the same bytes answer
// false/false (their popcounts are all even); with no claim the column
// is absent — parity is recomputed against the received bytes, never
// guessed.
func TestDecodeUARTParityGolden(t *testing.T) {
	ref := fixtureRef(t, "parity7e")

	got, err := DecodeUART(ref, DecodeUARTParams{Parity: "E"})
	if err != nil {
		t.Fatalf("DecodeUART E: %v", err)
	}
	if got.Parity != "E" || got.Baud != 115200 || got.IdleNS != 347_220 {
		t.Fatalf("echo = %+v, want parity E / 115200 / 347220", got)
	}
	if len(got.Frames) != 2 {
		t.Fatalf("frames = %d, want 2", len(got.Frames))
	}
	f0, f1 := got.Frames[0], got.Frames[1]
	if f0.ParityOK == nil || !*f0.ParityOK {
		t.Errorf("frame 0 parity_ok = %v, want true", f0.ParityOK)
	}
	if f1.ParityOK == nil || *f1.ParityOK {
		t.Errorf("frame 1 parity_ok = %v, want false (the broken 0x80)", f1.ParityOK)
	}
	if f0.TNS != 2_000_000_000 || f1.TNS != 2_001_041_660 || f1.GapBeforeNS != 694_440 {
		t.Errorf("frames = {+%d} {+%d gap %d}, want t 2e9 / 2001041660 gap 694440",
			f0.TNS, f1.TNS, f1.GapBeforeNS)
	}

	odd, err := DecodeUART(ref, DecodeUARTParams{Parity: "O"})
	if err != nil {
		t.Fatalf("DecodeUART O: %v", err)
	}
	for i, f := range odd.Frames {
		if f.ParityOK == nil || *f.ParityOK {
			t.Errorf("O claim frame %d parity_ok = %v, want false (popcounts are even)", i, f.ParityOK)
		}
	}

	plain, err := DecodeUART(ref, DecodeUARTParams{})
	if err != nil {
		t.Fatalf("DecodeUART plain: %v", err)
	}
	if plain.Parity != "" {
		t.Errorf("parity echo = %q, want \"\"", plain.Parity)
	}
	for i, f := range plain.Frames {
		if f.ParityOK != nil {
			t.Errorf("frame %d parity_ok = %v, want the column absent without a claim", i, *f.ParityOK)
		}
	}
}

// A baud override is a segmentation input, not just an echo: at 19200 the
// symbol time stretches (10e9/19200 = 520833), the back-to-back
// continuation of the modbus golden re-reads as a 4166664ns gap — past
// the new 2083332 threshold — and the four chunks answer four frames.
func TestDecodeUARTBaudOverrideResegments(t *testing.T) {
	ref := fixtureRef(t, "modbus")
	got, err := DecodeUART(ref, DecodeUARTParams{Baud: 19200})
	if err != nil {
		t.Fatalf("DecodeUART 19200: %v", err)
	}
	if got.Baud != 19200 || got.IdleNS != 2_083_332 {
		t.Fatalf("echo = baud %d idle %d, want 19200 / 2083332", got.Baud, got.IdleNS)
	}
	if len(got.Frames) != 4 {
		t.Fatalf("frames = %d, want 4 (the 9600-read continuation splits at 19200)", len(got.Frames))
	}
	if got.Frames[1].TNS != 1_008_333_328 || got.Frames[1].GapBeforeNS != 4_166_664 {
		t.Errorf("frame 1 = t %d gap %d, want t 1008333328 gap 4166664",
			got.Frames[1].TNS, got.Frames[1].GapBeforeNS)
	}
}

func TestDecodeUARTGuardsAndBounds(t *testing.T) {
	ref := fixtureRef(t, "modbus")
	if _, err := DecodeUART(ref, DecodeUARTParams{Parity: "X"}); err == nil || !strings.Contains(err.Error(), "parity") {
		t.Errorf("parity X err = %v, want a parity-domain error", err)
	}
	if _, err := DecodeUART(ref, DecodeUARTParams{Baud: 100}); err == nil || !strings.Contains(err.Error(), "baud") {
		t.Errorf("baud 100 err = %v, want a baud-domain error", err)
	}
	if _, err := DecodeUART(ref, DecodeUARTParams{Baud: 6_000_000}); err == nil || !strings.Contains(err.Error(), "baud") {
		t.Errorf("baud 6000000 err = %v, want a baud-domain error", err)
	}

	// the bounded reply rides decode too: 300 frames cut at 256, flagged.
	var stream []byte
	for i := 0; i < 300; i++ {
		stream = append(stream, encChunk(uint64(i)*1_000_000, []byte{byte(i)})...)
	}
	got, err := DecodeUART(synthRef(t, Meta{Baud: 115200, IdleSyms: 4}, stream), DecodeUARTParams{})
	if err != nil {
		t.Fatalf("DecodeUART bound: %v", err)
	}
	if len(got.Frames) != 256 || !got.Truncated {
		t.Fatalf("frames = %d truncated=%v, want 256 / true", len(got.Frames), got.Truncated)
	}

	// the empty capture answers an honest empty list.
	empty, err := DecodeUART(fixtureRef(t, "empty"), DecodeUARTParams{})
	if err != nil {
		t.Fatalf("DecodeUART empty: %v", err)
	}
	if len(empty.Frames) != 0 || empty.Truncated {
		t.Fatalf("empty = %+v, want zero frames / no truncation", empty)
	}
}
