package la

import (
	"path/filepath"
	"strings"
	"testing"
)

// fixtureRef loads one committed synthetic capture directory (meta.json +
// data.bin, hand-pinned chunk records — the frozen stream format written
// byte-exact, not through the store's machinery).
func fixtureRef(t *testing.T, name string) CaptureRef {
	t.Helper()
	dir := filepath.Join("testdata", name)
	m, err := loadMeta(dir)
	if err != nil {
		t.Fatalf("%s: loadMeta: %v", name, err)
	}
	return CaptureRef{
		ID:       m.CapID,
		Dir:      dir,
		DataPath: filepath.Join(dir, "data.bin"),
		MetaPath: filepath.Join(dir, "meta.json"),
		Meta:     m,
	}
}

// synthRef builds an in-test capture: a raw chunk stream against a meta,
// no object directory needed (the analysis reads DataPath + Meta only).
func synthRef(t *testing.T, m Meta, stream []byte) CaptureRef {
	t.Helper()
	if m.Mode == "" {
		m.Mode = "listen"
	}
	return CaptureRef{DataPath: writeTemp(t, stream), Meta: m}
}

// The golden modbus-shape capture (baud 9600, idle_syms 4 → idle_ns
// 4×1041666 = 4166664): chunk 1 rides back-to-back behind chunk 0 (gap 0,
// one frame), chunk 2's gap is EXACTLY the idle threshold (equality must
// split — the >= pin), chunk 3 sits at two thresholds out.
func TestFramesGoldenModbus(t *testing.T) {
	ref := fixtureRef(t, "modbus")
	got, err := Frames(ref, 0, 0)
	if err != nil {
		t.Fatalf("Frames: %v", err)
	}
	if got.Baud != 9600 || got.IdleNS != 4_166_664 {
		t.Fatalf("echo = baud %d idle_ns %d, want 9600 / 4166664", got.Baud, got.IdleNS)
	}
	if got.Truncated {
		t.Errorf("truncated = true on a 3-frame capture")
	}
	want := []struct {
		tNS uint64
		gap uint64
		n   int
		hex string
	}{
		{1_000_000_000, 0, 10, "01030000000ac40c55aa"},
		{1_014_583_324, 4_166_664, 7, "01031400010002"},
		{1_030_208_314, 8_333_328, 4, "deadbeef"},
	}
	if len(got.Frames) != len(want) {
		t.Fatalf("frames = %d, want %d (%+v)", len(got.Frames), len(want), got.Frames)
	}
	for i, w := range want {
		f := got.Frames[i]
		if f.TNS != w.tNS || f.GapBeforeNS != w.gap || f.NBytes != w.n || f.Bytes == nil ||
			!strings.EqualFold(hexOf(f.Bytes), w.hex) {
			t.Errorf("frame %d = {t=%d gap=%d n=%d bytes=%s}, want {t=%d gap=%d n=%d bytes=%s}",
				i, f.TNS, f.GapBeforeNS, f.NBytes, hexOf(f.Bytes), w.tNS, w.gap, w.n, w.hex)
		}
	}

	// ch: omitted (0) and the capture's own pin pass; any other channel errors.
	if _, err := Frames(ref, 38, 0); err != nil {
		t.Errorf("ch == meta.Pin (38) rejected: %v", err)
	}
	if _, err := Frames(ref, 39, 0); err == nil || !strings.Contains(err.Error(), "channel") {
		t.Errorf("ch = 39 err = %v, want a channel mismatch error", err)
	}

	// idle override: a threshold above every gap merges all 21 bytes into one frame.
	merged, err := Frames(ref, 0, 20_000_000)
	if err != nil {
		t.Fatalf("Frames override: %v", err)
	}
	if len(merged.Frames) != 1 || merged.IdleNS != 20_000_000 {
		t.Fatalf("merged = %d frames idle %d, want 1 frame idle 20000000", len(merged.Frames), merged.IdleNS)
	}
	if f := merged.Frames[0]; f.TNS != 1_000_000_000 || f.NBytes != 21 || len(f.Bytes) != 21 {
		t.Errorf("merged frame = %+v, want t=1e9 n=21 whole", f)
	}
}

func TestFramesGoldenPeriodic(t *testing.T) {
	ref := fixtureRef(t, "periodic")
	got, err := Frames(ref, 0, 0)
	if err != nil {
		t.Fatalf("Frames: %v", err)
	}
	if len(got.Frames) != 8 {
		t.Fatalf("frames = %d, want 8", len(got.Frames))
	}
	if got.IdleNS != 347_220 { // 4 × (10e9/115200 = 86805)
		t.Errorf("idle_ns = %d, want 347220", got.IdleNS)
	}
	for i := 1; i < 8; i++ {
		f := got.Frames[i]
		if w := uint64(i) * 1_041_660; f.TNS != 5_000_000_000+w {
			t.Errorf("frame %d t_ns = %d, want %d", i, f.TNS, 5_000_000_000+w)
		}
		if f.GapBeforeNS != 694_440 { // uniform 2×idle spacing
			t.Errorf("frame %d gap = %d, want 694440", i, f.GapBeforeNS)
		}
		if f.NBytes != 4 || len(f.Bytes) != 4 {
			t.Errorf("frame %d payload = %d bytes, want 4", i, f.NBytes)
		}
	}
	if got.Frames[0].GapBeforeNS != 0 {
		t.Errorf("first frame gap = %d, want 0 (nothing precedes it)", got.Frames[0].GapBeforeNS)
	}
}

func TestFramesEmptyCapture(t *testing.T) {
	ref := fixtureRef(t, "empty")
	got, err := Frames(ref, 0, 0)
	if err != nil {
		t.Fatalf("Frames on empty capture: %v", err)
	}
	if len(got.Frames) != 0 || got.Truncated || got.Baud != 115200 || got.IdleNS != 347_220 {
		t.Fatalf("empty = %+v, want zero frames / honest echo / no truncation", got)
	}
}

func TestFramesSingleChunk(t *testing.T) {
	ref := synthRef(t, Meta{Baud: 9600, IdleSyms: 4, Pin: 2}, encChunk(42, []byte("abc")))
	got, err := Frames(ref, 0, 0)
	if err != nil {
		t.Fatalf("Frames: %v", err)
	}
	if len(got.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(got.Frames))
	}
	f := got.Frames[0]
	if f.TNS != 42 || f.GapBeforeNS != 0 || f.NBytes != 3 || string(f.Bytes) != "abc" {
		t.Errorf("frame = %+v, want {42, 0, 3, \"abc\"}", f)
	}
}

// The bounded-reply pins: the frame list cuts at 256 with truncated:true,
// and a frame whose payload crosses 256 bytes answers a 256-byte preview
// beside the full n_bytes — never an unbounded structure.
func TestFramesTruncationBounds(t *testing.T) {
	m := Meta{Baud: 115200, IdleSyms: 4, Pin: 5}
	var stream []byte
	for i := 0; i < 300; i++ { // 300 chunks 1ms apart: every one its own frame
		stream = append(stream, encChunk(uint64(i)*1_000_000, []byte{byte(i)})...)
	}
	got, err := Frames(synthRef(t, m, stream), 0, 0)
	if err != nil {
		t.Fatalf("Frames: %v", err)
	}
	if len(got.Frames) != 256 || !got.Truncated {
		t.Fatalf("frames = %d truncated=%v, want 256 / true", len(got.Frames), got.Truncated)
	}

	big := make([]byte, 300)
	for i := range big {
		big[i] = byte(i)
	}
	got, err = Frames(synthRef(t, m, encChunk(7, big)), 0, 0)
	if err != nil {
		t.Fatalf("Frames big: %v", err)
	}
	if len(got.Frames) != 1 || !got.Truncated {
		t.Fatalf("big = %d frames truncated=%v, want 1 / true", len(got.Frames), got.Truncated)
	}
	f := got.Frames[0]
	if f.NBytes != 300 || len(f.Bytes) != 256 {
		t.Errorf("big frame n=%d preview=%d, want 300 / 256", f.NBytes, len(f.Bytes))
	}
}

func TestFramesModeAndBaudGuards(t *testing.T) {
	if _, err := Frames(synthRef(t, Meta{Mode: "edge", Baud: 9600}, nil), 0, 0); err == nil ||
		!strings.Contains(err.Error(), "LISTEN") {
		t.Errorf("edge mode err = %v, want the LISTEN-captures coach", err)
	}
	if _, err := Frames(synthRef(t, Meta{Mode: "garbage", Baud: 9600}, nil), 0, 0); err == nil || strings.Contains(err.Error(), "not landed") {
		t.Errorf("garbage mode err = %v, want a plain unknown-mode error", err)
	}
	if _, err := Frames(synthRef(t, Meta{Baud: 0}, encChunk(1, []byte("x"))), 0, 0); err == nil || !strings.Contains(err.Error(), "baud") {
		t.Errorf("baudless meta err = %v, want a baud-missing error", err)
	}
}

// The periodic golden: P (RS)×3 T — the longest repeating unit is the
// two-frame RS pair, three consecutive occurrences, period = one unit's
// span (t[frame 5] − t[frame 1]) / 2 = 2083320. The preamble and the tail
// frame are the 8 tail bytes.
func TestRepeatGoldenPeriodic(t *testing.T) {
	got, err := Repeat(fixtureRef(t, "periodic"))
	if err != nil {
		t.Fatalf("Repeat: %v", err)
	}
	if got.Repeats != 3 || got.TailBytes != 8 {
		t.Fatalf("repeat = %+v, want repeats 3 tail 8", got)
	}
	if got.PeriodNS == nil || *got.PeriodNS != 2_083_320 {
		t.Fatalf("period = %v, want 2083320", got.PeriodNS)
	}
}

func TestRepeatHonesty(t *testing.T) {
	for name, tc := range map[string]struct {
		frames   [][]byte // each entry: one chunk = one frame
		wantTail int64
	}{
		"empty capture": {nil, 0},
		"single frame":  {[][]byte{{0xAA, 0x00}}, 2},
		"two distinct":  {[][]byte{{0xAA, 0x00}, {0xBB}}, 3},
	} {
		var stream []byte
		for i, d := range tc.frames {
			stream = append(stream, encChunk(uint64(i)*1_000_000, d)...)
		}
		got, err := Repeat(synthRef(t, Meta{Baud: 115200, IdleSyms: 4}, stream))
		if err != nil {
			t.Fatalf("%s: Repeat: %v", name, err)
		}
		// <2 frames, or nothing repeating: repeats=0, no period, and the
		// whole payload is tail — as-is, never guessed.
		if got.Repeats != 0 || got.PeriodNS != nil || got.TailBytes != tc.wantTail {
			t.Errorf("%s: repeat = %+v, want repeats 0 / nil period / tail %d", name, got, tc.wantTail)
		}
	}
}

// Maximal-unit-first pin: five identical frames report the LONGEST unit —
// the two-frame AA block twice (repeats=2, one unit's span) — not the
// single frame five times. The frozen rule is 最長重複單元.
func TestRepeatLongestUnitWins(t *testing.T) {
	var stream []byte
	for i := 0; i < 5; i++ {
		stream = append(stream, encChunk(uint64(i)*1_000_000, []byte{0xA5})...)
	}
	got, err := Repeat(synthRef(t, Meta{Baud: 115200, IdleSyms: 4}, stream))
	if err != nil {
		t.Fatalf("Repeat: %v", err)
	}
	if got.Repeats != 2 || got.PeriodNS == nil || *got.PeriodNS != 2_000_000 || got.TailBytes != 1 {
		t.Fatalf("repeat = %+v, want repeats=2 period=2000000 tail=1", got)
	}
}

// The scan window is a frozen 8192 frames; beyond it the capture reads as
// tail. 9000 identical frames: within the window the longest unit is the
// half-window block (×2); the 808 frames past the window are tail bytes.
func TestRepeatScanWindow(t *testing.T) {
	var stream []byte
	for i := 0; i < 9000; i++ {
		stream = append(stream, encChunk(uint64(i)*1_000_000, []byte{0xAB, 0xCD})...)
	}
	got, err := Repeat(synthRef(t, Meta{Baud: 115200, IdleSyms: 4}, stream))
	if err != nil {
		t.Fatalf("Repeat: %v", err)
	}
	if got.Repeats != 2 || got.PeriodNS == nil || *got.PeriodNS != 4_096_000_000 {
		t.Fatalf("repeat = %+v, want repeats=2 period=4096000000", got)
	}
	if got.TailBytes != (9000-8192)*2 {
		t.Fatalf("tail_bytes = %d, want %d (the frames past the scan window)", got.TailBytes, (9000-8192)*2)
	}
}

func hexOf(b []byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, v := range b {
		out = append(out, hex[v>>4], hex[v&0xf])
	}
	return string(out)
}
