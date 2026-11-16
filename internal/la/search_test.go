package la

// search_test.go — la_search (LA 增強波 A): the three kinds over the
// committed fixtures (modbus listen bytes, edgenrz/edgepwm levels,
// sampwm samples) — goldens hand-derived from the frozen streams, the
// negative coach, the window, and the bound.

import (
	"strings"
	"testing"
)

func TestSearchBytesGoldenModbus(t *testing.T) {
	ref := fixtureRef(t, "modbus")
	got, err := Search(ref, SearchParams{Kind: "bytes", Pattern: "0103"})
	if err != nil {
		t.Fatalf("Search bytes: %v", err)
	}
	if got.Kind != "bytes" || got.Pattern != "0103" || got.NHits != 2 {
		t.Fatalf("echo = %+v, want 2 hits of 0103", got)
	}
	if len(got.Hits) != 2 {
		t.Fatalf("hits = %d", len(got.Hits))
	}
	h0, h1 := got.Hits[0], got.Hits[1]
	if h0.ByteOff != 0 || h0.ChunkI != 0 || h0.TNS != 1_000_000_000 {
		t.Errorf("hit 0 = %+v, want {off 0, chunk 0, t 1e9}", h0)
	}
	if h1.ByteOff != 10 || h1.ChunkI != 2 || h1.TNS != 1_014_583_324 {
		t.Errorf("hit 1 = %+v, want {off 10, chunk 2, t 1.014583324e9}", h1)
	}
	// context = ±8 bytes hex around the match; the match itself rides inside
	if !strings.Contains(h0.Context, "0103000000") {
		t.Errorf("hit 0 context = %q, want the match bytes inside", h0.Context)
	}

	// case-insensitive hex + whole-frame needle
	got, err = Search(ref, SearchParams{Kind: "bytes", Pattern: "DEADBEEF"})
	if err != nil || got.NHits != 1 || got.Hits[0].ByteOff != 17 || got.Hits[0].ChunkI != 3 ||
		got.Hits[0].TNS != 1_030_208_314 {
		t.Fatalf("DEADBEEF = %+v err %v, want one hit {off 17, chunk 3}", got, err)
	}

	// non-overlapping: "aa" in …c40c55aa — one hit inside frame 0
	got, err = Search(ref, SearchParams{Kind: "bytes", Pattern: "aa"})
	if err != nil || got.NHits != 1 || got.Hits[0].ByteOff != 9 {
		t.Fatalf("aa = %+v err %v, want the single trailing-55aa hit", got, err)
	}
}

func TestSearchBytesWindowAndBound(t *testing.T) {
	ref := fixtureRef(t, "modbus")
	// window [1.01e9, ∞): only the 0103 at t≈1.0146e9 qualifies
	got, err := Search(ref, SearchParams{Kind: "bytes", Pattern: "0103", FromNS: 1_010_000_000})
	if err != nil || got.NHits != 1 || got.Hits[0].ByteOff != 10 {
		t.Fatalf("windowed = %+v err %v, want the frame-1 hit only", got, err)
	}
	// the row bound: max 1 on a 2-hit pattern → truncated with n_hits 2
	got, err = Search(ref, SearchParams{Kind: "bytes", Pattern: "0103", Max: 1})
	if err != nil || got.NHits != 2 || len(got.Hits) != 1 || !got.Truncated {
		t.Fatalf("max=1 → n=%d rows=%d trunc=%v, want 2/1/true", got.NHits, len(got.Hits), got.Truncated)
	}
}

func TestSearchBytesNegatives(t *testing.T) {
	mod := fixtureRef(t, "modbus")
	cases := []struct {
		name string
		ref  CaptureRef
		p    SearchParams
		want string
	}{
		{"empty pattern", mod, SearchParams{Kind: "bytes", Pattern: ""}, "empty"},
		{"odd hex", mod, SearchParams{Kind: "bytes", Pattern: "abc"}, "hex byte sequence"},
		{"non-hex", mod, SearchParams{Kind: "bytes", Pattern: "zz"}, "hex byte sequence"},
		{"kind domain", mod, SearchParams{Kind: "nibble", Pattern: "01"}, `want "bytes"`},
		{"kind missing", mod, SearchParams{Pattern: "01"}, "kind is required"},
		{"bytes on edge", fixtureRef(t, "edgepwm"), SearchParams{Kind: "bytes", Pattern: "01"}, "LISTEN byte stream"},
		{"bytes on samples", fixtureRef(t, "sampwm"), SearchParams{Kind: "bytes", Pattern: "01"}, "LISTEN byte stream"},
	}
	for _, tc := range cases {
		_, err := Search(tc.ref, tc.p)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tc.name, err, tc.want)
		}
	}
}

func TestSearchBitsExpandedGolden(t *testing.T) {
	// edgenrz: the unit votes 100µs; the expansion of runs past run 0
	// is "0110010" + "10110010"×3 + "1111". The first "011" sits at
	// bit 0, owned by run 1 (the first L100).
	ref := fixtureRef(t, "edgenrz")
	got, err := Search(ref, SearchParams{Kind: "bits", Pattern: "011"})
	if err != nil {
		t.Fatalf("Search bits: %v", err)
	}
	h := got.Hits[0]
	if h.BitOff != 0 || h.RunI != 1 || h.TNS != 6_000_100_000 || h.Level != 0 || h.DurNS != 100_000 {
		t.Fatalf("first hit = %+v, want {bit 0, run 1, t 6.0001e9, 0, 100000}", h)
	}
	// the naive reference recount (independent of the seat's walker)
	exp := "0110010" + strings.Repeat("10110010", 3) + "1111"
	want := 0
	for i := 0; i+len("011") <= len(exp); {
		if exp[i:i+3] == "011" {
			want++
			i += 3
		} else {
			i++
		}
	}
	if got.NHits != want {
		t.Fatalf("n_hits = %d, want %d over expansion %q", got.NHits, want, exp)
	}

	// edgepwm: the lows outnumber 20:19 → the unit votes the 750µs
	// LOW; each period expands to one '0' (H250 rounds to zero units)
	// — "0" lands 20 times, one per period
	pwm := fixtureRef(t, "edgepwm")
	got, err = Search(pwm, SearchParams{Kind: "bits", Pattern: "0"})
	if err != nil || got.NHits != 20 || len(got.Hits) != 20 || got.Truncated {
		t.Fatalf("pwm 0 = n %d rows %d trunc %v err %v, want 20/20/false", got.NHits, len(got.Hits), got.Truncated, err)
	}
	if got.Hits[0].RunI != 1 || got.Hits[0].TNS != 4_000_250_000 || got.Hits[0].BitOff != 0 || got.Hits[0].DurNS != 750_000 {
		t.Fatalf("hit 0 = %+v, want {run 1, t 4.00025e9, bit 0, 750000}", got.Hits[0])
	}
	if got.Hits[3].RunI != 7 || got.Hits[3].TNS != 4_003_250_000 || got.Hits[3].BitOff != 3 {
		t.Fatalf("hit 3 = %+v, want {run 7, t 4.00325e9, bit 3}", got.Hits[3])
	}

	// sampwm ch0: expansion "1010…1" (15 bits), "10" lands 7 times
	sam := fixtureRef(t, "sampwm")
	got, err = Search(sam, SearchParams{Kind: "bits", Pattern: "10", Ch: 0})
	if err != nil || got.NHits != 7 || got.Hits[0].TNS != 7_000_005_000 {
		t.Fatalf("sampwm 10 = %+v err %v, want 7 hits, first at 7.000005e9", got, err)
	}
	// a pattern a literal run-level match could never fire (the "11")
	// fires here — the expansion is the semantics
	got, err = Search(sam, SearchParams{Kind: "bits", Pattern: "1"})
	if err != nil || got.NHits != 8 {
		t.Fatalf("sampwm 1 = n %d err %v, want 8 high units", got.NHits, err)
	}
}

func TestSearchBitsNegativesAndBound(t *testing.T) {
	ref := fixtureRef(t, "edgenrz")
	for _, tc := range []struct {
		name string
		p    SearchParams
		want string
	}{
		{"empty", SearchParams{Kind: "bits", Pattern: ""}, "empty"},
		{"alphabet", SearchParams{Kind: "bits", Pattern: "10a"}, "only 0 and 1"},
	} {
		if _, err := Search(ref, tc.p); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
	// listen refuses the level kinds with the domain coach
	mod := fixtureRef(t, "modbus")
	if _, err := Search(mod, SearchParams{Kind: "bits", Pattern: "10"}); err == nil || !strings.Contains(err.Error(), "LEVEL") {
		t.Errorf("bits on listen: err = %v, want the LEVEL coach", err)
	}
	// ch domain
	if _, err := Search(fixtureRef(t, "sampwm"), SearchParams{Kind: "bits", Pattern: "10", Ch: 9}); err == nil || !strings.Contains(err.Error(), "0..7") {
		t.Errorf("ch 9 on samples: err = %v, want the 0..7 coach", err)
	}
	// the bound: max 4 over 20 pwm hits
	got, err := Search(fixtureRef(t, "edgepwm"), SearchParams{Kind: "bits", Pattern: "0", Max: 4})
	if err != nil || got.NHits != 20 || len(got.Hits) != 4 || !got.Truncated {
		t.Fatalf("max=4 → n %d rows %d trunc %v", got.NHits, len(got.Hits), got.Truncated)
	}
}

func TestSearchTimingGolden(t *testing.T) {
	ref := fixtureRef(t, "edgenrz")
	minNS, maxNS := uint64(200_000), uint64(250_000)
	got, err := Search(ref, SearchParams{Kind: "timing", Timing: SearchTiming{MinNS: &minNS, MaxNS: &maxNS}})
	if err != nil {
		t.Fatalf("Search timing: %v", err)
	}
	// the four H200 and four L200 runs sit inside [200µs, 250µs); the
	// H310 tail is out at max; run 0 never qualifies
	if got.NHits != 8 {
		t.Fatalf("n_hits = %d, want 8 (4× H200 + 4× L200)", got.NHits)
	}
	h := got.Hits[0]
	if h.RunI != 2 || h.TNS != 6_000_200_000 || h.Level != 1 || h.DurNS != 200_000 {
		t.Fatalf("hit 0 = %+v, want {run 2, t 6.0002e9, 1, 200000}", h)
	}
	if last := got.Hits[7]; last.RunI != 21 || last.DurNS != 200_000 || last.Level != 0 {
		t.Fatalf("hit 7 = %+v, want the last L200 run 21", last)
	}

	// level filter: only the lows
	got, err = Search(ref, SearchParams{Kind: "timing", Timing: SearchTiming{MinNS: &minNS, Level: intPtr(0)}})
	if err != nil || got.NHits != 4 {
		t.Fatalf("level-0 timing = n %d err %v, want 4 L200s", got.NHits, err)
	}
	// samples: sampwm ch0 every run is 5000ns — min 5000 keeps all 15
	sam := fixtureRef(t, "sampwm")
	five := uint64(5_000)
	got, err = Search(sam, SearchParams{Kind: "timing", Ch: 0, Timing: SearchTiming{MinNS: &five}})
	if err != nil || got.NHits != 15 {
		t.Fatalf("sampwm timing = n %d err %v, want 15", got.NHits, err)
	}
}

func TestSearchTimingNegatives(t *testing.T) {
	ref := fixtureRef(t, "edgenrz")
	if _, err := Search(ref, SearchParams{Kind: "timing"}); err == nil || !strings.Contains(err.Error(), "predicate") {
		t.Errorf("bare timing: err = %v, want the predicate coach", err)
	}
	lv := 2
	if _, err := Search(ref, SearchParams{Kind: "timing", Timing: SearchTiming{Level: &lv}}); err == nil || !strings.Contains(err.Error(), "0|1") {
		t.Errorf("level 2: err = %v, want the 0|1 coach", err)
	}
}

// intPtr answers a fresh pointer (the test-table helper).
func intPtr(v int) *int { return &v }
