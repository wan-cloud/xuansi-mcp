package la

// search.go — la_search (LA 增強波 A): the pattern-search seat over a
// landed capture, three kinds. bytes: a hex byte sequence scanned
// through a LISTEN capture's chunk payload (the protocol byte stream).
// bits: a 0/1 string matched against a LEVEL capture's per-channel run
// sequence (each run one bit, regardless of width — the run expansion).
// timing: a predicate {min_ns?, max_ns?, level?} over the channel's
// run widths. All bounded (default 256 hits, hard cap 4096, the cut
// flagged), all deterministic — no fuzzy matching, no guesses.

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// The search bounds (rowsDefault/rowsMax precedent): the bits
// pattern's own length, and the hit-context preview windows.
const (
	searchPatMax  = 4096 // the bits pattern's own length bound
	searchCtxBits = 16   // the bits hit's ± context window (runs)
	searchCtxByte = 8    // the bytes hit's ± context window (bytes)
)

// SearchTiming is the timing kind's predicate: a run qualifies iff
// min_ns ≤ dur (when given), dur < max_ns (when given — half-open, the
// window discipline), and level == the claim (when given).
type SearchTiming struct {
	MinNS *uint64
	MaxNS *uint64
	Level *int
}

// SearchParams is the search seat's parameter set. Kind is required;
// Pattern rides bytes/bits; Timing rides the timing kind; Ch selects
// the channel (level kinds; omitted = 0, echoed); From/ToNS bound by
// absolute t_ns ([from, to), 0 = open); Max bounds the rows.
type SearchParams struct {
	Kind    string
	Pattern string
	Timing  SearchTiming
	Ch      int
	FromNS  uint64
	ToNS    uint64
	Max     int
}

// SearchHit is one match. The coordinate depends on the kind: bytes
// carries ByteOff (the whole-stream byte index) + ChunkI + the
// estimated TNS (the chunk's start_ns + the in-chunk byte offset ×
// the symbol time — chunks ride uniformly, the segmentation's own
// discipline); bits carries RunI (the mask-run index whose expansion
// holds the match's first bit — la_runs' coordinate) + BitOff (the
// expanded stream's index) + TNS; timing carries RunI + TNS + Level
// + DurNS. The context preview rides bytes (±8 bytes hex) and bits
// (±16 expanded bits).
type SearchHit struct {
	ByteOff int64  `json:"byte_off,omitempty"`
	ChunkI  int    `json:"chunk_i,omitempty"`
	RunI    int    `json:"run_i,omitempty"`
	BitOff  int    `json:"bit_off,omitempty"`
	TNS     uint64 `json:"t_ns"`
	Level   int    `json:"level,omitempty"`
	DurNS   uint64 `json:"dur_ns,omitempty"`
	Context string `json:"context,omitempty"`
}

// SearchResult is the bounded reply. NHits is the count of QUALIFYING
// matches (the population the rows are the bounded head of — the
// window applied); Truncated flags the cut.
type SearchResult struct {
	Kind      string      `json:"kind"`
	Pattern   string      `json:"pattern,omitempty"`
	Ch        int         `json:"ch,omitempty"`
	Hits      []SearchHit `json:"hits"`
	NHits     int         `json:"n_hits"`
	FromNS    uint64      `json:"from_ns,omitempty"`
	ToNS      uint64      `json:"to_ns,omitempty"`
	Truncated bool        `json:"truncated,omitempty"`
}

// Search scans one capture for the pattern. Non-overlapping successive
// matches (after a hit the scan resumes past it) for bytes/bits —
// stated, deterministic; timing is a filter and counts every
// qualifying run.
func Search(ref CaptureRef, p SearchParams) (SearchResult, error) {
	switch p.Kind {
	case "bytes":
		return searchBytes(ref, p)
	case "bits":
		return searchBits(ref, p)
	case "timing":
		return searchTiming(ref, p)
	case "":
		return SearchResult{}, fmt.Errorf(`la: search kind is required — "bytes" (a hex byte sequence over a listen capture's payload), "bits" (a 0/1 string over a level capture's run sequence), or "timing" (a {min_ns, max_ns, level} predicate over run widths)`)
	default:
		return SearchResult{}, fmt.Errorf(`la: search kind %q — want "bytes", "bits", or "timing"`, p.Kind)
	}
}

// searchBytes: the listen payload scan. The concatenated chunk data is
// the haystack; each hit carries the stream-wide byte offset, the
// owning chunk, and the estimated absolute time (start_ns + in-chunk
// offset × symbol time).
func searchBytes(ref CaptureRef, p SearchParams) (SearchResult, error) {
	switch ref.Meta.Mode {
	case "listen":
	case "edge", "samples":
		return SearchResult{}, fmt.Errorf("la: capture %s is mode %q — the bytes kind rides the LISTEN byte stream (the level captures carry no byte payload; their bits kind searches the run sequence)", ref.ID, ref.Meta.Mode)
	default:
		return SearchResult{}, fmt.Errorf("la: capture %s has unknown mode %q", ref.ID, ref.Meta.Mode)
	}
	if ref.Meta.Baud <= 0 {
		return SearchResult{}, fmt.Errorf("la: capture %s lacks baud — timing cannot derive (re-pull the object)", ref.ID)
	}
	pat := strings.TrimSpace(p.Pattern)
	if pat == "" {
		return SearchResult{}, fmt.Errorf(`la: search pattern is empty — bytes wants a hex byte sequence (e.g. "0103a0" for 01 03 A0)`)
	}
	needle, derr := hex.DecodeString(pat)
	if derr != nil || len(pat)%2 != 0 {
		return SearchResult{}, fmt.Errorf(`la: search pattern %q is not a hex byte sequence — want pairs of hex digits (e.g. "0103a0"; an odd length or a non-hex digit refuses)`, p.Pattern)
	}
	chunks, err := ReadChunks(ref.DataPath)
	if err != nil {
		return SearchResult{}, err
	}
	res := SearchResult{Kind: "bytes", Pattern: strings.ToLower(pat), FromNS: p.FromNS, ToNS: p.ToNS}
	sym := symNS(ref.Meta.Baud)

	// the haystack with per-byte provenance (chunk index, in-chunk offset)
	type prov struct{ chunk, inChunk int }
	var hay []byte
	var provs []prov
	for ci, c := range chunks {
		for bi := range c.Data {
			hay = append(hay, c.Data[bi])
			provs = append(provs, prov{ci, bi})
		}
	}
	capRows := clampRows(p.Max)
	for off := 0; off+len(needle) <= len(hay); {
		if !bytesEqualAt(hay, needle, off) {
			off++
			continue
		}
		tNS := chunks[provs[off].chunk].StartNS + uint64(provs[off].inChunk)*sym
		if inWindow(tNS, p.FromNS, p.ToNS) {
			res.NHits++
			if len(res.Hits) < capRows {
				lo := off - searchCtxByte
				if lo < 0 {
					lo = 0
				}
				hi := off + len(needle) + searchCtxByte
				if hi > len(hay) {
					hi = len(hay)
				}
				res.Hits = append(res.Hits, SearchHit{
					ByteOff: int64(off), ChunkI: provs[off].chunk, TNS: tNS,
					Context: hex.EncodeToString(hay[lo:hi]),
				})
			}
		}
		off += len(needle) // non-overlapping: resume past the hit
	}
	res.Truncated = res.NHits > len(res.Hits)
	return res, nil
}

// searchBits: the expanded-run match. Each of the channel's runs
// (past run 0, the arm artifact) expands to round(width / modal_unit)
// copies of its level — the same majority-vote unit la_bits derives
// its grid from — and the 0/1 pattern matches THAT bit string (a
// literal run-level match could never fire a "11": consecutive runs
// always alternate). The hit cites the run whose expansion holds the
// match's first bit (sub-run bit positions are not in the data) plus
// the expanded bit offset.
func searchBits(ref CaptureRef, p SearchParams) (SearchResult, error) {
	runs, t0, err := levelRunsChecked(ref, p.Ch)
	if err != nil {
		return SearchResult{}, err
	}
	pat := p.Pattern
	if pat == "" {
		return SearchResult{}, fmt.Errorf(`la: search pattern is empty — bits wants a 0/1 string (e.g. "10110" = high, low, high, high, low)`)
	}
	if len(pat) > searchPatMax {
		return SearchResult{}, fmt.Errorf("la: search pattern is %d bits — the bound is %d", len(pat), searchPatMax)
	}
	for _, c := range pat {
		if c != '0' && c != '1' {
			return SearchResult{}, fmt.Errorf("la: search pattern %q — bits wants only 0 and 1", p.Pattern)
		}
	}
	cr := channelRuns(runs, t0, p.Ch)
	if len(cr) < 2 {
		return SearchResult{}, fmt.Errorf("la: capture %s ch %d carries no signal runs — nothing to expand", ref.ID, p.Ch)
	}
	widths := make([]uint64, len(cr)-1)
	for i := 1; i < len(cr); i++ {
		widths[i-1] = cr[i].DurNS
	}
	unit, ok := modalWidth(widths)
	if !ok || unit == 0 {
		return SearchResult{}, fmt.Errorf("la: capture %s ch %d has no run widths to vote a bit unit from — the bits kind cannot expand", ref.ID, p.Ch)
	}

	// the expansion: bit j remembers the channel run that emitted it
	var bits []byte
	var owner []int
	for i := 1; i < len(cr); i++ {
		k := int((cr[i].DurNS + unit/2) / unit)
		for x := 0; x < k; x++ {
			bits = append(bits, byte('0'+cr[i].Level))
			owner = append(owner, i)
		}
	}

	res := SearchResult{Kind: "bits", Pattern: pat, Ch: p.Ch, FromNS: p.FromNS, ToNS: p.ToNS}
	capRows := clampRows(p.Max)
	for j := 0; j+len(pat) <= len(bits); {
		if !bytesEqualAt(bits, []byte(pat), j) {
			j++
			continue
		}
		ri := owner[j]
		if inWindow(cr[ri].TNS, p.FromNS, p.ToNS) {
			res.NHits++
			if len(res.Hits) < capRows {
				lo := j - searchCtxBits
				if lo < 0 {
					lo = 0
				}
				hi := j + len(pat) + searchCtxBits
				if hi > len(bits) {
					hi = len(bits)
				}
				res.Hits = append(res.Hits, SearchHit{
					RunI: cr[ri].RunI, BitOff: j, TNS: cr[ri].TNS,
					Level: cr[ri].Level, DurNS: cr[ri].DurNS,
					Context: string(bits[lo:hi]),
				})
			}
		}
		j += len(pat) // non-overlapping
	}
	res.Truncated = res.NHits > len(res.Hits)
	return res, nil
}

// searchTiming: the predicate filter over the channel's run widths.
// Run 0 (the arm artifact) is exempt — its width is not a signal fact
// (Diff's precedent, stated in the Desc).
func searchTiming(ref CaptureRef, p SearchParams) (SearchResult, error) {
	runs, t0, err := levelRunsChecked(ref, p.Ch)
	if err != nil {
		return SearchResult{}, err
	}
	if p.Timing.MinNS == nil && p.Timing.MaxNS == nil && p.Timing.Level == nil {
		return SearchResult{}, fmt.Errorf("la: search timing needs a predicate — at least one of min_ns, max_ns, level (a bare filter that qualifies nothing refuses; a plain run listing is la_runs' job)")
	}
	if p.Timing.Level != nil && *p.Timing.Level != 0 && *p.Timing.Level != 1 {
		return SearchResult{}, fmt.Errorf("la: search timing level %d — want 0|1", *p.Timing.Level)
	}
	res := SearchResult{Kind: "timing", Ch: p.Ch, FromNS: p.FromNS, ToNS: p.ToNS}
	cr := channelRuns(runs, t0, p.Ch)
	capRows := clampRows(p.Max)
	for i, r := range cr {
		if i == 0 {
			continue // run 0: the arm artifact
		}
		if p.Timing.MinNS != nil && r.DurNS < *p.Timing.MinNS {
			continue
		}
		if p.Timing.MaxNS != nil && r.DurNS >= *p.Timing.MaxNS {
			continue
		}
		if p.Timing.Level != nil && r.Level != *p.Timing.Level {
			continue
		}
		if !inWindow(r.TNS, p.FromNS, p.ToNS) {
			continue
		}
		res.NHits++
		if len(res.Hits) < capRows {
			res.Hits = append(res.Hits, SearchHit{
				RunI: r.RunI, TNS: r.TNS, Level: r.Level, DurNS: r.DurNS})
		}
	}
	res.Truncated = res.NHits > len(res.Hits)
	return res, nil
}

// levelRunsChecked is the level kinds' shared entry: the mode guard,
// the channel domain check, then the mask runs + t0.
func levelRunsChecked(ref CaptureRef, ch int) ([]Run, uint64, error) {
	if err := guardLevel(ref); err != nil {
		return nil, 0, err
	}
	if err := resolveCh(ref, ch); err != nil {
		return nil, 0, err
	}
	return levelRuns(ref)
}

// bytesEqualAt answers whether needle sits at hay[off].
func bytesEqualAt(hay, needle []byte, off int) bool {
	for k, b := range needle {
		if hay[off+k] != b {
			return false
		}
	}
	return true
}
