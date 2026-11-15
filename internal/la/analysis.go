package la

import "fmt"

// The L1 analysis bounds (constraint 10: every reply is bounded — a cut
// is flagged truncated, never silent, never unbounded).
const (
	// framesMax caps the frame list a reply carries (a full modbus
	// session slice; the count is derivable, the preview is not).
	framesMax = 256
	// frameBytesMax caps one frame's payload preview — modbus RTU's own
	// 256-byte frame ceiling; n_bytes rides beside it with the full
	// count.
	frameBytesMax = 256
	// repeatScanMax caps the periodicity scan window (frames). A longer
	// capture's frames beyond the window count as tail bytes — the bound
	// is on the scan's work, stated here, not hidden.
	repeatScanMax = 8192
)

// idleSymsDefault mirrors the fw's arm default (XS_LA_IDLE_SYMS_DEF):
// the threshold a meta without idle_syms (omitempty zero) armed under.
const idleSymsDefault = 4

// Frame is one idle-delimited frame of a listen capture: the timestamp
// of its first chunk, the estimated silence that preceded it, and the
// bounded payload view (full count in NBytes, at most frameBytesMax
// bytes in Bytes).
type Frame struct {
	TNS         uint64 `json:"t_ns"`
	GapBeforeNS uint64 `json:"gap_before_ns"` // first frame: 0 (nothing precedes it)
	NBytes      int    `json:"n_bytes"`
	Bytes       []byte `json:"bytes"`
}

// FramesResult is the bounded framing reply.
type FramesResult struct {
	Frames    []Frame `json:"frames"`
	Baud      int     `json:"baud"`                // the baud the timing derived from
	IdleNS    uint64  `json:"idle_ns"`             // the threshold actually applied
	Truncated bool    `json:"truncated,omitempty"` // the list or a preview was cut
}

// Frames segments a listen capture into idle-delimited frames — the L1
// framing primitive.
//
// ch selects the capture channel: listen captures are single-channel
// (the armed pin), so ch must be 0 (omitted) or the meta's pin — any
// other value is a refusal. The T10/T12 multi-channel seats reuse this
// signature with a real selector.
//
// idleNS overrides the idle threshold; 0 derives it from the capture's
// own hardware facts: idle_syms symbols (fw default 4) of silence at the
// capture's baud. A chunk whose estimated leading silence reaches the
// threshold opens a new frame (equality splits — the fw closes its own
// chunks at exactly idle_syms of silence); shorter gaps merge, the fw's
// continuation records like any other chunk.
//
// The reply is bounded: at most framesMax frames, each payload previewed
// at frameBytesMax; truncated flags either cut.
func Frames(ref CaptureRef, ch int, idleNS uint64) (FramesResult, error) {
	if err := guardListen(ref, ch); err != nil {
		return FramesResult{}, err
	}
	if idleNS == 0 {
		idleNS = defaultIdleNS(ref.Meta, ref.Meta.Baud)
	}
	chunks, err := ReadChunks(ref.DataPath)
	if err != nil {
		return FramesResult{}, err
	}
	res := FramesResult{Baud: ref.Meta.Baud, IdleNS: idleNS}
	for _, s := range segment(chunks, ref.Meta.Baud, idleNS) {
		if len(res.Frames) == framesMax {
			res.Truncated = true
			break
		}
		b, cut := preview(s.data)
		res.Truncated = res.Truncated || cut
		res.Frames = append(res.Frames, Frame{TNS: s.tNS, GapBeforeNS: s.gapNS, NBytes: len(s.data), Bytes: b})
	}
	return res, nil
}

// RepeatResult is the bounded periodicity account: counts and one
// duration — bounded by construction (the frozen reply shape).
type RepeatResult struct {
	PeriodNS  *uint64 `json:"period_ns,omitempty"` // mean span of one unit; nil with repeats=0
	Repeats   int     `json:"repeats"`             // consecutive occurrences of the longest unit (0 = none)
	TailBytes int64   `json:"tail_bytes"`          // payload bytes outside the repeating run (preamble + tail)
}

// Repeat measures a listen capture's frame-level periodicity: the
// LONGEST frame-sequence unit (fingerprinted by exact bytes) that
// repeats consecutively at least twice, its ns period (the mean span
// over the run), and the payload bytes outside the repeating run. Under
// two frames, or with nothing repeating, the answer is {repeats:0,
// tail_bytes:all} — as-is, never guessed.
//
// Frames segment with the capture's own default threshold (idle_syms ×
// symbol time); the scan window is the first repeatScanMax frames.
func Repeat(ref CaptureRef) (RepeatResult, error) {
	if err := guardListen(ref, 0); err != nil {
		return RepeatResult{}, err
	}
	chunks, err := ReadChunks(ref.DataPath)
	if err != nil {
		return RepeatResult{}, err
	}
	segs := segment(chunks, ref.Meta.Baud, defaultIdleNS(ref.Meta, ref.Meta.Baud))
	var total int64
	for i := range segs {
		total += int64(len(segs[i].data))
	}
	res := RepeatResult{TailBytes: total}

	m := len(segs)
	if m < 2 {
		return res, nil
	}
	if m > repeatScanMax {
		m = repeatScanMax
	}
	// Frame fingerprints: exact byte identities interned to integers —
	// every unit comparison below is integer equality, no hash guessing.
	id := make([]int, m)
	ids := make(map[string]int, m)
	for i := 0; i < m; i++ {
		k := string(segs[i].data)
		if v, ok := ids[k]; ok {
			id[i] = v
		} else {
			ids[k] = i
			id[i] = i
		}
	}

	// The longest-unit scan. For a candidate period L, match[i] says
	// "frame i equals frame i+L"; a maximal run of matches [a,b) carries
	// k = runLen/L + 1 consecutive occurrences of the unit starting at
	// a (the chain id[a+j] == … == id[a+kL+j] needs exactly kL matches).
	// The largest L with a qualifying run settles the unit; at that L
	// the longest run (then the earliest start) wins. O(m²/2) integer
	// compares at the 8192 window.
	bestL, bestK, bestS := 0, 0, 0
	for L := m / 2; L >= 1; L-- {
		runStart, runLen := -1, 0
		flush := func() {
			if runStart >= 0 && runLen >= L && runLen/L+1 > bestK {
				bestL, bestK, bestS = L, runLen/L+1, runStart
			}
		}
		for i := 0; i+L < m; i++ {
			if id[i] == id[i+L] {
				if runStart < 0 {
					runStart = i
				}
				runLen++
			} else {
				flush()
				runStart, runLen = -1, 0
			}
		}
		flush()
		if bestL != 0 {
			break // the largest period with a tandem wins — longest unit
		}
	}
	if bestL == 0 {
		return res, nil // nothing repeats consecutively — the honest zero
	}

	k, s, L := bestK, bestS, bestL
	// The period rides the run's span end-minus-start. Timestamps are
	// nominally monotonic (one writer), but the fw's start_ns is its
	// READ of a chunk's first batch — jitter can hand the walker a
	// non-monotonic pair, and a u64 underflow would wrap to ~1.8e19.
	// Guard: end < start means the span is not honestly measurable —
	// decline the period (the honest zero), keep the counts.
	if end := segs[s+(k-1)*L].tNS; end >= segs[s].tNS {
		period := (end - segs[s].tNS) / uint64(k-1)
		res.PeriodNS = &period
		res.Repeats = k
		var run int64
		for i := s; i < s+k*L; i++ {
			run += int64(len(segs[i].data))
		}
		res.TailBytes = total - run
	}
	return res, nil
}

// ---- the shared segmentation model ----

// segFrame is one idle-delimited frame's full account (unbounded
// internals — the bounds apply at reply build).
type segFrame struct {
	tNS   uint64
	gapNS uint64
	data  []byte
}

// symNS answers the on-wire nanoseconds of one symbol (UART character)
// at baud: the 10-bit char (start + 8 data + stop) of the listen
// engine's fixed 8N1 seat. A 7E1/7O1 line carries the same 10-bit char
// (start + 7 data + parity + stop), so a parity claim never shifts
// timing. Integer ns arithmetic, floored — the rounding model: the
// true symbol time is 10e9/baud ns; the floor drops the fractional
// part (error < 1ns per symbol, so a 4-symbol idle threshold is off by
// < 4ns — far under the fw's own BYTE-time granularity, never a
// boundary the segmentation could ride). 1200..5M all divide 1e9 within
// these bounds; a baud that did not would still floor deterministically.
func symNS(baud int) uint64 { return 10_000_000_000 / uint64(baud) }

// defaultIdleNS derives the idle threshold from the capture's own
// hardware facts: idle_syms symbols of silence at baud.
func defaultIdleNS(m Meta, baud int) uint64 {
	syms := m.IdleSyms
	if syms <= 0 {
		syms = idleSymsDefault
	}
	return uint64(syms) * symNS(baud)
}

// segment walks the chunk stream into idle-delimited frames. Chunks ride
// uniformly (a fw continuation record is just another chunk — the walker
// already flattened them; the analysis is structurally immune to the
// u16-ceiling continuation shape). A frame boundary opens where the gap
// before a chunk reaches the threshold; gap semantics take the chunk
// boundary as the finest grain — bytes inside a chunk are back-to-back
// by construction and are never split.
//
// The gap is chunk-start to chunk-start minus the previous payload's
// on-wire duration (n × symbol time at the segmentation baud). start_ns
// stamps the fw task's READ of a chunk's first batch (L0 semantics,
// delivery latency included), so the estimate can read negative on
// jitter — back-to-back — and clamps to zero.
func segment(chunks []Chunk, baud int, idleNS uint64) []segFrame {
	sym := symNS(baud)
	var out []segFrame
	for i, c := range chunks {
		gap := uint64(0)
		if i > 0 {
			prev := chunks[i-1]
			end := prev.StartNS + uint64(len(prev.Data))*sym
			if c.StartNS > end {
				gap = c.StartNS - end
			}
		}
		if i == 0 || gap >= idleNS {
			out = append(out, segFrame{tNS: c.StartNS, gapNS: gap, data: append([]byte(nil), c.Data...)})
		} else {
			last := &out[len(out)-1]
			last.data = append(last.data, c.Data...)
		}
	}
	return out
}

// preview answers the bounded payload view: the whole data when it fits
// frameBytesMax, else the first frameBytesMax bytes with cut=true.
func preview(data []byte) (b []byte, cut bool) {
	if len(data) > frameBytesMax {
		return data[:frameBytesMax], true
	}
	return data, false
}

// guardListen refuses the seats this wave's analysis cannot read: the
// capture must be a listen capture with a usable baud, and a channel
// selector must name the one channel a listen capture has (the armed
// pin).
func guardListen(ref CaptureRef, ch int) error {
	switch ref.Meta.Mode {
	case "listen":
	case "edge", "samples":
		return fmt.Errorf("la: capture %s is mode %q — the framing seats read LISTEN captures (the edge seats are la_edges/la_runs/…; samples decode with la_decode_bus)", ref.ID, ref.Meta.Mode)
	default:
		return fmt.Errorf("la: capture %s has unknown mode %q", ref.ID, ref.Meta.Mode)
	}
	if ref.Meta.Baud <= 0 {
		return fmt.Errorf("la: capture %s lacks baud — timing cannot derive (re-pull the object)", ref.ID)
	}
	if ch != 0 && ch != ref.Meta.Pin {
		return fmt.Errorf("la: capture %s is single-channel (pin %d) — channel %d does not exist", ref.ID, ref.Meta.Pin, ch)
	}
	return nil
}
