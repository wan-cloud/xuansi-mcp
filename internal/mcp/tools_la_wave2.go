package mcp

// tools_la_wave2.go — the LA second analysis batch's MCP seat
// (2026-11-17, LA 增強波 A): eight xuansi_la_* tools over the daemon's
// wave-2 crossings — the pattern search, the activity map, the
// universal bit decoder, the cross-channel accompaniment, the glitch
// account, the eye fold, the two-capture alignment, and the VCD
// export. All read the daemon-local capture-object store (no lease,
// no board contact — the board can be gone). The Descs are the
// product: each coaches the next step and states the honest bounds —
// pinned by tools_la_wave2_test.go's Desc audit.

import (
	"context"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// laSearchIn: kind picks the domain. bytes: a hex byte sequence over
// a LISTEN capture's payload. bits: a 0/1 string over a LEVEL
// capture's expanded run sequence. timing: a width predicate.
type laSearchIn struct {
	CaptureID string  `json:"capture_id" jsonschema:"the landed capture's id (bytes kind: a LISTEN capture; bits/timing: an EDGE or SAMPLES capture)"`
	Kind      string  `json:"kind" jsonschema:"\"bytes\" (hex byte sequence, e.g. \"0103a0\", scanned through a listen capture's chunk payload — hits carry byte_off/chunk_i and an estimated t_ns), \"bits\" (a 0/1 string, e.g. \"10110\", matched against a level capture's channel run sequence EXPANDED at the modal run unit — each run becomes round(width/unit) copies of its level; hits carry run_i + bit_off), or \"timing\" (a predicate over run widths: min_ns ≤ dur < max_ns, level 0|1 — the pulse-width filter)"`
	Pattern   string  `json:"pattern,omitempty" jsonschema:"the bytes/bits pattern (required for those kinds; whitespace-trimmed hex for bytes, 0/1 chars for bits; empty refuses)"`
	MinNS     *uint64 `json:"min_ns,omitempty" jsonschema:"timing only: the minimum run width (inclusive)"`
	MaxNS     *uint64 `json:"max_ns,omitempty" jsonschema:"timing only: the maximum run width (exclusive — half-open like the window semantics)"`
	Level     *int    `json:"level,omitempty" jsonschema:"timing only: 0|1 — keep only runs at this level"`
	Ch        int     `json:"ch,omitempty" jsonschema:"bits/timing only: the channel (edge: 0..len(pins)-1 = pins[i]; samples: 0..7 = D0..D7). Omit = 0; the reply echoes it so the read line is never a guess"`
	FromNS    uint64  `json:"from_ns,omitempty" jsonschema:"window start, absolute ns ([from, to), 0 = open)"`
	ToNS      uint64  `json:"to_ns,omitempty" jsonschema:"window end, absolute ns (exclusive, 0 = open)"`
	Max       int     `json:"max,omitempty" jsonschema:"hit bound: at most this many rows (default 256, hard cap 4096; n_hits carries the FULL qualifying count, truncated flags the cut)"`
}

type laSearchHit struct {
	ByteOff int64  `json:"byte_off,omitempty"`
	ChunkI  int    `json:"chunk_i,omitempty"`
	RunI    int    `json:"run_i,omitempty"`
	BitOff  int    `json:"bit_off,omitempty"`
	TNS     uint64 `json:"t_ns"`
	Level   int    `json:"level,omitempty"`
	DurNS   uint64 `json:"dur_ns,omitempty"`
	Context string `json:"context,omitempty"`
}

type laSearchOut struct {
	Kind      string        `json:"kind"`
	Pattern   string        `json:"pattern,omitempty"`
	Ch        int           `json:"ch,omitempty"`
	Hits      []laSearchHit `json:"hits"`
	NHits     int           `json:"n_hits"`
	FromNS    uint64        `json:"from_ns,omitempty"`
	ToNS      uint64        `json:"to_ns,omitempty"`
	Truncated bool          `json:"truncated,omitempty"`
}

type laActivityIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed capture's id (any mode: edge/samples count transitions, listen counts chunk arrivals with a bytes column)"`
	BinMS     int    `json:"bin_ms,omitempty" jsonschema:"the bin width in ms. Omit = derived so the window fills at most max_bins bins (the reply carries bin_ns). Given = exact — REFUSED with the minimum-bin coach when the window needs more bins than max_bins (never a silent coarsening)"`
	FromNS    uint64 `json:"from_ns,omitempty" jsonschema:"window start, absolute ns (0 = the capture's start)"`
	ToNS      uint64 `json:"to_ns,omitempty" jsonschema:"window end, absolute ns (0 = the capture's end)"`
	MaxBins   int    `json:"max_bins,omitempty" jsonschema:"the bin cap: default 256, hard cap 1024"`
}

type laActivityBin struct {
	FromNS uint64  `json:"from_ns"`
	ToNS   uint64  `json:"to_ns"`
	Edges  int     `json:"edges"`
	Bytes  int64   `json:"bytes,omitempty"`
	Ratio  float64 `json:"ratio"`
}

type laActivityOut struct {
	Mode      string          `json:"mode"`
	BinNS     uint64          `json:"bin_ns"`
	Bins      []laActivityBin `json:"bins"`
	NEdges    int             `json:"n_edges"`
	BusiestNS *uint64         `json:"busiest_from_ns,omitempty"`
	Truncated bool            `json:"truncated,omitempty"`
}

type laBitsIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed EDGE/SAMPLES capture's id (listen refuses — its bytes ARE the decode; use xuansi_la_decode_uart)"`
	Ch        int    `json:"ch,omitempty" jsonschema:"the channel (edge: pins[i]; samples: 0..7 = D0..D7). Omit = 0"`
	Rate      int    `json:"rate,omitempty" jsonschema:"the bit rate in bits/s. Omit = the run-width majority vote (the modal run IS one bit time on a clean NRZ; rate_derived=true in the reply). Given = the exact grid — refused beyond 1e9 (the 1ns floor) and on samples beyond the capture's own rate_hz"`
}

type laBitsSegment struct {
	I    int    `json:"i"`
	TNS  uint64 `json:"t_ns"`
	Bits string `json:"bits"`
}

type laBitsOut struct {
	Ch               int             `json:"ch"`
	RateNS           uint64          `json:"rate_ns"`
	RateDerived      bool            `json:"rate_derived"`
	FirstTNS         uint64          `json:"first_t_ns"`
	NBits            int             `json:"n_bits"`
	Segments         []laBitsSegment `json:"segments"`
	Violations       int             `json:"violations"`
	FirstViolationNS *uint64         `json:"first_violation_t_ns,omitempty"`
	Truncated        bool            `json:"truncated,omitempty"`
}

type laAdjacentIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed multi-channel capture's id (edge with ≥2 pins, or samples — a single-pin capture refuses: accompaniment is a cross-channel fact)"`
	Ch        int    `json:"ch" jsonschema:"the SUBJECT channel whose flips the window rides (edge: pins[ch]; samples: D_ch)"`
	WindowNS  uint64 `json:"window_ns,omitempty" jsonschema:"the ± window in ns. Omit = the subject's own beat ×2 (the modal gap between its flips); a channel with fewer than two flips refuses — give the window explicitly"`
}

type laAdjacentCh struct {
	Ch    int     `json:"ch"`
	Count int     `json:"count"`
	Ratio float64 `json:"ratio"`
}

type laAdjacentOut struct {
	Ch       int            `json:"ch"`
	WindowNS uint64         `json:"window_ns"`
	Flips    int            `json:"flips"`
	Peers    []laAdjacentCh `json:"peers"`
}

type laGlitchIn struct {
	CaptureID string   `json:"capture_id" jsonschema:"the landed EDGE/SAMPLES capture's id (listen refuses)"`
	Ch        int      `json:"ch,omitempty" jsonschema:"the channel (edge: pins[ch]; samples: D_ch). Omit = 0"`
	Ratio     *float64 `json:"ratio,omitempty" jsonschema:"the threshold multiplier: runs shorter than modal_run × ratio are the outliers. Domain (0, 1], default 0.5 — NOTE the modal run is the MAJORITY width: on an asymmetric duty (a 25%-duty PWM) the minority phase lists; read modal_run_ns in the reply before judging"`
}

type laGlitchRow struct {
	RunI  int    `json:"run_i"`
	TNS   uint64 `json:"t_ns"`
	Level int    `json:"level"`
	DurNS uint64 `json:"dur_ns"`
}

type laGlitchOut struct {
	Ch          int           `json:"ch"`
	ModeNS      uint64        `json:"modal_run_ns"`
	Ratio       float64       `json:"ratio"`
	ThresholdNS uint64        `json:"threshold_ns"`
	Count       int           `json:"count"`
	Rows        []laGlitchRow `json:"rows"`
	Truncated   bool          `json:"truncated,omitempty"`
}

type laEyeIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed EDGE/SAMPLES capture's id"`
	Ch        int    `json:"ch,omitempty" jsonschema:"the channel (edge: pins[ch]; samples: D_ch). Omit = 0"`
}

type laEyeOut struct {
	Ch        int     `json:"ch"`
	PeriodNS  uint64  `json:"period_ns"`
	NFold     int     `json:"n_fold"`
	MinHighNS *uint64 `json:"min_high_ns,omitempty"`
	MaxHighNS *uint64 `json:"max_high_ns,omitempty"`
	HighPPNS  *uint64 `json:"high_pp_ns,omitempty"`
	MinLowNS  *uint64 `json:"min_low_ns,omitempty"`
	MaxLowNS  *uint64 `json:"max_low_ns,omitempty"`
	LowPPNS   *uint64 `json:"low_pp_ns,omitempty"`
}

type laAlignIn struct {
	CaptureA string `json:"capture_a" jsonschema:"the reference capture's id (the segments and lag speak ITS axis)"`
	CaptureB string `json:"capture_b" jsonschema:"the second capture's id — SAME BOARD only (the la_diff rule: the alignment rides one clock); listen captures refuse (the level domain)"`
}

type laAlignSegment struct {
	FromNS   uint64  `json:"from_ns"`
	N        int     `json:"n"`
	OffsetNS float64 `json:"offset_ns"`
}

type laAlignOut struct {
	PeriodANS uint64           `json:"period_a_ns"`
	PeriodBNS uint64           `json:"period_b_ns"`
	LagNS     int64            `json:"lag_ns"`
	TolNS     uint64           `json:"tol_ns"`
	Matched   int              `json:"matched"`
	NA        int              `json:"n_events_a"`
	NB        int              `json:"n_events_b"`
	DriftPPM  *float64         `json:"drift_ppm,omitempty"`
	Segments  []laAlignSegment `json:"segments"`
}

type laExportIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed EDGE/SAMPLES capture's id (listen refuses — VCD is the LEVEL domain; the listen byte stream's seats are frames/decode_uart)"`
	Fmt       string `json:"fmt" jsonschema:"\"vcd\" (the only format this wave lands)"`
}

type laExportOut struct {
	Path      string `json:"path" jsonschema:"the written file's absolute path — ALWAYS <capture dir>/export.vcd (the object's own directory; no path parameter exists on this face, so traversal has nothing to ride)"`
	Bytes     int64  `json:"bytes"`
	Changes   int64  `json:"changes" jsonschema:"the value changes written (a same-tick multi-channel flip counts per channel)"`
	Truncated bool   `json:"truncated,omitempty" jsonschema:"the 2^22-change cap cut the export — the file holds the bounded head, stated here never silently"`
}

// registerLAWave2 adds the eight wave-2 tools (the register seam
// lives in tools_static.go).
func (ts *toolset) registerLAWave2(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_search",
		Description: "Search one capture for a PATTERN — the find-in-waveform seat. " + laAntiConfusion + ". " +
			"THREE KINDS. kind \"bytes\": a hex byte sequence (e.g. \"0103a0\") scanned through a LISTEN capture's chunk payload — every non-overlapping hit carries byte_off (the whole-stream byte index), chunk_i, an estimated t_ns (chunk start + in-chunk offset × symbol time), and a ±8-byte hex context. kind \"bits\": a 0/1 string (e.g. \"10110\") over a LEVEL capture's channel — the run sequence EXPANDED at the majority-vote unit (each run → round(width/unit) copies of its level), hits carry run_i (the la_runs coordinate) + bit_off + a ±16-bit context; a literal run-level match could never fire \"11\" (runs always alternate), the expansion is the semantics. kind \"timing\": a predicate {min_ns, max_ns, level} over run widths ([min, max), level 0|1) — the pulse-width filter that names every qualifying run {run_i, t_ns, level, dur_ns}. " +
			"Run 0 (the arm artifact) never matches the level kinds; the from_ns/to_ns window bounds hits by absolute t_ns. Bounded: rows default 256, hard cap 4096, n_hits carries the FULL qualifying count, truncated flags the cut. " +
			"NEXT: bytes hits cite byte_off — read the framing with xuansi_la_frames; bits hits cite run_i — read the neighbors with xuansi_la_runs, and xuansi_la_bits for the whole decoded stream at that rate. Daemon-local: no lease, no board contact.",
	}, ts.laSearch)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_activity",
		Description: "One capture's TIME-AXIS ACTIVITY MAP — where the capture was busy: equal bins of [from_ns, to), each with its edge count and the activity ratio (edges / the busiest bin, 1.0 = the peak). " + laAntiConfusion + ". " +
			"Per mode: edge counts TRANSITIONS per bin; samples counts CHANNEL edges (a same-tick multi-channel flip counts per channel — the mask popcount); listen counts CHUNK arrivals with a per-bin bytes column (the traffic volume). n_edges = the whole capture's count, busiest_from_ns names the peak bin. " +
			"bin_ms omitted → the width derived so the window fills at most max_bins bins (default 256, hard cap 1024; the reply carries bin_ns). bin_ms given → the exact width, REFUSED with the minimum-bin coach when the window needs more bins than the cap — never a silent coarsening. " +
			"NEXT: a bursty map (clusters of ratio 1.0 between silent spans) is framed traffic — window into a busy bin with xuansi_la_edges/xuansi_la_search; a flat map at a steady rate is a clock stream — xuansi_la_hist for its units. Daemon-local: no lease, no board contact.",
	}, ts.laActivity)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_bits",
		Description: "The UNIVERSAL DECODER — one channel resampled onto a bit grid and answered as a bit STRING you read directly (the seat computes every number; you never measure). " + laAntiConfusion + ". " +
			"The grid: rate omitted = the run-width majority vote (the modal run IS one bit time on a clean NRZ — rate_derived:true; the reply's rate_ns is the grid unit), rate given = the exact grid (1..1e9 bits/s; on samples refused beyond the capture's own rate_hz — bits finer than the sampling do not exist). The grid anchors at the channel's FIRST TRANSITION; run 0 (the arm artifact) is never a bit — the first decoded bit is the second wire bit (the ws2812 precedent, stated). " +
			"The honesty columns: violations counts every run whose width is NOT ≈ a whole number of grid cells (residual beyond rate_ns/4 — off-rate runs, sub-bit glitches); first_violation_t_ns names the first. A CLEAN protocol line answers violations 0. " +
			"Bounded: 256-bit segments, at most 4096 bits listed (n_bits carries the FULL count, truncated flags the cut). " +
			"THIS IS THE SELF-CONVERGING SEAT: take the bit string, frame it yourself (start bits, sync words, repeated units) — when the frame snaps out of the bits, the protocol is read; when violations pile up, the rate is wrong (try explicit rates around the derived one). NEXT: xuansi_la_search kind \"bits\" to FIND a framed unit inside it; xuansi_la_glitch when violations cluster (real glitches, not rate error). Daemon-local: no lease, no board contact.",
	}, ts.laBits)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_adjacent",
		Description: "The CROSS-CHANNEL accompaniment account — for every flip of ONE channel, did each OTHER channel flip within ±window_ns? The causal-linkage question (does the IRQ line move with the busy line? does CS falling accompany the clock burst?). " + laAntiConfusion + ". " +
			"window_ns omitted = the subject's own beat ×2 (the majority-vote modal gap between its flips; fewer than two flips refuses — give the window). Reply: per peer {ch, count = how many of the subject's flips were accompanied, ratio = count / the subject's flip count (1.0 = every flip had company)}, sorted count-descending then channel (deterministic). " +
			"Single-channel captures refuse (arm ≥2 pins, or use the samples bus). NOTE the direction: the account is symmetric in time (±window) but the RATIO speaks the subject's side — run it both directions when the linkage matters both ways. " +
			"NEXT: a high-ratio peer is a handshake — read the pair's relative timing with xuansi_la_bits (one channel each) and compare the anchors; xuansi_la_edges for the raw neighborhood. Daemon-local: no lease, no board contact.",
	}, ts.laAdjacent)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_glitch",
		Description: "The SUB-UNIT OUTLIER account — every run SHORTER than modal_run × ratio (default 0.5): the slivers a clean protocol never emits (contact bounce, capacitive coupling, DMA tears). " + laAntiConfusion + ". " +
			"modal_run_ns = the channel's run-width majority vote (the same unit xuansi_la_bits derives); threshold_ns = mode × ratio, echoed with the count; each row {run_i (the la_runs coordinate), t_ns, level, dur_ns}; run 0 (the arm artifact) neither votes nor lists. Rows bounded at 256 (count carries the FULL number, truncated flags the cut). " +
			"READ modal_run_ns BEFORE judging the rows: the threshold rides the MODAL run — on an asymmetric duty (a 25%-duty PWM votes the LOW its mode) the minority phase's every run lists as \"outlier\". That is the tool being honest about its definition, not a defect; ratio closer to 0 tightens it to true slivers. " +
			"NEXT: glitches clustering at one t_ns = a real event (read the neighborhood with xuansi_la_edges); uniformly scattered ones = a signal-quality story (check xuansi_la_bits' violations at the same channel — sub-bit runs show up in both). Daemon-local: no lease, no board contact.",
	}, ts.laGlitch)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_eye",
		Description: "The folded EYE numbers — one channel's alternation folded by its main period, answered as pure integers (the LLM measures NOTHING here; every number is computed). " + laAntiConfusion + ". " +
			"period_ns = the majority-vote modal PERIOD (each adjacent run-pair's span — one full cycle regardless of phase); n_fold = the folded pair count the statistics sit on. The openings: min/max high width, min/max low width, and the peak-to-peak spreads (high_pp_ns = max−min high width — the vertical jitter a receiver must tolerate). A one-sided line answers that level's columns absent, stated not zero-filled. " +
			"Needs ≥2 flips (one full alternation) — fewer refuses with the coach. Run 0 (the arm artifact) never folds. " +
			"NEXT: a clean eye (pp ≈ 0) at a known period = a healthy clock — cross-check with xuansi_la_hist kind \"period\"; a wide high_pp_ns against a tight low_pp_ns names WHICH level jitters — read the outliers with xuansi_la_glitch at a tight ratio. Daemon-local: no lease, no board contact.",
	}, ts.laEye)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_align",
		Description: "The TWO-CAPTURE ALIGNMENT — do two captures of one board share a beat, at what phase, and how far does that phase DRIFT across the span? " + laAntiConfusion + ". " +
			"Algorithm (stated): each side's beat = the majority-vote modal gap between its events (≥25% of the gaps must sit in the modal bin — aperiodic captures refuse honestly); the event lists cross-correlate on a circular axis of one beat (every pair votes, 512 bins); the winning phase refines against the NEAREST-event pairs (raw residuals — far events never match across the fold); the matched pairs fold into eight equal segments whose mean offsets carry the drift. " +
			"Reply: lag_ns = the NEAREST-PHASE alignment (inside ±half the beat — whole beats the two arms added are invisible from the streams alone), matched/n_events_a/n_events_b (events capped at 4096 per side, the head), tol_ns = beat/8, segments [{from_ns, n, offset_ns}] over capture A's axis, drift_ppm present iff ≥2 segments matched (the linear drift estimate; a drift past ±half the beat wraps, so the estimate saturates there — stated). " +
			"SAME-BOARD ONLY (the la_diff rule — one clock); pin sets must match on multi-pin captures; listen refuses (the level domain). " +
			"NEXT: drift_ppm ≈ 0 with matched ≈ n_events_a = two views of one stable signal (cross-validation, like a same=true diff); a growing offset walk = the clocks differ (a ~1000ppm walk reads as one board's RC against another's crystal) — window the segments with xuansi_la_edges to see it. Daemon-local: no lease, no board contact.",
	}, ts.laAlign)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_export",
		Description: "Export one capture as a VCD (Value Change Dump) — the file every waveform viewer (GTKWave, PulseView, WaveDrom) opens. " + laAntiConfusion + ". " +
			"THE PATH IS NEVER A PARAMETER: the file lands at <capture dir>/export.vcd beside data.bin (the store's own directory — traversal has nothing to ride); the write is idempotent (re-export overwrites). fmt \"vcd\" is the only format this wave lands. " +
			"Shape: timescale 1ns on the T0-RELATIVE axis (the absolute t0_ns rides a $comment); edge captures export one wire per armed pin (named pin<N>), samples the fixed DVP bus (D0..D7); the initial levels ride $dumpvars, every later mask change emits its timestamp once and the flipped channels' values (a same-tick multi-channel flip emits all its columns). " +
			"Bounded: at most 2^22 value changes (a larger capture truncates with truncated:true — the file holds the bounded head, stated in the reply, never a silent cut). LISTEN captures refuse — VCD is the LEVEL domain (a listen capture is a byte stream; its seats are xuansi_la_frames/xuansi_la_decode_uart). " +
			"NEXT: open it in your viewer for everything the bounded seats do not show; the cap cut a long capture → window the export source by re-capturing shorter, or read the tail with xuansi_la_edges. Daemon-local: no lease, no board contact.",
	}, ts.laExport)
}

// ---------------------------------------------------------------------------
// handlers — thin crossings over the daemon's wave-2 la_* requests

func (ts *toolset) laSearch(ctx context.Context, _ *mcpsdk.CallToolRequest, in laSearchIn) (*mcpsdk.CallToolResult, laSearchOut, error) {
	req := server.Request{Type: "la_search", CaptureID: in.CaptureID,
		Kind: in.Kind, Pattern: in.Pattern, Ch: in.Ch,
		FromNS: in.FromNS, ToNS: in.ToNS, Max: int64(in.Max),
		MinNS: in.MinNS, MaxNS: in.MaxNS, Level: in.Level}
	res, err := ts.call(ctx, req)
	if err != nil {
		return nil, laSearchOut{}, err
	}
	out := laSearchOut{
		Kind: str(res, "kind"), Pattern: str(res, "pattern"),
		Ch: num(res, "ch"), NHits: num(res, "n_hits"),
		FromNS: uint64(num(res, "from_ns")), ToNS: uint64(num(res, "to_ns")),
		Truncated: bol(res, "truncated"),
	}
	if rows, ok := res["hits"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			out.Hits = append(out.Hits, laSearchHit{
				ByteOff: int64(num(m, "byte_off")), ChunkI: num(m, "chunk_i"),
				RunI: num(m, "run_i"), BitOff: num(m, "bit_off"),
				TNS: uint64(num(m, "t_ns")), Level: num(m, "level"),
				DurNS: uint64(num(m, "dur_ns")), Context: str(m, "context"),
			})
		}
	}
	return nil, out, nil
}

func (ts *toolset) laActivity(ctx context.Context, _ *mcpsdk.CallToolRequest, in laActivityIn) (*mcpsdk.CallToolResult, laActivityOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_activity", CaptureID: in.CaptureID,
		BinMS: in.BinMS, FromNS: in.FromNS, ToNS: in.ToNS, MaxBins: in.MaxBins})
	if err != nil {
		return nil, laActivityOut{}, err
	}
	out := laActivityOut{
		Mode: str(res, "mode"), BinNS: uint64(num(res, "bin_ns")),
		NEdges: num(res, "n_edges"), Truncated: bol(res, "truncated"),
	}
	if rows, ok := res["bins"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			out.Bins = append(out.Bins, laActivityBin{
				FromNS: uint64(num(m, "from_ns")), ToNS: uint64(num(m, "to_ns")),
				Edges: num(m, "edges"), Bytes: int64(num(m, "bytes")),
				Ratio: f64(m, "ratio")})
		}
	}
	if v, ok := resU64Field(res, "busiest_from_ns"); ok {
		out.BusiestNS = &v
	}
	return nil, out, nil
}

func (ts *toolset) laBits(ctx context.Context, _ *mcpsdk.CallToolRequest, in laBitsIn) (*mcpsdk.CallToolResult, laBitsOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_bits", CaptureID: in.CaptureID,
		Ch: in.Ch, RateHz: in.Rate})
	if err != nil {
		return nil, laBitsOut{}, err
	}
	out := laBitsOut{
		Ch: num(res, "ch"), RateNS: uint64(num(res, "rate_ns")),
		RateDerived: bol(res, "rate_derived"), FirstTNS: uint64(num(res, "first_t_ns")),
		NBits: num(res, "n_bits"), Violations: num(res, "violations"),
		Truncated: bol(res, "truncated"),
	}
	if v, ok := resU64Field(res, "first_violation_t_ns"); ok {
		out.FirstViolationNS = &v
	}
	if rows, ok := res["segments"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			out.Segments = append(out.Segments, laBitsSegment{
				I: num(m, "i"), TNS: uint64(num(m, "t_ns")), Bits: str(m, "bits")})
		}
	}
	return nil, out, nil
}

func (ts *toolset) laAdjacent(ctx context.Context, _ *mcpsdk.CallToolRequest, in laAdjacentIn) (*mcpsdk.CallToolResult, laAdjacentOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_adjacent", CaptureID: in.CaptureID,
		Ch: in.Ch, WindowNS: in.WindowNS})
	if err != nil {
		return nil, laAdjacentOut{}, err
	}
	out := laAdjacentOut{
		Ch: num(res, "ch"), WindowNS: uint64(num(res, "window_ns")),
		Flips: num(res, "flips"),
	}
	if rows, ok := res["peers"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			out.Peers = append(out.Peers, laAdjacentCh{
				Ch: num(m, "ch"), Count: num(m, "count"), Ratio: f64(m, "ratio")})
		}
	}
	return nil, out, nil
}

func (ts *toolset) laGlitch(ctx context.Context, _ *mcpsdk.CallToolRequest, in laGlitchIn) (*mcpsdk.CallToolResult, laGlitchOut, error) {
	req := server.Request{Type: "la_glitch", CaptureID: in.CaptureID, Ch: in.Ch}
	if in.Ratio != nil {
		req.Ratio = in.Ratio
	}
	res, err := ts.call(ctx, req)
	if err != nil {
		return nil, laGlitchOut{}, err
	}
	out := laGlitchOut{
		Ch: num(res, "ch"), ModeNS: uint64(num(res, "modal_run_ns")),
		Ratio: f64(res, "ratio"), ThresholdNS: uint64(num(res, "threshold_ns")),
		Count: num(res, "count"), Truncated: bol(res, "truncated"),
	}
	if rows, ok := res["rows"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			out.Rows = append(out.Rows, laGlitchRow{
				RunI: num(m, "run_i"), TNS: uint64(num(m, "t_ns")),
				Level: num(m, "level"), DurNS: uint64(num(m, "dur_ns"))})
		}
	}
	return nil, out, nil
}

func (ts *toolset) laEye(ctx context.Context, _ *mcpsdk.CallToolRequest, in laEyeIn) (*mcpsdk.CallToolResult, laEyeOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_eye", CaptureID: in.CaptureID, Ch: in.Ch})
	if err != nil {
		return nil, laEyeOut{}, err
	}
	out := laEyeOut{
		Ch: num(res, "ch"), PeriodNS: uint64(num(res, "period_ns")),
		NFold: num(res, "n_fold"),
	}
	for _, col := range []struct {
		key string
		dst **uint64
	}{
		{"min_high_ns", &out.MinHighNS}, {"max_high_ns", &out.MaxHighNS},
		{"high_pp_ns", &out.HighPPNS},
		{"min_low_ns", &out.MinLowNS}, {"max_low_ns", &out.MaxLowNS},
		{"low_pp_ns", &out.LowPPNS},
	} {
		if v, ok := resU64Field(res, col.key); ok {
			*col.dst = &v
		}
	}
	return nil, out, nil
}

func (ts *toolset) laAlign(ctx context.Context, _ *mcpsdk.CallToolRequest, in laAlignIn) (*mcpsdk.CallToolResult, laAlignOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_align",
		CaptureID: in.CaptureA, CaptureIDB: in.CaptureB})
	if err != nil {
		return nil, laAlignOut{}, err
	}
	out := laAlignOut{
		PeriodANS: uint64(num(res, "period_a_ns")), PeriodBNS: uint64(num(res, "period_b_ns")),
		LagNS: i64(res, "lag_ns"), TolNS: uint64(num(res, "tol_ns")),
		Matched: num(res, "matched"), NA: num(res, "n_events_a"), NB: num(res, "n_events_b"),
	}
	if v, ok := res["drift_ppm"].(float64); ok {
		out.DriftPPM = &v
	}
	if rows, ok := res["segments"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			out.Segments = append(out.Segments, laAlignSegment{
				FromNS: uint64(num(m, "from_ns")), N: num(m, "n"),
				OffsetNS: f64(m, "offset_ns")})
		}
	}
	return nil, out, nil
}

func (ts *toolset) laExport(ctx context.Context, _ *mcpsdk.CallToolRequest, in laExportIn) (*mcpsdk.CallToolResult, laExportOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_export", CaptureID: in.CaptureID, Fmt: in.Fmt})
	if err != nil {
		return nil, laExportOut{}, err
	}
	return nil, laExportOut{
		Path:      str(res, "path"),
		Bytes:     int64(num(res, "bytes")),
		Changes:   int64(num(res, "changes")),
		Truncated: bol(res, "truncated"),
	}, nil
}
