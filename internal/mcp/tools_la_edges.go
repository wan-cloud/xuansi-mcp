package mcp

// tools_la_edges.go — the LA edge wave's MCP seat (T11): six
// xuansi_la_* tools over the daemon's edge crossings. All six read
// the daemon-local capture-object store (no lease, no board contact
// — the board can be gone); the capture itself rides xuansi_la_capture
// mode "edge" (the same blocking arm+verified pull, one seat over).
// The Descs are the product: each coaches the next step and states
// the honest bounds (the 256-row default / 4096 hard cap, the 2µs
// diff tolerance's resolution boundary, the decoder thresholds) —
// pinned by tools_la_edges_test.go's Desc audit.

import (
	"context"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/la"
	"xuansi/internal/server"
)

type laEdgesIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed EDGE capture's id (from xuansi_la_capture mode \"edge\" or xuansi_la_list)"`
	// FromNS/ToNS bound the rows by ABSOLUTE t_ns ([from, to), 0 =
	// open on that side); the first call omits them to read the head
	// and learn t0_ns.
	FromNS uint64 `json:"from_ns,omitempty" jsonschema:"window start, absolute ns — a run is listed iff its start t_ns ∈ [from, to). Omit/0 = from the capture's start. Read t0_ns from the reply (or xuansi_la_info) to place the window"`
	ToNS   uint64 `json:"to_ns,omitempty" jsonschema:"window end, absolute ns (exclusive). Omit/0 = to the capture's end"`
	Max    int    `json:"max,omitempty" jsonschema:"row bound: at most this many rows (default 256, hard cap 4096; a cut sets truncated:true — bounded, never silent)"`
}

type laEdgeRow struct {
	TNS   uint64 `json:"t_ns" jsonschema:"the instant this level STARTED — for every row but the first, the transition instant itself (the first row is the capture's opening level at t0_ns)"`
	Level int    `json:"level" jsonschema:"the line level after that instant: single-pin captures 0|1; multi-pin captures the MASK (bit i = pins[i]'s level)"`
	DurNS uint64 `json:"dur_ns" jsonschema:"how long this level held (until the next transition)"`
}

type laEdgesOut struct {
	Edges     []laEdgeRow `json:"edges"`
	T0NS      int64       `json:"t0_ns" jsonschema:"the capture's absolute time origin (the first run's start; every row's t_ns derives from it — the fw's single-source derivation)"`
	NRuns     int         `json:"n_runs" jsonschema:"the WHOLE capture's run count (the rows are a bounded view of it)"`
	Truncated bool        `json:"truncated,omitempty" jsonschema:"a BOUND cut happened (the max row cap) — more rows exist inside the window"`
}

type laRunsOut struct {
	Runs      []laRunRow `json:"runs"`
	T0NS      int64      `json:"t0_ns" jsonschema:"the capture's absolute time origin"`
	NRuns     int        `json:"n_runs" jsonschema:"the whole capture's run count"`
	SpanNS    uint64     `json:"span_ns" jsonschema:"the whole capture's time span (Σ every run's dur)"`
	Truncated bool       `json:"truncated,omitempty" jsonschema:"a BOUND cut happened (the max row cap)"`
}

type laRunRow struct {
	I     int    `json:"i" jsonschema:"the run's whole-capture index — the coordinate xuansi_la_measure's edge_i and xuansi_la_diff's run_index speak"`
	TNS   uint64 `json:"t_ns" jsonschema:"absolute start of the run"`
	Level int    `json:"level" jsonschema:"the level/mask held through the run"`
	DurNS uint64 `json:"dur_ns" jsonschema:"the run's width"`
}

type laHistIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed EDGE capture's id"`
	Kind      string `json:"kind" jsonschema:"what to histogram: \"pulse\" = every run's width (the symbol-rate estimator — est_baud is the baud when the modal pulse is one symbol, e.g. UART NRZ); \"period\" = each ADJACENT run pair's span dur[i]+dur[i+1] (one full cycle of an alternating signal — est_baud is then the frequency itself, the PWM/IR-carrier reading)"`
	Bins      int    `json:"bins,omitempty" jsonschema:"bin count 2..256 (default 32). Only populated bins are listed, ascending"`
}

type laHistOut struct {
	Kind     string      `json:"kind"`
	Bins     []laHistBin `json:"bins" jsonschema:"the populated bins, ascending lo_ns"`
	ModeNS   uint64      `json:"mode_ns,omitempty" jsonschema:"the MODAL duration: the median sample of the modal bin (ties to the lower bin) — exact for a clean signal"`
	EstBaud  float64     `json:"est_baud,omitempty" jsonschema:"1e9/mode_ns — the deterministic baud/frequency estimate (see kind for which reading it is)"`
	Duty     *float64    `json:"duty,omitempty" jsonschema:"single-pin captures only: Σ high-time / Σ all-time over the whole capture (the duty cycle; absent on multi-pin masks)"`
	NSamples int         `json:"n_samples" jsonschema:"the sample count (pulse: every run; period: runs−1)"`
}

type laHistBin struct {
	LoNS uint64 `json:"lo_ns"`
	HiNS uint64 `json:"hi_ns"`
	N    int    `json:"n"`
}

type laPointIn struct {
	TNS   *uint64 `json:"t_ns,omitempty" jsonschema:"an absolute instant (ns — the t_ns column's axis). PRECISION FLOOR: t_ns rides JSON float64 on every cross-process face, so values above 2^53 (~104 days of ns — only reachable on a board up longer than that continuously) lose exactness in transit; when citing large absolute t_ns prefer edge_i (an exact integer index)"`
	EdgeI *int    `json:"edge_i,omitempty" jsonschema:"a run index (the i column of xuansi_la_runs) — the point is that run's start"`
}

type laMeasureIn struct {
	CaptureID string    `json:"capture_id" jsonschema:"the landed EDGE capture's id"`
	A         laPointIn `json:"a" jsonschema:"point a: exactly one of t_ns or edge_i"`
	B         laPointIn `json:"b" jsonschema:"point b: exactly one of t_ns or edge_i"`
}

type laMeasureOut struct {
	DtNS   int64    `json:"dt_ns" jsonschema:"t_b − t_a, SIGNED (negative = the points were reversed; freq/duty are then absent with a note)"`
	FreqHz *float64 `json:"freq_hz,omitempty" jsonschema:"1e9/dt_ns, present iff dt_ns > 0"`
	Duty   *float64 `json:"duty,omitempty" jsonschema:"the fraction of [a, b) the (single-pin) line spent high — runs clipped to the window pro-rata, so mid-run points are honest"`
	Note   string   `json:"note,omitempty" jsonschema:"why freq/duty are absent, when they are"`
}

type laDiffIn struct {
	CaptureA string `json:"capture_a" jsonschema:"the reference capture's id (first_diff_t_ns and run_index speak ITS axes)"`
	CaptureB string `json:"capture_b" jsonschema:"the second capture's id"`
}

type laDiffOut struct {
	Same         bool    `json:"same" jsonschema:"true = the two captures agreed on every comparable run (masks equal, widths within tol_ns)"`
	FirstDiffTNS *uint64 `json:"first_diff_t_ns,omitempty" jsonschema:"absolute time of the first divergence, on capture A's axis"`
	RunIndex     *int    `json:"run_index,omitempty" jsonschema:"the divergent run's index in capture A (why \"length\": may equal n_runs_a — one stream ended with signal left)"`
	RunIndexB    *int    `json:"run_index_b,omitempty" jsonschema:"the same position in capture B"`
	Why          string  `json:"why,omitempty" jsonschema:"the divergence kind: \"level\" (masks differ), \"width\" (durations beyond tol_ns), \"length\" (one stream ended first)"`
	NRunsA       int     `json:"n_runs_a"`
	NRunsB       int     `json:"n_runs_b"`
	MatchedNS    uint64  `json:"matched_ns" jsonschema:"the span of the agreeing prefix (the walked distance before the verdict)"`
	TolNS        uint64  `json:"tol_ns" jsonschema:"the frozen width tolerance (2000ns) — it absorbs both engine accuracy bands (12.5ns tick runs; µs-scale task-clock-rebuilt runs past the 409µs frame threshold); differences BELOW it are not reported"`
}

type laDecodeWireIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed EDGE capture's id (single-pin — a wire protocol rides one line)"`
	Proto     string `json:"proto" jsonschema:"\"onewire\" (reset/presence + write slots: low ≥240µs = reset, presence = a ≥60µs low within 120µs of release, slot bit 1 iff the slot low <32µs; bytes assemble LSB-first) or \"ws2812\" (each (high,low) pair one bit, high ≥450ns = 1; a low ≥50µs is the reset/latch gap; bytes are the wire order G,R,B per LED, MSB-first)"`
}

type laWireTxn struct {
	TNS           uint64 `json:"t_ns" jsonschema:"the transaction's first run (onewire: the reset low / the first slot; ws2812: the first bit's high run)"`
	Kind          string `json:"kind" jsonschema:"onewire: \"reset\"|\"bytes\"; ws2812: \"frame\""`
	Presence      *bool  `json:"presence,omitempty" jsonschema:"onewire reset rows: the slave answered (true) or not (false)"`
	Latched       *bool  `json:"latched,omitempty" jsonschema:"ws2812 rows: true iff a reset gap closed the frame (false = the capture ended mid-frame)"`
	NBits         int    `json:"n_bits,omitempty" jsonschema:"the FULL bit count (bytes is a ≤64-byte preview)"`
	NBytes        int    `json:"n_bytes,omitempty"`
	NLEDs         int    `json:"n_leds,omitempty" jsonschema:"ws2812: n_bits/24"`
	RemainderBits int    `json:"remainder_bits,omitempty" jsonschema:"ws2812: n_bits%24 — a torn LED tail, stated not dropped"`
	Bytes         string `json:"bytes,omitempty" jsonschema:"hex, LSB-first (onewire) or wire-order GRB (ws2812)"`
}

type laDecodeWireOut struct {
	Proto     string      `json:"proto"`
	Txns      []laWireTxn `json:"txns" jsonschema:"at most 64 rows (a cut sets truncated:true)"`
	Truncated bool        `json:"truncated,omitempty"`
}

// registerLAEdges adds the six edge tools (the register seam lives in
// tools_static.go).
func (ts *toolset) registerLAEdges(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_edges",
		Description: "One EDGE capture's bounded TRANSITION view — each row {t_ns (the instant this level started), level (0|1, or the MASK on multi-pin captures: bit i = pins[i]), dur_ns (how long it held)}. The first row is the capture's opening level at t0_ns, not a transition. " + laAntiConfusion + ". " +
			"Reply {edges, t0_ns, n_runs, truncated}: the rows are a bounded view (max rows: default 256, hard cap 4096, the cut FLAGGED — never silent); the from_ns/to_ns window bounds by ABSOLUTE t_ns ([from, to), 0 = open) — omit them on the first call to read the head and learn t0_ns, then window. " +
			"READ IT LIKE THIS: a steady alternating dur_ns pair is a clock/PWM (confirm with xuansi_la_hist); a long idle run then bursts is framed traffic (xuansi_la_runs for the indices, xuansi_la_decode_wire when the shape is 1-Wire or WS2812). " +
			"NEXT: xuansi_la_hist (the duration statistics + duty), xuansi_la_measure (two points → dt/freq/duty), xuansi_la_diff (compare two captures of one signal). Daemon-local: no lease, no board contact.",
	}, ts.laEdges)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_runs",
		Description: "One EDGE capture's RUN-LENGTH view — the compressed encoding the engine itself recorded, each row {i (the run's whole-capture INDEX — the coordinate xuansi_la_measure's edge_i and xuansi_la_diff's run_index speak), t_ns, level, dur_ns} plus the whole-capture account {n_runs, span_ns}. " + laAntiConfusion + ". " +
			"Same bounded-window semantics as xuansi_la_edges (from_ns/to_ns absolute, max rows default 256 / cap 4096, truncated flags the cut) — the difference is the product: Edges reads transitions, Runs hands you the indices you cite back. " +
			"NEXT: cite an i into xuansi_la_measure ({edge_i}) for a two-point measurement; xuansi_la_edges for the transition reading. Daemon-local: no lease, no board contact.",
	}, ts.laRuns)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_hist",
		Description: "One EDGE capture's DETERMINISTIC duration statistics — the histogram that answers \"what are this line's timing units?\". " + laAntiConfusion + ". " +
			"kind \"pulse\" histograms EVERY run's width: mode_ns is the modal pulse (the median sample of the modal bin — exact on a clean signal), est_baud = 1e9/mode_ns is the SYMBOL-RATE estimate when the modal pulse is one symbol (UART-style NRZ; for a symmetric clock it reads 2× the frequency — use kind \"period\" there). kind \"period\" histograms each ADJACENT run pair's span (dur[i]+dur[i+1] — one full cycle of an alternating signal regardless of phase): est_baud is then the FREQUENCY itself (the PWM reading). " +
			"duty rides single-pin captures (Σ high / Σ all over the whole capture; ABSENT on multi-pin masks — a mask is not one line). bins 2..256 (default 32), only populated bins listed ascending; ties break to the LOWER bin. All deterministic — no curve fitting, no guessing past the data. " +
			"NEXT: a found mode is the timing unit — xuansi_la_measure pins it between two points ({edge_i} straight from xuansi_la_runs), xuansi_la_edges reads the raw rows around it. Daemon-local: no lease, no board contact.",
	}, ts.laHist)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_measure",
		Description: "One EDGE capture's TWO-POINT measurement: {dt_ns = t_b − t_a (SIGNED — reversed points answer negative with freq/duty absent and a note), freq_hz = 1e9/dt_ns (iff dt > 0), duty = the fraction of [a,b) the line spent high (single-pin captures; runs clipped to the window PRO-RATA, so points placed mid-run are honest)}. " + laAntiConfusion + ". " +
			"Each point is exactly one form: {t_ns: absolute ns} or {edge_i: a run index — the i column of xuansi_la_runs, resolving to that run's start}. edge_i is the sturdy form (indices survive window arithmetic); t_ns places a point anywhere, including between transitions — but t_ns crosses every face as JSON float64, so absolute values above 2^53 ns (~104 days of continuous uptime) lose low-bit exactness in transit: on a long-up board cite edge_i (exact integers both ways), not pasted t_ns digits. " +
			"Window honesty: a window overhanging the captured extent (start before the first edge, end past the last) answers duty over the full dt while the numerator covers only the capture — the note says the fraction under-reports. " +
			"NEXT: measure one period (edge_i of consecutive same-direction edges) → freq_hz is the line's rate; compare against xuansi_la_hist's est_baud for the population view. Daemon-local: no lease, no board contact.",
	}, ts.laMeasure)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_diff",
		Description: "Compare TWO edge captures of one signal — the two-probe experiment's verdict: \"do these captures show the same waveform?\". " + laAntiConfusion + ". " +
			"The walk pairs run i of capture_a with run i of capture_b, FIRST-EDGE ALIGNED: each capture's run 0 is the pre-history its arm opened on — its LEVEL compares (signal), its WIDTH never does (an arm artifact). Masks must match, then widths within the frozen tol_ns = 2000ns — the tolerance absorbs both engine accuracy bands (12.5ns tick-measured runs; µs-scale task-clock-rebuilt runs past the 409µs frame threshold), so differences BELOW 2µs are beyond the engine's cross-capture repeatability and are not reported. " +
			"Reply: same=true with matched_ns (the agreeing prefix's span), or the first divergence {first_diff_t_ns (capture A's absolute axis), run_index / run_index_b, why ∈ level|width|length} — \"length\" = one stream ended with signal left (run_index may then EQUAL n_runs_a: past its own end, an honest marker). n_runs_a/n_runs_b carry both lengths. " +
			"SAME-BOARD ONLY (frozen): the engines' timing must come from one board's clock — captures from different boards refuse with the coach. Both single-pin captures always compare (two views of one line, e.g. a protocol probed at two points); multi-pin captures compare only with EQUAL pin sets (mask bits must mean the same lines). " +
			"NEXT: a same=true pair cross-validates the capture chain; a divergence at run_index i localizes WHERE — read both sides with xuansi_la_runs and cite the row into xuansi_la_measure. Daemon-local: no lease, no board contact.",
	}, ts.laDiff)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_decode_wire",
		Description: "Decode one EDGE capture as a WIRE PROTOCOL — the transaction list with the frozen thresholds stated (deterministic readers, never guessers: shapes the thresholds cannot place surface in the row's honest counts, not an error). " + laAntiConfusion + ". " +
			"proto \"onewire\": low ≥240µs = a RESET row (presence: a ≥60µs low starting within 120µs of the release — the slave's answer); every other low is a SLOT, bit 1 iff the slot's low <32µs; bytes assemble LSB-first; a ≥480µs high closes the bytes row. " +
			"proto \"ws2812\": every (high, low) pair is one BIT, high ≥450ns = 1; bytes are the wire order (G,R,B per LED, MSB-first per byte); a low ≥50µs is the RESET gap that LATCHES the frame (latched:false = the capture ended mid-frame; remainder_bits = a torn LED tail, stated not dropped). RUN 0 NEVER BECOMES A BIT — its width is the arm artifact (the capture's opening level held since arm), so a line that sat high at arm loses its first bit honestly (n_bits short by one) rather than guessing it." +
			"Bounded: ≤64 transaction rows (truncated flags the cut), bytes previews ≤64 bytes with n_bits carrying the full count. Single-pin captures only — a wire protocol rides ONE line (multi-pin refuses). " +
			"NEXT: the transaction bytes are the protocol conversation — read them against the device's register map; xuansi_la_edges around a row's t_ns shows the raw timing when a decode looks wrong. Daemon-local: no lease, no board contact.",
	}, ts.laDecodeWire)
}

// ---------------------------------------------------------------------------
// handlers — thin crossings over the daemon's edge la_* requests

func (ts *toolset) laEdges(ctx context.Context, _ *mcpsdk.CallToolRequest, in laEdgesIn) (*mcpsdk.CallToolResult, laEdgesOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_edges", CaptureID: in.CaptureID,
		FromNS: in.FromNS, ToNS: in.ToNS, Max: int64(in.Max)})
	if err != nil {
		return nil, laEdgesOut{}, err
	}
	return nil, laEdgesOut{
		Edges:     laEdgeRowsDecode(res["edges"]),
		T0NS:      i64(res, "t0_ns"),
		NRuns:     num(res, "n_runs"),
		Truncated: bol(res, "truncated"),
	}, nil
}

func (ts *toolset) laRuns(ctx context.Context, _ *mcpsdk.CallToolRequest, in laEdgesIn) (*mcpsdk.CallToolResult, laRunsOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_runs", CaptureID: in.CaptureID,
		FromNS: in.FromNS, ToNS: in.ToNS, Max: int64(in.Max)})
	if err != nil {
		return nil, laRunsOut{}, err
	}
	return nil, laRunsOut{
		Runs:      laRunRowsDecode(res["runs"]),
		T0NS:      i64(res, "t0_ns"),
		NRuns:     num(res, "n_runs"),
		SpanNS:    uint64(num(res, "span_ns")),
		Truncated: bol(res, "truncated"),
	}, nil
}

func (ts *toolset) laHist(ctx context.Context, _ *mcpsdk.CallToolRequest, in laHistIn) (*mcpsdk.CallToolResult, laHistOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_hist", CaptureID: in.CaptureID,
		Kind: in.Kind, Bins: in.Bins})
	if err != nil {
		return nil, laHistOut{}, err
	}
	out := laHistOut{
		Kind:     str(res, "kind"),
		ModeNS:   uint64(num(res, "mode_ns")),
		EstBaud:  f64(res, "est_baud"),
		NSamples: num(res, "n_samples"),
	}
	if rows, ok := res["bins"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			out.Bins = append(out.Bins, laHistBin{
				LoNS: uint64(num(m, "lo_ns")), HiNS: uint64(num(m, "hi_ns")), N: num(m, "n")})
		}
	}
	if _, has := res["duty"]; has {
		d := f64(res, "duty")
		out.Duty = &d
	}
	return nil, out, nil
}

func (ts *toolset) laMeasure(ctx context.Context, _ *mcpsdk.CallToolRequest, in laMeasureIn) (*mcpsdk.CallToolResult, laMeasureOut, error) {
	a := la.MeasurePoint{TNS: in.A.TNS, EdgeI: in.A.EdgeI}
	b := la.MeasurePoint{TNS: in.B.TNS, EdgeI: in.B.EdgeI}
	res, err := ts.call(ctx, server.Request{Type: "la_measure", CaptureID: in.CaptureID,
		A: &a, B: &b})
	if err != nil {
		return nil, laMeasureOut{}, err
	}
	out := laMeasureOut{DtNS: i64(res, "dt_ns"), Note: str(res, "note")}
	if _, has := res["freq_hz"]; has {
		f := f64(res, "freq_hz")
		out.FreqHz = &f
	}
	if _, has := res["duty"]; has {
		d := f64(res, "duty")
		out.Duty = &d
	}
	return nil, out, nil
}

func (ts *toolset) laDiff(ctx context.Context, _ *mcpsdk.CallToolRequest, in laDiffIn) (*mcpsdk.CallToolResult, laDiffOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_diff",
		CaptureID: in.CaptureA, CaptureIDB: in.CaptureB})
	if err != nil {
		return nil, laDiffOut{}, err
	}
	out := laDiffOut{
		Same:      bol(res, "same"),
		Why:       str(res, "why"),
		NRunsA:    num(res, "n_runs_a"),
		NRunsB:    num(res, "n_runs_b"),
		MatchedNS: uint64(num(res, "matched_ns")),
		TolNS:     uint64(num(res, "tol_ns")),
	}
	if v, ok := resU64Field(res, "first_diff_t_ns"); ok {
		out.FirstDiffTNS = &v
	}
	if v, ok := res["run_index"].(float64); ok {
		i := int(v)
		out.RunIndex = &i
	}
	if v, ok := res["run_index_b"].(float64); ok {
		i := int(v)
		out.RunIndexB = &i
	}
	return nil, out, nil
}

func (ts *toolset) laDecodeWire(ctx context.Context, _ *mcpsdk.CallToolRequest, in laDecodeWireIn) (*mcpsdk.CallToolResult, laDecodeWireOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_decode_wire", CaptureID: in.CaptureID, Proto: in.Proto})
	if err != nil {
		return nil, laDecodeWireOut{}, err
	}
	out := laDecodeWireOut{Proto: str(res, "proto"), Truncated: bol(res, "truncated")}
	if rows, ok := res["txns"].([]any); ok {
		for _, el := range rows {
			m, _ := el.(map[string]any)
			row := laWireTxn{
				TNS:           uint64(num(m, "t_ns")),
				Kind:          str(m, "kind"),
				NBits:         num(m, "n_bits"),
				NBytes:        num(m, "n_bytes"),
				NLEDs:         num(m, "n_leds"),
				RemainderBits: num(m, "remainder_bits"),
				Bytes:         str(m, "bytes"),
			}
			if p, ok := m["presence"].(bool); ok {
				row.Presence = &p
			}
			if l, ok := m["latched"].(bool); ok {
				row.Latched = &l
			}
			out.Txns = append(out.Txns, row)
		}
	}
	return nil, out, nil
}

// laEdgeRowsDecode / laRunRowsDecode walk the daemon's row arrays
// into the seat's shapes.
func laEdgeRowsDecode(v any) []laEdgeRow {
	rows := arr(v)
	out := make([]laEdgeRow, 0, len(rows))
	for _, el := range rows {
		m, ok := el.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, laEdgeRow{
			TNS:   uint64(num(m, "t_ns")),
			Level: num(m, "level"),
			DurNS: uint64(num(m, "dur_ns")),
		})
	}
	return out
}

func laRunRowsDecode(v any) []laRunRow {
	rows := arr(v)
	out := make([]laRunRow, 0, len(rows))
	for _, el := range rows {
		m, ok := el.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, laRunRow{
			I:     num(m, "i"),
			TNS:   uint64(num(m, "t_ns")),
			Level: num(m, "level"),
			DurNS: uint64(num(m, "dur_ns")),
		})
	}
	return out
}
