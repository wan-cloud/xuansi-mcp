package mcp

// tools_la.go — the LA wave's MCP seat, first batch (T7): six
// xuansi_la_* tools over the daemon's la_* requests. xuansi_la_capture
// is the arm point (blocking pull-back, the xuansi_capture precedent
// one seat over); the other five read the daemon-local capture-object
// store (no lease, no board contact). The Descs are the product: each
// coaches the next step and states the honest bounds (base64 bytes,
// the 8192-frame window's tail_bytes account, the parity claim's
// 7E1/7O1 domain) — frozen coaching discipline, pinned by
// tools_la_test.go's Desc audit.

import (
	"context"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// laAntiConfusion is the frozen one-liner every la_* Desc carries (or
// points at): the distinction from xuansi_capture — event-source
// recording vs LA capture object. State it, prevent the mix.
const laAntiConfusion = "This is the LA capture-OBJECT face — NOT xuansi_capture (that tool records one board EVENT SOURCE to a spooled file for post-processing; no timing, no framing, no analysis)"

type laCaptureIn struct {
	Board string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Mode  string `json:"mode,omitempty" jsonschema:"capture mode; this wave lands \"listen\" only (passive UART listen — RX-only by construction, the engine has no TX path). The edge/samples modes arrive with later waves and refuse here. Omit = listen"`
	Port  int    `json:"port" jsonschema:"the hw UART the listen engine rides: 1 (uart1) or 2 (uart2)"`
	Pin   int    `json:"pin" jsonschema:"the GPIO the line under study is wired to — explicit, always: the ports' DEFAULT pins are board-specific traps this face never guesses, and the fw's refusal lists the banned pins"`
	Baud  any    `json:"baud" jsonschema:"the line's baud: an integer 1200..5000000, or \"auto\" — the board measures it (RMT bit-width) and the reply's meta.baud carries the resolved value"`
	// RingKB sizes the on-board ring; 0 (omitted) = the fw default.
	RingKB int `json:"ring_kb,omitempty" jsonschema:"on-board ring buffer KB (0 = fw default). Admission: active rings + this request must fit la_ring_budget_kb=4096 (read caps) — the board's refusal itemizes the account. The capture ends reason \"full\" when the ring fills"`
	// Seconds is the time bound → the fw's max_ms (the arm crosses
	// ×1000, clamped at 600 like capture's own seconds rule).
	Seconds int64  `json:"seconds,omitempty" jsonschema:"time bound in seconds (1..600 — the fw's max_ms cap is 600000ms): the capture ends reason \"limit\" when it passes; omit for the fw default. The call BLOCKS until the capture ends AND the verified pull-back lands"`
	Holder  string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type laCaptureOut struct {
	CaptureID string         `json:"capture_id" jsonschema:"the landed object's id — the key every other la_* tool takes"`
	Path      string         `json:"path" jsonschema:"absolute data.bin path on the daemon's host (the frozen chunk stream; post-process with programs when the bounded analysis views are not enough)"`
	Meta      map[string]any `json:"meta" jsonschema:"the object's full meta ledger: baud (the RESOLVED value — what \"auto\" measured), bytes/chunks, t_start_ns/t_end_ns, reason (full|stopped|limit), sha256 (the verified two-pass identity), the fw's arm account (ring_kb, max_ms, chunks_dropped, bytes_dropped, hw_ovf, armed_ns, stopped_ns). incomplete ABSENT = the lossless promise HELD; incomplete:true = forensics only"`
}

// laMeta is the one-object detail read's shape: the meta verbatim.
type laInfoIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed object's id (from xuansi_la_capture's reply or xuansi_la_list)"`
}

type laFramesIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed object's id (from xuansi_la_capture's reply or xuansi_la_list)"`
	// IdleNS overrides the segmentation threshold; 0 = the capture's
	// own (idle_syms × bit time at meta.baud).
	IdleNS uint64 `json:"idle_ns,omitempty" jsonschema:"idle threshold override in nanoseconds — the silence that splits frames. Omit/0 = the capture's own derivation (idle_syms symbols × the baud's bit time). Finer = more/shorter frames, coarser = fewer/longer; every la analysis applies the same cut"`
}

// laFrame is one bounded frame row (the daemon's frozen L1 shape).
type laFrame struct {
	TNS         uint64 `json:"t_ns" jsonschema:"timestamp of the frame's first chunk (the fw task's read of it — delivery latency included)"`
	GapBeforeNS uint64 `json:"gap_before_ns" jsonschema:"estimated silence since the previous frame's last byte (0 on the first frame) — the protocol's rhythm"`
	NBytes      int    `json:"n_bytes" jsonschema:"the frame's FULL payload byte count"`
	Bytes       string `json:"bytes" jsonschema:"the payload preview, STANDARD BASE64 (decode before reading) — at most 256 bytes; n_bytes carries the true length"`
}

type laFramesOut struct {
	Frames    []laFrame `json:"frames"`
	Baud      int       `json:"baud" jsonschema:"the baud the timing derived from (the capture's resolved meta baud)"`
	IdleNS    uint64    `json:"idle_ns" jsonschema:"the idle threshold actually applied"`
	Truncated bool      `json:"truncated,omitempty" jsonschema:"a BOUND cut happened: more than 256 frames, or a frame previewed at the 256-byte cap — the views are bounded, the cut is flagged, never silent"`
}

type laRepeatIn struct {
	CaptureID string `json:"capture_id" jsonschema:"the landed object's id (from xuansi_la_capture's reply or xuansi_la_list)"`
}

type laRepeatOut struct {
	PeriodNS  *uint64 `json:"period_ns,omitempty" jsonschema:"mean span of one repeating unit; ABSENT with repeats=0 = nothing repeats consecutively (the honest zero)"`
	Repeats   int     `json:"repeats" jsonschema:"consecutive occurrences of the longest repeating frame-sequence unit (≥2 = a periodic run: a poll loop, a heartbeat, a fixed-format broadcast)"`
	TailBytes int64   `json:"tail_bytes" jsonschema:"payload bytes OUTSIDE the longest repeating run (preamble + traffic after it — the irregular part). ALSO the window bound's one account: frames beyond the 8192-frame scan window surface ONLY here as tail, no separate flag"`
}

type laDecodeParams struct {
	Baud   int    `json:"baud,omitempty" jsonschema:"baud override (integer 1200..5000000): re-segments at this symbol time — the gaps and idle threshold re-derive, the BYTES are never rewritten. Use when meta.baud looks wrong (a mis-measured \"auto\"); omit for the capture's own"`
	Parity string `json:"parity,omitempty" jsonschema:"parity claim: \"E\" or \"O\" — adds a per-frame parity_ok column checked against hardware facts. The judgment ASSUMES the line is actually 7E1/7O1 (the 8N1 listen seat holds those 10-bit characters whole; an 8E1/8O1 line desyncs the hardware listener before this seat — other line shapes make the verdict a reference, not a verdict)"`
}

type laDecodeIn struct {
	CaptureID string          `json:"capture_id" jsonschema:"the landed object's id (from xuansi_la_capture's reply or xuansi_la_list)"`
	Params    *laDecodeParams `json:"params,omitempty" jsonschema:"optional overrides {baud, parity}; omit for the capture's own baud and no parity column"`
}

type laUARTFrame struct {
	TNS         uint64 `json:"t_ns"`
	GapBeforeNS uint64 `json:"gap_before_ns"`
	NBytes      int    `json:"n_bytes"`
	Bytes       string `json:"bytes" jsonschema:"the payload preview, STANDARD BASE64 (decode before reading) — at most 256 bytes; n_bytes carries the true length"`
	ParityOK    *bool  `json:"parity_ok,omitempty" jsonschema:"present iff a parity claim rode the decode: every byte of the frame satisfies the claim (true) or some byte does not (false)"`
}

type laDecodeOut struct {
	Frames    []laUARTFrame `json:"frames"`
	Baud      int           `json:"baud" jsonschema:"the baud the decode segmented at (the override when one rode, else the capture's meta baud)"`
	IdleNS    uint64        `json:"idle_ns" jsonschema:"the idle threshold the decode applied"`
	Parity    string        `json:"parity,omitempty" jsonschema:"the parity claim that rode the decode (\"E\"|\"O\"; absent = none)"`
	Truncated bool          `json:"truncated,omitempty" jsonschema:"a BOUND cut happened: more than 256 frames, or a frame previewed at the 256-byte cap"`
}

// registerLA adds the six la_* tools (the register seam lives in
// tools_static.go).
func (ts *toolset) registerLA(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_capture",
		Description: "Arm the board's LA LISTEN ENGINE (protocol v9) and BLOCK until the capture ends AND the on-board ring is pulled back VERIFIED (two-pass sha256 identity) — the answer is a capture OBJECT the other la_* tools analyze. " + laAntiConfusion + ": xuansi_capture records an event source to a file; THIS arms the hardware UART as a passive protocol listener (RX-only by construction — no TX path exists, fw-frozen) that timestamps every byte, then lands captures/la/<capture_id>/{data.bin,meta.json} on the daemon host. " +
			"THE WIRE: connect the line under study to pin (a free GPIO on the listening board — its pin map; the fw refuses banned pins with the list), port selects the hw UART (1/2), baud the line's integer rate 1200..5000000 or \"auto\" (measured; meta.baud carries what it measured). ring_kb sizes the on-board ring (admission vs la_ring_budget_kb=4096 — read caps; ring full ends reason \"full\"), seconds the time bound (1..600; ends reason \"limit\"). " +
			"BLOCKING: the call holds until the pull-back is verified — budget seconds plus read-back time. " +
			"THE VERDICT IS IN THE REPLY: meta.incomplete ABSENT = the lossless promise HELD (sha256 verified); an ERROR naming the capture id = the pull broke (cancel, link death, checksum) and the incomplete object landed as FORENSICS — readable via xuansi_la_info/xuansi_la_frames, never a trustworthy capture. " +
			"Mutual exclusion: the same hw UART cannot run bench uart primitives and the listener at once (the board refuses, naming the conflict — both directions). " +
			"FIRST LOOK after a capture: xuansi_la_frames (the idle framing — the protocol's sentence structure), xuansi_la_repeat when the traffic looks periodic (poll loops, heartbeats), xuansi_la_decode_uart for a per-frame parity verdict. Holder semantics: " + holderDesc + ".",
	}, ts.laCapture)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_list",
		Description: "List the daemon's landed LA capture objects, newest first — every object the la_* analysis tools can read (the daemon-local store, every board's captures; no lease, no board contact — the board can be gone). " + laAntiConfusion + " (that face's spool files are not objects here). " +
			"Each row is the object's full meta: identity (capture_id, board, mode, port/pin), the resolved baud, the payload account (bytes, chunks, t_start_ns/t_end_ns), how the capture ended (reason full|stopped|limit), the fw's arm ledger mirrored at pull time (ring_kb, max_ms, chunks_dropped, bytes_dropped, hw_ovf, armed_ns, stopped_ns — the honest loss columns), sha256 (the verified identity), and incomplete:true on a pull that broke (forensics only). " +
			"Retention is FIFO over a 512MB quota — oldest objects delete first, the newest never alone. NEXT: pick a capture_id, read it with xuansi_la_info, then xuansi_la_frames for the framing view.",
	}, ts.laList)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_info",
		Description: "One LA capture object's full meta ledger — the same columns xuansi_la_list rows carry, read one object deep. Read the ledger BEFORE analyzing: bytes/chunks = the traffic volume, t_start_ns/t_end_ns = the window, reason = how it ended (full = the ring filled at its byte bound, limit = the time bound passed, stopped = ended early), baud = the rate the timing derives from. " +
			"The honest loss columns: chunks_dropped/bytes_dropped nonzero = the engine dropped payload under congestion (the account is short against what the wire carried); hw_ovf = hardware overflow events. armed_ns/stopped_ns = the engine's own clock. sha256 = the verified two-pass identity (ABSENT on an incomplete landing — never trust it as lossless). " +
			"Daemon-local: no lease, no board contact. NEXT: xuansi_la_frames on this id (the framing view), xuansi_la_repeat (is it periodic?), xuansi_la_decode_uart (parity verdict).",
	}, ts.laInfo)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_frames",
		Description: "Segment one listen capture into idle-delimited FRAMES — the L1 framing primitive: where the protocol's sentences begin and end. " + laAntiConfusion + ". " +
			"Reply {frames, baud, idle_ns, truncated}: each frame {t_ns, gap_before_ns (the silence before it — the protocol's rhythm), n_bytes (the FULL count), bytes (the payload preview, STANDARD BASE64 — decode before reading; ≤256 bytes per frame)}. idle_ns = the threshold applied (your override, or the capture's own idle_syms × bit-time derivation). truncated=true flags a BOUND cut (the 256-frame list cap or a 256-byte preview cap) — the bounded head view, never silent, never unbounded. " +
			"READ IT LIKE THIS: a steady gap_before_ns with equal n_bytes = a fixed-format heartbeat (confirm with xuansi_la_repeat); a fixed gap with varying payloads = command/response pairs; gap clusters name the protocol's cadence. " +
			"NEXT: xuansi_la_repeat (the periodicity account), xuansi_la_decode_uart (per-frame parity verdict + a baud re-check when meta.baud looks wrong). Daemon-local: no lease, no board contact.",
	}, ts.laFrames)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_repeat",
		Description: "Measure one listen capture's frame-level PERIODICITY: the LONGEST frame-sequence unit (fingerprinted by exact bytes) that repeats CONSECUTIVELY. " + laAntiConfusion + ". " +
			"Reply {period_ns, repeats, tail_bytes}: repeats ≥2 with period_ns (the mean span of one unit) = a periodic run — a poll loop, a heartbeat, a status broadcast; repeats=0 with period_ns ABSENT = nothing repeats consecutively (the honest zero — sporadic or varying traffic); tail_bytes = payload bytes OUTSIDE the longest run (the preamble before it + the irregular traffic after it). " +
			"WINDOW BOUND, stated once and honestly: the scan reads the first 8192 frames; frames beyond the window are NOT flagged anywhere else — they surface ONLY as tail_bytes (a >8192-frame capture counts the rest as tail; there is no separate overflow flag by design). " +
			"Frames segment at the capture's own idle threshold (the same cut xuansi_la_frames shows). NEXT: a found period is the cadence — xuansi_la_frames to read the unit's bytes, xuansi_la_decode_uart for its parity discipline; tail_bytes ≈ the whole capture with repeats=0 means the traffic is transactional: read the frames directly. Daemon-local: no lease, no board contact.",
	}, ts.laRepeat)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_decode_uart",
		Description: "The frame-level UART decode of one listen capture — the same idle framing xuansi_la_frames applies, plus an optional PARITY CLAIM checked against hardware facts (recomputed from the received bytes and the parity bit the wire carried — never guessed). " + laAntiConfusion + ". " +
			"params {baud, parity}: baud (integer 1200..5000000) RE-SEGMENTS at that symbol time (gaps and the idle threshold re-derive — the BYTES are never rewritten): use it when meta.baud looks wrong (a mis-measured \"auto\", or the line is not what the arm claimed). parity \"E\"|\"O\" adds the per-frame parity_ok column. " +
			"PARITY'S HONEST DOMAIN (frozen statement): the listen engine captures fixed 8N1, so a parity claim reads the capture as a 7-data-bit + parity line — 7E1/7O1, the same 10-bit character whose bits the 8N1 seat holds whole: each received byte then CARRIES the line's parity bit (in its MSB), and parity_ok says every byte of the frame satisfies the claim (even popcount for E, odd for O). THE JUDGMENT ASSUMES THE LINE IS ACTUALLY 7E1/7O1: an 8E1/8O1 line (11-bit characters) desyncs the 8N1 listener AT THE HARDWARE before this seat exists — its frames would already read garbage; any other line shape makes the verdict a REFERENCE, not a verdict. " +
			"Without a claim the reply is frames' shape exactly (bytes still STANDARD BASE64, n_bytes the full count, truncated the same bound flag). NEXT: parity_ok=false on every frame of an otherwise clean decode = the line's parity discipline mismatches the claim — try the other one; the byte payloads are the protocol itself: read them framed (xuansi_la_frames) against the device's register map. Daemon-local: no lease, no board contact.",
	}, ts.laDecodeUART)
}

// ---------------------------------------------------------------------------
// handlers — thin crossings over the daemon's la_* requests

func (ts *toolset) laCapture(ctx context.Context, _ *mcpsdk.CallToolRequest, in laCaptureIn) (*mcpsdk.CallToolResult, laCaptureOut, error) {
	res, err := ts.callHeld(ctx, server.Request{
		Type: "la_capture", Board: in.Board, Holder: in.Holder,
		Mode: in.Mode, LaPort: in.Port, LaPin: in.Pin, Baud: in.Baud,
		RingKB: in.RingKB, MaxSeconds: in.Seconds,
	})
	if err != nil {
		return nil, laCaptureOut{}, err
	}
	return nil, laCaptureOut{
		CaptureID: str(res, "capture_id"),
		Path:      str(res, "path"),
		Meta:      obj(res["meta"]),
	}, nil
}

func (ts *toolset) laList(ctx context.Context, _ *mcpsdk.CallToolRequest, _ struct{}) (*mcpsdk.CallToolResult, any, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_list"})
	if err != nil {
		return nil, nil, err
	}
	// the daemon's listing crosses VERBATIM (the meta shape is frozen
	// at capture — the seat does not re-shape ground truth).
	return nil, res["captures"], nil
}

func (ts *toolset) laInfo(ctx context.Context, _ *mcpsdk.CallToolRequest, in laInfoIn) (*mcpsdk.CallToolResult, any, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_info", CaptureID: in.CaptureID})
	if err != nil {
		return nil, nil, err
	}
	return nil, res, nil // the full meta verbatim
}

func (ts *toolset) laFrames(ctx context.Context, _ *mcpsdk.CallToolRequest, in laFramesIn) (*mcpsdk.CallToolResult, laFramesOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_frames", CaptureID: in.CaptureID, IdleNS: in.IdleNS})
	if err != nil {
		return nil, laFramesOut{}, err
	}
	return nil, laFramesOut{
		Frames:    laFramesDecode(res["frames"]),
		Baud:      int(num(res, "baud")),
		IdleNS:    uint64(num(res, "idle_ns")),
		Truncated: bol(res, "truncated"),
	}, nil
}

func (ts *toolset) laRepeat(ctx context.Context, _ *mcpsdk.CallToolRequest, in laRepeatIn) (*mcpsdk.CallToolResult, laRepeatOut, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_repeat", CaptureID: in.CaptureID})
	if err != nil {
		return nil, laRepeatOut{}, err
	}
	out := laRepeatOut{Repeats: num(res, "repeats"), TailBytes: int64(num(res, "tail_bytes"))}
	if p, ok := resU64Field(res, "period_ns"); ok {
		out.PeriodNS = &p
	}
	return nil, out, nil
}

func (ts *toolset) laDecodeUART(ctx context.Context, _ *mcpsdk.CallToolRequest, in laDecodeIn) (*mcpsdk.CallToolResult, laDecodeOut, error) {
	req := server.Request{Type: "la_decode_uart", CaptureID: in.CaptureID}
	if in.Params != nil {
		if in.Params.Baud != 0 {
			req.Baud = in.Params.Baud
		}
		req.Parity = in.Params.Parity
	}
	res, err := ts.call(ctx, req)
	if err != nil {
		return nil, laDecodeOut{}, err
	}
	out := laDecodeOut{
		Frames:    laUARTFramesDecode(res["frames"]),
		Baud:      int(num(res, "baud")),
		IdleNS:    uint64(num(res, "idle_ns")),
		Parity:    str(res, "parity"),
		Truncated: bol(res, "truncated"),
	}
	return nil, out, nil
}

// ---------------------------------------------------------------------------
// tolerant decoders (the daemon replies are JSON-decoded maps)

// laFramesDecode walks the daemon's frame array into the seat's rows.
func laFramesDecode(v any) []laFrame {
	rows := arr(v)
	out := make([]laFrame, 0, len(rows))
	for _, el := range rows {
		m, ok := el.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, laFrame{
			TNS:         uint64(num(m, "t_ns")),
			GapBeforeNS: uint64(num(m, "gap_before_ns")),
			NBytes:      num(m, "n_bytes"),
			Bytes:       str(m, "bytes"),
		})
	}
	return out
}

// laUARTFramesDecode adds the optional parity column on top.
func laUARTFramesDecode(v any) []laUARTFrame {
	rows := arr(v)
	out := make([]laUARTFrame, 0, len(rows))
	for _, el := range rows {
		m, ok := el.(map[string]any)
		if !ok {
			continue
		}
		f := laUARTFrame{
			TNS:         uint64(num(m, "t_ns")),
			GapBeforeNS: uint64(num(m, "gap_before_ns")),
			NBytes:      num(m, "n_bytes"),
			Bytes:       str(m, "bytes"),
		}
		if p, ok := m["parity_ok"].(bool); ok {
			f.ParityOK = &p
		}
		out = append(out, f)
	}
	return out
}

// resU64Field reads one unsigned field across the JSON float64 shape.
func resU64Field(m map[string]any, k string) (uint64, bool) {
	f, ok := m[k].(float64)
	if !ok || f < 0 {
		return 0, false
	}
	return uint64(f), true
}
