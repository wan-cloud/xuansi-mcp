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
	"errors"
	"fmt"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/apiclient"
	"xuansi/internal/la"
	"xuansi/internal/server"
)

// laAntiConfusion is the frozen one-liner every la_* Desc carries (or
// points at): the distinction from xuansi_capture — event-source
// recording vs LA capture object. State it, prevent the mix.
const laAntiConfusion = "This is the LA capture-OBJECT face — NOT xuansi_capture (that tool records one board EVENT SOURCE to a spooled file for post-processing; its rows carry at most per-event timestamps — no waveform timing, no framing, no analysis)"

type laCaptureIn struct {
	Board string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Mode  string `json:"mode,omitempty" jsonschema:"capture mode: \"listen\" (default — passive UART listen on one pin, RX-only by construction), \"edge\" (the RMT run-length engine: 1..4 pins, no port/baud — the line's every transition as {level, dur_ns} runs, 12.5ns resolution; two views of one signal compare with xuansi_la_diff), or \"samples\" (the LCD_CAM 8-channel sampler: the fixed DVP bus D0..D7 = GPIO{11,9,8,10,12,18,17,16}, one byte per tick — rate_hz picks the notch; decode with xuansi_la_decode_bus)"`
	// Port/Baud/Pin arm the LISTEN engine (mode "listen");
	// Pins/Trig arm the EDGE engine (mode "edge");
	// RateHz/Trig arm the SAMPLES engine (mode "samples").
	Port int `json:"port,omitempty" jsonschema:"LISTEN only: the hw UART the listen engine rides, 1 (uart1) or 2 (uart2)"`
	Pin  int `json:"pin,omitempty" jsonschema:"LISTEN only: the GPIO the line under study is wired to — explicit, always: the ports' DEFAULT pins are board-specific traps this face never guesses, and the fw's refusal lists the banned pins"`
	Baud any `json:"baud,omitempty" jsonschema:"LISTEN only: the line's baud: an integer 1200..5000000, or \"auto\" — the board measures it (RMT bit-width) and the reply's meta.baud carries the resolved value"`
	// Pins arms the edge engine.
	Pins []int `json:"pins,omitempty" jsonschema:"EDGE only (mode \"edge\"): 1..4 GPIOs whose lines' transitions record as run-length {mask, dur_ns} — mask bit i = pins[i]'s level; the RMT group runs at the pinned 80MHz/12.5ns"`
	// Trig is the edge/sample arm's optional trigger (the frozen
	// small language — a line-level match, no protocol semantics).
	Trig *laTrigIn `json:"trig,omitempty" jsonschema:"EDGE/SAMPLES only: optional trigger. EDGE: {pin (a pins[] member), kind \"edge_r\"|\"edge_f\"|\"level\"|\"module\", level 0|1 (kind \"level\"), module \"<name>\" (kind \"module\" — a LOADED ABI 2 trigger module, bench.wasm.load first; no pin: the whole run stream is the scan window), pre_kb < ring_kb} — the ring runs CIRCULAR (keeping pre_kb of history) until the match, then fills linearly (reason \"trig\"; a module error ends reason \"err\"). SAMPLES: {ch 0..7 (= D0..D7), kind \"bits\" {mask, value, width?}, or kind \"module\" (the RLE run window; refused above 2MHz), pre_kb} — same circular→linear shape over the sample stream; without one it fills linearly from arm (reason \"full\")"`
	// RingKB sizes the on-board ring; REQUIRED 64..4096 in every mode
	// — no fw default exists on any arm (LA final review C1).
	RingKB int `json:"ring_kb,omitempty" jsonschema:"on-board ring buffer KB. REQUIRED in every mode, 64..4096 — no fw default exists (LISTEN included; the ring is the capture's whole history bound). Admission: active rings + this request must fit la_ring_budget_kb=4096 (read caps) — the board's refusal itemizes the account. The capture ends reason \"full\" (no trig) when the ring fills"`
	// RateHz arms the SAMPLES engine.
	RateHz int `json:"rate_hz,omitempty" jsonschema:"SAMPLES only (mode \"samples\"): the sample rate — one of the six notches {1000000, 2000000, 5000000, 10000000, 20000000, 40000000} (exact 160MHz integer divisions, no other rate exists; ≤20MHz is the lossless band — 40MHz carries a documented ~0.1-0.2% tear cost)"`
	// Seconds is the time bound → the fw's max_ms (the arm crosses
	// ×1000, clamped at 600 like capture's own seconds rule).
	Seconds int64  `json:"seconds,omitempty" jsonschema:"time bound in seconds (1..600 — the fw's max_ms cap is 600000ms): the capture ends reason \"limit\" when it passes; omit for the fw default. The call BLOCKS until the capture ends AND the verified pull-back lands"`
	Holder  string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
	// Stream (LA wave C) flips the pull into the live sliding window.
	Stream bool `json:"stream,omitempty" jsonschema:"LISTEN/EDGE only: stream the capture off the board WHILE it captures — the pull loop reads 32640B windows at the measured p90 budget (~200ms cadence; effective 0.215-0.322MB/s) as the ring fills, and the ring RECYCLES behind the read cursor: ring_kb is the slack pool, a reader a full ring behind ends the capture reason full with the ring closing the account (no silent loss), and the landed object is the whole stream with the tail verified. REFUSED for samples (the 1-40MB/s stream outpaces any link — waveS verdict; capture stopped and pull post-stop) and for edge+trig (the pre-trigger ring is circular; drop trig to stream)"`
}

// laTrigIn is the edge/sample arm's trigger crossing.
type laTrigIn struct {
	Pin   int    `json:"pin,omitempty" jsonschema:"EDGE: the trigger pin — must be one of the armed pins[]"`
	Ch    int    `json:"ch,omitempty" jsonschema:"SAMPLES (kinds edge_r/edge_f/level): the trigger channel 0..7 (= D0..D7, the sample byte's bit)"`
	Kind  string `json:"kind" jsonschema:"\"edge_r\" (0→1) | \"edge_f\" (1→0) | \"level\" (the line sits at level — a level already true at arm fires immediately, pre-window empty) | \"bits\" (SAMPLES only: the bit-window match sample&mask==value&mask for width consecutive samples) | \"module\" (wave D: a LOADED ABI 2 wasm trigger module scans the run window and answers fire — no pin/ch; the mirror carries module and, once fired, fire_ns)"`
	Level *int   `json:"level,omitempty" jsonschema:"0|1, required for kind \"level\" (its PRESENCE is checked — an omitted level refuses)"`
	Mask  int    `json:"mask,omitempty" jsonschema:"SAMPLES kind \"bits\": the window mask 0..255 (u8 bitmask, nonzero — a zero mask matches everything and refuses)"`
	Value int    `json:"value,omitempty" jsonschema:"SAMPLES kind \"bits\": the window value 0..255 (u8 bitmask — the window match is sample&mask==value&mask)"`
	Width int    `json:"width,omitempty" jsonschema:"SAMPLES kind \"bits\": consecutive matching samples 1..64 (0/omitted = 1)"`
	PreKB int    `json:"pre_kb,omitempty" jsonschema:"pre-trigger history kept (KB, < ring_kb; 0 = no history — the capture starts at the trigger)"`
	// Module rides kind "module" only (LA wave D).
	Module string `json:"module,omitempty" jsonschema:"kind \"module\" only: the loaded ABI 2 trigger module's name (bench.wasm.load first — the board's refusal names the load state); on fire the meta trig carries module + fire_ns (the wasm decision instant, paired with stopped_ns)"`
}

type laCaptureOut struct {
	CaptureID string         `json:"capture_id" jsonschema:"the landed object's id — the key every other la_* tool takes"`
	Path      string         `json:"path" jsonschema:"absolute data.bin path on the daemon's host (the frozen chunk stream; post-process with programs when the bounded analysis views are not enough)"`
	Meta      map[string]any `json:"meta" jsonschema:"the object's full meta ledger: baud (the RESOLVED value — what \"auto\" measured), bytes/chunks, t_start_ns/t_end_ns, reason (full|stopped|limit|trig|err — the fw's ending vocabulary verbatim), sha256 (the verified identity), the fw's arm account (ring_kb, max_ms, chunks_dropped, bytes_dropped, hw_ovf, armed_ns, stopped_ns, fire_ns on kind-module captures), zone_sat:true + elapsed_ms on a samples capture whose RLE record zone saturated before max_ms (high-entropy input shortened the window — lower rate_hz / raise ring_kb / shorten the wait), streamed:true on a live-pulled object (the [0, tail) prefix crossed live — tail re-read verified). incomplete ABSENT = the lossless promise HELD; incomplete:true = forensics only"`
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
		Description: "Arm one board's LA capture ENGINE (protocol v9) and BLOCK until the capture ends AND the on-board ring is pulled back VERIFIED (two-pass sha256 identity) — the answer is a capture OBJECT the la_* tools analyze. " + laAntiConfusion + ": xuansi_capture records an event source to a file; THIS arms real capture hardware. " +
			"THREE MODES. mode \"listen\" (default): the hardware UART as a passive protocol listener (RX-only by construction — no TX path exists, fw-frozen) that timestamps every byte — connect the line to pin, port selects the hw UART (1/2), baud the integer rate 1200..5000000 or \"auto\" (measured; meta.baud carries it); analyze with xuansi_la_frames/repeat/decode_uart. mode \"edge\": the RMT run-length engine — 1..4 pins (NO port/baud) record every line transition as {mask, dur_ns} runs at the pinned 80MHz/12.5ns (mask bit i = pins[i]'s level); ring_kb is REQUIRED (64..4096 — the ring is the whole history bound; full = ring filled, limit = the seconds bound, trig = the optional trigger fired, stopped = ended early, err = a kind-module trigger's wasm failure — the honest ending vocabulary crosses verbatim); optional trig {pin ∈ pins, kind edge_r|edge_f|level|module, level, module (a LOADED ABI 2 trigger module — bench.wasm.load first; the run stream is the scan window), pre_kb} arms a pre-trigger circular window; analyze with xuansi_la_edges/runs/hist/measure/diff/decode_wire — two captures of one signal (two probes, two views) compare with xuansi_la_diff (same-board only). mode \"samples\": the LCD_CAM 8-channel sampler — the FIXED DVP bus D0..D7 = GPIO{11,9,8,10,12,18,17,16} (bit i of every sample byte = D_i's level; no pin choice exists), rate_hz one of the six notches {1,2,5,10,20,40}MHz (≤20MHz is the lossless band — 40MHz carries a documented ~0.1-0.2% tear cost), ring_kb REQUIRED; the µs-anchored t0_ns + rate_hz derive every sample's absolute time; CAPACITY SEMANTICS (≤20MHz = the lossless RLE lane): the true window is record-zone capacity ÷ the input's entropy rate, NOT max_ms — a background entropy source (e.g. a ~135kHz clock = ~0.7B/sample at 2MHz) fills a 512KB-1MB ring in 0.25-0.5s and reason \"full\" closes long before the time bound; meta zone_sat:true + elapsed_ms (the E-window pair, present iff the zone saturated first) make the shortened window face-visible — the coach when it bites: lower rate_hz, raise ring_kb, or shorten the wait (traffic armed at +0.15..0.4s falls OUTSIDE a window nobody accounted for — F-REV1's false \"dead lane\" verdicts were exactly this); optional trig {ch 0..7 | kind \"bits\" {mask,value,width?} | kind \"module\" (the RLE run stream is the scan window; the fw refuses above 2MHz — window traffic vs the AOT scan lane), pre_kb} rides the same circular→linear discipline — NOTE trigger lanes: edge_r/edge_f/level ride a hardware GPIO interrupt (all six rates); kind \"bits\" rides the firmware software scan lane (measured ~3-5MB/s) and the fw REFUSES bits arms above 2000000 Hz with a coach pointing at the edge/level hardware lane (a bits window above scan throughput would be missed silently — do not retry the same shape at a high rate); decode with xuansi_la_decode_bus (i2c/spi). " +
			"FIELD MATRIX (a foreign field REFUSES with the omit-coach — never a silent ignore, so a dropped triggering intent cannot mis-capture): listen = {port, pin, baud}; edge = {pins[, trig]}; samples = {rate_hz[, trig]}; shared = {ring_kb, seconds, stream}. pins/rate_hz/trig on listen, port/baud/rate_hz on edge, port/baud/pins on samples — each refusal names the field and the mode it belongs to (listen has NO trigger: a triggered capture is mode \"edge\" with trig). " +
			"STREAMING (stream:true, wave C — LISTEN/EDGE only): pull the capture LIVE off the board while it captures — the loop reads 32640B windows at the measured p90 budget (~200ms; effective 0.215-0.322MB/s) and the ring RECYCLES behind the read cursor: ring_kb becomes the SLACK POOL (a reader a full ring behind ends the capture reason full with the ring closing the account — no silent loss), the assembled object is the WHOLE stream (live prefix + ring tail, tail re-read verified, meta.streamed marks the prefix that has no ring-side second source). stream+samples refuses (the 1-40MB/s stream outpaces any link — waveS verdict); stream+edge refuses while trig is armed (the pre-trigger ring is circular — drop trig to stream). Use it for captures longer than the ring: at 115200 baud the stream keeps up 20×, at 921600 2.4-3.3×; samples stay post-stop by design. " +
			"BLOCKING: the call holds until the pull-back is verified — budget the capture's own bound plus read-back time. The block rides a DEDICATED daemon lane: every other xuansi tool keeps answering while it holds (drive a stimulus board on another lease mid-capture — that is the workflow). The lane is ONE per process: concurrent xuansi_la_capture calls SERIALIZE on it (the second queues behind the first's whole arm→pull) — run captures you need truly concurrent from separate processes/holders, or arm them sequentially. " +
			"THE VERDICT IS IN THE REPLY: meta.incomplete ABSENT = the lossless promise HELD (sha256 verified); an ERROR naming the capture id = the pull broke (cancel, link death, checksum) and the incomplete object landed as FORENSICS — readable via xuansi_la_info/xuansi_la_edges, never a trustworthy capture. " +
			"Mutual exclusion (listen): the same hw UART cannot run bench uart primitives and the listener at once (the board refuses, naming the conflict — both directions). Mutual exclusion (edge): the RMT group is exclusive against bench.rmt in both directions. Mutual exclusion (samples): the DVP/LCD_CAM engine is exclusive against the CAMERA in both directions (arm refuses \"camera active; camera.off first\"), and the 9 DVP pins are claimed whole — a script holding ANY of them (e.g. bench.i2c on GPIO8/9) refuses the arm. " +
			"FIRST LOOK after a listen capture: xuansi_la_frames (the idle framing — the protocol's sentence structure), xuansi_la_repeat when the traffic looks periodic (poll loops, heartbeats), xuansi_la_decode_uart for a per-frame parity verdict; after an edge capture: xuansi_la_edges (the transition view) then xuansi_la_hist (the timing units); after a samples capture: xuansi_la_decode_bus (the i2c/spi transaction list). Holder semantics: " + holderDesc + ".",
	}, ts.laCapture)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_list",
		Description: "List the daemon's landed LA capture objects, newest first — every object the la_* analysis tools can read (the daemon-local store, every board's captures; no lease, no board contact — the board can be gone). " + laAntiConfusion + " (that face's spool files are not objects here). " +
			"Optional board narrows the rows to one board's captures: the row's board column (the puller-recorded mac6, full or prefix — matched over the STORE, not the live registry, so a gone board's captures still list; omit for every board's captures). " +
			"Each row is the object's full meta: identity (capture_id, board, mode, port/pin), the resolved baud, the payload account (bytes, chunks, t_start_ns/t_end_ns), how the capture ended (reason full|stopped|limit|trig|err — the fw's ending vocabulary verbatim; err = a kind-module wasm failure), the fw's arm ledger mirrored at pull time (ring_kb, max_ms, chunks_dropped, bytes_dropped, hw_ovf, armed_ns, stopped_ns — the honest loss columns; samples rows add zone_sat/elapsed_ms when the RLE record zone saturated before max_ms — high-entropy input shortened the window), sha256 (the verified identity), streamed:true on a live-pulled object, and incomplete:true on a pull that broke (forensics only). " +
			"Retention is FIFO over a 512MB quota — oldest objects delete first, the newest never alone. NEXT: pick a capture_id, read it with xuansi_la_info, then xuansi_la_frames for the framing view.",
	}, ts.laList)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name: "xuansi_la_info",
		Description: "One LA capture object's full meta ledger — the same columns xuansi_la_list rows carry, read one object deep. Read the ledger BEFORE analyzing: bytes/chunks = the traffic volume, t_start_ns/t_end_ns = the window, reason = how it ended (full = the ring filled at its byte bound, limit = the time bound passed, stopped = ended early, trig = the trigger fired, err = a kind-module wasm failure), baud = the rate the timing derives from. " +
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

// laClient answers the LA face's dedicated long-call lane, dialing it
// lazily (most processes never capture; the lane exists exactly for
// the blocking pull). A dead lane is re-dialed on the next call — the
// daemon's per-connection holder binding rides each request anyway.
func (ts *toolset) laClient() (*apiclient.Client, error) {
	ts.laMu.Lock()
	defer ts.laMu.Unlock()
	if ts.laCli != nil {
		select {
		case <-ts.laCli.Done():
			ts.laCli.Close() // the dead lane's teardown is idempotent
		default:
			return ts.laCli, nil // still live: reuse the lane
		}
	}
	c, err := apiclient.Dial(ts.c.Path())
	if err != nil {
		return nil, fmt.Errorf("la capture lane: %w", err)
	}
	ts.laCli = c
	return c, nil
}

// closeLA tears the LA lane down (the test env's cleanup seam;
// production exits with the process).
func (ts *toolset) closeLA() {
	ts.laMu.Lock()
	defer ts.laMu.Unlock()
	if ts.laCli != nil {
		ts.laCli.Close()
		ts.laCli = nil
	}
}

// callLALane crosses one la_* request on the dedicated lane with the
// lane's disposable-identity rule (T7 fix round 1): a class-"holder"
// refusal means the LANE's daemon-side binding disagrees with the
// holder this process stamped — a mid-session grace-adopt with an
// explicit holder bound the lane to the adopter, while the holder
// mirror is first-wins and never follows, so every later holder-less
// capture would be refused on that lane FOREVER. The lane is
// disposable (the shared connection is not): drop it, redial fresh
// (the fresh binding takes whatever the mirror now stamps), retry the
// crossing ONCE. A second refusal is the honest error. Concurrent
// captures on the dropped lane surface as a lost-connection error and
// self-heal on their next call — noted, accepted (two concurrent
// blocking captures adopting different holders is the edge of the
// edge).
func (ts *toolset) callLALane(ctx context.Context, req server.Request) (map[string]any, error) {
	c, err := ts.laClient()
	if err != nil {
		return nil, err
	}
	res, err := ts.callHeldOn(ctx, c, req)
	var rerr *server.RPCError
	if !errors.As(err, &rerr) || rerr.Class != "holder" {
		return res, err // not the binding wedge: cross as-is
	}
	ts.closeLA() // the mis-bound lane goes; the shared conn keeps its identity
	c, err = ts.laClient()
	if err != nil {
		return nil, err
	}
	return ts.callHeldOn(ctx, c, req)
}

func (ts *toolset) laCapture(ctx context.Context, _ *mcpsdk.CallToolRequest, in laCaptureIn) (*mcpsdk.CallToolResult, laCaptureOut, error) {
	// the dedicated lane: this call blocks up to seconds + pull time —
	// the shared connection keeps serving every other tool meanwhile
	// (drive a stimulus board on it mid-capture; that is the workflow)
	req := server.Request{
		Type: "la_capture", Board: in.Board, Holder: in.Holder,
		Mode: in.Mode, LaPort: in.Port, LaPin: in.Pin, Baud: in.Baud,
		RingKB: in.RingKB, MaxSeconds: in.Seconds, RateHz: in.RateHz,
		Stream: in.Stream,
	}
	if len(in.Pins) > 0 {
		req.Pins = in.Pins
	}
	if in.Trig != nil {
		// the u8 pair's domain gate (LA 二輪審查 2026-11-16 I-1): the
		// ints cross fine into this seat, but the TrigMeta crossing is
		// uint8 — an out-of-domain value would wrap SILENTLY (300→44,
		// 256→0) and arm a different window than the caller asked for.
		// Refused HERE, zero daemon contact.
		if in.Trig.Mask < 0 || in.Trig.Mask > 255 || in.Trig.Value < 0 || in.Trig.Value > 255 {
			return nil, laCaptureOut{}, &server.RPCError{
				Class: "api",
				Msg: fmt.Sprintf("la_capture: trig mask/value are u8 bitmasks over the sample byte — mask %d, value %d out of domain 0..255 (the u8 crossing would wrap them silently: 300→44, 256→0), refused client-side",
					in.Trig.Mask, in.Trig.Value),
			}
		}
		req.Trig = &la.TrigMeta{Pin: in.Trig.Pin, Ch: in.Trig.Ch, Kind: in.Trig.Kind,
			Level: in.Trig.Level, PreKB: in.Trig.PreKB,
			Mask: uint8(in.Trig.Mask), Value: uint8(in.Trig.Value), Width: in.Trig.Width,
			Module: in.Trig.Module}
	}
	res, err := ts.callLALane(ctx, req)
	if err != nil {
		return nil, laCaptureOut{}, err
	}
	return nil, laCaptureOut{
		CaptureID: str(res, "capture_id"),
		Path:      str(res, "path"),
		Meta:      obj(res["meta"]),
	}, nil
}

// laListIn is la_list's optional row filter (LA 二輪審查 2026-11-16
// I-2: the documented {board?} crossed nowhere — the tool took a zero
// input and the daemon ignored the field. The filter lands on both
// faces now).
type laListIn struct {
	Board string `json:"board,omitempty" jsonschema:"optional row filter: keep only the captures whose meta board column (the puller-recorded mac6) matches — full mac6 or a prefix, matched straight over the store (NOT the live registry's selector resolution: a board the daemon no longer discovers still lists its captures, and an alias matches nothing — read the mac6 off a row). Omit for every board's captures"`
}

func (ts *toolset) laList(ctx context.Context, _ *mcpsdk.CallToolRequest, in laListIn) (*mcpsdk.CallToolResult, any, error) {
	res, err := ts.call(ctx, server.Request{Type: "la_list", Board: in.Board})
	if err != nil {
		return nil, nil, err
	}
	// the daemon's {captures:[…]} view crosses VERBATIM (the meta shape
	// is frozen at capture — the seat does not re-shape ground truth).
	return nil, res, nil
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
