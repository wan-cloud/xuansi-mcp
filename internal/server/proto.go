// proto.go — the daemon↔agent internal wire contract (design doc §8:
// unix socket + 長度前綴 JSON，內部可讀優先). This file is the single
// source of truth for the message shapes; batch 6 (MCP) codes against
// this comment.
//
// # Framing
//
//	[u32 BE len][len bytes of UTF-8 JSON]
//
// len counts ONLY the JSON payload. Messages above MaxMsgBytes (1 MiB —
// the largest legitimate payload is an fs_write at the 16 KiB board
// file cap with ample margin) are a protocol violation: the writer
// refuses them and the reader drops the connection.
//
// # Message types (the "type" key discriminates)
//
// Request  {"type": <req type>, "id": <int>, ...params}
// Reply    {"type":"reply", "id", "result" | "error"}
// Event    {"type":"event", "board": <mac6>, "event": {"source","data"}}
// CaptureDone {"type":"capture_done", "capture_id", "path", "bytes",
// "reason":"limit"|"stopped"|"error"} (MB-level capture completion
// push; lands with T12 — documented here first)
//
// Every request gets exactly one reply carrying the same id. Events are
// pushed unsolicited (only after watch_events established the
// subscription for that connection) and carry no id.
//
// # Delivery policy (per agent connection)
//
// Each connection's outbound queue holds 256 messages — its replies and
// its event pushes. Events are fire-and-forget (fw §8 tap semantics):
// an event that meets a full queue is dropped (logged), so a slow
// consumer can never stall the board's read loop. Replies are never
// SILENTLY dropped: a reply that meets a full queue evicts the OLDEST
// QUEUED EVENT (logged) — a slow agent still receives every answer, at
// the price of its oldest unseen events. The one wedged shape — a
// client that pipelines so many requests the queue holds nothing but
// replies, then stops reading — is bounded, never blocking: the daemon
// waits up to 1s for the writer to drain (with the connection's queue
// lock released, so every other connection keeps being served), then
// evicts the OLDEST QUEUED REPLY — logged and counted. Nothing in the
// delivery path may stall the daemon.
//
// # Requests
//
//	list_boards    {}                          → {boards:[{mac6,alias,sources,host,port,usb_path,fw}],mdns_error}
//	board_status   {board}                      → {board:{...}, connected, lease:{holder,since,session,grace_deadline}?|null, scripts?, scripts_error?}
//	lease          {board, holder?, transport?} → {board, holder, session, transport?, warning?}
//	release        {board, holder?}             → {board, holder}
//	eval           {board, code, args?, holder?} → {out, value?}   (lease required)
//	submit         {board, code, tag, args?, holder?} → board ack {tag, queued}  (lease required)
//	script_start   {board, name, code, holder?}  → board ack       (lease required; runs under the LEASE's session namespace — the T10 eval ruling)
//	script_stop    {board, name, holder?}        → board ack       (lease required)
//	fs_write       {board, path, data, holder?}  → board ack       (lease required; data = base64 (JSON []byte); ≤16384B enforced daemon-side with class api, mirroring the board's own refusal)
//	fs_read        {board, path}                 → {data: base64}  (read-only, no lease)
//	watch_events   {board, source?}              → {board}         (subscribes THIS connection; source, when given, is additionally events.subscribe'd board-side)
//	op             {board, op, args?, holder?}   → board reply result  (the ops face — see below)
//	capture_start  {board, source, max_bytes?, max_seconds?, dest?, holder?} → {capture_id, path}  (lease required; see the capture section below)
//	capture_stop   {capture_id}                  → {path, bytes, reason:"stopped"}
//
// eval/submit/op args numbers: the fw args bridge is int-only (v3 §4),
// so the daemon validates the args tree BEFORE any board contact — an
// integral float64 (JSON's only integer shape) converts to int64 and
// msgpack-encodes as an int; a non-integral float, or one at or beyond
// ±2^53 — where the float64 decode may have already corrupted a JSON
// integer literal client-side, a sign-symmetric hazard, hence a
// sign-symmetric guard — is a daemon-side "api" error naming the
// offending path under the face's prefix ("eval args: a.b[2]:
// non-integer or |v|≥2^53 number", same shape for op as "op args: …").
// The largest integers that cross are ±(2^53-1).
//
// # The ops face (op passthrough)
//
// op forwards ONE allowlisted board op over the warm controller under
// eval's lease gate — uniformly: EVERY allowlisted op requires the
// lease, including the read-only diag/caps ops (the gate is the face's
// shape, not each op's). The allowlist (server.go's opAllowlist — the
// single source; every rendered listing is sorted from it):
//
//	budget.get, budget.set, caps, diag.heapinfo, diag.i2cslave,
//	diag.pins, diag.stats, log.read, wifi.off, wifi.setup, wifi.status
//
// diag.i2cslave toggles the board's INTERNAL I2C echo-slave loopback
// fixture ({mode:-1..1} = status/off/on) — the b7 driver-pipeline
// loopback's sparring partner; it crosses under the same uniform gate
// and args walk as every other op.
//
// budget.get is the one assembled view: the frozen wire has no getter
// op (budget.set refuses its no-keys form), so the daemon reads caps —
// whose limits block reflects the runtime budgets — and projects the
// {eval_ms, event_ms} pair; the board never sees a budget.get op.
//
// args cross through the SAME numeric walk as eval/submit (controller
// ruling): integral JSON numbers convert to int64 and msgpack-encode
// as ints (budget.set's counters would otherwise reach the fw as
// floats and be refused), while non-integral or ±2^53 numbers refuse
// daemon-side under the "op args:" prefix — every other value shape,
// strings included (wifi.setup's ssid/password), crosses verbatim (the
// board is the authority). The board's reply result — or its error
// class/msg, verbatim like eval's — is the reply. Anything else
// (eval/submit/fs.*/script.*/session.* and every other diag.*) is a
// daemon-side "server" class refusal naming the op: those faces keep
// their own request types with their own semantics.
//
// # Capture (the MB-level face — lands with T12, documented here first)
//
//	capture_start {board, source, max_bytes?=64MiB, max_seconds?=600, dest?}
//	              → {capture_id, path}            (lease required)
//	capture_stop  {capture_id} → {path, bytes, reason:"stopped"}
//	push          {"type":"capture_done", capture_id, path, bytes,
//	              reason:"limit"|"stopped"|"error"}
//
// A capture spools one watched source to a host file (the daemon is
// the writer, design §8): $XDG_DATA_HOME/xuansi/captures/<UTC
// ts>-<mac6>-<source>-cap<N>.bin|.jsonl — uart/stdout land byte-exact
// as raw streams, gpio/net as JSON lines ({"ts","source","data"} per
// line, ts fixed-width RFC3339 nanos). The default name embeds the
// capture sequence N (the capture_id's number): unique by
// construction, even for two same-second starts of the same
// board+source. An explicit dest NEVER truncates: files open
// O_CREAT|O_EXCL, and a path owned by a live capture or an existing
// file is a server-class refusal naming the collision. source is the
// subscribable set gpio.N / uart1.rx / uart2.rx /
// stdout / net; "job" is refused with an api-class error (§8: job
// results are direct-delivered to the submitting connection, not a
// subscribable source) and anything else is a server-class refusal.
// dest, when given, must be a bare basename (no separators) inside
// the captures root. The start subscribes the source board-side
// (events.subscribe, like watch_events); stopping does NOT
// unsubscribe — another watcher of the same source must not lose its
// events (daemon policy: subscriptions die with the connection only).
// Bounds clamp to 1..64MiB and 1..600s. A RAW chunk that would cross
// max_bytes is truncated so the file lands exactly on the cap; a JSON
// line that would cross is left UNWRITTEN (a truncated line would be
// corrupt JSON). The bounds end a capture (reason:"limit"),
// capture_stop ends it early (reason:"stopped", flushing the tail the
// tap already fed before the stop signal), a disk failure ends
// it as reason:"error"; daemon shutdown with live captures ends them
// reason:"stopped" (a shutdown is a deliberate stop, not a failure).
// Every ending pushes capture_done exactly once to the starting
// connection; capture_stop on an unknown/finished id is a server-class
// error (the push carries the reason that actually won any race).
//
// BOARD selectors are mac6 / alias / hostname prefixes resolved by the
// discovery registry (T9 Resolve): unique prefix hits, ambiguity or no
// match is a structured error listing candidates.
//
// # Transport (the lease request's deliberate USB route)
//
// lease's optional "transport" field selects the controller route:
//
//	"" (absent) | "tcp"  the warm TCP route — the default everywhere;
//	                       a plain lease never touches the board
//	"usb"                  the DELIBERATE USB fallback (T10 ruling:
//	                       WiFi 不可達時的後備). Opening the CDC port
//	                       REBOOTS the board — running scripts are
//	                       killed. The success reply carries
//	                       transport:"usb" plus a "warning" stating
//	                       exactly that, and the daemon log records the
//	                       same line.
//
// USB is reached ONLY through this explicit field (CLI: `xuansi lease
// BOARD --usb`). There is NEVER a silent fallback: a failed TCP dial
// reports unreachable and does NOT auto-try USB — the caller decides
// whether a reboot is acceptable. The held/refusal checks run BEFORE
// the USB open (a lease held by someone else must not reboot the
// board); a lease whose USB open then fails stays held host-side
// (same as a TCP lease on an unreachable board — release or retry).
// Warm-controller reuse is transport-agnostic: whichever controller
// is live serves every subsequent request — a USB lease's warm link
// carries default-transport ops up to release, with no implicit
// dialing of the other seam.
// An unknown transport value is a "server" class error. Releasing a
// USB-transport lease (and daemon shutdown on a live USB controller)
// voluntarily YIELDS the controller slot first (§12 讓位 — observer
// hello, then close): the board cannot see a CDC close, so this is
// what lets the next TCP lease take controller without any reboot
// ritual.
//
// # Holder semantics (the lease identity — read this twice)
//
// The lease holder is a client-supplied string carried on requests
// ("holder"). A connection's holder binds at its first request that
// carries one; afterwards a different holder on the same connection is
// a "holder" class error. Requests without "holder" act as the
// connection's bound holder; a connection that never supplied one uses
// a generated per-connection id.
//
// A socket disconnect is the agent-gone signal: every live lease of
// that holder enters the grace TTL (board state KEPT). Re-entering
// within the TTL — from ANY connection — re-supplies the same holder
// and the lease is adopted back live without touching the board
// (scripts keep running, pins stay claimed). This is how the CLI
// (xuansi lease --holder H … eval --holder H) and batch 6's MCP agents
// survive across invocations/processes.
//
// # Error taxonomy ({"error":{"class","msg"}})
//
// Board op errors preserve the fw class and msg VERBATIM (the six bench
// classes oom/compile/rt/api/budget/timeout, plus the frame-layer
// class protocol, including ` at byte N` suffixes). Daemon-side
// classes: held (fields holder/since/grace_expiry present), not_held,
// teardown (retry shortly), no_board, ambiguous, holder (connection
// bound to a different holder), unreachable (dial/hello failed),
// slot_held (controller arbitration window exhausted — the deliberate
// USB reboot lease is the caller's explicit last resort), api
// (daemon-side refusals that deliberately mirror the board's own
// class: fs_write above the 16 KiB single-file cap, and eval/submit
// args numbers that cannot cross the int-only args bridge — see the
// args note above), server (malformed request, unknown type, bad
// params, bad transport, off-allowlist op).
package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxMsgBytes caps one framed message (see the framing contract above).
const MaxMsgBytes = 1 << 20

// Request is one client request. Only the fields the documented request
// types read are typed; unknown keys are ignored.
type Request struct {
	Type   string `json:"type"`
	ID     int64  `json:"id"`
	Board  string `json:"board,omitempty"`
	Holder string `json:"holder,omitempty"`
	// Transport selects the lease route: "" (default) = warm TCP,
	// "usb" = the deliberate fallback whose open reboots the board.
	// Lease only; see the transport contract above. Unknown values
	// are a "server" class error.
	Transport string `json:"transport,omitempty"`
	Code      string `json:"code,omitempty"`
	Tag       string `json:"tag,omitempty"`
	Name      string `json:"name,omitempty"`
	Path      string `json:"path,omitempty"`
	Data      []byte `json:"data,omitempty"` // JSON base64 ↔ board bin
	Source    string `json:"source,omitempty"`
	// Op is the op passthrough request's board op — allowlisted (see
	// the ops-face note in the contract above); eval/submit/fs/script/
	// session ops are NOT reachable through it.
	Op   string `json:"op,omitempty"`
	Args any    `json:"args,omitempty"` // eval/submit: array|map → Lua ARGS; op: the op's args object — numbers validated daemon-side either way
	// CaptureID addresses one capture (capture_stop's target).
	CaptureID string `json:"capture_id,omitempty"`
	// MaxBytes / MaxSeconds bound one capture (0 = the 64MiB / 600s
	// defaults; clamped 1..64MiB and 1..600 daemon-side).
	MaxBytes   int64 `json:"max_bytes,omitempty"`
	MaxSeconds int64 `json:"max_seconds,omitempty"`
	// Dest overrides a capture's filename (a bare basename inside the
	// captures root — path separators are refused daemon-side).
	Dest string `json:"dest,omitempty"`
}

// RPCError is the structured error half of a reply.
type RPCError struct {
	Class string `json:"class"`
	Msg   string `json:"msg"`
	// Held-detail fields (class "held" only): who holds it, since when,
	// and the grace deadline when the holder is gone.
	Holder      string `json:"holder,omitempty"`
	Since       string `json:"since,omitempty"`
	GraceExpiry string `json:"grace_expiry,omitempty"`
}

// Error renders like a board error line: "<class>: <msg>".
func (e *RPCError) Error() string { return e.Class + ": " + e.Msg }

// Reply is the response half of a request: exactly one per request id,
// carrying result XOR error.
type Reply struct {
	Type   string    `json:"type"` // "reply"
	ID     int64     `json:"id"`
	Result any       `json:"result,omitempty"`
	Error  *RPCError `json:"error,omitempty"`
}

// WireEvent is the §8 event payload: source (gpio.N / uartN.rx /
// stdout / net / job) and its data map.
type WireEvent struct {
	Source string         `json:"source"`
	Data   map[string]any `json:"data,omitempty"`
}

// EventPush is the unsolicited board event: {"type":"event", board, event}.
type EventPush struct {
	Type  string    `json:"type"` // "event"
	Board string    `json:"board"`
	Event WireEvent `json:"event"`
}

// CaptureDone is the unsolicited capture-completion push (T12):
// {"type":"capture_done", capture_id, path, bytes, reason}. It rides
// the starting connection's outbound queue with REPLY priority (never
// dropped for a full queue the way event pushes are — it is the only
// answer a blocking capture waiter gets).
type CaptureDone struct {
	Type      string `json:"type"` // "capture_done"
	CaptureID string `json:"capture_id"`
	Path      string `json:"path"`
	Bytes     int64  `json:"bytes"`
	Reason    string `json:"reason"` // "limit" | "stopped" | "error"
}

// WriteMsg frames and writes one JSON message.
func WriteMsg(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("server: encode message: %w", err)
	}
	if len(payload) > MaxMsgBytes {
		return fmt.Errorf("server: message %d bytes exceeds the %d limit", len(payload), MaxMsgBytes)
	}
	hdr := make([]byte, 4, 4+len(payload))
	binary.BigEndian.PutUint32(hdr, uint32(len(payload)))
	if _, err := w.Write(append(hdr, payload...)); err != nil {
		return fmt.Errorf("server: write message: %w", err)
	}
	return nil
}

// ReadFrame reads one length-prefixed payload (bytes, undecoded). A
// declared length above MaxMsgBytes is fatal for the connection (the
// caller must treat the error as unrecoverable); a payload that decodes
// to garbage is the caller's to classify.
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("server: read header: %w", err)
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxMsgBytes {
		return nil, fmt.Errorf("server: frame length %d exceeds the %d limit", n, MaxMsgBytes)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("server: read %d-byte payload: %w", n, err)
	}
	return payload, nil
}

// ReadMsg reads one framed message and decodes it into v.
func ReadMsg(r io.Reader, v any) error {
	payload, err := ReadFrame(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("server: decode %d-byte payload: %w", len(payload), err)
	}
	return nil
}
