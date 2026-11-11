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
// the largest legitimate payload is a chunked fs_write: ~786KB of file
// bytes base64-encoded with ample margin; the board itself carries the
// file 64 KiB at a time over the chunked transport) are a protocol
// violation: the writer refuses them and the reader drops the
// connection.
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
//	list_boards    {}                          → {boards:[{mac6,alias,sources,host,ip,port,usb_path,fw,fw_version,boot_epoch?,reset_reason?,chip?,last_seen?,transport,last_seen_age_ms,stale}],mdns_error}
//	board_status   {board}                      → {board:{...}, connected, transport, last_seen_age_ms, stale, lease:{holder,since,session,grace_deadline}?|null, scripts?, scripts_error?}
//	lease          {board, holder?, transport?} → {board, holder, session, transport:"tcp"|"usb", rebooted:bool, transport_note?}
//	release        {board, holder?}             → {board, holder}
//	eval           {board, code, args?, holder?} → {out, value?}   (lease required)
//	submit         {board, code, tag, args?, holder?} → board ack {tag, queued}  (lease required)
//	script_start   {board, name, code, holder?}  → board ack       (lease required; runs under the LEASE's session namespace — the T10 eval ruling)
//	script_stop    {board, name, holder?}        → board ack       (lease required)
//	fs_write       {board, path, data, holder?, sha256?} → {size}     (lease required; data = base64 (JSON []byte). Chunks are 64KB since B15 §4.5: <=65472B with no sha256 key is one verbatim fs.write; everything else rides the chunked transport — fs.write_part per 65472B chunk + one fs.write_commit carrying the payload's sha256 — transparent here: same request, same {size} reply. A PRESENT sha256 key (64 hex) switches the write to the verified path for ANY size: even a small payload rides write_part + commit and the board's own commit-sha verdict is the verification (the daemon refuses a malformed sha with zero board contact). B10-T1 + B15 §4.5②)
//	fs_read        {board, path}                 → {data: base64}  (read-only, no lease; assembled from fs.read_part windows — files beyond the fw's 64KB chunk come back complete; reply shape unchanged. B10-T1; the independent read tool — the push path no longer reads back, B15 §4.5②)
//	fs_rm          {board, path, holder?}        → {}              (lease required; the removal path — the board's own fs.rm {path} verbatim, same gate as fs_write; a missing file is the board's api not-found, class/msg unmodified)
//	session_reset  {board, session?, holder?}    → {stopped:n}     (lease required; session omitted = 0. The board's own session.reset {session} verbatim under eval's gate — releases that session's scripts AND pins. ns-0 is where bare evals claim pins (owner _eval, shared across evals, §2/§3.2): the b7 S4b HIL could only sweep it by the release-lease/re-lease detour because session.* is deliberately outside the ops allowlist (batch-6 ruling) — THIS request type is the dedicated path instead. {stopped:n} counts scripts stopped (a pins-only ns-0 sweep answers 0 while still releasing the pins); board error class/msg crosses unmodified)
//	job_wait       {board, tag, timeout_ms}      → {done:bool, value?, error?, elapsed_ms}  (read-only, NO lease, never dials the board — B15 §4.3: the daemon parks on its OWN event tap for the job's completion event. Tracks jobs SUBMITTED THROUGH THIS daemon (the board direct-delivers job events to the submitting connection — a foreign connection's jobs are invisible, honestly): a tag never submitted and never seen is an api refusal; done:true carries the job's bridged value, or error {class, msg, hint — the cleanup ritual} when the job died; timeout_ms 1..55000 bounds the wait (under the 60s request ceiling) and a timeout answers {done:false, elapsed_ms} — a RESULT, never an error)
//	watch_events   {board, source?}              → {board}         (subscribes THIS connection; source, when given, is additionally events.subscribe'd board-side)
//	op             {board, op, args?, holder?}   → board reply result  (the ops face — see below)
//	capture_start  {board, source, max_bytes?, max_seconds?, dest?, holder?} → {capture_id, path}  (lease required; see the capture section below)
//	capture_stop   {capture_id}                  → {path, bytes, reason:"stopped"}
//	boards_pin     {mac6, ip, port}              → {pinned, endpoint}  (⑨: declares the registry's pin source — see the pin section below)
//	boards_unpin   {mac6}                        → {unpinned}       (removes exactly the pin declaration)
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
// # board.reboot (the synthesized boot-visibility event, B10-T8b/T8c;
// reasons[] with B12-P2 T6)
//
// The daemon sights each board's boot identity at every tapped hello
// (boot_epoch, the fw's NVS per-boot counter). A CHANGED epoch fires
// one board event — source "board", data {kind:"board.reboot", board,
// prev_epoch, new_epoch, reset_reason, gap_ms, [fw_build pair],
// [fw_version pair], [reasons], [reasons_partial]} — fanned out to
// watchers like any board event. A same-epoch redial is not a reboot
// (§13); epoch 0 (pre-b9 fw without the key) is "unknown" and never
// fires on its own (0→N does — a reflash reboots). The baseline is
// in-memory: a daemon restart re-baselines silently.
//
// reasons (protocol v6 fw) recovers the boot-causes an epoch delta > 1
// would otherwise lose: the fw's NVS last-8 reset-reason ring (hello
// reset_history, oldest→newest, its final entry this boot's reason)
// contributes its tail delta entries — so reasons[last] ==
// reset_reason, always. delta ≤ 1: the key is absent (nothing was
// lost). delta > 8 (or a ring shorter than delta): the 8 (or fewer)
// kept entries ride and reasons_partial:true flags the cut. Pre-v6 fw
// (no ring): the key is absent, honestly.
//
// # board.transport (the synthesized route-switch event, B15 §4.4)
//
// The daemon fires one board event on every switch of a daemon-held
// controller link — source "board", data {kind:"transport", from, to,
// rebooted} — fanned out like any board event. from is the route in
// hand before the switch ("" when no live controller), to the route
// after ("tcp"/"usb"); rebooted is true only when the switch OPENED
// the CDC port (§2: one connection = one device session — the USB
// open IS a reboot). The switch sites:
//
//   - any lease taking the USB route with a fresh open (auto-fallback
//     or explicit; from "tcp" or "" → "usb", rebooted:true);
//   - the USB→TCP upgrade — a default lease finding a warm USB
//     controller with TCP live again ("usb" → "tcp", rebooted:false —
//     §13 redial ≠ reboot), OR the endpoint-drift seam rebuild closing
//     a live USB route whose fresh resolution is TCP-capable
//     (supervisor()'s rebuild branch — the same upgrade completing
//     outside any lease; TCP revived WITH an endpoint change).
//
// A same-route re-lease, a release/shutdown yield (the route drops to
// none, not to another route), and mere redials never fire.
//
// # The pin face (⑨, B15 §4.9)
//
// boards_pin {mac6, ip, port} declares the registry's third source:
// an explicit endpoint for a board this host's discovery cannot route
// (cross-subnet mDNS blindness, multicast-blocked APs). The pin takes
// effect IMMEDIATELY (list_boards shows the entry with source "pin"
// right away; a still-undiscovered board appears as a pin-only entry)
// and survives every discovery refresh — the pinned endpoint also
// OVERRIDES the mdns face's address while pinned (the pin exists
// because discovery gets this board's route wrong for this host).
// Dial priority is usb > pin > mdns. Runtime pins are in-memory
// daemon state (they do not survive a daemon restart); the durable
// form is boards.conf's `name=mac6@ip:port` line, read at daemon
// start. boards_unpin removes exactly the pin declaration — a board
// another source still sees keeps that face; a pin-only entry
// degrades to last-known (stale). A bad mac6 / empty ip / port
// outside 1..65535 is a "server" class refusal (the registry is the
// validator; a refused pin materializes nothing).
//
// # Transport (the lease route decision, B15 §4.4)
//
// lease's optional "transport" field still OVERRIDES; omitted, the
// DAEMON resolves the route from the live registry sources:
//
//	"" (absent)            the daemon decides — a TCP-capable source
//	                       (pin/mdns) live ⇒ tcp (NEVER a gratuitous
//	                       USB reboot, whatever USB's state; the lease
//	                       itself touches no board); only USB live ⇒
//	                       usb, the AUTO-FALLBACK: the CDC open happens
//	                       AT LEASE TIME and REBOOTS the board,
//	                       disclosed in the reply (rebooted:true +
//	                       transport_note — never silent); no live
//	                       source ⇒ the host-side lease of an
//	                       unreachable board (ops refuse fast with
//	                       cause "source gone").
//	"tcp"                  force the TCP route (honest unreachable at
//	                       op time when TCP is dead — never a silent
//	                       USB open).
//	"usb"                  force the USB route: same §2 reboot, same
//	                       disclosure.
//
// The reply always names the resolved route: transport ("tcp"/"usb"),
// rebooted (true only when THIS lease opened the CDC port — a
// USB-route lease that found the controller already warm rides it
// with no second open, rebooted:false and the warm note), and
// transport_note (the disclosure text, present on every usb-route
// lease). The held/refusal checks run BEFORE any USB open (a board
// leased by someone else must not be rebooted); a lease whose USB
// open then fails stays held host-side (release or retry).
//
// USB→TCP upgrade: a default (or explicit-tcp) lease that finds a
// warm USB controller with TCP live again YIELDS the §12 slot and
// re-arms the TCP route (no board touch; the first op dials) — one
// board.transport event carries the switch (below).
//
// Warm-controller reuse stays transport-agnostic: whichever controller
// is live serves every subsequent request — a USB lease's warm link
// carries default-transport ops up to release, with no implicit
// dialing of the other seam. Releasing a USB-transport lease (and
// daemon shutdown on a live USB controller) voluntarily YIELDS the
// controller slot first (§12 讓位 — observer hello, then close): the
// board cannot see a CDC close, so this is what lets the next TCP
// lease take controller without any reboot ritual.
// An unknown transport value is a "server" class error.
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
// class: eval/submit args numbers that cannot cross the int-only args
// bridge — see the args note above), server (malformed request, unknown
// type, bad params, bad transport, off-allowlist op). The fs_write
// 16 KiB cap refusal is gone (B10-T1): bigger writes ride the chunked
// transport instead of being refused.
//
// # Board-pathology fields (B15 §4.1④, board-touching failures)
//
// A board-touching op that fails TRANSPORT-side (never reached the
// board, lost the link, or could not take the controller slot) carries
// two extra fields inside error: cause ∈ {"source gone" (no live
// discovery source — refused with zero dial), "dial failed" (endpoint
// did not answer within the bounded ~18s dial), "link dead" (the
// connection died under the op, or the op could not complete over
// it — the daemon re-establishes; resend is reasonable), "arbitration"
// (§12 controller-slot window exhausted)} and last_seen_age_ms (int;
// milliseconds since the daemon's last protocol crossing with the
// board, -1 = never crossed). A board-ANSWERED error (class
// oom/compile/rt/api/budget/timeout/protocol crossing verbatim) means
// the board WAS reached — those carry neither field.
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
	// Transport selects the lease route: "" (default) = the daemon
	// decides from the live sources (TCP when a TCP-capable source is
	// live; the USB auto-fallback — with the reboot disclosure in the
	// reply — when only USB is), "tcp" forces the TCP route, "usb"
	// forces the fallback whose open reboots the board. Lease only;
	// see the transport contract above. Unknown values are a "server"
	// class error.
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
	// Session is session_reset's target namespace — 0 (the bare-eval
	// membership) when omitted; see the session_reset contract line.
	Session int `json:"session,omitempty"`
	// CaptureID addresses one capture (capture_stop's target).
	CaptureID string `json:"capture_id,omitempty"`
	// MaxBytes / MaxSeconds bound one capture (0 = the 64MiB / 600s
	// defaults; clamped 1..64MiB and 1..600 daemon-side).
	MaxBytes   int64 `json:"max_bytes,omitempty"`
	MaxSeconds int64 `json:"max_seconds,omitempty"`
	// Dest overrides a capture's filename (a bare basename inside the
	// captures root — path separators are refused daemon-side).
	Dest string `json:"dest,omitempty"`
	// BudgetMs is eval/submit's per-call budget override (§9.6, protocol
	// v6): 1..30000 forwarded to the board as the args key budget_ms for
	// THIS call only; 0 (omitted) = the session budget, wire key absent.
	// The reply wait derives from it daemon-side (replclient's
	// opTimeoutFor: budget+margin), so a budgeted call is never cut off
	// by the fixed eval ceiling.
	BudgetMs int64 `json:"budget_ms,omitempty"`
	// Sha256 is fs_write's expected-content digest (B15 §4.5②): when
	// present (64 hex chars — anything else is a zero-contact api
	// refusal) the write rides the VERIFIED path for any size, the
	// chunked transport landing the file with one fs.write_commit
	// carrying this sha; the board's own commit verdict (its
	// chunk-by-chunk ledger) is the verification. Absent = the classic
	// split (<=65472B one fs.write, above it the chunked transport with
	// the daemon-computed digest).
	Sha256 string `json:"sha256,omitempty"`
	// TimeoutMS bounds job_wait's park on the event tap (1..55000 —
	// under the 60s per-request ceiling so the timeout reply always
	// leaves the daemon before the request ctx dies).
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
	// Mac6 is boards_pin/boards_unpin's exact board id (⑨): 6 hex
	// chars, validated — never a selector.
	Mac6 string `json:"mac6,omitempty"`
	// IP/Port are boards_pin's pinned endpoint (validated daemon-side;
	// the reply echoes the joined endpoint).
	IP   string `json:"ip,omitempty"`
	Port int    `json:"port,omitempty"`
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
	// Board-pathology fields (B15 §4.1④, board-touching failures only):
	// why the crossing failed and how long ago the daemon last reached
	// the board. Cause is one of the four-value taxonomy — "source gone"
	// (registry has no live source; refused with zero dial), "dial
	// failed" (endpoint did not answer within the bounded attempts),
	// "link dead" (connection died under the op, or the op could not
	// complete over it — daemon redials), "arbitration" (the §12
	// controller-slot window exhausted). LastSeenAgeMS is -1 when the
	// daemon never crossed the board at all. Both absent on non-board
	// errors (lease gating, malformed requests, board-ANSWERED errors —
	// an OpError crossed verbatim means the board was reached fine).
	Cause         string `json:"cause,omitempty"`
	LastSeenAgeMS *int64 `json:"last_seen_age_ms,omitempty"`
}

// Error renders like a board error line: "<class>: <msg>", plus the
// pathology bracket when the daemon attaches it — the CLI and the MCP
// tool-error text are this exact string, so an agent reading either
// seat sees the cause and the last-contact age without a second call.
func (e *RPCError) Error() string {
	s := e.Class + ": " + e.Msg
	if e.Cause != "" {
		s += " [cause: " + e.Cause + "]"
	}
	if e.LastSeenAgeMS != nil {
		s += fmt.Sprintf(" [last_seen_age_ms: %d]", *e.LastSeenAgeMS)
	}
	return s
}

// Reply is the response half of a request: exactly one per request id,
// carrying result XOR error.
type Reply struct {
	Type   string    `json:"type"` // "reply"
	ID     int64     `json:"id"`
	Result any       `json:"result,omitempty"`
	Error  *RPCError `json:"error,omitempty"`
}

// WireEvent is the §8 event payload: source (gpio.N / uartN.rx /
// stdout / net / job; "board" for the daemon-synthesized kinds —
// board.reboot, transport) and its data map.
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
