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
//
// Every request gets exactly one reply carrying the same id. Events are
// pushed unsolicited (only after watch_events established the
// subscription for that connection) and carry no id.
//
// # Requests
//
//	list_boards    {}                          → {boards:[{mac6,alias,sources,host,port,usb_path,fw}],mdns_error}
//	board_status   {board}                      → {board:{...}, connected, lease:{holder,since,session,grace_deadline}?|null, scripts?, scripts_error?}
//	lease          {board, holder?}             → {board, holder, session}
//	release        {board, holder?}             → {board, holder}
//	eval           {board, code, args?, holder?} → {out, value?}   (lease required)
//	submit         {board, code, tag, args?, holder?} → board ack {tag, queued}  (lease required)
//	script_start   {board, name, code, holder?}  → board ack       (lease required; runs under the LEASE's session namespace — the T10 eval ruling)
//	script_stop    {board, name, holder?}        → board ack       (lease required)
//	fs_write       {board, path, data, holder?}  → board ack       (lease required; data = base64 (JSON []byte); ≤16384B enforced daemon-side)
//	fs_read        {board, path}                 → {data: base64}  (read-only, no lease)
//	watch_events   {board, source?}              → {board}         (subscribes THIS connection; source, when given, is additionally events.subscribe'd board-side)
//
// BOARD selectors are mac6 / alias / hostname prefixes resolved by the
// discovery registry (T9 Resolve): unique prefix hits, ambiguity or no
// match is a structured error listing candidates.
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
// slot_held (controller arbitration window exhausted — USB reboot is
// the caller's explicit last resort), server (malformed request,
// unknown type, bad params).
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
	Code   string `json:"code,omitempty"`
	Tag    string `json:"tag,omitempty"`
	Name   string `json:"name,omitempty"`
	Path   string `json:"path,omitempty"`
	Data   []byte `json:"data,omitempty"` // JSON base64 ↔ board bin
	Source string `json:"source,omitempty"`
	Args   any    `json:"args,omitempty"` // eval/submit: array|map → Lua ARGS
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
