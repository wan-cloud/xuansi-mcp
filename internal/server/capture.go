package server

// capture.go — the daemon's MB-level capture face (design §8 大檔資料
// 落地: the DAEMON writes the file, the agent gets the path). One
// capture_start opens a session: the board's all-sources tap (server.go
// tap/broadcast) feeds matching events to a spool goroutine that
// appends to $XDG_DATA_HOME/xuansi/captures/<UTC ts>-<mac6>-
// <source>.bin|.jsonl; the bounds (max_bytes / max_seconds), an early
// capture_stop, a disk failure, or daemon shutdown end it — each
// ending pushes exactly one {"type":"capture_done"} to the STARTING
// connection (reason limit | stopped | error). The wire contract and
// the deliberate corner rulings live in proto.go's capture section.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"xuansi/internal/replclient"
)

// Capture bounds and plumbing (proto.go's capture contract).
const (
	captureDefaultMaxBytes   = 64 << 20 // 64MiB
	captureMaxBytesCap       = 64 << 20
	captureDefaultMaxSeconds = 600
	captureMaxSecondsCap     = 600
	// captureQueue bounds one session's event backlog (fed by the tap,
	// drained by the spool). Fire-and-forget: a full queue drops the
	// event, logged — a spool wedged on a slow disk must never stall
	// the board's read loop (the same policy as the outbound queue).
	captureQueue = 256
	// captureStopWait bounds how long capture_stop waits for the spool
	// goroutine to exit before replying with the count it has (the
	// request loop of that one connection waits; every other keeps
	// flowing).
	captureStopWait = 2 * time.Second

	// captureTSLayout is the JSONL ts format — RFC3339 with the FULL
	// 9-digit nanosecond fraction (fixed width, no trailing-zero
	// trimming): every line a given event map produces has the same
	// length, so byte-cap reasoning (and the test that pins it) is
	// deterministic.
	captureTSLayout = "2006-01-02T15:04:05.000000000Z"
)

// captureFormat is how one source family lands in the file.
type captureFormat int

const (
	capRaw   captureFormat = iota // uart1.rx / uart2.rx / stdout → concatenated bytes (.bin)
	capJSONL                      // gpio.N / net → one JSON line per event (.jsonl)
)

// gpioSourceRe is the syntactic shape of a gpio.N source (the pin
// RANGE is the board's to enforce — api out-of-range comes from the
// events.subscribe it precedes).
var gpioSourceRe = regexp.MustCompile(`^gpio\.\d+$`)

// captureFormatFor maps a source to its file format, refusing the
// non-subscribable and unknown (proto.go: "job" is api-class — §8
// direct delivery; anything else is a server-class bad param).
func captureFormatFor(source string) (captureFormat, *RPCError) {
	switch source {
	case "uart1.rx", "uart2.rx", "stdout":
		return capRaw, nil
	case "net":
		return capJSONL, nil
	case "job":
		return 0, &RPCError{Class: "api",
			Msg: `capture: source "job" is not a subscribable source (protocol §8: job results are direct-delivered to the submitting connection only) — capture gpio.N / uart1.rx / uart2.rx / stdout / net instead`}
	default:
		if gpioSourceRe.MatchString(source) {
			return capJSONL, nil
		}
		return 0, &RPCError{Class: "server",
			Msg: fmt.Sprintf("capture: unknown source %q — the subscribable set is gpio.N / uart1.rx / uart2.rx / stdout / net", source)}
	}
}

// capturesRoot is the default capture root: $XDG_DATA_HOME/xuansi/
// captures, or ~/.local/share/xuansi/captures when XDG_DATA_HOME is
// unset — the same XDG data convention the MCP workspace uses. Tests
// inject their root through Opts.CaptureRoot, never the environment.
func capturesRoot() string {
	if d := os.Getenv("XDG_DATA_HOME"); d != "" {
		return filepath.Join(d, "xuansi", "captures")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".local", "share", "xuansi", "captures")
	}
	return filepath.Join(home, ".local", "share", "xuansi", "captures")
}

// osCreateCapture is the default capture-file opener: O_CREAT|O_EXCL —
// a capture NEVER truncates (review round 1): a path already owned by
// a live capture or an earlier landed file is a clean refusal, not a
// destroyed file. The default name is unique by construction (the
// capture sequence rides it), so this backstop mostly guards explicit
// dests and the same-second daemon-restart corner.
func osCreateCapture(path string) (io.WriteCloser, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}

// captureSession is one live capture: the file, the bounds, and the
// spool goroutine's plumbing. Exactly one of its endings runs —
// captureEnd's once guarantees it.
type captureSession struct {
	id       string
	board    string // mac6
	source   string
	path     string
	format   captureFormat
	maxBytes int64
	deadline time.Time

	file    io.WriteCloser
	written int64 // atomic: the spool writes, stop readers observe

	evCh    chan replclient.Event // tap → spool (fire-and-forget)
	stopCh  chan struct{}         // capture_stop / shutdown signal
	stopped atomic.Bool           // stopCh closed exactly once

	exited chan struct{} // closed when the session fully ended
	once   sync.Once     // the ending runs exactly once, ever
	cc     *clientConn   // the starting connection (done push target)
}

// captureLine is one JSONL record (gpio.N / net): the arrival time,
// the source, and the event's data map verbatim.
type captureLine struct {
	TS     string         `json:"ts"`
	Source string         `json:"source"`
	Data   map[string]any `json:"data"`
}

// rawPayload extracts the byte stream of one raw-family event: the fw
// event frames carry uart bytes as {data: bin} and stdout text as
// {data: str} (frozen protocol §8) — both concatenate byte-exact. An
// event missing its key is skipped, not fatal (logged by the caller).
func rawPayload(source string, data map[string]any) ([]byte, bool) {
	switch source {
	case "stdout":
		if s, ok := data["data"].(string); ok {
			return []byte(s), true
		}
	default: // uart1.rx / uart2.rx
		if b, ok := data["data"].([]byte); ok {
			return b, true
		}
	}
	return nil, false
}

// clampCapture returns the bounded value: v when in [lo,hi], the
// default when zero, the nearest bound otherwise.
func clampCapture(v, lo, hi, def int64) int64 {
	switch {
	case v == 0:
		return def
	case v < lo:
		return lo
	case v > hi:
		return hi
	default:
		return v
	}
}

// hCaptureStart serves capture_start: lease-gated like eval, the
// source validated and subscribed board-side, the file opened under
// the captures root, the spool goroutine launched, and the start reply
// carrying {capture_id, path} — from here on every ending pushes
// capture_done exactly once (proto.go's capture contract).
func (s *Server) hCaptureStart(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	format, ferr := captureFormatFor(req.Source)
	if ferr != nil {
		return nil, ferr
	}
	maxBytes := clampCapture(req.MaxBytes, 1, captureMaxBytesCap, captureDefaultMaxBytes)
	maxSeconds := clampCapture(req.MaxSeconds, 1, captureMaxSecondsCap, captureDefaultMaxSeconds)

	if req.Dest != "" {
		if req.Dest != filepath.Base(req.Dest) || req.Dest == "." || req.Dest == ".." || strings.ContainsAny(req.Dest, `/\`) {
			return nil, &RPCError{Class: "server",
				Msg: fmt.Sprintf("capture: dest %q must be a bare filename inside the captures root (no path separators)", req.Dest)}
		}
	}

	// the id and the default filename's uniqueness both ride the
	// capture sequence: <ts>-<mac6>-<source>-cap<N>.ext is unique by
	// construction, even for two same-second starts of the same
	// board+source (review round 1's collision)
	s.mu.Lock()
	root := s.captureRoot
	open := s.captureOpen
	s.captureSeq++
	seq := s.captureSeq
	s.mu.Unlock()
	id := "cap-" + strconv.FormatInt(seq, 10)

	name := req.Dest
	if name == "" {
		ext := ".jsonl"
		if format == capRaw {
			ext = ".bin"
		}
		name = time.Now().UTC().Format("20060102T150405") + "-" + b.MAC6 + "-" + req.Source + "-cap" + strconv.FormatInt(seq, 10) + ext
	}

	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf("capture: mkdir %s: %v", root, err)}
	}
	path := filepath.Join(root, name)
	file, oerr := open(path)
	if oerr != nil {
		if errors.Is(oerr, os.ErrExist) {
			return nil, &RPCError{Class: "server",
				Msg: fmt.Sprintf("capture: dest %q already exists — never truncated (a live capture or an earlier landed file owns it; omit dest for the unique default name)", name)}
		}
		return nil, &RPCError{Class: "server", Msg: fmt.Sprintf("capture: open %s: %v", path, oerr)}
	}

	cs := &captureSession{
		id: id, board: b.MAC6, source: req.Source, path: path, format: format,
		maxBytes: maxBytes,
		deadline: time.Now().Add(time.Duration(maxSeconds) * time.Second),
		file:     file,
		evCh:     make(chan replclient.Event, captureQueue),
		stopCh:   make(chan struct{}),
		exited:   make(chan struct{}),
		cc:       cc,
	}
	// registered BEFORE the board-side subscribe so no flowing event
	// can slip past (the tap starts feeding the moment the map entry
	// exists; the spool below drains it)
	s.mu.Lock()
	s.captures[id] = cs
	s.mu.Unlock()

	// The board-side subscription makes the source actually flow to
	// the daemon's tap (mirroring watch_events). A refusal (e.g. api
	// "uart1 not setup") never leaves a session behind: the file closes
	// and is unlinked (the never-started capture would otherwise leave
	// 0-byte litter), the entry goes, the error crosses verbatim — no
	// done push, because no capture ever started.
	ctl, err := s.warmController(ctx, b)
	if err == nil {
		_, err = ctl.Op(ctx, "events.subscribe", map[string]any{"source": req.Source})
	}
	if err != nil {
		s.captureEnd(cs, "", false)
		// captureEnd closed the file; nothing was ever spooled to it,
		// so remove the empty shell (final review)
		if rerr := os.Remove(cs.path); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			s.logf("server: capture %s: remove unstarted file: %v", cs.id, rerr)
		}
		return nil, err
	}

	s.wg.Add(1)
	go s.spoolCapture(cs)
	s.logf("server: capture %s started (%s %s → %s, max %dMiB/%ds)", id, b.MAC6, req.Source, path, maxBytes>>20, maxSeconds)
	return map[string]any{"capture_id": id, "path": path}, nil
}

// hCaptureStop serves capture_stop: signal the session, wait bounded
// for the spool to exit (its count is final then), reply the stopped
// view. Any connection may stop a capture — the CLI's one-shot
// connections could not otherwise pair with their start. Unknown /
// finished ids are server-class errors.
func (s *Server) hCaptureStop(req *Request) (any, error) {
	if req.CaptureID == "" {
		return nil, &RPCError{Class: "server", Msg: "capture_stop: capture_id is required"}
	}
	s.mu.Lock()
	cs := s.captures[req.CaptureID]
	s.mu.Unlock()
	if cs == nil {
		return nil, &RPCError{Class: "server",
			Msg: fmt.Sprintf("capture_stop: unknown capture id %q (finished or never-started captures cannot be stopped)", req.CaptureID)}
	}
	cs.signalStop()
	select {
	case <-cs.exited:
	case <-time.After(captureStopWait):
		s.logf("server: capture %s: stop wait timed out — replying with the current count", cs.id)
	}
	// A capture that finished itself between the lookup and the signal
	// replies "stopped" here while its push carries the reason that
	// won the race (limit/error) — both are the truth, one beat apart.
	return map[string]any{
		"path": cs.path, "bytes": atomic.LoadInt64(&cs.written), "reason": "stopped",
	}, nil
}

// signalStop closes the stop channel exactly once (concurrent stops
// and daemon shutdown funnel through the same atomic).
func (cs *captureSession) signalStop() {
	if cs.stopped.CompareAndSwap(false, true) {
		close(cs.stopCh)
	}
}

// spoolCapture is the session's writer goroutine: events off the tap
// feed land in the file until an ending fires (bytes/time limit, stop
// signal) or a write fails. Never wedges the server: every path ends
// in captureEnd exactly once and the goroutine exits.
func (s *Server) spoolCapture(cs *captureSession) {
	defer s.wg.Done()
	timer := time.NewTimer(time.Until(cs.deadline))
	defer timer.Stop()
	for {
		select {
		case ev := <-cs.evCh:
			if s.captureWrite(cs, ev) {
				return
			}
		case <-cs.stopCh:
			// Flush the tail the tap already fed: events queued before
			// the stop signal land in the file (the tap feeds
			// synchronously under the server lock, so program order
			// holds; the drain is bounded by max_bytes as always).
			for {
				select {
				case ev := <-cs.evCh:
					if s.captureWrite(cs, ev) {
						return
					}
				default:
					s.captureEnd(cs, "stopped", true)
					return
				}
			}
		case <-timer.C:
			s.captureEnd(cs, "limit", true)
			return
		}
	}
}

// captureWrite lands one event and reports whether the session ended
// (limit or disk error — captureEnd already ran). RAW chunks that
// would cross the byte cap truncate onto it exactly; JSONL lines that
// would cross are left unwritten (a truncated line is corrupt JSON).
func (s *Server) captureWrite(cs *captureSession, ev replclient.Event) (ended bool) {
	var payload []byte
	if cs.format == capRaw {
		b, ok := rawPayload(cs.source, ev.Data)
		if !ok {
			s.logf("server: capture %s: %s event without its %q payload key — skipped", cs.id, ev.Source, "data")
			return false
		}
		payload = b
		if room := cs.maxBytes - atomic.LoadInt64(&cs.written); int64(len(payload)) > room {
			payload = payload[:room]
		}
	} else {
		line, jerr := json.Marshal(captureLine{
			TS: time.Now().UTC().Format(captureTSLayout), Source: ev.Source, Data: ev.Data,
		})
		if jerr != nil {
			s.logf("server: capture %s: encode event: %v — skipped", cs.id, jerr)
			return false
		}
		payload = append(line, '\n')
		if atomic.LoadInt64(&cs.written)+int64(len(payload)) > cs.maxBytes {
			s.captureEnd(cs, "limit", true)
			return true
		}
	}
	if len(payload) > 0 {
		n, werr := cs.file.Write(payload)
		atomic.AddInt64(&cs.written, int64(n))
		if werr != nil {
			s.logf("server: capture %s: disk write failed (%v) — ending", cs.id, werr)
			s.captureEnd(cs, "error", true)
			return true
		}
	}
	if atomic.LoadInt64(&cs.written) >= cs.maxBytes {
		s.captureEnd(cs, "limit", true)
		return true
	}
	return false
}

// captureEnd ends the session exactly once: the file closes, the map
// entry goes, exited closes (the stop reply's count is final from
// here), and when notify is set the capture_done push rides the
// starting connection with the final count — reply-priority delivery
// (proto.go), dropped only if that connection is already gone.
func (s *Server) captureEnd(cs *captureSession, reason string, notify bool) {
	cs.once.Do(func() {
		if err := cs.file.Close(); err != nil {
			s.logf("server: capture %s: close file: %v", cs.id, err)
		}
		s.mu.Lock()
		delete(s.captures, cs.id)
		s.mu.Unlock()
		close(cs.exited)
		if notify {
			s.logf("server: capture %s done (%s): %d bytes → %s", cs.id, reason, atomic.LoadInt64(&cs.written), cs.path)
			if !cs.cc.send(&CaptureDone{
				Type: "capture_done", CaptureID: cs.id, Path: cs.path,
				Bytes: atomic.LoadInt64(&cs.written), Reason: reason,
			}) {
				s.logf("server: capture %s: done push dropped (starting connection gone)", cs.id)
			}
		}
	})
}

// feedCapturesLocked hands one tapped board event to every live
// capture of that board+source (caller holds srv.mu). Fire-and-forget
// like the watcher fan-out: a full session queue drops the event,
// logged — never a stall under the server lock.
func (s *Server) feedCapturesLocked(mac6 string, ev replclient.Event) {
	for _, cs := range s.captures {
		if cs.board != mac6 || cs.source != ev.Source {
			continue
		}
		select {
		case cs.evCh <- ev:
		default:
			s.logf("server: capture %s queue full — event dropped", cs.id)
		}
	}
}

// stopCapturesShutdown ends every live capture at daemon shutdown:
// files close, done pushes say "stopped" (a shutdown is a deliberate
// stop, not a failure — proto.go's capture contract). Best effort
// toward the connections (they drop moments later), bounded per
// session so Close never hangs on one.
func (s *Server) stopCapturesShutdown() {
	s.mu.Lock()
	live := make([]*captureSession, 0, len(s.captures))
	for _, cs := range s.captures {
		live = append(live, cs)
	}
	s.mu.Unlock()
	for _, cs := range live {
		cs.signalStop()
		select {
		case <-cs.exited:
		case <-time.After(captureStopWait):
		}
	}
}
