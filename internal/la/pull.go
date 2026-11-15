package la

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"xuansi/internal/replclient"
)

// ReadWindow is la.read's len cap: 32640 — the fw's reply-frame cap
// (XS_REPL_REPLY_MAX-128), the same constant form fs.read_part rides
// (plan constraint 1: window ops, one frozen cap, no new constant).
const ReadWindow = 32640

// Pull errors — wrapped with the specifics, matched with errors.Is.
var (
	// ErrIncomplete marks a landing that broke mid-pull (link death,
	// cancellation): data.bin holds whatever arrived, meta carries
	// incomplete:true, and the lossless promise did NOT hold.
	ErrIncomplete = errors.New("la: capture landed incomplete")
	// ErrChecksum is the identity assertion's failure: the landed
	// bytes cannot be proven equal to the ring's (digest mismatch,
	// short pull, or unparseable stream). Never a warning.
	ErrChecksum = errors.New("la: pull-back checksum identity failed")
)

// waitPoll is the done-wait's dead-link poll cadence: a link that dies
// silently between ops (keepalive verdict ≈6s) surfaces within one
// tick instead of hanging the wait on ctx.
const waitPoll = 400 * time.Millisecond

// endOfIntentBound caps the detached best-effort stop/disarm of a
// broken or refused pull: the board-side capture must end even though
// the caller's ctx may be done, but never without a bound of its own.
const endOfIntentBound = 5 * time.Second

// evQueue bounds the pull's event backlog. Progress is throttled by
// the fw (≥32KB or ≥5s — a full 4MB ring draws ≤~130), so a 512-slot
// buffer always leaves room for the one done the wait rides on; the
// handler itself never blocks (the OnEvent discipline).
const evQueue = 512

// Board is the pull's connection seam. Production is the daemon's
// board.Controller (Op/OnEvent/Dead satisfy this verbatim); tests
// script a fake.
type Board interface {
	Op(ctx context.Context, op string, args map[string]any) (map[string]any, error)
	OnEvent(sourcePrefix string, fn func(replclient.Event)) (cancel func())
	Dead() bool
}

// PullBoard bundles what PullCapture needs from the board: the
// identity for meta's board column plus the live connection the quintet
// rides.
type PullBoard struct {
	ID   string // mac6 (or alias) — recorded verbatim
	Conn Board
}

// ArmArgs is la.listen_arm's parameter set (the fw v9 face is the
// authority; the daemon crosses these verbatim and the board's refusal
// — coach message included — crosses back verbatim). Baud is an int or
// the string "auto"; IdleSyms/MaxMS ride omitted at zero (the fw's
// own defaults apply).
type ArmArgs struct {
	Port     int
	Pin      int
	Baud     any // int | "auto"
	RingKB   int
	IdleSyms int
	MaxMS    int
}

func (a ArmArgs) wire() map[string]any {
	m := map[string]any{"port": a.Port, "pin": a.Pin, "baud": a.Baud, "ring_kb": a.RingKB}
	if a.IdleSyms > 0 {
		m["idle_syms"] = a.IdleSyms
	}
	if a.MaxMS > 0 {
		m["max_ms"] = a.MaxMS
	}
	return m
}

// PullCapture arms one listen capture on the board and pulls it back
// losslessly: subscribe the "la" event source (before arm — a done the
// subscriber missed would hang the wait), arm, wait the done event
// (ring full / max_ms limit; a silent link death surfaces via the
// dead poll), stop (the idempotent authoritative ledger), info (the
// 12-column mirror), read the ring in ReadWindow windows until eof,
// then VERIFY — a second full pass whose sha256 must equal the first,
// plus the accounting identities (Σ windows == total; parsed Σ n ==
// stop's bytes) — and disarm. The landed object is the returned ref;
// any break after arm lands the partial object with incomplete:true
// and returns it together with the wrapped error.
func (s *Store) PullCapture(ctx context.Context, board PullBoard, armArgs ArmArgs) (CaptureRef, error) {
	// 1. Make the "la" source flow to this connection before anything
	// arms — the done notification is the pull's only completion
	// signal. A refusal here (old fw, unknown source) crosses
	// verbatim and nothing lands.
	if _, err := board.Conn.Op(ctx, "events.subscribe", map[string]any{"source": "la"}); err != nil {
		return CaptureRef{}, err
	}

	// 2. The local handler rides BEFORE arm for the same reason: a
	// done pushed during the arm op's own round trip is already
	// queued when the wait starts.
	evCh, cancelEv := subscribeLA(board.Conn)
	defer cancelEv()

	// 3. Arm. The board's refusal (coach message included) crosses
	// verbatim; no object is created for a refused arm.
	res, err := board.Conn.Op(ctx, "la.listen_arm", armArgs.wire())
	if err != nil {
		return CaptureRef{}, err
	}
	capNum, okCap := anyI(res["cap"])
	baud, okBaud := anyI(res["baud"])
	if !okCap || !okBaud {
		return CaptureRef{}, fmt.Errorf("la: la.listen_arm reply missing cap/baud: %v", res)
	}

	// 4. From here on a capture exists board-side: the object lands
	// even on a broken pull (incomplete), so reserve its directory
	// and the empty data.bin now, and keep one buffered writer for
	// the file's whole lifetime (every exit path flushes it).
	id, dir, err := s.reserveID(board.ID, "listen")
	if err != nil {
		endOfIntent(board.Conn, capNum)
		return CaptureRef{}, fmt.Errorf("la: reserve capture dir: %v", err)
	}
	// The reservation ends with this pull, whichever way it lands:
	// once released, an unlanded dir is honest litter to the next
	// sweep again.
	defer s.unreserve(id)
	ref := CaptureRef{ID: id, Dir: dir,
		DataPath: filepath.Join(dir, "data.bin"),
		MetaPath: filepath.Join(dir, "meta.json")}
	file, err := os.OpenFile(ref.DataPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		endOfIntent(board.Conn, capNum)
		return CaptureRef{}, fmt.Errorf("la: open %s: %v", ref.DataPath, err)
	}
	bw := bufio.NewWriter(file)
	meta := Meta{CapID: id, Board: board.ID, Mode: "listen",
		Port: armArgs.Port, Pin: armArgs.Pin, Baud: int(baud)}
	land := func(cause error) (CaptureRef, error) {
		bw.Flush()
		file.Close()
		return s.landIncomplete(ref, meta, board.Conn, capNum, cause)
	}

	// 5. Wait the done event for OUR cap (waitDone owns the loop:
	// progress and foreign caps flow past; ctx cancellation ends the
	// wait (landIncomplete's end-of-intent stop ends the board-side
	// capture — it must not outlive a caller that gave up); a
	// silently dead link surfaces on the poll tick).
	if werr := waitDone(ctx, board.Conn, evCh, capNum); werr != nil {
		return land(fmt.Errorf("%w: %w", ErrIncomplete, werr))
	}

	// 6. Stop: the idempotent, authoritative ledger. On a capture that
	// self-ended (full/limit) it echoes the same reason; on a live
	// one it ends it (reason "stopped").
	res, err = board.Conn.Op(ctx, "la.stop", map[string]any{"cap": capNum})
	if err != nil {
		return land(fmt.Errorf("%w: la.stop: %w", ErrIncomplete, err))
	}
	stopBytes, _ := anyI(res["bytes"])
	meta.Reason, _ = res["reason"].(string)

	// 7. Info: the 12-column ledger mirror. Best-effort — the data
	// path needs only stop+read; a missing ledger leaves zeros.
	mirrorLedger(ctx, board.Conn, capNum, &meta)

	// 8. Pass 1 — read the ring in windows until eof, landing bytes
	// into data.bin under a running sha256.
	pass1 := sha256.New()
	total, err := readAll(ctx, board.Conn, capNum, func(_ int64, data []byte) error {
		pass1.Write(data)
		_, err := bw.Write(data)
		return err
	})
	if err != nil {
		return land(fmt.Errorf("%w: read pass: %w", ErrIncomplete, err))
	}
	if err := bw.Flush(); err != nil {
		return land(fmt.Errorf("%w: flush: %w", ErrIncomplete, err))
	}
	if err := file.Close(); err != nil {
		return land(fmt.Errorf("%w: close: %w", ErrIncomplete, err))
	}
	meta.StreamBytes = total

	// 9. The landed stream must parse whole — a torn record or
	// trailing garbage means the landed bytes are not the ring's.
	stats, perr := parseStats(ref.DataPath, false)
	if perr != nil {
		return land(fmt.Errorf("%w: landed stream does not parse: %v", ErrChecksum, perr))
	}
	meta.TStartNS, meta.TEndNS = stats.TStartNS, stats.TEndNS
	meta.Bytes, meta.Chunks = stats.Payload, stats.Chunks

	// 10. Pass 2 — the identity assertion itself: a second full read
	// of the static ring must hash identical to pass 1. The ring is
	// stopped, so any difference is a transfer fault — an error, not
	// a warning.
	pass2 := sha256.New()
	if _, err := readAll(ctx, board.Conn, capNum, func(_ int64, data []byte) error {
		pass2.Write(data)
		return nil
	}); err != nil {
		return land(fmt.Errorf("%w: verify pass: %w", ErrIncomplete, err))
	}
	d1, d2 := hex.EncodeToString(pass1.Sum(nil)), hex.EncodeToString(pass2.Sum(nil))
	meta.SHA256 = d1
	if d1 != d2 {
		return land(fmt.Errorf("%w: sha256 pass1=%s pass2=%s — the landed bytes are not the ring's", ErrChecksum, d1, d2))
	}
	if stats.Payload != stopBytes {
		return land(fmt.Errorf("%w: payload accounting: parsed Σn=%d but la.stop reports bytes=%d", ErrChecksum, stats.Payload, stopBytes))
	}

	// 11. Disarm — release the pin and the ring. The data is verified
	// already, so a disarm failure fails the call without tainting
	// the object (the lossless promise held; the fw-side ring is
	// still held, honestly reported).
	if _, err := board.Conn.Op(ctx, "la.disarm", map[string]any{"cap": capNum}); err != nil {
		meta.PulledAt = s.clock()
		ref.Meta = meta
		if _, werr := s.writeMeta(meta); werr != nil {
			return CaptureRef{}, fmt.Errorf("la: disarm failed (%v) and the meta landing failed too: %v", err, werr)
		}
		return ref, fmt.Errorf("la: capture %s landed and verified, but la.disarm failed: %v (board-side ring still held)", id, err)
	}

	// 12. Land the meta and enforce retention (best-effort — a
	// retention hiccup never fails a landed capture).
	meta.PulledAt = s.clock()
	ref.Meta = meta
	if _, err := s.writeMeta(meta); err != nil {
		return CaptureRef{}, fmt.Errorf("la: land meta for %s: %v", id, err)
	}
	_, _ = s.Retention()
	return ref, nil
}

// subscribeLA arms the pull families' local "la" event handler (the
// shared step 2: registered BEFORE arm — a done pushed during the arm
// op's own round trip is already queued when the wait starts; the
// handler itself never blocks the connection's read goroutine).
func subscribeLA(conn Board) (chan replclient.Event, func()) {
	evCh := make(chan replclient.Event, evQueue)
	cancel := conn.OnEvent("la", func(ev replclient.Event) {
		select {
		case evCh <- ev:
		default: // never block the connection's read goroutine
		}
	})
	return evCh, cancel
}

// landIncomplete is the broken-pull landing: parse what landed
// tolerantly (a torn tail is expected, not fatal), mark the meta
// incomplete, attempt the end-of-intent stop+disarm (idempotent both),
// and answer the ref together with the cause — the object is forensics
// for a pull whose lossless promise did not hold. The identity
// assertion's failures ride here too, cause carrying ErrChecksum.
func (s *Store) landIncomplete(ref CaptureRef, meta Meta, conn Board, capNum int64, cause error) (CaptureRef, error) {
	endOfIntent(conn, capNum)
	if meta.Mode == "edge" {
		if st, err := parseRunStats(ref.DataPath, true); err == nil {
			meta.Bytes, meta.Runs = st.Stream, int(st.Runs)
			if meta.T0NS > 0 { // the axis is absolute only when the ledger landed
				meta.TStartNS = uint64(meta.T0NS)
				meta.TEndNS = uint64(meta.T0NS) + st.SpanNS
			}
		}
	} else if meta.Mode == "samples" {
		// the byte form's tolerant account: no records to parse —
		// whatever landed is that many samples (a short pull is the
		// incompleteness itself, stated by the flag).
		if fi, err := os.Stat(ref.DataPath); err == nil {
			meta.Bytes = fi.Size()
			meta.Samples = fi.Size()
			if meta.T0NS > 0 && meta.RateHz > 0 {
				meta.TStartNS = uint64(meta.T0NS)
				meta.TEndNS = uint64(meta.T0NS) + uint64(fi.Size())*tickOf(meta.RateHz)
			}
		}
	} else if st, err := parseStats(ref.DataPath, true); err == nil {
		meta.TStartNS, meta.TEndNS = st.TStartNS, st.TEndNS
		meta.Bytes, meta.Chunks = st.Payload, st.Chunks
	}
	if fi, err := os.Stat(ref.DataPath); err == nil {
		meta.StreamBytes = fi.Size()
	}
	meta.Incomplete = true
	meta.PulledAt = s.clock()
	ref.Meta = meta
	if _, err := s.writeMeta(meta); err != nil {
		return CaptureRef{}, fmt.Errorf("%v; and the incomplete meta landing failed: %v", cause, err)
	}
	_, _ = s.Retention()
	return ref, cause
}

// waitDone is the pull families' done-event wait (PullCapture's step
// 5, extracted for the edge pull): progress and foreign caps flow
// past, ctx cancellation ends the wait, and a silently dead link
// surfaces on the poll tick (a keepalive verdict ≈6s would otherwise
// hang the wait on ctx alone).
func waitDone(ctx context.Context, conn Board, evCh <-chan replclient.Event, capNum int64) error {
	tick := time.NewTicker(waitPoll)
	defer tick.Stop()
	for {
		select {
		case ev := <-evCh:
			if d := ev.Data; d["event"] == "done" {
				if c, ok := anyI(d["cap"]); ok && c == capNum {
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if conn.Dead() {
				return fmt.Errorf("link died while waiting for the done event")
			}
		}
	}
}

// endOfIntent bounds a broken or refused pull's board-side cleanup:
// stop then disarm, best-effort on a DETACHED bounded context (the
// caller's ctx may be the very reason the pull is ending; la.stop is
// idempotent, so a capture that already stopped just echoes its
// reason). Errors are swallowed — the landing's cause is the answer.
func endOfIntent(conn Board, capNum int64) {
	ectx, cancel := context.WithTimeout(context.Background(), endOfIntentBound)
	defer cancel()
	_, _ = conn.Op(ectx, "la.stop", map[string]any{"cap": capNum})
	_, _ = conn.Op(ectx, "la.disarm", map[string]any{"cap": capNum})
}

// readAll walks la.read windows from off 0 until eof, handing each
// window's bytes to sink, and answers the ring's total. The window is
// the frozen ReadWindow; convergence is eof with the off/total
// identities checked on every reply — a ring that mutates mid-read, a
// window that stalls short, or an eof that lands before the bytes do
// is an error (mirroring hFsRead's walk with the stopped ring's
// stricter accounting).
func readAll(ctx context.Context, conn Board, capNum int64, sink func(off int64, data []byte) error) (int64, error) {
	var total int64 = -1
	var off int64
	for {
		res, err := conn.Op(ctx, "la.read", map[string]any{"cap": capNum, "off": off, "len": int64(ReadWindow)})
		if err != nil {
			return 0, err
		}
		data, _ := res["data"].([]byte)
		t, ok := anyU64(res["total"])
		if !ok {
			return 0, fmt.Errorf("la.read reply missing total")
		}
		if total < 0 {
			total = int64(t)
		} else if int64(t) != total {
			return total, fmt.Errorf("ring total mutated mid-read (%d → %d) — the capture is not stopped", total, t)
		}
		eof, _ := res["eof"].(bool)
		if len(data) > 0 {
			if err := sink(off, data); err != nil {
				return total, err
			}
			off += int64(len(data))
		}
		if eof || total == 0 {
			if off != total {
				return total, fmt.Errorf("short pull: eof at %d of %d stream bytes", off, total)
			}
			return total, nil
		}
		if len(data) == 0 {
			return total, fmt.Errorf("empty window at off %d with %d stream bytes to go", off, total-off)
		}
		if off >= total {
			return total, nil // eof lagging the accounting identities: converged
		}
	}
}

// mirrorLedger copies the fw's 12-column la.info meta into the meta's
// ledger fields. Best-effort by design: the lossless promise needs
// only stop+read; a missing ledger leaves honest zeros.
func mirrorLedger(ctx context.Context, conn Board, capNum int64, meta *Meta) {
	res, err := conn.Op(ctx, "la.info", map[string]any{"cap": capNum})
	if err != nil {
		return
	}
	caps, _ := res["caps"].([]any)
	for _, el := range caps {
		m, ok := el.(map[string]any)
		if !ok {
			continue
		}
		if c, ok := anyI(m["cap"]); !ok || c != capNum {
			continue
		}
		lm, _ := m["meta"].(map[string]any)
		if v, ok := anyI(lm["ring_kb"]); ok {
			meta.RingKB = int(v)
		}
		if v, ok := anyI(lm["idle_syms"]); ok {
			meta.IdleSyms = int(v)
		}
		if v, ok := anyI(lm["max_ms"]); ok {
			meta.MaxMS = int(v)
		}
		if v, ok := anyI(lm["chunks_dropped"]); ok {
			meta.ChunksDropped = v
		}
		if v, ok := anyI(lm["bytes_dropped"]); ok {
			meta.BytesDropped = v
		}
		if v, ok := anyI(lm["hw_ovf"]); ok {
			meta.HwOvf = v
		}
		if v, ok := anyI(lm["armed_ns"]); ok {
			meta.ArmedNS = v
		}
		if v, ok := anyI(lm["stopped_ns"]); ok {
			meta.StoppedNS = v
		}
		return
	}
}

// anyI reads an integral value across the int (test fakes) and int64
// (msgpack decode) shapes.
func anyI(v any) (int64, bool) {
	switch v := v.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	}
	return 0, false
}

// anyU64 is anyI's unsigned view (totals are u32 on the wire).
func anyU64(v any) (uint64, bool) {
	if v, ok := anyI(v); ok && v >= 0 {
		return uint64(v), true
	}
	return 0, false
}
