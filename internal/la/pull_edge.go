package la

// pull_edge.go — the edge capture's pull (T11, the T10 engine's
// daemon half): the same lossless discipline PullCapture rides for
// listen captures, one seat over — arm (la.edge_arm), done wait,
// stop, info (the 13-key edge ledger), two verified read passes,
// disarm — plus the run-stream accounting identities the edge format
// carries: parsed runs × 9 == stream total == la.stop's bytes (the
// fw's runs==total/9 invariant, re-derived from the landed bytes).

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"xuansi/internal/replclient"
)

// edgeResHzPinned is the fw's frozen edge resolution (XS_LA_EDGE_RES_HZ,
// 80MHz / 12.5ns tick) — res_hz is pinned, never a choice.
const edgeResHzPinned = 80_000_000

// EdgeTrig is the edge arm's trigger: the frozen small language (no
// protocol semantics — a line-level match).
type EdgeTrig struct {
	Pin   int    // a pins[] member
	Kind  string // "edge_r" | "edge_f" | "level"
	Level *int   // 0|1, kind "level" only (pointer: presence is domain)
	PreKB int    // pre-trigger window KB (0 = no history)
}

func (t EdgeTrig) wire() map[string]any {
	m := map[string]any{"pin": int64(t.Pin), "kind": t.Kind}
	if t.Kind == "level" && t.Level != nil {
		m["level"] = int64(*t.Level)
	}
	if t.PreKB > 0 {
		m["pre_kb"] = int64(t.PreKB)
	}
	return m
}

// EdgeArmArgs is la.edge_arm's parameter set (the fw v9 face is the
// authority; the daemon crosses these verbatim and the board's
// refusal — coach message included — crosses back verbatim).
type EdgeArmArgs struct {
	Pins   []int
	ResHz  int // 0 = the pinned 80MHz (any other value the fw refuses)
	RingKB int // REQUIRED for edge (64..4096): no fw default exists
	MaxMS  int // 0 = the fw default (600000)
	Trig   *EdgeTrig
}

func (a EdgeArmArgs) wire() map[string]any {
	pins := make([]any, len(a.Pins)) // the wire domain carries arrays as []any
	for i, p := range a.Pins {
		pins[i] = int64(p)
	}
	m := map[string]any{"pins": pins, "res_hz": int64(edgeResHzPinned), "ring_kb": int64(a.RingKB)}
	if a.MaxMS > 0 {
		m["max_ms"] = int64(a.MaxMS)
	}
	if a.Trig != nil {
		m["trig"] = a.Trig.wire()
	}
	return m
}

// PullEdge arms one edge capture on the board and pulls it back
// losslessly — PullCapture's discipline over the edge quintet
// (la.edge_arm → done event → la.stop → la.info → la.read windows ×2
// verified → la.disarm). The landed object carries the edge meta
// (pins, res_hz, the mirrored trigger + t0_ns ledger); the identity
// assertion is the run form's own: the landed stream parses whole
// into runs, runs×9 == total == stop's bytes. Any break after arm
// lands the partial object with incomplete:true and returns it
// together with the wrapped error.
func (s *Store) PullEdge(ctx context.Context, board PullBoard, args EdgeArmArgs) (CaptureRef, error) {
	// 1–2. The "la" source flows to this connection before anything
	// arms (a missed done would hang the wait).
	if _, err := board.Conn.Op(ctx, "events.subscribe", map[string]any{"source": "la"}); err != nil {
		return CaptureRef{}, err
	}
	evCh := make(chan replclient.Event, evQueue)
	cancelEv := board.Conn.OnEvent("la", func(ev replclient.Event) {
		select {
		case evCh <- ev:
		default: // never block the connection's read goroutine
		}
	})
	defer cancelEv()

	// 3. Arm — the board's refusal (coach message included) crosses
	// verbatim; no object is created for a refused arm.
	res, err := board.Conn.Op(ctx, "la.edge_arm", args.wire())
	if err != nil {
		return CaptureRef{}, err
	}
	capNum, okCap := anyI(res["cap"])
	resHz, okRes := anyI(res["res_hz"])
	if !okCap || !okRes {
		return CaptureRef{}, fmt.Errorf("la: la.edge_arm reply missing cap/res_hz: %v", res)
	}

	// 4. The capture exists board-side: reserve the object dir and
	// the empty data.bin now (every exit path lands something).
	id, dir, err := s.reserveID(board.ID, "edge")
	if err != nil {
		endOfIntent(board.Conn, capNum)
		return CaptureRef{}, fmt.Errorf("la: reserve capture dir: %v", err)
	}
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
	meta := Meta{CapID: id, Board: board.ID, Mode: "edge", ResHz: int(resHz)}
	land := func(cause error) (CaptureRef, error) {
		bw.Flush()
		file.Close()
		return s.landIncomplete(ref, meta, board.Conn, capNum, cause)
	}

	// 5. Wait the done event for OUR cap.
	if werr := waitDone(ctx, board.Conn, evCh, capNum); werr != nil {
		return land(fmt.Errorf("%w: %w", ErrIncomplete, werr))
	}

	// 6. Stop: the idempotent, authoritative ledger.
	res, err = board.Conn.Op(ctx, "la.stop", map[string]any{"cap": capNum})
	if err != nil {
		return land(fmt.Errorf("%w: la.stop: %w", ErrIncomplete, err))
	}
	stopBytes, _ := anyI(res["bytes"])
	meta.Reason, _ = res["reason"].(string)

	// 7. Info: the 13-key edge ledger mirror (res_hz, trig, trig_fired,
	// runs, runs_dropped, hw_ovf, ring_kb, max_ms, reason, armed_ns,
	// stopped_ns, t0_ns + the top-level pins). Best-effort like the
	// listen pull — but t0_ns is the analysis axis: a capture whose
	// mirror did not land refuses the edge analysis seats honestly.
	mirrorEdgeLedger(ctx, board.Conn, capNum, &meta)

	// 8. Pass 1 — read the ring into data.bin under a running sha256.
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

	// 9. The landed stream must parse whole into runs — the run form's
	// identity: runs×9 == total == stop's bytes.
	stats, perr := parseRunStats(ref.DataPath, false)
	if perr != nil {
		return land(fmt.Errorf("%w: landed stream does not parse: %v", ErrChecksum, perr))
	}
	meta.Runs = int(stats.Runs)
	meta.Bytes = stats.Stream
	if meta.T0NS > 0 {
		meta.TStartNS = uint64(meta.T0NS)
		meta.TEndNS = uint64(meta.T0NS) + stats.SpanNS
	}

	// 10. Pass 2 — the identity assertion: a second full read of the
	// static ring must hash identical to pass 1.
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
	if stats.Stream != total || stats.Stream != stopBytes {
		return land(fmt.Errorf("%w: run accounting: parsed %d runs ×9=%d, stream total %d, la.stop bytes=%d — the invariant runs×9==total==bytes broke", ErrChecksum, stats.Runs, stats.Stream, total, stopBytes))
	}

	// 11. Disarm — the data is verified; a disarm failure fails the
	// call without tainting the object.
	if _, err := board.Conn.Op(ctx, "la.disarm", map[string]any{"cap": capNum}); err != nil {
		meta.PulledAt = s.clock()
		ref.Meta = meta
		if _, werr := s.writeMeta(meta); werr != nil {
			return CaptureRef{}, fmt.Errorf("la: disarm failed (%v) and the meta landing failed too: %v", err, werr)
		}
		return ref, fmt.Errorf("la: capture %s landed and verified, but la.disarm failed: %v (board-side ring still held)", id, err)
	}

	// 12. Land the meta and enforce retention (best-effort).
	meta.PulledAt = s.clock()
	ref.Meta = meta
	if _, err := s.writeMeta(meta); err != nil {
		return CaptureRef{}, fmt.Errorf("la: land meta for %s: %v", id, err)
	}
	_, _ = s.Retention()
	return ref, nil
}

// mirrorEdgeLedger copies the fw's edge meta (the 13-key form plus
// the top-level pins) into the meta's edge columns. Best-effort by
// design (the lossless promise needs only stop+read); the columns
// that did not land stay zero/absent and the analysis seats refuse
// honestly on the missing facts.
func mirrorEdgeLedger(ctx context.Context, conn Board, capNum int64, meta *Meta) {
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
		if pins, ok := m["pins"].([]any); ok {
			meta.Pins = nil
			for _, p := range pins {
				if v, ok := anyI(p); ok {
					meta.Pins = append(meta.Pins, int(v))
				}
			}
		}
		lm, _ := m["meta"].(map[string]any)
		if v, ok := anyI(lm["res_hz"]); ok && meta.ResHz == 0 {
			meta.ResHz = int(v)
		}
		if v, ok := anyI(lm["runs_dropped"]); ok {
			meta.RunsDropped = v
		}
		if v, ok := anyI(lm["hw_ovf"]); ok {
			meta.HwOvf = v
		}
		if v, ok := anyI(lm["ring_kb"]); ok {
			meta.RingKB = int(v)
		}
		if v, ok := anyI(lm["max_ms"]); ok {
			meta.MaxMS = int(v)
		}
		if v, ok := anyI(lm["armed_ns"]); ok {
			meta.ArmedNS = v
		}
		if v, ok := anyI(lm["stopped_ns"]); ok {
			meta.StoppedNS = v
		}
		if v, ok := anyI(lm["t0_ns"]); ok {
			meta.T0NS = v
		}
		if b, ok := lm["trig_fired"].(bool); ok {
			meta.TrigFired = b
		}
		if tm, ok := lm["trig"].(map[string]any); ok {
			t := TrigMeta{PreKB: 0}
			if v, ok := anyI(tm["pin"]); ok {
				t.Pin = int(v)
			}
			if k, ok := tm["kind"].(string); ok {
				t.Kind = k
			}
			if v, ok := anyI(tm["level"]); ok {
				lv := int(v)
				t.Level = &lv
			}
			if v, ok := anyI(tm["pre_kb"]); ok {
				t.PreKB = int(v)
			}
			meta.Trig = &t
		} else if _, exists := lm["trig"]; exists {
			meta.Trig = nil // the board's explicit nil (no trigger armed)
		}
		return
	}
}
