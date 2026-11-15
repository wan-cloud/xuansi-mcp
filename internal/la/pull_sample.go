package la

// pull_sample.go — the samples capture's pull (T13, the T12 engine's
// daemon half): the same lossless discipline PullCapture/PullEdge
// ride, one seat over — arm (la.sample_arm), done wait, stop, info
// (the sample ledger), two verified read passes, disarm. The T12
// stream is RECORD-FREE (packed u8, one tick one byte): the identity
// assertion is the byte form's own — landed bytes == ring total ==
// la.stop's bytes, two sha256 passes equal. The meta lands the sample
// columns (rate_hz, samples, t0_ns, the ch/mask/value/width trigger)
// plus the frozen 8-pin DVP bus map (an engine constant the fw's
// la.info does not list — mirrored here once, cited from xs_la.h's
// k_sample_pin_d).

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// sampleRates are the six frozen notches (160MHz integer divisions —
// the T12 spike's verdict); any other rate the fw refuses.
var sampleRates = [6]int{1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000, 40_000_000}

// SampleRateOK answers whether rate is one of the six notches.
func SampleRateOK(rate int) bool {
	for _, r := range sampleRates {
		if r == rate {
			return true
		}
	}
	return false
}

// sampleBusPins is the engine's frozen 8-channel DVP bus (xs_la.h's
// k_sample_pin_d): bit i of every sample byte = this pin's level.
var sampleBusPins = [8]int{11, 9, 8, 10, 12, 18, 17, 16}

// SampleBusPins answers a fresh copy of the frozen bus map (the
// meta's pins column; i = the sample byte's bit i).
func SampleBusPins() []int {
	return append([]int(nil), sampleBusPins[:]...)
}

// SampleTrig is la.sample_arm's trigger: the frozen small language,
// one channel of the eight (ch 0..7 = D0..D7). bits = the bit-window
// match (sample&mask == value&mask for width consecutive samples —
// T12's addition over the edge language).
type SampleTrig struct {
	Ch    int    // 0..7 (= D0..D7)
	Kind  string // "edge_r" | "edge_f" | "level" | "bits"
	Level *int   // 0|1, kind "level" only
	Mask  uint8  // kind "bits" only
	Value uint8  // kind "bits" only
	Width int    // 1..64, kind "bits" only (0 = 1)
	PreKB int    // pre-trigger history KB (< ring_kb; 0 = none)
}

func (t SampleTrig) wire() map[string]any {
	// ch rides only the channel kinds — kind "bits" matches the whole
	// sample byte (mask/value) and names no channel: the fw's arm face
	// accepts bits without ch (its own coach says "ch not used"), and
	// its la.info mirror then answers the no-channel sentinel ch=255.
	m := map[string]any{"kind": t.Kind}
	if t.Kind != "bits" {
		m["ch"] = int64(t.Ch)
	}
	if t.Kind == "level" && t.Level != nil {
		m["level"] = int64(*t.Level)
	}
	if t.Kind == "bits" {
		m["mask"] = int64(t.Mask)
		m["value"] = int64(t.Value)
		if t.Width > 1 {
			m["width"] = int64(t.Width)
		}
	}
	if t.PreKB > 0 {
		m["pre_kb"] = int64(t.PreKB)
	}
	return m
}

// SampleArmArgs is la.sample_arm's parameter set (the fw v9 face is
// the authority; the daemon crosses these verbatim and the board's
// refusal — coach message included — crosses back verbatim).
type SampleArmArgs struct {
	RateHz int // REQUIRED: one of the six notches
	RingKB int // REQUIRED 64..4096 (no fw default)
	MaxMS  int // 0 = the fw default (600000)
	Trig   *SampleTrig
}

func (a SampleArmArgs) wire() map[string]any {
	m := map[string]any{"rate_hz": int64(a.RateHz), "ring_kb": int64(a.RingKB)}
	if a.MaxMS > 0 {
		m["max_ms"] = int64(a.MaxMS)
	}
	if a.Trig != nil {
		m["trig"] = a.Trig.wire()
	}
	return m
}

// PullSample arms one samples capture on the board and pulls it back
// losslessly — the frozen quintet over the T12 engine. The landed
// object carries the sample meta (rate_hz, samples, t0_ns, the
// mirrored trigger); the identity assertion is the byte form's own
// (file == total == stop's bytes; two sha256 passes equal). Any break
// after arm lands the partial object with incomplete:true and returns
// it together with the wrapped error.
func (s *Store) PullSample(ctx context.Context, board PullBoard, args SampleArmArgs) (CaptureRef, error) {
	// 1–2. The "la" source flows to this connection before anything
	// arms (a missed done would hang the wait).
	if _, err := board.Conn.Op(ctx, "events.subscribe", map[string]any{"source": "la"}); err != nil {
		return CaptureRef{}, err
	}
	evCh, cancelEv := subscribeLA(board.Conn)
	defer cancelEv()

	// 3. Arm — the board's refusal (coach message included) crosses
	// verbatim; no object is created for a refused arm.
	res, err := board.Conn.Op(ctx, "la.sample_arm", args.wire())
	if err != nil {
		return CaptureRef{}, err
	}
	capNum, okCap := anyI(res["cap"])
	rateHz, okRate := anyI(res["rate_hz"])
	if !okCap || !okRate {
		return CaptureRef{}, fmt.Errorf("la: la.sample_arm reply missing cap/rate_hz: %v", res)
	}

	// 4. The capture exists board-side: reserve the object dir and the
	// empty data.bin now (every exit path lands something).
	id, dir, err := s.reserveID(board.ID, "sample")
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
	meta := Meta{CapID: id, Board: board.ID, Mode: "samples",
		Pins: SampleBusPins(), RateHz: int(rateHz)}
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

	// 7. Info: the sample ledger mirror (rate_hz, samples, trig with
	// ch/mask/value/width, trig_fired, hw_ovf, ring_kb, max_ms,
	// reason, armed_ns, stopped_ns, t0_ns). Best-effort like the other
	// pulls — but t0_ns is the decode's axis: a capture whose mirror
	// did not land refuses the analysis seats honestly.
	mirrorSampleLedger(ctx, board.Conn, capNum, &meta)

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

	// 9. The byte form's account: one byte per tick, no records —
	// the landed file IS the sample count, and the axis derives.
	meta.Samples = total
	meta.Bytes = total
	if meta.T0NS > 0 {
		meta.TStartNS = uint64(meta.T0NS)
		meta.TEndNS = uint64(meta.T0NS) + uint64(total)*tickOf(int(rateHz))
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
	if total != stopBytes {
		return land(fmt.Errorf("%w: sample accounting: landed %d samples but la.stop reports bytes=%d", ErrChecksum, total, stopBytes))
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

// mirrorSampleLedger copies the fw's sample meta (the 11-key form:
// rate_hz, samples, trig, trig_fired, hw_ovf, ring_kb, max_ms,
// reason, armed_ns, stopped_ns, t0_ns) into the meta's sample
// columns. Best-effort by design; the columns that did not land stay
// zero and the analysis seats refuse honestly on the missing facts.
func mirrorSampleLedger(ctx context.Context, conn Board, capNum int64, meta *Meta) {
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
		if v, ok := anyI(lm["rate_hz"]); ok && meta.RateHz == 0 {
			meta.RateHz = int(v)
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
			if v, ok := anyI(tm["ch"]); ok {
				t.Ch = int(v)
			}
			if k, ok := tm["kind"].(string); ok {
				t.Kind = k
			}
			if v, ok := anyI(tm["level"]); ok {
				lv := int(v)
				t.Level = &lv
			}
			if v, ok := anyI(tm["mask"]); ok {
				t.Mask = uint8(v)
			}
			if v, ok := anyI(tm["value"]); ok {
				t.Value = uint8(v)
			}
			if v, ok := anyI(tm["width"]); ok {
				t.Width = int(v)
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
