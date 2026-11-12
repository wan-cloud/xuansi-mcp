package server

// stream.go — B16-T6 C1: the daemon's board→host stream subscription
// leg (spec §4.1). The fw's v8 face gives per-connection arming
// (stream.arm), a bounded per-channel ring with gap accounting, and a
// coalesced `stream` event source; this file owns the daemon-side
// bookkeeping and the stream_read request:
//
//   - the lease does NOT auto-arm (subscription is an agent's explicit
//     intent — the spec's ruling; unlike the log face's forensic
//     necessity). The FIRST stream_read on a channel arms it; every
//     read drains {frames, next, gap} off the warm controller.
//   - lease release and the lease teardown disarm every armed channel
//     (the explicit end of intent); a controller redial resets the
//     bookkeeping honestly (the board cleared the arms on detach —
//     streamReset rides the tap's identity-change path).
//   - the `stream` event source is subscribed on the controller at the
//     first arm and unsubscribed at full disarm: ring notifications
//     reach the events face (board event, source "stream", data
//     {ch,pending,gap}) — the de-polling wake-up for stream_read.
//   - protocol_ver < 8 boards refuse honestly (the face is a v8
//     overlay; xuansi_frames' single-shot collection semantics are
//     unchanged for every era).

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/replclient"
)

// streamReadTimeout bounds the stream.read op wait (the board's reply is
// bounded by its own budget arithmetic — a full 64KB batch is one reply).
const streamReadTimeout = 12 * time.Second

// streamOutFrame is one drained frame on the wire reply: the §16
// sub-header split open (the consumer's reassembly vocabulary) plus the
// payload base64 (JSON has no binary).
type streamOutFrame struct {
	Ch      byte   `json:"ch"`
	Seq     uint32 `json:"seq"`
	Idx     uint16 `json:"idx"`
	Total   uint16 `json:"total"`
	DataB64 string `json:"data_b64"`
}

// hStreamRead serves stream_read {board, ch, max?}: arms on first sight
// (explicit intent per call site — the lease alone never arms), then
// drains one batch. The reply shape is the board's frozen v8 reply with
// the frames decoded open: {frames:[{ch,seq,idx,total,data_b64}], next,
// gap, armed:[...]}.
func (s *Server) hStreamRead(ctx context.Context, cc *clientConn, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	if req.Ch < 1 || req.Ch > 254 {
		return nil, &RPCError{Class: "server",
			Msg: fmt.Sprintf("stream_read: ch %d out of range 1..254 (the fw's §16 channel domain; 0/255 reserved)", req.Ch)}
	}
	if req.Max < 0 || req.Max > 64 {
		return nil, &RPCError{Class: "server",
			Msg: fmt.Sprintf("stream_read: max %d out of range 1..64 (0 = the board's default batch bound)", req.Max)}
	}
	if err := s.ensureLiveLease(cc, req, b.MAC6); err != nil {
		return nil, err
	}
	// The v8 gate: honest refusal for older protocol eras (the ops do
	// not exist there — unknown-op would be a worse answer than the
	// feature gate).
	if v := s.protocolVer(b.MAC6); v > 0 && v < 8 {
		return nil, &RPCError{Class: "server",
			Msg: fmt.Sprintf("stream_read: board %s reports protocol_ver=%d — the stream subscription face is a v8 overlay (B16-T6); use the frames face of run_lua/submit for single-shot collection", b.MAC6, v)}
	}

	db := s.supervisor(b)
	ctl, err := s.warmController(ctx, b)
	if err != nil {
		return nil, err
	}
	ch := byte(req.Ch)

	// First read on this channel arms it (and subscribes the `stream`
	// source once — the notification wake-up). A redial already reset
	// the bookkeeping (streamReset on the tap's identity change), so a
	// stale arm flag cannot skip the board-side re-arm.
	if !db.streamArmed(ch) {
		if _, err := ctl.Op(ctx, "stream.arm", map[string]any{"ch": int(ch)}); err != nil {
			return nil, err
		}
		if err := db.streamMarkArmed(ch, true); err != nil {
			return nil, err
		}
		s.logf("server: board %s: ch %d armed (stream_read first sight — explicit intent; ring 64KB on-board)", b.MAC6, ch)
		if err := s.streamEnsureSubscribed(ctx, db, ctl); err != nil {
			// honest non-fatal: the read path still works (polling);
			// the notification wake-up is what is missing.
			s.logf("server: board %s: stream source subscribe failed (reads still work; notifications off): %v", b.MAC6, err)
		}
	}

	args := map[string]any{"ch": int(ch)}
	if req.Max > 0 {
		args["max"] = req.Max
	}
	octx, cancel := context.WithTimeout(ctx, streamReadTimeout)
	defer cancel()
	res, err := ctl.Op(octx, "stream.read", args)
	if err != nil {
		return nil, err
	}
	s.noteBoardContact(b.MAC6)

	out := map[string]any{
		"board": b.MAC6,
		"ch":    int(ch),
	}
	if arr, ok := res["frames"].([]any); ok {
		out["frames"] = decodeStreamFrames(arr)
	} else {
		out["frames"] = []streamOutFrame{}
	}
	if v, ok := resU64(res, "next"); ok {
		out["next"] = v
	}
	if v, ok := resU64(res, "gap"); ok {
		out["gap"] = v
	}
	out["armed"] = db.streamArmedList()
	return out, nil
}

// decodeStreamFrames splits each drained bin on its §16 sub-header
// (ch, seq, idx, total) and base64s the payload.
func decodeStreamFrames(arr []any) []streamOutFrame {
	out := make([]streamOutFrame, 0, len(arr))
	for _, el := range arr {
		body, ok := el.([]byte)
		if !ok || len(body) < 9 {
			continue // off-contract element: skipped honestly (counted by the board's own gap/next, not here)
		}
		out = append(out, streamOutFrame{
			Ch:      body[0],
			Seq:     uint32(body[1])<<24 | uint32(body[2])<<16 | uint32(body[3])<<8 | uint32(body[4]),
			Idx:     uint16(body[5])<<8 | uint16(body[6]),
			Total:   uint16(body[7])<<8 | uint16(body[8]),
			DataB64: base64.StdEncoding.EncodeToString(body[9:]),
		})
	}
	return out
}

// hBootReceipts serves boot_receipts {board}: the D1 receipt history
// view (last-8 + the adopted skeleton baseline + the chip cross-check).
func (s *Server) hBootReceipts(ctx context.Context, req *Request) (any, error) {
	b, rerr := s.resolve(req.Board)
	if rerr != nil {
		return nil, rerr
	}
	recs := s.bootRecords().Receipts(b.MAC6)
	chip := ""
	if db, ok := s.lookupSupervisorBoard(b.MAC6); ok && db != nil {
		s.mu.Lock()
		if db.bootSeen != nil {
			chip = db.bootSeen.chip
		}
		s.mu.Unlock()
	}
	// The chip cross-check (spec §4D): the anchor family vs the hello
	// chip word — a mismatch is itself an anomaly signal, surfaced per
	// receipt (absent chip = pre-first-hello, honestly unknown).
	out := make([]map[string]any, 0, len(recs))
	for _, r := range recs {
		m := map[string]any{
			"at":         r.At,
			"kind":       r.Kind,
			"tier":       r.Tier,
			"arbiter":    r.Arbiter,
			"skeleton_n": r.SkeletonN,
		}
		if r.ResetRaw != "" {
			m["reset_raw"] = r.ResetRaw
		}
		if r.RomVer != "" {
			m["rom_ver"] = r.RomVer
		}
		if r.Family != "" {
			m["family"] = r.Family
		}
		if r.ChipRev != "" {
			m["chip_rev"] = r.ChipRev
		}
		if r.Notes != "" {
			m["notes"] = r.Notes
		}
		if r.DriftN > 0 {
			m["drift_n"] = r.DriftN
		}
		// The triad's middle and baseline legs at the seat (fix-round
		// Important-1): the machine diff lines and the baseline text
		// they were taken against — absent when there was nothing to
		// compare (a first capture) or nothing drifted, honestly.
		if len(r.Diff) > 0 {
			d := make([]string, len(r.Diff))
			copy(d, r.Diff)
			m["diff"] = d
		}
		if len(r.Baseline) > 0 {
			b := make([]string, len(r.Baseline))
			copy(b, r.Baseline)
			m["baseline"] = b
		}
		if r.JunkText != "" {
			m["junk_text"] = r.JunkText
		}
		if r.Family != "" && chip != "" {
			m["chip_match"] = ChipMatch(r.Family, chip)
		}
		out = append(out, m)
	}
	sk := s.bootRecords().Skeleton(b.MAC6)
	view := map[string]any{
		"board":          b.MAC6,
		"receipts":       out,
		"skeleton_lines": len(sk),
	}
	// The adopted baseline's own text (the "what this board usually
	// looks like" leg — bounded by the same persistence bound).
	if len(sk) > 0 {
		if len(sk) > baselineBound {
			sk = sk[:baselineBound]
		}
		view["skeleton"] = sk
	}
	return view, nil
}

// ---------------------------------------------------------------------------
// bookkeeping (daemonBoard side)

func (db *daemonBoard) streamArmed(ch byte) bool {
	db.streamMu.Lock()
	defer db.streamMu.Unlock()
	return db.streamArms[ch]
}

func (db *daemonBoard) streamMarkArmed(ch byte, on bool) error {
	db.streamMu.Lock()
	defer db.streamMu.Unlock()
	if db.streamArms == nil {
		db.streamArms = map[byte]bool{}
	}
	if on {
		db.streamArms[ch] = true
	} else {
		delete(db.streamArms, ch)
	}
	return nil
}

func (db *daemonBoard) streamArmedList() []int {
	db.streamMu.Lock()
	defer db.streamMu.Unlock()
	out := make([]int, 0, len(db.streamArms))
	for ch := range db.streamArms {
		out = append(out, int(ch))
	}
	// sorted for a stable seat
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// streamReset drops the arm bookkeeping on a controller identity change
// (redial/drift): the board cleared the arms on the old link's detach —
// the next stream_read re-arms honestly. Called from the tap's
// identity-change path (same place jobReset rides).
func (db *daemonBoard) streamReset() {
	db.streamMu.Lock()
	defer db.streamMu.Unlock()
	db.streamArms = nil
	db.streamSub = false
}

// streamEnsureSubscribed subscribes the controller to the `stream`
// event source once (idempotent per controller generation).
func (s *Server) streamEnsureSubscribed(ctx context.Context, db *daemonBoard, ctl board.Controller) error {
	db.streamMu.Lock()
	need := !db.streamSub
	db.streamMu.Unlock()
	if !need {
		return nil
	}
	if _, err := ctl.Op(ctx, "events.subscribe", map[string]any{"source": "stream"}); err != nil {
		return err
	}
	db.streamMu.Lock()
	db.streamSub = true
	db.streamMu.Unlock()
	return nil
}

// streamDisarmAll disarms every armed channel + unsubscribes the source
// (the lease's end-of-intent path: release and leaseTeardown). Best
// effort per channel — a dead board keeps its own detach semantics, and
// the bookkeeping clears regardless (the next lease re-arms on demand).
func (s *Server) streamDisarmAll(ctx context.Context, mac6 string, why string) {
	b, ok := s.lookupExact(mac6)
	if !ok {
		return
	}
	db := s.supervisor(b)
	arms := db.streamArmedList()
	db.streamReset()
	if len(arms) == 0 {
		return
	}
	ctl, err := s.warmController(ctx, b)
	if err != nil {
		s.logf("server: board %s: stream disarm skipped (%s) — no warm controller: %v", mac6, why, err)
		return
	}
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, ch := range arms {
		if _, err := ctl.Op(tctx, "stream.disarm", map[string]any{"ch": ch}); err != nil {
			s.logf("server: board %s: stream.disarm ch %d failed (%s): %v", mac6, ch, why, err)
		}
	}
	if _, err := ctl.Op(tctx, "events.unsubscribe", map[string]any{"source": "stream"}); err != nil {
		s.logf("server: board %s: stream source unsubscribe failed (%s): %v", mac6, why, err)
	}
	s.logf("server: board %s: stream disarmed %v (%s — intent ended with the lease)", mac6, arms, why)
}

// ---------------------------------------------------------------------------
// D1: the CDC-open banner capture → classification → receipt

// bootCaptured is the DialOpts.BootCapture closure for one board: the
// drain window's tail IS the reboot's banner (zero false positives by
// construction); stash it for epoch arbitration at the tap's hello.
// Runs on the dialing goroutine (inside ConnectUSB under connectMu) —
// no board calls, no connectMu re-entry.
func (s *Server) bootCaptured(mac6 string) func([]byte) {
	return func(raw []byte) {
		b, ok := s.lookupExact(mac6)
		if !ok {
			return
		}
		db := s.supervisor(b)
		s.mu.Lock()
		db.pendingBanner = raw
		s.mu.Unlock()
	}
}

// resolvePendingBanner runs at the tap's hello (the post-open sighting):
// the epoch backstop arbitrates the stashed capture — +N confirms the
// open's reboot, unchanged honestly downgrades to corruption — then the
// classifier's verdict lands in the durable record and the log.
func (s *Server) resolvePendingBanner(mac6 string, db *daemonBoard, prev *bootSight, h *replclient.HelloInfo) {
	s.mu.Lock()
	raw := db.pendingBanner
	db.pendingBanner = nil
	s.mu.Unlock()
	if raw == nil {
		return
	}
	var prevEpoch uint32
	noBase := prev == nil
	if prev != nil {
		prevEpoch = prev.epoch
	}
	arbiter := ArbitrateEpoch(prevEpoch, h.BootEpoch, noBase)
	if err := s.bootRecords().Record(mac6, raw, arbiter); err != nil {
		s.logf("server: board %s: boot receipt persist failed: %v", mac6, err)
	}
	recs := s.bootRecords().Receipts(mac6)
	if len(recs) > 0 {
		r := recs[len(recs)-1]
		note := ""
		if r.Family != "" && h.Chip != "" && !ChipMatch(r.Family, h.Chip) {
			note = fmt.Sprintf(" CHIP-MISMATCH(family=%s hello=%s)", r.Family, h.Chip)
		}
		s.logf("server: board %s boot banner classified: kind=%s tier=%s arbiter=%s reset_raw=%s rom=%s rev=%s%s",
			mac6, r.Kind, r.Tier, arbiter, r.ResetRaw, r.RomVer, r.ChipRev, note)
	}
}

// protocolVer answers the board's last-sighted protocol era (0 unknown
// — pre-first-hello; the honest gate only refuses KNOWN-old eras).
func (s *Server) protocolVer(mac6 string) int64 {
	if db, ok := s.lookupSupervisorBoard(mac6); ok {
		s.mu.Lock()
		seen := db.bootSeen
		s.mu.Unlock()
		if seen != nil {
			return seen.protoVer
		}
	}
	return 0
}

// lookupSupervisorBoard returns the daemonBoard without creating one
// (nil, false when the board has never been crossed).
func (s *Server) lookupSupervisorBoard(mac6 string) (*daemonBoard, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, ok := s.sups[mac6]
	return db, ok
}
