package la

// pull_stream.go — the wave C live sliding-window pulls (listen/edge):
// the same lossless discipline as the post-stop quintet, reshaped
// around a LIVE read loop — arm, stream 32640B windows off the live
// ring at the measured p90 budget (the board advances a consumed
// cursor and recycles the ring behind it; a reader a full ring behind
// ends the capture reason "full" with the ring closing the account —
// no silent loss), then on the done event (or the capture's own
// full/limit ending): stop, the TAIL read of what the loop never saw,
// the whole-file parse + stop-bytes identity, and the tail's two-pass
// re-read hash. The landed object is the SAME capture shape; meta
// gains the honest "streamed" mark: its [0, tail) prefix crossed live
// with no ring-side second source (the ring recycled it by design) —
// the prefix's integrity account is the live reads themselves plus
// the whole-stream parse below.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"time"

	"xuansi/internal/replclient"
)

// streamPollIdle is the caught-up live loop's re-poll cadence: waveS
// measured the pull pipeline at a 10-15ms fixed op RTT with p90 ≈
// 1.5-2× p50 (131-234ms at the 32640 window on the boards tried) — a
// caught-up reader waits one p90 budget before polling again instead
// of spinning the link; a writer that pulls ahead surfaces on the
// very next poll.
const streamPollIdle = 200 * time.Millisecond

// streamLive runs the live window loop from off 0 until the done
// event for OUR cap, ctx end, or link death. Each la.read serves
// [off, published) clamped to ReadWindow — eof means caught-up (NOT
// stopped); the loop then waits one poll budget or the done event.
// Windows hand to sink in stream order; the answer is the consumed
// cursor (the live-assembled prefix length). Progress and foreign-cap
// events flow past; the done that ends the loop was queued the moment
// it landed (subscribeLA registered before arm).
func streamLive(ctx context.Context, conn Board, evCh <-chan replclient.Event, capNum int64, sink func(off int64, data []byte) error) (int64, error) {
	var cursor int64
	tick := time.NewTicker(streamPollIdle)
	defer tick.Stop()
	done := func(d map[string]any) bool {
		if d["event"] != "done" {
			return false
		}
		if c, ok := anyI(d["cap"]); ok && c == capNum {
			return true
		}
		return false
	}
	drainDone := func() bool {
		for {
			select {
			case ev := <-evCh:
				if done(ev.Data) {
					return true
				}
			default:
				return false
			}
		}
	}
	for {
		if drainDone() {
			return cursor, nil
		}
		res, err := conn.Op(ctx, "la.read", map[string]any{
			"cap": capNum, "off": cursor, "len": int64(ReadWindow)})
		if err != nil {
			return cursor, err
		}
		data, _ := res["data"].([]byte)
		if _, ok := anyU64(res["total"]); !ok {
			return cursor, fmt.Errorf("la.read reply missing total")
		}
		eof, _ := res["eof"].(bool)
		if len(data) > 0 {
			if err := sink(cursor, data); err != nil {
				return cursor, err
			}
			cursor += int64(len(data))
		}
		if !eof {
			continue // the writer is ahead: read the next window now
		}
		// caught up: wait the done event or one poll budget
		select {
		case ev := <-evCh:
			if done(ev.Data) {
				return cursor, nil
			}
		case <-ctx.Done():
			return cursor, ctx.Err()
		case <-tick.C:
			if conn.Dead() {
				return cursor, fmt.Errorf("link died while streaming")
			}
		}
	}
}

// streamPull is the wave C core both mode wrappers ride: the quintet
// reshaped — arm → streamLive → stop → mirror → tail readAllFrom →
// flush → finish (the mode's whole-file parse + stop-bytes identity) →
// the tail's two-pass hash (re-read [tail, total) must equal the
// landed file's tail) → disarm → land. Any break after arm lands the
// incomplete object exactly like the post-stop pulls.
func (s *Store) streamPull(ctx context.Context, board PullBoard,
	armOp string, wire map[string]any, mode string,
	newMeta func(armRes map[string]any) (Meta, error),
	mirror func(ctx context.Context, conn Board, capNum int64, meta *Meta),
	finish func(meta *Meta, dataPath string, total, stopBytes int64) error) (CaptureRef, error) {

	// 1-2. The "la" source flows before anything arms.
	if _, err := board.Conn.Op(ctx, "events.subscribe", map[string]any{"source": "la"}); err != nil {
		return CaptureRef{}, err
	}
	evCh, cancelEv := subscribeLA(board.Conn)
	defer cancelEv()

	// 3. Arm — the board's refusal (coach message included) crosses
	// verbatim; no object is created for a refused arm.
	res, err := board.Conn.Op(ctx, armOp, wire)
	if err != nil {
		return CaptureRef{}, err
	}
	capNum, okCap := anyI(res["cap"])
	if !okCap {
		return CaptureRef{}, fmt.Errorf("la: %s reply missing cap: %v", armOp, res)
	}

	// 4. The capture exists board-side: reserve the object dir now.
	id, dir, err := s.reserveID(board.ID, mode)
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
	meta, merr := newMeta(res)
	if merr != nil {
		endOfIntent(board.Conn, capNum)
		return CaptureRef{}, merr
	}
	land := func(cause error) (CaptureRef, error) {
		bw.Flush()
		file.Close()
		return s.landIncomplete(ref, meta, board.Conn, capNum, cause)
	}

	// 5. The LIVE loop — windows land as they publish (the p90 budget
	// cadence); the done event (full/limit/self-stop) ends it. pass1
	// hashes the whole stream live bytes AND tail bytes alike.
	pass1 := sha256.New()
	sink := func(_ int64, data []byte) error {
		pass1.Write(data)
		_, werr := bw.Write(data)
		return werr
	}
	cursor, lerr := streamLive(ctx, board.Conn, evCh, capNum, sink)
	if lerr != nil {
		return land(fmt.Errorf("%w: live loop: %w", ErrIncomplete, lerr))
	}

	// 6. Stop: the idempotent, authoritative ledger (a capture that
	// self-ended echoes its reason — "full" here is the ring-closing
	// fallback the live reader fell behind by).
	res, err = board.Conn.Op(ctx, "la.stop", map[string]any{"cap": capNum})
	if err != nil {
		return land(fmt.Errorf("%w: la.stop: %w", ErrIncomplete, err))
	}
	stopBytes, _ := anyI(res["bytes"])
	meta.Reason, _ = res["reason"].(string)

	// 7. Info: the ledger mirror (mode's own columns).
	mirror(ctx, board.Conn, capNum, &meta)

	// 8. The TAIL — the stopped ring serves [cursor, total); the
	// assembled object = live prefix + tail = the whole stream.
	total, terr := readAllFrom(ctx, board.Conn, capNum, cursor, sink)
	if terr != nil {
		return land(fmt.Errorf("%w: tail read: %w", ErrIncomplete, terr))
	}
	if err := bw.Flush(); err != nil {
		return land(fmt.Errorf("%w: flush: %w", ErrIncomplete, err))
	}
	if err := file.Close(); err != nil {
		return land(fmt.Errorf("%w: close: %w", ErrIncomplete, err))
	}
	meta.StreamBytes = total

	// 9. The mode's whole-file parse + identity vs la.stop's bytes
	// (listen: parsed Σn == bytes; edge: runs×9 == total == bytes).
	if ferr := finish(&meta, ref.DataPath, total, stopBytes); ferr != nil {
		return land(ferr)
	}

	// 10. The tail's two-pass identity: a re-read of [cursor, total)
	// over the static ring must hash equal to the landed file's tail.
	// The [0, cursor) prefix has no second source (recycled by design)
	// — its account is the live reads plus the parse above; meta says
	// so honestly (streamed:true).
	tailHash := sha256.New()
	if _, err := readAllFrom(ctx, board.Conn, capNum, cursor, func(_ int64, data []byte) error {
		tailHash.Write(data)
		return nil
	}); err != nil {
		return land(fmt.Errorf("%w: verify pass: %w", ErrIncomplete, err))
	}
	if serr := verifyFileTail(ref.DataPath, cursor, total, tailHash); serr != nil {
		return land(fmt.Errorf("%w: %v", ErrChecksum, serr))
	}
	meta.SHA256 = hex.EncodeToString(pass1.Sum(nil))
	meta.Streamed = true

	// 11. Disarm — the data is verified; a disarm failure fails the
	// call without tainting the object (the post-stop pulls' rule).
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

// verifyFileTail hashes data.bin's [tailStart, total) bytes and checks
// them against want (the ring re-read's hash); it also pins the file
// length to total — the assembled-vs-ledger account (live prefix +
// ring tail == the stream total, no byte unaccounted).
func verifyFileTail(path string, tailStart, total int64, want hash.Hash) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if int64(len(raw)) != total {
		return fmt.Errorf("assembled account: data.bin is %d bytes but the stream total is %d — the live prefix + ring tail do not add up", len(raw), total)
	}
	h := sha256.New()
	h.Write(raw[tailStart:])
	if hex.EncodeToString(h.Sum(nil)) != hex.EncodeToString(want.Sum(nil)) {
		return fmt.Errorf("tail identity: the ring re-read hash does not match the landed tail (%d bytes from off %d)", total-tailStart, tailStart)
	}
	return nil
}

// PullCaptureStream arms one LISTEN capture and pulls it back live —
// the wave C sliding window over PullCapture's discipline. The landed
// object is the same capture shape (chunk stream parseable whole,
// parsed Σn == la.stop's bytes), with meta.streamed marking the live
// prefix and its honest verification story.
func (s *Store) PullCaptureStream(ctx context.Context, board PullBoard, arm ArmArgs) (CaptureRef, error) {
	return s.streamPull(ctx, board, "la.listen_arm", arm.wire(), "listen",
		func(res map[string]any) (Meta, error) {
			baud, ok := anyI(res["baud"])
			if !ok {
				return Meta{}, fmt.Errorf("la: la.listen_arm reply missing baud: %v", res)
			}
			return Meta{Board: board.ID, Mode: "listen",
				Port: arm.Port, Pin: arm.Pin, Baud: int(baud)}, nil
		},
		mirrorLedger,
		func(meta *Meta, dataPath string, total, stopBytes int64) error {
			stats, perr := parseStats(dataPath, false)
			if perr != nil {
				return fmt.Errorf("%w: landed stream does not parse: %v", ErrChecksum, perr)
			}
			meta.TStartNS, meta.TEndNS = stats.TStartNS, stats.TEndNS
			meta.Bytes, meta.Chunks = stats.Payload, stats.Chunks
			if stats.Payload != stopBytes {
				return fmt.Errorf("%w: payload accounting: parsed Σn=%d but la.stop reports bytes=%d", ErrChecksum, stats.Payload, stopBytes)
			}
			return nil
		})
}

// PullEdgeStream arms one EDGE capture (triggerless — the server face
// refuses stream+trig with the coach) and pulls it back live. The
// identity assertion is the run form's own: the assembled stream
// parses whole, runs×9 == total == la.stop's bytes.
func (s *Store) PullEdgeStream(ctx context.Context, board PullBoard, arm EdgeArmArgs) (CaptureRef, error) {
	return s.streamPull(ctx, board, "la.edge_arm", arm.wire(), "edge",
		func(res map[string]any) (Meta, error) {
			resHz, ok := anyI(res["res_hz"])
			if !ok {
				return Meta{}, fmt.Errorf("la: la.edge_arm reply missing res_hz: %v", res)
			}
			return Meta{Board: board.ID, Mode: "edge", ResHz: int(resHz)}, nil
		},
		mirrorEdgeLedger,
		func(meta *Meta, dataPath string, total, stopBytes int64) error {
			stats, perr := parseRunStats(dataPath, false)
			if perr != nil {
				return fmt.Errorf("%w: landed stream does not parse: %v", ErrChecksum, perr)
			}
			meta.Runs = int(stats.Runs)
			meta.Bytes = stats.Stream
			if meta.T0NS > 0 {
				meta.TStartNS = uint64(meta.T0NS)
				meta.TEndNS = uint64(meta.T0NS) + stats.SpanNS
			}
			if stats.Stream != total || stats.Stream != stopBytes {
				return fmt.Errorf("%w: run accounting: parsed %d runs ×9=%d, stream total %d, la.stop bytes=%d — the invariant runs×9==total==bytes broke", ErrChecksum, stats.Runs, stats.Stream, total, stopBytes)
			}
			return nil
		})
}
