package board

// frames_test.go — the generic 0x02 frame collector's unit face (B12-P2
// T5): the T2 reassembler bookkeeping generalized from camera.go
// (seq/idx/total, one-loss abandonments, tombstones) with TOLERANT
// per-idx buffering — out-of-cursor-order chunks still complete a
// frame, an incomplete sequence counts lost at window close; the fw's
// ch=2 stream_send host stamp ([u8 name_len][name][data] on EVERY
// chunk — xs_wasm.c); the §5.6 bounds (≤16 frames / ≤1 MiB cumulative,
// honest drop counts, still-open tails lost at snapshot); and the hub
// that fans one connection's single sink slot out to concurrent
// consumers.

import (
	"bytes"
	"testing"

	"xuansi/internal/replclient"
)

// stampChunk builds one ch=2 module chunk exactly the way the fw's
// stream_send host-stamp writes it (xs_wasm.c n_stream_send): every
// chunk of a sequence carries [u8 name_len][name][data part].
func stampChunk(name string, part []byte) []byte {
	out := make([]byte, 0, 1+len(name)+len(part))
	out = append(out, byte(len(name)))
	out = append(out, name...)
	return append(out, part...)
}

// TestFrameCollectorStampedSingleChunk: one complete single-chunk ch=2
// frame lands with the host-stamped module name parsed off the payload
// and the payload bytes verbatim.
func TestFrameCollectorStampedSingleChunk(t *testing.T) {
	c := NewFrameCollector()
	jpeg := []byte{0xFF, 0xD8, 1, 2, 3, 0xFF, 0xD9}
	c.Sink(2, 1, 0, 1, stampChunk("jpge", jpeg))
	snap := c.Snapshot()
	if len(snap.Frames) != 1 {
		t.Fatalf("frames = %d, want 1 (dropped %d)", len(snap.Frames), snap.Dropped)
	}
	f := snap.Frames[0]
	if f.Ch != 2 || f.Name != "jpge" || !bytes.Equal(f.Data, jpeg) {
		t.Fatalf("frame = {ch:%d name:%q data:%x}, want {2, jpge, %x}", f.Ch, f.Name, f.Data, jpeg)
	}
	if snap.Dropped != 0 {
		t.Fatalf("dropped = %d, want 0", snap.Dropped)
	}
}

// TestFrameCollectorChunkedOutOfOrder: a frame split across chunks whose
// idx order scrambles (1, 0, 2) still reassembles as ONE frame with the
// parts concatenated in idx order — the T2 reassembler semantics.
func TestFrameCollectorChunkedOutOfOrder(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 7, 1, 3, stampChunk("jpge", []byte("B")))
	c.Sink(2, 7, 0, 3, stampChunk("jpge", []byte("A")))
	c.Sink(2, 7, 2, 3, stampChunk("jpge", []byte("C")))
	snap := c.Snapshot()
	if len(snap.Frames) != 1 || snap.Dropped != 0 {
		t.Fatalf("snap = %+v, want one frame no drops", snap)
	}
	f := snap.Frames[0]
	if f.Name != "jpge" || !bytes.Equal(f.Data, []byte("ABC")) {
		t.Fatalf("frame = {name:%q data:%q}, want jpge/ABC", f.Name, f.Data)
	}
}

// TestFrameCollectorIncompleteSequenceLostAtClose: a sequence missing a
// chunk never completes — at window close it counts as exactly one
// loss (the tolerant buffer holds it open, the close is the verdict);
// a fully-arriving out-of-order set instead completes (see below).
func TestFrameCollectorIncompleteSequenceLostAtClose(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 1, 0, 3, stampChunk("jpge", []byte("A")))
	c.Sink(2, 1, 2, 3, stampChunk("jpge", []byte("C"))) // 1 never arrives
	snap := c.Snapshot()
	if len(snap.Frames) != 0 {
		t.Fatalf("frames = %d, want 0", len(snap.Frames))
	}
	if snap.Dropped != 1 {
		t.Fatalf("dropped = %d, want exactly 1", snap.Dropped)
	}
}

// TestFrameCollectorLateGapFillCompletes: the tolerant shape — chunks
// arriving out of cursor order (1, 0, 2) reassemble into ONE frame in
// idx order; a duplicate idx is idempotent.
func TestFrameCollectorLateGapFillCompletes(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 7, 1, 3, stampChunk("jpge", []byte("B")))
	c.Sink(2, 7, 0, 3, stampChunk("jpge", []byte("A")))
	c.Sink(2, 7, 0, 3, stampChunk("jpge", []byte("A"))) // duplicate: no-op
	c.Sink(2, 7, 2, 3, stampChunk("jpge", []byte("C")))
	snap := c.Snapshot()
	if len(snap.Frames) != 1 || snap.Dropped != 0 {
		t.Fatalf("snap = %+v, want one frame no drops", snap)
	}
	f := snap.Frames[0]
	if f.Name != "jpge" || !bytes.Equal(f.Data, []byte("ABC")) {
		t.Fatalf("frame = {name:%q data:%q}, want jpge/ABC", f.Name, f.Data)
	}
}

// TestFrameCollectorSixteenFrameBound: the 16-frame bound truncates and
// counts every further completed frame as dropped (§5.6).
func TestFrameCollectorSixteenFrameBound(t *testing.T) {
	c := NewFrameCollector()
	for i := 0; i < 17; i++ {
		c.Sink(2, uint32(i+1), 0, 1, stampChunk("jpge", []byte{byte(i)}))
	}
	snap := c.Snapshot()
	if len(snap.Frames) != FramesMax {
		t.Fatalf("frames = %d, want %d", len(snap.Frames), FramesMax)
	}
	if snap.Dropped != 1 {
		t.Fatalf("dropped = %d, want 1 (the 17th)", snap.Dropped)
	}
	if f := snap.Frames[16-1]; f.Data[0] != 15 {
		t.Fatalf("last kept frame = %d, want the 16th (payload 15)", f.Data[0])
	}
}

// TestFrameCollectorByteBound: the 1 MiB cumulative payload bound keeps
// the frames that fit and drops the one that would overflow —
// first-come-first-kept truncation with an honest count.
func TestFrameCollectorByteBound(t *testing.T) {
	c := NewFrameCollector()
	first := bytes.Repeat([]byte{1}, 600*1024)
	second := bytes.Repeat([]byte{2}, 600*1024)
	c.Sink(2, 1, 0, 1, stampChunk("jpge", first))
	c.Sink(2, 2, 0, 1, stampChunk("jpge", second))
	snap := c.Snapshot()
	if len(snap.Frames) != 1 {
		t.Fatalf("frames = %d, want 1 (the first)", len(snap.Frames))
	}
	if !bytes.Equal(snap.Frames[0].Data, first) {
		t.Fatalf("kept frame is not the first 600K payload")
	}
	if snap.Dropped != 1 {
		t.Fatalf("dropped = %d, want 1 (the overflow)", snap.Dropped)
	}
}

// TestFrameCollectorOversizeFrameDropped: a single frame larger than the
// whole byte bound is dropped (abandoned mid-assembly once its bucket
// can no longer fit), never buffered whole.
func TestFrameCollectorOversizeFrameDropped(t *testing.T) {
	c := NewFrameCollector()
	part := bytes.Repeat([]byte{7}, 600*1024)
	c.Sink(2, 1, 0, 3, stampChunk("jpge", part))
	c.Sink(2, 1, 1, 3, stampChunk("jpge", part)) // bucket crosses 1 MiB: abandon
	snap := c.Snapshot()
	if len(snap.Frames) != 0 || snap.Dropped != 1 {
		t.Fatalf("snap = %+v, want no frames and exactly one drop", snap)
	}
}

// TestFrameCollectorUnstampedChannel: channels other than the module
// channel carry no host stamp — the payload crosses verbatim with an
// empty name (diag.streamtest's ch=254 shape).
func TestFrameCollectorUnstampedChannel(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(254, 3, 0, 1, []byte("raw-bytes"))
	snap := c.Snapshot()
	if len(snap.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(snap.Frames))
	}
	f := snap.Frames[0]
	if f.Ch != 254 || f.Name != "" || !bytes.Equal(f.Data, []byte("raw-bytes")) {
		t.Fatalf("frame = {ch:%d name:%q data:%q}, want {254, \"\", raw-bytes}", f.Ch, f.Name, f.Data)
	}
}

// TestFrameCollectorMalformedStampAbandons: a ch=2 chunk whose stamp
// header claims more name bytes than the chunk carries cannot come from
// the fw's emitter — abandon the sequence once, honestly counted.
func TestFrameCollectorMalformedStampAbandons(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 1, 0, 2, []byte{5, 'a', 'b'})             // claims a 5-byte name, carries 2
	c.Sink(2, 1, 1, 2, stampChunk("jpge", []byte("x"))) // follower of a dead seq
	snap := c.Snapshot()
	if len(snap.Frames) != 0 || snap.Dropped != 1 {
		t.Fatalf("snap = %+v, want no frames and exactly one drop", snap)
	}
}

// TestFrameCollectorTotalDisagreementAbandons: a chunk whose total
// disagrees with the sequence's established total abandons it (the
// emitter contradicted itself — camera.go's rule).
func TestFrameCollectorTotalDisagreementAbandons(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 1, 0, 2, stampChunk("jpge", []byte("A")))
	c.Sink(2, 1, 1, 3, stampChunk("jpge", []byte("B")))
	snap := c.Snapshot()
	if len(snap.Frames) != 0 || snap.Dropped != 1 {
		t.Fatalf("snap = %+v, want no frames and exactly one drop", snap)
	}
}

// TestFrameCollectorOutOfRangeIdxAbandons (T5 review minor ①): the one
// emitter-contract defense the completion path lacked — a chunk whose
// idx is at or beyond the sequence's total can never be part of the
// frame (its bucket would otherwise satisfy the have==total completion
// check with an in-range chunk still missing, assembling a holey
// frame). Both arrival shapes abandon exactly once: out-of-range on a
// FRESH bucket and on an in-flight one (ch=1: unstamped, so the
// idx/total contract is the only thing under test).
func TestFrameCollectorOutOfRangeIdxAbandons(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(1, 1, 1, 1, []byte("x")) // fresh bucket: first chunk already idx ≥ total
	c.Sink(1, 2, 0, 2, []byte("a")) // in-flight bucket: idx 0 of 2 buffers fine
	c.Sink(1, 2, 2, 2, []byte("c")) // then idx 2 arrives — beyond total 2
	snap := c.Drain()
	if len(snap.Frames) != 0 || snap.Dropped != 2 {
		t.Fatalf("snap = %+v, want no frames and two drops (each out-of-range sequence abandoned exactly once)", snap)
	}
}

// TestFrameCollectorSnapshotCountsOpenTails: a still-open sequence at
// snapshot time can no longer complete (on the ordered stream its
// remaining chunks would have preceded the reply) — counted lost.
func TestFrameCollectorSnapshotCountsOpenTails(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 1, 0, 2, stampChunk("jpge", []byte("A")))
	snap := c.Snapshot()
	if len(snap.Frames) != 0 || snap.Dropped != 1 {
		t.Fatalf("snap = %+v, want the open tail counted lost", snap)
	}
	// snapshot is read-only: a second look sees the same state.
	if snap2 := c.Snapshot(); snap2.Dropped != 1 || len(snap2.Frames) != 0 {
		t.Fatalf("second snapshot = %+v, want the same state", snap2)
	}
}

// TestFrameCollectorDrainResets: drain hands the window's frames over
// and empties the collector — the next window starts clean (the
// pending-job buffer's per-event shape).
func TestFrameCollectorDrainResets(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 1, 0, 1, stampChunk("jpge", []byte("one")))
	snap := c.Drain()
	if len(snap.Frames) != 1 || string(snap.Frames[0].Data) != "one" {
		t.Fatalf("drain = %+v, want the one frame", snap)
	}
	c.Sink(2, 2, 0, 1, stampChunk("jpge", []byte("two")))
	snap = c.Drain()
	if len(snap.Frames) != 1 || string(snap.Frames[0].Data) != "two" {
		t.Fatalf("drain = %+v, want only the second window's frame", snap)
	}
}

// TestFrameCollectorResetBetweenAttempts: reset empties buckets and
// counts — the redial-retry shape (a re-executed op must not graft the
// dead attempt's partial sequences onto the retry's frames).
func TestFrameCollectorResetBetweenAttempts(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 1, 0, 3, stampChunk("jpge", []byte("A"))) // attempt 1 partial
	c.Reset()
	c.Sink(2, 1, 0, 3, stampChunk("jpge", []byte("X"))) // attempt 2, same seq
	c.Sink(2, 1, 1, 3, stampChunk("jpge", []byte("Y")))
	c.Sink(2, 1, 2, 3, stampChunk("jpge", []byte("Z")))
	snap := c.Snapshot()
	if len(snap.Frames) != 1 || snap.Dropped != 0 || !bytes.Equal(snap.Frames[0].Data, []byte("XYZ")) {
		t.Fatalf("snap = %+v, want exactly XYZ reassembled clean", snap)
	}
}

// TestFrameCollectorCompletedSeqTombstonesStraggler (final-review
// minor): completing a sequence leaves its seq tombstoned, the retired
// camera collector's rule — a straggler chunk re-delivered after
// completion must find the dead bucket and drop silently. Without the
// tombstone a full re-delivery founds a fresh bucket that completes
// AGAIN (a duplicate frame), and a partial one an open tail that
// counts lost at window close.
func TestFrameCollectorCompletedSeqTombstonesStraggler(t *testing.T) {
	c := NewFrameCollector()
	c.Sink(2, 1, 0, 1, stampChunk("jpge", []byte("one"))) // completes
	c.Sink(2, 1, 0, 1, stampChunk("jpge", []byte("one"))) // straggler: full re-delivery
	c.Sink(2, 2, 0, 2, stampChunk("jpge", []byte("A")))   // completes (chunked frame)
	c.Sink(2, 2, 1, 2, stampChunk("jpge", []byte("B")))
	c.Sink(2, 2, 0, 2, stampChunk("jpge", []byte("A")))   // straggler: partial re-delivery
	c.Sink(2, 3, 0, 1, stampChunk("jpge", []byte("two"))) // a later frame is unaffected
	snap := c.Drain()
	if len(snap.Frames) != 3 || snap.Dropped != 0 {
		t.Fatalf("snap = %+v, want exactly three frames (one, AB, two) and zero drops", snap)
	}
	if d := string(snap.Frames[0].Data) + string(snap.Frames[1].Data) + string(snap.Frames[2].Data); d != "oneABtwo" {
		t.Fatalf("frames = %q, want oneABtwo in completion order", d)
	}
}

// TestFrameHubFansOutAndRemoves: the hub fans one connection's chunks
// out to every registered consumer; a removed consumer stops receiving;
// the hub tolerates a nil-ish empty registration set.
func TestFrameHubFansOutAndRemoves(t *testing.T) {
	h := NewFrameHub()
	var a, b []byte
	removeA := h.Add(func(ch byte, seq uint32, idx, total uint16, payload []byte) {
		a = append(a, payload...)
	})
	h.Add(func(ch byte, seq uint32, idx, total uint16, payload []byte) {
		b = append(b, payload...)
	})
	h.Sink(2, 1, 0, 1, stampChunk("jpge", []byte("one")))
	removeA()
	h.Sink(2, 2, 0, 1, stampChunk("jpge", []byte("two")))
	if string(a) != string(stampChunk("jpge", []byte("one"))) {
		t.Fatalf("consumer A saw %q, want only the first stamped chunk (removed before two)", a)
	}
	if string(b) != string(stampChunk("jpge", []byte("one")))+string(stampChunk("jpge", []byte("two"))) {
		t.Fatalf("consumer B saw %q, want both stamped chunks", b)
	}
}

// compile-time: the hub's Sink is a StreamSink (what a controller's
// OnStream slot takes).
var _ replclient.StreamSink = (*FrameHub)(nil).Sink
var _ replclient.StreamSink = (*FrameCollector)(nil).Sink
