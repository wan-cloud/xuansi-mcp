package replclient

// stream_test.go pins the 0x02 data-frame path inside this package:
// the wire layout byte for byte, the frameReader's tolerance of data
// frames under split reads and len==0 neighbors, the reassembler's
// per-(ch,seq) assembly with gap invalidation, and the full client
// path with the mock board scripting interleaved ctrl/data bursts and
// gapped chunk sequences.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

// dataFrame builds one 0x02 wire frame: [u32 BE len][0x02][ch][u32 BE
// seq][u16 BE idx][u16 BE total][payload], len counting the type byte,
// the 9-byte sub-header and the payload — the layout the firmware
// emits.
func dataFrame(ch byte, seq uint32, idx, total uint16, payload []byte) []byte {
	f := make([]byte, headerLen+streamHeaderLen+len(payload))
	binary.BigEndian.PutUint32(f[:4], uint32(len(f)-4))
	f[4] = frameTypeData
	f[5] = ch
	binary.BigEndian.PutUint32(f[6:10], seq)
	binary.BigEndian.PutUint16(f[10:12], idx)
	binary.BigEndian.PutUint16(f[12:14], total)
	copy(f[headerLen+streamHeaderLen:], payload)
	return f
}

// streamFrame wraps one data frame's wire bytes into the Frame the
// frameReader produces — type kept, body verbatim — so reassembler
// tests ride the exact wire layout instead of a parallel body builder.
func streamFrame(ch byte, seq uint32, idx, total uint16, payload []byte) Frame {
	wire := dataFrame(ch, seq, idx, total, payload)
	return Frame{Type: frameTypeData, Payload: wire[headerLen:]}
}

// streamChunk records what a StreamSink saw, in arrival order.
type streamChunk struct {
	ch      byte
	seq     uint32
	idx     uint16
	total   uint16
	payload []byte
}

// TestStreamWireLayout pins the 0x02 layout byte for byte: the length
// prefix counts type + sub-header + payload (so the payload cap is
// maxFrame minus 10), the channel sits right after the type byte,
// then seq, idx, total in big-endian, then the raw payload.
func TestStreamWireLayout(t *testing.T) {
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	f := dataFrame(1, 0x01020304, 5, 7, payload)
	if len(f) != headerLen+streamHeaderLen+len(payload) {
		t.Fatalf("frame length = %d, want %d", len(f), headerLen+streamHeaderLen+len(payload))
	}
	if got := binary.BigEndian.Uint32(f[:4]); got != uint32(streamHeaderLen+1+len(payload)) {
		t.Fatalf("length prefix = %d, want %d", got, streamHeaderLen+1+len(payload))
	}
	if f[4] != 0x02 {
		t.Fatalf("type = %#x, want 0x02", f[4])
	}
	if f[5] != 1 {
		t.Fatalf("ch = %d, want 1", f[5])
	}
	if got := binary.BigEndian.Uint32(f[6:10]); got != 0x01020304 {
		t.Fatalf("seq = %#x, want 0x01020304", got)
	}
	if got := binary.BigEndian.Uint16(f[10:12]); got != 5 {
		t.Fatalf("idx = %d, want 5", got)
	}
	if got := binary.BigEndian.Uint16(f[12:14]); got != 7 {
		t.Fatalf("total = %d, want 7", got)
	}
	if !bytes.Equal(f[headerLen+streamHeaderLen:], payload) {
		t.Fatalf("payload = %x, want %x", f[headerLen+streamHeaderLen:], payload)
	}
}

// TestReassemblerAssemblesChunks: three in-order chunks of one (ch,seq)
// assemble into the concatenated payload — ok fires exactly on the
// last chunk — and a single-chunk sequence (total=1) completes on its
// first frame. No gaps anywhere.
func TestReassemblerAssemblesChunks(t *testing.T) {
	r := newReassembler()

	parts := [][]byte{[]byte("AAAA"), []byte("BBBB"), []byte("CC")}
	var got []byte
	var gotSeq uint32
	completions := 0
	for i, p := range parts {
		seq, data, ok := r.feed(streamFrame(1, 42, uint16(i), 3, p))
		if ok != (i == len(parts)-1) {
			t.Fatalf("chunk %d: ok=%v, want %v", i, ok, i == len(parts)-1)
		}
		if !ok && (seq != 0 || data != nil) {
			t.Fatalf("chunk %d: incomplete feed returned (%d, %v)", i, seq, data)
		}
		if ok {
			got, gotSeq = data, seq
			completions++
		}
	}
	if completions != 1 || gotSeq != 42 || string(got) != "AAAABBBBCC" {
		t.Fatalf("completion = (%d, seq %d, %q), want (1, 42, \"AAAABBBBCC\")", completions, gotSeq, got)
	}
	if r.gaps.Load() != 0 {
		t.Fatalf("gaps = %d, want 0", r.gaps.Load())
	}

	if _, data, ok := r.feed(streamFrame(254, 43, 0, 1, []byte("solo"))); !ok || string(data) != "solo" {
		t.Fatalf("single-chunk seq = (%q, %v), want (\"solo\", true)", data, ok)
	}
	if r.gaps.Load() != 0 {
		t.Fatalf("gaps after single-chunk = %d, want 0", r.gaps.Load())
	}
}

// TestReassemblerGapInvalidatesAndCounts: an idx jump means a chunk
// was lost (the protocol never retransmits) — the sequence is
// abandoned and counted once; the jump's late followers drop quietly
// without a second count, and the channel's next sequence reassembles
// cleanly. A gap is a statistic here, never a connection failure.
func TestReassemblerGapInvalidatesAndCounts(t *testing.T) {
	r := newReassembler()

	if _, _, ok := r.feed(streamFrame(1, 7, 0, 4, []byte("a"))); ok {
		t.Fatal("first chunk reported complete")
	}
	if _, _, ok := r.feed(streamFrame(1, 7, 2, 4, []byte("c"))); ok {
		t.Fatal("gapped sequence reported complete")
	}
	if r.gaps.Load() != 1 {
		t.Fatalf("gaps after jump = %d, want 1", r.gaps.Load())
	}
	if _, _, ok := r.feed(streamFrame(1, 7, 3, 4, []byte("d"))); ok {
		t.Fatal("abandoned sequence completed after the fact")
	}
	if r.gaps.Load() != 1 {
		t.Fatalf("gaps after late follower = %d, want 1", r.gaps.Load())
	}
	if _, data, ok := r.feed(streamFrame(1, 8, 0, 1, []byte("next"))); !ok || string(data) != "next" {
		t.Fatalf("post-gap sequence = (%q, %v), want (\"next\", true)", data, ok)
	}
	if r.gaps.Load() != 1 {
		t.Fatalf("gaps after recovery = %d, want 1", r.gaps.Load())
	}
}

// TestReassemblerIgnoresCtrlAndKeepsChannelsApart: ctrl frames
// interleaved between chunks are not the reassembler's business — they
// leave its state untouched — and chunks of two channels (camera 1,
// diag 254) sharing one seq number assemble in their own buckets.
func TestReassemblerIgnoresCtrlAndKeepsChannelsApart(t *testing.T) {
	r := newReassembler()
	ctrlPayload, err := encodeMessage(map[string]any{"op": "event", "id": int64(0)})
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}

	steps := []struct {
		f    Frame
		want bool // completes?
	}{
		{streamFrame(1, 3, 0, 2, []byte("cam-0")), false},
		{Frame{Type: frameTypeCtrl, Payload: ctrlPayload}, false},
		{streamFrame(254, 3, 0, 1, []byte("diag-done")), true}, // same seq, other channel
		{streamFrame(1, 3, 1, 2, []byte("cam-1")), true},
		{Frame{Type: frameTypeCtrl, Payload: ctrlPayload}, false},
	}
	for i, step := range steps {
		if _, _, ok := r.feed(step.f); ok != step.want {
			t.Fatalf("feed %d: ok=%v, want %v", i, ok, step.want)
		}
	}
	if r.gaps.Load() != 0 {
		t.Fatalf("gaps = %d, want 0", r.gaps.Load())
	}
}

// TestReassemblerMaxPayloadChunk: one maximal chunk — maxStreamPayload
// (65526) bytes, the most the frame limit admits — travels the wire,
// frames, and reassembles intact; one byte more overflows the frame
// limit and is the reader's fatal error.
func TestReassemblerMaxPayloadChunk(t *testing.T) {
	payload := make([]byte, maxStreamPayload)
	for i := range payload { // patterned: truncation cannot pass unnoticed
		payload[i] = byte(i)
	}

	wire := dataFrame(1, 1, 0, 1, payload)
	if got := binary.BigEndian.Uint32(wire[:4]); got != maxFrame {
		t.Fatalf("max-payload frame length prefix = %d, want %d", got, maxFrame)
	}
	fs := readFrames(t, bytes.NewReader(wire))
	if len(fs) != 1 {
		t.Fatalf("decoded %d frames, want 1", len(fs))
	}
	if fs[0].Type != frameTypeData {
		t.Fatalf("max chunk type = %#x, want data", fs[0].Type)
	}
	r := newReassembler()
	seq, data, ok := r.feed(fs[0])
	if !ok || seq != 1 || !bytes.Equal(data, payload) {
		t.Fatalf("max chunk: ok=%v seq=%d len(data)=%d", ok, seq, len(data))
	}

	over := dataFrame(1, 2, 0, 1, make([]byte, maxStreamPayload+1))
	fr := newFrameReader(bytes.NewReader(over))
	if _, err := fr.next(); !errors.Is(err, errFatal) {
		t.Fatalf("oversize chunk err = %v, want errFatal", err)
	}
}

// TestReassemblerRejectsBadTotals: total=0 — a sequence of zero
// chunks — can never complete; it is invalidated and counted, and
// leaves no wedge (the channel's next sequence still delivers). A
// sequence whose total changes mid-flight disagrees with itself and is
// abandoned the same way.
func TestReassemblerRejectsBadTotals(t *testing.T) {
	r := newReassembler()

	if _, _, ok := r.feed(streamFrame(1, 5, 0, 0, []byte("x"))); ok {
		t.Fatal("total=0 sequence reported complete")
	}
	if r.gaps.Load() != 1 {
		t.Fatalf("gaps after total=0 = %d, want 1", r.gaps.Load())
	}
	if _, data, ok := r.feed(streamFrame(1, 6, 0, 1, []byte("fine"))); !ok || string(data) != "fine" {
		t.Fatalf("sequence after total=0 = (%q, %v)", data, ok)
	}

	if _, _, ok := r.feed(streamFrame(1, 7, 0, 3, []byte("a"))); ok {
		t.Fatal("later-inconsistent sequence completed")
	}
	if _, _, ok := r.feed(streamFrame(1, 7, 1, 4, []byte("b"))); ok {
		t.Fatal("inconsistent sequence completed")
	}
	if r.gaps.Load() != 2 {
		t.Fatalf("gaps = %d, want 2", r.gaps.Load())
	}
}

// TestReassemblerEvictsStaleSequence: a sequence whose tail was lost
// (its received chunks in order, then the channel moves on) can never
// finish — the reassembler drops its bucket when the channel's next
// sequence begins, counting one gap, so the bucket set stays bounded
// by the live channels instead of leaking one bucket per lost tail.
func TestReassemblerEvictsStaleSequence(t *testing.T) {
	r := newReassembler()

	if _, _, ok := r.feed(streamFrame(1, 100, 0, 4, []byte("a"))); ok {
		t.Fatal("stale sequence completed early")
	}
	if _, _, ok := r.feed(streamFrame(1, 100, 1, 4, []byte("b"))); ok {
		t.Fatal("stale sequence completed early")
	}
	if r.gaps.Load() != 0 {
		t.Fatalf("gaps before eviction = %d, want 0", r.gaps.Load())
	}
	if _, data, ok := r.feed(streamFrame(1, 101, 0, 1, []byte("z"))); !ok || string(data) != "z" {
		t.Fatalf("sequence after stale = (%q, %v)", data, ok)
	}
	if r.gaps.Load() != 1 {
		t.Fatalf("gaps after eviction = %d, want 1", r.gaps.Load())
	}
}

// TestClientStreamSinkInterleaved: the full client path — the mock
// board bursts a three-chunk sequence with a job ctrl event wedged
// between the chunks, then the op reply. The stream sink sees the
// chunks in order, the event subscriber still gets its event, the op
// still answers, and the chunks reassemble to the original payload:
// 0x02 and 0x01 share one connection without losing either side.
func TestClientStreamSinkInterleaved(t *testing.T) {
	c, b := newTestClient(t)
	ctx := context.Background()

	parts := [][]byte{[]byte("JPE"), []byte("G!"), []byte("tail")}
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		b.sendData(1, 9, 0, 3, parts[0])
		b.pushEvent("job", map[string]any{"tag": "t1"})
		b.sendData(1, 9, 1, 3, parts[1])
		b.sendData(1, 9, 2, 3, parts[2])
		return map[string]any{"out": "ok"}, nil
	})

	chunks := make(chan streamChunk, 8)
	c.OnStream(func(ch byte, seq uint32, idx, total uint16, payload []byte) {
		chunks <- streamChunk{ch: ch, seq: seq, idx: idx, total: total, payload: payload}
	})
	events := make(chan Event, 1)
	cancel := c.OnEvent("job", func(e Event) { events <- e })
	defer cancel()

	if out, _, err := c.Eval(ctx, "return 1", nil); err != nil || out != "ok" {
		t.Fatalf("eval during data burst = (%q, %v)", out, err)
	}
	e := recvEvent(t, events)
	if e.Source != "job" || e.Data["tag"] != "t1" {
		t.Fatalf("interleaved event = %#v", e)
	}

	waitFor(t, func() bool { return len(chunks) == 3 })
	r := newReassembler()
	var assembled []byte
	for i := 0; i < 3; i++ {
		ck := <-chunks
		if ck.ch != 1 || ck.seq != 9 || ck.idx != uint16(i) || ck.total != 3 {
			t.Fatalf("chunk %d = %+v", i, ck)
		}
		if seq, data, ok := r.feed(streamFrame(ck.ch, ck.seq, ck.idx, ck.total, ck.payload)); ok {
			if i != 2 {
				t.Fatalf("chunk %d completed the sequence early", i)
			}
			if seq != 9 || string(data) != "JPEG!tail" {
				t.Fatalf("assembled = (seq %d, %q), want (9, \"JPEG!tail\")", seq, data)
			}
			assembled = data
		}
	}
	if assembled == nil {
		t.Fatal("interleaved burst never completed")
	}
}

// TestClientStreamGapScenario: a gapped burst — chunk 0, then 2 and 3
// with 1 lost on the wire — reaches the sink frame-intact while the
// connection stays alive (a gap is never a resync failure), the
// reassembler counts exactly one gap, and the op reply still lands
// with len==0 frames sprinkled ahead of every frame.
func TestClientStreamGapScenario(t *testing.T) {
	c, b := newTestClient(t)
	ctx := context.Background()

	b.setZeroLen(true)
	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		b.sendData(254, 0, 0, 4, []byte("a"))
		b.sendData(254, 0, 2, 4, []byte("c")) // idx 1 never arrives
		b.sendData(254, 0, 3, 4, []byte("d"))
		return map[string]any{"out": "ok"}, nil
	})

	chunks := make(chan streamChunk, 8)
	c.OnStream(func(ch byte, seq uint32, idx, total uint16, payload []byte) {
		chunks <- streamChunk{ch: ch, seq: seq, idx: idx, total: total, payload: payload}
	})

	if out, _, err := c.Eval(ctx, "return 1", nil); err != nil || out != "ok" {
		t.Fatalf("eval after gapped burst = (%q, %v)", out, err)
	}
	if c.Dead() {
		t.Fatalf("connection died on a gapped burst: %s", c.DeadReason())
	}

	waitFor(t, func() bool { return len(chunks) == 3 })
	r := newReassembler()
	completed := 0
	for i := 0; i < 3; i++ {
		ck := <-chunks
		if _, _, ok := r.feed(streamFrame(ck.ch, ck.seq, ck.idx, ck.total, ck.payload)); ok {
			completed++
		}
	}
	if completed != 0 || r.gaps.Load() != 1 {
		t.Fatalf("gapped burst: completed=%d gaps=%d, want 0/1", completed, r.gaps.Load())
	}
}

// TestClientStreamWithoutSink: data frames with no sink registered are
// dropped, not fatal — a daemon that never opted into streams stays
// connected to a 0x02-emitting board (the frames are self-describing;
// acceptance is not protocol-version gated).
func TestClientStreamWithoutSink(t *testing.T) {
	c, b := newTestClient(t)
	ctx := context.Background()

	b.on("eval", func(args map[string]any) (map[string]any, *boardError) {
		b.sendData(1, 0, 0, 1, []byte("nobody-listening"))
		return map[string]any{"out": "ok"}, nil
	})
	if out, _, err := c.Eval(ctx, "return 1", nil); err != nil || out != "ok" {
		t.Fatalf("eval beside uncollected data = (%q, %v)", out, err)
	}
	if c.Dead() {
		t.Fatalf("connection died on uncollected data frames: %s", c.DeadReason())
	}
}
