package board

// camera_test.go — CameraShot's unit face (B9 task 5): one synchronous
// camera.capture whose 0x02 ch=1 chunks precede the op reply on the
// same connection. Pinned here: chunked assembly in board order, the
// metadata pull from the reply, the gap rule (an idx jump abandons the
// sequence and counts exactly once; a tombstone's followers drop
// quietly), op-error pass-through, args shaping (zero values ride the
// board defaults), and the per-call sink registration contract — the
// registration a redial's fresh connection needs (T2's per-connection
// ruling), plus the exit-time clear that keeps one capture's
// stragglers out of the next call's collector.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"xuansi/internal/replclient"
)

// newCamFake scripts one camera capture over a fresh fake controller.
func newCamFake(chunk int, frames ...[]byte) *fakeCtl {
	f := newFakeCtl()
	f.camFrames = frames
	f.camChunk = chunk
	return f
}

// jpegish builds a SOI…EOI-framed body of n payload bytes.
func jpegish(n int) []byte {
	b := make([]byte, 0, n+4)
	b = append(b, 0xFF, 0xD8)
	for i := 0; i < n; i++ {
		b = append(b, byte(i%251))
	}
	return append(b, 0xFF, 0xD9)
}

// TestCameraShotAssemblesChunkedFrames: a two-frame capture with the
// first frame split across chunks lands byte-exact in board order,
// with the reply's geometry/sensor/frame count pulled alongside.
func TestCameraShotAssemblesChunkedFrames(t *testing.T) {
	f0, f1 := jpegish(20), jpegish(9)
	ctl := newCamFake(7, f0, f1) // f0 → 4 chunks (7,7,7,2), f1 → 2
	ctl.camW, ctl.camH = 320, 240

	res, err := CameraShot(context.Background(), ctl, CameraOpts{Frames: 2, Framesize: "QVGA"})
	if err != nil {
		t.Fatalf("CameraShot: %v", err)
	}
	if len(res.JPEGs) != 2 || !bytes.Equal(res.JPEGs[0], f0) || !bytes.Equal(res.JPEGs[1], f1) {
		t.Fatalf("JPEGs = %d frames, first-equal=%v second-equal=%v — want the two bodies byte-exact in board order", len(res.JPEGs), bytes.Equal(res.JPEGs[0], f0), bytes.Equal(res.JPEGs[1], f1))
	}
	if res.W != 320 || res.H != 240 || res.Sensor != "GC2145" || res.Frames != 2 {
		t.Fatalf("reply pull = w=%d h=%d sensor=%q frames=%d, want 320/240/GC2145/2", res.W, res.H, res.Sensor, res.Frames)
	}
	if res.Gaps != 0 {
		t.Fatalf("gaps = %d, want 0 on an in-order stream", res.Gaps)
	}

	// args shaped from opts
	ops := ctl.opsSnapshot()
	if len(ops) == 0 || ops[0].op != "camera.capture" {
		t.Fatalf("ops = %v, want camera.capture recorded", ops)
	}
	if got, _ := ops[0].args["frames"].(int64); got != 2 {
		t.Fatalf("args[frames] = %#v, want int64 2", ops[0].args["frames"])
	}
	if got, _ := ops[0].args["framesize"].(string); got != "QVGA" {
		t.Fatalf("args[framesize] = %#v, want QVGA", ops[0].args["framesize"])
	}
	// the sink was registered on THIS connection before the op ran,
	// and cleared again when the call ended
	if !ctl.sinkAtOp {
		t.Fatal("the op ran with NO sink registered — registration must precede the op")
	}
	if ctl.sink != nil {
		t.Fatal("sink still registered after CameraShot returned — the call must clear it")
	}
}

// TestCameraShotZeroOptsRideBoardDefaults: a zero CameraOpts crosses as
// an empty args map — the board's own defaults (1 frame, VGA) apply.
func TestCameraShotZeroOptsRideBoardDefaults(t *testing.T) {
	ctl := newCamFake(0, jpegish(5))
	if _, err := CameraShot(context.Background(), ctl, CameraOpts{}); err != nil {
		t.Fatalf("CameraShot: %v", err)
	}
	ops := ctl.opsSnapshot()
	if len(ops) == 0 {
		t.Fatal("no op recorded")
	}
	if n := len(ops[0].args); n != 0 {
		t.Fatalf("args = %#v, want empty (zero opts ride the board defaults)", ops[0].args)
	}
}

// TestCameraShotGapAbandonsSequenceOnce: a lost chunk (idx jump)
// abandons exactly that sequence — counted once, its later chunks and
// a straggler re-delivery drop quietly — while the intact frames land.
func TestCameraShotGapAbandonsSequenceOnce(t *testing.T) {
	f0, f1, f2 := jpegish(12), jpegish(6), jpegish(6)
	ctl := newCamFake(4, f0, f1, f2) // each frame → chunks of 4
	ctl.camSkip = map[uint32]int{10: 1} // frame 0 loses chunk idx 1
	// stragglers after the frames: the lost chunk re-delivered (tombstone
	// follower) and a headless sighting of a fresh sequence
	ctl.camTail = []camChunkT{
		{seq: 10, idx: 1, total: 3, data: []byte{9, 9, 9, 9}},
		{seq: 99, idx: 2, total: 3, data: []byte{7}},
	}

	res, err := CameraShot(context.Background(), ctl, CameraOpts{Frames: 3})
	if err != nil {
		t.Fatalf("CameraShot: %v", err)
	}
	if res.Gaps != 2 {
		t.Fatalf("gaps = %d, want 2 (frame 0's idx jump + the headless seq-99 sighting, each once)", res.Gaps)
	}
	if len(res.JPEGs) != 2 || !bytes.Equal(res.JPEGs[0], f1) || !bytes.Equal(res.JPEGs[1], f2) {
		t.Fatalf("JPEGs = %d frames (f1-eq=%v f2-eq=%v), want the two intact frames only", len(res.JPEGs), len(res.JPEGs) > 0 && bytes.Equal(res.JPEGs[0], f1), len(res.JPEGs) > 1 && bytes.Equal(res.JPEGs[1], f2))
	}
	if res.Frames != 3 {
		t.Fatalf("reply frames = %d, want 3 (the board's own count — the gap is reported, not hidden)", res.Frames)
	}
}

// TestCameraShotCountsLostTailAtReply: a sequence whose TAIL chunk
// never arrives triggers no idx jump — the loss surfaces when the op
// reply ends the capture and the still-open bucket is counted.
func TestCameraShotCountsLostTailAtReply(t *testing.T) {
	f0, f1 := jpegish(8), jpegish(8)
	ctl := newCamFake(4, f0, f1)     // each frame → chunks of 4,4
	ctl.camSkip = map[uint32]int{11: 1} // frame 1 loses its LAST chunk

	res, err := CameraShot(context.Background(), ctl, CameraOpts{Frames: 2})
	if err != nil {
		t.Fatalf("CameraShot: %v", err)
	}
	if len(res.JPEGs) != 1 || !bytes.Equal(res.JPEGs[0], f0) {
		t.Fatalf("JPEGs = %d (f0-eq=%v), want only the intact frame 0", len(res.JPEGs), len(res.JPEGs) > 0 && bytes.Equal(res.JPEGs[0], f0))
	}
	if res.Gaps != 1 {
		t.Fatalf("gaps = %d, want 1 — the lost tail counts at reply time", res.Gaps)
	}
}

// TestCameraShotOpErrorPassesThrough: a board refusal crosses verbatim
// (the OpError shape callers classify), with nothing collected.
func TestCameraShotOpErrorPassesThrough(t *testing.T) {
	ctl := newCamFake(0, jpegish(5))
	ctl.opErr = map[string]error{
		"camera.capture": &replclient.OpError{Op: "camera.capture", Class: "api", Msg: "pin 4 held by gpio"},
	}
	res, err := CameraShot(context.Background(), ctl, CameraOpts{})
	if err == nil {
		t.Fatal("the board refusal must cross")
	}
	var oe *replclient.OpError
	if !errors.As(err, &oe) || oe.Class != "api" {
		t.Fatalf("err = %v, want the board's OpError class api", err)
	}
	if len(res.JPEGs) != 0 || res.Gaps != 0 {
		t.Fatalf("res = %+v, want nothing collected on refusal", res)
	}
}

// TestCameraShotRegistersSinkPerCall (the T2 redial ruling): the sink
// is PER-CONNECTION and set-only — a redial's fresh connection starts
// with none. CameraShot therefore registers its collector on the
// controller it is ABOUT to use, on every call: a first attempt dying
// with the link loses nothing, and the retried call on the fresh
// controller gets its sink before its op — the server's one-redial
// retry loop relies on exactly this.
func TestCameraShotRegistersSinkPerCall(t *testing.T) {
	dead := newCamFake(0, jpegish(5))
	dead.opErr = map[string]error{
		"camera.capture": fmt.Errorf("%w: link closed by board", replclient.ErrDead),
	}
	_, err := CameraShot(context.Background(), dead, CameraOpts{})
	if !errors.Is(err, replclient.ErrDead) {
		t.Fatalf("first attempt err = %v, want ErrDead", err)
	}
	if !dead.sinkAtOp {
		t.Fatal("the dying attempt never registered its sink — registration precedes the op on EVERY controller")
	}

	// the caller redials (server shape): a FRESH controller, and the
	// retried call must self-register on it
	fresh := newCamFake(8, jpegish(16))
	res, err := CameraShot(context.Background(), fresh, CameraOpts{})
	if err != nil {
		t.Fatalf("retried CameraShot: %v", err)
	}
	if !fresh.sinkAtOp {
		t.Fatal("the retried call's op ran with no sink on the FRESH controller — per-call re-registration is missing")
	}
	if len(res.JPEGs) != 1 || fresh.sink != nil {
		t.Fatalf("retried res = %+v sink-cleared=%v, want the frame landed and the sink cleared", res, fresh.sink == nil)
	}
}
