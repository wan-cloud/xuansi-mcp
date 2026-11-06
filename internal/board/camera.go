// camera.go — the camera face's board-side half (B9 task 5): one
// synchronous camera.capture whose 0x02 ch=1 stream frames precede the
// op reply on the same connection. CameraShot registers a stream sink
// for the call, issues the op, and reads what the sink collected once
// the reply landed: on the ordered TCP stream every chunk of this
// capture has already been dispatched by the time the reply arrives,
// so there is nothing to wait for after Op returns.
package board

import (
	"context"
	"sync"
)

// CameraOpts parameterize one shot; zero values ride the board's own
// defaults (frames=1, framesize=VGA).
type CameraOpts struct {
	Frames    int    // 1..4; 0 = the board default
	Framesize string // "QVGA" | "VGA"; "" = the board default
}

// CameraResult is one completed capture: the assembled JPEG bodies in
// board order (a gap drops its entry), the geometry the board
// reported, and the reassembly loss count.
type CameraResult struct {
	JPEGs  [][]byte
	W, H   int    // the board's reported geometry
	Sensor string // e.g. "GC2145"
	Frames int    // the board's own frame count (JPEGs + lost frames)
	Gaps   int    // sequences abandoned on chunk-loss jumps
}

// cameraStreamCh is the 0x02 overlay's camera channel (1 = camera;
// diag.streamtest's 254 and everything else are not ours).
const cameraStreamCh = 1

// CameraShot runs one camera.capture over ctl and returns what its
// stream carried. The sink is PER-CONNECTION state (T2's ruling: a
// redial's fresh connection starts with none), so every call
// registers its own collector on the controller it is about to use —
// a caller's one-redial retry gets a fresh registration for free —
// and clears it on exit, so one capture's stragglers can never leak
// into a later call's collector. A mid-capture link death surfaces as
// the op's ErrDead: return it; retry on a fresh controller.
func CameraShot(ctx context.Context, ctl Controller, opts CameraOpts) (CameraResult, error) {
	args := map[string]any{}
	if opts.Frames > 0 {
		args["frames"] = int64(opts.Frames)
	}
	if opts.Framesize != "" {
		args["framesize"] = opts.Framesize
	}
	col := &cameraCollector{buckets: map[uint32]*cameraSeq{}}
	ctl.OnStream(col.sink)
	defer ctl.OnStream(nil)
	res, err := ctl.Op(ctx, "camera.capture", args)
	if err != nil {
		return CameraResult{}, err
	}
	out := col.result()
	out.W, out.H = camInt(res, "w"), camInt(res, "h")
	out.Sensor, _ = res["sensor"].(string)
	out.Frames = camInt(res, "frames")
	return out, nil
}

// camInt pulls an int from a board reply (msgpack ints decode int64).
func camInt(res map[string]any, k string) int {
	switch v := res[k].(type) {
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}

// cameraSeq is one in-flight sequence — the T2 reassembler's per-key
// semantics: chunks append in idx order, a jump abandons the whole
// sequence exactly once, and a tombstone swallows its late followers.
type cameraSeq struct {
	total uint16
	next  uint16 // the next idx this bucket accepts
	data  []byte
	dead  bool
}

// cameraCollector is the sink-side reassembler for one CameraShot
// call. Unlike the long-lived reassembler it needs no stale eviction:
// a call's collector lives exactly from its op's issue to its reply,
// and a lost sequence's bucket is tombstoned on first loss.
type cameraCollector struct {
	mu      sync.Mutex
	buckets map[uint32]*cameraSeq
	jpegs   [][]byte
	gaps    int
}

// sink is the StreamSink the connection's router calls per parsed 0x02
// chunk (router-goroutine discipline: quick, no callbacks into the
// client). Payload slices are per-frame private copies — safe to keep.
func (c *cameraCollector) sink(ch byte, seq uint32, idx, total uint16, payload []byte) {
	if ch != cameraStreamCh {
		return // other channels are not ours
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.buckets[seq]
	if b != nil && b.dead {
		return // a tombstoned sequence's followers drop without recount
	}
	if b == nil {
		if total == 0 {
			c.abandon(seq) // a zero-chunk sequence can never complete
			return
		}
		b = &cameraSeq{total: total}
		c.buckets[seq] = b
	} else if b.total != total {
		c.abandon(seq) // the emitter disagrees with itself about the count
		return
	}
	switch {
	case idx < b.next:
		return // re-delivered chunk: already appended
	case idx > b.next:
		c.abandon(seq) // a jump: an earlier chunk of this sequence was lost
		return
	}
	b.data = append(b.data, payload...)
	b.next++
	if b.next < b.total {
		return
	}
	delete(c.buckets, seq) // complete: the frame is done
	c.jpegs = append(c.jpegs, b.data)
}

// abandon tombstones seq and counts the loss exactly once.
func (c *cameraCollector) abandon(seq uint32) {
	if b := c.buckets[seq]; b != nil {
		b.dead = true
		b.data = nil
	} else {
		c.buckets[seq] = &cameraSeq{dead: true}
	}
	c.gaps++
}

// result snapshots the completed frames in completion order — the
// board streams its frames sequentially, so that is board order. The
// reply's arrival ends the capture: every still-open sequence is a
// loss (its remaining chunks can no longer arrive — they would have
// preceded the reply on the ordered stream), counted here alongside
// the jump-time abandonments. Tombstoned buckets are already counted.
func (c *cameraCollector) result() CameraResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	lost := 0
	for _, b := range c.buckets {
		if !b.dead {
			lost++
		}
	}
	return CameraResult{
		JPEGs: append([][]byte(nil), c.jpegs...),
		Gaps:  c.gaps + lost,
	}
}
