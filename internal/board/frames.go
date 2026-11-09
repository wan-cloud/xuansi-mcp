package board

// frames.go — the generic 0x02 stream-frame collector (B12-P2 T5, spec
// §5.6): whatever frames arrive on a connection inside a consumer's
// window ride that consumer's result — an eval reply or a pending-job
// event — bounded and honestly counted. The reassembler generalizes
// camera.go's cameraSeq/cameraCollector (the T2 bookkeeping:
// seq/idx/total, one-loss abandonments, tombstones) along two axes:
// any channel, and the fw's ch=2 stream_send host stamp (xs_wasm.c):
// EVERY chunk of a module frame carries [u8 name_len][name][data
// part], so the stamp is parsed per chunk and only the data parts
// concatenate. Where cameraSeq required in-order idx (an append
// cursor; a jump abandons), this collector buffers chunks per idx — a
// lower idx arriving late still completes the frame (the seam can
// present chunks out of cursor order); a sequence that never completes
// counts lost at window close.

import (
	"sync"

	"xuansi/internal/replclient"
)

// moduleStreamCh is the wasm module channel: the only stamped emitter
// (fw xs_wasm n_stream_send). Other channels (diag.streamtest's 254)
// carry raw payloads with no stamp — their frames cross verbatim with
// an empty name.
const moduleStreamCh = 2

// §5.6 bounds: at most 16 completed frames / 1 MiB cumulative payload
// per window; the excess is truncated with an honest frames_dropped
// count (the camera path's gaps-honesty, moved over).
const (
	FramesMax      = 16
	FramesBytesMax = 1 << 20
)

// Frame is one completed 0x02 stream frame: the channel it rode, the
// module name the fw's host stamp carried ("" on unstamped channels),
// and the assembled payload.
type Frame struct {
	Ch   byte
	Name string
	Data []byte
}

// FrameSnapshot is one window's take: the completed frames in
// completion order plus every honestly-counted loss (bound
// truncations, gap abandonments, still-open tails at snapshot time —
// on the ordered stream a frame's chunks all precede the reply/event
// that closes its window, so an open sequence can no longer complete).
type FrameSnapshot struct {
	Frames  []Frame
	Dropped int
}

// streamKey buckets by (ch, seq): the channels' emitters keep
// independent seq counters, so seq alone could collide across them.
type streamKey struct {
	ch  byte
	seq uint32
}

// frameSeq is one in-flight sequence, generalized from cameraSeq: the
// T2 bookkeeping (seq/idx/total, one-loss abandonments, tombstones)
// with TOLERANT chunk storage — parts buffer per idx instead of an
// append cursor, so a lower idx that arrives late still completes the
// frame (plan case ④; the seam can present chunks out of cursor order
// even though today's wire is ordered). A sequence that never completes
// counts lost at window close.
type frameSeq struct {
	total    uint16
	name     string // the host stamp's module name (identical on every chunk)
	parts    map[uint16][]byte
	have     int // distinct idx received
	buffered int // summed part bytes (the oversize guard's ledger)
	dead     bool
}

// FrameCollector is the sink-side reassembler for one consumer window
// (one eval call, or the pending-job buffer's span between drains).
// Like cameraCollector it needs no stale eviction: a window's
// collector lives exactly as long as its window, and a lost sequence's
// bucket is tombstoned on first loss.
type FrameCollector struct {
	mu      sync.Mutex
	buckets map[streamKey]*frameSeq
	frames  []Frame
	bytes   int // cumulative kept payload (the 1 MiB bound's ledger)
	dropped int
}

// NewFrameCollector builds an empty collector.
func NewFrameCollector() *FrameCollector {
	return &FrameCollector{buckets: map[streamKey]*frameSeq{}}
}

// Sink is the StreamSink a connection's router calls per parsed 0x02
// chunk (router-goroutine discipline: quick, no callbacks into the
// client). Payload slices may be reused by the router — every keep is
// an append-copy into collector-owned buffers.
func (c *FrameCollector) Sink(ch byte, seq uint32, idx, total uint16, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := streamKey{ch, seq}
	b := c.buckets[k]
	if b != nil && b.dead {
		return // a tombstoned sequence's followers drop without recount
	}
	name, data, ok := parseStamp(ch, payload)
	if !ok {
		c.abandon(k) // a stamp header the fw's emitter cannot produce
		return
	}
	if b == nil {
		if total == 0 {
			c.abandon(k) // a zero-chunk sequence can never complete
			return
		}
		b = &frameSeq{total: total, parts: map[uint16][]byte{}}
		c.buckets[k] = b
	} else if b.total != total {
		c.abandon(k) // the emitter disagrees with itself about the count
		return
	}
	if name != "" {
		b.name = name // identical on every chunk; kept for the frame
	}
	if _, dup := b.parts[idx]; dup {
		return // re-delivered chunk: already buffered
	}
	b.parts[idx] = data
	b.have++
	b.buffered += len(data)
	if b.buffered > FramesBytesMax {
		// the frame can never fit the bound — abandon now instead of
		// buffering it whole (memory stays bounded; the drop is the
		// one the completion check would have counted anyway)
		c.abandon(k)
		return
	}
	if b.have < int(b.total) {
		return
	}
	delete(c.buckets, k) // complete: the frame is done
	assembled := make([]byte, 0, b.buffered)
	for i := uint16(0); i < b.total; i++ {
		assembled = append(assembled, b.parts[i]...)
	}
	if len(c.frames) >= FramesMax || c.bytes+len(assembled) > FramesBytesMax {
		c.dropped++ // bound truncation: honest, first-come-first-kept
		return
	}
	c.frames = append(c.frames, Frame{Ch: ch, Name: b.name, Data: assembled})
	c.bytes += len(assembled)
}

// parseStamp splits one chunk per the fw's stamping: ch=2 chunks carry
// [u8 name_len][name][data part]; anything else crosses verbatim. A
// malformed stamp (claims more name than the chunk carries — the fw's
// emitter cannot produce one) reports ok=false so the caller abandons
// the sequence.
func parseStamp(ch byte, payload []byte) (name string, data []byte, ok bool) {
	if ch != moduleStreamCh {
		return "", payload, true
	}
	if len(payload) < 1 {
		return "", nil, false
	}
	n := int(payload[0])
	if len(payload) < 1+n {
		return "", nil, false
	}
	return string(payload[1 : 1+n]), payload[1+n:], true
}

// abandon tombstones the sequence and counts the loss exactly once.
func (c *FrameCollector) abandon(k streamKey) {
	if b := c.buckets[k]; b != nil {
		b.dead = true
		b.parts = nil
		b.buffered = 0
	} else {
		c.buckets[k] = &frameSeq{dead: true}
	}
	c.dropped++
}

// snapshot is the shared read: the completed frames plus every honest
// loss, still-open non-dead buckets counted as lost tails.
func (c *FrameCollector) snapshot(reset bool) FrameSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	lost := 0
	for _, b := range c.buckets {
		if !b.dead {
			lost++
		}
	}
	snap := FrameSnapshot{
		Frames:  append([]Frame(nil), c.frames...),
		Dropped: c.dropped + lost,
	}
	if reset {
		c.buckets = map[streamKey]*frameSeq{}
		c.frames = nil
		c.bytes = 0
		c.dropped = 0
	}
	return snap
}

// Snapshot reads the window without ending it (read-only; the frames'
// Data buffers are collector-owned and never rewritten).
func (c *FrameCollector) Snapshot() FrameSnapshot { return c.snapshot(false) }

// Drain is Snapshot plus reset: it hands the window's frames over and
// empties the collector — the pending-job buffer's per-event shape.
func (c *FrameCollector) Drain() FrameSnapshot { return c.snapshot(true) }

// Reset empties the collector mid-window (between a redial retry's
// attempts: the re-executed op must not graft the dead attempt's
// partial sequences onto the retry's frames).
func (c *FrameCollector) Reset() {
	c.mu.Lock()
	c.buckets = map[streamKey]*frameSeq{}
	c.frames = nil
	c.bytes = 0
	c.dropped = 0
	c.mu.Unlock()
}

// FrameHub fans one connection's 0x02 chunks out to every registered
// consumer. It exists because the connection's sink slot is single
// (router swap semantics — set-only, no stacking): the per-call
// collectors of overlapping evals and the pending-job buffer share one
// warm link, and (pre-v6) the camera path's per-call install clobbers
// the slot — the hub is re-asserted around every streaming op so every
// clobber self-heals. Installed via Controller.OnStream (per
// supervisor.go's contract a redial's fresh connection starts empty;
// the next warm re-asserts the hub on the link it returns).
type FrameHub struct {
	mu    sync.Mutex
	next  uint64
	sinks map[uint64]replclient.StreamSink
}

// NewFrameHub builds an empty hub.
func NewFrameHub() *FrameHub {
	return &FrameHub{sinks: map[uint64]replclient.StreamSink{}}
}

// Add registers one consumer and returns its remove func. Sinks run on
// the router goroutine (the event-handler discipline).
func (h *FrameHub) Add(sink replclient.StreamSink) (remove func()) {
	h.mu.Lock()
	id := h.next
	h.next++
	h.sinks[id] = sink
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		delete(h.sinks, id)
		h.mu.Unlock()
	}
}

// Sink is the hub's own StreamSink (what OnStream takes); it fans each
// chunk out to a snapshot of the registered consumers.
func (h *FrameHub) Sink(ch byte, seq uint32, idx, total uint16, payload []byte) {
	h.mu.Lock()
	sinks := make([]replclient.StreamSink, 0, len(h.sinks))
	for _, s := range h.sinks {
		sinks = append(sinks, s)
	}
	h.mu.Unlock()
	for _, s := range sinks {
		s(ch, seq, idx, total, payload)
	}
}
