package replclient

// stream.go — the consumer half of the 0x02 data-frame overlay: the
// sub-header parsing the router feeds sinks with, and the per-(ch,seq)
// chunk reassembler. Data frames are board→host only and may interleave
// with ctrl frames on one connection; a chunk gap invalidates its
// sequence — no retransmit, no resync (sync lives in the frame layer) —
// and is counted, never escalated to a connection failure.

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// StreamSink receives one parsed data chunk. Sinks run on the router
// goroutine — the event-handler discipline: be quick, never call back
// into the client's blocking API.
type StreamSink func(ch byte, seq uint32, idx, total uint16, payload []byte)

const (
	// streamHeaderLen is the 0x02 sub-header after the type byte:
	// 1-byte channel, 4-byte sequence, 2-byte chunk index, 2-byte chunk
	// count, all big-endian.
	streamHeaderLen = 9
	// maxStreamPayload is the largest chunk body one frame admits:
	// maxFrame minus the type byte and the sub-header.
	maxStreamPayload = maxFrame - 1 - streamHeaderLen
)

// parseStreamChunk splits a data frame's body into its sub-header
// fields and the raw chunk bytes. ok is false when the body is shorter
// than the sub-header — a truncated emitter. The frame layer stays in
// sync either way (the length prefix still bounded the frame), so
// callers drop such frames instead of failing the connection.
func parseStreamChunk(p []byte) (ch byte, seq uint32, idx, total uint16, payload []byte, ok bool) {
	if len(p) < streamHeaderLen {
		return 0, 0, 0, 0, nil, false
	}
	return p[0],
		binary.BigEndian.Uint32(p[1:5]),
		binary.BigEndian.Uint16(p[5:7]),
		binary.BigEndian.Uint16(p[7:9]),
		p[streamHeaderLen:],
		true
}

// streamKey buckets in-flight chunks: channels and sequences never
// share a bucket even when interleaved on one connection.
type streamKey struct {
	ch  byte
	seq uint32
}

// streamBucket is one sequence under assembly. A channel's chunks
// arrive in strictly increasing (seq, idx) order — one stream, one
// emitter — so next is both the next expected index and the count
// already appended.
type streamBucket struct {
	total uint16
	next  uint16
	data  []byte
}

// reassembler assembles data chunks back into whole sequences. A chunk
// whose idx runs past the bucket's next expected index means an earlier
// chunk was lost; the sequence is abandoned — never delivered — and
// counted in gaps. Late followers of an abandoned sequence drop without
// a second count; a sequence whose total reads zero or changes
// mid-flight is abandoned the same way. gaps is safe to read from any
// goroutine.
type reassembler struct {
	gaps atomic.Int64

	mu      sync.Mutex
	buckets map[streamKey]*streamBucket
}

func newReassembler() *reassembler {
	return &reassembler{buckets: make(map[streamKey]*streamBucket)}
}

// feed offers one frame. It returns ok=true exactly when that frame
// completed a sequence; data is then the assembled body (a fresh
// buffer) and seq its sequence number. Ctrl frames and data frames
// with a truncated sub-header are ignored.
func (r *reassembler) feed(f Frame) (seq uint32, data []byte, ok bool) {
	if f.Type != frameTypeData {
		return 0, nil, false // ctrl traffic: not ours to assemble
	}
	ch, seq, idx, total, payload, parsed := parseStreamChunk(f.Payload)
	if !parsed {
		return 0, nil, false // truncated sub-header: noise, not a sequence
	}
	key := streamKey{ch: ch, seq: seq}

	r.mu.Lock()
	defer r.mu.Unlock()

	if total == 0 {
		// A zero-chunk sequence can never complete; drop any state it
		// might have collected and count it as lost.
		delete(r.buckets, key)
		r.gaps.Add(1)
		return 0, nil, false
	}
	b := r.buckets[key]
	if b == nil {
		if idx != 0 {
			// Not a first chunk with no bucket: follower of an abandoned
			// (or never-seen) sequence — already counted, drop quietly.
			return 0, nil, false
		}
		b = &streamBucket{total: total}
		r.buckets[key] = b
		r.evictStale(key)
	} else if b.total != total {
		// The emitter disagrees with itself about the chunk count: the
		// sequence cannot be trusted.
		delete(r.buckets, key)
		r.gaps.Add(1)
		return 0, nil, false
	}
	if idx < b.next {
		return 0, nil, false // re-delivered chunk: already appended
	}
	if idx > b.next {
		// A jump: an earlier chunk of this sequence was lost. Abandon
		// the whole sequence; its later chunks drop quietly above.
		delete(r.buckets, key)
		r.gaps.Add(1)
		return 0, nil, false
	}
	b.data = append(b.data, payload...)
	b.next++
	if b.next < b.total {
		return 0, nil, false
	}
	delete(r.buckets, key) // complete: the bucket is done
	return seq, b.data, true
}

// evictStale drops the channel's other in-flight sequences. Chunks
// arrive in strictly increasing (seq, idx) order per channel, so once
// key's first chunk exists, every other bucket on that channel is a
// lost tail that can never finish; each counts one gap. This bounds
// the bucket set by the live channel count instead of leaking one
// bucket per lost tail.
func (r *reassembler) evictStale(key streamKey) {
	for k := range r.buckets {
		if k.ch == key.ch && k.seq != key.seq {
			delete(r.buckets, k)
			r.gaps.Add(1)
		}
	}
}
