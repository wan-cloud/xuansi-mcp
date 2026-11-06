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

// streamBucket is one sequence's state on a channel. Live buckets
// assemble: a channel's chunks arrive in strictly increasing (seq, idx)
// order — one stream, one emitter — so next is both the next expected
// index and the count already appended. A dead bucket is a tombstone:
// its sequence was abandoned and counted, kept only so late followers
// are recognized instead of recounted; each first sighting on the
// channel retires it (bounded to one live bucket plus one tombstone
// per channel).
type streamBucket struct {
	dead  bool
	total uint16
	next  uint16
	data  []byte
}

// reassembler assembles data chunks back into whole sequences. Every
// abandonment — a lost head (first sighting already past chunk 0), an
// idx jump (a middle chunk lost), a lost tail (the channel's next
// first sighting retires the unfinished bucket), a total that reads
// zero or changes mid-flight — counts exactly one gap, via tombstones
// and live-bucket retirement; a follower of an already-counted
// sequence never recounts it. Abandoned sequences are never delivered
// (no retransmit, no resync — sync lives in the frame layer) and gaps
// is never escalated to a connection failure. gaps is safe to read
// from any goroutine. Out-of-contract stragglers — a chunk of a
// sequence the channel has already retired — count as fresh losses:
// the counter is a diagnostic, not an invariant over broken emitters.
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

	b := r.buckets[key]
	if b != nil && b.dead {
		return 0, nil, false // follower of an abandoned sequence: already counted
	}
	if total == 0 {
		// A zero-chunk sequence can never complete; count it once and
		// tombstone whatever state the key had.
		r.abandon(key, b)
		return 0, nil, false
	}
	if b == nil {
		if idx != 0 {
			// First sighting already past chunk 0: the head was lost
			// and the sequence can never assemble. Tombstone it (the
			// count happens now, once), then retire the channel's older
			// in-flight state — this sighting proves those tails can no
			// longer arrive.
			r.abandon(key, nil)
			r.evictStale(key)
			return 0, nil, false
		}
		b = &streamBucket{total: total}
		r.buckets[key] = b
		r.evictStale(key)
	} else if b.total != total {
		// The emitter disagrees with itself about the chunk count: the
		// sequence cannot be trusted.
		r.abandon(key, b)
		return 0, nil, false
	}
	if idx < b.next {
		return 0, nil, false // re-delivered chunk: already appended
	}
	if idx > b.next {
		// A jump: an earlier chunk of this sequence was lost. Abandon
		// the whole sequence; its later chunks drop quietly above.
		r.abandon(key, b)
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

// abandon counts one gap for key's sequence and leaves a tombstone:
// the live bucket (b non-nil) is marked dead with its data freed, or a
// fresh tombstone is planted when none exists — the lost-head case,
// where the first sighting already proves the sequence unrecoverable.
// Followers of a tombstoned sequence find it in feed and drop without
// recounting; the tombstone lives until the channel's next first
// sighting retires it.
func (r *reassembler) abandon(key streamKey, b *streamBucket) {
	if b == nil {
		r.buckets[key] = &streamBucket{dead: true}
	} else {
		b.dead = true
		b.data = nil
	}
	r.gaps.Add(1)
}

// evictStale retires the channel's other sequences, called when key is
// first sighted (chunk 0 or a headless sighting — either proves the
// channel has moved on). Chunks arrive in strictly increasing (seq,
// idx) order per channel, so every other live bucket is a lost tail
// counting its one and only gap here; tombstones are already counted
// and drop silently. This bounds the state to one live bucket plus one
// tombstone per channel instead of leaking a bucket per lost tail.
func (r *reassembler) evictStale(key streamKey) {
	for k, b := range r.buckets {
		if k.ch == key.ch && k.seq != key.seq {
			if !b.dead {
				r.gaps.Add(1)
			}
			delete(r.buckets, k)
		}
	}
}
