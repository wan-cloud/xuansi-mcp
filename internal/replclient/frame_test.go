package replclient

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
)

// chunkReader serves data in fixed-size chunks however large the caller's
// buffer is, simulating a transport that splits frame headers across
// deliveries (§9 — no header atomicity).
type chunkReader struct {
	data  []byte
	chunk int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.chunk
	if n > len(p) {
		n = len(p)
	}
	if n > len(r.data) {
		n = len(r.data)
	}
	n = copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

// readFrames decodes every frame a reader yields, in order, until EOF.
func readFrames(t *testing.T, r io.Reader) []Frame {
	t.Helper()
	fr := newFrameReader(r)
	var out []Frame
	for {
		f, err := fr.next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("frameReader.next: %v", err)
		}
		out = append(out, f)
	}
}

// TestCodecRoundTrip: values across the protocol domain (nil/bool/int64/
// str/bin/array/map, nested — §1) survive an encode→decode round trip,
// with bin staying bin and ints coming back as int64.
func TestCodecRoundTrip(t *testing.T) {
	in := map[string]any{
		"nil":   nil,
		"bool":  true,
		"int":   int64(-42),
		"str":   "懸絲",
		"bin":   []byte{0x00, 0x01, 0xfe, 0xff},
		"array": []any{int64(1), "two", nil, []any{int64(3)}},
		"map":   map[string]any{"nested": map[string]any{"deep": int64(8)}},
	}
	payload, err := encodeMessage(in)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	out, err := decodeMessage(payload)
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip mismatch:\n got %#v\nwant %#v", out, in)
	}
}

// TestEncodeRequestDeterministic: the same request always encodes to the
// same bytes, with the key order pinned to op/id/args (mirrors
// repl.py request()); args absent means a two-pair fixmap.
func TestEncodeRequestDeterministic(t *testing.T) {
	a, err := encodeRequest("ping", int64(1), nil)
	if err != nil {
		t.Fatalf("encodeRequest: %v", err)
	}
	b, err := encodeRequest("ping", int64(1), nil)
	if err != nil {
		t.Fatalf("encodeRequest: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("request encoding not deterministic:\n%x\n%x", a, b)
	}
	if a[0] != 0x82 { // msgpack fixmap, two pairs
		t.Fatalf("args-less request first byte = %#x, want fixmap(2)", a[0])
	}
	m, err := decodeMessage(a)
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}
	if m["op"] != "ping" || m["id"] != int64(1) {
		t.Fatalf("decoded request = %#v", m)
	}
	if _, ok := m["args"]; ok {
		t.Fatal("args key must be absent when args is nil")
	}

	c, err := encodeRequest("eval", int64(2), map[string]any{"code": "return 1"})
	if err != nil {
		t.Fatalf("encodeRequest: %v", err)
	}
	if c[0] != 0x83 { // msgpack fixmap, three pairs
		t.Fatalf("request with args first byte = %#x, want fixmap(3)", c[0])
	}
	m, err = decodeMessage(c)
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}
	args, ok := m["args"].(map[string]any)
	if !ok || args["code"] != "return 1" {
		t.Fatalf("decoded args = %#v", m["args"])
	}
}

// TestFrameHeaderShape: a frame is [u32 BE len][0x01][payload] with len
// counting the type byte plus payload (§1).
func TestFrameHeaderShape(t *testing.T) {
	payload := []byte{0xc0} // msgpack nil
	f, err := frame(payload)
	if err != nil {
		t.Fatalf("frame: %v", err)
	}
	if len(f) != len(payload)+5 {
		t.Fatalf("frame length = %d, want %d", len(f), len(payload)+5)
	}
	if got := binary.BigEndian.Uint32(f[:4]); got != uint32(len(payload)+1) {
		t.Fatalf("length prefix = %d, want %d", got, len(payload)+1)
	}
	if f[4] != 0x01 {
		t.Fatalf("type byte = %#x, want 0x01", f[4])
	}
	if !bytes.Equal(f[5:], payload) {
		t.Fatalf("payload not verbatim: %x", f[5:])
	}
}

// TestFrameRejectsOversizePayload: the writer side never emits a frame
// the board would treat as fatal — payloads above maxFrame-1 are refused
// (§1/§7).
func TestFrameRejectsOversizePayload(t *testing.T) {
	big := make([]byte, maxFrame)
	if _, err := frame(big); err == nil {
		t.Fatal("frame accepted a 65536-byte payload, want refusal")
	}
	f, err := frame(big[:maxFrame-1])
	if err != nil {
		t.Fatalf("frame rejected a max-size payload: %v", err)
	}
	if got := binary.BigEndian.Uint32(f[:4]); got != maxFrame {
		t.Fatalf("max-size frame length prefix = %d, want %d", got, maxFrame)
	}
}

// TestFrameReaderReassemblesSplitFrames: frames survive arbitrary read
// granularity — 1-byte and 3-byte chunks both reassemble every payload
// in order.
func TestFrameReaderReassemblesSplitFrames(t *testing.T) {
	var wire []byte
	var want [][]byte
	for i := 0; i < 3; i++ {
		payload, err := encodeMessage(map[string]any{"op": "reply", "id": int64(i)})
		if err != nil {
			t.Fatalf("encodeMessage: %v", err)
		}
		f, err := frame(payload)
		if err != nil {
			t.Fatalf("frame: %v", err)
		}
		wire = append(wire, f...)
		want = append(want, payload)
	}
	for _, chunk := range []int{1, 3} {
		got := readFrames(t, &chunkReader{data: append([]byte(nil), wire...), chunk: chunk})
		if len(got) != len(want) {
			t.Fatalf("chunk %d: decoded %d frames, want %d", chunk, len(got), len(want))
		}
		for i := range want {
			if got[i].Type != frameTypeCtrl {
				t.Fatalf("chunk %d frame %d type = %#x, want ctrl", chunk, i, got[i].Type)
			}
			if !bytes.Equal(got[i].Payload, want[i]) {
				t.Fatalf("chunk %d frame %d = %x, want %x", chunk, i, got[i].Payload, want[i])
			}
		}
	}
}

// TestFrameReaderToleratesZeroLen: a zero length prefix is decided at
// the 4-byte header boundary and skipped without losing sync (§1) —
// frames before and after still decode.
func TestFrameReaderToleratesZeroLen(t *testing.T) {
	var wire []byte
	var want [][]byte
	for i := 0; i < 3; i++ {
		payload, err := encodeMessage(map[string]any{"op": "reply", "id": int64(i)})
		if err != nil {
			t.Fatalf("encodeMessage: %v", err)
		}
		f, err := frame(payload)
		if err != nil {
			t.Fatalf("frame: %v", err)
		}
		wire = append(wire, f...)
		want = append(want, payload)
		if i < 2 {
			wire = append(wire, 0, 0, 0, 0) // empty frame: header only
		}
	}
	got := readFrames(t, bytes.NewReader(wire))
	if len(got) != len(want) {
		t.Fatalf("decoded %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Type != frameTypeCtrl {
			t.Fatalf("frame %d type = %#x, want ctrl", i, got[i].Type)
		}
		if !bytes.Equal(got[i].Payload, want[i]) {
			t.Fatalf("frame %d = %x, want %x", i, got[i].Payload, want[i])
		}
	}
}

// TestFrameReaderFatalOversize: a length prefix above MAX_FRAME is a
// fatal desync (§7) — the reader refuses instead of trying to resync.
func TestFrameReaderFatalOversize(t *testing.T) {
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], maxFrame+1)
	hdr[4] = 0x01
	fr := newFrameReader(bytes.NewReader(hdr[:]))
	_, err := fr.next()
	if !errors.Is(err, errFatal) {
		t.Fatalf("err = %v, want errFatal", err)
	}
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("65537")) {
		t.Fatalf("error does not name the oversize length: %v", err)
	}
}

// TestFrameReaderFatalBadType: a type byte outside the defined set —
// ctrl 0x01, data 0x02 — is fatal on receipt (§7); 0x03 stands in for
// every reserved byte.
func TestFrameReaderFatalBadType(t *testing.T) {
	payload, err := encodeMessage(map[string]any{"op": "reply"})
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	f, err := frame(payload)
	if err != nil {
		t.Fatalf("frame: %v", err)
	}
	f[4] = 0x03
	fr := newFrameReader(bytes.NewReader(f))
	if _, err := fr.next(); !errors.Is(err, errFatal) {
		t.Fatalf("err = %v, want errFatal", err)
	}
}

// TestFrameReaderDataFramesSurviveSplitAndZeroLen: 0x02 data frames
// cross the reader under the same conditions as ctrl frames — headers
// split across reads, len==0 neighbors — with type and body verbatim.
// The reader stays a byte mover: it does not interpret the data
// sub-header, so a body too short to hold one still frames fine (the
// stream consumer drops it; sync never depended on it).
func TestFrameReaderDataFramesSurviveSplitAndZeroLen(t *testing.T) {
	frames := [][]byte{
		dataFrame(1, 9, 0, 1, []byte("jpeg-bytes")),              // camera single-chunk
		dataFrame(254, 0xFFFFFFFF, 65535, 65535, nil),            // sub-header only
		dataFrame(254, 3, 2, 8, bytes.Repeat([]byte{0xA5}, 300)), // a mid-sequence diag chunk
	}
	var wire []byte
	for i, f := range frames {
		wire = append(wire, f...)
		if i < len(frames)-1 {
			wire = append(wire, 0, 0, 0, 0) // empty frame between data frames
		}
	}
	for _, chunk := range []int{1, 3} {
		got := readFrames(t, &chunkReader{data: append([]byte(nil), wire...), chunk: chunk})
		if len(got) != len(frames) {
			t.Fatalf("chunk %d: decoded %d frames, want %d", chunk, len(got), len(frames))
		}
		for i := range frames {
			if got[i].Type != frameTypeData {
				t.Fatalf("chunk %d frame %d type = %#x, want data", chunk, i, got[i].Type)
			}
			if !bytes.Equal(got[i].Payload, frames[i][headerLen:]) {
				t.Fatalf("chunk %d frame %d body = %x, want %x", chunk, i, got[i].Payload, frames[i][headerLen:])
			}
		}
	}
}

// TestDecodeRejectsTrailingBytes: a payload with trailing bytes is
// malformed (mirrors the used != len(payload) check in repl.py) — the
// router drops such frames without losing sync.
func TestDecodeRejectsTrailingBytes(t *testing.T) {
	payload, err := encodeMessage(map[string]any{"op": "ping"})
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	if _, err := decodeMessage(append(payload, 0xc0)); err == nil {
		t.Fatal("decodeMessage accepted trailing bytes")
	}
}
