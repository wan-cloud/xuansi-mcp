package replclient

// fs_stream_test.go — the v8 upload lane's client half (B16-T7 C2):
// FsStreamWrite opens a stream with fs.stream_write (ack), pumps the
// payload as ch=253 0x02 frames ack-paced (the CDC driver drops bytes
// past its 12KB ring — back-to-back frames desync; the per-frame
// {received} ack is the flow control), and lands the file on the
// expected_len auto-commit (the second reply shape carrying the open
// request's id). The mock board plays the fw's exact wire behavior:
// uplink frames consumed only after fs.stream_write (§7 outside it),
// ch checked, per-frame acks, commit with sha verdict.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// streamRecording captures what the mock board saw on the uplink lane.
// mu guards the appends (the serve goroutine) against concurrent flight
// polling from test goroutines (the window tests watch frames arrive
// while the pump is still mid-upload).
type streamRecording struct {
	mu       sync.Mutex
	frames   [][]byte // concatenated payload bytes per data frame
	chs      []byte
	fatalMsg string
}

// count reports how many frames arrived so far (safe while the pump runs).
func (r *streamRecording) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.frames)
}

// scriptStream arms the mock board's upload lane for path: the open
// reply (ack or direct commit), per-frame {received} acks, and the
// auto-commit with the sha verdict over what actually arrived.
func scriptStream(b *mockBoard, path string, wantSha string, failCommit bool) *streamRecording {
	rec := &streamRecording{}
	st := &mockUpstream{path: path, wantSha: wantSha, failCommit: failCommit, rec: rec}
	b.setUplink(st)
	return rec
}

func TestFsStreamWriteHappyPath(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	data := make([]byte, 150000)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	rec := scriptStream(b, "/lua/big.tmp", "", false)
	if err := c.FsStreamWrite(context.Background(), "/lua/big.tmp", data, ""); err != nil {
		t.Fatalf("FsStreamWrite: %v", err)
	}
	// frames: chunk count at the lane's chunk size, byte-exact union
	wantChunks := (len(data) + FsStreamChunk - 1) / FsStreamChunk
	if len(rec.frames) != wantChunks {
		t.Fatalf("board saw %d frames, want %d", len(rec.frames), wantChunks)
	}
	var all []byte
	for _, f := range rec.frames {
		all = append(all, f...)
	}
	if string(all) != string(data) {
		t.Fatalf("uplink bytes diverge: got %d bytes, want %d", len(all), len(data))
	}
	for _, ch := range rec.chs {
		if ch != 253 {
			t.Fatalf("uplink frame ch=%d, want 253", ch)
		}
	}
}

func TestFsStreamWriteShaVerdict(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	data := []byte("stream me across the lane")
	rec := scriptStream(b, "/lua/x.tmp", "sha-will-be-computed", false)
	if err := c.FsStreamWrite(context.Background(), "/lua/x.tmp", data, "sha-will-be-computed"); err != nil {
		t.Fatalf("FsStreamWrite: %v", err)
	}
	if len(rec.frames) != 1 || string(rec.frames[0]) != string(data) {
		t.Fatalf("frames = %v", rec.frames)
	}
}

func TestFsStreamWriteCommitRefusal(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	scriptStream(b, "/lua/y.tmp", "", true)
	err := c.FsStreamWrite(context.Background(), "/lua/y.tmp", []byte("payload"), "")
	if err == nil {
		t.Fatal("commit refusal must surface")
	}
	var oe *OpError
	if !errors.As(err, &oe) || oe.Class != "api" {
		t.Fatalf("err = %v, want api OpError", err)
	}
	if !strings.Contains(oe.Msg, "stream commit failed") {
		t.Fatalf("msg = %q", oe.Msg)
	}
}

func TestFsStreamWriteDeadMidPump(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	data := make([]byte, 3*FsStreamChunk)
	st := &mockUpstream{path: "/lua/z.tmp", rec: &streamRecording{}, killAfterFrame: 1, kill: b}
	b.setUplink(st)
	err := c.FsStreamWrite(context.Background(), "/lua/z.tmp", data, "")
	if err == nil {
		t.Fatal("dead link mid-pump must surface")
	}
}

func TestFsStreamWriteEmptyFile(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	rec := scriptStream(b, "/lua/empty.tmp", "", false)
	if err := c.FsStreamWrite(context.Background(), "/lua/empty.tmp", nil, ""); err != nil {
		t.Fatalf("empty stream: %v", err)
	}
	if len(rec.frames) != 0 {
		t.Fatalf("empty stream sent %d frames", len(rec.frames))
	}
}

func TestFsStreamWriteOpenRefused(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	b.handlers["fs.stream_write"] = func(args map[string]any) (map[string]any, *boardError) {
		return nil, &boardError{class: "api", msg: "stream already active on this connection (path '/lua/a.tmp') - fs.stream_abort it first"}
	}
	err := c.FsStreamWrite(context.Background(), "/lua/b.tmp", []byte("x"), "")
	var oe *OpError
	if !errors.As(err, &oe) || oe.Class != "api" ||
		!strings.Contains(oe.Msg, "already active") {
		t.Fatalf("err = %v, want the board's refusal verbatim", err)
	}
}

// usbKindLink masks the mock's pipe as a CDC source (Kind "usb") — the
// pump discipline is per source and the strict leg must hold there.
type usbKindLink struct{ Link }

func (usbKindLink) Kind() string { return "usb" }

// parkAcks_ arms a stream's reply parking (see deliverStreamReply).
func (b *mockBoard) parkAcks_(st *mockUpstream) {
	b.mu.Lock()
	st.parkAcks = true
	b.mu.Unlock()
}

// TestFsStreamWriteTCPWindowFlight: the TCP pump keeps
// uplinkWindowTCP frames on the wire with ZERO acks returned — the
// flight window is real (strict stop-and-wait would park at 1) — and
// never exceeds it; releasing the parked acks completes the upload
// byte-exact with every per-frame ack consumed in order.
func TestFsStreamWriteTCPWindowFlight(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	const frames = 20
	data := make([]byte, frames*FsStreamChunk)
	for i := range data {
		data[i] = byte(i*11 + 5)
	}
	st := &mockUpstream{path: "/lua/w.tmp", rec: &streamRecording{}}
	b.setUplink(st)
	b.parkAcks_(st)

	type res struct {
		err error
	}
	done := make(chan res, 1)
	go func() { done <- res{c.FsStreamWrite(context.Background(), "/lua/w.tmp", data, "")} }()

	// zero acks sent, yet the window fills: exactly uplinkWindowTCP
	// frames arrive at the board (parked replies prove none came back).
	waitFor(t, func() bool { return st.rec.count() >= uplinkWindowTCP })
	time.Sleep(60 * time.Millisecond) // give a would-be 7th frame time to misbehave
	if n := st.rec.count(); n != uplinkWindowTCP {
		t.Fatalf("flight depth with zero acks = %d, want exactly %d", n, uplinkWindowTCP)
	}
	b.unparkStreamAcks(st)

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("FsStreamWrite: %v", r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("upload did not finish after ack release")
	}
	got := st.rec.countFrames()
	if len(got) != frames {
		t.Fatalf("board saw %d frames, want %d", len(got), frames)
	}
	var all []byte
	for _, f := range got {
		all = append(all, f...)
	}
	if string(all) != string(data) {
		t.Fatalf("uplink bytes diverge: %dB vs %dB", len(all), len(data))
	}
}

// TestFsStreamWriteUSBStrictPacing: the CDC pump stays strict
// stop-and-wait — with the first frame's ack parked, a second frame
// must NOT reach the board (the 12KB RX ring discipline: one un-acked
// frame is the most a USB link may have on the wire).
func TestFsStreamWriteUSBStrictPacing(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(usbKindLink{b.Link})
	defer c.Close()

	data := make([]byte, 3*FsStreamChunk)
	st := &mockUpstream{path: "/lua/u.tmp", rec: &streamRecording{}}
	b.setUplink(st)
	b.parkAcks_(st)

	done := make(chan error, 1)
	go func() { done <- c.FsStreamWrite(context.Background(), "/lua/u.tmp", data, "") }()

	waitFor(t, func() bool { return st.rec.count() >= 1 })
	time.Sleep(80 * time.Millisecond)
	if n := st.rec.count(); n != 1 {
		t.Fatalf("strict source put %d frames on the wire before the first ack, want 1", n)
	}
	b.unparkStreamAcks(st)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("FsStreamWrite: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("upload did not finish after ack release")
	}
	if n := st.rec.count(); n != 3 {
		t.Fatalf("board saw %d frames, want 3", n)
	}
}

// TestFsStreamWriteWindowAckGap: an ack that does not name exactly the
// next in-flight frame's end is refused — the ledger is strict even
// with a window open (a cumulative count skipping ahead or lagging is
// a protocol breach, not something to resync around).
func TestFsStreamWriteWindowAckGap(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	data := make([]byte, 4*FsStreamChunk)
	st := &mockUpstream{path: "/lua/g.tmp", rec: &streamRecording{}, skew: 7}
	b.setUplink(st)

	err := c.FsStreamWrite(context.Background(), "/lua/g.tmp", data, "")
	if err == nil || !strings.Contains(err.Error(), "ack received=") {
		t.Fatalf("err = %v, want the ack-ledger refusal", err)
	}
}

func TestUplinkFrameShape(t *testing.T) {
	p := []byte{1, 2, 3}
	f := uplinkFrame(7, p)
	if len(f) != 4+1+1+4+3 {
		t.Fatalf("len = %d", len(f))
	}
	if f[4] != 0x02 || f[5] != 253 {
		t.Fatalf("type/ch = %#x/%d", f[4], f[5])
	}
	if f[6] != 0 || f[7] != 0 || f[8] != 0 || f[9] != 7 {
		t.Fatalf("seq bytes = %v", f[6:10])
	}
	if string(f[10:]) != string(p) {
		t.Fatal("payload tail mismatch")
	}
	// the length prefix counts type+subheader+payload (§16 rule)
	got := int(f[0])<<24 | int(f[1])<<16 | int(f[2])<<8 | int(f[3])
	if got != 1+5+len(p) {
		t.Fatalf("len prefix = %d", got)
	}
}

// mockUpstream is the mock board's upload-lane state for one stream
// (mirrors the fw: one active stream per connection).
type mockUpstream struct {
	path           string
	wantSha        string
	failCommit     bool
	rec            *streamRecording
	received       int
	expected       int64
	id             any
	killAfterFrame int // 0 = never; 1+ = kill the link after N frames
	kill           *mockBoard

	parkAcks bool             // park replies instead of sending (flight tests)
	pending  []map[string]any // parked replies (under mockBoard.mu)
	skew     int              // report received+N in acks (the ack-gap test)
}

// setUplink arms (or with nil clears) the board's upload lane; the
// fs.stream_write handler and the 0x02 consumption path both consult
// it. Outside an armed stream a host→board 0x02 stays the §7 fatal.
func (b *mockBoard) setUplink(st *mockUpstream) {
	b.mu.Lock()
	b.uplink = st
	b.mu.Unlock()
	if st != nil {
		prev := b.handlers["fs.stream_write"]
		b.handlers["fs.stream_write"] = func(args map[string]any) (map[string]any, *boardError) {
			if prev != nil {
				// a test's own script wins (the refusal tests)
				if _, berr := prev(args); berr != nil {
					return nil, berr
				}
			}
			b.mu.Lock()
			if n := len(b.requests); n > 0 {
				st.id = b.requests[n-1].id
			}
			b.mu.Unlock()
			st.expected, _ = args["expected_len"].(int64)
			st.received = 0
			if st.expected == 0 {
				return map[string]any{
					"committed": true, "path": st.path, "len": int64(0),
				}, nil
			}
			return map[string]any{
				"streaming": true, "path": st.path,
				"expected_len": st.expected,
			}, nil
		}
	}
}

// consumeUplink: one host→board 0x02 body ([ch][seq][payload] — the
// reader strips the type byte) against the armed stream. Per-frame ack
// {received} or the auto-commit; false = not a legal uplink frame (the
// §7 fatal the mock then plays, exactly like the pre-v8 fw).
func (b *mockBoard) consumeUplink(body []byte) bool {
	b.mu.Lock()
	st := b.uplink
	b.mu.Unlock()
	if st == nil || len(body) < 5 {
		return false
	}
	if body[0] != 253 {
		return false
	}
	payload := body[5:]
	st.rec.mu.Lock()
	st.rec.frames = append(st.rec.frames, append([]byte{}, payload...))
	st.rec.chs = append(st.rec.chs, body[0])
	st.rec.mu.Unlock()
	st.received += len(payload)
	if st.killAfterFrame > 0 && len(st.rec.countFrames()) >= st.killAfterFrame && st.kill != nil {
		st.kill.conn.Close()
		return true // consumed but the link is dying
	}
	if int64(st.received) < st.expected {
		rep := map[string]any{"op": "reply", "id": st.id,
			"result": map[string]any{"received": int64(st.received + st.skew)}}
		b.deliverStreamReply(st, rep)
		return true
	}
	// auto-commit (the fw's frozen §17 verdict shape)
	res := map[string]any{"committed": true, "path": st.path,
		"len": st.expected}
	if st.wantSha != "" {
		res["sha_ok"] = true
	}
	if st.failCommit {
		b.deliverStreamReply(st, map[string]any{"op": "reply", "id": st.id,
			"error": map[string]any{"class": "api",
				"msg": "stream commit failed (scripted)"}})
		return true
	}
	b.deliverStreamReply(st, map[string]any{"op": "reply", "id": st.id, "result": res})
	return true
}

// countFrames snapshots the frame slice under the recording's lock.
func (r *streamRecording) countFrames() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.frames))
	copy(out, r.frames)
	return out
}

// deliverStreamReply sends one lane reply now, or parks it when the
// stream's acks are parked (the flight tests hold replies back to
// observe how many frames the client puts on the wire un-acked).
func (b *mockBoard) deliverStreamReply(st *mockUpstream, rep map[string]any) {
	b.mu.Lock()
	park := st.parkAcks
	if park {
		st.pending = append(st.pending, rep)
	}
	b.mu.Unlock()
	if !park {
		b.send(rep)
	}
}

// unparkStreamAcks releases every parked reply in order and lets later
// replies flow direct. The whole flush holds the write mutex so the
// ordered pending sequence can never interleave with a newer reply
// (the client would read a later ack first and rightly refuse it).
func (b *mockBoard) unparkStreamAcks(st *mockUpstream) {
	b.wmu.Lock()
	defer b.wmu.Unlock()
	b.mu.Lock()
	st.parkAcks = false
	pending := st.pending
	st.pending = nil
	b.mu.Unlock()
	for _, m := range pending {
		payload, err := encodeMessage(m)
		if err != nil {
			continue
		}
		f, err := frame(payload)
		if err != nil {
			continue
		}
		b.conn.Write(f)
	}
}

var _ = time.Second
