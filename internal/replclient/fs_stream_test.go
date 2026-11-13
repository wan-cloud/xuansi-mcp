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
	"testing"
	"time"
)

// streamRecording captures what the mock board saw on the uplink lane.
type streamRecording struct {
	frames   [][]byte // concatenated payload bytes per data frame
	chs      []byte
	fatalMsg string
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
	st.rec.frames = append(st.rec.frames, append([]byte{}, payload...))
	st.rec.chs = append(st.rec.chs, body[0])
	st.received += len(payload)
	if st.killAfterFrame > 0 && len(st.rec.frames) >= st.killAfterFrame && st.kill != nil {
		st.kill.conn.Close()
		return true // consumed but the link is dying
	}
	if int64(st.received) < st.expected {
		b.send(map[string]any{"op": "reply", "id": st.id,
			"result": map[string]any{"received": int64(st.received)}})
		return true
	}
	// auto-commit (the fw's frozen §17 verdict shape)
	res := map[string]any{"committed": true, "path": st.path,
		"len": st.expected}
	if st.wantSha != "" {
		res["sha_ok"] = true
	}
	if st.failCommit {
		b.send(map[string]any{"op": "reply", "id": st.id,
			"error": map[string]any{"class": "api",
				"msg": "stream commit failed (scripted)"}})
		return true
	}
	b.send(map[string]any{"op": "reply", "id": st.id, "result": res})
	return true
}

var _ = time.Second
