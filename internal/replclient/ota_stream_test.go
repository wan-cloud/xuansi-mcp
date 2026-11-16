package replclient

// ota_stream_test.go — the v10 uplink lane's ota cargo (OTA ops wave
// T6): OtaStreamWrite opens with ota.begin (ack carrying the target
// slot), rides the SAME pumps as FsStreamWrite (per-frame {received}
// acks, the source-shaped pacing), and lands on the staged auto-commit
// ({staged:true, slot, len, sha_ok:true} — the lane's terminal shape).
// The mock's uplink lane plays the fw's exact wire behavior with
// st.ota armed (fs_stream_test.go's mockUpstream).

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// scriptOta arms the mock board's ota upload lane.
func scriptOta(b *mockBoard, wantSha string, failCommit bool) *streamRecording {
	rec := &streamRecording{}
	st := &mockUpstream{ota: true, wantSha: wantSha, failCommit: failCommit, rec: rec}
	b.setUplink(st)
	return rec
}

func TestOtaStreamWriteHappyPath(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	data := make([]byte, 150000) // spans the TCP window at FsStreamChunk
	for i := range data {
		data[i] = byte(i*13 + 7)
	}
	rec := scriptOta(b, strings.Repeat("a", 64), false)
	slot, err := c.OtaStreamWrite(context.Background(), data, strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("OtaStreamWrite: %v", err)
	}
	if slot != "ota_1" {
		t.Fatalf("slot = %q, want ota_1 (the ack's and commit's label)", slot)
	}
	wantChunks := (len(data) + FsStreamChunk - 1) / FsStreamChunk
	if len(rec.countFrames()) != wantChunks {
		t.Fatalf("board saw %d frames, want %d", len(rec.countFrames()), wantChunks)
	}
	var all []byte
	for _, f := range rec.countFrames() {
		all = append(all, f...)
	}
	if string(all) != string(data) {
		t.Fatalf("uplink bytes diverge: %dB vs %dB", len(all), len(data))
	}
	for _, ch := range rec.chs {
		if ch != 253 {
			t.Fatalf("uplink frame ch=%d, want 253", ch)
		}
	}
}

func TestOtaStreamWriteCommitRefusal(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	scriptOta(b, strings.Repeat("a", 64), true)
	_, err := c.OtaStreamWrite(context.Background(), []byte("a plausible image body"), strings.Repeat("a", 64))
	if err == nil {
		t.Fatal("the staged refusal must surface")
	}
	var oe *OpError
	if !errors.As(err, &oe) || oe.Class != "api" || !strings.Contains(oe.Msg, "commit failed") {
		t.Fatalf("err = %v, want the board's api refusal verbatim", err)
	}
}

func TestOtaStreamWriteOpenRefused(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	b.handlers["ota.begin"] = func(args map[string]any) (map[string]any, *boardError) {
		return nil, &boardError{class: "api",
			msg: "ota busy: stream owned by another connection (ota.abort on that connection frees it)"}
	}
	_, err := c.OtaStreamWrite(context.Background(), []byte("image"), strings.Repeat("a", 64))
	var oe *OpError
	if !errors.As(err, &oe) || oe.Class != "api" || !strings.Contains(oe.Msg, "another connection") {
		t.Fatalf("err = %v, want the begin refusal verbatim", err)
	}
}

func TestOtaStreamWriteClientSideGuards(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	if _, err := c.OtaStreamWrite(context.Background(), nil, strings.Repeat("a", 64)); err == nil {
		t.Fatal("an empty image must refuse client-side (the fw refuses len=0)")
	}
	if _, err := c.OtaStreamWrite(context.Background(), []byte("image"), "short"); err == nil {
		t.Fatal("a short sha pin must refuse client-side (zero frames moved)")
	}
}

// TestOtaStreamWriteShaVerdictRequired: the staged reply without
// sha_ok (a board that skipped the pin verdict) is a shape breach.
func TestOtaStreamWriteShaVerdictRequired(t *testing.T) {
	b := newMockBoard(t)
	c := NewClient(b.Link)
	defer c.Close()

	scriptOta(b, "", false) // wantSha "" → the mock's staged reply lacks sha_ok
	_, err := c.OtaStreamWrite(context.Background(), []byte("image bytes"), strings.Repeat("b", 64))
	if err == nil || !strings.Contains(err.Error(), "sha_ok") {
		t.Fatalf("err = %v, want the missing-sha_ok breach named", err)
	}
}
