package replclient

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// TestFsWriteSendsBinAndEnforcesCap: fs.write carries the bytes as one
// msgpack bin with the path; data above the 65472B single-op cap (the
// B15 §4.5 wire-realizable bound on protocol_ver=7 fw) is refused
// client-side pointing at the chunked face — the board sees no request
// at all.
func TestFsWriteSendsBinAndEnforcesCap(t *testing.T) {
	c, b := newTestClient(t)
	var gotPath string
	var gotData []byte
	b.on("fs.write", func(args map[string]any) (map[string]any, *boardError) {
		gotPath, _ = args["path"].(string)
		gotData, _ = args["data"].([]byte)
		return map[string]any{"size": int64(len(gotData))}, nil
	})

	data := bytes.Repeat([]byte{0xA5}, 65472)
	if err := c.FsWrite(context.Background(), "/lua/t.tmp", data); err != nil {
		t.Fatalf("fs.write 65472B: %v", err)
	}
	if gotPath != "/lua/t.tmp" || !bytes.Equal(gotData, data) {
		t.Fatalf("board saw path=%q len(data)=%d, want /lua/t.tmp 65472", gotPath, len(gotData))
	}

	before := len(b.got())
	err := c.FsWrite(context.Background(), "/lua/big.tmp", make([]byte, 65473))
	if err == nil {
		t.Fatal("65473B single-op write accepted")
	}
	if !strings.Contains(err.Error(), "65472") {
		t.Fatalf("cap error = %v, want it to name the 65472B cap", err)
	}
	if len(b.got()) != before {
		t.Fatal("over-cap write must not reach the board")
	}
}

// TestFsReadRoundTrip: fs.read returns the board's bin byte-exact; a
// missing file surfaces as the board's api error.
func TestFsReadRoundTrip(t *testing.T) {
	c, b := newTestClient(t)
	want := []byte{0, 1, 2, 0xFF, 0xFE, 'x'}
	b.on("fs.read", func(args map[string]any) (map[string]any, *boardError) {
		if args["path"] != "/lua/t.bin" {
			return nil, &boardError{class: "api", msg: "not found"}
		}
		return map[string]any{"data": want}, nil
	})

	got, err := c.FsRead(context.Background(), "/lua/t.bin")
	if err != nil {
		t.Fatalf("fs.read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("fs.read = %v, want %v", got, want)
	}

	_, err = c.FsRead(context.Background(), "/lua/missing.bin")
	var oe *OpError
	if !errors.As(err, &oe) || oe.Class != "api" {
		t.Fatalf("missing file err = %v, want api OpError", err)
	}
}
