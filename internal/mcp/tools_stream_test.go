package mcp

// tools_stream_test.go — B16-T6 seat checks (the MCP-seat discipline:
// the seat's judgment face is what the tools ACTUALLY return): the
// stream_read crossing (typed frames decode, next/gap/armed) and the
// boot_receipts verbatim view.

import (
	"encoding/base64"
	"strings"
	"testing"
)

// TestStreamReadToolCrossing: the tool forwards stream_read {board, ch,
// max} and decodes the daemon's reply frames open — ch/seq/idx/total
// split + base64 payload, next/gap/armed intact.
func TestStreamReadToolCrossing(t *testing.T) {
	frameBody := []byte{2, 0, 0, 0, 7, 0, 0, 0, 1, 0xde, 0xad}
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"board":  "d75884",
		"ch":     float64(2),
		"frames": []any{map[string]any{"ch": float64(2), "seq": float64(7), "idx": float64(0), "total": float64(1), "data_b64": base64.StdEncoding.EncodeToString(frameBody[9:])}},
		"next":   float64(8),
		"gap":    float64(0),
		"armed":  []any{float64(2)},
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_stream_read", map[string]any{"board": "d75884", "ch": 2, "max": 8})
	if res.IsError {
		t.Fatalf("stream_read failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "stream_read" || req.Ch != 2 || req.Max != 8 {
		t.Fatalf("daemon request: %+v", req)
	}
	txt := resultText(t, res)
	for _, want := range []string{
		`"seq":7`, `"idx":0`, `"total":1`, `"next":8`, `"gap":0`, `"armed":[2]`,
		base64.StdEncoding.EncodeToString(frameBody[9:]),
	} {
		if !strings.Contains(txt, want) {
			t.Errorf("reply missing %q:\n%s", want, txt)
		}
	}
}

// TestBootReceiptsToolVerbatim: boot_receipts is the read-only verbatim
// crossing — the daemon's receipt view reaches the seat unreshaped
// (ground truth frozen at capture, not re-judged at a second seat).
func TestBootReceiptsToolVerbatim(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"board": "d75884",
		"receipts": []any{map[string]any{
			"at": "2026-11-13T05:00:00Z", "kind": "banner", "tier": "T0",
			"arbiter": "confirmed", "reset_raw": "0x15", "rom_ver": "20210327",
			"chip_rev": "0.2", "chip_match": true,
		}},
		"skeleton_lines": 21,
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_boot_receipts", map[string]any{"board": "d75884"})
	if res.IsError {
		t.Fatalf("boot_receipts failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "boot_receipts" || req.Board != "d75884" {
		t.Fatalf("daemon request: %+v", req)
	}
	txt := resultText(t, res)
	for _, want := range []string{`"reset_raw":"0x15"`, `"arbiter":"confirmed"`, `"chip_match":true`, `"skeleton_lines":21`} {
		if !strings.Contains(txt, want) {
			t.Errorf("reply missing %q:\n%s", want, txt)
		}
	}
}
