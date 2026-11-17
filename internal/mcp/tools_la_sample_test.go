package mcp

// tools_la_sample_test.go — the samples seats (T13) against the
// scripted fake daemon: the samples capture's crossing shape
// (rate_hz/trig-ch ride the lane, listen columns absent), the decode
// tool's reply shape, and the Desc coaching audit for the frozen
// discipline of this wave.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestLACaptureSamplesCrossingShape: mode "samples" crosses
// rate_hz/trig-ch on the dedicated lane with NO listen/edge columns;
// the reply's meta rides verbatim.
func TestLACaptureSamplesCrossingShape(t *testing.T) {
	e, reqs := newEnvLA(t, []step{{result: map[string]any{
		"capture_id": "20261115T101530-97c1ac-la-sample-1", "path": "/p/data.bin",
		"meta": map[string]any{"cap_id": "s1", "mode": "samples", "rate_hz": 2e7,
			"samples": 5000.0, "t0_ns": 5e9, "pins": []any{11.0, 9.0, 8.0, 10.0, 12.0, 18.0, 17.0, 16.0}},
	}}})

	res := e.call("xuansi_la_capture", map[string]any{
		"board": "97c1ac", "mode": "samples", "rate_hz": 20000000, "ring_kb": 256, "seconds": 8,
		"trig": map[string]any{"ch": 3, "kind": "edge_f", "pre_kb": 4},
	})
	if res.IsError {
		t.Fatalf("samples capture failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "la_capture" || req.Mode != "samples" || req.RateHz != 20000000 ||
		req.LaPort != 0 || req.Baud != nil || len(req.Pins) != 0 || req.RingKB != 256 || req.MaxSeconds != 8 {
		t.Errorf("request = %+v, want mode samples with rate/ring and NO listen/edge columns", req)
	}
	if req.Trig == nil || req.Trig.Ch != 3 || req.Trig.Kind != "edge_f" || req.Trig.PreKB != 4 {
		t.Errorf("trig = %+v, want the edge_f channel trigger", req.Trig)
	}
	m := structured(t, res)
	if m["capture_id"] != "20261115T101530-97c1ac-la-sample-1" {
		t.Errorf("capture_id = %v", m["capture_id"])
	}
	if meta, _ := m["meta"].(map[string]any); meta["mode"] != "samples" || meta["rate_hz"] != 2e7 {
		t.Errorf("meta = %#v, want the samples ledger verbatim", m["meta"])
	}
}

// TestLADecodeBusReplyShape: the decode seat decodes the daemon reply
// into the frozen shape (rows, echoes, the optional columns'
// presence AND absence).
func TestLADecodeBusReplyShape(t *testing.T) {
	e, reqs := newEnv(t, []step{
		{result: map[string]any{ // la_decode_bus spi
			"proto": "spi", "mode": 0.0, "cs_mask": 8.0, "rate_hz": 2e7, "t0_ns": 5e9,
			"sck_half_ns": 1000.0, "n_txn": 3.0,
			"txns": []any{
				map[string]any{"t_ns": 5.0000001e9, "cs": 8.0, "n_bits": 16.0, "n_bytes": 2.0,
					"mosi": "03a5", "miso": "00ff"},
				map[string]any{"t_ns": 5.0000022e9, "cs": 8.0, "n_bits": 16.0, "n_bytes": 2.0,
					"mosi": "c303", "miso": "14a5"},
			}}},
		{result: map[string]any{ // la_decode_bus i2c
			"proto": "i2c", "rate_hz": 1e6, "t0_ns": 9e9, "n_txn": 1.0,
			"txns": []any{map[string]any{
				"t_ns": 9.0000001e9, "addr": 40.0, "rw": "w", "n_bits": 27.0, "n_bytes": 2.0,
				"bytes": "1234", "acks": []any{true, true, false}}}}},
		{result: map[string]any{ // la_decode_bus i2c override
			"proto": "i2c", "rate_hz": 1e6, "t0_ns": 9e9, "n_txn": 0.0, "txns": []any{}}},
	}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_la_decode_bus", map[string]any{
		"capture_id": "s1", "proto": "spi", "params": map[string]any{"mode": 0, "cs_mask": 8}})
	if res.IsError {
		t.Fatalf("xuansi_la_decode_bus failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "la_decode_bus" || req.Proto != "spi" || req.BusMode != 0 || req.CSMask != 8 {
		t.Errorf("decode request = %+v", req)
	}
	dm := structured(t, res)
	if dm["proto"] != "spi" || dm["cs_mask"] != 8.0 || dm["rate_hz"] != 2e7 ||
		dm["sck_half_ns"] != 1000.0 || dm["n_txn"] != 3.0 {
		t.Errorf("spi echo = %#v", dm)
	}
	rows, _ := dm["txns"].([]any)
	if len(rows) != 2 {
		t.Fatalf("spi rows = %d", len(rows))
	}
	if r := rows[0].(map[string]any); r["mosi"] != "03a5" || r["miso"] != "00ff" || r["cs"] != 8.0 {
		t.Errorf("spi row 0 = %#v", r)
	}
	if r := rows[1].(map[string]any); r["mosi"] != "c303" {
		t.Errorf("spi row 1 = %#v", r)
	}

	// the i2c shape: addr/rw/acks ride, the spi columns absent
	res = e.call("xuansi_la_decode_bus", map[string]any{
		"capture_id": "s2", "proto": "i2c"})
	if res.IsError {
		t.Fatalf("i2c decode failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Proto != "i2c" || req.SdaCh != nil || req.SclCh != nil {
		t.Errorf("i2c request = %+v, want the default channels", req)
	}
	im := structured(t, res)
	irows, _ := im["txns"].([]any)
	if len(irows) != 1 {
		t.Fatalf("i2c rows = %d", len(irows))
	}
	r := irows[0].(map[string]any)
	if r["addr"] != 40.0 || r["rw"] != "w" || r["bytes"] != "1234" {
		t.Errorf("i2c row = %#v", r)
	}
	if acks, _ := r["acks"].([]any); len(acks) != 3 || acks[2] != false {
		t.Errorf("i2c acks = %#v, want the final NAK", r["acks"])
	}
	if _, has := r["mosi"]; has {
		t.Errorf("i2c row carries a spi column: %#v", r)
	}

	// the channel override crosses as pointers
	sda, scl := 4, 5
	res = e.call("xuansi_la_decode_bus", map[string]any{
		"capture_id": "s2", "proto": "i2c",
		"params": map[string]any{"sda": sda, "scl": scl}})
	if res.IsError {
		t.Fatalf("override decode failed: %s", resultText(t, res))
	}
	req = wantReq(t, reqs)
	if req.SdaCh == nil || *req.SdaCh != 4 || req.SclCh == nil || *req.SclCh != 5 {
		t.Errorf("override request = %+v", req)
	}
}

// TestLASampleDescAudit: the frozen coaching discipline of the
// samples wave — the channel map (the wiring IS the interface), the
// SCK-edge alignment, the honest bounds, the next step.
func TestLASampleDescAudit(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	desc := map[string]string{}
	for _, tool := range lr.Tools {
		desc[tool.Name] = tool.Description
	}
	audit := map[string][]string{
		"xuansi_la_decode_bus": {
			"NOT xuansi_capture", "D_i", "SCK=D0", "CSN1=D3", "FIRST SCK edge",
			"sck_half_ns", "xuansi_la_measure", "64", "no lease",
		},
	}
	for name, wants := range audit {
		d := desc[name]
		if d == "" {
			t.Errorf("tool %s not served", name)
			continue
		}
		for _, want := range wants {
			if !strings.Contains(d, want) {
				t.Errorf("%s Desc lacks %q", name, want)
			}
		}
	}
	// the capture tool's Desc now names all three modes and the
	// samples exits
	for _, want := range []string{"\"samples\"", "LCD_CAM", "rate_hz", "six notches", "xuansi_la_decode_bus", "camera active"} {
		if !strings.Contains(desc["xuansi_la_capture"], want) {
			t.Errorf("xuansi_la_capture Desc lacks %q", want)
		}
	}
	// wave B supp (F-REV1): the capacity-semantics coach — the
	// E-window pair and what to do when it bites
	for _, want := range []string{"CAPACITY SEMANTICS", "zone_sat:true", "elapsed_ms", "lower rate_hz, raise ring_kb, or shorten the wait"} {
		if !strings.Contains(desc["xuansi_la_capture"], want) {
			t.Errorf("xuansi_la_capture Desc lacks %q", want)
		}
	}
}
