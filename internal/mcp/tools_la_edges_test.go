package mcp

// tools_la_edges_test.go — the edge seats (T11) against the scripted
// fake daemon: the edge capture's crossing shape (pins/trig ride the
// lane, port/baud absent), the six tools' reply shapes, and the Desc
// coaching audit for the frozen discipline of this wave.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// TestLACaptureEdgeCrossingShape: mode "edge" crosses pins/trig on
// the dedicated lane with NO listen columns; the reply's meta rides
// verbatim.
func TestLACaptureEdgeCrossingShape(t *testing.T) {
	e, reqs := newEnvLA(t, []step{{result: map[string]any{
		"capture_id": "20261115T101530-97c1ac-la-edge-1", "path": "/p/data.bin",
		"meta": map[string]any{"cap_id": "e1", "mode": "edge", "pins": []any{2.0, 39.0}, "res_hz": 8e7, "t0_ns": 5e9, "runs": 2147.0},
	}}})

	res := e.call("xuansi_la_capture", map[string]any{
		"board": "97c1ac", "mode": "edge", "pins": []int{2, 39}, "ring_kb": 128, "seconds": 4,
		"trig": map[string]any{"pin": 2, "kind": "edge_r", "pre_kb": 4},
	})
	if res.IsError {
		t.Fatalf("edge capture failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "la_capture" || req.Mode != "edge" || req.Holder != "mcp-fixed01" ||
		req.LaPort != 0 || req.Baud != nil || req.LaPin != 0 {
		t.Errorf("request = %+v, want mode edge with NO listen columns", req)
	}
	if len(req.Pins) != 2 || req.Pins[0] != 2 || req.Pins[1] != 39 || req.RingKB != 128 || req.MaxSeconds != 4 {
		t.Errorf("edge arm columns = %+v", req)
	}
	if req.Trig == nil || req.Trig.Pin != 2 || req.Trig.Kind != "edge_r" || req.Trig.PreKB != 4 {
		t.Errorf("trig = %+v, want the edge_r trigger", req.Trig)
	}
	m := structured(t, res)
	if m["capture_id"] != "20261115T101530-97c1ac-la-edge-1" {
		t.Errorf("capture_id = %v", m["capture_id"])
	}
	if meta, _ := m["meta"].(map[string]any); meta["mode"] != "edge" || meta["t0_ns"] != 5e9 {
		t.Errorf("meta = %#v, want the edge ledger verbatim", m["meta"])
	}
}

// TestLAEdgeToolReplyShapes: the six seats decode the daemon replies
// into the frozen shapes (rows, echoes, optional columns' presence
// AND absence).
func TestLAEdgeToolReplyShapes(t *testing.T) {
	e, reqs := newEnv(t, []step{
		{result: map[string]any{ // la_edges
			"edges": []any{
				map[string]any{"t_ns": 5e9, "level": 1.0, "dur_ns": 200000.0},
				map[string]any{"t_ns": 5.0002e9, "level": 0.0, "dur_ns": 13000.0},
			}, "t0_ns": 5e9, "n_runs": 2147.0, "truncated": true}},
		{result: map[string]any{ // la_runs
			"runs": []any{
				map[string]any{"i": 0.0, "t_ns": 5e9, "level": 1.0, "dur_ns": 200000.0},
			}, "t0_ns": 5e9, "n_runs": 2147.0, "span_ns": 71463000.0}},
		{result: map[string]any{ // la_hist
			"kind": "pulse", "mode_ns": 13158.0, "est_baud": 75996.4,
			"duty": 0.487, "n_samples": 2147.0,
			"bins": []any{
				map[string]any{"lo_ns": 13157.0, "hi_ns": 13160.0, "n": 1100.0},
				map[string]any{"lo_ns": 561000.0, "hi_ns": 561003.0, "n": 32.0},
			}}},
		{result: map[string]any{ // la_measure
			"dt_ns": 2000000.0, "freq_hz": 500.0, "duty": 0.25}},
		{result: map[string]any{ // la_diff same
			"same": true, "n_runs_a": 2147.0, "n_runs_b": 2149.0,
			"matched_ns": 71460000.0, "tol_ns": 2000.0}},
		{result: map[string]any{ // la_decode_wire
			"proto": "ws2812",
			"txns": []any{map[string]any{
				"t_ns": 8.0001e9, "kind": "frame", "n_bits": 48.0, "n_bytes": 6.0,
				"n_leds": 2.0, "remainder_bits": 0.0, "bytes": "123456abcdef", "latched": true}},
		}},
	}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_la_edges", map[string]any{"capture_id": "e1", "from_ns": 5e9, "to_ns": 5.1e9, "max": 2})
	if res.IsError {
		t.Fatalf("xuansi_la_edges failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Type != "la_edges" || req.FromNS != 5e9 || req.ToNS != 5.1e9 || req.Max != 2 {
		t.Errorf("edges request = %+v", req)
	}
	em := structured(t, res)
	if em["t0_ns"] != 5e9 || em["n_runs"] != 2147.0 || em["truncated"] != true {
		t.Errorf("edges echo = %#v", em)
	}
	rows, _ := em["edges"].([]any)
	if len(rows) != 2 {
		t.Fatalf("edges rows = %d", len(rows))
	}
	if r := rows[1].(map[string]any); r["level"] != 0.0 || r["dur_ns"] != 13000.0 {
		t.Errorf("edges row 1 = %#v", r)
	}

	res = e.call("xuansi_la_runs", map[string]any{"capture_id": "e1"})
	rm := structured(t, res)
	if rm["span_ns"] != 71463000.0 || rm["n_runs"] != 2147.0 {
		t.Errorf("runs echo = %#v", rm)
	}
	if rrows, _ := rm["runs"].([]any); len(rrows) != 1 || rrows[0].(map[string]any)["i"] != 0.0 {
		t.Errorf("runs rows = %#v", rm["runs"])
	}

	res = e.call("xuansi_la_hist", map[string]any{"capture_id": "e1", "kind": "pulse", "bins": 32})
	hm := structured(t, res)
	if hm["mode_ns"] != 13158.0 || hm["est_baud"] != 75996.4 || hm["duty"] != 0.487 || hm["n_samples"] != 2147.0 {
		t.Errorf("hist reply = %#v", hm)
	}
	if bins, _ := hm["bins"].([]any); len(bins) != 2 || bins[0].(map[string]any)["n"] != 1100.0 {
		t.Errorf("hist bins = %#v", hm["bins"])
	}

	res = e.call("xuansi_la_measure", map[string]any{
		"capture_id": "e1", "a": map[string]any{"edge_i": 0}, "b": map[string]any{"edge_i": 4}})
	mm := structured(t, res)
	if mm["dt_ns"] != 2000000.0 || mm["freq_hz"] != 500.0 || mm["duty"] != 0.25 {
		t.Errorf("measure reply = %#v", mm)
	}

	res = e.call("xuansi_la_diff", map[string]any{"capture_a": "e1", "capture_b": "e2"})
	dm := structured(t, res)
	if dm["same"] != true || dm["matched_ns"] != 71460000.0 || dm["tol_ns"] != 2000.0 {
		t.Errorf("diff reply = %#v", dm)
	}
	if _, has := dm["first_diff_t_ns"]; has {
		t.Errorf("same diff carried first_diff_t_ns: %#v", dm)
	}

	res = e.call("xuansi_la_decode_wire", map[string]any{"capture_id": "e1", "proto": "ws2812"})
	wm := structured(t, res)
	txns, _ := wm["txns"].([]any)
	if len(txns) != 1 {
		t.Fatalf("wire txns = %#v", wm["txns"])
	}
	tx := txns[0].(map[string]any)
	if tx["bytes"] != "123456abcdef" || tx["n_leds"] != 2.0 || tx["latched"] != true {
		t.Errorf("wire txn = %#v", tx)
	}
}

// TestLAEdgeDiffDivergenceShape: the divergence reply carries the
// optional columns through (run_index may equal n_runs_a on length).
func TestLAEdgeDiffDivergenceShape(t *testing.T) {
	e, _ := newEnv(t, []step{{result: map[string]any{
		"same": false, "first_diff_t_ns": 5.0002e9, "run_index": 8.0, "run_index_b": 8.0,
		"why": "length", "n_runs_a": 8.0, "n_runs_b": 12.0, "matched_ns": 3750000.0, "tol_ns": 2000.0,
	}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_la_diff", map[string]any{"capture_a": "a", "capture_b": "b"})
	dm := structured(t, res)
	if dm["same"] != false || dm["why"] != "length" || dm["run_index"] != 8.0 || dm["run_index_b"] != 8.0 {
		t.Errorf("divergence reply = %#v", dm)
	}
}

// TestLAEdgeCrossBoardRefusalCrosses: the same-board coach rides the
// error verbatim.
func TestLAEdgeCrossBoardRefusalCrosses(t *testing.T) {
	coach := `la_diff: diff refuses captures from different boards (97c1ac vs d75884) — the comparison rides one board's clock; capture both views on the same board (e.g. two edge captures of one transmission)`
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{Class: "api", Msg: coach}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_la_diff", map[string]any{"capture_a": "a", "capture_b": "b"})
	if !res.IsError {
		t.Fatalf("cross-board diff did not surface: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "same board") {
		t.Errorf("error text = %q, want the same-board coach", got)
	}
}

// TestLAEdgesDescAudit (the frozen Desc discipline, edge wave): every
// served Desc carries its next-step tool names and the wave's honest
// bounds — the window semantics, the row caps, the diff tolerance,
// the decoder thresholds.
func TestLAEdgesDescAudit(t *testing.T) {
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
		"xuansi_la_edges": {
			"NOT xuansi_capture", "MASK", "t0_ns", "256", "4096", "truncated",
			"xuansi_la_hist", "xuansi_la_measure", "xuansi_la_diff", "no lease",
		},
		"xuansi_la_runs": {
			"i", "edge_i", "run_index", "n_runs", "span_ns", "256", "xuansi_la_measure", "no lease",
		},
		"xuansi_la_hist": {
			"mode_ns", "est_baud", "duty", "period", "median", "LOWER bin", "xuansi_la_measure", "no lease",
		},
		"xuansi_la_measure": {
			"SIGNED", "edge_i", "PRO-RATA", "freq_hz", "duty", "xuansi_la_runs", "no lease",
			// minors wave: the float64 precision floor — large t_ns
			// values lose exactness above 2^53; edge_i is the exact form
			"2^53", "float64",
		},
		"xuansi_la_diff": {
			"FIRST-EDGE ALIGNED", "2000ns", "why", "length", "SAME-BOARD", "matched_ns", "xuansi_la_runs", "no lease",
		},
		"xuansi_la_decode_wire": {
			"onewire", "ws2812", "240", "32", "450", "50", "ONE line", "64", "no lease",
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
	// the capture tool's Desc now names both modes and the edge exits
	for _, want := range []string{"\"edge\"", "RMT", "ring_kb", "trig", "xuansi_la_edges", "xuansi_la_diff"} {
		if !strings.Contains(desc["xuansi_la_capture"], want) {
			t.Errorf("xuansi_la_capture Desc lacks %q", want)
		}
	}
}
