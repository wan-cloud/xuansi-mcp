package mcp

// tools_la_wave2_test.go — the LA second analysis batch (LA 增強波 A,
// 2026-11-17) against the scripted fake daemon: the eight seats'
// request crossings and reply shapes (the wave's frozen columns),
// then the Desc coaching audit pinning the wave's frozen statements.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// TestLAWave2RequestCrossings: each seat crosses its typed columns to
// the daemon request verbatim (the grab-bag fields ride their names).
func TestLAWave2RequestCrossings(t *testing.T) {
	minNS, maxNS, level := uint64(1_000), uint64(50_000), 1
	ratio := 0.25
	e, reqs := newEnv(t, []step{
		{result: map[string]any{"kind": "timing", "n_hits": 0.0}},
		{result: map[string]any{"mode": "edge", "bin_ns": 1e6, "n_edges": 39.0}},
		{result: map[string]any{"ch": 0.0, "rate_ns": 100000.0}},
		{result: map[string]any{"ch": 1.0, "window_ns": 10000.0, "flips": 15.0}},
		{result: map[string]any{"ch": 0.0, "modal_run_ns": 100000.0, "ratio": 0.5, "threshold_ns": 50000.0, "count": 1.0}},
		{result: map[string]any{"ch": 0.0, "period_ns": 1e6, "n_fold": 38.0}},
		{result: map[string]any{"period_a_ns": 250000.0, "period_b_ns": 250000.0, "lag_ns": 123456.0, "tol_ns": 31250.0, "matched": 39.0, "n_events_a": 39.0, "n_events_b": 39.0}},
		{result: map[string]any{"path": "/p/export.vcd", "bytes": 1234.0, "changes": 39.0}},
	}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_la_search", map[string]any{
		"capture_id": "e1", "kind": "timing", "min_ns": 1000, "max_ns": 50000, "level": 1,
		"ch": 0, "from_ns": 5e9, "to_ns": 6e9, "max": 10})
	if res.IsError {
		t.Fatalf("search failed: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "la_search" || req.Kind != "timing" || req.Ch != 0 ||
		req.FromNS != 5e9 || req.ToNS != 6e9 || req.Max != 10 ||
		req.MinNS == nil || *req.MinNS != minNS || req.MaxNS == nil || *req.MaxNS != maxNS ||
		req.Level == nil || *req.Level != level {
		t.Errorf("search request = %+v", req)
	}

	res = e.call("xuansi_la_activity", map[string]any{
		"capture_id": "e1", "bin_ms": 1, "max_bins": 64, "from_ns": 5e9})
	if res.IsError {
		t.Fatalf("activity failed: %s", resultText(t, res))
	}
	if req = wantReq(t, reqs); req.Type != "la_activity" || req.BinMS != 1 || req.MaxBins != 64 || req.FromNS != 5e9 {
		t.Errorf("activity request = %+v", req)
	}

	res = e.call("xuansi_la_bits", map[string]any{"capture_id": "e1", "ch": 0, "rate": 10000})
	if res.IsError {
		t.Fatalf("bits failed: %s", resultText(t, res))
	}
	if req = wantReq(t, reqs); req.Type != "la_bits" || req.Ch != 0 || req.RateHz != 10000 {
		t.Errorf("bits request = %+v", req)
	}

	res = e.call("xuansi_la_adjacent", map[string]any{"capture_id": "s1", "ch": 1, "window_ns": 10000})
	if res.IsError {
		t.Fatalf("adjacent failed: %s", resultText(t, res))
	}
	if req = wantReq(t, reqs); req.Type != "la_adjacent" || req.Ch != 1 || req.WindowNS != 10000 {
		t.Errorf("adjacent request = %+v", req)
	}

	res = e.call("xuansi_la_glitch", map[string]any{"capture_id": "e1", "ch": 0, "ratio": 0.25})
	if res.IsError {
		t.Fatalf("glitch failed: %s", resultText(t, res))
	}
	if req = wantReq(t, reqs); req.Type != "la_glitch" || req.Ch != 0 || req.Ratio == nil || *req.Ratio != ratio {
		t.Errorf("glitch request = %+v", req)
	}

	res = e.call("xuansi_la_eye", map[string]any{"capture_id": "e1", "ch": 0})
	if res.IsError {
		t.Fatalf("eye failed: %s", resultText(t, res))
	}
	if req = wantReq(t, reqs); req.Type != "la_eye" || req.Ch != 0 {
		t.Errorf("eye request = %+v", req)
	}

	res = e.call("xuansi_la_align", map[string]any{"capture_a": "e1", "capture_b": "e2"})
	if res.IsError {
		t.Fatalf("align failed: %s", resultText(t, res))
	}
	if req = wantReq(t, reqs); req.Type != "la_align" || req.CaptureID != "e1" || req.CaptureIDB != "e2" {
		t.Errorf("align request = %+v", req)
	}

	res = e.call("xuansi_la_export", map[string]any{"capture_id": "e1", "fmt": "vcd"})
	if res.IsError {
		t.Fatalf("export failed: %s", resultText(t, res))
	}
	if req = wantReq(t, reqs); req.Type != "la_export" || req.Fmt != "vcd" {
		t.Errorf("export request = %+v", req)
	}
	// the export input schema carries NO path parameter (the frozen
	// anti-traversal statement)
	em := structured(t, res)
	if em["path"] != "/p/export.vcd" {
		t.Errorf("export reply = %#v", em)
	}
}

// TestLAWave2ReplyShapes: the seats decode the daemon replies into
// the frozen shapes (the wave's optional columns: presence AND
// absence).
func TestLAWave2ReplyShapes(t *testing.T) {
	e, _ := newEnv(t, []step{
		{result: map[string]any{ // la_search bits
			"kind": "bits", "pattern": "011", "ch": 0.0, "n_hits": 2.0,
			"hits": []any{
				map[string]any{"run_i": 1.0, "bit_off": 0.0, "t_ns": 6.0001e9, "level": 0.0, "dur_ns": 100000.0, "context": "0110010"},
				map[string]any{"run_i": 8.0, "bit_off": 9.0, "t_ns": 6.001e9, "level": 1.0, "dur_ns": 200000.0, "context": "101100101"},
			}, "truncated": true}},
		{result: map[string]any{ // la_activity
			"mode": "listen", "bin_ns": 5e6, "n_edges": 4.0,
			"busiest_from_ns": 5e6,
			"bins": []any{
				map[string]any{"from_ns": 0.0, "to_ns": 5e6, "edges": 1.0, "bytes": 1.0, "ratio": 0.5},
				map[string]any{"from_ns": 5e6, "to_ns": 1e7, "edges": 2.0, "bytes": 2.0, "ratio": 1.0},
			}}},
		{result: map[string]any{ // la_bits
			"ch": 0.0, "rate_ns": 100000.0, "rate_derived": true, "first_t_ns": 6.0001e9,
			"n_bits": 35.0, "violations": 2.0, "first_violation_t_ns": 6.0032e9, "truncated": true,
			"segments": []any{map[string]any{"i": 0.0, "t_ns": 6.0001e9, "bits": "01100101011001010110010101100101011"}}}},
		{result: map[string]any{ // la_adjacent
			"ch": 0.0, "window_ns": 10000.0, "flips": 15.0,
			"peers": []any{
				map[string]any{"ch": 1.0, "count": 15.0, "ratio": 1.0},
				map[string]any{"ch": 2.0, "count": 0.0, "ratio": 0.0},
			}}},
		{result: map[string]any{ // la_glitch
			"ch": 0.0, "modal_run_ns": 100000.0, "ratio": 0.5, "threshold_ns": 50000.0,
			"count": 1.0, "truncated": false,
			"rows": []any{map[string]any{"run_i": 25.0, "t_ns": 6.00334e9, "level": 0.0, "dur_ns": 30000.0}}}},
		{result: map[string]any{ // la_eye
			"ch": 0.0, "period_ns": 1e6, "n_fold": 38.0,
			"min_high_ns": 250000.0, "max_high_ns": 250000.0, "high_pp_ns": 0.0,
			"min_low_ns": 750000.0, "max_low_ns": 750000.0, "low_pp_ns": 0.0}},
		{result: map[string]any{ // la_align
			"period_a_ns": 250000.0, "period_b_ns": 250000.0, "lag_ns": -15433.0,
			"tol_ns": 31250.0, "matched": 39.0, "n_events_a": 39.0, "n_events_b": 39.0,
			"drift_ppm": 996.4,
			"segments": []any{
				map[string]any{"from_ns": 4.00025e9, "n": 5.0, "offset_ns": -11.5},
				map[string]any{"from_ns": 4.0016875e9, "n": 6.0, "offset_ns": 8.25},
			}}},
		{result: map[string]any{ // la_export
			"path": "/captures/la/e1/export.vcd", "bytes": 4096.0, "changes": 39.0}},
	}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_la_search", map[string]any{"capture_id": "e1", "kind": "bits", "pattern": "011"})
	sm := structured(t, res)
	if sm["n_hits"] != 2.0 || sm["truncated"] != true || sm["pattern"] != "011" {
		t.Errorf("search reply = %#v", sm)
	}
	if hits, _ := sm["hits"].([]any); len(hits) != 2 {
		t.Fatalf("search hits = %#v", sm["hits"])
	} else if h := hits[1].(map[string]any); h["run_i"] != 8.0 || h["bit_off"] != 9.0 || h["context"] != "101100101" {
		t.Errorf("hit 1 = %#v", h)
	}

	res = e.call("xuansi_la_activity", map[string]any{"capture_id": "l1"})
	am := structured(t, res)
	if am["bin_ns"] != 5e6 || am["n_edges"] != 4.0 || am["busiest_from_ns"] != 5e6 {
		t.Errorf("activity reply = %#v", am)
	}
	if bins, _ := am["bins"].([]any); len(bins) != 2 || bins[0].(map[string]any)["bytes"] != 1.0 {
		t.Errorf("activity bins = %#v", am["bins"])
	}

	res = e.call("xuansi_la_bits", map[string]any{"capture_id": "e1"})
	bm := structured(t, res)
	if bm["rate_derived"] != true || bm["n_bits"] != 35.0 || bm["violations"] != 2.0 ||
		bm["first_violation_t_ns"] != 6.0032e9 || bm["truncated"] != true {
		t.Errorf("bits reply = %#v", bm)
	}
	if segs, _ := bm["segments"].([]any); len(segs) != 1 ||
		segs[0].(map[string]any)["bits"] != "01100101011001010110010101100101011" {
		t.Errorf("bits segments = %#v", bm["segments"])
	}

	res = e.call("xuansi_la_adjacent", map[string]any{"capture_id": "s1", "ch": 0})
	pm := structured(t, res)
	if pm["flips"] != 15.0 {
		t.Errorf("adjacent reply = %#v", pm)
	}
	if peers, _ := pm["peers"].([]any); len(peers) != 2 || peers[0].(map[string]any)["ratio"] != 1.0 {
		t.Errorf("adjacent peers = %#v", pm["peers"])
	}

	res = e.call("xuansi_la_glitch", map[string]any{"capture_id": "e1"})
	gm := structured(t, res)
	if gm["modal_run_ns"] != 100000.0 || gm["threshold_ns"] != 50000.0 || gm["count"] != 1.0 {
		t.Errorf("glitch reply = %#v", gm)
	}
	if rows, _ := gm["rows"].([]any); len(rows) != 1 || rows[0].(map[string]any)["dur_ns"] != 30000.0 {
		t.Errorf("glitch rows = %#v", gm["rows"])
	}

	res = e.call("xuansi_la_eye", map[string]any{"capture_id": "e1"})
	ym := structured(t, res)
	if ym["period_ns"] != 1e6 || ym["n_fold"] != 38.0 || ym["high_pp_ns"] != 0.0 {
		t.Errorf("eye reply = %#v", ym)
	}
	if _, has := ym["min_high_ns"]; !has {
		t.Error("eye reply lacks min_high_ns (a two-sided fold carries it)")
	}

	res = e.call("xuansi_la_align", map[string]any{"capture_a": "e1", "capture_b": "e2"})
	lm := structured(t, res)
	if lm["lag_ns"] != -15433.0 || lm["matched"] != 39.0 || lm["drift_ppm"] != 996.4 {
		t.Errorf("align reply = %#v", lm)
	}
	if segs, _ := lm["segments"].([]any); len(segs) != 2 || segs[1].(map[string]any)["offset_ns"] != 8.25 {
		t.Errorf("align segments = %#v", lm["segments"])
	}

	res = e.call("xuansi_la_export", map[string]any{"capture_id": "e1", "fmt": "vcd"})
	xm := structured(t, res)
	if xm["path"] != "/captures/la/e1/export.vcd" || xm["changes"] != 39.0 || xm["bytes"] != 4096.0 {
		t.Errorf("export reply = %#v", xm)
	}
	if _, has := xm["truncated"]; has {
		t.Error("an uncut export carried truncated")
	}
}

// TestLAWave2ErrorCrossing: the daemon's coach refusals surface as
// tool errors verbatim (the zero-contact domain gates and the
// library's mode coaches).
func TestLAWave2ErrorCrossing(t *testing.T) {
	coach := `la_export: fmt "csv" — this wave lands "vcd" only (the level domain as a Value Change Dump every viewer opens)`
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{Class: "api", Msg: coach}}}, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_la_export", map[string]any{"capture_id": "e1", "fmt": "csv"})
	if !res.IsError {
		t.Fatalf("the fmt refusal did not surface: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "vcd") {
		t.Errorf("error text = %q, want the vcd coach", got)
	}
}

// TestLAWave2DescAudit (the frozen Desc discipline, wave A): every
// served Desc carries the anti-confusion line, its next-step tool
// names, and the wave's honest bounds — the row caps, the modal-unit
// statements, the nearest-phase lag, the export path rule.
func TestLAWave2DescAudit(t *testing.T) {
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
		"xuansi_la_search": {
			"NOT xuansi_capture", "bytes", "bits", "timing", "byte_off", "run_i",
			"256", "4096", "n_hits", "xuansi_la_runs", "xuansi_la_bits", "no lease",
		},
		"xuansi_la_activity": {
			"NOT xuansi_capture", "ratio", "popcount", "bytes", "busiest_from_ns",
			"1024", "REFUSED", "xuansi_la_hist", "no lease",
		},
		"xuansi_la_bits": {
			"NOT xuansi_capture", "majority vote", "rate_derived", "violations",
			"first_violation_t_ns", "256", "4096", "SELF-CONVERGING", "xuansi_la_glitch", "no lease",
		},
		"xuansi_la_adjacent": {
			"NOT xuansi_capture", "±window_ns", "modal gap", "ratio", "both directions",
			"xuansi_la_bits", "no lease",
		},
		"xuansi_la_glitch": {
			"NOT xuansi_capture", "modal_run_ns", "ratio", "0.5", "run 0", "256",
			"xuansi_la_edges", "no lease",
		},
		"xuansi_la_eye": {
			"NOT xuansi_capture", "period_ns", "n_fold", "high_pp_ns", "jitter",
			"xuansi_la_hist", "xuansi_la_glitch", "no lease",
		},
		"xuansi_la_align": {
			"NOT xuansi_capture", "NEAREST-PHASE", "drift_ppm", "4096", "SAME-BOARD",
			"SAME-BOARD ONLY", "segments", "xuansi_la_edges", "no lease",
		},
		"xuansi_la_export": {
			"NOT xuansi_capture", "PATH IS NEVER A PARAMETER", "export.vcd", "idempotent",
			"2^22", "truncated", "LISTEN captures refuse", "no lease",
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
}
