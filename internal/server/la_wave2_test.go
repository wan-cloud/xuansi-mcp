package server

// la_wave2_test.go — the LA wave-2 daemon face (LA 增強波 A,
// 2026-11-17) through the real Server: landed edge/samples objects
// read back by every new analysis request, the export landing inside
// the capture's own directory, and the honest refusals (mode guards,
// the same-board align rule, the export fmt domain, the bin cap).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// landObject writes one capture object straight into the store (the
// pull machinery already proven — this wave's crossings are
// daemon-local reads over landed bytes).
func landObject(t *testing.T, root, id, fixture string) string {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../la/testdata/" + fixture + "/data.bin")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := os.ReadFile("../la/testdata/" + fixture + "/meta.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.bin"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), meta, 0o600); err != nil {
		t.Fatal(err)
	}
	return id
}

// wave2Env lands the four fixtures under one store root.
func wave2Env(t *testing.T) (*env, string, string, string, string) {
	e := newEnv(t, nil)
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la")
	return e,
		landObject(t, e.srv.laStore.Root, "edgepwm", "edgepwm"),
		landObject(t, e.srv.laStore.Root, "edgenrz", "edgenrz"),
		landObject(t, e.srv.laStore.Root, "sampwm", "sampwm"),
		landObject(t, e.srv.laStore.Root, "modbus", "modbus")
}

func TestLaWave2GreenPathCrossings(t *testing.T) {
	e, pwm, nrz, samp, mod := wave2Env(t)
	c := dialSock(t, e.sock)

	// la_search timing over the NRZ fixture: 8 width-200µs runs
	minNS, maxNS := 200_000, 250_000
	rep, _ := c.call(1, map[string]any{"type": "la_search", "capture_id": nrz,
		"kind": "timing", "min_ns": minNS, "max_ns": maxNS})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("la_search: %v", rerr)
	}
	sm := asMap(rep["result"])
	if asInt(sm["n_hits"]) != 8 {
		t.Errorf("la_search = %#v, want 8 hits", sm)
	}

	// la_search bytes over the listen fixture
	rep, _ = c.call(2, map[string]any{"type": "la_search", "capture_id": mod,
		"kind": "bytes", "pattern": "0103"})
	sm = asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil || asInt(sm["n_hits"]) != 2 {
		t.Errorf("la_search bytes = %#v err %v, want 2 hits", sm, rerr)
	}

	// la_activity over the PWM: 1ms bins → 20 bins, 39 edges
	rep, _ = c.call(3, map[string]any{"type": "la_activity", "capture_id": pwm, "bin_ms": 1})
	sm = asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil || asInt(sm["n_edges"]) != 39 ||
		len(sm["bins"].([]any)) != 20 || asStr(sm["mode"]) != "edge" {
		t.Errorf("la_activity = %#v err %v, want 20 bins / 39 edges", sm, rerr)
	}

	// la_bits over the NRZ: derived 100µs grid, 35 bits, 2 violations
	rep, _ = c.call(4, map[string]any{"type": "la_bits", "capture_id": nrz})
	sm = asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil || asInt(sm["n_bits"]) != 35 ||
		asInt(sm["violations"]) != 2 || asInt(sm["rate_ns"]) != 100_000 {
		t.Errorf("la_bits = %#v err %v, want 35 bits / 2 violations / 100µs", sm, rerr)
	}

	// la_adjacent over the samples bus: D1 accompanies every D0 flip
	rep, _ = c.call(5, map[string]any{"type": "la_adjacent", "capture_id": samp, "ch": 0})
	sm = asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("la_adjacent: %v", rerr)
	}
	peers := sm["peers"].([]any)
	if top := asMap(peers[0].(map[string]any)); asInt(top["ch"]) != 1 || asInt(top["count"]) != 15 {
		t.Errorf("la_adjacent top peer = %#v, want ch1 count 15", top)
	}

	// la_glitch / la_eye over their fixtures
	rep, _ = c.call(6, map[string]any{"type": "la_glitch", "capture_id": nrz})
	sm = asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil || asInt(sm["count"]) != 1 ||
		asInt(sm["modal_run_ns"]) != 100_000 {
		t.Errorf("la_glitch = %#v err %v, want 1 outlier at 100µs mode", sm, rerr)
	}
	rep, _ = c.call(7, map[string]any{"type": "la_eye", "capture_id": pwm})
	sm = asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil || asInt(sm["period_ns"]) != 1_000_000 {
		t.Errorf("la_eye = %#v err %v, want the 1ms fold", sm, rerr)
	}

	// la_align self: lag 0, matched 39
	rep, _ = c.call(8, map[string]any{"type": "la_align", "capture_id": pwm, "capture_id_b": pwm})
	sm = asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil || asInt(sm["lag_ns"]) != 0 || asInt(sm["matched"]) != 39 {
		t.Errorf("la_align self = %#v err %v, want lag 0 / 39 matched", sm, rerr)
	}

	// la_export: the file lands beside data.bin, idempotent
	rep, _ = c.call(9, map[string]any{"type": "la_export", "capture_id": pwm, "fmt": "vcd"})
	sm = asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("la_export: %v", rerr)
	}
	path := asStr(sm["path"])
	if filepath.Dir(path) != filepath.Join(e.srv.laStore.Root, pwm) || filepath.Base(path) != "export.vcd" {
		t.Errorf("export path = %q, want <store>/%s/export.vcd", path, pwm)
	}
	if asInt(sm["changes"]) != 39 {
		t.Errorf("export changes = %v, want 39", sm["changes"])
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the export did not land: %v", err)
	}
	rep, _ = c.call(10, map[string]any{"type": "la_export", "capture_id": pwm, "fmt": "vcd"})
	sm2 := asMap(rep["result"])
	if asInt(sm2["changes"]) != asInt(sm["changes"]) {
		t.Errorf("re-export changes = %v, want idempotent %v", sm2["changes"], sm["changes"])
	}
}

func TestLaWave2Refusals(t *testing.T) {
	e, pwm, _, _, mod := wave2Env(t)
	c := dialSock(t, e.sock)

	// the level seats refuse the listen object with the domain coach
	for _, tc := range []struct {
		req map[string]any
		op  string
	}{
		{map[string]any{"type": "la_bits", "capture_id": mod}, "la_bits"},
		{map[string]any{"type": "la_search", "capture_id": mod, "kind": "timing", "min_ns": 1}, "la_search"},
		{map[string]any{"type": "la_glitch", "capture_id": mod}, "la_glitch"},
		{map[string]any{"type": "la_eye", "capture_id": mod}, "la_eye"},
		{map[string]any{"type": "la_export", "capture_id": mod, "fmt": "vcd"}, "la_export"},
	} {
		rep, _ := c.call(20, tc.req)
		if rerr := asMap(rep["error"]); rerr == nil || !strings.Contains(asStr(rerr["msg"]), "LEVEL") {
			t.Errorf("%s on listen: err = %v, want the LEVEL-domain coach", tc.op, rerr)
		}
	}

	// a missing capture is the api miss with the la_list coach
	rep, _ := c.call(21, map[string]any{"type": "la_bits", "capture_id": "gone-9"})
	if rerr := asMap(rep["error"]); rerr == nil || !strings.Contains(asStr(rerr["msg"]), "la_list") {
		t.Errorf("missing capture: %v, want the la_list coach", rerr)
	}

	// cross-board align refuses (the Diff rule)
	landObject(t, e.srv.laStore.Root, "other-board", "edgepwm")
	other := filepath.Join(e.srv.laStore.Root, "other-board", "meta.json")
	if b, err := os.ReadFile(other); err == nil {
		os.WriteFile(other, []byte(strings.Replace(string(b), `"97c1ac"`, `"d75884"`, 1)), 0o600)
	}
	rep, _ = c.call(22, map[string]any{"type": "la_align", "capture_id": pwm, "capture_id_b": "other-board"})
	if rerr := asMap(rep["error"]); rerr == nil || !strings.Contains(asStr(rerr["msg"]), "different boards") {
		t.Errorf("cross-board align: %v, want the same-board coach", rerr)
	}

	// the export fmt domain is a zero-contact api refusal
	rep, _ = c.call(23, map[string]any{"type": "la_export", "capture_id": pwm, "fmt": "csv"})
	if rerr := asMap(rep["error"]); rerr == nil || asStr(rerr["class"]) != "api" ||
		!strings.Contains(asStr(rerr["msg"]), "vcd") {
		t.Errorf("fmt csv: %v, want the api-class vcd coach", rerr)
	}
	// and an unknown fmt string (not just a known-wrong one)
	rep, _ = c.call(24, map[string]any{"type": "la_export", "capture_id": pwm, "fmt": ""})
	if rerr := asMap(rep["error"]); rerr == nil || asStr(rerr["class"]) != "api" {
		t.Errorf("fmt omitted: %v, want the api-class refusal", rerr)
	}

	// the activity bin cap refuses with the minimum-bin coach
	rep, _ = c.call(25, map[string]any{"type": "la_activity", "capture_id": pwm, "bin_ms": 1, "max_bins": 8})
	if rerr := asMap(rep["error"]); rerr == nil || !strings.Contains(asStr(rerr["msg"]), "bin_ms 1 over") {
		t.Errorf("bin cap: %v, want the min-bin coach", rerr)
	}

	// a capture id that is not a bare name refuses at the store
	// (the path-domain guard the export rides too)
	rep, _ = c.call(26, map[string]any{"type": "la_export", "capture_id": "../" + pwm, "fmt": "vcd"})
	if rerr := asMap(rep["error"]); rerr == nil || asStr(rerr["class"]) != "api" {
		t.Errorf("traversal id: %v, want the api refusal", rerr)
	}
}

// TestLaWave2SamplesExportChanges: the samples export crosses with
// the popcount account (30 changes over the sampwm fixture) and the
// header names the DVP channels.
func TestLaWave2SamplesExportChanges(t *testing.T) {
	e, _, _, samp, _ := wave2Env(t)
	c := dialSock(t, e.sock)
	rep, _ := c.call(30, map[string]any{"type": "la_export", "capture_id": samp, "fmt": "vcd"})
	sm := asMap(rep["result"])
	if rerr := asMap(rep["error"]); rerr != nil || asInt(sm["changes"]) != 30 {
		t.Errorf("samples export = %#v err %v, want 30 changes", sm, rerr)
	}
	raw, err := os.ReadFile(asStr(sm["path"]))
	if err != nil || !strings.Contains(string(raw), "$var wire 1 ! D0 $end") {
		t.Errorf("samples VCD header missing D0: err %v", err)
	}
}
