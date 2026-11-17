package server

// la_waveC_test.go — the wave C live-stream crossing through the real
// Server: the green stream pull (a fake board serving the LIVE read
// face — the published prefix growing, the done push at caught-up),
// the two zero-contact stream refusals (samples: the waveS throughput
// verdict; edge+trig: the circular pre-trigger ring), and the wave D
// kind=module arm crossing (the whole-window wire shape).

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"xuansi/internal/replclient"
)

func TestLaCaptureStreamGreenPath(t *testing.T) {
	const mac = "38f2c1"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la")
	ring, err := os.ReadFile("../la/testdata/modbus/data.bin")
	if err != nil {
		t.Fatal(err)
	}

	// the stream face: la.read serves the LIVE window (reveal-all on
	// the first read; the caught-up read pushes done and freezes the
	// capture — post-stop reads then serve [off, total) plainly).
	var mu sync.Mutex
	live := true
	total := int64(len(ring))
	streamRead := func(args map[string]any) (map[string]any, *replclient.OpError) {
		off := asInt(args["off"])
		ln := asInt(args["len"])
		mu.Lock()
		isLive := live
		mu.Unlock()
		if isLive {
			mu.Lock()
			live = false
			mu.Unlock()
			e.fakes[mac].push("la", map[string]any{
				"event": "done", "cap": int64(1), "bytes": int64(21), "reason": "limit"})
		}
		end := off + ln
		if end > total {
			end = total
		}
		var data []byte
		if off < total {
			data = ring[off:end]
		}
		return map[string]any{"data": data, "total": total,
			"eof": end >= total}, nil
	}
	f := e.fakes[mac]
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(11), "role": "controller",
			"fw": "xuansi-fw v0.7.0", "fw_build": "wc1", "fs": true}, nil
	})
	f.on("la.listen_arm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"cap": int64(1), "baud": int64(9600)}, nil
	})
	f.on("la.read", streamRead)
	f.on("la.stop", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"bytes": int64(21), "reason": "limit"}, nil
	})
	f.on("la.info", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"caps": []any{map[string]any{
			"cap": int64(1), "mode": "listen", "port": int64(1), "pin": int64(38),
			"state": "stopped", "bytes": int64(21), "total": total,
			"meta": map[string]any{"ring_kb": int64(64), "idle_syms": int64(4),
				"chunks_dropped": int64(0), "bytes_dropped": int64(0),
				"hw_ovf": int64(0), "armed_ns": int64(1), "stopped_ns": int64(2)},
		}}}, nil
	})
	f.on("la.disarm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{}, nil
	})

	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ := c.call(2, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"mode": "listen", "la_port": 1, "la_pin": 38, "baud": "auto", "ring_kb": 64,
		"max_seconds": 5, "stream": true})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("la_capture stream: %v", rerr)
	}
	res := asMap(rep["result"])
	meta := asMap(res["meta"])
	if meta["streamed"] != true {
		t.Errorf("meta.streamed = %#v, want true (the live-pull mark)", meta["streamed"])
	}
	if _, incomplete := meta["incomplete"]; incomplete {
		t.Errorf("a verified stream pull must not carry incomplete: %#v", meta)
	}
	if asInt(meta["bytes"]) != 21 || asInt(meta["chunks"]) != 4 {
		t.Errorf("meta account = bytes %v chunks %v, want the verified 21/4", meta["bytes"], meta["chunks"])
	}
	if got, rerr := os.ReadFile(asStr(res["path"])); rerr != nil || len(got) != len(ring) {
		t.Errorf("data.bin = %d bytes (err %v), want the whole ring's %d", len(got), rerr, len(ring))
	}
}

func TestLaCaptureStreamRefusals(t *testing.T) {
	const mac = "97c1ac"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la")
	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}

	// samples + stream: the waveS throughput verdict, zero-contact.
	rep, _ := c.call(2, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"mode": "samples", "rate_hz": 1_000_000, "ring_kb": 64, "stream": true})
	rerr := asMap(rep["error"])
	if rerr == nil || asStr(rerr["class"]) != "api" ||
		!strings.Contains(asStr(rerr["msg"]), "stream:true is LISTEN/EDGE only") ||
		!strings.Contains(asStr(rerr["msg"]), "0.215-0.322MB/s") {
		t.Errorf("samples stream refusal = %#v, want the waveS coach", rerr)
	}

	// edge + trig + stream: the circular pre-trigger ring, zero-contact.
	rep, _ = c.call(3, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"mode": "edge", "pins": []any{2}, "ring_kb": 64, "stream": true,
		"trig": map[string]any{"pin": 2, "kind": "edge_r"}})
	rerr = asMap(rep["error"])
	if rerr == nil || asStr(rerr["class"]) != "api" ||
		!strings.Contains(asStr(rerr["msg"]), "pre-trigger edge ring is CIRCULAR") {
		t.Errorf("edge+trig stream refusal = %#v, want the circular-ring coach", rerr)
	}
}

// TestLaCaptureEdgeModuleTrigWire pins the wave D arm crossing: the
// kind=module trig crosses to la.edge_arm as the whole-window form
// (kind, module — NO pin), and the pull lands with the module mirror.
func TestLaCaptureEdgeModuleTrigWire(t *testing.T) {
	const mac = "97c1ac"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la")
	f := e.fakes[mac]
	var armTrig map[string]any
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(11), "role": "controller",
			"fw": "xuansi-fw v0.7.0", "fw_build": "wd1", "fs": true}, nil
	})
	f.on("la.edge_arm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		armTrig, _ = args["trig"].(map[string]any)
		f.push("la", map[string]any{"event": "done", "cap": int64(1),
			"bytes": int64(27), "reason": "trig"})
		return map[string]any{"cap": int64(1), "res_hz": int64(80_000_000)}, nil
	})
	f.on("la.stop", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"bytes": int64(27), "reason": "trig"}, nil
	})
	f.on("la.info", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"caps": []any{map[string]any{
			"cap": int64(1), "mode": "edge", "state": "stopped",
			"bytes": int64(27), "total": int64(27), "pins": []any{int64(2)},
			"meta": map[string]any{
				"res_hz": int64(80_000_000), "trig_fired": true,
				"trig": map[string]any{"pin": int64(255), "kind": "module",
					"module": "trigpat", "pre_kb": int64(0), "fire_ns": int64(123)},
				"runs": int64(3), "runs_dropped": int64(0), "hw_ovf": int64(0),
				"ring_kb": int64(64), "max_ms": int64(4000), "reason": "trig",
				"armed_ns": int64(1), "stopped_ns": int64(2),
				"t0_ns": int64(5_000_000_000)},
		}}}, nil
	})
	ring := make([]byte, 27) // 3 zero runs — parses whole, identity holds
	f.on("la.read", func(args map[string]any) (map[string]any, *replclient.OpError) {
		off := asInt(args["off"])
		end := off + 32640
		if end > 27 {
			end = 27
		}
		var data []byte
		if off < 27 {
			data = ring[off:end]
		}
		return map[string]any{"data": data, "total": int64(27), "eof": end >= 27}, nil
	})
	f.on("la.disarm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{}, nil
	})

	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ := c.call(2, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"mode": "edge", "pins": []any{2}, "ring_kb": 64,
		"trig": map[string]any{"kind": "module", "module": "trigpat"}})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("la_capture edge module trig: %v", rerr)
	}
	if armTrig == nil || armTrig["kind"] != "module" || armTrig["module"] != "trigpat" {
		t.Errorf("arm trig wire = %#v, want the whole-window module form", armTrig)
	}
	if _, hasPin := armTrig["pin"]; hasPin {
		t.Errorf("the module form carries NO pin: %#v", armTrig)
	}
	meta := asMap(asMap(rep["result"])["meta"])
	trig := asMap(meta["trig"])
	if asStr(trig["kind"]) != "module" || asStr(trig["module"]) != "trigpat" ||
		asInt(trig["fire_ns"]) != 123 || asInt(trig["pin"]) != 255 {
		t.Errorf("module trig mirror = %#v (want pin255/module/fire_ns)", trig)
	}
	if asInt(meta["fire_ns"]) != 123 {
		t.Errorf("top-level fire_ns = %v, want the guardrail-② column", meta["fire_ns"])
	}
}
