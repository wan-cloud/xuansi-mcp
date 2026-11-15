package server

// la_sample_test.go — the samples face (T13) through the real Server:
// the green pull (the v9 fake board serving the sample quintet over a
// CS-framed mode-0 SPI stream) landing a verified object, the domain
// refusals (rate not a notch, ring missing, listen columns, bits
// trigger without mask), and la_decode_bus reading the landed object
// back daemon-locally — plus the cross-mode refusals.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"xuansi/internal/replclient"
)

// scriptLASampleBoard wires the fake board's v9 sample quintet: the
// arm reply, the done push inside arm, the sample ledger, and the
// ring = a CS-framed mode-0 SPI byte (the decode's fixture shape).
func scriptLASampleBoard(f *fakeCtl, ring []byte, stopBytes int64) {
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(9), "role": "controller",
			"fw": "xuansi-fw v0.5.0", "fw_build": "v9bee", "fs": true}, nil
	})
	f.on("la.sample_arm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		f.push("la", map[string]any{"event": "done", "cap": int64(1), "bytes": stopBytes, "reason": "trig"})
		return map[string]any{"cap": int64(1), "rate_hz": args["rate_hz"]}, nil
	})
	f.on("la.edge_arm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		f.push("la", map[string]any{"event": "done", "cap": int64(2), "bytes": stopBytes, "reason": "full"})
		return map[string]any{"cap": int64(2), "res_hz": int64(80_000_000)}, nil
	})
	f.on("la.stop", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"bytes": stopBytes, "reason": "trig"}, nil
	})
	f.on("la.info", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"caps": []any{map[string]any{
			"cap": int64(1), "mode": "sample", "state": "stopped",
			"bytes": stopBytes, "total": int64(len(ring)),
			"meta": map[string]any{"rate_hz": int64(20_000_000), "samples": int64(len(ring)),
				"trig": map[string]any{"ch": int64(3), "kind": "edge_f", "pre_kb": int64(4)},
				"trig_fired": true, "hw_ovf": int64(0), "ring_kb": int64(256),
				"max_ms": int64(6000), "reason": "trig", "armed_ns": int64(1),
				"stopped_ns": int64(2), "t0_ns": int64(3_000_000_000)},
		}}}, nil
	})
	f.on("la.read", func(args map[string]any) (map[string]any, *replclient.OpError) {
		off, total := asInt(args["off"]), int64(len(ring))
		end := off + 32640
		if end > total {
			end = total
		}
		data := []byte{}
		if off < total {
			data = ring[off:end]
		}
		return map[string]any{"data": data, "total": total, "eof": end >= total}, nil
	})
	f.on("la.disarm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{}, nil
	})
}

// lasampleRing: idle, one CS-framed mode-0 byte 0xC3/0x3C, idle —
// 4 ticks per half period at 20MS/s = 50ns tick (SCK 2.5MHz).
func lasampleRing() []byte {
	var buf []byte
	cur := byte(0x08) // CS high, SCK/MOSI/MISO idle low
	for i := 0; i < 4; i++ {
		buf = append(buf, cur)
	}
	cur &^= 0x08 // CS falls
	for i := 0; i < 3; i++ {
		buf = append(buf, cur)
	}
	mosi, miso := byte(0xC3), byte(0x3C)
	for i := 7; i >= 0; i-- {
		mbit := mosi >> uint(i) & 1
		ibit := miso >> uint(i) & 1
		cur = cur&^0x06 | mbit<<1 | ibit<<2
		buf = append(buf, cur)   // SCK low half
		buf = append(buf, cur|1) // SCK rises (sample here)
	}
	cur |= 0x08 // CS rises
	for i := 0; i < 4; i++ {
		buf = append(buf, cur)
	}
	return buf
}

func TestLaCaptureSamplesGreenPathAndDecodeBus(t *testing.T) {
	const mac = "97c1ac"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la")
	ring := lasampleRing()
	scriptLASampleBoard(e.fakes[mac], ring, int64(len(ring)))

	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ := c.call(2, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"mode": "samples", "rate_hz": 20_000_000, "ring_kb": 256, "max_seconds": 6,
		"trig": map[string]any{"ch": 3, "kind": "edge_f", "pre_kb": 4}})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("la_capture samples: %v", rerr)
	}
	res := asMap(rep["result"])
	capID := asStr(res["capture_id"])
	if !strings.Contains(capID, mac+"-la-sample-") {
		t.Errorf("capture_id = %q, want the <ts>-%s-la-sample-<n> shape", capID, mac)
	}
	meta := asMap(res["meta"])
	if asStr(meta["mode"]) != "samples" || asInt(meta["rate_hz"]) != 20_000_000 {
		t.Errorf("meta identity = %v/%v, want samples/20MHz", meta["mode"], meta["rate_hz"])
	}
	if asInt(meta["samples"]) != int64(len(ring)) || asStr(meta["reason"]) != "trig" {
		t.Errorf("meta account = samples %v reason %v, want %d/trig", meta["samples"], meta["reason"], len(ring))
	}
	if asInt(meta["t_start_ns"]) != 3_000_000_000 {
		t.Errorf("t_start_ns = %v, want the t0 ledger", meta["t_start_ns"])
	}
	if _, incomplete := meta["incomplete"]; incomplete {
		t.Errorf("a verified pull must not carry incomplete: %#v", meta)
	}
	if got, rerr := os.ReadFile(asStr(res["path"])); rerr != nil || len(got) != len(ring) {
		t.Errorf("data.bin = %d bytes (err %v), want the ring's %d", len(got), rerr, len(ring))
	}
	// the pins column carries the frozen DVP bus map (bit i = D_i)
	if pins, ok := meta["pins"].([]any); !ok || len(pins) != 8 || asInt(pins[0]) != 11 {
		t.Errorf("meta pins = %#v, want the frozen 8-pin bus map", meta["pins"])
	}

	// the trig mirror crossed
	trig := asMap(meta["trig"])
	if asInt(trig["ch"]) != 3 || asStr(trig["kind"]) != "edge_f" {
		t.Errorf("trig mirror = %#v", trig)
	}

	// la_decode_bus reads the landed object daemon-locally
	rep, _ = c.call(3, map[string]any{"type": "la_decode_bus", "capture_id": capID, "proto": "spi"})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("la_decode_bus: %v", rerr)
	}
	d := asMap(rep["result"])
	if asStr(d["proto"]) != "spi" || asInt(d["n_txn"]) != 1 {
		t.Fatalf("decode = %#v, want one spi txn", d)
	}
	txns := d["txns"].([]any)
	tx := asMap(txns[0])
	if asStr(tx["mosi"]) != "c3" || asStr(tx["miso"]) != "3c" || asInt(tx["n_bits"]) != 8 {
		t.Errorf("txn = %#v, want the c3/3c byte", tx)
	}
	if asInt(d["cs_mask"]) != 8 {
		t.Errorf("decode echo = cs_mask %v, want 8 (mode 0 omits itself — the default)", d["cs_mask"])
	}
	// the sck half period: 1 tick per half × 50ns = 50ns
	if asInt(d["sck_half_ns"]) != 50 {
		t.Errorf("sck_half_ns = %v, want 50", d["sck_half_ns"])
	}
}

func TestLaCaptureSamplesDomainRefusals(t *testing.T) {
	const mac = "97c1ac"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la")
	scriptLASampleBoard(e.fakes[mac], lasampleRing(), int64(len(lasampleRing())))
	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	bad := []struct {
		name string
		args map[string]any
		want string
	}{
		{"rate off-notch", map[string]any{"mode": "samples", "rate_hz": 15_000_000, "ring_kb": 256}, "six notches"},
		{"rate missing", map[string]any{"mode": "samples", "ring_kb": 256}, "rate_hz is required"},
		{"ring missing", map[string]any{"mode": "samples", "rate_hz": 1_000_000}, "ring_kb"},
		{"listen columns", map[string]any{"mode": "samples", "rate_hz": 1_000_000, "ring_kb": 256, "la_port": 1}, "LISTEN/EDGE"},
		{"bits without mask", map[string]any{"mode": "samples", "rate_hz": 1_000_000, "ring_kb": 256,
			"trig": map[string]any{"kind": "bits"}}, "mask"},
		{"edge trig ch out of range", map[string]any{"mode": "samples", "rate_hz": 1_000_000, "ring_kb": 256,
			"trig": map[string]any{"kind": "edge_r", "ch": 8}}, "0..7"},
		{"level without level", map[string]any{"mode": "samples", "rate_hz": 1_000_000, "ring_kb": 256,
			"trig": map[string]any{"kind": "level", "ch": 0}}, "level is REQUIRED"},
	}
	for _, tc := range bad {
		args := map[string]any{"type": "la_capture", "board": mac, "holder": "h1"}
		for k, v := range tc.args {
			args[k] = v
		}
		rep, _ := c.call(2, args)
		er := asMap(rep["error"])
		if er == nil {
			t.Errorf("%s: no error (zero-contact domain refusal expected)", tc.name)
			continue
		}
		if !strings.Contains(asStr(er["msg"]), tc.want) {
			t.Errorf("%s: msg = %q, want it to contain %q", tc.name, asStr(er["msg"]), tc.want)
		}
		if asStr(er["class"]) != "api" {
			t.Errorf("%s: class = %q, want api (zero-contact)", tc.name, asStr(er["class"]))
		}
	}
}

func TestLaDecodeBusCrossModeAndParamRefusals(t *testing.T) {
	const mac = "97c1ac"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la")
	ring := lasampleRing()
	scriptLASampleBoard(e.fakes[mac], ring, int64(len(ring)))
	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ := c.call(2, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"mode": "samples", "rate_hz": 20_000_000, "ring_kb": 256})
	if asMap(rep["error"]) != nil {
		t.Fatalf("samples capture: %v", asMap(rep["error"]))
	}
	capID := asStr(asMap(rep["result"])["capture_id"])

	// an edge capture refuses the bus decode (the samples-mode coach)
	rep, _ = c.call(3, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"mode": "edge", "pins": []int{11}, "ring_kb": 64})
	if asMap(rep["error"]) != nil {
		t.Fatalf("edge capture: %v", asMap(rep["error"]))
	}
	edgeID := asStr(asMap(rep["result"])["capture_id"])
	rep, _ = c.call(4, map[string]any{"type": "la_decode_bus", "capture_id": edgeID, "proto": "spi"})
	if er := asMap(rep["error"]); er == nil || !strings.Contains(asStr(er["msg"]), "samples") {
		t.Errorf("edge capture decode: %v, want the samples-mode coach", er)
	}

	// the spi param domain over the wire
	rep, _ = c.call(5, map[string]any{"type": "la_decode_bus", "capture_id": capID,
		"proto": "spi", "bus_mode": 4})
	if er := asMap(rep["error"]); er == nil || !strings.Contains(asStr(er["msg"]), "mode") {
		t.Errorf("mode 4: %v, want the mode refusal", er)
	}
	rep, _ = c.call(6, map[string]any{"type": "la_decode_bus", "capture_id": capID,
		"proto": "i2c", "bus_mode": 2})
	if er := asMap(rep["error"]); er == nil || !strings.Contains(asStr(er["msg"]), "i2c form") {
		t.Errorf("i2c with spi params: %v, want the form refusal", er)
	}
	rep, _ = c.call(7, map[string]any{"type": "la_decode_bus", "capture_id": capID, "proto": "can"})
	if er := asMap(rep["error"]); er == nil || !strings.Contains(asStr(er["msg"]), "i2c") {
		t.Errorf("proto can: %v, want the proto refusal", er)
	}
}
