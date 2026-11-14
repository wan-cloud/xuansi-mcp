package server

// la_test.go — the LA wave's daemon face (T7) through the real Server:
// the green pull (the v9 fake board serving the listen quintet over
// T6's frozen modbus stream) landing a verified object and the
// analysis requests reading it back, the v9 era gate, the zero-contact
// domain refusals, and the residual-capture sweep at release (carried
// item 3, the streamDisarmAll precedent).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"xuansi/internal/replclient"
)

// scriptLABoard wires one fake board for the v9 listen face: the hello
// answers protocol_ver 9 and the la quintet serves the given ring
// bytes with stopBytes payload (the frozen stream fixture's identity:
// parsed Σn must equal la.stop's bytes for the pull to verify).
func scriptLABoard(f *fakeCtl, ring []byte, stopBytes int64) {
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(9), "role": "controller",
			"fw": "xuansi-fw v0.5.0", "fw_build": "v9bee", "fs": true}, nil
	})
	f.on("la.listen_arm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		// the capture self-ends the moment it arms: the done push rides
		// the subscriber PullCapture registered before arm (the pull's
		// own ordering discipline)
		f.push("la", map[string]any{"event": "done", "cap": int64(1), "bytes": stopBytes, "reason": "full"})
		return map[string]any{"cap": int64(1), "baud": int64(9600)}, nil
	})
	f.on("la.stop", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"bytes": stopBytes, "reason": "full"}, nil // idempotent echo
	})
	f.on("la.info", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"caps": []any{map[string]any{
			"cap": int64(1), "mode": "listen", "port": int64(1), "pin": int64(38),
			"state": "stopped", "bytes": stopBytes, "total": int64(len(ring)),
			"meta": map[string]any{"ring_kb": int64(64), "idle_syms": int64(4),
				"chunks_dropped": int64(0), "bytes_dropped": int64(0), "hw_ovf": int64(0),
				"armed_ns": int64(1), "stopped_ns": int64(2)},
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

// TestLaCaptureGreenPathAndAnalysis: lease → la_capture blocks through
// the quintet and answers {capture_id, path, meta} of a VERIFIED
// object (no incomplete), the object lands on disk, and every
// analysis request reads it back daemon-locally.
func TestLaCaptureGreenPathAndAnalysis(t *testing.T) {
	const mac = "38f2c1"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la") // the store's test seam (singleton, retargeted before first use)
	ring, err := os.ReadFile("../la/testdata/modbus/data.bin")
	if err != nil {
		t.Fatal(err)
	}
	scriptLABoard(e.fakes[mac], ring, 21)

	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ := c.call(2, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"mode": "listen", "la_port": 1, "la_pin": 38, "baud": "auto", "ring_kb": 64, "max_seconds": 5})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("la_capture: %v", rerr)
	}
	res := asMap(rep["result"])
	capID := asStr(res["capture_id"])
	if !strings.Contains(capID, mac+"-la-listen-") {
		t.Errorf("capture_id = %q, want the <ts>-%s-la-listen-<n> shape", capID, mac)
	}
	path := asStr(res["path"])
	if !strings.HasSuffix(path, "data.bin") {
		t.Errorf("path = %q, want the object's data.bin", path)
	}
	meta := asMap(res["meta"])
	if asStr(meta["board"]) != mac || asStr(meta["mode"]) != "listen" {
		t.Errorf("meta identity = %v/%v, want %s/listen", meta["board"], meta["mode"], mac)
	}
	if asInt(meta["baud"]) != 9600 || asInt(meta["bytes"]) != 21 || asInt(meta["chunks"]) != 4 {
		t.Errorf("meta account = baud %v bytes %v chunks %v, want the verified 9600/21/4", meta["baud"], meta["bytes"], meta["chunks"])
	}
	if asStr(meta["reason"]) != "full" {
		t.Errorf("reason = %v, want full (the done push's reason)", meta["reason"])
	}
	if _, incomplete := meta["incomplete"]; incomplete {
		t.Errorf("a verified pull must not carry incomplete: %#v", meta)
	}
	if asStr(meta["sha256"]) == "" {
		t.Error("a verified pull must carry its sha256 identity")
	}
	// the object on disk is the ring verbatim
	if got, rerr := os.ReadFile(path); rerr != nil || len(got) != len(ring) {
		t.Errorf("data.bin = %d bytes (err %v), want the ring's %d", len(got), rerr, len(ring))
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(path), "meta.json")); err != nil {
		t.Errorf("meta.json did not land: %v", err)
	}
	// the arm crossed with the tool's facts: auto baud, max_ms 5000
	for _, call := range e.fakes[mac].got() {
		if call.op == "la.listen_arm" {
			if asInt(call.args["max_ms"]) != 5000 || call.args["baud"] != "auto" || asInt(call.args["ring_kb"]) != 64 {
				t.Errorf("la.listen_arm args = %#v, want auto/64/5000", call.args)
			}
		}
	}

	// la_list: the object is listed
	rep, _ = c.call(3, map[string]any{"type": "la_list"})
	list, _ := asMap(rep["result"])["captures"].([]any)
	if len(list) != 1 {
		t.Fatalf("la_list = %d rows, want 1", len(list))
	}
	if asStr(asMap(list[0])["cap_id"]) != capID {
		t.Errorf("la_list row = %v, want the fresh id", list[0])
	}
	// la_info: the same meta, complete
	rep, _ = c.call(4, map[string]any{"type": "la_info", "capture_id": capID})
	info := asMap(rep["result"])
	if asStr(info["cap_id"]) != capID || asInt(info["baud"]) != 9600 {
		t.Errorf("la_info = %#v, want the landed meta", info)
	}
	if _, incomplete := info["incomplete"]; incomplete {
		t.Errorf("la_info reports incomplete on a verified object: %#v", info)
	}
	// la_frames: the framing view over the real object
	rep, _ = c.call(5, map[string]any{"type": "la_frames", "capture_id": capID})
	frames := asMap(rep["result"])
	if asInt(frames["baud"]) != 9600 || asInt(frames["idle_ns"]) <= 0 {
		t.Errorf("la_frames echoes = %#v, want baud 9600 + a derived threshold", frames)
	}
	rows, _ := frames["frames"].([]any)
	if len(rows) < 1 {
		t.Fatalf("la_frames = %d frames over the modbus fixture, want ≥1", len(rows))
	}
	if _, truncated := frames["truncated"]; truncated {
		t.Errorf("a 4-chunk fixture must not truncate: %#v", frames)
	}
	// la_repeat: the periodicity account answers its bounded columns
	rep, _ = c.call(6, map[string]any{"type": "la_repeat", "capture_id": capID})
	repeat := asMap(rep["result"])
	if _, has := repeat["repeats"]; !has {
		t.Errorf("la_repeat lacks repeats: %#v", repeat)
	}
	if asInt(repeat["tail_bytes"]) < 0 {
		t.Errorf("la_repeat tail_bytes = %v, want ≥0", repeat["tail_bytes"])
	}
	// la_decode_uart: the parity column rides the claim
	rep, _ = c.call(7, map[string]any{"type": "la_decode_uart", "capture_id": capID, "parity": "E"})
	decode := asMap(rep["result"])
	drows, _ := decode["frames"].([]any)
	if len(drows) != len(rows) {
		t.Fatalf("decode rows = %d, want frames' %d", len(drows), len(rows))
	}
	for i, r := range drows {
		if _, has := asMap(r)["parity_ok"]; !has {
			t.Errorf("decode row %d lacks parity_ok under the E claim: %#v", i, r)
		}
	}
}

// TestLaCaptureV8GateRefuses: a sighted pre-v9 board refuses honestly
// (the ops do not exist in its era — the feature gate, not unknown-op).
func TestLaCaptureV8GateRefuses(t *testing.T) {
	const mac = "38f2c1"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	f := e.fakes[mac]
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(8), "role": "controller", "fs": true}, nil
	})
	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	// establish the sighting the gate reads (one op crossing → hello)
	if rep, _ := c.call(2, map[string]any{"type": "op", "board": mac, "holder": "h1", "op": "wifi.status"}); rep["error"] != nil {
		t.Fatalf("op: %#v", rep)
	}
	rep, _ := c.call(3, map[string]any{"type": "la_capture", "board": mac, "holder": "h1",
		"la_port": 1, "la_pin": 38, "baud": 9600})
	rerr := errOf(t, rep)
	if asStr(rerr["class"]) != "server" || !strings.Contains(asStr(rerr["msg"]), "protocol_ver=8") {
		t.Errorf("v8 gate error = %#v, want the honest v9-overlay refusal", rerr)
	}
}

// TestLaCaptureDomainRefusalsZeroContact: the arm-domain refusals are
// zero-contact api errors (no dial, no board op) — the cheap gate
// ahead of the fw's own coach messages.
func TestLaCaptureDomainRefusalsZeroContact(t *testing.T) {
	const mac = "38f2c1"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	dials := e.dialedTCP(mac)
	bad := []map[string]any{
		{"la_port": 0, "la_pin": 38, "baud": 9600},
		{"la_port": 3, "la_pin": 38, "baud": 9600},
		{"la_port": 1, "la_pin": 0, "baud": 9600},
		{"la_port": 1, "la_pin": 38, "baud": 600},
		{"la_port": 1, "la_pin": 38, "baud": 5_000_001},
		{"la_port": 1, "la_pin": 38, "baud": 9600.5},
		{"la_port": 1, "la_pin": 38, "baud": "Autos"},
		{"la_port": 1, "la_pin": 38},
		{"la_port": 1, "la_pin": 38, "baud": 9600, "ring_kb": 8192},
		{"la_port": 1, "la_pin": 38, "baud": 9600, "mode": "edge"},
	}
	for i, req := range bad {
		r := map[string]any{"type": "la_capture", "board": mac, "holder": "h1"}
		for k, v := range req {
			r[k] = v
		}
		rep, _ := c.call(int64(10+i), r)
		rerr := errOf(t, rep)
		if asStr(rerr["class"]) != "api" {
			t.Errorf("case %d (%v): error = %#v, want api class", i, req, rerr)
		}
	}
	if got := e.dialedTCP(mac); got != dials {
		t.Errorf("domain refusals dialed %d→%d — the gate must be zero-contact", dials, got)
	}
}

// TestLaCaptureNeedsLease: no lease, no capture — the holder gate.
func TestLaCaptureNeedsLease(t *testing.T) {
	const mac = "38f2c1"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "la_capture", "board": mac,
		"la_port": 1, "la_pin": 38, "baud": 9600})
	rerr := errOf(t, rep)
	if asStr(rerr["class"]) != "not_held" {
		t.Errorf("unleased la_capture error = %#v, want not_held", rerr)
	}
}

// TestLaAnalysisMissCoaches: the object reads' local miss is an api
// refusal carrying the la_list coach (daemon-local, zero board
// contact).
func TestLaAnalysisMissCoaches(t *testing.T) {
	e := newEnv(t, nil) // zero boards: the store is empty and local
	e.srv.laStore.Root = filepath.Join(t.TempDir(), "la")
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "la_info", "capture_id": "nope-1"})
	rerr := errOf(t, rep)
	if asStr(rerr["class"]) != "api" || !strings.Contains(asStr(rerr["msg"]), "la_list") {
		t.Errorf("miss error = %#v, want the api refusal with the la_list coach", rerr)
	}
	rep, _ = c.call(2, map[string]any{"type": "la_list"})
	list, _ := asMap(rep["result"])["captures"].([]any)
	if len(list) != 0 {
		t.Errorf("empty store listed %d rows", len(list))
	}
}

// TestLaDisarmAllSweepsOnRelease (carried item 3): release sweeps the
// board's residual la captures — la.info enumerates, each cap gets
// la.stop + la.disarm, the "la" source subscription ends, one log
// line. The streamDisarmAll precedent's shape.
func TestLaDisarmAllSweepsOnRelease(t *testing.T) {
	const mac = "38f2c1"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	f := e.fakes[mac]
	scriptLABoard(f, nil, 0)
	// residual state: one armed cap the pull never cleaned
	f.on("la.info", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"caps": []any{map[string]any{
			"cap": int64(7), "mode": "listen", "port": int64(1), "pin": int64(38),
			"state": "armed", "bytes": int64(0), "total": int64(0),
		}}}, nil
	})
	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	if rep, _ := c.call(2, map[string]any{"type": "release", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("release: %#v", rep)
	}
	// the sweep rides the release reply (synchronous, best-effort)
	var stopped, disarmed, unsub bool
	for _, call := range f.got() {
		switch call.op {
		case "la.stop":
			stopped = asInt(call.args["cap"]) == 7
		case "la.disarm":
			disarmed = asInt(call.args["cap"]) == 7
		case "events.unsubscribe":
			unsub = asStr(call.args["source"]) == "la"
		}
	}
	if !stopped || !disarmed || !unsub {
		t.Errorf("sweep ops: stop(%v) disarm(%v) unsubscribe-la(%v) — want all three over the residual cap 7", stopped, disarmed, unsub)
	}
}

// TestLaDisarmAllQuietOnCleanBoard: a board whose la.info answers no
// caps sweeps nothing (no stop/disarm noise on every release).
func TestLaDisarmAllQuietOnCleanBoard(t *testing.T) {
	const mac = "38f2c1"
	e := newEnv(t, nil, mkBoard(mac, "bench2.local"))
	f := e.fakes[mac]
	scriptLABoard(f, nil, 0)
	f.on("la.info", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"caps": []any{}}, nil
	})
	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("lease: %#v", rep)
	}
	if rep, _ := c.call(2, map[string]any{"type": "release", "board": mac, "holder": "h1"}); rep["error"] != nil {
		t.Fatalf("release: %#v", rep)
	}
	for _, call := range f.got() {
		if call.op == "la.stop" || call.op == "la.disarm" {
			t.Errorf("clean board swept %s — the no-caps case must be silent", call.op)
		}
	}
}

// TestLaCaptureRequestCeiling: the la_capture request's own ceiling
// derives from the arm bound (seconds → max_ms + slack), not the 60s
// default — the unit pin; the integration behavior (a pull that runs
// past 60s surviving) is the HIL seat's account.
func TestLaCaptureRequestCeiling(t *testing.T) {
	for _, tc := range []struct {
		seconds int64
		want    time.Duration
	}{
		{0, 600*time.Second + laPullSlack},
		{5, 5*time.Second + laPullSlack},
		{600, 600*time.Second + laPullSlack},
		{900, 600*time.Second + laPullSlack}, // clamped, capture's own rule
	} {
		if got := laCaptureCeil(tc.seconds); got != tc.want {
			t.Errorf("laCaptureCeil(%d) = %s, want %s", tc.seconds, got, tc.want)
		}
	}
}
