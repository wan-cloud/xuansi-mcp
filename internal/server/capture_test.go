package server

// capture_test.go — the MB-level capture face (task 12, design §8 大檔
// 落地): capture_start/capture_stop over the real unix socket against
// the scripted fake board, the landed files checked byte-exact against
// independently computed expectations, and every ending (limit /
// stopped / error) pinned to exactly one capture_done push.

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// newCaptureEnv builds a leased test env with the captures root pinned
// to a temp dir (the injectable-root seam, under the server lock so
// the request goroutines read it race-free).
func newCaptureEnv(t *testing.T) (*env, *sockClient) {
	t.Helper()
	e := newEnv(t, nil, mkBoard("d75884", "192.168.7.1"))
	e.srv.mu.Lock()
	e.srv.captureRoot = t.TempDir()
	e.srv.mu.Unlock()
	c := dialSock(t, e.sock)
	reply, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "h1"})
	if e := asMap(reply["error"]); e != nil {
		t.Fatalf("lease: %s: %s", e["class"], e["msg"])
	}
	return e, c
}

// collect reads messages until the reply carrying rid (when nonzero)
// and wantDone capture_done pushes arrived.
func (c *sockClient) collect(t *testing.T, rid int64, wantDone int) (reply map[string]any, dones []map[string]any) {
	t.Helper()
	for {
		m := c.recv()
		switch m["type"] {
		case "capture_done":
			dones = append(dones, m)
		case "reply":
			if rid != 0 && asInt(m["id"]) == rid {
				reply = m
			}
		default: // an event push mid-collect is none of our business
		}
		if (rid == 0 || reply != nil) && len(dones) >= wantDone {
			return reply, dones
		}
	}
}

// collectAll reads until every reply id in rids AND wantDone
// capture_done pushes have arrived, returning the done pushes (the
// replies' interleaving with the pushes is free).
func (c *sockClient) collectAll(t *testing.T, rids map[int64]bool, wantDone int) (dones []map[string]any) {
	t.Helper()
	for {
		m := c.recv()
		switch m["type"] {
		case "capture_done":
			dones = append(dones, m)
		case "reply":
			delete(rids, asInt(m["id"]))
		}
		if len(rids) == 0 && len(dones) >= wantDone {
			return dones
		}
	}
}

// captureStart runs one capture_start and returns its result.
func captureStart(t *testing.T, c *sockClient, id int64, req map[string]any) map[string]any {
	t.Helper()
	req["type"] = "capture_start"
	reply, _ := c.call(id, req)
	if e := asMap(reply["error"]); e != nil {
		t.Fatalf("capture_start: %s: %s", e["class"], e["msg"])
	}
	return asMap(reply["result"])
}

// captureStop runs one capture_stop and returns (result, errorMap).
func captureStop(t *testing.T, c *sockClient, id int64, capID string) (map[string]any, map[string]any) {
	t.Helper()
	reply, _ := c.call(id, map[string]any{"type": "capture_stop", "capture_id": capID})
	if e := asMap(reply["error"]); e != nil {
		return nil, e
	}
	return asMap(reply["result"]), nil
}

// TestCaptureUartRawByteExact: uart bin chunks land concatenated
// byte-exact in one .bin file; an early stop ends it reason "stopped"
// with exactly one capture_done push.
func TestCaptureUartRawByteExact(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	res := captureStart(t, c, 2, map[string]any{
		"board": "d75884", "source": "uart1.rx", "max_seconds": 60,
	})
	capID, path := asStr(res["capture_id"]), asStr(res["path"])
	if capID == "" || path == "" {
		t.Fatalf("capture_start result = %#v", res)
	}
	if !regexp.MustCompile(`^\d{8}T\d{6}-d75884-uart1\.rx-cap\d+\.bin$`).MatchString(filepath.Base(path)) {
		t.Errorf("capture filename = %q, want <UTCYYYYmmddTHHMMSS>-d75884-uart1.rx-cap<N>.bin", filepath.Base(path))
	}
	// the source was subscribed board-side so events actually flow
	subbed := false
	for _, mc := range f.got() {
		if mc.op == "events.subscribe" && mc.args["source"] == "uart1.rx" {
			subbed = true
		}
	}
	if !subbed {
		t.Errorf("capture_start never subscribed uart1.rx board-side (ops: %v)", f.got())
	}

	chunks := [][]byte{{0x00, 0x01, 0xFF}, []byte("xuansi-\xE6\x87\x89"), {0x80, 0x7F}}
	for _, ch := range chunks {
		f.push("uart1.rx", map[string]any{"data": ch})
	}

	c.send(map[string]any{"type": "capture_stop", "id": 3, "capture_id": capID})
	reply, dones := c.collect(t, 3, 1)
	if e := asMap(reply["error"]); e != nil {
		t.Fatalf("capture_stop: %s: %s", e["class"], e["msg"])
	}
	stop := asMap(reply["result"])
	if asStr(stop["reason"]) != "stopped" || stop["path"] != path {
		t.Errorf("capture_stop result = %#v, want reason stopped at %s", stop, path)
	}
	if got := asInt(stop["bytes"]); got != int64(len(chunks[0])+len(chunks[1])+len(chunks[2])) {
		t.Errorf("capture_stop bytes = %d, want %d", got, len(chunks[0])+len(chunks[1])+len(chunks[2]))
	}
	if len(dones) != 1 {
		t.Fatalf("got %d capture_done pushes, want exactly 1 (%v)", len(dones), dones)
	}
	d := dones[0]
	if d["capture_id"] != capID || d["reason"] != "stopped" || d["path"] != path || asInt(d["bytes"]) != asInt(stop["bytes"]) {
		t.Errorf("capture_done = %#v", d)
	}

	// the file is byte-exact against an independently built expectation
	want := append(append(append([]byte{}, chunks[0]...), chunks[1]...), chunks[2]...)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read capture file: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("capture file = %q, want %q", got, want)
	}
	c.recvNone() // no second push, no wedge
}

// TestCaptureStdoutRaw: stdout str chunks concatenate as raw bytes.
func TestCaptureStdoutRaw(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	res := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "stdout"})
	capID, path := asStr(res["capture_id"]), asStr(res["path"])
	if !strings.HasSuffix(path, "-d75884-stdout-cap1.bin") {
		t.Errorf("stdout capture filename = %q, want -d75884-stdout-cap1.bin", path)
	}
	f.push("stdout", map[string]any{"data": "hello "})
	f.push("stdout", map[string]any{"data": "world\n"})

	c.send(map[string]any{"type": "capture_stop", "id": 3, "capture_id": capID})
	reply, dones := c.collect(t, 3, 1)
	if e := asMap(reply["error"]); e != nil {
		t.Fatalf("capture_stop: %v", e)
	}
	if len(dones) != 1 || dones[0]["reason"] != "stopped" {
		t.Fatalf("capture_done pushes = %v, want exactly one stopped", dones)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read capture file: %v", err)
	}
	if string(got) != "hello world\n" {
		t.Errorf("stdout capture file = %q, want %q", got, "hello world\n")
	}
}

// TestCaptureGpioJSONL: gpio.N lands as one JSON line per event with
// ts/source/data keys and the .jsonl extension.
func TestCaptureGpioJSONL(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	res := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "gpio.4"})
	capID, path := asStr(res["capture_id"]), asStr(res["path"])
	if !strings.HasSuffix(path, "-d75884-gpio.4-cap1.jsonl") {
		t.Errorf("gpio capture filename = %q, want -d75884-gpio.4-cap1.jsonl", path)
	}
	f.push("gpio.4", map[string]any{"edge": "rising", "level": int64(1)})
	f.push("gpio.4", map[string]any{"edge": "falling", "level": int64(0)})

	c.send(map[string]any{"type": "capture_stop", "id": 3, "capture_id": capID})
	reply, dones := c.collect(t, 3, 1)
	if e := asMap(reply["error"]); e != nil {
		t.Fatalf("capture_stop: %v", e)
	}
	if len(dones) != 1 || dones[0]["reason"] != "stopped" {
		t.Fatalf("capture_done pushes = %v, want exactly one stopped", dones)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read capture file: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("capture file has %d lines, want 2: %q", len(lines), raw)
	}
	for i, want := range []int64{1, 0} {
		var line struct {
			TS     string         `json:"ts"`
			Source string         `json:"source"`
			Data   map[string]any `json:"data"`
		}
		if err := json.Unmarshal([]byte(lines[i]), &line); err != nil {
			t.Fatalf("line %d is not JSON: %v (%q)", i, err, lines[i])
		}
		if line.TS == "" || line.Source != "gpio.4" {
			t.Errorf("line %d = ts %q source %q", i, line.TS, line.Source)
		}
		if asInt(line.Data["level"]) != want || line.Data["edge"] == "" {
			t.Errorf("line %d data = %#v, want level %d with an edge", i, line.Data, want)
		}
	}
}

// TestCaptureNetJSONL: net state events land as JSON lines too.
func TestCaptureNetJSONL(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	res := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "net", "dest": "netlog.jsonl"})
	capID, path := asStr(res["capture_id"]), asStr(res["path"])
	if filepath.Base(path) != "netlog.jsonl" { // dest override stays inside the root
		t.Errorf("dest override path = %q, want netlog.jsonl", path)
	}
	f.push("net", map[string]any{"state": "up", "ssid": "bench", "ip": "192.168.7.10"})

	c.send(map[string]any{"type": "capture_stop", "id": 3, "capture_id": capID})
	reply, dones := c.collect(t, 3, 1)
	if e := asMap(reply["error"]); e != nil {
		t.Fatalf("capture_stop: %v", e)
	}
	if len(dones) != 1 || dones[0]["reason"] != "stopped" {
		t.Fatalf("capture_done = %v, want exactly one stopped", dones)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read capture file: %v", err)
	}
	var line struct {
		TS     string         `json:"ts"`
		Source string         `json:"source"`
		Data   map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &line); err != nil {
		t.Fatalf("net capture file is not one JSON line: %v (%q)", err, raw)
	}
	if line.Source != "net" || asStr(line.Data["state"]) != "up" || asStr(line.Data["ssid"]) != "bench" {
		t.Errorf("net line = %#v", line)
	}
	if asInt(dones[0]["bytes"]) != int64(len(raw)) {
		t.Errorf("capture_done bytes = %v, want the file size %d", dones[0]["bytes"], len(raw))
	}
}

// TestCaptureMaxBytesLimit: a RAW chunk crossing the byte cap is
// truncated so the file lands on exactly max_bytes; the done push says
// limit with the exact count, and the session is gone afterwards.
func TestCaptureMaxBytesLimit(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	res := captureStart(t, c, 2, map[string]any{
		"board": "d75884", "source": "uart2.rx", "max_bytes": 5, "max_seconds": 60,
	})
	capID, path := asStr(res["capture_id"]), asStr(res["path"])

	f.push("uart2.rx", map[string]any{"data": []byte{1, 2, 3}})
	f.push("uart2.rx", map[string]any{"data": []byte{4, 5, 6, 7}})

	_, dones := c.collect(t, 0, 1)
	if len(dones) != 1 {
		t.Fatalf("got %d capture_done pushes, want 1", len(dones))
	}
	d := dones[0]
	if d["capture_id"] != capID || d["reason"] != "limit" || asInt(d["bytes"]) != 5 {
		t.Errorf("capture_done = %#v, want cap %s limit 5 bytes", d, capID)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read capture file: %v", err)
	}
	if string(got) != "\x01\x02\x03\x04\x05" {
		t.Errorf("truncated capture file = %q, want the first 5 bytes", got)
	}
	// the finished session cannot be stopped again
	if _, e := captureStop(t, c, 3, capID); e == nil {
		t.Errorf("stopping a finished capture id did not fail")
	}
	c.recvNone()
}

// TestCaptureMaxSecondsLimit: the time bound ends the capture reason
// "limit" with the bytes written so far.
func TestCaptureMaxSecondsLimit(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	start := time.Now()
	res := captureStart(t, c, 2, map[string]any{
		"board": "d75884", "source": "stdout", "max_seconds": 1, "max_bytes": 1048576,
	})
	capID := asStr(res["capture_id"])
	f.push("stdout", map[string]any{"data": "chunk"})

	_, dones := c.collect(t, 0, 1)
	if len(dones) != 1 || dones[0]["reason"] != "limit" || dones[0]["capture_id"] != capID {
		t.Fatalf("capture_done = %v, want one limit push for %s", dones, capID)
	}
	if asInt(dones[0]["bytes"]) != 5 {
		t.Errorf("capture_done bytes = %v, want 5", dones[0]["bytes"])
	}
	if e := time.Since(start); e < 900*time.Millisecond {
		t.Errorf("limit fired after %v, want the 1s bound honored", e)
	}
	c.recvNone()
}

// TestCaptureDiskErrorUnwedged: a failing writer ends the capture
// reason "error" exactly once and the server keeps serving.
func TestCaptureDiskErrorUnwedged(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	e.srv.mu.Lock()
	e.srv.captureOpen = func(string) (io.WriteCloser, error) {
		return failWriter{}, nil
	}
	e.srv.mu.Unlock()

	res := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "uart1.rx"})
	capID := asStr(res["capture_id"])
	f.push("uart1.rx", map[string]any{"data": []byte("boom")})

	_, dones := c.collect(t, 0, 1)
	if len(dones) != 1 || dones[0]["reason"] != "error" || dones[0]["capture_id"] != capID {
		t.Fatalf("capture_done = %v, want exactly one error push for %s", dones, capID)
	}
	// the server is unwedged: the next request is served
	reply, _ := c.call(3, map[string]any{"type": "list_boards"})
	if e := asMap(reply["error"]); e != nil {
		t.Fatalf("list_boards after disk error: %s: %s", e["class"], e["msg"])
	}
	if _, e := captureStop(t, c, 4, capID); e == nil {
		t.Errorf("stopping the errored capture did not fail")
	}
	c.recvNone()
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, os.ErrClosed }
func (failWriter) Close() error              { return nil }

// TestCaptureJobRefused: job is a direct-delivery source, not a
// subscribable one — the refusal says so. Absent/unknown sources are
// local errors too.
func TestCaptureJobRefused(t *testing.T) {
	_, c := newCaptureEnv(t)

	reply, _ := c.call(2, map[string]any{"type": "capture_start", "board": "d75884", "source": "job"})
	e := errOf(t, reply)
	if e == nil || asStr(e["class"]) != "api" {
		t.Fatalf("job capture error = %#v, want class api", e)
	}
	if !strings.Contains(asStr(e["msg"]), "job") || !strings.Contains(asStr(e["msg"]), "subscribable") {
		t.Errorf("job refusal msg = %q, want it to name job's direct delivery", asStr(e["msg"]))
	}

	for _, src := range []string{"", "uart3.rx", "gpio"} {
		reply, _ = c.call(3, map[string]any{"type": "capture_start", "board": "d75884", "source": src})
		e = errOf(t, reply)
		if e == nil || asStr(e["class"]) != "server" {
			t.Errorf("source %q error = %#v, want class server", src, e)
		}
	}
	// none of the refusals leaked a session: the ids never existed
	if _, e := captureStop(t, c, 4, "cap-1"); e == nil {
		t.Errorf("stopping a never-started capture id did not fail")
	}
}

// TestCaptureStopUnknownAndDoubleStop: unknown ids and second stops
// are server-class errors.
func TestCaptureStopUnknownAndDoubleStop(t *testing.T) {
	_, c := newCaptureEnv(t)

	c.send(map[string]any{"type": "capture_stop", "id": 2, "capture_id": "cap-nope"})
	reply, dones := c.collect(t, 2, 0)
	if e := errOf(t, reply); e == nil || asStr(e["class"]) != "server" {
		t.Fatalf("unknown-id stop error = %#v, want class server", e)
	}
	if len(dones) != 0 {
		t.Errorf("unknown-id stop produced %d done pushes", len(dones))
	}

	res := captureStart(t, c, 3, map[string]any{"board": "d75884", "source": "stdout"})
	capID := asStr(res["capture_id"])
	c.send(map[string]any{"type": "capture_stop", "id": 4, "capture_id": capID})
	reply, dones = c.collect(t, 4, 1)
	if e := asMap(reply["error"]); e != nil {
		t.Fatalf("first stop: %v", e)
	}
	if len(dones) != 1 || dones[0]["reason"] != "stopped" {
		t.Fatalf("done pushes = %v, want exactly one stopped", dones)
	}

	c.send(map[string]any{"type": "capture_stop", "id": 5, "capture_id": capID})
	reply, dones = c.collect(t, 5, 0)
	if e := errOf(t, reply); e == nil || asStr(e["class"]) != "server" {
		t.Fatalf("double stop error = %#v, want class server", e)
	}
	if len(dones) != 0 {
		t.Errorf("double stop produced %d done pushes", len(dones))
	}
	c.recvNone()
}

// TestCaptureRequiresLease: like eval, capture_start needs the lease.
func TestCaptureRequiresLease(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "192.168.7.1"))
	c := dialSock(t, e.sock)
	reply, _ := c.call(2, map[string]any{"type": "capture_start", "board": "d75884", "source": "stdout"})
	if errOf(t, reply) == nil || asStr(asMap(reply["error"])["class"]) != "not_held" {
		t.Fatalf("unleased capture_start reply = %#v, want class not_held", reply)
	}
}

// TestCaptureDestSanitized: dest must be a bare filename — path
// separators and traversal are refused daemon-side.
func TestCaptureDestSanitized(t *testing.T) {
	_, c := newCaptureEnv(t)
	for _, dest := range []string{"sub/x.bin", "/etc/passwd", "..", "a/../b.bin"} {
		reply, _ := c.call(2, map[string]any{"type": "capture_start", "board": "d75884", "source": "stdout", "dest": dest})
		e := errOf(t, reply)
		if e == nil || asStr(e["class"]) != "server" {
			t.Errorf("dest %q error = %#v, want class server", dest, e)
		}
	}
}

// TestCaptureTwoSessionsDistinct: two live captures on one connection
// get distinct ids, distinct files, and one done push each.
func TestCaptureTwoSessionsDistinct(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	r1 := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "uart1.rx"})
	r2 := captureStart(t, c, 3, map[string]any{"board": "d75884", "source": "stdout"})
	id1, id2 := asStr(r1["capture_id"]), asStr(r2["capture_id"])
	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("capture ids = %q / %q, want two distinct", id1, id2)
	}
	if r1["path"] == r2["path"] {
		t.Fatalf("both captures share the path %v", r1["path"])
	}

	f.push("uart1.rx", map[string]any{"data": []byte("AB")})
	f.push("stdout", map[string]any{"data": "CD"})

	c.send(map[string]any{"type": "capture_stop", "id": 4, "capture_id": id1})
	c.send(map[string]any{"type": "capture_stop", "id": 5, "capture_id": id2})
	dones := c.collectAll(t, map[int64]bool{4: true, 5: true}, 2)
	seen := map[string]int{}
	for _, d := range dones {
		seen[d["capture_id"].(string)]++
		if d["reason"] != "stopped" {
			t.Errorf("capture_done = %#v, want reason stopped", d)
		}
	}
	if seen[id1] != 1 || seen[id2] != 1 {
		t.Fatalf("done pushes per id = %v, want exactly one each", seen)
	}
	if got, err := os.ReadFile(asStr(r1["path"])); err != nil || string(got) != "AB" {
		t.Errorf("uart capture file = %q (%v), want AB", got, err)
	}
	if got, err := os.ReadFile(asStr(r2["path"])); err != nil || string(got) != "CD" {
		t.Errorf("stdout capture file = %q (%v), want CD", got, err)
	}
	c.recvNone()
}

// captureRootOf reads a test env's injected captures root.
func captureRootOf(t *testing.T, e *env) string {
	t.Helper()
	e.srv.mu.Lock()
	defer e.srv.mu.Unlock()
	return e.srv.captureRoot
}

// TestCaptureSameSourceConcurrent (review round 1): two LIVE captures
// of the same board+source started in the same UTC second get distinct
// paths (the default name embeds the capture sequence), both files
// land byte-exact — the tap feeds both sessions — and each session
// pushes its done exactly once. Pre-fix, both mapped to the same
// second-resolution path and os.Create truncated the first capture's
// file mid-flight.
func TestCaptureSameSourceConcurrent(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	r1 := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "uart1.rx"})
	r2 := captureStart(t, c, 3, map[string]any{"board": "d75884", "source": "uart1.rx"})
	p1, p2 := asStr(r1["path"]), asStr(r2["path"])
	if p1 == p2 {
		t.Fatalf("two same-second same-source captures share the path %q", p1)
	}
	for _, p := range []string{p1, p2} {
		if !regexp.MustCompile(`^\d{8}T\d{6}-d75884-uart1\.rx-cap\d+\.bin$`).MatchString(filepath.Base(p)) {
			t.Errorf("capture filename = %q, want <UTCYYYYmmddTHHMMSS>-d75884-uart1.rx-cap<N>.bin", filepath.Base(p))
		}
	}

	chunks := [][]byte{[]byte("AAA"), []byte("BBB")}
	for _, ch := range chunks {
		f.push("uart1.rx", map[string]any{"data": ch})
	}

	c.send(map[string]any{"type": "capture_stop", "id": 4, "capture_id": asStr(r1["capture_id"])})
	c.send(map[string]any{"type": "capture_stop", "id": 5, "capture_id": asStr(r2["capture_id"])})
	dones := c.collectAll(t, map[int64]bool{4: true, 5: true}, 2)
	seen := map[string]int{}
	for _, d := range dones {
		seen[d["capture_id"].(string)]++
	}
	if seen[asStr(r1["capture_id"])] != 1 || seen[asStr(r2["capture_id"])] != 1 {
		t.Fatalf("done pushes per id = %v, want exactly one each", seen)
	}

	// both files hold the FULL stream byte-exact (the tap feeds every
	// matching session) — no truncation, no interleaving on one fd
	want := "AAABBB"
	for i, p := range []string{p1, p2} {
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read capture file %d: %v", i, err)
		}
		if string(got) != want {
			t.Errorf("capture file %d = %q, want %q", i, got, want)
		}
	}
	c.recvNone()
}

// TestCaptureStopRestartSameSecond (review round 1): the CLI shape —
// stop a capture and immediately start the same board+source again
// within the same UTC second: the second capture gets a fresh path and
// the first capture's landed file stays byte-exact. Pre-fix, the
// restart truncated the first file.
func TestCaptureStopRestartSameSecond(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	r1 := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "stdout"})
	f.push("stdout", map[string]any{"data": "first-run"})
	c.send(map[string]any{"type": "capture_stop", "id": 3, "capture_id": asStr(r1["capture_id"])})
	if reply, dones := c.collect(t, 3, 1); len(dones) != 1 || dones[0]["reason"] != "stopped" {
		t.Fatalf("first stop: reply=%v dones=%v", reply, dones)
	}

	r2 := captureStart(t, c, 4, map[string]any{"board": "d75884", "source": "stdout"})
	p1, p2 := asStr(r1["path"]), asStr(r2["path"])
	if p1 == p2 {
		t.Fatalf("restart in the same second reused the path %q", p1)
	}
	f.push("stdout", map[string]any{"data": "second-run"})
	c.send(map[string]any{"type": "capture_stop", "id": 5, "capture_id": asStr(r2["capture_id"])})
	if _, dones := c.collect(t, 5, 1); len(dones) != 1 {
		t.Fatalf("second stop dones = %v", dones)
	}

	if got, _ := os.ReadFile(p1); string(got) != "first-run" {
		t.Errorf("first capture's file = %q after the restart — truncated?", got)
	}
	if got, _ := os.ReadFile(p2); string(got) != "second-run" {
		t.Errorf("second capture's file = %q, want second-run", got)
	}
}

// TestCaptureDestCollisionRefused (review round 1): an explicit dest
// NEVER truncates — a path owned by a live capture or an existing file
// is refused with a server-class error naming the collision, and the
// existing file's contents are untouched.
func TestCaptureDestCollisionRefused(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]

	// a landed file from an unrelated past capture
	root := captureRootOf(t, e)
	if err := os.WriteFile(filepath.Join(root, "landed.bin"), []byte("PRECIOUS"), 0o600); err != nil {
		t.Fatal(err)
	}

	// live-session collision
	r1 := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "stdout", "dest": "dup.bin"})
	reply, _ := c.call(3, map[string]any{"type": "capture_start", "board": "d75884", "source": "stdout", "dest": "dup.bin"})
	e1 := errOf(t, reply)
	if e1 == nil || asStr(e1["class"]) != "server" {
		t.Fatalf("live-dest collision error = %#v, want class server", e1)
	}
	if !strings.Contains(asStr(e1["msg"]), "dup.bin") {
		t.Errorf("collision msg = %q, want it to name dup.bin", asStr(e1["msg"]))
	}

	// on-disk collision (pre-existing file)
	reply, _ = c.call(4, map[string]any{"type": "capture_start", "board": "d75884", "source": "stdout", "dest": "landed.bin"})
	e2 := errOf(t, reply)
	if e2 == nil || asStr(e2["class"]) != "server" {
		t.Fatalf("on-disk dest collision error = %#v, want class server", e2)
	}

	// the live capture keeps landing; the pre-existing file is intact
	f.push("stdout", map[string]any{"data": "payload"})
	c.send(map[string]any{"type": "capture_stop", "id": 5, "capture_id": asStr(r1["capture_id"])})
	if _, dones := c.collect(t, 5, 1); len(dones) != 1 {
		t.Fatalf("live capture's done = %v", dones)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "dup.bin")); string(got) != "payload" {
		t.Errorf("dup.bin = %q, want the live capture's payload", got)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "landed.bin")); string(got) != "PRECIOUS" {
		t.Errorf("landed.bin = %q, want PRECIOUS untouched", got)
	}
}

// TestCaptureJSONLLineAtCap (review round 1's minor #4): the JSONL
// byte-cap boundary — a line that fits exactly (== max_bytes) lands
// and the capture ends limit at that count; a line that would cross
// is left UNWRITTEN (a truncated line is corrupt JSON), ending limit
// at zero bytes. The expected length is computed independently from
// the wire shape (fixed-width ts, sorted map keys).
func TestCaptureJSONLLineAtCap(t *testing.T) {
	e, c := newCaptureEnv(t)
	f := e.fakes["d75884"]
	data := map[string]any{"edge": "rising", "level": int64(1)}
	line, err := json.Marshal(captureLine{TS: "2026-11-05T06:03:14.516726121Z", Source: "gpio.4", Data: data})
	if err != nil {
		t.Fatal(err)
	}
	L := int64(len(line) + 1) // + '\n'

	// exact fit: the line lands, limit fires at exactly L bytes
	res := captureStart(t, c, 2, map[string]any{"board": "d75884", "source": "gpio.4", "max_bytes": L})
	f.push("gpio.4", data)
	if _, dones := c.collect(t, 0, 1); len(dones) != 1 || dones[0]["reason"] != "limit" || asInt(dones[0]["bytes"]) != L {
		t.Fatalf("exact-fit done = %v, want one limit push at %d bytes", dones, L)
	}
	if got, _ := os.ReadFile(asStr(res["path"])); int64(len(got)) != L || !strings.HasSuffix(string(got), "\n") {
		t.Errorf("exact-fit file = %q (%d B), want the whole %d-byte line", got, len(got), L)
	}

	// crossing line: nothing written, limit at zero
	res = captureStart(t, c, 3, map[string]any{"board": "d75884", "source": "gpio.4", "max_bytes": L - 1})
	f.push("gpio.4", data)
	if _, dones := c.collect(t, 0, 1); len(dones) != 1 || dones[0]["reason"] != "limit" || asInt(dones[0]["bytes"]) != 0 {
		t.Fatalf("crossing-line done = %v, want one limit push at 0 bytes", dones)
	}
	if got, _ := os.ReadFile(asStr(res["path"])); len(got) != 0 {
		t.Errorf("crossing-line file = %q, want empty (the line was left unwritten)", got)
	}
	c.recvNone()
}
