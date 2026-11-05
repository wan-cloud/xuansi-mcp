package mcp

// workspace_test.go — task 11's driver-workspace tests: the put/push/
// read-back pipeline against the fake daemon (wire shapes asserted on
// every crossing: the holder-less op caps, the held fs_write carrying
// the sanitized /drv/<name>.lua path, the holder-less fs_read), the
// --@api gate (missing / mismatched with BOTH values in the refusal),
// the board's single-file cap enforced at PUSH time (the workspace
// itself keeps fat files), the read-back sha256 verify that leaves the
// manifest untouched on mismatch, footprint idempotency, concurrent
// pushes merged into a consistent manifest, name/version sanitization,
// and the version-defaults-to-Active semantic. The workspace root is
// injected via Options.WorkspaceRoot — never by mutating the
// environment.

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// ---------------------------------------------------------------------------
// helpers

// drvBody builds a driver file whose first line is first, padded to at
// least minLen bytes.
func drvBody(first string, minLen int) string {
	s := first + "\n-- driver body\nreturn {}\n"
	for len(s) < minLen {
		s += "-- padding padding padding padding padding\n"
	}
	return s
}

// capsStep is one scripted op-caps reply at api_version ver.
func capsStep(ver int) step {
	return step{result: map[string]any{"api_version": float64(ver), "primitives": map[string]any{}}}
}

// servePushDaemon scripts the push pipeline's daemon half: op caps →
// api_version ver; fs_write records the payload per (board, path);
// fs_read echoes the recorded bytes back byte-exact (tamper flips the
// last bit first, simulating a corrupt/truncated board file). Anything
// else on the wire fails the test.
func servePushDaemon(t *testing.T, fd *fakeDaemon, ver int, tamper bool) chan server.Request {
	t.Helper()
	reqs := make(chan server.Request, 16)
	files := map[string][]byte{} // board\x00path → last written payload
	fd.serve(t, func(conn net.Conn) {
		for {
			var req server.Request
			if err := server.ReadMsg(conn, &req); err != nil {
				return // client close: expected shutdown path
			}
			select {
			case reqs <- req:
			default:
				t.Errorf("push daemon: unexpected extra request %+v", req)
			}
			var rep *server.Reply
			switch req.Type {
			case "op":
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{
					"api_version": float64(ver), "primitives": map[string]any{}}}
			case "fs_write":
				files[req.Board+"\x00"+req.Path] = req.Data
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{"ok": true}}
			case "fs_read":
				data := append([]byte(nil), files[req.Board+"\x00"+req.Path]...)
				if tamper && len(data) > 0 {
					data[len(data)-1] ^= 1
				}
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{
					"data": base64.StdEncoding.EncodeToString(data)}}
			default:
				t.Errorf("push daemon: unexpected request type %s", req.Type)
				rep = &server.Reply{Type: "reply", ID: req.ID,
					Error: &server.RPCError{Class: "server", Msg: "unexpected " + req.Type}}
			}
			if err := server.WriteMsg(conn, rep); err != nil {
				t.Errorf("push daemon reply: %v", err)
				return
			}
		}
	})
	return reqs
}

// newWsEnv wires an env against a scripted daemon with an injected
// temp workspace root (script may be nil: nothing crosses, and any
// request fails the test — the zero-contact proof).
func newWsEnv(t *testing.T, script []step) (*env, chan server.Request, string) {
	t.Helper()
	fd := newFakeDaemon(t)
	reqs := fd.serveScript(t, script)
	root := t.TempDir()
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01", WorkspaceRoot: root})
	return e, reqs, root
}

// newWsPushEnv wires an env against the push-pipeline daemon.
func newWsPushEnv(t *testing.T, ver int, tamper bool) (*env, chan server.Request, string) {
	t.Helper()
	fd := newFakeDaemon(t)
	reqs := servePushDaemon(t, fd, ver, tamper)
	root := t.TempDir()
	e := wireMCP(t, fd, Options{Holder: "mcp-fixed01", WorkspaceRoot: root})
	return e, reqs, root
}

// mustPut stores one driver version or fails the test; returns its sha256.
func mustPut(t *testing.T, e *env, name, ver, content string) string {
	t.Helper()
	res := e.call("xuansi_ws_put", map[string]any{"name": name, "version": ver, "content": content})
	if res.IsError {
		t.Fatalf("ws_put %s@%s failed: %s", name, ver, resultText(t, res))
	}
	m := structured(t, res)
	sha, _ := m["sha256"].(string)
	if size, _ := m["size"].(float64); int(size) != len(content) {
		t.Errorf("ws_put size = %v, want %d", m["size"], len(content))
	}
	return sha
}

// wsManifest loads the workspace's manifest straight off disk.
func wsManifest(t *testing.T, root string) *Manifest {
	t.Helper()
	m, err := loadManifest(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	return m
}

// ---------------------------------------------------------------------------
// the --@api directive parser + the manifest file

// TestParseAPIDirective: the fw gate's demanded first line — an integer
// after "--@api ", CRLF tolerated, everything else malformed.
func TestParseAPIDirective(t *testing.T) {
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{"--@api 1\nlocal M = {}\nreturn M\n", 1, true},
		{"--@api 12\nbody", 12, true},
		{"--@api 1", 1, true},             // first line without newline
		{"--@api 1\r\nbody\r\n", 1, true}, // CRLF tolerated
		{"", 0, false},                    // empty file
		{"\n--@api 1\n", 0, false},        // directive not on line 1
		{"local M = {}\n--@api 1\n", 0, false},
		{"-- @api 1\n", 0, false},  // spacing matters
		{"--@api x\n", 0, false},   // not an integer
		{"--@api 1.5\n", 0, false}, // no semver, no floats
		{"--@api 1 \n", 0, false},  // trailing space is malformed
		{"--@api\n", 0, false},     // missing N
		{"--@api -1\n", 0, false},  // digits only
	}
	for _, c := range cases {
		got, err := parseAPIDirective(c.in)
		if c.ok && (err != nil || got != c.want) {
			t.Errorf("parseAPIDirective(%q) = %d, %v; want %d, nil", c.in, got, err, c.want)
		}
		if !c.ok && err == nil {
			t.Errorf("parseAPIDirective(%q) = %d; want an error", c.in, got)
		}
	}
}

// TestManifestLoadWriteRoundTrip: a missing manifest is an empty one
// (fresh workspace); an atomic write loads back equal; a corrupt file
// refuses instead of being silently overwritten.
func TestManifestLoadWriteRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")

	m0, err := loadManifest(path)
	if err != nil || m0.Drivers == nil || len(m0.Drivers) != 0 {
		t.Fatalf("loadManifest(missing) = %+v, %v; want an empty manifest, nil", m0, err)
	}

	want := &Manifest{Drivers: map[string]DriverEntry{
		"ads1115": {
			Active: "1.3.0",
			Files: map[string]FileMeta{
				"1.3.0": {Sha256: "abc", Size: 42, PushedAt: time.Now().UTC(),
					Boards: map[string]time.Time{"d75884": time.Now().UTC()}},
			},
		},
	}}
	if err := writeManifestAtomic(path, want); err != nil {
		t.Fatalf("writeManifestAtomic: %v", err)
	}
	got, err := loadManifest(path)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if got.Drivers["ads1115"].Active != "1.3.0" {
		t.Errorf("round-trip Active = %q, want 1.3.0", got.Drivers["ads1115"].Active)
	}
	fm := got.Drivers["ads1115"].Files["1.3.0"]
	if fm.Sha256 != "abc" || fm.Size != 42 || len(fm.Boards) != 1 || fm.PushedAt.IsZero() {
		t.Errorf("round-trip FileMeta = %+v, want the written fields back", fm)
	}

	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadManifest(path); err == nil {
		t.Errorf("loadManifest(corrupt) succeeded, want a refusal")
	}
}

// ---------------------------------------------------------------------------
// the push pipeline

// TestDriverPutPushReadBackHappyPath: put stores (sha256, size, Files
// entry, NOT Active); push runs the gated pipeline — holder-less op
// caps, HELD fs_write of the sanitized /drv/<name>.lua path carrying
// the decoded bytes, holder-less fs_read back — and only then the
// manifest records Active, PushedAt and the board footprint.
func TestDriverPutPushReadBackHappyPath(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	content := "--@api 1\n-- ads1115 v1.3.0\nlocal M = {}\nreturn M\n"
	noContact(t, reqs) // ws_put is pure workspace: zero daemon contact

	sha := mustPut(t, e, "ads1115", "1.3.0", content)
	if sha != sha256hex([]byte(content)) {
		t.Errorf("ws_put sha256 = %s, want the content's", sha)
	}
	if b, err := os.ReadFile(filepath.Join(root, "drivers", "ads1115@1.3.0.lua")); err != nil || string(b) != content {
		t.Errorf("workspace file = %q, %v; want the stored content verbatim", b, err)
	}
	m := wsManifest(t, root)
	if act := m.Drivers["ads1115"].Active; act != "" {
		t.Errorf("after put only, Active = %q; storing must NOT activate", act)
	}

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.3.0",
	})
	if res.IsError {
		t.Fatalf("driver_push failed: %s", resultText(t, res))
	}
	// wire shapes, in pipeline order
	capsReq := wantReq(t, reqs)
	if capsReq.Type != "op" || capsReq.Op != "caps" || capsReq.Board != "d75884" || capsReq.Holder != "" {
		t.Errorf("caps request = %+v, want a holder-less op caps on d75884", capsReq)
	}
	writeReq := wantReq(t, reqs)
	if writeReq.Type != "fs_write" || writeReq.Path != "/drv/ads1115.lua" || writeReq.Holder != "mcp-fixed01" {
		t.Errorf("fs_write request = %+v, want the held write at /drv/ads1115.lua", writeReq)
	}
	if !reflect.DeepEqual(writeReq.Data, []byte(content)) {
		t.Errorf("fs_write data = %q, want the driver file verbatim", writeReq.Data)
	}
	readReq := wantReq(t, reqs)
	if readReq.Type != "fs_read" || readReq.Path != "/drv/ads1115.lua" || readReq.Holder != "" {
		t.Errorf("fs_read request = %+v, want the holder-less read-back at the same path", readReq)
	}

	out := structured(t, res)
	if out["board"] != "d75884" || out["path"] != "/drv/ads1115.lua" || out["sha256"] != sha {
		t.Errorf("push result = %v, want board/path/sha256", out)
	}
	if bytes, _ := out["bytes"].(float64); int(bytes) != len(content) {
		t.Errorf("push bytes = %v, want %d", out["bytes"], len(content))
	}

	m = wsManifest(t, root)
	e13 := m.Drivers["ads1115"]
	if e13.Active != "1.3.0" {
		t.Errorf("Active = %q, want 1.3.0 after the push", e13.Active)
	}
	fm := e13.Files["1.3.0"]
	if fm.Sha256 != sha || fm.Size != len(content) || fm.PushedAt.IsZero() || fm.Boards["d75884"].IsZero() {
		t.Errorf("Files[1.3.0] = %+v, want sha/size/push time/board footprint", fm)
	}
}

// TestDriverPushAPIMismatchRefusedBeforeWrite: --@api 2 against a
// board reporting api_version 1 is refused locally — BOTH values in
// the message — after the caps answer but before any flash write; the
// manifest stays untouched.
func TestDriverPushAPIMismatchRefusedBeforeWrite(t *testing.T) {
	e, reqs, root := newWsEnv(t, []step{capsStep(1)})
	mustPut(t, e, "ads1115", "1.0.0", drvBody("--@api 2", 64))

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.0.0",
	})
	if !res.IsError {
		t.Fatalf("mismatched --@api push succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api:", "--@api 2", "bench.api_version 1"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q (both values)", got, want)
		}
	}
	if req := wantReq(t, reqs); req.Type != "op" || req.Op != "caps" {
		t.Errorf("only crossing = %+v, want the caps fetch", req)
	}
	noContact(t, reqs) // no fs_write ever left

	m := wsManifest(t, root)
	fm := m.Drivers["ads1115"].Files["1.0.0"]
	if m.Drivers["ads1115"].Active != "" || !fm.PushedAt.IsZero() || len(fm.Boards) != 0 {
		t.Errorf("manifest after refusal = %+v, want Active=\"\" and no push footprint", m.Drivers["ads1115"])
	}
}

// TestDriverPushAPIDirectiveMissingRefusedLocally: a driver without
// the --@api first line never crosses to the daemon at all — the error
// names the file.
func TestDriverPushAPIDirectiveMissingRefusedLocally(t *testing.T) {
	e, reqs, root := newWsEnv(t, nil)
	mustPut(t, e, "mcp9600", "0.9.0", "-- driver without a gate line\nlocal M = {}\n")

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "mcp9600", "version": "0.9.0",
	})
	if !res.IsError {
		t.Fatalf("ungated push succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api:", "mcp9600@0.9.0.lua", "--@api"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	noContact(t, reqs)
	if m := wsManifest(t, root); m.Drivers["mcp9600"].Active != "" {
		t.Errorf("Active = %q after a local refusal, want none", m.Drivers["mcp9600"].Active)
	}
}

// TestWorkspacePutAllowsFatFileButPushRefuses: the workspace keeps
// files above the board's 16384B single-file cap (history is not
// disposable); the PUSH refuses with the fs_write-style message after
// the api gate passed.
func TestWorkspacePutAllowsFatFileButPushRefuses(t *testing.T) {
	e, reqs, root := newWsEnv(t, []step{capsStep(1)})
	content := drvBody("--@api 1", 20000)
	if len(content) <= fsWriteMax {
		t.Fatalf("test content is %dB, want it above the %dB cap", len(content), fsWriteMax)
	}
	mustPut(t, e, "fatdrv", "2.0.0", content) // storing is size-free

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "fatdrv", "version": "2.0.0",
	})
	if !res.IsError {
		t.Fatalf("fat push succeeded: %+v", res)
	}
	want := fmt.Sprintf("api: fs_write /drv/fatdrv.lua: %d bytes exceeds the %dB single-file cap (refused client-side)", len(content), fsWriteMax)
	if got := resultText(t, res); got != want {
		t.Errorf("error text = %q, want exactly %q", got, want)
	}
	if req := wantReq(t, reqs); req.Type != "op" {
		t.Errorf("only crossing = %+v, want the caps fetch", req)
	}
	noContact(t, reqs)
	if _, err := os.Stat(filepath.Join(root, "drivers", "fatdrv@2.0.0.lua")); err != nil {
		t.Errorf("workspace dropped the fat file: %v", err)
	}
	if m := wsManifest(t, root); m.Drivers["fatdrv"].Active != "" {
		t.Errorf("Active = %q after a size refusal, want none", m.Drivers["fatdrv"].Active)
	}
}

// TestDriverPushReadBackMismatchLeavesManifestUntouched: when the
// board's file reads back different bytes, the push fails and the
// manifest records nothing — history only ever holds verified deploys.
func TestDriverPushReadBackMismatchLeavesManifestUntouched(t *testing.T) {
	e, _, root := newWsPushEnv(t, 1, true) // tamper flips the last bit on read-back
	content := "--@api 1\n-- v1\nreturn {}\n"
	mustPut(t, e, "ads1115", "1.1.0", content)

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.1.0",
	})
	if !res.IsError {
		t.Fatalf("tampered read-back push succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"read-back", "manifest untouched", sha256hex([]byte(content))} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}

	m := wsManifest(t, root)
	fm := m.Drivers["ads1115"].Files["1.1.0"]
	if m.Drivers["ads1115"].Active != "" || !fm.PushedAt.IsZero() || len(fm.Boards) != 0 {
		t.Errorf("manifest after read-back mismatch = %+v, want no activation/footprint", m.Drivers["ads1115"])
	}
	if fm.Sha256 != sha256hex([]byte(content)) || fm.Size != len(content) {
		t.Errorf("put-time Files entry = %+v, want it intact", fm)
	}
}

// TestDriverPushFootprintIdempotent: re-pushing the same version
// updates timestamps/Boards in place — no duplicate Files or Boards
// entries, Active stable.
func TestDriverPushFootprintIdempotent(t *testing.T) {
	e, _, root := newWsPushEnv(t, 1, false)
	content := "--@api 1\n-- v1.3.0\nreturn {}\n"
	mustPut(t, e, "ads1115", "1.3.0", content)

	for i := 0; i < 2; i++ {
		res := e.call("xuansi_driver_push", map[string]any{
			"board": "d75884", "name": "ads1115", "version": "1.3.0",
		})
		if res.IsError {
			t.Fatalf("push %d failed: %s", i+1, resultText(t, res))
		}
	}

	m := wsManifest(t, root)
	e13 := m.Drivers["ads1115"]
	if e13.Active != "1.3.0" {
		t.Errorf("Active = %q, want 1.3.0", e13.Active)
	}
	if len(e13.Files) != 1 {
		t.Errorf("Files = %v, want exactly the one version entry (no duplicates)", e13.Files)
	}
	fm := e13.Files["1.3.0"]
	if len(fm.Boards) != 1 || fm.Boards["d75884"].IsZero() {
		t.Errorf("Boards = %v, want exactly the one d75884 footprint", fm.Boards)
	}
	if fm.PushedAt.IsZero() {
		t.Errorf("PushedAt not recorded")
	}
}

// TestConcurrentPushesConsistentManifest: two interleaved pushes of
// different versions to different boards serialize on the manifest
// mutex — last-writer-wins on Active, BOTH board footprints present,
// exactly two version entries.
func TestConcurrentPushesConsistentManifest(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	mustPut(t, e, "ads1115", "1.3.0", drvBody("--@api 1", 64)+"-- v1.3.0\n")
	mustPut(t, e, "ads1115", "1.2.0", drvBody("--@api 1", 48)+"-- v1.2.0\n")

	outA := e.callAsync("xuansi_driver_push", map[string]any{"board": "d75884", "name": "ads1115", "version": "1.3.0"})
	outB := e.callAsync("xuansi_driver_push", map[string]any{"board": "aabbcc", "name": "ads1115", "version": "1.2.0"})
	for _, out := range []chan *mcpsdk.CallToolResult{outA, outB} {
		if res := waitResult(t, out, "concurrent push"); res.IsError {
			t.Fatalf("concurrent push failed: %s", resultText(t, res))
		}
	}
	for i := 0; i < 6; i++ { // caps+write+read per push, some interleaving
		wantReq(t, reqs)
	}

	m := wsManifest(t, root)
	e13 := m.Drivers["ads1115"]
	if len(e13.Files) != 2 {
		t.Fatalf("Files = %v, want both versions", e13.Files)
	}
	if e13.Files["1.3.0"].Boards["d75884"].IsZero() || e13.Files["1.2.0"].Boards["aabbcc"].IsZero() {
		t.Errorf("board footprints = %v / %v, want BOTH present", e13.Files["1.3.0"].Boards, e13.Files["1.2.0"].Boards)
	}
	if act := e13.Active; act != "1.3.0" && act != "1.2.0" {
		t.Errorf("Active = %q, want last-writer-wins of the two", act)
	}
}

// TestDriverPushRefreshesManifestShaFromDisk (B7 minors item 1): a
// workspace file modified on disk after its ws_put must not leave a
// STALE manifest checksum — the push ships the CURRENT bytes and the
// manifest records exactly those (single source = disk at push time).
// Pre-fix behavior (red): the "fill only when empty" guard kept the
// put-time sha, so the manifest claimed bytes the board never saw.
func TestDriverPushRefreshesManifestShaFromDisk(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	v1 := "--@api 1\n-- v1 original\nreturn {}\n"
	v2 := "--@api 1\n-- v2 modified on disk behind the manifest's back\nreturn {changed=true}\n"
	mustPut(t, e, "ads1115", "1.0.0", v1)

	// external modification of the stored file (out-of-band edit)
	if err := os.WriteFile(filepath.Join(root, "drivers", "ads1115@1.0.0.lua"), []byte(v2), 0o644); err != nil {
		t.Fatal(err)
	}

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("push of the externally modified file failed: %s", resultText(t, res))
	}
	// the pipeline crossing order: caps, write, read-back
	wantReq(t, reqs)
	writeReq := wantReq(t, reqs)
	wantReq(t, reqs)
	if !reflect.DeepEqual(writeReq.Data, []byte(v2)) {
		t.Fatalf("fs_write shipped %q, want the MODIFIED disk bytes", writeReq.Data)
	}

	sha2 := sha256hex([]byte(v2))
	if out := structured(t, res); out["sha256"] != sha2 {
		t.Errorf("push result sha256 = %v, want the modified bytes'", out["sha256"])
	}
	m := wsManifest(t, root)
	fm := m.Drivers["ads1115"].Files["1.0.0"]
	if fm.Sha256 != sha2 {
		t.Errorf("manifest sha256 = %s, want the disk-at-push-time %s (put-time sha %s must not survive)",
			fm.Sha256, sha2, sha256hex([]byte(v1)))
	}
	if fm.Size != len(v2) {
		t.Errorf("manifest size = %d, want %d (the modified bytes)", fm.Size, len(v2))
	}
}

// ---------------------------------------------------------------------------
// defaults, sanitization, caps reuse, list/get

// TestDriverPushDefaultsToActiveVersion: an omitted version re-deploys
// the ACTIVE one (rollback says its version explicitly); no active
// version and no explicit one is a local refusal; a never-put version
// names the missing file.
func TestDriverPushDefaultsToActiveVersion(t *testing.T) {
	e, reqs, _ := newWsPushEnv(t, 1, false)
	v13 := "--@api 1\n-- v1.3.0 active\nreturn {}\n"
	v12 := "--@api 1\n-- v1.2.0 old\nreturn {}\n"
	mustPut(t, e, "ads1115", "1.3.0", v13)
	mustPut(t, e, "ads1115", "1.2.0", v12)

	if res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.3.0",
	}); res.IsError {
		t.Fatalf("explicit push failed: %s", resultText(t, res))
	}
	for i := 0; i < 3; i++ {
		wantReq(t, reqs)
	}

	// default = Active (1.3.0), NOT the newest put (1.2.0). No caps
	// cache in this static-only env, so this push makes the same three
	// crossings as any push.
	res := e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "ads1115"})
	if res.IsError {
		t.Fatalf("default-version push failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Type != "op" || req.Op != "caps" {
		t.Errorf("default-version crossing = %+v, want the caps fetch first", req)
	}
	if req := wantReq(t, reqs); req.Type != "fs_write" || !reflect.DeepEqual(req.Data, []byte(v13)) {
		t.Errorf("default-version write = %+v (%d bytes), want the ACTIVE 1.3.0 payload", req, len(req.Data))
	}
	wantReq(t, reqs) // the read-back

	// no active version: local refusal, no daemon contact
	mustPut(t, e, "orphan", "0.1.0", "--@api 1\nreturn {}\n")
	res = e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "orphan"})
	if !res.IsError {
		t.Fatalf("push without an active version succeeded: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "no active version") {
		t.Errorf("error text = %q, want the no-active-version refusal", got)
	}
	noContact(t, reqs)

	// a version that was never put names the missing file
	res = e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "9.9.9",
	})
	if !res.IsError || !strings.Contains(resultText(t, res), "ads1115@9.9.9.lua") {
		t.Errorf("never-put push = %v (%v), want a refusal naming the file", res, resultText(t, res))
	}
}

// TestWorkspaceSanitizationRefusesTraversal: names outside
// [a-z0-9_-]+ and versions outside [a-z0-9.]+ are local refusals —
// "../evil", "a/b", "A!" never reach a path construction; the drivers
// directory stays empty.
func TestWorkspaceSanitizationRefusesTraversal(t *testing.T) {
	e, reqs, root := newWsEnv(t, nil)
	for _, name := range []string{"../evil", "a/b", "A!", "ads 1115", ""} {
		res := e.call("xuansi_ws_put", map[string]any{"name": name, "version": "1.0.0", "content": "--@api 1\n"})
		if !res.IsError {
			t.Errorf("ws_put name %q succeeded, want a local refusal", name)
			continue
		}
		if got := resultText(t, res); !strings.HasPrefix(got, "api:") || !strings.Contains(got, "name") {
			t.Errorf("ws_put name %q error = %q, want an api-class refusal about the name", name, got)
		}
	}
	for _, ver := range []string{"1.0.0/x", "../1.0", "1.0 B", ""} {
		res := e.call("xuansi_ws_put", map[string]any{"name": "ads1115", "version": ver, "content": "--@api 1\n"})
		if !res.IsError || !strings.HasPrefix(resultText(t, res), "api:") {
			t.Errorf("ws_put version %q = %v, want an api-class refusal", ver, res)
		}
	}
	res := e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "../evil", "version": "1.0.0"})
	if !res.IsError || !strings.HasPrefix(resultText(t, res), "api:") {
		t.Errorf("driver_push traversal name = %v, want an api-class refusal", res)
	}
	res = e.call("xuansi_ws_get", map[string]any{"name": "../evil", "version": "1.0.0"})
	if !res.IsError || !strings.HasPrefix(resultText(t, res), "api:") {
		t.Errorf("ws_get traversal name = %v, want an api-class refusal", res)
	}
	noContact(t, reqs)

	drvs, err := os.ReadDir(filepath.Join(root)) // root has no drivers/ dir at all until a valid put
	if err == nil && len(drvs) != 0 {
		t.Errorf("workspace wrote %v despite only refusals", drvs)
	}
}

// TestDriverPushReusesFreshCapsCache: with the board in the local list
// and its caps cached fresh (the dynamic refresh's cache), the push
// skips its own op caps — the first crossing is the fs_write; a stale
// cache re-fetches.
func TestDriverPushReusesFreshCapsCache(t *testing.T) {
	e, reqs, _ := newWsPushEnv(t, 1, false)
	// no Refresh loop: reconcile the event face's board list directly
	// (this is what refreshEvents does with the refresh's answer) and
	// seed the caps cache the way a refresh would.
	e.ts.ev.reconcile([]dynBoard{{mac6: "d75884"}})
	e.ts.dyn.capsStore("d75884", 1)
	mustPut(t, e, "ads1115", "1.0.0", "--@api 1\n-- cached caps gate\nreturn {}\n")

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("push with cached caps failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Type != "fs_write" {
		t.Errorf("first crossing = %+v, want fs_write (caps must come from the cache)", req)
	}
	wantReq(t, reqs) // the read-back

	// stale cache: the next push fetches caps again
	e.ts.dyn.mu.Lock()
	stale := e.ts.dyn.caps["d75884"]
	stale.at = time.Now().Add(-2 * capsFreshTTL)
	e.ts.dyn.caps["d75884"] = stale
	e.ts.dyn.mu.Unlock()
	if res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.0.0",
	}); res.IsError {
		t.Fatalf("push with stale caps failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Type != "op" || req.Op != "caps" {
		t.Errorf("first crossing after staleness = %+v, want op caps", req)
	}
}

// TestWorkspaceListAndGetRoundTrip: ws_list reports per driver the
// active version and every version with size+sha256 (names and
// versions sorted); ws_get returns the stored content and its sha256.
func TestWorkspaceListAndGetRoundTrip(t *testing.T) {
	e, _, _ := newWsPushEnv(t, 1, false)
	a := "--@api 1\n-- ads v1.3.0\nreturn {}\n"
	b := "--@api 1\n-- ads v1.2.0\nreturn {}\n"
	z := "--@api 1\n-- zdriver v0.1.0\nreturn {}\n"
	mustPut(t, e, "ads1115", "1.3.0", a)
	mustPut(t, e, "ads1115", "1.2.0", b)
	mustPut(t, e, "zdriver", "0.1.0", z)

	res := e.call("xuansi_ws_list", nil)
	if res.IsError {
		t.Fatalf("ws_list failed: %s", resultText(t, res))
	}
	m := structured(t, res)
	drivers, _ := m["drivers"].([]any)
	if len(drivers) != 2 {
		t.Fatalf("drivers = %v, want two", m["drivers"])
	}
	d0, _ := drivers[0].(map[string]any)
	if d0["name"] != "ads1115" {
		t.Errorf("drivers[0] = %v, want ads1115 first (sorted)", d0)
	}
	if act, _ := d0["active"].(string); act != "" { // omitempty: absent before any push
		t.Errorf("drivers[0].active = %v, want none before a first push", d0["active"])
	}
	vers, _ := d0["versions"].([]any)
	if len(vers) != 2 {
		t.Fatalf("ads1115 versions = %v, want both", d0["versions"])
	}
	v0, _ := vers[0].(map[string]any)
	if v0["version"] != "1.2.0" || v0["sha256"] != sha256hex([]byte(b)) {
		t.Errorf("versions[0] = %v, want 1.2.0 with its checksum", v0)
	}
	if size, _ := v0["size"].(float64); int(size) != len(b) {
		t.Errorf("versions[0].size = %v, want %d", v0["size"], len(b))
	}

	// after a push, the active version shows
	if res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.3.0",
	}); res.IsError {
		t.Fatalf("push failed: %s", resultText(t, res))
	}
	res = e.call("xuansi_ws_list", nil)
	m = structured(t, res)
	drivers, _ = m["drivers"].([]any)
	d0, _ = drivers[0].(map[string]any)
	if d0["active"] != "1.3.0" {
		t.Errorf("active after push = %v, want 1.3.0", d0["active"])
	}

	res = e.call("xuansi_ws_get", map[string]any{"name": "ads1115", "version": "1.2.0"})
	if res.IsError {
		t.Fatalf("ws_get failed: %s", resultText(t, res))
	}
	g := structured(t, res)
	if g["content"] != b || g["sha256"] != sha256hex([]byte(b)) {
		t.Errorf("ws_get = %v, want the stored v1.2.0 content + checksum", g)
	}

	// a version that is not there says so
	res = e.call("xuansi_ws_get", map[string]any{"name": "ads1115", "version": "9.9.9"})
	if !res.IsError || !strings.Contains(resultText(t, res), "ads1115@9.9.9.lua") {
		t.Errorf("ws_get missing = %v, want a refusal naming the file", res)
	}
}
