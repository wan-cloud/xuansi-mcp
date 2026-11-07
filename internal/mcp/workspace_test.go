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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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
			case "fs_rm":
				delete(files, req.Board+"\x00"+req.Path)
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{}}
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

// TestWriteDurableFileNeverTorn (B8 item 5 red unit): the atomic
// (temp+Sync+rename+dir-fsync) write means the final path is always
// absent-or-COMPLETE under concurrent readers — a reader loop hammering
// the file while a writer alternates two payloads never observes a
// partial file (the pre-B8 in-place O_TRUNC write was durable but
// tearable mid-write, leaving the manifest's old sha disagreeing with
// the half-written driver until a push re-verified). The crash half of
// the story — the rename itself surviving power loss — stays
// code-review-only: it cannot be unit-proven without crash simulation.
func TestWriteDurableFileNeverTorn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "driver.lua")
	a := bytes.Repeat([]byte("A"), 4<<20)
	b := bytes.Repeat([]byte("b"), 4<<20)
	if err := writeDurableFile(path, a, 0o644); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	var torn atomic.Int64
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Inter-read gap (CI matrix round 5, final): a real
				// reader never zero-gap loops — and on windows the
				// writer's MoveFileEx-replace still needs an instant
				// with the target unopened even against share-delete
				// readers, so a ~99%-duty hammer loop starves it.
				// 2ms per reader leaves ample rename windows while the
				// 100-write storm still runs under continuous
				// contention; the torn assertions are unchanged.
				time.Sleep(2 * time.Millisecond)
				got, err := readFileShared(path) // share-delete read + transient-contention retry (windows; see readshared_windows.go)
				if errors.Is(err, fs.ErrNotExist) {
					continue // absent is legal (absent-or-complete)
				}
				if err != nil {
					t.Errorf("reader: %v", err)
					return
				}
				if !bytes.Equal(got, a) && !bytes.Equal(got, b) {
					torn.Add(1) // a prefix/fragment — the torn state
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		if err := writeDurableFile(path, a, 0o644); err != nil {
			t.Fatalf("write A: %v", err)
		}
		if err := writeDurableFile(path, b, 0o644); err != nil {
			t.Fatalf("write b: %v", err)
		}
	}
	close(stop)
	readers.Wait()
	if n := torn.Load(); n != 0 {
		t.Fatalf("readers observed %d torn reads — the final path must always be absent-or-complete", n)
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

// TestWorkspaceFatFilePushesWholeOverChunkedTransport: the workspace
// keeps files of any size, and since B10-T1 the PUSH crosses whole —
// the daemon-side chunked transport (fs.write_part × N + sha-verifying
// commit) carries payloads above the board's 16384B single-op cap, so
// a fat .aot-era driver deploys in one push with the read-back verify.
func TestWorkspaceFatFilePushesWholeOverChunkedTransport(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	content := drvBody("--@api 1", 20000)
	if len(content) <= 16384 {
		t.Fatalf("test content is %dB, want it above the 16384B single-op cap", len(content))
	}
	mustPut(t, e, "fatdrv", "2.0.0", content) // storing is size-free

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "fatdrv", "version": "2.0.0",
	})
	if res.IsError {
		t.Fatalf("fat push failed: %s", resultText(t, res))
	}
	// wire shapes, in pipeline order
	capsReq := wantReq(t, reqs)
	if capsReq.Type != "op" || capsReq.Op != "caps" {
		t.Errorf("first crossing = %+v, want the holder-less op caps", capsReq)
	}
	writeReq := wantReq(t, reqs)
	if writeReq.Type != "fs_write" || writeReq.Path != "/drv/fatdrv.lua" || writeReq.Holder != "mcp-fixed01" {
		t.Errorf("fs_write request = %+v, want the held write at /drv/fatdrv.lua", writeReq)
	}
	if len(writeReq.Data) != len(content) {
		t.Errorf("fs_write carried %d bytes, want the whole %d-byte driver", len(writeReq.Data), len(content))
	}
	readReq := wantReq(t, reqs)
	if readReq.Type != "fs_read" || readReq.Path != "/drv/fatdrv.lua" {
		t.Errorf("fs_read request = %+v, want the holder-less read-back", readReq)
	}
	if _, err := os.Stat(filepath.Join(root, "drivers", "fatdrv@2.0.0.lua")); err != nil {
		t.Errorf("workspace dropped the fat file: %v", err)
	}
	if m := wsManifest(t, root); m.Drivers["fatdrv"].Active != "2.0.0" {
		t.Errorf("Active = %q after a whole-file push, want 2.0.0", m.Drivers["fatdrv"].Active)
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

// ---------------------------------------------------------------------------
// xuansi_driver_fetch (B10 task 5: the fetch leg — index → workspace)

// fetchFileSpec is one fake-index file: what the index DECLARES
// (sha256/bytes — may lie) and what the server SERVES (body, or a
// non-200 status to simulate a mid-download failure).
type fetchFileSpec struct {
	path   string
	sha256 string
	bytes  int
	body   []byte
	status int
}

// fetchModSpec is one fake-index module entry.
type fetchModSpec struct {
	name, kind, version string
	files               []fetchFileSpec
}

// fetchSpec builds an honest file spec (declared sha/bytes match the
// body); lieSha overrides the declared sha for mismatch tests.
func fetchSpec(t *testing.T, path string, body []byte, lieSha string) fetchFileSpec {
	t.Helper()
	sha := sha256hex(body)
	if lieSha != "" {
		sha = lieSha
	}
	return fetchFileSpec{path: path, sha256: sha, bytes: len(body), body: body}
}

// newFetchIndex serves a fake T3-shaped index plus its module files
// and returns the index URL. hits counts every request so local
// refusals can prove zero HTTP contact.
func newFetchIndex(t *testing.T, mods []fetchModSpec) (idxURL string, hits *atomic.Int64) {
	t.Helper()
	type fileEnt struct {
		Path   string `json:"path"`
		Sha256 string `json:"sha256"`
		Bytes  int    `json:"bytes"`
	}
	type modEnt struct {
		Name    string    `json:"name"`
		Kind    string    `json:"kind"`
		Version string    `json:"version"`
		ABI     int       `json:"abi"`
		Files   []fileEnt `json:"files"`
	}
	modsJ := make([]modEnt, 0, len(mods))
	for _, m := range mods {
		me := modEnt{Name: m.name, Kind: m.kind, Version: m.version, ABI: 1}
		for _, f := range m.files {
			me.Files = append(me.Files, fileEnt{Path: f.path, Sha256: f.sha256, Bytes: f.bytes})
		}
		modsJ = append(modsJ, me)
	}
	idxJ, err := json.Marshal(map[string]any{"index_ver": 1, "modules": modsJ})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	var n atomic.Int64
	mux.HandleFunc("/index.json", func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(idxJ)
	})
	for _, m := range mods {
		for _, f := range m.files {
			f := f
			mux.HandleFunc("/"+f.path, func(w http.ResponseWriter, _ *http.Request) {
				n.Add(1)
				if f.status != 0 {
					w.WriteHeader(f.status)
					return
				}
				_, _ = w.Write(f.body)
			})
		}
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL + "/index.json", &n
}

// fetchWasmBody builds a deterministic fake wasm artifact (real magic).
func fetchWasmBody(n int) []byte {
	b := append([]byte(nil), wasmMagic[:]...)
	b = append(b, 0x01, 0x00, 0x00, 0x00) // a version dword, like the real files
	for len(b) < n {
		b = append(b, byte('w'))
	}
	return b
}

// fetchAotBody builds a deterministic fake AOT artifact (real magic).
func fetchAotBody(n int) []byte {
	b := append([]byte(nil), aotMagic[:]...)
	b = append(b, 0x04, 0x00, 0x00, 0x00)
	for len(b) < n {
		b = append(b, byte('a'))
	}
	return b
}

// TestDriverFetchTwoFilesKindAndManifest: the happy fetch — a two-file
// index entry (wasm delivery + aot auxiliary) lands BOTH artifacts
// byte-exact under drivers/, the manifest persists the index's Kind,
// and the version's FileMeta records the artifact a bare push deploys
// (.wasm — the .lua > .wasm > .aot priority with no .lua stored).
// Fetching never crosses to the daemon.
func TestDriverFetchTwoFilesKindAndManifest(t *testing.T) {
	e, reqs, root := newWsEnv(t, nil)
	wbody, abody := fetchWasmBody(64), fetchAotBody(48)
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "fakemod", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{
			fetchSpec(t, "modules/fakemod/1.0.0/fakemod.wasm", wbody, ""),
			fetchSpec(t, "modules/fakemod/1.0.0/fakemod.aot", abody, ""),
		},
	}})

	res := e.call("xuansi_driver_fetch", map[string]any{
		"name": "fakemod", "version": "1.0.0", "index_url": url,
	})
	if res.IsError {
		t.Fatalf("driver_fetch failed: %s", resultText(t, res))
	}
	noContact(t, reqs) // the fetch leg never touches the daemon

	m := structured(t, res)
	if m["name"] != "fakemod" || m["version"] != "1.0.0" || m["kind"] != "driver" {
		t.Errorf("fetch result identity = %v, want fakemod/1.0.0/driver", m)
	}
	if tot, _ := m["bytes"].(float64); int(tot) != len(wbody)+len(abody) {
		t.Errorf("fetch bytes = %v, want %d (the total)", m["bytes"], len(wbody)+len(abody))
	}
	files, _ := m["files"].([]any)
	if len(files) != 2 {
		t.Fatalf("files = %v, want the two artifacts", m["files"])
	}
	for i, want := range []struct {
		path string
		sha  string
		n    int
	}{
		{"drivers/fakemod@1.0.0.wasm", sha256hex(wbody), len(wbody)},
		{"drivers/fakemod@1.0.0.aot", sha256hex(abody), len(abody)},
	} {
		f, _ := files[i].(map[string]any)
		if f["path"] != want.path || f["sha256"] != want.sha || int(f["bytes"].(float64)) != want.n {
			t.Errorf("files[%d] = %v, want %s with the verified checksum", i, f, want.path)
		}
	}

	// both artifacts on disk, byte-exact
	for _, want := range []struct {
		path string
		body []byte
	}{{"drivers/fakemod@1.0.0.wasm", wbody}, {"drivers/fakemod@1.0.0.aot", abody}} {
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(want.path)))
		if err != nil || !bytes.Equal(b, want.body) {
			t.Errorf("workspace %s = %d bytes, %v; want the downloaded body verbatim", want.path, len(b), err)
		}
	}

	// manifest: Kind persisted, FileMeta = the push-deployed artifact
	// (.wasm), Active untouched (storing is NOT activating)
	mf := wsManifest(t, root)
	ent := mf.Drivers["fakemod"]
	if ent.Kind != "driver" {
		t.Errorf("manifest Kind = %q, want driver", ent.Kind)
	}
	fm := ent.Files["1.0.0"]
	if fm.Sha256 != sha256hex(wbody) || fm.Size != len(wbody) {
		t.Errorf("Files[1.0.0] = %+v, want the .wasm's sha/size (the bare-push artifact)", fm)
	}
	if ent.Active != "" {
		t.Errorf("Active = %q after a fetch, want none (a push activates)", ent.Active)
	}
}

// TestDriverFetchShaMismatchRefusedZeroWrites: a file whose sha256
// disagrees with the index is refused, and NOTHING is written — no
// manifest, no drivers/ dir (the download is staged and verified in
// memory before the first durable write).
func TestDriverFetchShaMismatchRefusedZeroWrites(t *testing.T) {
	e, _, root := newWsEnv(t, nil)
	body := fetchWasmBody(32)
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "fakemod", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{fetchSpec(t, "modules/fakemod/1.0.0/fakemod.wasm", body, sha256hex(fetchWasmBody(33)))},
	}})

	res := e.call("xuansi_driver_fetch", map[string]any{
		"name": "fakemod", "version": "1.0.0", "index_url": url,
	})
	if !res.IsError {
		t.Fatalf("mismatched-sha fetch succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api:", "sha256", "modules/fakemod/1.0.0/fakemod.wasm", sha256hex(body)} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "manifest.json")); !os.IsNotExist(err) {
		t.Errorf("manifest.json exists after a refused fetch: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "drivers")); !os.IsNotExist(err) {
		t.Errorf("drivers/ dir exists after a refused fetch: %v", err)
	}
}

// TestDriverFetchNameAbsentRefused: a name the index does not carry is
// a typed refusal naming the index; exactly ONE HTTP request happened
// (the index itself); nothing written.
func TestDriverFetchNameAbsentRefused(t *testing.T) {
	e, _, root := newWsEnv(t, nil)
	url, hits := newFetchIndex(t, []fetchModSpec{{
		name: "hello", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{fetchSpec(t, "modules/hello/1.0.0/hello.wasm", fetchWasmBody(16), "")},
	}})

	res := e.call("xuansi_driver_fetch", map[string]any{"name": "nosuch", "index_url": url})
	if !res.IsError {
		t.Fatalf("absent-name fetch succeeded: %+v", res)
	}
	got := resultText(t, res)
	if !strings.HasPrefix(got, "api:") || !strings.Contains(got, "nosuch") || !strings.Contains(got, url) {
		t.Errorf("error text = %q, want an api-class refusal naming the module and index", got)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("HTTP hits = %d, want exactly 1 (the index; no file download for an absent name)", n)
	}
	if _, err := os.Stat(filepath.Join(root, "manifest.json")); !os.IsNotExist(err) {
		t.Errorf("manifest.json exists after a refused fetch: %v", err)
	}
}

// TestDriverFetchMidDownload404Atomic (Review Focus #5,
// download-atomicity): a TWO-file entry whose SECOND file 404s mid-
// download leaves ZERO manifest writes and no half state — the first
// (already verified) file never touched the workspace either.
func TestDriverFetchMidDownload404Atomic(t *testing.T) {
	e, _, root := newWsEnv(t, nil)
	wbody := fetchWasmBody(64)
	url, hits := newFetchIndex(t, []fetchModSpec{{
		name: "fakemod", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{
			fetchSpec(t, "modules/fakemod/1.0.0/fakemod.wasm", wbody, ""),
			{path: "modules/fakemod/1.0.0/fakemod.aot", sha256: sha256hex(fetchAotBody(48)), bytes: 48, status: 404},
		},
	}})

	res := e.call("xuansi_driver_fetch", map[string]any{
		"name": "fakemod", "version": "1.0.0", "index_url": url,
	})
	if !res.IsError {
		t.Fatalf("fetch with a dying second file succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api:", "fakemod.aot", "404"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	if n := hits.Load(); n != 3 {
		t.Errorf("HTTP hits = %d, want 3 (index + file1 + the 404)", n)
	}
	if _, err := os.Stat(filepath.Join(root, "manifest.json")); !os.IsNotExist(err) {
		t.Errorf("manifest.json exists after a mid-download failure: %v (zero manifest writes promised)", err)
	}
	if _, err := os.Stat(filepath.Join(root, "drivers")); !os.IsNotExist(err) {
		t.Errorf("drivers/ dir exists after a mid-download failure: %v (no half workspace state promised)", err)
	}
}

// TestDriverFetchIdempotent: fetching the same version twice re-verifies
// and re-writes the same bytes — one driver, one version entry, one
// file on disk each, manifest stable.
func TestDriverFetchIdempotent(t *testing.T) {
	e, _, root := newWsEnv(t, nil)
	body := fetchWasmBody(40)
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "fakemod", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{fetchSpec(t, "modules/fakemod/1.0.0/fakemod.wasm", body, "")},
	}})
	for i := 0; i < 2; i++ {
		res := e.call("xuansi_driver_fetch", map[string]any{
			"name": "fakemod", "version": "1.0.0", "index_url": url,
		})
		if res.IsError {
			t.Fatalf("fetch %d failed: %s", i+1, resultText(t, res))
		}
	}
	m := wsManifest(t, root)
	ent := m.Drivers["fakemod"]
	if len(ent.Files) != 1 || ent.Files["1.0.0"].Sha256 != sha256hex(body) {
		t.Errorf("manifest after re-fetch = %+v, want exactly the one version entry", ent)
	}
	b, err := os.ReadFile(filepath.Join(root, "drivers", "fakemod@1.0.0.wasm"))
	if err != nil || !bytes.Equal(b, body) {
		t.Errorf("workspace file after re-fetch = %d bytes, %v; want the same body", len(b), err)
	}
	listRes := e.call("xuansi_ws_list", nil)
	if listRes.IsError {
		t.Fatalf("ws_list failed: %s", resultText(t, listRes))
	}
	drivers, _ := structured(t, listRes)["drivers"].([]any)
	if len(drivers) != 1 {
		t.Errorf("ws_list drivers = %v, want the one entry", drivers)
	}
}

// TestDriverFetchVersionDefaultHighest: an omitted version picks the
// index's HIGHEST for the name (dotted-numeric order: 1.2.0 > 1.10.0?
// no — 1.10.0 > 1.2.0, numeric segments); an explicit version picks
// exactly it; an absent version names what the index has.
func TestDriverFetchVersionDefaultHighest(t *testing.T) {
	e, _, root := newWsEnv(t, nil)
	b100, b110, b120 := fetchWasmBody(16), fetchWasmBody(17), fetchWasmBody(18)
	url, _ := newFetchIndex(t, []fetchModSpec{
		{name: "fakemod", kind: "driver", version: "1.2.0",
			files: []fetchFileSpec{fetchSpec(t, "modules/fakemod/1.2.0/fakemod.wasm", b120, "")}},
		{name: "fakemod", kind: "driver", version: "1.10.0",
			files: []fetchFileSpec{fetchSpec(t, "modules/fakemod/1.10.0/fakemod.wasm", b110, "")}},
		{name: "fakemod", kind: "driver", version: "1.0.0",
			files: []fetchFileSpec{fetchSpec(t, "modules/fakemod/1.0.0/fakemod.wasm", b100, "")}},
	})

	res := e.call("xuansi_driver_fetch", map[string]any{"name": "fakemod", "index_url": url})
	if res.IsError {
		t.Fatalf("default-version fetch failed: %s", resultText(t, res))
	}
	if m := structured(t, res); m["version"] != "1.10.0" {
		t.Errorf("default version = %v, want the highest 1.10.0 (numeric segments, not lexical)", m["version"])
	}
	if _, err := os.Stat(filepath.Join(root, "drivers", "fakemod@1.10.0.wasm")); err != nil {
		t.Errorf("highest-version artifact not stored: %v", err)
	}

	if res := e.call("xuansi_driver_fetch", map[string]any{
		"name": "fakemod", "version": "1.0.0", "index_url": url,
	}); res.IsError {
		t.Fatalf("explicit-version fetch failed: %s", resultText(t, res))
	} else if m := structured(t, res); m["version"] != "1.0.0" {
		t.Errorf("explicit version = %v, want 1.0.0", m["version"])
	}

	res = e.call("xuansi_driver_fetch", map[string]any{
		"name": "fakemod", "version": "9.9.9", "index_url": url,
	})
	if !res.IsError {
		t.Fatalf("absent-version fetch succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"9.9.9", "1.0.0", "1.2.0", "1.10.0"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to name %q among the available versions", got, want)
		}
	}
}

// TestDriverFetchLocalRefusalsZeroContact: name/version/scheme
// refusals happen BEFORE any HTTP (hits stay at the running count);
// a garbage or future-index_ver index is a typed refusal too.
func TestDriverFetchLocalRefusalsZeroContact(t *testing.T) {
	e, _, root := newWsEnv(t, nil)
	url, hits := newFetchIndex(t, []fetchModSpec{{
		name: "fakemod", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{fetchSpec(t, "modules/fakemod/1.0.0/fakemod.wasm", fetchWasmBody(16), "")},
	}})
	base := hits.Load()

	// local refusals: zero HTTP contact
	for _, args := range []map[string]any{
		{"name": "../evil", "index_url": url},
		{"name": "A!", "index_url": url},
		{"name": "fakemod", "version": "1/x", "index_url": url},
		{"name": "fakemod", "index_url": "ftp://example.com/index.json"},
		{"name": "fakemod", "index_url": "not a url"},
	} {
		res := e.call("xuansi_driver_fetch", args)
		if !res.IsError || !strings.HasPrefix(resultText(t, res), "api:") {
			t.Errorf("fetch %v = %v, want a local api-class refusal", args["name"], res)
		}
	}
	if n := hits.Load(); n != base {
		t.Errorf("HTTP hits moved %d → %d on purely-local refusals", base, n)
	}
	if _, err := os.Stat(filepath.Join(root, "manifest.json")); !os.IsNotExist(err) {
		t.Errorf("manifest.json exists after only refusals: %v", err)
	}

	// garbage index body: one HTTP hit, typed refusal
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json at all"))
	}))
	t.Cleanup(garbage.Close)
	res := e.call("xuansi_driver_fetch", map[string]any{"name": "fakemod", "index_url": garbage.URL + "/index.json"})
	if !res.IsError || !strings.Contains(resultText(t, res), "not valid JSON") {
		t.Errorf("garbage-index fetch = %v (%v), want a not-valid-JSON refusal", res, resultText(t, res))
	}
}

// TestDriverPushWasmArtifactMagicGated: the B10 push gate branches by
// artifact — a stored .wasm pushes to /drv/<name>.wasm with NO caps
// crossing (the wasm track's compatibility axis is the module ABI the
// loader checks, not bench.api_version), read-back verified, manifest
// Active set; a garbage-magic .wasm is refused locally with zero
// crossings. Binary artifacts are stored out-of-band (ws_put's content
// is a JSON string — the fetch tool is the binary-safe storer).
func TestDriverPushWasmArtifactMagicGated(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	body := fetchWasmBody(96)
	drvDir := filepath.Join(root, "drivers")
	if err := os.MkdirAll(drvDir, 0o755); err != nil {
		t.Fatal(err)
	}
	wasmPath := filepath.Join(drvDir, "hellow@1.0.0.wasm")
	if err := os.WriteFile(wasmPath, body, 0o644); err != nil {
		t.Fatal(err)
	}

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "hellow", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("wasm push failed: %s", resultText(t, res))
	}
	// pipeline: fs_write FIRST (no op caps for a wasm artifact), then the read-back
	writeReq := wantReq(t, reqs)
	if writeReq.Type != "fs_write" || writeReq.Path != "/drv/hellow.wasm" || writeReq.Holder != "mcp-fixed01" {
		t.Errorf("first crossing = %+v, want the held fs_write at /drv/hellow.wasm (no caps for the wasm track)", writeReq)
	}
	if !bytes.Equal(writeReq.Data, body) {
		t.Errorf("fs_write data = %d bytes, want the artifact verbatim", len(writeReq.Data))
	}
	readReq := wantReq(t, reqs)
	if readReq.Type != "fs_read" || readReq.Path != "/drv/hellow.wasm" {
		t.Errorf("read-back = %+v, want fs_read at /drv/hellow.wasm", readReq)
	}
	noContact(t, reqs)
	if out := structured(t, res); out["path"] != "/drv/hellow.wasm" || out["sha256"] != sha256hex(body) {
		t.Errorf("push result = %v, want the wasm path + verified sha", out)
	}
	if m := wsManifest(t, root); m.Drivers["hellow"].Active != "1.0.0" {
		t.Errorf("Active = %q after the wasm push, want 1.0.0", m.Drivers["hellow"].Active)
	}

	// garbage magic: refused locally, zero crossings, manifest Active untouched
	garbage := []byte("GARBAGE-not-a-module-at-all--------")
	if err := os.WriteFile(wasmPath, garbage, 0o644); err != nil {
		t.Fatal(err)
	}
	res = e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "hellow", "version": "1.0.0",
	})
	if !res.IsError {
		t.Fatalf("garbage-magic push succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api:", "hellow@1.0.0.wasm", "magic"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	noContact(t, reqs)
}

// TestDriverPushAotArtifactAndLuaPriority: an .aot artifact pushes to
// /drv/<name>.aot under the AOT magic gate; and when BOTH a .lua and a
// binary artifact are stored, the classic .lua wins the push —
// pre-B10 workspaces behave exactly as before.
func TestDriverPushAotArtifactAndLuaPriority(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	abody := fetchAotBody(64)
	drvDir := filepath.Join(root, "drivers")
	if err := os.MkdirAll(drvDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// the .aot leg
	if err := os.WriteFile(filepath.Join(drvDir, "watchaot@1.0.0.aot"), abody, 0o644); err != nil {
		t.Fatal(err)
	}
	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "watchaot", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("aot push failed: %s", resultText(t, res))
	}
	writeReq := wantReq(t, reqs)
	if writeReq.Type != "fs_write" || writeReq.Path != "/drv/watchaot.aot" {
		t.Errorf("aot push crossing = %+v, want fs_write at /drv/watchaot.aot", writeReq)
	}
	wantReq(t, reqs) // the read-back

	// the priority leg: .lua + .wasm both stored → the .lua deploys
	// (caps crossing first — the --@api gate unchanged)
	luaBody := "--@api 1\n-- classic wins\nreturn {}\n"
	mustPut(t, e, "mixmod", "1.0.0", luaBody)
	if err := os.WriteFile(filepath.Join(drvDir, "mixmod@1.0.0.wasm"), fetchWasmBody(32), 0o644); err != nil {
		t.Fatal(err)
	}
	res = e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "mixmod", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("mixed-artifact push failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Type != "op" || req.Op != "caps" {
		t.Errorf("mixed push first crossing = %+v, want op caps (the .lua gate runs)", req)
	}
	if req := wantReq(t, reqs); req.Type != "fs_write" || req.Path != "/drv/mixmod.lua" || !bytes.Equal(req.Data, []byte(luaBody)) {
		t.Errorf("mixed push write = %+v, want the .lua artifact at /drv/mixmod.lua", req)
	}
	wantReq(t, reqs) // the read-back
}

// TestDriverPushMissingArtifactRefusalNamesCandidates: a version with
// NO stored artifact under any extension is the classic refusal, now
// also pointing at the fetch tool.
func TestDriverPushMissingArtifactRefusalNamesCandidates(t *testing.T) {
	e, reqs, _ := newWsEnv(t, nil)
	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ghost", "version": "9.9.9",
	})
	if !res.IsError {
		t.Fatalf("artifact-less push succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api:", "ghost@9.9.9.lua", "xuansi_driver_fetch"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	noContact(t, reqs)
}

// ---------------------------------------------------------------------------
// driver_pull (B8 task 1: the pipeline's removal path)

// TestDriverPullClearsActiveKeepsFootprints: after a verified push,
// pull removes the board's effective file through ONE held fs_rm of
// /drv/<name>.lua and clears the manifest's Active - while the Files
// entries and per-board push footprints STAY (history never lies;
// rollback = re-push), so ws_list still offers every stored version.
func TestDriverPullClearsActiveKeepsFootprints(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	content := "--@api 1\n-- b8pull v1.0.0\nreturn {tag=\"b8pull\"}\n"
	mustPut(t, e, "b8pull", "1.0.0", content)
	if res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "b8pull", "version": "1.0.0",
	}); res.IsError {
		t.Fatalf("push failed: %s", resultText(t, res))
	}
	for i := 0; i < 3; i++ { // caps + write + read-back
		wantReq(t, reqs)
	}

	res := e.call("xuansi_driver_pull", map[string]any{"board": "d75884", "name": "b8pull"})
	if res.IsError {
		t.Fatalf("pull failed: %s", resultText(t, res))
	}
	rmReq := wantReq(t, reqs)
	if rmReq.Type != "fs_rm" || rmReq.Path != "/drv/b8pull.lua" || rmReq.Holder != "mcp-fixed01" {
		t.Errorf("fs_rm request = %+v, want the held removal at /drv/b8pull.lua", rmReq)
	}
	out := structured(t, res)
	if v, _ := out["pulled"].(bool); !v || out["path"] != "/drv/b8pull.lua" {
		t.Errorf("pull result = %v, want {pulled:true, path:/drv/b8pull.lua}", out)
	}

	// ws_list: the version + its checksum still listed, active ABSENT
	res = e.call("xuansi_ws_list", nil)
	if res.IsError {
		t.Fatalf("ws_list failed: %s", resultText(t, res))
	}
	var listed bool
	if drs, ok := structured(t, res)["drivers"].([]any); ok {
		for _, d := range drs {
			dm, _ := d.(map[string]any)
			if dm["name"] != "b8pull" {
				continue
			}
			if act, _ := dm["active"].(string); act != "" {
				t.Errorf("ws_list active = %q, want absent after the pull", act)
			}
			if vs, ok2 := dm["versions"].([]any); ok2 && len(vs) == 1 {
				if vm, _ := vs[0].(map[string]any); vm["version"] == "1.0.0" && vm["sha256"] == sha256hex([]byte(content)) {
					listed = true
				}
			}
		}
	}
	if !listed {
		t.Errorf("ws_list after pull does not offer b8pull@1.0.0 with its checksum (history must stay)")
	}

	// the manifest on disk: Active "", the version entry + footprint kept
	m := wsManifest(t, root)
	ent := m.Drivers["b8pull"]
	if ent.Active != "" {
		t.Errorf("Active = %q, want \"\" after the pull", ent.Active)
	}
	fm := ent.Files["1.0.0"]
	if fm.Sha256 != sha256hex([]byte(content)) || fm.Size != len(content) {
		t.Errorf("Files[1.0.0] = %+v, want the history kept verbatim", fm)
	}
	if len(fm.Boards) != 1 || fm.Boards["d75884"].IsZero() {
		t.Errorf("Boards = %v, want the d75884 push footprint KEPT (history never lies)", fm.Boards)
	}
}

// TestDriverPullBoardErrorVerbatimAndManifestUntouched: a pull whose
// fs_rm is refused (the board's api not-found on an already-absent
// file) crosses that error VERBATIM and rewrites nothing - Active
// stays exactly as it was.
func TestDriverPullBoardErrorVerbatimAndManifestUntouched(t *testing.T) {
	content := "--@api 1\n-- ghost v1.0.0\nreturn {}\n"
	want := "rm '/drv/ghost.lua' failed (No such file or directory)"
	e, reqs, root := newWsEnv(t, []step{
		capsStep(1),                          // push: caps
		{result: map[string]any{"ok": true}}, // push: fs_write
		{result: map[string]any{"data": base64.StdEncoding.EncodeToString([]byte(content))}}, // push: fs_read back
		{rerr: &server.RPCError{Class: "api", Msg: want}},                                    // pull: fs_rm refused
	})
	mustPut(t, e, "ghost", "1.0.0", content)
	if res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ghost", "version": "1.0.0",
	}); res.IsError {
		t.Fatalf("push failed: %s", resultText(t, res))
	}
	for i := 0; i < 3; i++ { // the push's caps + write + read-back
		wantReq(t, reqs)
	}

	res := e.call("xuansi_driver_pull", map[string]any{"board": "d75884", "name": "ghost"})
	if !res.IsError {
		t.Fatalf("pull of an absent file succeeded: %+v", res)
	}
	if got := resultText(t, res); got != "api: "+want {
		t.Errorf("error text = %q, want exactly %q", got, "api: "+want)
	}
	rmReq := wantReq(t, reqs)
	if rmReq.Type != "fs_rm" || rmReq.Path != "/drv/ghost.lua" {
		t.Errorf("fs_rm request = %+v, want the removal at /drv/ghost.lua", rmReq)
	}

	m := wsManifest(t, root)
	if act := m.Drivers["ghost"].Active; act != "1.0.0" {
		t.Errorf("Active after the refused pull = %q, want 1.0.0 untouched", act)
	}
	if fm := m.Drivers["ghost"].Files["1.0.0"]; fm.Boards["d75884"].IsZero() {
		t.Errorf("footprint disturbed by the refused pull: %+v", fm)
	}
}

// TestDriverPullNameValidation: names outside wsNameRe are local
// api-class refusals - nothing crosses, the manifest is not read.
func TestDriverPullNameValidation(t *testing.T) {
	e, reqs, _ := newWsEnv(t, nil)
	for _, name := range []string{"../evil", "a/b", "A!", "", "x y"} {
		res := e.call("xuansi_driver_pull", map[string]any{"board": "d75884", "name": name})
		if !res.IsError || !strings.HasPrefix(resultText(t, res), "api:") {
			t.Errorf("pull name %q = %v (%v), want a local api-class refusal", name, res, resultText(t, res))
		}
	}
	noContact(t, reqs)
}

// TestDriverPullUnknownNameNoManifestNoise: pulling a driver the
// workspace never recorded still removes the board file (that is the
// cleanup path's point) but writes NO empty manifest entry - history
// records pushes, not the absence of them.
func TestDriverPullUnknownNameNoManifestNoise(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	res := e.call("xuansi_driver_pull", map[string]any{"board": "d75884", "name": "leftover"})
	if res.IsError {
		t.Fatalf("pull of a never-stored name failed: %s", resultText(t, res))
	}
	if req := wantReq(t, reqs); req.Type != "fs_rm" || req.Path != "/drv/leftover.lua" {
		t.Errorf("fs_rm request = %+v, want the removal at /drv/leftover.lua", req)
	}
	if out := structured(t, res); out["pulled"] != true || out["path"] != "/drv/leftover.lua" {
		t.Errorf("pull result = %v, want {pulled:true, path:/drv/leftover.lua}", out)
	}
	m := wsManifest(t, root)
	if _, ok := m.Drivers["leftover"]; ok {
		t.Errorf("manifest grew a %q entry with no history: %+v", "leftover", m.Drivers["leftover"])
	}
}

// TestDriverPullConcurrentWithPush: a pull racing a re-push of the
// same driver serializes on wsMu - the manifest file is never torn
// (it reloads whole) and the final Active is EXACTLY one of the two
// outcomes: "" (pull's write won) or the pushed version (push's write
// won). Whichever wins, the stored version and its board footprint
// survive - history never lies.
func TestDriverPullConcurrentWithPush(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	content := "--@api 1\n-- race v1.0.0\nreturn {}\n"
	mustPut(t, e, "race", "1.0.0", content)
	if res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "race", "version": "1.0.0",
	}); res.IsError {
		t.Fatalf("seed push failed: %s", resultText(t, res))
	}
	for i := 0; i < 3; i++ {
		wantReq(t, reqs)
	}

	outPull := e.callAsync("xuansi_driver_pull", map[string]any{"board": "d75884", "name": "race"})
	outPush := e.callAsync("xuansi_driver_push", map[string]any{"board": "d75884", "name": "race", "version": "1.0.0"})
	for _, out := range []chan *mcpsdk.CallToolResult{outPull, outPush} {
		if res := waitResult(t, out, "concurrent pull/push"); res.IsError {
			t.Fatalf("concurrent pull/push failed: %s", resultText(t, res))
		}
	}
	for i := 0; i < 4; i++ { // pull: fs_rm; push: caps + write + read-back
		wantReq(t, reqs)
	}

	m, err := loadManifest(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatalf("manifest torn by the race (cannot reload): %v", err)
	}
	ent, ok := m.Drivers["race"]
	if !ok || len(ent.Files) != 1 {
		t.Fatalf("manifest after the race = %+v, want the one whole version entry", m.Drivers)
	}
	fm := ent.Files["1.0.0"]
	if fm.Sha256 != sha256hex([]byte(content)) || fm.Size != len(content) || fm.Boards["d75884"].IsZero() {
		t.Fatalf("history disturbed by the race: %+v", fm)
	}
	if ent.Active != "" && ent.Active != "1.0.0" {
		t.Fatalf("Active = %q, want exactly one of the two outcomes (\"\" or 1.0.0)", ent.Active)
	}
}
