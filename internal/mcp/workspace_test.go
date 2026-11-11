package mcp

// workspace_test.go — task 11's driver-workspace tests: the put/push
// pipeline against the fake daemon (wire shapes asserted on every
// crossing: the holder-less op caps, the held fs_write carrying the
// sanitized /drv/<name>.lua path AND its sha256 — the verified path
// whose board-side commit verdict is the push's verification, B15
// §4.5②; no read-back crossing anymore), the --@api gate (missing /
// mismatched with BOTH values in the refusal), the board's single-file
// cap enforced at PUSH time (the workspace itself keeps fat files),
// the commit-sha mismatch that leaves the manifest untouched on
// mismatch, footprint idempotency, concurrent pushes merged into a
// consistent manifest, name/version sanitization, and the
// version-defaults-to-Active semantic. The workspace root is
// injected via Options.WorkspaceRoot — never by mutating the
// environment.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
// api_version ver; fs_write records the payload per (board, path) and
// — when the request carries the verified path's sha256 key — acts the
// board's commit verdict: the sha of the bytes that actually STAGED
// must equal it, else an api refusal in the fw's own spelling and
// nothing lands (tamper flips the last staged bit first, simulating a
// corrupted/mid-transfer byte). Anything else on the wire fails the
// test.
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
				staged := append([]byte(nil), req.Data...)
				if tamper && len(staged) > 0 {
					staged[len(staged)-1] ^= 1 // the mid-transfer byte that goes wrong
				}
				if req.Sha256 != "" {
					if got := sha256hex(staged); got != req.Sha256 {
						rep = &server.Reply{Type: "reply", ID: req.ID,
							Error: &server.RPCError{Class: "api",
								Msg: fmt.Sprintf("sha256 mismatch (.part=%s, expected=%s)", got, req.Sha256)}}
						break // the commit verdict refuses; nothing lands
					}
				}
				files[req.Board+"\x00"+req.Path] = staged
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{"ok": true}}
			case "fs_read":
				data := append([]byte(nil), files[req.Board+"\x00"+req.Path]...)
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

// TestManifestLoadsKindlessOldManifest (fix round 1, minor a): a
// pre-B10 manifest JSON without the kind field loads unchanged and
// Kind reads "" — the additive field must never break old workspaces.
func TestManifestLoadsKindlessOldManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	old := `{"drivers":{"ads1115":{"active":"1.3.0","files":{"1.3.0":{"sha256":"abc","size":42}}}}}`
	if err := os.WriteFile(path, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(path)
	if err != nil {
		t.Fatalf("loadManifest(kindless) = %v, want a clean load", err)
	}
	e := m.Drivers["ads1115"]
	if e.Active != "1.3.0" || e.Kind != "" || e.Files["1.3.0"].Sha256 != "abc" {
		t.Errorf("kindless entry = %+v, want Active/Files back and Kind \"\"", e)
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

// TestDriverPutPushCommitShaHappyPath: put stores (sha256, size, Files
// entry, NOT Active); push runs the gated pipeline — holder-less op
// caps, HELD fs_write of the sanitized /drv/<name>.lua path carrying
// the decoded bytes AND their sha256 (the verified path — B15 §4.5②:
// the board's commit-sha verdict is the verification, the byte
// read-back is retired) — and only then the manifest records Active,
// PushedAt and the board footprint. The reply carries
// verified_by:"commit-sha256".
func TestDriverPutPushCommitShaHappyPath(t *testing.T) {
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
	// wire shapes, in pipeline order: caps, then the one verified write
	// (no fs_read — the read-back is retired, §4.5②)
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
	if writeReq.Sha256 != sha {
		t.Errorf("fs_write sha256 = %q, want the payload's %q (the commit verdict's expected digest)", writeReq.Sha256, sha)
	}
	noContact(t, reqs)

	out := structured(t, res)
	if out["board"] != "d75884" || out["path"] != "/drv/ads1115.lua" || out["sha256"] != sha {
		t.Errorf("push result = %v, want board/path/sha256", out)
	}
	if bytes, _ := out["bytes"].(float64); int(bytes) != len(content) {
		t.Errorf("push bytes = %v, want %d", out["bytes"], len(content))
	}
	if out["verified_by"] != "commit-sha256" {
		t.Errorf("verified_by = %v, want commit-sha256", out["verified_by"])
	}
	if _, has := out["kind"]; has {
		t.Errorf("kind = %v on a hand-put driver, want honest absence", out["kind"])
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
// keeps files of any size, and the PUSH crosses whole — the daemon-side
// chunked transport (fs.write_part × N + sha-verifying commit; 64KB
// chunks since B15 §4.5) carries payloads above the board's old 16KB
// single-op cap, so a fat .aot-era driver deploys in one push — an
// 111KB module crosses as ONE internal fs_write whose sha256 the
// board's commit verifies (the 2-chunk split is pinned server-side in
// internal/server/fs_chunked_test.go).
func TestWorkspaceFatFilePushesWholeOverChunkedTransport(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	content := drvBody("--@api 1", 113664) // 111KB
	if len(content) <= 65472 {
		t.Fatalf("test content is %dB, want it above the 65472B chunk", len(content))
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
	if writeReq.Sha256 != sha256hex([]byte(content)) {
		t.Errorf("fs_write sha256 = %s, want the whole payload's digest (verified in 2×64KB chunks board-side)", writeReq.Sha256)
	}
	noContact(t, reqs) // no read-back: the commit verdict verified it
	if _, err := os.Stat(filepath.Join(root, "drivers", "fatdrv@2.0.0.lua")); err != nil {
		t.Errorf("workspace dropped the fat file: %v", err)
	}
	if m := wsManifest(t, root); m.Drivers["fatdrv"].Active != "2.0.0" {
		t.Errorf("Active = %q after a whole-file push, want 2.0.0", m.Drivers["fatdrv"].Active)
	}
}

// TestDriverPushCommitShaMismatchLeavesManifestUntouched: when the
// bytes that actually stage on the board differ from the payload (the
// tamper fake corrupts one mid-transfer byte), the commit-sha verdict
// REFUSES the write — the fw's own api error crosses verbatim and the
// manifest records nothing. History only ever holds verified deploys.
func TestDriverPushCommitShaMismatchLeavesManifestUntouched(t *testing.T) {
	e, _, root := newWsPushEnv(t, 1, true) // tamper corrupts the staged bytes
	content := "--@api 1\n-- v1\nreturn {}\n"
	mustPut(t, e, "ads1115", "1.1.0", content)

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ads1115", "version": "1.1.0",
	})
	if !res.IsError {
		t.Fatalf("corrupted-staging push succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api:", "sha256 mismatch", "expected=" + sha256hex([]byte(content))} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q (the fw's commit verdict)", got, want)
		}
	}

	m := wsManifest(t, root)
	fm := m.Drivers["ads1115"].Files["1.1.0"]
	if m.Drivers["ads1115"].Active != "" || !fm.PushedAt.IsZero() || len(fm.Boards) != 0 {
		t.Errorf("manifest after commit-sha mismatch = %+v, want no activation/footprint", m.Drivers["ads1115"])
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
	for i := 0; i < 4; i++ { // caps+write per push, some interleaving
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
	// the pipeline crossing order: caps, write
	wantReq(t, reqs)
	writeReq := wantReq(t, reqs)
	noContact(t, reqs)
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
	for i := 0; i < 2; i++ {
		wantReq(t, reqs)
	}

	// default = Active (1.3.0), NOT the newest put (1.2.0). No caps
	// cache in this static-only env, so this push makes the same two
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
	noContact(t, reqs) // the verified write is the last crossing

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
	noContact(t, reqs) // the verified write is the last crossing

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

	// a version that is not there says so, naming both storing paths
	res = e.call("xuansi_ws_get", map[string]any{"name": "ads1115", "version": "9.9.9"})
	got := resultText(t, res)
	if !res.IsError || !strings.Contains(got, "ads1115@9.9.9") || !strings.Contains(got, "xuansi_ws_put") || !strings.Contains(got, "xuansi_driver_fetch") {
		t.Errorf("ws_get missing = %v (%v), want a refusal naming the version and both storing tools", res, got)
	}
}

// TestWsGetBinaryArtifactAndPriority: ws_get resolves the stored
// artifact with the SAME first-extension-wins priority a bare push
// deploys (the B10-T8 generalization): a .lua version returns
// verbatim text with encoding ABSENT (the pre-B10 face
// byte-identical), a binary .wasm artifact — storable only through
// xuansi_driver_fetch — returns standard base64 with
// encoding="base64" and sha256 of the DECODED bytes (a JSON string
// would mangle invalid UTF-8; fs_read's data_b64 is the precedent),
// and when both slots hold the same version the .lua serves.
func TestWsGetBinaryArtifactAndPriority(t *testing.T) {
	e, _, root := newWsEnv(t, nil)
	luaSrc := "--@api 1\n-- wg v1.0.0\nreturn {}\n"
	mustPut(t, e, "wg", "1.0.0", luaSrc)
	body := fetchWasmBody(48) // opens with the \0asm module magic, like a fetched artifact
	drvDir := filepath.Join(root, "drivers")
	if err := os.WriteFile(filepath.Join(drvDir, "wg@2.0.0.wasm"), body, 0o644); err != nil {
		t.Fatal(err)
	}

	// the .lua face: verbatim text, encoding absent, artifact named
	res := e.call("xuansi_ws_get", map[string]any{"name": "wg", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("ws_get lua failed: %s", resultText(t, res))
	}
	g := structured(t, res)
	if g["content"] != luaSrc || g["sha256"] != sha256hex([]byte(luaSrc)) {
		t.Errorf("ws_get lua = %v, want verbatim content + checksum", g)
	}
	if enc, _ := g["encoding"].(string); enc != "" {
		t.Errorf("ws_get lua encoding = %q, want absent", enc)
	}
	if g["artifact"] != "drivers/wg@1.0.0.lua" {
		t.Errorf("ws_get lua artifact = %v, want drivers/wg@1.0.0.lua", g["artifact"])
	}

	// the binary face: base64 content, decoded sha, artifact named
	res = e.call("xuansi_ws_get", map[string]any{"name": "wg", "version": "2.0.0"})
	if res.IsError {
		t.Fatalf("ws_get wasm failed: %s", resultText(t, res))
	}
	g = structured(t, res)
	if g["encoding"] != "base64" || g["artifact"] != "drivers/wg@2.0.0.wasm" {
		t.Errorf("ws_get wasm envelope = %v, want encoding=base64 + the wasm artifact", g)
	}
	dec, derr := base64.StdEncoding.DecodeString(fmt.Sprint(g["content"]))
	if derr != nil || !bytes.Equal(dec, body) {
		t.Errorf("ws_get wasm content does not decode to the stored bytes (err=%v, got %d bytes, want %d)", derr, len(dec), len(body))
	}
	if g["sha256"] != sha256hex(body) {
		t.Errorf("ws_get wasm sha256 = %v, want the DECODED bytes' checksum", g["sha256"])
	}

	// priority: both slots at one version — the .lua serves, like a bare push deploys
	mustPut(t, e, "wg", "3.0.0", luaSrc)
	if err := os.WriteFile(filepath.Join(drvDir, "wg@3.0.0.wasm"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	res = e.call("xuansi_ws_get", map[string]any{"name": "wg", "version": "3.0.0"})
	if res.IsError {
		t.Fatalf("ws_get both-slots failed: %s", resultText(t, res))
	}
	g = structured(t, res)
	if g["content"] != luaSrc || g["artifact"] != "drivers/wg@3.0.0.lua" {
		t.Errorf("ws_get both-slots = %v, want the .lua slot serving (pushExts priority)", g)
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
	target string // the aot target key riding files[] (P3-T12's index face)
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
		Target string `json:"target,omitempty"`
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
			me.Files = append(me.Files, fileEnt{Path: f.path, Sha256: f.sha256, Bytes: f.bytes, Target: f.target})
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
	aotSpec := fetchSpec(t, "modules/fakemod/1.0.0/fakemod.aot", abody, "")
	aotSpec.target = "esp32s3" // P3-T12's index face: files[] carries the aot target
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "fakemod", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{
			fetchSpec(t, "modules/fakemod/1.0.0/fakemod.wasm", wbody, ""),
			aotSpec,
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
	// P3-T13: the .aot's index target lands as the version's aot record
	// — the push pairing's claim source
	if slot, ok := fm.Aot["esp32s3"]; !ok || slot.Sha256 != sha256hex(abody) || slot.Size != len(abody) {
		t.Errorf("Files[1.0.0].Aot[esp32s3] = %+v (ok=%v), want the .aot's sha/size", slot, ok)
	}
	if ent.Active != "" {
		t.Errorf("Active = %q after a fetch, want none (a push activates)", ent.Active)
	}

	// ws_list surfaces the manifest's Kind — the seat sees the index's
	// module kind without reading the manifest file (B15 W1.T4's kind
	// passthrough face)
	lres := e.call("xuansi_ws_list", map[string]any{})
	if lres.IsError {
		t.Fatalf("ws_list failed: %s", resultText(t, lres))
	}
	drivers, _ := structured(t, lres)["drivers"].([]any)
	if len(drivers) != 1 {
		t.Fatalf("ws_list drivers = %v, want the one", drivers)
	}
	if d0, _ := drivers[0].(map[string]any); d0["kind"] != "driver" {
		t.Errorf("ws_list kind = %v, want the index's kind surfaced", d0["kind"])
	}
}

// TestKindPassthroughTolerantAndCanary (B15 W1.T4): the drivers index's
// kind field crosses VERBATIM wherever the index carries it — today's
// module-kind vocabulary (driver/algorithm) and the canary vocabulary
// the drivers-repo wave adds (production/test-canary) alike — and an
// index WITHOUT the key fetches cleanly with the kind honestly absent
// everywhere (fetch reply, manifest, ws_list, driver_push reply): the
// passthrough must tolerate absence, never invent a value.
func TestKindPassthroughTolerantAndCanary(t *testing.T) {
	wbody := fetchWasmBody(48)

	// a kindless index (the pre-canary index shape, and any index_ver 1
	// entry that simply omits the key)
	mux := http.NewServeMux()
	idxJ := fmt.Sprintf(`{"index_ver":1,"modules":[{"name":"quietmod","version":"1.0.0","abi":1,`+
		`"files":[{"path":"modules/quietmod/1.0.0/quietmod.wasm","sha256":%q,"bytes":%d}]}]}`,
		sha256hex(wbody), len(wbody))
	mux.HandleFunc("/index.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(idxJ))
	})
	mux.HandleFunc("/modules/quietmod/1.0.0/quietmod.wasm", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(wbody)
	})
	kindless := httptest.NewServer(mux)
	t.Cleanup(kindless.Close)

	e, _, root := newWsEnv(t, nil)
	res := e.call("xuansi_driver_fetch", map[string]any{
		"name": "quietmod", "version": "1.0.0", "index_url": kindless.URL + "/index.json",
	})
	if res.IsError {
		t.Fatalf("kindless fetch failed: %s", resultText(t, res))
	}
	if _, has := structured(t, res)["kind"]; has {
		t.Errorf("kindless fetch reply carries kind = %v, want honest absence", structured(t, res)["kind"])
	}
	if m := wsManifest(t, root); m.Drivers["quietmod"].Kind != "" {
		t.Errorf("kindless manifest Kind = %q, want empty", m.Drivers["quietmod"].Kind)
	}
	lres := e.call("xuansi_ws_list", map[string]any{})
	drivers, _ := structured(t, lres)["drivers"].([]any)
	if len(drivers) != 1 {
		t.Fatalf("ws_list drivers = %v, want the one", drivers)
	}
	d0, _ := drivers[0].(map[string]any)
	if d0["name"] != "quietmod" {
		t.Fatalf("ws_list driver = %v, want quietmod", d0)
	}
	if _, has := d0["kind"]; has {
		t.Errorf("ws_list kind = %v on a kindless fetch, want absent", d0["kind"])
	}

	// a CANARY-kind index (the drivers-repo wave's vocabulary): the
	// value crosses verbatim — fetch reply, manifest, ws_list, and the
	// driver_push reply
	cbody := fetchWasmBody(56)
	curl, _ := newFetchIndex(t, []fetchModSpec{{
		name: "hello", kind: "test-canary", version: "1.0.0",
		files: []fetchFileSpec{fetchSpec(t, "modules/hello/1.0.0/hello.wasm", cbody, "")},
	}})
	e2, reqs, root2 := newWsPushEnv(t, 1, false)
	res = e2.call("xuansi_driver_fetch", map[string]any{
		"name": "hello", "version": "1.0.0", "index_url": curl,
	})
	if res.IsError {
		t.Fatalf("canary fetch failed: %s", resultText(t, res))
	}
	if k := structured(t, res)["kind"]; k != "test-canary" {
		t.Errorf("canary fetch kind = %v, want test-canary verbatim", k)
	}
	if m := wsManifest(t, root2); m.Drivers["hello"].Kind != "test-canary" {
		t.Errorf("canary manifest Kind = %q, want test-canary", m.Drivers["hello"].Kind)
	}
	lres = e2.call("xuansi_ws_list", map[string]any{})
	drivers, _ = structured(t, lres)["drivers"].([]any)
	var sawCanary bool
	for _, d := range drivers {
		dm, _ := d.(map[string]any)
		if dm["name"] == "hello" {
			sawCanary = dm["kind"] == "test-canary"
		}
	}
	if !sawCanary {
		t.Errorf("ws_list never surfaced hello's test-canary kind: %v", drivers)
	}

	// the push reply carries the kind too (the deploy seat sees what it
	// is deploying)
	pres := e2.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "hello", "version": "1.0.0",
	})
	if pres.IsError {
		t.Fatalf("canary push failed: %s", resultText(t, pres))
	}
	if k := structured(t, pres)["kind"]; k != "test-canary" {
		t.Errorf("push reply kind = %v, want test-canary (the manifest's)", k)
	}
	if req := wantReq(t, reqs); req.Type != "fs_write" || req.Sha256 != sha256hex(cbody) {
		t.Errorf("push fs_write = %+v, want the verified write carrying the artifact's sha", req)
	}
	noContact(t, reqs)
}

// TestDriverFetchAotTargetHandling (P3-T13): the .aot target field's
// edge cases — a target-less .aot stores UNPAIRED (no record, no
// claim: a bare push deploys the resolved single), and a hostile
// target string is refused before anything lands (a target is a
// manifest key, but the workspace still refuses keys outside the
// namespace's [a-z0-9-] vocabulary).
func TestDriverFetchAotTargetHandling(t *testing.T) {
	// leg 1: target-less .aot — honest non-record
	e, _, root := newWsEnv(t, nil)
	abody := fetchAotBody(48)
	url, hits := newFetchIndex(t, []fetchModSpec{{
		name: "notarget", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{fetchSpec(t, "modules/notarget/1.0.0/notarget.aot", abody, "")},
	}})
	res := e.call("xuansi_driver_fetch", map[string]any{
		"name": "notarget", "version": "1.0.0", "index_url": url,
	})
	if res.IsError {
		t.Fatalf("target-less fetch failed: %s", resultText(t, res))
	}
	if fm := wsManifest(t, root).Drivers["notarget"].Files["1.0.0"]; fm.Aot != nil {
		t.Errorf("Files[1.0.0].Aot = %+v, want nil (no target, no pairing claim)", fm.Aot)
	}

	// leg 2: hostile target — refused, zero writes
	e2, _, root2 := newWsEnv(t, nil)
	bad := fetchSpec(t, "modules/bad/1.0.0/bad.aot", abody, "")
	bad.target = "ESP32-S3/../../x"
	url2, hits2 := newFetchIndex(t, []fetchModSpec{{
		name: "bad", kind: "driver", version: "1.0.0",
		files: []fetchFileSpec{bad},
	}})
	res2 := e2.call("xuansi_driver_fetch", map[string]any{
		"name": "bad", "version": "1.0.0", "index_url": url2,
	})
	if !res2.IsError {
		t.Fatalf("hostile target accepted: %+v", res2)
	}
	if got := resultText(t, res2); !strings.Contains(got, "target") {
		t.Errorf("refusal = %q, want the target gate named", got)
	}
	if _, err := os.Stat(filepath.Join(root2, "drivers")); !os.IsNotExist(err) {
		t.Errorf("drivers/ exists after the refusal, want zero writes")
	}
	_ = hits
	_ = hits2
}

// ---------------------------------------------------------------------------
// P3-T13: F-5 paired push — the chip source, the pair legs, rollback,
// and the explicit single-artifact sibling sweep

// pairD is servePushDaemon's T13 twin: the same fs store plus per-path
// failure injection (a refused fs_write, a commit-sha mismatch — the
// corruptSha leg refuses the write in the fw's own spelling and lands
// nothing, exactly the verdict a corrupted/mid-transfer byte draws —
// and a REAL fs_rm refusal) and the file table exposed for final-state
// assertions. fs_rm of an ABSENT file refuses with the fw's own
// "'path' not found" vocabulary (the sweep tolerance's contract).
type pairD struct {
	fd         *fakeDaemon
	reqs       chan server.Request
	files      map[string][]byte // board\x00path → payload
	failWrite  map[string]string // path → api error msg on fs_write
	corruptSha map[string]bool   // path → refuse the write as a sha mismatch, land nothing
	failRm     map[string]string // path → api error msg on fs_rm (a REAL refusal)
}

func newPairEnv(t *testing.T) (*env, *pairD, string) {
	t.Helper()
	pd := &pairD{
		reqs:       make(chan server.Request, 32),
		files:      map[string][]byte{},
		failWrite:  map[string]string{},
		corruptSha: map[string]bool{},
		failRm:     map[string]string{},
	}
	pd.fd = newFakeDaemon(t)
	pd.fd.serve(t, func(conn net.Conn) {
		for {
			var req server.Request
			if err := server.ReadMsg(conn, &req); err != nil {
				return // client close: expected shutdown path
			}
			select {
			case pd.reqs <- req:
			default:
				t.Errorf("pair daemon: unexpected extra request %+v", req)
			}
			var rep *server.Reply
			switch req.Type {
			case "fs_write":
				if msg, ok := pd.failWrite[req.Path]; ok {
					rep = &server.Reply{Type: "reply", ID: req.ID,
						Error: &server.RPCError{Class: "api", Msg: msg}}
					break
				}
				if pd.corruptSha[req.Path] && req.Sha256 != "" {
					// the board's commit verdict on corrupted staging: the
					// fw's own spelling, nothing landed (only its .part,
					// kept for a from-off=0 retry)
					staged := append([]byte(nil), req.Data...)
					staged[len(staged)-1] ^= 1
					rep = &server.Reply{Type: "reply", ID: req.ID,
						Error: &server.RPCError{Class: "api",
							Msg: fmt.Sprintf("sha256 mismatch (.part=%s, expected=%s)", sha256hex(staged), req.Sha256)}}
					break
				}
				pd.files[req.Board+"\x00"+req.Path] = req.Data
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{"ok": true}}
			case "fs_rm":
				if msg, ok := pd.failRm[req.Path]; ok {
					rep = &server.Reply{Type: "reply", ID: req.ID,
						Error: &server.RPCError{Class: "api", Msg: msg}}
					break
				}
				if _, ok := pd.files[req.Board+"\x00"+req.Path]; !ok {
					// the fw's REAL remove-path vocabulary (xs_fs_remove's
					// unlink branch, POSIX strerror — pinned by the P3-T15
					// seat check; the read path's "'%s' not found (...)"
					// spelling is a different branch and never rides fs_rm)
					rep = &server.Reply{Type: "reply", ID: req.ID,
						Error: &server.RPCError{Class: "api", Msg: fmt.Sprintf("rm '%s' failed (No such file or directory)", req.Path)}}
					break
				}
				delete(pd.files, req.Board+"\x00"+req.Path)
				rep = &server.Reply{Type: "reply", ID: req.ID, Result: map[string]any{}}
			default:
				t.Errorf("pair daemon: unexpected request type %s", req.Type)
				rep = &server.Reply{Type: "reply", ID: req.ID,
					Error: &server.RPCError{Class: "server", Msg: "unexpected " + req.Type}}
			}
			if err := server.WriteMsg(conn, rep); err != nil {
				t.Errorf("pair daemon reply: %v", err)
				return
			}
		}
	})
	root := t.TempDir()
	e := wireMCP(t, pd.fd, Options{Holder: "mcp-fixed01", WorkspaceRoot: root})
	return e, pd, root
}

// seedChipBoard plants the event face's board list with one board whose
// hello chip is chip — the pairing decision's architecture source (the
// daemon-cached hello chip; no fresh crossing).
func seedChipBoard(t *testing.T, e *env, mac6, chip string) {
	t.Helper()
	es := e.ts.ev
	if es == nil {
		t.Fatal("event face not wired")
	}
	es.mu.Lock()
	es.boards = []dynBoard{{mac6: mac6, chip: chip}}
	es.mu.Unlock()
}

// storeArtifact plants one artifact file under drivers/ (no manifest
// touch — the pairing record is the caller's business).
func storeArtifact(t *testing.T, root, name, ver, ext string, body []byte) string {
	t.Helper()
	p := filepath.Join(root, "drivers", name+"@"+ver+"."+ext)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// storePairVersion plants the T12 dual-product shape: .wasm + .aot on
// disk plus the manifest's aot.<target> record (what a dual-product
// fetch or a paired build lands). Returns the two bodies.
func storePairVersion(t *testing.T, root, name, ver, target string) (wasm, aot []byte) {
	t.Helper()
	wasm, aot = fetchWasmBody(64), fetchAotBody(48)
	storeArtifact(t, root, name, ver, "wasm", wasm)
	storeArtifact(t, root, name, ver, "aot", aot)
	m, err := loadManifest(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	e := m.entry(name)
	fm := e.Files[ver]
	fm.Sha256, fm.Size = sha256hex(wasm), len(wasm)
	fm.Aot = map[string]AotSlot{target: {Sha256: sha256hex(aot), Size: len(aot)}}
	e.Files[ver] = fm
	m.Drivers[name] = e
	if err := writeManifestAtomic(filepath.Join(root, "manifest.json"), m); err != nil {
		t.Fatalf("writeManifestAtomic: %v", err)
	}
	return wasm, aot
}

// TestChipTargetMapping (ruling 1): the fw hello chip string maps to
// the aot target namespace's key — ESP32-S3 → esp32s3, everything else
// (unknown chip, empty) maps nothing: the conservative .wasm single.
func TestChipTargetMapping(t *testing.T) {
	if tgt, ok := boardTarget("ESP32-S3"); !ok || tgt != "esp32s3" {
		t.Errorf(`boardTarget("ESP32-S3") = %q, %v; want "esp32s3", true`, tgt, ok)
	}
	for _, chip := range []string{"ESP32-C6", "ESP32-S2", "esp32-s3", "", "ESP32-S3 "} {
		if _, ok := boardTarget(chip); ok {
			t.Errorf("boardTarget(%q) = ok, want unmapped (the conservative single)", chip)
		}
	}
}

// TestDriverPushPairedWasmAotHappyPath: a bare push whose version
// carries aot.<target> AND whose board's chip maps to that target
// deploys the PAIR — the .aot leg FIRST (ruling 2), then the .wasm —
// both commit-verified (each fs_write carries its payload's sha256;
// the board's verdict is the verification), no sweep, Active moves,
// and the reply claims the pair honestly (paired/aot_path/aot_sha256).
func TestDriverPushPairedWasmAotHappyPath(t *testing.T) {
	e, pd, root := newPairEnv(t)
	seedChipBoard(t, e, "d75884", "ESP32-S3")
	wasm, aot := storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("paired push failed: %s", resultText(t, res))
	}
	// wire order: the .aot leg first, then .wasm — one verified write
	// each, no read-backs (the commit verdict verified both)
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.aot"},
		{"fs_write", "/drv/pairmod.wasm"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs) // a paired push sweeps NOTHING — both slots are fresh
	if !bytes.Equal(pd.files["d75884\x00/drv/pairmod.aot"], aot) {
		t.Errorf("board .aot slot = %d bytes, want the artifact verbatim", len(pd.files["d75884\x00/drv/pairmod.aot"]))
	}

	out := structured(t, res)
	if out["paired"] != true {
		t.Errorf("paired = %#v, want true (the pair landed — never a false claim)", out["paired"])
	}
	if out["path"] != "/drv/pairmod.wasm" || out["sha256"] != sha256hex(wasm) {
		t.Errorf("out path/sha = %v, want the .wasm deliverable leg's", out)
	}
	if out["aot_path"] != "/drv/pairmod.aot" || out["aot_sha256"] != sha256hex(aot) {
		t.Errorf("out aot leg = %v, want the .aot path + verified sha", out)
	}
	if out["verified_by"] != "commit-sha256" {
		t.Errorf("verified_by = %v, want commit-sha256 (both legs)", out["verified_by"])
	}
	if _, swept := out["swept"]; swept {
		t.Errorf("out swept = %v, want absent (no sweep on the paired push)", out["swept"])
	}

	// manifest: Active moves, the board footprint lands, and the
	// version's own aot record SURVIVES the push cycle.
	m := wsManifest(t, root)
	ent := m.Drivers["pairmod"]
	if ent.Active != "1.0.0" {
		t.Errorf("Active = %q, want 1.0.0", ent.Active)
	}
	fm := ent.Files["1.0.0"]
	if slot, ok := fm.Aot["esp32s3"]; !ok || slot.Sha256 != sha256hex(aot) {
		t.Errorf("Files[1.0.0].Aot[esp32s3] = %+v (ok=%v), want the record intact", slot, ok)
	}
	if _, ok := fm.Boards["d75884"]; !ok {
		t.Errorf("Files[1.0.0].Boards = %v, want the d75884 footprint", fm.Boards)
	}

	// ws_list surfaces the aot record — the seat can SEE pairing coming
	lres := e.call("xuansi_ws_list", map[string]any{})
	if lres.IsError {
		t.Fatalf("ws_list failed: %s", resultText(t, lres))
	}
	lm := structured(t, lres)
	drivers, _ := lm["drivers"].([]any)
	if len(drivers) != 1 {
		t.Fatalf("ws_list drivers = %v, want the one", lm["drivers"])
	}
	d0, _ := drivers[0].(map[string]any)
	vers, _ := d0["versions"].([]any)
	if len(vers) != 1 {
		t.Fatalf("ws_list versions = %v, want the one", d0["versions"])
	}
	v0, _ := vers[0].(map[string]any)
	aotm, _ := v0["aot"].(map[string]any)
	if aotm == nil || aotm["esp32s3"] == nil {
		t.Errorf("ws_list versions[0].aot = %v, want the esp32s3 record visible", v0["aot"])
	}
}

// TestDriverPushPairedWasmLegFailRollsBackAot: the pair's .wasm leg
// failing rolls the WHOLE pair back out — the .aot always (it landed
// verified moments ago); the .wasm's own slot needs no rollback rm:
// since the commit-sha verdict (B15 §4.5②) an unverified leg lands
// NOTHING at the logical path (the fw keeps only the .part staging
// file, which never shadows the real path and is truncated by the next
// push's off=0 reopen). A rollback rm that itself fails errors naming
// the exact residual state. The manifest is untouched on every leg.
func TestDriverPushPairedWasmLegFailRollsBackAot(t *testing.T) {
	// leg 1: the .wasm staging corrupts mid-transfer — the commit-sha
	// verdict refuses it, the .aot rolls back, the board's slot table
	// ends empty of the pair
	e, pd, root := newPairEnv(t)
	seedChipBoard(t, e, "d75884", "ESP32-S3")
	storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")
	pd.corruptSha["/drv/pairmod.wasm"] = true

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if !res.IsError {
		t.Fatalf("paired push with a failing .wasm leg succeeded: %+v", res)
	}
	// wire order: .aot write, .wasm write (verdict-refused), then the
	// ROLLBACK fs_rm of the landed .aot half
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.aot"},
		{"fs_write", "/drv/pairmod.wasm"},
		{"fs_rm", "/drv/pairmod.aot"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs)
	for _, slot := range []string{"/drv/pairmod.aot", "/drv/pairmod.wasm"} {
		if _, still := pd.files["d75884\x00"+slot]; still {
			t.Errorf("board still holds %s after the rollback — neither half may survive", slot)
		}
	}

	got := resultText(t, res)
	for _, want := range []string{"sha256 mismatch", "rolled back", "/drv/pairmod.aot", "neither half", "manifest untouched"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	m := wsManifest(t, root)
	ent := m.Drivers["pairmod"]
	if ent.Active != "" {
		t.Errorf("Active = %q after a rolled-back pair, want none", ent.Active)
	}
	if fm := ent.Files["1.0.0"]; len(fm.Boards) != 0 || !fm.PushedAt.IsZero() {
		t.Errorf("Files[1.0.0] = %+v, want zero push footprints (pre-push manifest state)", fm)
	}

	// leg 2: the board REFUSES the .wasm fs_write outright — same
	// rollback (the .aot only), same honest error
	e2, pd2, root2 := newPairEnv(t)
	seedChipBoard(t, e2, "d75884", "ESP32-S3")
	storePairVersion(t, root2, "pairmod", "1.0.0", "esp32s3")
	pd2.failWrite["/drv/pairmod.wasm"] = "flash write refused: no space left on device"
	res2 := e2.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if !res2.IsError {
		t.Fatalf("paired push with a refused .wasm write succeeded: %+v", res2)
	}
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.aot"},
		{"fs_write", "/drv/pairmod.wasm"},
		{"fs_rm", "/drv/pairmod.aot"},
	} {
		req := wantReq(t, pd2.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("leg2 crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd2.reqs)
	if got := resultText(t, res2); !strings.Contains(got, "neither half") {
		t.Errorf("leg2 error text = %q, want the neither-half claim (true here: the write never landed)", got)
	}

	// leg 3: the .aot rollback rm itself fails — the error names the
	// VERIFIED residual half honestly instead of claiming neither
	e3, pd3, root3 := newPairEnv(t)
	seedChipBoard(t, e3, "d75884", "ESP32-S3")
	storePairVersion(t, root3, "pairmod", "1.0.0", "esp32s3")
	pd3.failWrite["/drv/pairmod.wasm"] = "flash write refused: no space left on device"
	pd3.failRm["/drv/pairmod.aot"] = "lease vanished mid-rollback"
	res3 := e3.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if !res3.IsError {
		t.Fatalf("paired push with a failing .aot rollback succeeded: %+v", res3)
	}
	got3 := resultText(t, res3)
	for _, want := range []string{"could not clear", "verified .aot at /drv/pairmod.aot", "lease vanished mid-rollback", "xuansi_driver_pull", "manifest untouched"} {
		if !strings.Contains(got3, want) {
			t.Errorf("leg3 error text = %q, want it to contain %q", got3, want)
		}
	}
	if _, still := pd3.files["d75884\x00/drv/pairmod.aot"]; !still {
		t.Errorf("leg3 board lost the .aot the daemon refused to rm — the fixture disagrees with the script")
	}
	if m3 := wsManifest(t, root3); m3.Drivers["pairmod"].Active != "" {
		t.Errorf("leg3 Active = %q after the failed rollback, want none (manifest untouched)", m3.Drivers["pairmod"].Active)
	}
}

// TestDriverPushPairedAotLegFailNoResidue: the .aot leg failing leaves
// NO residue — a refused fs_write changed nothing (daemon contract),
// and since the commit-sha verdict a leg whose staging corrupted never
// lands either (the verdict refuses; only the fw's own .part remains,
// which never shadows the real path). Either way the .wasm leg never
// crosses and the manifest stays untouched.
func TestDriverPushPairedAotLegFailNoResidue(t *testing.T) {
	e, pd, root := newPairEnv(t)
	seedChipBoard(t, e, "d75884", "ESP32-S3")

	// leg 1: the board refuses the .aot fs_write
	storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")
	pd.failWrite["/drv/pairmod.aot"] = "flash write refused: no space left on device"
	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if !res.IsError {
		t.Fatalf("paired push with a refused .aot write succeeded: %+v", res)
	}
	if got := resultText(t, res); !strings.Contains(got, "no space left on device") {
		t.Errorf("error text = %q, want the board's refusal verbatim", got)
	}
	req := wantReq(t, pd.reqs)
	if req.Type != "fs_write" || req.Path != "/drv/pairmod.aot" {
		t.Errorf("crossing = %s %s, want the .aot fs_write", req.Type, req.Path)
	}
	noContact(t, pd.reqs) // no .wasm leg, no rollback rm (nothing of ours landed)
	if _, ok := pd.files["d75884\x00/drv/pairmod.aot"]; ok {
		t.Errorf("board holds an .aot after a refused .aot write")
	}

	// leg 2: the .aot staging corrupts mid-transfer — the commit-sha
	// verdict refuses, nothing lands, no cleanup rm needed ("aot 失敗→無殘留")
	e2, pd2, root2 := newPairEnv(t)
	seedChipBoard(t, e2, "d75884", "ESP32-S3")
	storePairVersion(t, root2, "pairmod", "1.0.0", "esp32s3")
	pd2.corruptSha["/drv/pairmod.aot"] = true
	res2 := e2.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if !res2.IsError {
		t.Fatalf("paired push with a corrupt .aot staging succeeded: %+v", res2)
	}
	if got := resultText(t, res2); !strings.Contains(got, "sha256 mismatch") {
		t.Errorf("error text = %q, want the commit verdict crossing verbatim", got)
	}
	req = wantReq(t, pd2.reqs)
	if req.Type != "fs_write" || req.Path != "/drv/pairmod.aot" {
		t.Errorf("crossing = %s %s, want the .aot fs_write (the only one)", req.Type, req.Path)
	}
	noContact(t, pd2.reqs) // no .wasm leg, no rm — the verdict left nothing to clean
	if _, ok := pd2.files["d75884\x00/drv/pairmod.aot"]; ok {
		t.Errorf("board holds the corrupt .aot — the verdict must have refused the landing")
	}
	if m := wsManifest(t, root2); m.Drivers["pairmod"].Active != "" {
		t.Errorf("Active = %q after the failed .aot leg, want none", m.Drivers["pairmod"].Active)
	}
}

// TestDriverPushPairUnknownChipConservativeSingle: a chip the target
// map does not carry (ESP32-C6 today) pairs NOTHING — the bare push is
// the current .wasm single, the reply says paired=false and carries no
// aot leg (never a false pairing claim).
func TestDriverPushPairUnknownChipConservativeSingle(t *testing.T) {
	e, pd, root := newPairEnv(t)
	seedChipBoard(t, e, "d75884", "ESP32-C6")
	wasm, _ := storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("push failed: %s", resultText(t, res))
	}
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.wasm"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s (the .wasm single only)", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs)
	out := structured(t, res)
	if out["paired"] != false {
		t.Errorf("paired = %#v, want false (unknown chip: no pairing claimed)", out["paired"])
	}
	if _, ok := out["aot_path"]; ok {
		t.Errorf("out carries aot_path = %v on an unpaired push", out["aot_path"])
	}
	if out["sha256"] != sha256hex(wasm) {
		t.Errorf("sha256 = %v, want the .wasm's", out["sha256"])
	}
}

// TestDriverPushPairNoAotRecordSingle: a manifest WITHOUT the version's
// aot.<target> record pairs nothing even with both files on disk — the
// record is the claim, the file alone is not (pre-T13 workspaces and
// target-less fetch indexes keep the current single-artifact behavior).
func TestDriverPushPairNoAotRecordSingle(t *testing.T) {
	e, pd, root := newPairEnv(t)
	seedChipBoard(t, e, "d75884", "ESP32-S3")
	storeArtifact(t, root, "pairmod", "1.0.0", "wasm", fetchWasmBody(64))
	storeArtifact(t, root, "pairmod", "1.0.0", "aot", fetchAotBody(48))

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("push failed: %s", resultText(t, res))
	}
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.wasm"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs)
	if out := structured(t, res); out["paired"] != false {
		t.Errorf("paired = %#v, want false (no manifest record, no pair)", out["paired"])
	}
}

// TestDriverPushPairAotShaDriftRefused: a manifest aot record whose
// sha disagrees with the artifact on disk is a workspace inconsistency
// — refusing beats pushing an unverified pair (the corrupt-manifest
// discipline). Zero crossings.
func TestDriverPushPairAotShaDriftRefused(t *testing.T) {
	e, pd, root := newPairEnv(t)
	seedChipBoard(t, e, "d75884", "ESP32-S3")
	wasm, _ := storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")
	// the .aot file replaced behind the manifest's back
	storeArtifact(t, root, "pairmod", "1.0.0", "aot", fetchAotBody(96))
	_ = wasm

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if !res.IsError {
		t.Fatalf("push with a drifted .aot record succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"aot.esp32s3", "disagree", "driver_fetch"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	noContact(t, pd.reqs)
	if m := wsManifest(t, root); m.Drivers["pairmod"].Active != "" {
		t.Errorf("Active = %q after the refusal, want none", m.Drivers["pairmod"].Active)
	}
}

// TestDriverPushPairAotFileAbsentDegradesSingle: a record whose file is
// gone simply cannot pair — the bare push degrades to the .wasm single
// (honest, not an error: ws_get/list would not see the file either).
func TestDriverPushPairAotFileAbsentDegradesSingle(t *testing.T) {
	e, pd, root := newPairEnv(t)
	seedChipBoard(t, e, "d75884", "ESP32-S3")
	storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")
	if err := os.Remove(filepath.Join(root, "drivers", "pairmod@1.0.0.aot")); err != nil {
		t.Fatal(err)
	}

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("push failed: %s", resultText(t, res))
	}
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.wasm"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs)
	if out := structured(t, res); out["paired"] != false {
		t.Errorf("paired = %#v, want false (no artifact, no pair)", out["paired"])
	}
}

// TestDriverPushPairUnresolvedBoardSingle: a board the local list does
// not carry has no chip to map — the conservative .wasm single (ruling
// 3: no new assumptions, no fresh crossing to fetch one).
func TestDriverPushPairUnresolvedBoardSingle(t *testing.T) {
	e, pd, root := newPairEnv(t)
	// no seedChipBoard: the event face's list is empty
	storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("push failed: %s", resultText(t, res))
	}
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.wasm"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs)
	if out := structured(t, res); out["paired"] != false {
		t.Errorf("paired = %#v, want false (no chip source, no pair)", out["paired"])
	}
}

// TestDriverPushExplicitWasmSweepsSiblings: artifact "wasm" pushes
// exactly the .wasm and EMPTIES every other slot a push could have
// written (.lua, .aot — the F-5 sweep, pull's mirror) — a stale .aot
// can never shadow the fresh .wasm. The reply lists what it swept.
func TestDriverPushExplicitWasmSweepsSiblings(t *testing.T) {
	e, pd, root := newPairEnv(t)
	wasm, _ := storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")
	// the board already holds a STALE .aot and a stray .lua
	stale := fetchAotBody(128)
	pd.files["d75884\x00/drv/pairmod.aot"] = stale
	pd.files["d75884\x00/drv/pairmod.lua"] = []byte("--@api 1\nold\n")

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0", "artifact": "wasm",
	})
	if res.IsError {
		t.Fatalf("explicit wasm push failed: %s", resultText(t, res))
	}
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.wasm"},
		{"fs_rm", "/drv/pairmod.lua"},
		{"fs_rm", "/drv/pairmod.aot"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs)
	if _, ok := pd.files["d75884\x00/drv/pairmod.aot"]; ok {
		t.Errorf("the stale .aot survived the sweep — the F-5 foot-gun stays armed")
	}
	if _, ok := pd.files["d75884\x00/drv/pairmod.lua"]; ok {
		t.Errorf("the stray .lua survived the sweep")
	}
	if got := pd.files["d75884\x00/drv/pairmod.wasm"]; !bytes.Equal(got, wasm) {
		t.Errorf("board .wasm = %d bytes, want the fresh artifact verbatim", len(got))
	}

	out := structured(t, res)
	if out["paired"] != false {
		t.Errorf("paired = %#v, want false (an explicit single is never a pair claim)", out["paired"])
	}
	swept, _ := out["swept"].([]any)
	if len(swept) != 2 || swept[0] != "/drv/pairmod.lua" || swept[1] != "/drv/pairmod.aot" {
		t.Errorf("swept = %v, want both sibling paths in pushExts order", out["swept"])
	}
	if m := wsManifest(t, root); m.Drivers["pairmod"].Active != "1.0.0" {
		t.Errorf("Active = %q, want 1.0.0 (the sweep completed)", m.Drivers["pairmod"].Active)
	}
}

// TestDriverPushExplicitAotSweepsWasmSibling: artifact "aot" mirrors —
// the .aot lands alone and the .wasm/.lua slots empty.
func TestDriverPushExplicitAotSweepsWasmSibling(t *testing.T) {
	e, pd, root := newPairEnv(t)
	_, aot := storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")
	pd.files["d75884\x00/drv/pairmod.wasm"] = fetchWasmBody(999)

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0", "artifact": "aot",
	})
	if res.IsError {
		t.Fatalf("explicit aot push failed: %s", resultText(t, res))
	}
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/pairmod.aot"},
		{"fs_rm", "/drv/pairmod.lua"},
		{"fs_rm", "/drv/pairmod.wasm"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs)
	if _, ok := pd.files["d75884\x00/drv/pairmod.wasm"]; ok {
		t.Errorf("the .wasm survived the .aot push's sweep")
	}
	if got := pd.files["d75884\x00/drv/pairmod.aot"]; !bytes.Equal(got, aot) {
		t.Errorf("board .aot = %d bytes, want the artifact verbatim", len(got))
	}
	out := structured(t, res)
	if out["path"] != "/drv/pairmod.aot" {
		t.Errorf("path = %v, want the .aot slot", out["path"])
	}
}

// TestDriverPushExplicitSweepFailureHandling: a sweep fs_rm failing
// with a REAL error (not the fw's not-found) refuses honestly — the
// push landed but the manifest stays untouched and the error names the
// armed foot-gun; a not-found refusal is the absent-slot noise the
// sweep tolerates (already empty = the desired end state).
func TestDriverPushExplicitSweepFailureHandling(t *testing.T) {
	e, pd, root := newPairEnv(t)
	storePairVersion(t, root, "pairmod", "1.0.0", "esp32s3")
	pd.files["d75884\x00/drv/pairmod.aot"] = fetchAotBody(128) // the stale shield
	pd.failRm["/drv/pairmod.aot"] = "lease vanished mid-sweep"

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0", "artifact": "wasm",
	})
	if !res.IsError {
		t.Fatalf("push with a failing sweep succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"sweep", "/drv/pairmod.aot", "lease vanished mid-sweep", "manifest untouched"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	if m := wsManifest(t, root); m.Drivers["pairmod"].Active != "" {
		t.Errorf("Active = %q after the failed sweep, want none (error ⇒ manifest untouched)", m.Drivers["pairmod"].Active)
	}

	// the tolerated shape: BOTH of the fw's absent-file spellings are
	// sweep noise — the failRm line below pins the read-path spelling
	// ("'path' not found (...)"); the fake board's natural absent-slot
	// path now returns the REMOVE-path spelling ("rm 'path' failed (No
	// such file or directory)" — the real xs_fs_remove vocabulary the
	// P3-T15 board seat caught), which the absent .lua slot exercises
	// in this same push: both must be tolerated or this push fails.
	e2, pd2, root2 := newPairEnv(t)
	storePairVersion(t, root2, "pairmod", "1.0.0", "esp32s3")
	pd2.failRm["/drv/pairmod.aot"] = "'/drv/pairmod.aot' not found (No such file or directory)"
	res2 := e2.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0", "artifact": "wasm",
	})
	if res2.IsError {
		t.Fatalf("push with an absent sibling slot failed: %s", resultText(t, res2))
	}
	if out := structured(t, res2); out["swept"] != nil {
		t.Errorf("swept = %v, want absent (the .aot slot was already empty)", out["swept"])
	}
}

// TestDriverPushExplicitArtifactRefusals: the artifact key's enum and
// the missing-artifact refusal — "lua" is refused (the key is the
// binary single-artifact override; a .lua deploys bare like always), a
// garbage value is refused, and artifact "aot" with no stored .aot is
// a zero-crossing local refusal naming the candidates.
func TestDriverPushExplicitArtifactRefusals(t *testing.T) {
	e, pd, root := newPairEnv(t)
	storeArtifact(t, root, "pairmod", "1.0.0", "wasm", fetchWasmBody(64))

	for _, artifact := range []string{"lua", "AOT", "wasm2"} {
		res := e.call("xuansi_driver_push", map[string]any{
			"board": "d75884", "name": "pairmod", "version": "1.0.0", "artifact": artifact,
		})
		if !res.IsError {
			t.Fatalf("artifact %q accepted: %+v", artifact, res)
		}
		// the ENUM refusal, not the missing-artifact fall-through (the
		// seat check's S6 catch: a typo'd key must name the enum, not
		// send the agent looking for a fetch)
		if got := resultText(t, res); !strings.Contains(got, `"wasm" or "aot"`) || !strings.Contains(got, artifact) {
			t.Errorf("artifact %q refusal = %q, want the enum gate naming the bad value", artifact, got)
		}
	}
	noContact(t, pd.reqs)

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "pairmod", "version": "1.0.0", "artifact": "aot",
	})
	if !res.IsError {
		t.Fatalf("artifact aot with no stored .aot succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"pairmod@1.0.0.aot", "not in the workspace"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	noContact(t, pd.reqs)
}

// TestDriverPushBareAotOnlyStillSingle: a version holding ONLY an .aot
// (the build lane's shape) keeps the current bare behavior — one .aot
// leg, paired=false, no sweep. Loader preference is fw semantics; the
// daemon does not re-judge it.
func TestDriverPushBareAotOnlyStillSingle(t *testing.T) {
	e, pd, root := newPairEnv(t)
	seedChipBoard(t, e, "d75884", "ESP32-S3")
	abody := fetchAotBody(48)
	storeArtifact(t, root, "onlyaot", "1.0.0", "aot", abody)

	res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "onlyaot", "version": "1.0.0",
	})
	if res.IsError {
		t.Fatalf("aot-only push failed: %s", resultText(t, res))
	}
	for i, want := range []struct {
		typ  string
		path string
	}{
		{"fs_write", "/drv/onlyaot.aot"},
	} {
		req := wantReq(t, pd.reqs)
		if req.Type != want.typ || req.Path != want.path {
			t.Errorf("crossing %d = %s %s, want %s %s", i, req.Type, req.Path, want.typ, want.path)
		}
	}
	noContact(t, pd.reqs)
	out := structured(t, res)
	if out["paired"] != false || out["path"] != "/drv/onlyaot.aot" || out["sha256"] != sha256hex(abody) {
		t.Errorf("out = %v, want the single .aot leg with paired=false", out)
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

// TestDriverFetchHostileVersionRefusedZeroWrites (fix round 1): a
// version chosen FROM the index is re-gated against wsVerRe before any
// path or manifest use — a hostile index carrying "1.0/../../evil"
// must be a typed refusal with zero writes (the omission-version path
// was the exploitable one; the explicit path is gated at input).
func TestDriverFetchHostileVersionRefusedZeroWrites(t *testing.T) {
	e, _, root := newWsEnv(t, nil)
	url, hits := newFetchIndex(t, []fetchModSpec{{
		name: "fakemod", kind: "driver", version: "1.0/../../evil",
		files: []fetchFileSpec{fetchSpec(t, "modules/fakemod/1.0/../../evil/fakemod.wasm", fetchWasmBody(16), "")},
	}})

	// the omitted-version path: the index's chosen version is the attack
	res := e.call("xuansi_driver_fetch", map[string]any{"name": "fakemod", "index_url": url})
	if !res.IsError {
		t.Fatalf("hostile-version fetch succeeded: %+v", res)
	}
	got := resultText(t, res)
	for _, want := range []string{"api:", "1.0/../../evil", "[a-z0-9.]+"} {
		if !strings.Contains(got, want) {
			t.Errorf("error text = %q, want it to contain %q", got, want)
		}
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("HTTP hits = %d, want exactly 1 (the index; no file downloads for a refused version)", n)
	}
	if _, err := os.Stat(filepath.Join(root, "manifest.json")); !os.IsNotExist(err) {
		t.Errorf("manifest.json exists after a hostile-version refusal: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "drivers")); !os.IsNotExist(err) {
		t.Errorf("drivers/ dir exists after a hostile-version refusal: %v", err)
	}

	// the explicit-version path: refused at the input gate, ZERO HTTP
	res = e.call("xuansi_driver_fetch", map[string]any{
		"name": "fakemod", "version": "1.0/../../evil", "index_url": url,
	})
	if !res.IsError || !strings.HasPrefix(resultText(t, res), "api:") {
		t.Errorf("explicit hostile version = %v (%v), want a local api-class refusal", res, resultText(t, res))
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("HTTP hits moved to %d on the explicit refusal (want unchanged at 1 — refused before any HTTP)", n)
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
	// pipeline: fs_write FIRST and ONLY (no op caps for a wasm artifact;
	// no read-back — the sha256 the write carries is verified by the
	// board's commit, B15 §4.5②)
	writeReq := wantReq(t, reqs)
	if writeReq.Type != "fs_write" || writeReq.Path != "/drv/hellow.wasm" || writeReq.Holder != "mcp-fixed01" {
		t.Errorf("first crossing = %+v, want the held fs_write at /drv/hellow.wasm (no caps for the wasm track)", writeReq)
	}
	if !bytes.Equal(writeReq.Data, body) {
		t.Errorf("fs_write data = %d bytes, want the artifact verbatim", len(writeReq.Data))
	}
	if writeReq.Sha256 != sha256hex(body) {
		t.Errorf("fs_write sha256 = %s, want the artifact's digest", writeReq.Sha256)
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
	noContact(t, reqs) // the verified write is the last crossing

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
	noContact(t, reqs) // the verified write is the last crossing
}

// TestDriverPushShortArtifactMagicRefused (fix round 1, minor b): a
// below-4-byte .wasm/.aot artifact cannot carry the magic — refused
// locally with zero crossings, same as garbage magic.
func TestDriverPushShortArtifactMagicRefused(t *testing.T) {
	e, reqs, root := newWsPushEnv(t, 1, false)
	drvDir := filepath.Join(root, "drivers")
	if err := os.MkdirAll(drvDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(drvDir, "shorty@1.0.0.wasm"), []byte{0x00, 0x61}, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(drvDir, "shorty@1.0.0.aot"), []byte{0x00, 0x61, 0x6f}, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, ext := range []string{"wasm", "aot"} {
		res := e.call("xuansi_driver_push", map[string]any{
			"board": "d75884", "name": "shorty", "version": "1.0.0",
		})
		if !res.IsError {
			t.Errorf("short .%s push succeeded, want a local refusal", ext)
			continue
		}
		got := resultText(t, res)
		if !strings.HasPrefix(got, "api:") || !strings.Contains(got, "magic") {
			t.Errorf("short .%s error = %q, want an api-class magic refusal", ext, got)
		}
	}
	noContact(t, reqs)
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
	for i := 0; i < 2; i++ { // caps + write
		wantReq(t, reqs)
	}

	res := e.call("xuansi_driver_pull", map[string]any{"board": "d75884", "name": "b8pull"})
	if res.IsError {
		t.Fatalf("pull failed: %s", resultText(t, res))
	}
	// the sweep (B10-T8): one held fs_rm per extension slot a push could
	// have written — .lua, .wasm, .aot in pushExts order (the board file
	// for this driver is the .lua one; the other slots ack their
	// absence, which the lenient push env answers as removals)
	for i, ext := range []string{"lua", "wasm", "aot"} {
		rmReq := wantReq(t, reqs)
		if rmReq.Type != "fs_rm" || rmReq.Path != "/drv/b8pull."+ext || rmReq.Holder != "mcp-fixed01" {
			t.Errorf("fs_rm request %d = %+v, want the held removal at /drv/b8pull.%s", i, rmReq, ext)
		}
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
		capsStep(1),                                       // push: caps
		{result: map[string]any{"ok": true}},              // push: fs_write (verified — the commit verdict)
		{rerr: &server.RPCError{Class: "api", Msg: want}}, // pull: fs_rm .lua refused
		{rerr: &server.RPCError{Class: "api", Msg: "rm '/drv/ghost.wasm' failed (No such file or directory)"}}, // pull: .wasm slot equally absent
		{rerr: &server.RPCError{Class: "api", Msg: "rm '/drv/ghost.aot' failed (No such file or directory)"}},  // pull: .aot slot equally absent
	})
	mustPut(t, e, "ghost", "1.0.0", content)
	if res := e.call("xuansi_driver_push", map[string]any{
		"board": "d75884", "name": "ghost", "version": "1.0.0",
	}); res.IsError {
		t.Fatalf("push failed: %s", resultText(t, res))
	}
	for i := 0; i < 2; i++ { // the push's caps + write
		wantReq(t, reqs)
	}

	res := e.call("xuansi_driver_pull", map[string]any{"board": "d75884", "name": "ghost"})
	if !res.IsError {
		t.Fatalf("pull of an absent file succeeded: %+v", res)
	}
	if got := resultText(t, res); got != "api: "+want {
		t.Errorf("error text = %q, want exactly %q (the FIRST slot's refusal — the .lua probe)", got, "api: "+want)
	}
	for i, p := range []string{"/drv/ghost.lua", "/drv/ghost.wasm", "/drv/ghost.aot"} {
		if req := wantReq(t, reqs); req.Type != "fs_rm" || req.Path != p {
			t.Errorf("fs_rm request %d = %+v, want the sweep reaching %s", i, req, p)
		}
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
	for i, ext := range []string{"lua", "wasm", "aot"} { // the full sweep, all auto-acked by this env
		if req := wantReq(t, reqs); req.Type != "fs_rm" || req.Path != "/drv/leftover."+ext {
			t.Errorf("fs_rm request %d = %+v, want the sweep at /drv/leftover.%s", i, req, ext)
		}
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
	for i := 0; i < 2; i++ {
		wantReq(t, reqs)
	}

	outPull := e.callAsync("xuansi_driver_pull", map[string]any{"board": "d75884", "name": "race"})
	outPush := e.callAsync("xuansi_driver_push", map[string]any{"board": "d75884", "name": "race", "version": "1.0.0"})
	for _, out := range []chan *mcpsdk.CallToolResult{outPull, outPush} {
		if res := waitResult(t, out, "concurrent pull/push"); res.IsError {
			t.Fatalf("concurrent pull/push failed: %s", resultText(t, res))
		}
	}
	for i := 0; i < 5; i++ { // pull: fs_rm ×3 (the sweep); push: caps + write
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

// TestDriverPullRemovesFetchedWasmBoardFile: the generalized pull
// sweeps EVERY extension slot a push could have written (B10-T8): a
// fetched .wasm deployed to /drv/<name>.wasm is removed through the
// .wasm slot's held fs_rm while the ABSENT .lua/.aot slots' api
// not-found refusals are tolerated, the reported path names the .wasm
// (the first REMOVED slot), and the manifest's Active clears with the
// Files history kept verbatim.
func TestDriverPullRemovesFetchedWasmBoardFile(t *testing.T) {
	body := fetchWasmBody(64)
	nf := func(p string) *server.RPCError {
		return &server.RPCError{Class: "api", Msg: "rm '" + p + "' failed (No such file or directory)"}
	}
	e, reqs, root := newWsEnv(t, []step{
		{result: map[string]any{"ok": true}}, // push: fs_write (verified — the commit verdict; wasm track: no caps crossing)
		{rerr: nf("/drv/wbmod.lua")},         // pull: .lua slot absent — tolerated
		{result: map[string]any{}},           // pull: .wasm slot removed
		{rerr: nf("/drv/wbmod.aot")},         // pull: .aot slot absent — tolerated
	})
	drvDir := filepath.Join(root, "drivers")
	if err := os.MkdirAll(drvDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(drvDir, "wbmod@1.0.0.wasm"), body, 0o644); err != nil {
		t.Fatal(err)
	}

	res := e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "wbmod", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("wasm push failed: %s", resultText(t, res))
	}
	for i := 0; i < 1; i++ { // push: the one verified fs_write
		wantReq(t, reqs)
	}

	res = e.call("xuansi_driver_pull", map[string]any{"board": "d75884", "name": "wbmod"})
	if res.IsError {
		t.Fatalf("pull failed: %s", resultText(t, res))
	}
	for i, p := range []string{"/drv/wbmod.lua", "/drv/wbmod.wasm", "/drv/wbmod.aot"} {
		if req := wantReq(t, reqs); req.Type != "fs_rm" || req.Path != p || req.Holder != "mcp-fixed01" {
			t.Errorf("fs_rm request %d = %+v, want the held sweep at %s", i, req, p)
		}
	}
	if out := structured(t, res); out["pulled"] != true || out["path"] != "/drv/wbmod.wasm" {
		t.Errorf("pull result = %v, want {pulled:true, path:/drv/wbmod.wasm} (the removed slot)", out)
	}

	// the manifest: Active cleared, the wasm version's history kept
	m := wsManifest(t, root)
	ent := m.Drivers["wbmod"]
	if ent.Active != "" {
		t.Errorf("Active = %q after the wasm pull, want \"\"", ent.Active)
	}
	if fm := ent.Files["1.0.0"]; fm.Sha256 != sha256hex(body) || fm.Size != len(body) {
		t.Errorf("Files[1.0.0] = %+v, want the wasm history kept verbatim", fm)
	}
}
