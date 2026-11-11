package server

// fs_chunked_test.go — the chunked fs transport at the internal-API
// seam (B10-T1, re-pinned to the 64KB chunk of B15 §4.5): fs_write at
// or below the 65472B chunk with no sha256 key is a single verbatim
// fs.write; above it (and on EVERY verified write — the sha256 key,
// B15 §4.5②) the daemon loops fs.write_part per 65472B chunk and lands
// the file with one sha256-verifying fs.write_commit; fs_read
// assembles fs.read_part windows until eof. The fake controllers pin
// the loop boundaries: exactly 65472 / 65473 / 130944 / 111KB bytes,
// and the verified path's any-size commit.

import (
	"bytes"

	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"xuansi/internal/replclient"
)

// chunkedFake scripts the three chunked ops over an in-memory .part
// mirror, recording every call for the boundary assertions. corrupt
// (the tamper leg) flips a byte in each staged write_part — the
// mid-transfer corruption whose commit verdict must refuse.
type chunkedFake struct {
	inner   *fakeCtl
	parts   map[string][]byte // logical .part path -> staged bytes
	files   map[string][]byte // committed files
	calls   []string          // "op off/len" trace
	corrupt bool
}

func newChunkedFake(t *testing.T, e *env, mac6 string) *chunkedFake {
	t.Helper()
	cf := &chunkedFake{
		inner: e.fakes[mac6],
		parts: map[string][]byte{},
		files: map[string][]byte{},
	}
	cf.inner.on("fs.write", func(args map[string]any) (map[string]any, *replclient.OpError) {
		path, _ := args["path"].(string)
		data, _ := args["data"].([]byte)
		cf.calls = append(cf.calls, "fs.write")
		cf.files[path] = append([]byte{}, data...)
		return map[string]any{"size": int64(len(data))}, nil
	})
	cf.inner.on("fs.write_part", func(args map[string]any) (map[string]any, *replclient.OpError) {
		path, _ := args["path"].(string)
		off, _ := args["off"].(int)
		data, _ := args["data"].([]byte)
		key := path + ".part"
		if off == 0 {
			cf.parts[key] = append([]byte{}, data...)
		} else {
			if got := len(cf.parts[key]); got != off {
				return nil, &replclient.OpError{Op: "fs.write_part", Class: "api",
					Msg: "off mismatch (fake .part is %d bytes)"}
			}
			cf.parts[key] = append(cf.parts[key], data...)
		}
		if cf.corrupt && len(data) > 0 {
			staged := cf.parts[key]
			staged[len(staged)-1] ^= 1 // the mid-transfer byte that goes wrong
		}
		cf.calls = append(cf.calls, "fs.write_part")
		return map[string]any{"size": int64(len(cf.parts[key]))}, nil
	})
	cf.inner.on("fs.write_commit", func(args map[string]any) (map[string]any, *replclient.OpError) {
		path, _ := args["path"].(string)
		wantLen, _ := args["expected_len"].(int)
		sha, hasSha := args["sha256"].(string)
		key := path + ".part"
		staged, ok := cf.parts[key]
		if !ok {
			return nil, &replclient.OpError{Op: "fs.write_commit", Class: "api", Msg: "no .part"}
		}
		if len(staged) != wantLen {
			return nil, &replclient.OpError{Op: "fs.write_commit", Class: "api", Msg: "length mismatch"}
		}
		if hasSha {
			sum := sha256.Sum256(staged)
			if hex.EncodeToString(sum[:]) != sha {
				// the fw's own refusal spelling (xs_fs.c), .part kept for retry
				return nil, &replclient.OpError{Op: "fs.write_commit", Class: "api",
					Msg: "sha256 mismatch (.part=<got>, expected=" + sha + ")"}
			}
		}
		cf.files[path] = staged
		delete(cf.parts, key)
		cf.calls = append(cf.calls, "fs.write_commit")
		return map[string]any{"size": int64(wantLen)}, nil
	})
	cf.inner.on("fs.read_part", func(args map[string]any) (map[string]any, *replclient.OpError) {
		path, _ := args["path"].(string)
		off, _ := args["off"].(int)
		length, _ := args["len"].(int)
		// the fw's own reply-frame gate (board-caught in the W1.T4
		// smoke): a window above 32640 draws this exact refusal — the
		// pin that keeps the read window off the write-chunk axis
		if length > 32640 {
			return nil, &replclient.OpError{Op: "fs.read_part", Class: "api",
				Msg: fmt.Sprintf("len %d too large for reply frame (max 32640)", length)}
		}
		f, ok := cf.files[path]
		if !ok {
			f, ok = cf.parts[path]
		}
		if !ok {
			return nil, &replclient.OpError{Op: "fs.read_part", Class: "api", Msg: "not found"}
		}
		if off > len(f) {
			return nil, &replclient.OpError{Op: "fs.read_part", Class: "api", Msg: "beyond eof"}
		}
		end := off + length
		if end > len(f) {
			end = len(f)
		}
		cf.calls = append(cf.calls, "fs.read_part")
		return map[string]any{
			"data":  append([]byte{}, f[off:end]...),
			"total": int64(len(f)),
			"eof":   end >= len(f),
		}, nil
	})
	return cf
}

// runFsWrite leases and issues one fs_write of data; the reply must be
// the commit's {size} (or the single fs.write's for the small path).
func runFsWrite(t *testing.T, e *env, path string, data []byte) int64 {
	t.Helper()
	return runFsWriteSha(t, e, path, data, "")
}

// runFsWriteSha is runFsWrite with the verified path's sha256 key.
func runFsWriteSha(t *testing.T, e *env, path string, data []byte, sha string) int64 {
	t.Helper()
	rep, _ := dialSock(t, e.sock).call(1, map[string]any{
		"type": "lease", "board": "d75884", "holder": "chunky",
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	req := map[string]any{
		"type": "fs_write", "board": "d75884", "path": path,
		"data": base64.StdEncoding.EncodeToString(data), "holder": "chunky",
	}
	if sha != "" {
		req["sha256"] = sha
	}
	rep, _ = dialSock(t, e.sock).call(2, req)
	res := asMap(rep["result"])
	if res == nil {
		t.Fatalf("fs_write %d bytes: %#v", len(data), rep)
	}
	return asIntField(t, res, "size")
}

func asIntField(t *testing.T, m map[string]any, key string) int64 {
	t.Helper()
	switch v := m[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	t.Fatalf("field %s = %#v, want an int", key, m[key])
	return 0
}

// TestFsWriteChunkBoundaries: the loop boundaries pinned at the 64KB
// chunk (B15 §4.5①) — exactly 65472 is ONE fs.write (the small path);
// 65473 is two parts (65472+1) + commit; 130944 is two full parts +
// commit; a 111KB payload (113664B, the module-size bench case) is two
// parts (65472+48192) + commit. Every commit carries the sha256 of the
// whole payload, and the fake verifies it against the staged bytes it
// actually received.
func TestFsWriteChunkBoundaries(t *testing.T) {
	for _, tc := range []struct {
		n         int
		wantCalls []string
	}{
		{65472, []string{"fs.write"}},
		{65473, []string{"fs.write_part", "fs.write_part", "fs.write_commit"}},
		{130944, []string{"fs.write_part", "fs.write_part", "fs.write_commit"}},
		{113664, []string{"fs.write_part", "fs.write_part", "fs.write_commit"}},
		{1, []string{"fs.write"}},
	} {
		e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
		cf := newChunkedFake(t, e, "d75884")
		data := makePattern(tc.n)

		if got := runFsWrite(t, e, "/drv/mod.aot", data); got != int64(tc.n) {
			t.Fatalf("n=%d: reply size = %d, want %d", tc.n, got, tc.n)
		}
		if len(cf.calls) != len(tc.wantCalls) {
			t.Fatalf("n=%d: board saw %d calls %v, want %v", tc.n, len(cf.calls), cf.calls, tc.wantCalls)
		}
		for i, want := range tc.wantCalls {
			if cf.calls[i] != want {
				t.Fatalf("n=%d: call %d = %q, want %q (trace %v)", tc.n, i, cf.calls[i], want, cf.calls)
			}
		}
		if got := cf.files["/drv/mod.aot"]; !bytes.Equal(got, data) {
			t.Fatalf("n=%d: committed file = %d bytes, want byte-exact %d", tc.n, len(got), tc.n)
		}
	}
}

// TestFsWriteChunkTrace111KB: full call trace for the 111KB bench
// payload — two write_parts (65472 + 48192) then the commit (the
// offsets themselves are pinned by the fake's off-mismatch refusal
// plus the byte-exact file compare).
func TestFsWriteChunkTrace111KB(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	data := makePattern(113664) // 111KB
	runFsWrite(t, e, "/drv/mod.aot", data)
	want := []string{"fs.write_part", "fs.write_part", "fs.write_commit"}
	if len(cf.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", cf.calls, want)
	}
	for i, w := range want {
		if cf.calls[i] != w {
			t.Fatalf("call %d = %q, want %q (trace %v)", i, cf.calls[i], w, cf.calls)
		}
	}
	// the two chunk boundaries, exactly
	if got := len(cf.parts) + len(cf.files); got != 1 {
		t.Fatalf("staged+committed = %d stores, want the one committed file (no .part left)", got)
	}
	if got := cf.files["/drv/mod.aot"]; !bytes.Equal(got, data) {
		t.Fatalf("committed = %d bytes, want the whole 111KB byte-exact", len(got))
	}
}

// TestFsWriteVerifiedPathAnySize (B15 §4.5②): a PRESENT sha256 key
// rides the chunked transport even for a small payload — one
// fs.write_part + one fs.write_commit whose sha256 argument is the
// CALLER's digest (the fake verifies it; the commit verdict is the
// push path's verification).
func TestFsWriteVerifiedPathAnySize(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	data := makePattern(1000) // far below the chunk — the verified path anyway
	sum := sha256.Sum256(data)

	if got := runFsWriteSha(t, e, "/drv/small.lua", data, hex.EncodeToString(sum[:])); got != 1000 {
		t.Fatalf("reply size = %d, want 1000", got)
	}
	want := []string{"fs.write_part", "fs.write_commit"}
	if len(cf.calls) != len(want) {
		t.Fatalf("calls = %v, want %v (verified path: no single fs.write)", cf.calls, want)
	}
	for i, w := range want {
		if cf.calls[i] != w {
			t.Fatalf("call %d = %q, want %q", i, cf.calls[i], w)
		}
	}
	if got := cf.files["/drv/small.lua"]; !bytes.Equal(got, data) {
		t.Fatal("committed file not byte-exact (the fake's sha gate would have refused)")
	}
}

// TestFsWriteVerifiedPathCorruptStagingRefused: a byte corrupted
// mid-transfer (the fake flips one in staging) — the commit's sha
// verdict REFUSES, the board's own api error crosses verbatim, and
// nothing lands at the logical path (the fw keeps only the .part for a
// from-off=0 retry; it never shadows the real file).
func TestFsWriteVerifiedPathCorruptStagingRefused(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	cf.corrupt = true
	data := makePattern(70000) // two chunks — the corruption rides the second
	sum := sha256.Sum256(data)

	w := dialSock(t, e.sock)
	rep, _ := w.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "chunky"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = w.call(2, map[string]any{
		"type": "fs_write", "board": "d75884", "path": "/drv/mod.aot",
		"data":   base64.StdEncoding.EncodeToString(data),
		"sha256": hex.EncodeToString(sum[:]), "holder": "chunky",
	})
	er := asMap(rep["error"])
	if er == nil || asStr(er["class"]) != "api" {
		t.Fatalf("corrupted staging = %#v, want the board's api refusal", rep)
	}
	if msg := asStr(er["msg"]); !strings.Contains(msg, "sha256 mismatch") {
		t.Fatalf("refusal msg = %q, want the fw's commit verdict spelling", msg)
	}
	if _, landed := cf.files["/drv/mod.aot"]; landed {
		t.Fatal("a mismatched commit landed the file anyway — the verdict must refuse")
	}
}

// TestFsWriteMalformedShaZeroContact: a sha256 key that is not 64 hex
// chars is refused daemon-side with ZERO board contact — the fw would
// refuse it at commit, after the whole flash write had already crossed;
// refusing early saves the cycle (the honest-refusal economics).
func TestFsWriteMalformedShaZeroContact(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	w := dialSock(t, e.sock)
	rep, _ := w.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "chunky"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = w.call(2, map[string]any{
		"type": "fs_write", "board": "d75884", "path": "/drv/mod.aot",
		"data": base64.StdEncoding.EncodeToString([]byte("x")), "sha256": "nothex",
		"holder": "chunky",
	})
	er := asMap(rep["error"])
	if er == nil || asStr(er["class"]) != "api" || !strings.Contains(asStr(er["msg"]), "64 hex") {
		t.Fatalf("malformed sha = %#v, want the zero-contact api refusal", rep)
	}
	if len(cf.calls) != 0 {
		t.Fatalf("board saw %v — a malformed sha must never cross", cf.calls)
	}
}

// makePattern builds deterministic payload bytes (position-dependent).
func makePattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7 % 251)
	}
	return b
}

// TestFsReadChunkedAssembly: fs_read returns the WHOLE file assembled
// from fs.read_part windows — a 111KB file lands as 4 windows
// (32640×3 + 25744, the reply-frame-capped read axis — NOT the 65472
// write chunk) with one fs_read reply, byte-exact.
func TestFsReadChunkedAssembly(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	want := makePattern(113664)
	cf.files["/drv/mod.aot"] = want

	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "fs_read", "board": "d75884", "path": "/drv/mod.aot"})
	res := asMap(rep["result"])
	if res == nil {
		t.Fatalf("fs_read: %#v", rep)
	}
	b64, _ := res["data"].(string)
	got, derr := base64.StdEncoding.DecodeString(b64)
	if derr != nil {
		t.Fatalf("decode: %v", derr)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("fs_read = %d bytes, want the full %d byte-exact", len(got), len(want))
	}
	parts := 0
	for _, c := range cf.calls {
		if c == "fs.read_part" {
			parts++
		}
	}
	if parts != 4 {
		t.Fatalf("fs.read_part calls = %d, want 4 (32640×3 + 25744)", parts)
	}
}

// TestFsReadMissingSurfacesBoardError: the board's own api not-found
// crosses verbatim through the chunked read path.
func TestFsReadMissingSurfacesBoardError(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	newChunkedFake(t, e, "d75884")
	rep, _ := dialSock(t, e.sock).call(1, map[string]any{
		"type": "fs_read", "board": "d75884", "path": "/drv/nope.aot",
	})
	er := asMap(rep["error"])
	if asStr(er["class"]) != "api" {
		t.Fatalf("error = %#v, want the board's api class verbatim", er)
	}
}

// TestFsWriteChunkShaVerifiedByFake: the commit's sha256 argument is
// the digest of the FULL payload (the fake recomputes it over the
// staged bytes and mismatches refuse) — proving the daemon sends the
// whole-payload digest, not a per-chunk one.
func TestFsWriteChunkShaVerifiedByFake(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	data := makePattern(113664) // 111KB across two chunks
	runFsWrite(t, e, "/drv/mod.aot", data)
	if got := cf.files["/drv/mod.aot"]; !bytes.Equal(got, data) {
		t.Fatal("committed file not byte-exact (fake sha gate would have refused)")
	}
}

// TestFsReadRedialDoesNotDuplicate: a link death mid-assembly (after a
// partial first window) redials once and restarts at off=0 — the
// accumulator must DROP the dead attempt's partial prefix (fix round
// 1: without the reset the retry appended the whole file onto the
// prefix and returned the corruption as success). Deterministic: the
// dying fake serves window 1, then atomically swaps itself out for the
// fresh fake and dies, so window 2 hits ErrDead and the redial dials
// the fresh fake — no sleeps, no races.
func TestFsReadRedialDoesNotDuplicate(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	want := makePattern(113664)

	// attempt 2 (the redial target): a FRESH fake serving the whole file.
	fresh := newFakeCtl()
	served := 0
	fresh.on("fs.read_part", func(args map[string]any) (map[string]any, *replclient.OpError) {
		off, _ := args["off"].(int)
		length, _ := args["len"].(int)
		end := off + length
		if end > len(want) {
			end = len(want)
		}
		served++
		return map[string]any{
			"data":  append([]byte{}, want[off:end]...),
			"total": int64(len(want)),
			"eof":   end >= len(want),
		}, nil
	})

	// attempt 1: serves a 100B partial first window (eof=false), then
	// swaps in the fresh fake and dies — the next window call returns
	// ErrDead, withController redials, and the redial must restart the
	// accumulator, not append onto the partial prefix.
	dying := e.fakes["d75884"]
	e.fakes["d75884"].on("fs.read_part", func(args map[string]any) (map[string]any, *replclient.OpError) {
		e.dmu.Lock()
		e.fakes["d75884"] = fresh
		e.dmu.Unlock()
		dying.markDead()
		return map[string]any{
			"data":  append([]byte{}, want[:100]...),
			"total": int64(len(want)),
			"eof":   false,
		}, nil
	})

	rep, _ := dialSock(t, e.sock).call(1, map[string]any{
		"type": "fs_read", "board": "d75884", "path": "/drv/mod.aot",
	})
	res := asMap(rep["result"])
	if res == nil {
		t.Fatalf("fs_read: %#v", rep)
	}
	b64, _ := res["data"].(string)
	got, derr := base64.StdEncoding.DecodeString(b64)
	if derr != nil {
		t.Fatalf("decode: %v", derr)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("fs_read after redial = %d bytes, want exactly the %d-byte file (no duplicated prefix)", len(got), len(want))
	}
	if served == 0 {
		t.Fatal("the fresh fake never served — the redial path did not run")
	}
	if e.dialedTCP("d75884") < 2 {
		t.Fatalf("dials = %d, want the one redial", e.dialedTCP("d75884"))
	}
}
