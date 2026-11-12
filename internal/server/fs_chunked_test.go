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
	"time"

	"xuansi/internal/replclient"
)

// chunkedFake scripts the three chunked ops over an in-memory .part
// mirror, recording every call for the boundary assertions. corrupt
// (the tamper leg) flips a byte in each staged write_part — the
// mid-transfer corruption whose commit verdict must refuse. readLens
// records every fs.read_part's REQUESTED len (the walk's window — the
// W3.T4a era-clamp pin); writeCap > 0 arms an era-capped write gate
// (the v6 fs_write asymmetry pin — 0 = the uncapped default).
type chunkedFake struct {
	inner    *fakeCtl
	parts    map[string][]byte // logical .part path -> staged bytes
	files    map[string][]byte // committed files
	calls    []string          // "op off/len" trace
	readLens []int             // every fs.read_part's requested len
	writeCap int               // >0: refuse write payloads over this (era gate)
	corrupt  bool
}

func newChunkedFake(t *testing.T, e *env, mac6 string) *chunkedFake {
	return newChunkedFakeEra(t, e, mac6, 7)
}

// newChunkedFakeEra is newChunkedFake with the fake board sighted at
// protocol_ver pv. The chunked transport is a v7-era face (the 64KB
// chunk and the 32640 reply frame were both measured on v7 builds),
// so the default fake says 7; the W3.T4a clamp tests script the
// pre-v7 eras. pv<7 also lowers the fake's own fs.read_part gate to
// the era's 16384 cap with the v6 fw's refusal spelling.
func newChunkedFakeEra(t *testing.T, e *env, mac6 string, pv int64) *chunkedFake {
	t.Helper()
	cf := &chunkedFake{
		inner: e.fakes[mac6],
		parts: map[string][]byte{},
		files: map[string][]byte{},
	}
	cf.inner.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": pv, "role": "controller",
			"fw": "xuansi-fw v0.5.0", "fw_build": "4d9d9ca", "fs": true,
		}, nil
	})
	cf.inner.on("fs.write", func(args map[string]any) (map[string]any, *replclient.OpError) {
		path, _ := args["path"].(string)
		data, _ := args["data"].([]byte)
		if cf.writeCap > 0 && len(data) > cf.writeCap {
			// the era-capped write gate (the W3.T4a asymmetry pin):
			// a v6-era fw refuses an over-cap write chunk honestly —
			// the daemon must surface this verbatim, NOT chunk down
			return nil, &replclient.OpError{Op: "fs.write", Class: "api",
				Msg: fmt.Sprintf("chunk too large (%d > %d bytes)", len(data), cf.writeCap)}
		}
		cf.calls = append(cf.calls, "fs.write")
		cf.files[path] = append([]byte{}, data...)
		return map[string]any{"size": int64(len(data))}, nil
	})
	cf.inner.on("fs.write_part", func(args map[string]any) (map[string]any, *replclient.OpError) {
		path, _ := args["path"].(string)
		off, _ := args["off"].(int)
		data, _ := args["data"].([]byte)
		if cf.writeCap > 0 && len(data) > cf.writeCap {
			return nil, &replclient.OpError{Op: "fs.write_part", Class: "api",
				Msg: fmt.Sprintf("chunk too large (%d > %d bytes)", len(data), cf.writeCap)}
		}
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
	readCap := 32640
	if pv < 7 {
		readCap = 16384
	}
	cf.inner.on("fs.read_part", func(args map[string]any) (map[string]any, *replclient.OpError) {
		path, _ := args["path"].(string)
		off, _ := args["off"].(int)
		length, _ := args["len"].(int)
		cf.readLens = append(cf.readLens, length)
		if length > readCap {
			if pv >= 7 {
				// the fw's own reply-frame gate (board-caught in the
				// W1.T4 smoke): a window above 32640 draws this exact
				// refusal — the pin that keeps the read window off the
				// write-chunk axis
				return nil, &replclient.OpError{Op: "fs.read_part", Class: "api",
					Msg: fmt.Sprintf("len %d too large for reply frame (max %d)", length, readCap)}
			}
			// the v6 fw's own spelling (the W3.T3 field finding:
			// fw 0.4.0 refuses ANY window over 16384 with exactly
			// this, regardless of file size)
			return nil, &replclient.OpError{Op: "fs.read_part", Class: "api",
				Msg: fmt.Sprintf("chunk too large (%d > %d bytes)", length, readCap)}
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

// TestFsReadWindowClampsByProtocolVer (W3.T4a): the walk's window
// follows the board's cached protocol era — the hello sighting's
// protocol_ver. A pv6 board (fw 0.4.0 — Board #1 in the field) caps
// fs.read_part at 16384 and refuses any larger window with "chunk
// too large (N > 16384 bytes)" REGARDLESS of file size, so the
// daemon's old fixed 32640 window killed EVERY fs_read on it (the
// W3.T3 source-corroborated finding); the walk must clamp to 16384
// there and keep the 32640 reply-frame window on pv>=7. 40000B pins
// both eras' arithmetic: pv6 = 16384×2 + 7232 (3 windows), pv7 =
// 32640 + 7360 (2 windows).
func TestFsReadWindowClampsByProtocolVer(t *testing.T) {
	for _, tc := range []struct {
		pv        int64
		wantLen   int
		wantParts int
	}{
		{6, 16384, 3},
		{7, 32640, 2},
	} {
		e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
		// The startup audit taps a DEFAULT-hello (pv5) sighting on the
		// board before this body runs — wait it out, then swap in the
		// era's fake and kill the audit-warmed link. The walk's first
		// window then hits the corpse, withController redials onto the
		// era fake, and the redial's re-tap sights the scripted
		// protocol_ver — the real semantics the clamp rides (an era
		// change surfaces on the next reconnect's re-tap).
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			b, ok := e.srv.lookupExact("d75884")
			if ok && b.Hello != nil {
				break // the audit's tap enriched its sighting
			}
			time.Sleep(5 * time.Millisecond)
		}
		if b, ok := e.srv.lookupExact("d75884"); !ok || b.Hello == nil {
			t.Fatalf("pv%d: the startup audit never sighted the board", tc.pv)
		}
		old := e.fakes["d75884"]
		e.dmu.Lock()
		e.fakes["d75884"] = newFakeCtl()
		e.dmu.Unlock()
		cf := newChunkedFakeEra(t, e, "d75884", tc.pv)
		old.markDead() // the audit-warmed link dies under the walk

		want := makePattern(40000)
		cf.files["/drv/mod.aot"] = want

		rep, _ := dialSock(t, e.sock).call(1, map[string]any{
			"type": "fs_read", "board": "d75884", "path": "/drv/mod.aot",
		})
		res := asMap(rep["result"])
		if res == nil {
			t.Fatalf("pv%d: fs_read: %#v (the walk's window drew the era's refusal?)", tc.pv, rep)
		}
		b64, _ := res["data"].(string)
		got, derr := base64.StdEncoding.DecodeString(b64)
		if derr != nil {
			t.Fatalf("pv%d: decode: %v", tc.pv, derr)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("pv%d: fs_read = %d bytes, want the full 40000 byte-exact", tc.pv, len(got))
		}
		if len(cf.readLens) != tc.wantParts {
			t.Fatalf("pv%d: fs.read_part calls = %d (lens %v), want %d", tc.pv, len(cf.readLens), cf.readLens, tc.wantParts)
		}
		for i, l := range cf.readLens {
			if l != tc.wantLen {
				t.Fatalf("pv%d: window %d = %dB, want the era's %dB window (lens %v)", tc.pv, i, l, tc.wantLen, cf.readLens)
			}
		}
	}
}

// TestFsWriteChunkEraAgnostic (W3.T4a): the WRITE path is
// deliberately NOT era-clamped — the W1.T4 ruling keeps the chunk
// trio moving together by design: an old-fw board honestly refuses
// an over-cap write with its own chunk-too-large error, and that
// refusal IS the era signal. A pv6 fake (write gate armed at 16384)
// still receives the daemon's 65472B write_part — and refuses it;
// the daemon must surface the board's refusal verbatim, never
// silently chunk down. The read/write asymmetry is the point: reads
// must not break (a field 0.4.0 board still serves fs_read), writes
// teach (fsReadWindow's comment).
func TestFsWriteChunkEraAgnostic(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFakeEra(t, e, "d75884", 6)
	cf.writeCap = 16384 // the v6 fw's write gate — same class as its read gate

	w := dialSock(t, e.sock)
	rep, _ := w.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "chunky"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = w.call(2, map[string]any{
		"type": "fs_write", "board": "d75884", "path": "/drv/mod.aot",
		"data": base64.StdEncoding.EncodeToString(makePattern(70000)), // 65472 + 4528
	})
	er := asMap(rep["error"])
	if er == nil || asStr(er["class"]) != "api" || !strings.Contains(asStr(er["msg"]), "chunk too large") {
		t.Fatalf("v6 fs_write = %#v, want the board's chunk-too-large refusal crossing verbatim (writes are NOT clamped — the W1.T4 trio ruling)", rep)
	}
	if len(cf.files) != 0 || len(cf.parts) != 0 {
		t.Fatal("a refused write must land nothing")
	}
}
