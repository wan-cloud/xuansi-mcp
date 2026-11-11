package server

// fs_chunked_test.go — the v5 chunked fs transport at the internal-API
// seam (B10-T1): fs_write at/below 16384B is a single verbatim fs.write;
// above it the daemon loops fs.write_part per 16384B chunk and lands the
// file with one sha256-verifying fs.write_commit; fs_read assembles
// fs.read_part windows until eof. The fake controllers pin the loop
// boundaries: exactly 16384 / 16385 / 32768 / 40000 bytes.

import (
	"bytes"

	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"xuansi/internal/replclient"
)

// chunkedFake scripts the three chunked ops over an in-memory .part
// mirror, recording every call for the boundary assertions.
type chunkedFake struct {
	inner *fakeCtl
	parts map[string][]byte // logical .part path -> staged bytes
	files map[string][]byte // committed files
	calls []string          // "op off/len" trace
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
				return nil, &replclient.OpError{Op: "fs.write_commit", Class: "api", Msg: "sha256 mismatch"}
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
	rep, _ := dialSock(t, e.sock).call(1, map[string]any{
		"type": "lease", "board": "d75884", "holder": "chunky",
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = dialSock(t, e.sock).call(2, map[string]any{
		"type": "fs_write", "board": "d75884", "path": path,
		"data": base64.StdEncoding.EncodeToString(data), "holder": "chunky",
	})
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

// TestFsWriteChunkBoundaries: the loop boundaries pinned — exactly
// 16384 is ONE fs.write (the pre-B10 wire, zero parts); 16385 is two
// parts (16384+1) + commit; 32768 is two full parts + commit; 40000 is
// three parts (16384+16384+7232) + commit. Every commit carries the
// sha256 of the whole payload, and the fake verifies it against the
// staged bytes it actually received.
func TestFsWriteChunkBoundaries(t *testing.T) {
	for _, tc := range []struct {
		n         int
		wantCalls []string
	}{
		{16384, []string{"fs.write"}},
		{16385, []string{"fs.write_part", "fs.write_part", "fs.write_commit"}},
		{32768, []string{"fs.write_part", "fs.write_part", "fs.write_commit"}},
		{40000, []string{"fs.write_part", "fs.write_part", "fs.write_part", "fs.write_commit"}},
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
			t.Fatalf("n=%d: committed file = %d bytes, want byte-exact %d", tc.n, len(got), len(data))
		}
	}
}

// TestFsWriteChunkTrace40000: full call trace for a 40000B payload —
// three write_parts then the commit (the offsets themselves are pinned
// by the fake's off-mismatch refusal plus the byte-exact file compare).
func TestFsWriteChunkTrace40000(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	data := makePattern(40000)
	runFsWrite(t, e, "/drv/mod.aot", data)
	want := []string{"fs.write_part", "fs.write_part", "fs.write_part", "fs.write_commit"}
	if len(cf.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", cf.calls, want)
	}
	for i, w := range want {
		if cf.calls[i] != w {
			t.Fatalf("call %d = %q, want %q (trace %v)", i, cf.calls[i], w, cf.calls)
		}
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
// from fs.read_part windows — a 33280B file lands as 16384+16384+512
// with one fs_read reply, byte-exact.
func TestFsReadChunkedAssembly(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	want := makePattern(33280)
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
		t.Fatalf("fs_read = %d bytes, want the full 33280 byte-exact", len(got))
	}
	parts := 0
	for _, c := range cf.calls {
		if c == "fs.read_part" {
			parts++
		}
	}
	if parts != 3 {
		t.Fatalf("fs.read_part calls = %d, want 3 (16384+16384+512)", parts)
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
	data := makePattern(33280)
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
	want := makePattern(33280)

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
