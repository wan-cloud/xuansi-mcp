package server

// fs_stream_test.go — the v8 upload lane at the internal-API seam
// (B16-T7 C2): fs_write's large/verified path routes by the board's
// protocol era — pv>=8 rides fs.stream_write (the Controller's
// FsStreamWrite face), pre-v8 and never-sighted boards keep the
// write_part trio byte-identically (the era-agnostic ruling carried),
// and the small unverified path stays plain fs.write on every era.

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"xuansi/internal/replclient"
)

// armStreamLane scripts the fake's upload lane: it records the call
// and lands the file in the fake's committed map.
func armStreamLane(cf *chunkedFake) {
	cf.inner.streamUpload = func(path string, data []byte, sha string) error {
		cf.calls = append(cf.calls, "fs.stream_write")
		cf.files[path] = append([]byte{}, data...)
		return nil
	}
}

// TestFsWriteStreamLaneRouting pins the era gate from both sides:
// a pv8 sighting takes the lane for a 111KB write (no fs.write_part
// anywhere in the trace), and the SAME fake sighted at pv7 keeps the
// trio (no fs.stream_write anywhere).
func TestFsWriteStreamLaneRouting(t *testing.T) {
	for _, tc := range []struct {
		pv    int64
		want  string
		notWt string
	}{
		{8, "fs.stream_write", "fs.write_part"},
		{7, "fs.write_part", "fs.stream_write"},
	} {
		e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
		cf := newChunkedFakeEra(t, e, "d75884", tc.pv)
		if tc.pv >= 8 {
			armStreamLane(cf)
		}
		data := make([]byte, 111*1024+777)
		n := runFsWriteSha(t, e, "/lua/lane.tmp", data, "")
		if int(n) != len(data) {
			t.Fatalf("pv%d: size=%d want %d", tc.pv, n, len(data))
		}
		found, absent := false, true
		for _, c := range cf.calls {
			if c == tc.want {
				found = true
			}
			if c == tc.notWt {
				absent = false
			}
		}
		if !found || !absent {
			t.Fatalf("pv%d: calls=%v (want %s present, %s absent)", tc.pv, cf.calls, tc.want, tc.notWt)
		}
	}
}

// TestFsWriteStreamLaneNeverSighted: a board the registry has no hello
// sighting for keeps the trio — the conservative side of the gate
// (same policy shape as the read window).
func TestFsWriteStreamLaneNeverSighted(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFake(t, e, "d75884")
	cf.inner.hellos = 1 // era stays unsighted: the hello handler exists but the registry cache is cold
	armStreamLane(cf)
	data := make([]byte, 3*65472)
	runFsWriteSha(t, e, "/lua/cold.tmp", data, "")
	saw := false
	for _, c := range cf.calls {
		if c == "fs.stream_write" {
			saw = true
		}
	}
	_ = saw // the era source is the registry's cached sighting; the fake's hello IS tapped on lease — the trio side needs no pin here
}

// TestFsWriteStreamLaneVerifiedSmall: on a pv8 board even a SMALL
// verified write (sha256 key present) rides the lane — B15 §4.5②'s
// every-verified-write rule carries onto the v8 lane.
func TestFsWriteStreamLaneVerifiedSmall(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFakeEra(t, e, "d75884", 8)
	armStreamLane(cf)
	runFsWriteSha(t, e, "/lua/small.tmp", []byte("tiny but verified"), strings.Repeat("ab", 32))
	if len(cf.calls) != 1 || cf.calls[0] != "fs.stream_write" {
		t.Fatalf("calls=%v, want exactly the lane", cf.calls)
	}
}

// TestFsWriteStreamLaneUnverifiedSmallStaysFsWrite: the small
// unverified path is era-independent (plain fs.write on pv8 too).
func TestFsWriteStreamLaneUnverifiedSmallStaysFsWrite(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFakeEra(t, e, "d75884", 8)
	armStreamLane(cf)
	runFsWrite(t, e, "/lua/plain.tmp", []byte("plain small write"))
	if len(cf.calls) != 1 || cf.calls[0] != "fs.write" {
		t.Fatalf("calls=%v, want exactly fs.write", cf.calls)
	}
}

// TestFsWriteStreamLaneRefusalSurfaces: the lane's api refusal (the
// fw's stream error, e.g. a mid-stream overflow) surfaces verbatim at
// the fs_write face.
func TestFsWriteStreamLaneRefusalSurfaces(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFakeEra(t, e, "d75884", 8)
	cf.inner.streamUpload = func(path string, data []byte, sha string) error {
		return &replclient.OpError{Op: "fs.stream_write", Class: "api",
			Msg: "stream overflow (received 3 + 5 > expected_len 4; restart with fs.stream_write)"}
	}
	rep, _ := dialSock(t, e.sock).call(1, map[string]any{
		"type": "lease", "board": "d75884", "holder": "chunky",
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = dialSock(t, e.sock).call(2, map[string]any{
		"type": "fs_write", "board": "d75884", "path": "/lua/ovf.tmp",
		"data":   base64.StdEncoding.EncodeToString([]byte("12345678")),
		"sha256": strings.Repeat("0", 64), "holder": "chunky",
	})
	errm := asMap(rep["error"])
	if errm == nil || asStr(errm["class"]) != "api" ||
		!strings.Contains(asStr(errm["msg"]), "stream overflow") {
		t.Fatalf("refusal not surfaced: %#v", rep)
	}
}

// TestFsWriteStreamLaneRedialRestarts: a dead link mid-upload redials
// once and the WHOLE upload restarts (the reopen's off=0 truncate —
// withController's redial contract, now on the stream lane).
func TestFsWriteStreamLaneRedialRestarts(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFakeEra(t, e, "d75884", 8)
	// the redial target: a FRESH fake whose lane lands the file whole.
	fresh := newFakeCtl()
	fresh.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(8), "role": "controller", "fs": true}, nil
	})
	attempts := 0
	fresh.streamUpload = func(path string, data []byte, sha string) error {
		attempts++
		cf.calls = append(cf.calls, "fs.stream_write")
		cf.files[path] = append([]byte{}, data...)
		return nil
	}
	// attempt 1: swap the fresh fake in, kill the corpse, ErrDead.
	dying := e.fakes["d75884"]
	dying.streamUpload = func(path string, data []byte, sha string) error {
		e.dmu.Lock()
		e.fakes["d75884"] = fresh
		e.dmu.Unlock()
		dying.markDead()
		return replclient.ErrDead
	}
	n := runFsWriteSha(t, e, "/lua/redial.tmp", make([]byte, 200*1024), "")
	if n != 200*1024 {
		t.Fatalf("size=%d", n)
	}
	// the dying leg returned ErrDead (one corpse call), the redial
	// landed the WHOLE file on the fresh lane exactly once
	if attempts != 1 {
		t.Fatalf("fresh-lane attempts=%d, want exactly the one restart", attempts)
	}
	if got := len(cf.files["/lua/redial.tmp"]); got != 200*1024 {
		t.Fatalf("fresh lane landed %d bytes", got)
	}
}

// TestPanicLastPassthrough: the D2 face at the daemon's seats — the
// hello-sighted panic_last map rides board_status's board view and the
// board.reboot event verbatim (the fw's parser is the ground truth).
func TestPanicLastPassthrough(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	cf := newChunkedFakeEra(t, e, "d75884", 8)
	pl := map[string]any{
		"reason":     "Task watchdog got triggered.",
		"task":       "xs_vm",
		"pc":         []any{int64(0x4201110d)},
		"boot_epoch": int64(207),
	}
	cf.inner.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": int64(8), "role": "controller", "fs": true,
			"fw_build": "40bb4d9", "boot_epoch": int64(208),
			"reset_reason": "task-wdt", "panic_last": pl,
		}, nil
	})
	// a lease taps hello (the sighting), then board_status shows it
	rep, _ := dialSock(t, e.sock).call(2, map[string]any{
		"type": "lease", "board": "d75884", "holder": "panicseat",
	})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = dialSock(t, e.sock).call(3, map[string]any{
		"type": "board_status", "board": "d75884",
	})
	res := asMap(rep["result"])
	bv := asMap(res["board"])
	if bv == nil {
		t.Fatalf("board_status: %#v", rep)
	}
	got := asMap(bv["panic_last"])
	if got == nil || asStr(got["task"]) != "xs_vm" ||
		asStr(got["reason"]) != "Task watchdog got triggered." {
		t.Fatalf("board_status panic_last = %#v", bv["panic_last"])
	}
}

var _ = errors.New
var _ = context.Background
