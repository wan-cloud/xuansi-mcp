package server

// ota_test.go — the OTA face's server-half boardless legs (OTA ops
// wave T6): ota.push through the daemon socket against a scripted fake
// controller (the no-op row, the occupant refusal + force push, the
// era gate), ota.status's verbatim crossing, the hello ota trio's
// passthrough into list_boards, and the ops-face allowlist's five new
// ota entries.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/discover"
	"xuansi/internal/replclient"
	"xuansi/internal/testutil"
)

// mkOtaBin builds a plausible image: the 0xE9 magic, a 256B app_desc
// at 0x20 (magic 0xABCD5432, version, elf sha) — ota.ParseAppDesc's
// frozen coordinates. Answers (path, elfShaHex, fileSHA).
func mkOtaBin(t *testing.T, ver, elfHex string) (string, string, string) {
	t.Helper()
	sha, err := hex.DecodeString(elfHex)
	if err != nil || len(sha) != 32 {
		t.Fatalf("fixture sha %q: %v", elfHex, err)
	}
	bin := make([]byte, 0x20+256)
	bin[0] = 0xE9
	d := bin[0x20:]
	binary.LittleEndian.PutUint32(d[0:4], 0xABCD5432)
	copy(d[0x10:0x30], ver)
	copy(d[0x90:0xB0], sha)
	p := filepath.Join(t.TempDir(), "app.bin")
	if err := os.WriteFile(p, bin, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(bin)
	return p, elfHex, hex.EncodeToString(sum[:])
}

// armOtaUpload installs the ota lane hook under the fake's lock (the
// script callback runs on whatever goroutine dials first — the startup
// audit's — so the field write must not race the request goroutine's
// locked read).
func (f *fakeCtl) armOtaUpload(fn func(data []byte, sha string) (string, error)) {
	f.mu.Lock()
	f.otaUpload = fn
	f.mu.Unlock()
}

// scriptOtaHello arms the fake's hello with the v10 era + the ota trio.
func scriptOtaHello(f *fakeCtl, proto int64, slot, imgState, otaSha string) {
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": proto, "role": "controller", "fw_build": "t6test",
			"fw_version": "0.6.0", "ota_slot": slot, "ota_img_state": imgState,
			"ota_sha": otaSha, "boot_epoch": int64(1250), "reset_reason": "poweron",
		}, nil
	})
}

// scriptOtaIdle arms the preflight ota.status with a running identity
// DISTINCT from the pushed bin (the transport legs' precondition).
func scriptOtaIdle(f *fakeCtl, runningSha string) {
	f.on("ota.status", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"state": "idle", "running_ver": "0.6.0", "running_slot": "ota_1",
			"running_sha": runningSha, "img_state": "valid",
		}, nil
	})
}

// newOtaEnv builds the env with the board's fake SCRIPTED FROM BIRTH
// (the DialTCP seam constructs it with script installed): the startup
// audit's own crossing sights the same hello face the tests assert
// with — no tap-race between the audit's default hello and the test's
// scripting (tap dedupes per controller, so the first connect's hello
// IS the sighting; making it deterministic is the helper's whole job).
// The one fake instance serves every dial of the test.
func newOtaEnv(t *testing.T, mac, host string, script func(*fakeCtl)) *env {
	t.Helper()
	e := &env{
		t:         t,
		fakes:     map[string]*fakeCtl{},
		dialed:    map[string]int{},
		usbFakes:  map[string]*fakeCtl{},
		usbDialed: map[string]int{},
	}
	boards := []discover.Board{mkBoard(mac, host)}
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) { return boards, nil }),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) { return boards, nil }),
	)
	e.sock = filepath.Join(testutil.ShortSockDir(t), "d.sock")
	srv, err := New(Opts{
		SockPath:        e.sock,
		Registry:        reg,
		GraceTTL:        10 * time.Second,
		RefreshInterval: -1,
		Log:             newTestLogf(t, "srv: "),
		BootRecordsDir:  filepath.Join(testutil.ShortSockDir(t), "bootrec"),
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: newTestLogf(t, "sup: "),
				DialTCP: func() (board.Controller, error) {
					e.dmu.Lock()
					defer e.dmu.Unlock()
					e.dialed[ref.MAC6]++
					if e.fakes[ref.MAC6] == nil {
						f := newFakeCtl()
						script(f)
						e.fakes[ref.MAC6] = f
					}
					return e.fakes[ref.MAC6], nil
				},
				DialUSB: func() (board.Controller, error) {
					return nil, fmt.Errorf("no usb fake (OTA policy: never dialed)")
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e.srv = srv
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	awaitSweep(t, srv.reg, 1)
	return e
}

// fakeOf reads the env's fake under the dial lock (the audit's dial
// goroutine may still be installing it when the test body runs).
func (e *env) fakeOf(mac6 string) *fakeCtl {
	e.dmu.Lock()
	defer e.dmu.Unlock()
	return e.fakes[mac6]
}

// TestOtaPushNoOpRow: the daemon sights the board at v10, the preflight
// reports the running image already IS the bin's build — one no-op row,
// zero transport, zero activate.
func TestOtaPushNoOpRow(t *testing.T) {
	const mac = "97c1ac"
	path, elf, _ := mkOtaBin(t, "0.6.0", strings.Repeat("bb", 32))
	e := newOtaEnv(t, mac, "bench-ota.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_1", "valid", elf)
		scriptOtaIdle(f, elf) // running == the bin
	})

	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("ota.push: %v", rerr)
	}
	rows, _ := rep["result"].(map[string]any)["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("rows = %#v, want 1", rep["result"])
	}
	r := asMap(rows[0])
	if asStr(r["outcome"]) != "no-op" || asStr(r["board"]) != mac {
		t.Fatalf("row = %#v, want %s/no-op", r, mac)
	}
	for _, call := range e.fakeOf(mac).got() {
		if call.op == "ota.begin+0x02" || call.op == "ota.activate" {
			t.Fatalf("a no-op crossed %s — nothing may move", call.op)
		}
	}
}

// TestOtaPushRefusedThenForce: an armed lease refuses the push with the
// holder listed; force pushes through (transport + activate + the
// post-reboot hello triple match).
func TestOtaPushRefusedThenForce(t *testing.T) {
	const mac = "97c1ac"
	path, elf, fileSHA := mkOtaBin(t, "0.7.0", strings.Repeat("cc", 32))
	e := newOtaEnv(t, mac, "bench-ota.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_0", "valid", elf) // the post-activate hello reports the NEW build
		scriptOtaIdle(f, strings.Repeat("aa", 32))   // running the OLD build
		f.armOtaUpload(func(data []byte, sha string) (string, error) {
			if sha != fileSHA {
				return "", &replclient.OpError{Op: "ota.begin", Class: "api", Msg: "sha pin mismatch"}
			}
			return "ota_0", nil
		})
		f.on("ota.activate", func(args map[string]any) (map[string]any, *replclient.OpError) {
			return map[string]any{"activated": true, "slot": "ota_0", "ver": "0.7.0", "sha": elf, "reboot": true}, nil
		})
		f.on("ota.confirm", func(args map[string]any) (map[string]any, *replclient.OpError) {
			return map[string]any{"confirmed": true}, nil
		})
	})
	f := e.fakeOf(mac)

	c := dialSock(t, e.sock)
	lrep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "bench-agent"})
	if asMap(lrep["error"]) != nil {
		t.Fatalf("lease: %#v", lrep)
	}
	rep, _ := c.call(2, map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("ota.push: %v", rerr)
	}
	rows, _ := rep["result"].(map[string]any)["rows"].([]any)
	r := asMap(rows[0])
	if asStr(r["outcome"]) != "refused" {
		t.Fatalf("outcome = %v, want refused (%#v)", r["outcome"], r)
	}
	if !strings.Contains(asStr(r["detail"]), "bench-agent") {
		t.Fatalf("detail = %q, want the lease holder listed", asStr(r["detail"]))
	}
	for _, call := range f.got() {
		if call.op == "ota.begin+0x02" {
			t.Fatal("the refused push must not transport")
		}
	}

	rep, _ = c.call(3, map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path, "force": true, "confirm": true})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("ota.push(force): %v", rerr)
	}
	rows, _ = rep["result"].(map[string]any)["rows"].([]any)
	r = asMap(rows[0])
	if asStr(r["outcome"]) != "pushed" {
		t.Fatalf("outcome = %v, want pushed (%#v)", r["outcome"], r)
	}
	if !strings.Contains(asStr(r["detail"]), "triple match") {
		t.Fatalf("detail = %q, want the triple-match account", asStr(r["detail"]))
	}
	from, to := asMap(r["from"]), asMap(r["to"])
	if asStr(from["ver"]) != "0.6.0" || asStr(to["ver"]) != "0.7.0" ||
		asStr(from["slot"]) != "ota_1" || asStr(to["slot"]) != "ota_0" {
		t.Fatalf("from/to = %#v → %#v", from, to)
	}
	sawUpload, sawConfirm := false, false
	for _, call := range f.got() {
		switch call.op {
		case "ota.begin+0x02":
			sawUpload = true
		case "ota.confirm":
			sawConfirm = true
		}
	}
	if !sawUpload || !sawConfirm {
		t.Fatalf("force push crossings: upload=%v confirm=%v, want both", sawUpload, sawConfirm)
	}
}

// TestOtaPushEraGate: a board sighted at protocol_ver 9 refuses — the
// ota.* face is a v10 overlay; force does not help, zero board ops.
func TestOtaPushEraGate(t *testing.T) {
	const mac = "97c1ac"
	path, _, _ := mkOtaBin(t, "0.7.0", strings.Repeat("cc", 32))
	e := newOtaEnv(t, mac, "bench-ota9.local", func(f *fakeCtl) {
		scriptOtaHello(f, 9, "", "", "") // a pre-v10 sighting, from birth
		scriptOtaIdle(f, strings.Repeat("aa", 32))
	})

	c := dialSock(t, e.sock)
	// the first crossing (any board-touching request) sights the era
	if rep, _ := c.call(1, map[string]any{"type": "ota.status", "board": mac}); asMap(rep["error"]) != nil {
		t.Fatalf("ota.status (the sighting crossing): %#v", rep)
	}
	rep, _ := c.call(2, map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path})
	rows, _ := rep["result"].(map[string]any)["rows"].([]any)
	r := asMap(rows[0])
	if asStr(r["outcome"]) != "refused" || !strings.Contains(asStr(r["detail"]), "protocol_ver=9") {
		t.Fatalf("row = %#v, want the era refusal", r)
	}
	for _, call := range e.fakeOf(mac).got() {
		if call.op == "ota.status" && len(call.args) == 0 {
			continue // the sighting crossing itself
		}
		if call.op == "ota.begin+0x02" || call.op == "ota.activate" {
			t.Fatalf("an era-gated board saw %s", call.op)
		}
	}
}

// TestOtaStatusVerbatim: the ro request crosses ota.status and returns
// the board's result verbatim plus the resolved board id — no lease.
func TestOtaStatusVerbatim(t *testing.T) {
	const mac = "97c1ac"
	e := newOtaEnv(t, mac, "bench-ota.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_1", "valid", strings.Repeat("aa", 32))
		f.on("ota.status", func(args map[string]any) (map[string]any, *replclient.OpError) {
			return map[string]any{"state": "staged", "slot": "ota_0", "staged_len": int64(1521392),
				"staged_sha": strings.Repeat("dd", 32), "running_slot": "ota_1",
				"running_ver": "0.6.0", "running_sha": strings.Repeat("aa", 32), "img_state": "valid"}, nil
		})
	})

	c := dialSock(t, e.sock)
	rep, _ := c.call(7, map[string]any{"type": "ota.status", "board": mac})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("ota.status: %v", rerr)
	}
	res := asMap(rep["result"])
	if asStr(res["board"]) != mac || asStr(res["state"]) != "staged" || asStr(res["slot"]) != "ota_0" {
		t.Fatalf("result = %#v, want the board's status verbatim + board", res)
	}
	if asInt(res["staged_len"]) != 1521392 {
		t.Fatalf("staged_len = %v", res["staged_len"])
	}
}

// TestListBoardsOtaPassthrough: the hello ota trio rides the boot-
// identity passthrough into list_boards (the boot_epoch precedent).
func TestListBoardsOtaPassthrough(t *testing.T) {
	const mac = "97c1ac"
	e := newOtaEnv(t, mac, "bench-ota.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_1", "valid", strings.Repeat("ee", 32))
		scriptOtaIdle(f, strings.Repeat("ee", 32))
	})

	c := dialSock(t, e.sock)
	// a crossing first: the tap's hello enriches the registry entry
	if rep, _ := c.call(1, map[string]any{"type": "ota.status", "board": mac}); asMap(rep["error"]) != nil {
		t.Fatalf("ota.status (the sighting crossing): %#v", rep)
	}
	rep, _ := c.call(2, map[string]any{"type": "list_boards"})
	boards, _ := asMap(rep["result"])["boards"].([]any)
	var found bool
	for _, b := range boards {
		m := asMap(b)
		if asStr(m["mac6"]) != mac {
			continue
		}
		found = true
		if asStr(m["ota_slot"]) != "ota_1" || asStr(m["ota_img_state"]) != "valid" || asStr(m["ota_sha"]) != strings.Repeat("ee", 32) {
			t.Fatalf("ota trio = %v/%v/%v, want ota_1/valid/ee…", m["ota_slot"], m["ota_img_state"], m["ota_sha"])
		}
	}
	if !found {
		t.Fatalf("board %s missing from list_boards: %#v", mac, rep["result"])
	}
}

// TestOpAllowlistOta: the five ota ops ride the op face under the lease
// gate; the allowlist renders exactly 25 with all five present.
func TestOpAllowlistOta(t *testing.T) {
	const mac = "97c1ac"
	e := newOtaEnv(t, mac, "bench-ota.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_1", "valid", strings.Repeat("aa", 32))
		f.on("ota.begin", func(args map[string]any) (map[string]any, *replclient.OpError) {
			return nil, &replclient.OpError{Class: "api", Msg: "ota busy: stream owned by another connection (ota.abort on that connection frees it)"}
		})
	})

	c := dialSock(t, e.sock)
	lrep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "h1"})
	if asMap(lrep["error"]) != nil {
		t.Fatalf("lease: %#v", lrep)
	}
	rep, _ := c.call(2, map[string]any{"type": "op", "board": mac, "holder": "h1", "op": "ota.begin",
		"args": map[string]any{"len": 1521392, "sha256": strings.Repeat("ab", 32)}})
	if rerr := asMap(rep["error"]); rerr == nil || !strings.Contains(asStr(rerr["msg"]), "another connection") {
		t.Fatalf("op ota.begin = %#v, want the board's refusal verbatim", rep)
	}

	ops := AllowlistedOps()
	if len(ops) != 25 {
		t.Fatalf("allowlist = %d ops (%v), want 25", len(ops), ops)
	}
	for _, want := range []string{"ota.begin", "ota.abort", "ota.status", "ota.activate", "ota.confirm"} {
		var hit bool
		for _, op := range ops {
			if op == want {
				hit = true
			}
		}
		if !hit {
			t.Fatalf("allowlist missing %s (%v)", want, ops)
		}
	}
}
