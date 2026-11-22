package server

// ota_gate_test.go — the OTA push's lease gate (⑨/D1, owner ruling
// 2026-11-23: the pusher must hold the board's lease): the five gate
// truths through the daemon socket — no lease refuses with the
// take-the-lease coaching, another holder's lease refuses with the
// ErrHeld text (force included), the pusher's own live lease
// authorizes (the unforced push lands, and the own lease never
// appears in the occupants disclosure), a same-holder grace lease is
// adopted then pushed (the ensureLiveLease mirror), and force NEVER
// bypasses the gate.

import (
	"strings"
	"testing"
	"time"

	"xuansi/internal/replclient"
)

// scriptOtaClean arms the no-occupants world: no running scripts, no
// live LA captures — the unforced push's only remaining gate is the
// lease.
func scriptOtaClean(f *fakeCtl) {
	f.on("script.list", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"out": "scripts (0):"}, nil
	})
	f.on("la.info", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"caps": []any{}}, nil
	})
}

// armGatePushTail scripts the healthy push machine (transport pinning
// the file sha, activate receipt, confirm — the post-reboot hello comes
// from the env's scriptOtaHello): the gate tests' shared "the push
// would land" world.
func armGatePushTail(f *fakeCtl, elf, fileSHA string) {
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
}

// TestOtaPushRefusedWithoutLease: a holder-less connection, no lease
// anywhere — the row refuses with the take-the-lease coaching (the
// 2026-11-23 ruling) and nothing moves.
func TestOtaPushRefusedWithoutLease(t *testing.T) {
	const mac = "97c1ac"
	path, elf, fileSHA := mkOtaBin(t, "0.7.0", strings.Repeat("cc", 32))
	e := newOtaEnv(t, mac, "bench-gate1.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_0", "valid", elf) // the post-activate hello reports the NEW build
		scriptOtaIdle(f, strings.Repeat("aa", 32))   // running the OLD build
		scriptOtaClean(f)
		armGatePushTail(f, elf, fileSHA) // the world would let the push land — only the gate can stop it
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
	if asStr(r["outcome"]) != "refused" {
		t.Fatalf("outcome = %v, want refused (%#v)", r["outcome"], r)
	}
	detail := asStr(r["detail"])
	if !strings.Contains(detail, "take the lease first") || !strings.Contains(detail, "2026-11-23 ruling") {
		t.Fatalf("detail = %q, want the take-the-lease coaching (the 2026-11-23 ruling)", detail)
	}
	for _, call := range e.fakeOf(mac).got() {
		if call.op == "ota.begin+0x02" || call.op == "ota.activate" {
			t.Fatalf("a lease-refused push crossed %s — zero board motion", call.op)
		}
	}
}

// TestOtaPushRefusedForOtherHolder: a live lease of ANOTHER holder —
// the row refuses with lease.ErrHeld's text (the holder named), and
// force does not take another holder's board.
func TestOtaPushRefusedForOtherHolder(t *testing.T) {
	const mac = "97c1ac"
	path, elf, fileSHA := mkOtaBin(t, "0.7.0", strings.Repeat("cc", 32))
	e := newOtaEnv(t, mac, "bench-gate2.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_0", "valid", elf)
		scriptOtaIdle(f, strings.Repeat("aa", 32))
		scriptOtaClean(f)
		armGatePushTail(f, elf, fileSHA)
	})

	cOwner := dialSock(t, e.sock)
	if rep, _ := cOwner.call(1, map[string]any{"type": "lease", "board": mac, "holder": "bench-owner"}); asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	c := dialSock(t, e.sock) // the pusher: a bare connection, no lease of its own
	for i, force := range []bool{false, true} {
		req := map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path}
		if force {
			req["force"] = true
		}
		rep, _ := c.call(int64(2+i), req)
		if rerr := asMap(rep["error"]); rerr != nil {
			t.Fatalf("ota.push(force=%v): %v", force, rerr)
		}
		rows, _ := rep["result"].(map[string]any)["rows"].([]any)
		r := asMap(rows[0])
		if asStr(r["outcome"]) != "refused" {
			t.Fatalf("force=%v: outcome = %v, want refused (%#v)", force, r["outcome"], r)
		}
		if detail := asStr(r["detail"]); !strings.Contains(detail, "held by bench-owner") {
			t.Fatalf("force=%v: detail = %q, want lease.ErrHeld's text naming the holder", force, detail)
		}
	}
	for _, call := range e.fakeOf(mac).got() {
		if call.op == "ota.begin+0x02" || call.op == "ota.activate" {
			t.Fatalf("another holder's board saw %s — the gate refuses with zero board motion", call.op)
		}
	}
}

// TestOtaPushOwnLiveLeasePushesAndOwnNotOccupant: the pusher's own
// live lease AUTHORIZES — the unforced push lands once the board is
// otherwise free, and while another occupant still refuses it the
// disclosure lists that occupant but never the pusher's own lease.
func TestOtaPushOwnLiveLeasePushesAndOwnNotOccupant(t *testing.T) {
	const mac = "97c1ac"
	path, elf, fileSHA := mkOtaBin(t, "0.7.0", strings.Repeat("cc", 32))
	e := newOtaEnv(t, mac, "bench-gate3.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_0", "valid", elf)
		scriptOtaIdle(f, strings.Repeat("aa", 32))
		scriptOtaClean(f)
		f.on("script.list", func(args map[string]any) (map[string]any, *replclient.OpError) {
			return map[string]any{"out": "scripts (1): watchdog"}, nil // a REAL occupant
		})
		armGatePushTail(f, elf, fileSHA)
	})
	f := e.fakeOf(mac)

	c := dialSock(t, e.sock)
	if rep, _ := c.call(1, map[string]any{"type": "lease", "board": mac, "holder": "pusher-1"}); asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}

	// leg 1: the script occupant refuses the unforced push — the own
	// lease is NOT in the disclosure (it is the authority, not an
	// obstruction).
	rep, _ := c.call(2, map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path})
	rows, _ := rep["result"].(map[string]any)["rows"].([]any)
	r := asMap(rows[0])
	if asStr(r["outcome"]) != "refused" {
		t.Fatalf("outcome = %v, want refused on the script occupant (%#v)", r["outcome"], r)
	}
	detail := asStr(r["detail"])
	if !strings.Contains(detail, "running scripts: watchdog") {
		t.Fatalf("detail = %q, want the script occupant listed", detail)
	}
	if strings.Contains(detail, "lease: holder") {
		t.Fatalf("detail = %q — the pusher's own lease must never appear in the occupants list", detail)
	}

	// leg 2: the occupant cleared — the SAME unforced push lands.
	f.on("script.list", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"out": "scripts (0):"}, nil
	})
	rep, _ = c.call(3, map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("ota.push: %v", rerr)
	}
	rows, _ = rep["result"].(map[string]any)["rows"].([]any)
	r = asMap(rows[0])
	if asStr(r["outcome"]) != "pushed" || !strings.Contains(asStr(r["detail"]), "triple match") {
		t.Fatalf("row = %#v, want pushed with the triple-match account", r)
	}
}

// TestOtaPushOwnGraceAdoptsThenPushes: a same-holder GRACE lease (the
// holder's connection dropped) — the gate adopts it back live and the
// push lands: wherever a same-holder eval would pass, ota.push passes
// (the ensureLiveLease mirror).
func TestOtaPushOwnGraceAdoptsThenPushes(t *testing.T) {
	const mac = "97c1ac"
	path, elf, fileSHA := mkOtaBin(t, "0.7.0", strings.Repeat("cc", 32))
	e := newOtaEnv(t, mac, "bench-gate4.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_0", "valid", elf)
		scriptOtaIdle(f, strings.Repeat("aa", 32))
		scriptOtaClean(f)
		armGatePushTail(f, elf, fileSHA)
	})

	c1 := dialSock(t, e.sock)
	if rep, _ := c1.call(1, map[string]any{"type": "lease", "board": mac, "holder": "agent-g"}); asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	c1.conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	graced := false
	for time.Now().Before(deadline) && !graced {
		for _, ls := range e.srv.Leases().Snapshot() {
			if ls.Board == mac && !ls.GraceDeadline.IsZero() {
				graced = true
			}
		}
		if !graced {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !graced {
		t.Fatalf("lease never entered grace after disconnect: %+v", e.srv.Leases().Snapshot())
	}

	c2 := dialSock(t, e.sock) // the holder returns on a new connection
	rep, _ := c2.call(2, map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path, "holder": "agent-g"})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("ota.push: %v", rerr)
	}
	rows, _ := rep["result"].(map[string]any)["rows"].([]any)
	r := asMap(rows[0])
	if asStr(r["outcome"]) != "pushed" || !strings.Contains(asStr(r["detail"]), "triple match") {
		t.Fatalf("row = %#v, want the grace lease adopted and the push landed", r)
	}
	for _, ls := range e.srv.Leases().Snapshot() {
		if ls.Board == mac && (!ls.GraceDeadline.IsZero() || ls.Holder != "agent-g") {
			t.Fatalf("lease after the push = %+v, want live agent-g (adopted)", ls)
		}
	}
}

// TestOtaPushForceDoesNotBypassLeaseGate: force answers "kill the
// occupants", never "skip the authority" — with no lease at all the
// forced push still refuses with the take-the-lease coaching.
func TestOtaPushForceDoesNotBypassLeaseGate(t *testing.T) {
	const mac = "97c1ac"
	path, elf, fileSHA := mkOtaBin(t, "0.7.0", strings.Repeat("cc", 32))
	e := newOtaEnv(t, mac, "bench-gate5.local", func(f *fakeCtl) {
		scriptOtaHello(f, 10, "ota_0", "valid", elf)
		scriptOtaIdle(f, strings.Repeat("aa", 32))
		scriptOtaClean(f)
		armGatePushTail(f, elf, fileSHA) // the machine is fully armed — only the gate can stop it
	})

	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "ota.push", "boards": []string{mac}, "path": path, "force": true, "confirm": true})
	if rerr := asMap(rep["error"]); rerr != nil {
		t.Fatalf("ota.push(force): %v", rerr)
	}
	rows, _ := rep["result"].(map[string]any)["rows"].([]any)
	r := asMap(rows[0])
	if asStr(r["outcome"]) != "refused" {
		t.Fatalf("outcome = %v, want refused — force never bypasses the lease gate (%#v)", r["outcome"], r)
	}
	if detail := asStr(r["detail"]); !strings.Contains(detail, "take the lease first") {
		t.Fatalf("detail = %q, want the take-the-lease coaching", detail)
	}
	for _, call := range e.fakeOf(mac).got() {
		if call.op == "ota.begin+0x02" || call.op == "ota.activate" || call.op == "ota.confirm" {
			t.Fatalf("a forced but leaseless push crossed %s — zero board motion", call.op)
		}
	}
}
