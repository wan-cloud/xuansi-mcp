package server

// reboot_test.go — B10-T8b unit face: boot-epoch sightings → the
// synthetic board.reboot event. The fw's hello boot_epoch (NVS
// per-boot counter) is the reboot arbiter: a CHANGED epoch between two
// daemon sightings = the board rebooted while we were away (panic
// auto-reboot included — cause-agnostic by design); the SAME epoch on
// a redial is a mere reconnect (§13 redial ≠ reboot). Detection rides
// tap()'s hello (server.go noteBootSighting); the event rides the
// ordinary event broadcast, source "board", kind board.reboot, into
// every watcher (the MCP events ring included — zero new tools).
//
// The board is USB-source-only so the startup audit (which taps every
// TCP-dialable board, async from New) cannot race a baseline sighting
// in before the test scripts its epoch: the FIRST sighting is the
// test's own deliberate warm touch, deterministic.

import (
	"testing"

	"xuansi/internal/discover"
	"xuansi/internal/replclient"
)

// rebootBoard is one USB-source board (audit-skipped) on the shared
// test env.
func rebootBoard() discover.Board {
	b := mkBoard("d75884", "xuansi-d75884.local")
	b.Sources = []string{discover.SourceUSB} // no TCP face to audit → no startup tap
	return b
}

// scriptHello pins one fake's hello reply (the epoch/fw/reset identity
// a sighting will observe).
func scriptHello(f *fakeCtl, epoch int64, fwBuild, reset string) {
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": int64(5), "role": "controller", "fw": "xuansi-fw v0.1.0",
			"fw_build": fwBuild, "fs": true,
			"boot_epoch": epoch, "reset": reset, "reset_reason": reset,
		}, nil
	})
}

// rebootEventData digs {kind, prev_epoch, new_epoch, reset_reason,
// gap_ms, fw_build*} out of one event push, failing with shape.
func rebootEventData(t *testing.T, ev map[string]any) map[string]any {
	t.Helper()
	if asStr(ev["type"]) != "event" || asStr(ev["board"]) != "d75884" {
		t.Fatalf("push = %#v", ev)
	}
	inner := asMap(ev["event"])
	if asStr(inner["source"]) != "board" {
		t.Fatalf("source = %#v, want board", inner)
	}
	d := asMap(inner["data"])
	if asStr(d["kind"]) != "board.reboot" {
		t.Fatalf("kind = %#v, want board.reboot", d)
	}
	return d
}

// TestBoardRebootEventOnEpochChange: first sighting = silent baseline;
// an epoch change across a redial = ONE board.reboot event with the
// full payload; same-build redial carries no fw keys.
func TestBoardRebootEventOnEpochChange(t *testing.T) {
	e := newEnv(t, nil, rebootBoard())
	scriptHello(e.fakes["d75884"], 7, "aaabbb", "usb")
	c := dialSock(t, e.sock)

	// warm touch: the tap's hello is the FIRST sighting (baseline).
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "reboot"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, evs := c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}

	w := dialSock(t, e.sock)
	if rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"}); asMap(rep["error"]) != nil {
		t.Fatalf("watch: %#v", rep)
	}
	// first sighting already happened before the watcher armed; the
	// lease connection itself must ALSO have seen nothing.
	w.recvNone()
	if len(evs) != 0 {
		t.Fatalf("first sighting emitted events: %#v", evs)
	}

	// the board reboots (epoch 7 → 8, cause "panic"): the redial's
	// hello detects it and the watcher hears board.reboot.
	fresh := newFakeCtl()
	scriptHello(fresh, 8, "aaabbb", "panic")
	e.fakes["d75884"].markDead()
	e.dmu.Lock()
	e.fakes["d75884"] = fresh
	e.dmu.Unlock()

	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "code": "return 2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("post-reboot eval (redial): %#v", rep)
	}
	d := rebootEventData(t, w.recv())
	if asInt(d["prev_epoch"]) != 7 || asInt(d["new_epoch"]) != 8 {
		t.Fatalf("epochs = %v → %v, want 7 → 8", d["prev_epoch"], d["new_epoch"])
	}
	if asStr(d["reset_reason"]) != "panic" {
		t.Fatalf("reset_reason = %#v, want panic", d["reset_reason"])
	}
	if asInt(d["gap_ms"]) < 0 {
		t.Fatalf("gap_ms = %v, want ≥ 0", d["gap_ms"])
	}
	if _, has := d["fw_build_prev"]; has {
		t.Fatalf("same-build reboot carries fw keys: %#v", d)
	}
	if asStr(d["board"]) != "d75884" {
		t.Fatalf("board = %#v", d["board"])
	}
	w.recvNone() // exactly one event
}

// TestBoardRebootEventFwChangeIncluded: a changed fw build between
// sightings adds the fw pair to the payload (an epoch change that is
// also a reflash says so).
func TestBoardRebootEventFwChangeIncluded(t *testing.T) {
	e := newEnv(t, nil, rebootBoard())
	scriptHello(e.fakes["d75884"], 3, "aaaaaaa", "power-on")
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "rebootfw"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}

	w := dialSock(t, e.sock)
	if rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"}); asMap(rep["error"]) != nil {
		t.Fatalf("watch: %#v", rep)
	}

	fresh := newFakeCtl()
	scriptHello(fresh, 4, "ccccccc", "usb") // reflash + reboot: build AND epoch changed
	e.fakes["d75884"].markDead()
	e.dmu.Lock()
	e.fakes["d75884"] = fresh
	e.dmu.Unlock()

	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "code": "return 2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("post-reflash eval: %#v", rep)
	}
	d := rebootEventData(t, w.recv())
	if asStr(d["fw_build_prev"]) != "aaaaaaa" || asStr(d["fw_build"]) != "ccccccc" {
		t.Fatalf("fw pair = %#v, want aaaaaaa → ccccccc", d)
	}
}

// TestBoardRedialSameEpochSilent: a redial whose hello reports the
// SAME epoch is a reconnect, not a reboot (§13) — no event, even for
// epoch 0 (older fw without the key: unknown ≠ changed).
func TestBoardRedialSameEpochSilent(t *testing.T) {
	e := newEnv(t, nil, rebootBoard())
	scriptHello(e.fakes["d75884"], 0, "oldfw00", "usb") // fw without boot_epoch
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "redial"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	rep, _ = c.call(2, map[string]any{"type": "eval", "board": "d75884", "code": "return 1"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("eval: %#v", rep)
	}

	w := dialSock(t, e.sock)
	if rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"}); asMap(rep["error"]) != nil {
		t.Fatalf("watch: %#v", rep)
	}

	fresh := newFakeCtl()
	scriptHello(fresh, 0, "oldfw00", "usb") // same epoch 0 on the redial
	e.fakes["d75884"].markDead()
	e.dmu.Lock()
	e.fakes["d75884"] = fresh
	e.dmu.Unlock()

	rep, _ = c.call(3, map[string]any{"type": "eval", "board": "d75884", "code": "return 2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("redial eval: %#v", rep)
	}
	w.recvNone() // same epoch = reconnect only: silent
}
