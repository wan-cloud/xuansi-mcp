package server

// stream_test.go — B16-T6 C1/D1 server legs: the stream_read request
// (arm on first sight, drain, lease-gated) + the release/teardown
// disarm + the boot-banner capture pipeline (capture → epoch
// arbitration → durable receipt → the boot_receipts view).

import (
	"context"
	"strings"
	"testing"
	"time"

	"xuansi/internal/replclient"
)

func leaseForStream(t *testing.T, w *sockClient) {
	t.Helper()
	rep, _ := w.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "st"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
}

// streamFake scripts a v8 board's stream face on the fake controller.
func streamFake(f *fakeCtl, frames []any, next, gap int64) {
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": int64(8), "role": "controller", "fw": "xuansi-fw v0.1.0",
			"fw_build": "fb93acb", "fs": true, "chip": "ESP32-S3",
			"boot_epoch": int64(7), "reset_reason": "usb",
		}, nil
	})
	f.on("stream.arm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"ch": args["ch"]}, nil
	})
	f.on("stream.read", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"frames": frames, "next": next, "gap": gap}, nil
	})
	f.on("stream.disarm", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"ch": args["ch"]}, nil
	})
}

// TestStreamReadArmsOnFirstSightThenJustDrains: the first stream_read
// arms the channel (+ subscribes the stream source once); the second
// read drains with NO second arm — and the reply's frames decode the
// §16 sub-headers open (ch/seq/idx/total split + base64 payload).
func TestStreamReadArmsOnFirstSightThenJustDrains(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	body := []byte{254, 0, 0, 0, 9, 0, 0, 0, 2, 1, 2, 3, 4} // ch254 seq9 idx0 total2 + 4B payload
	streamFake(e.fakes["d75884"], []any{body}, 10, 0)
	leaseForStream(t, w)

	rep, _ := w.call(2, map[string]any{"type": "stream_read", "board": "d75884", "ch": 254, "holder": "st"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("stream_read#1: %#v", rep)
	}
	res := asMap(rep["result"])
	if res["ch"] != float64(254) || res["next"] != float64(10) {
		t.Fatalf("stream_read#1 result: %#v", res)
	}
	frames, ok := res["frames"].([]any)
	if !ok || len(frames) != 1 {
		t.Fatalf("frames: %#v", res["frames"])
	}
	fm := asMap(frames[0])
	if fm["ch"] != float64(254) || fm["seq"] != float64(9) || fm["total"] != float64(2) {
		t.Fatalf("decoded frame: %#v", fm)
	}
	if fm["data_b64"] != "AQIDBA==" {
		t.Fatalf("data_b64: %#v", fm["data_b64"])
	}

	rep, _ = w.call(3, map[string]any{"type": "stream_read", "board": "d75884", "ch": 254, "holder": "st"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("stream_read#2: %#v", rep)
	}

	// Exactly ONE arm + ONE source subscribe across both reads.
	var arms, subs int
	for _, c := range e.fakes["d75884"].got() {
		if c.op == "stream.arm" {
			arms++
		}
		if c.op == "events.subscribe" && c.args["source"] == "stream" {
			subs++
		}
	}
	if arms != 1 || subs != 1 {
		t.Fatalf("arm=%d subscribe=%d (want 1/1)", arms, subs)
	}
}

// TestStreamReadRefusesPreV8: a board whose last hello sighting is an
// older protocol era gets the honest feature-gate refusal — never an
// unknown-op board error.
func TestStreamReadRefusesPreV8(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	streamFake(e.fakes["d75884"], nil, 0, 0)
	e.fakes["d75884"].on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(7), "role": "controller", "fw": "x", "fw_build": "b", "fs": true}, nil
	})
	leaseForStream(t, w)
	rep, _ := w.call(2, map[string]any{"type": "stream_read", "board": "d75884", "ch": 254, "holder": "st"})
	errm := asMap(rep["error"])
	if errm == nil {
		t.Fatalf("pre-v8 stream_read must refuse: %#v", rep)
	}
	if errm["class"] != "server" {
		t.Fatalf("refusal class: %#v", errm)
	}
}

// TestStreamReadChDomain: the channel domain is refused client-side
// (no board contact needed).
func TestStreamReadChDomain(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	leaseForStream(t, w)
	for _, ch := range []int{0, 255} {
		rep, _ := w.call(2, map[string]any{"type": "stream_read", "board": "d75884", "ch": ch, "holder": "st"})
		if asMap(rep["error"]) == nil {
			t.Fatalf("ch %d must refuse: %#v", ch, rep)
		}
	}
}

// TestReleaseDisarmsStream: release ends the holder's stream intent —
// the armed channel is disarmed and the source unsubscribed on the
// board.
func TestReleaseDisarmsStream(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	streamFake(e.fakes["d75884"], nil, 0, 0)
	leaseForStream(t, w)
	rep, _ := w.call(2, map[string]any{"type": "stream_read", "board": "d75884", "ch": 254, "holder": "st"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("stream_read: %#v", rep)
	}
	rep, _ = w.call(3, map[string]any{"type": "release", "board": "d75884", "holder": "st"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("release: %#v", rep)
	}
	var disarms, unsub int
	for _, c := range e.fakes["d75884"].got() {
		if c.op == "stream.disarm" {
			disarms++
		}
		if c.op == "events.unsubscribe" && c.args["source"] == "stream" {
			unsub++
		}
	}
	if disarms != 1 || unsub != 1 {
		t.Fatalf("disarm=%d unsubscribe=%d (want 1/1)", disarms, unsub)
	}
}

// TestStreamEventRidesTheEventsFace: a `stream` notification pushed on
// the controller's tap reaches the watchers' event pushes (the
// de-polling wake-up for stream_read consumers).
func TestStreamEventRidesTheEventsFace(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	w := dialSock(t, e.sock)
	rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("watch_events: %#v", rep)
	}
	e.fakes["d75884"].push("stream", map[string]any{"ch": int64(254), "pending": int64(3), "gap": int64(0)})
	ev := w.recv()
	if asStr(ev["type"]) != "event" || asStr(ev["board"]) != "d75884" {
		t.Fatalf("event push = %#v", ev)
	}
	inner := asMap(ev["event"])
	if asStr(inner["source"]) != "stream" {
		t.Fatalf("event = %#v", inner)
	}
	d := asMap(inner["data"])
	if d["ch"] != float64(254) || d["pending"] != float64(3) {
		t.Fatalf("stream notification data = %#v", d)
	}
}

// TestBootBannerCaptureToReceipt: the D1 pipeline end-to-end at the
// unit seam — the CDC capture closure stashes the banner, the tap's
// hello resolves the epoch arbitration (+1 confirms), the receipt lands
// durably, and the boot_receipts view exposes the deterministic
// columns + the chip cross-check.
func TestBootBannerCaptureToReceipt(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	f := e.fakes["d75884"]
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": int64(8), "role": "controller", "fw": "xuansi-fw v0.1.0",
			"fw_build": "fb93acb", "fs": true, "chip": "ESP32-S3",
			"boot_epoch": int64(151), "reset_reason": "usb",
		}, nil
	})
	db := e.srv.supervisor(mkBoard("d75884", "xuansi-d75884.local"))
	prev := &bootSight{epoch: 150, fwBuild: "old", at: time.Now().Add(-time.Minute)}

	// The capture fires mid-dial; the tap's hello resolves it (the real
	// order: noteBootSighting first — it sets the chip/protoVer sighting
	// the receipt view's cross-checks read — then the banner).
	e.srv.bootCaptured("d75884")([]byte(realBannerBoard1))
	h, err := f.Hello(false, "xuansi-daemon")
	if err != nil {
		t.Fatal(err)
	}
	e.srv.noteBootSighting("d75884", db, h)
	e.srv.resolvePendingBanner("d75884", db, prev, h)

	recs := e.srv.bootRecords().Receipts("d75884")
	if len(recs) != 1 {
		t.Fatalf("receipts: %d", len(recs))
	}
	r := recs[0]
	if r.Kind != "banner" || r.Tier != "T0" || r.Arbiter != "confirmed" {
		t.Fatalf("receipt verdict: %+v", r)
	}
	if r.ResetRaw != "0x15" || r.RomVer != "20210327" || r.ChipRev != "0.2" {
		t.Fatalf("receipt columns: %+v", r)
	}

	// The view: chip_match true (esp32s3 ↔ ESP32-S3); first capture
	// carries NO diff/baseline (nothing compared against — the triad's
	// honest negative), the top-level skeleton is the adopted baseline.
	w := dialSock(t, e.sock)
	rep, _ := w.call(2, map[string]any{"type": "boot_receipts", "board": "d75884"})
	res := asMap(rep["result"])
	if res["board"] != "d75884" {
		t.Fatalf("boot_receipts: %#v", res)
	}
	list, ok := res["receipts"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("receipts view: %#v", res["receipts"])
	}
	m := asMap(list[0])
	if m["reset_raw"] != "0x15" || m["arbiter"] != "confirmed" || m["chip_match"] != true {
		t.Fatalf("receipt view entry: %#v", m)
	}
	if _, has := m["diff"]; has {
		t.Fatalf("first capture must not carry a diff: %#v", m)
	}
	if _, has := m["baseline"]; has {
		t.Fatalf("first capture must not carry a baseline: %#v", m)
	}
	if sk, ok := res["skeleton"].([]any); !ok || len(sk) == 0 {
		t.Fatalf("view must expose the adopted skeleton text: %#v", res["skeleton"])
	}

	// The fix-round positive (Important-1): a DRIFTED second capture —
	// the seat sees the machine diff lines and the compared-against
	// baseline verbatim (the triad's middle and baseline legs at the
	// seat, not just their counts).
	drifted := strings.Replace(realBannerBoard1,
		"I (25) boot: Multicore bootloader\r\n",
		"I (25) boot: Single core bootloader\r\n", 1) +
		"I (99) boot: brand new boot line\r\n"
	e.srv.bootCaptured("d75884")([]byte(drifted))
	h2, _ := f.Hello(false, "xuansi-daemon")
	e.srv.noteBootSighting("d75884", db, h2)
	prev2 := &bootSight{epoch: h.BootEpoch, at: time.Now()}
	e.srv.resolvePendingBanner("d75884", db, prev2, h2)

	rep, _ = w.call(3, map[string]any{"type": "boot_receipts", "board": "d75884"})
	res = asMap(rep["result"])
	list, _ = res["receipts"].([]any)
	if len(list) != 2 {
		t.Fatalf("receipts after drift: %#v", list)
	}
	m2 := asMap(list[1])
	diff, ok := m2["diff"].([]any)
	if !ok || len(diff) == 0 {
		t.Fatalf("drifted receipt must expose diff lines at the seat: %#v", m2)
	}
	found := false
	for _, el := range diff {
		if s, ok := el.(string); ok && strings.HasPrefix(s, "- I (<R>) boot: Multicore bootloader") {
			found = true
		}
	}
	if !found {
		t.Errorf("diff must name the replaced line:\n%v", diff)
	}
	base, ok := m2["baseline"].([]any)
	if !ok || len(base) == 0 {
		t.Fatalf("drifted receipt must expose the baseline text: %#v", m2)
	}
	found = false
	for _, el := range base {
		if s, ok := el.(string); ok && s == "I (<R>) boot: Multicore bootloader" {
			found = true
		}
	}
	if !found {
		t.Errorf("baseline must carry the previous skeleton's old line:\n%v", base)
	}
}

// TestBootReceiptsSkeletonLinesBoundedView: T6-① — the view's
// skeleton_lines and skeleton are ONE view. A >128-line skeleton
// (off-contract input: a capture whose normalized form exceeds the
// persistence bound; real skeletons run 40-60 lines) must report the
// TRUNCATED count beside the truncated text, never the full count
// beside a cut text.
func TestBootReceiptsSkeletonLinesBoundedView(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	f := e.fakes["d75884"]
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(8), "role": "controller",
			"fw": "x", "fw_build": "b", "fs": true, "boot_epoch": int64(151)}, nil
	})
	db := e.srv.supervisor(mkBoard("d75884", "xuansi-d75884.local"))

	// An anchored capture with >baselineBound distinct skeleton lines:
	// T0 banner verdict → the capture's normalized form IS adopted as
	// the baseline, unbounded at adoption (the bound is a view/persist
	// leg). Distinct letter runs survive digit fuzzing unchanged.
	var sb strings.Builder
	sb.WriteString("ESP-ROM:esp32s3-20210327\r\n")
	for i := 0; i < baselineBound+40; i++ {
		sb.WriteString("I (xx) boot: filler ")
		sb.WriteString(strings.Repeat("x", i/26+1))
		sb.WriteByte(byte('a' + i%26))
		sb.WriteString("\r\n")
	}
	e.srv.bootCaptured("d75884")([]byte(sb.String()))
	h, _ := f.Hello(false, "xuansi-daemon")
	e.srv.resolvePendingBanner("d75884", db, nil, h) // first sighting: baseline arbiter

	if sk := e.srv.bootRecords().Skeleton("d75884"); len(sk) <= baselineBound {
		t.Fatalf("fixture sanity: adopted skeleton = %d lines, want > %d (the off-contract input)", len(sk), baselineBound)
	}

	w := dialSock(t, e.sock)
	rep, _ := w.call(2, map[string]any{"type": "boot_receipts", "board": "d75884"})
	res := asMap(rep["result"])
	lines, _ := res["skeleton_lines"].(float64)
	sk, ok := res["skeleton"].([]any)
	if !ok || len(sk) == 0 {
		t.Fatalf("view skeleton text absent: %#v", res["skeleton"])
	}
	if int(lines) != baselineBound || len(sk) != baselineBound {
		t.Fatalf("skeleton_lines/skeleton diverge on the bounded view: lines=%v skeleton=%d (both want %d)",
			res["skeleton_lines"], len(sk), baselineBound)
	}
}

// TestBootBannerCorruptionDowngradeOnFlatEpoch: the capture's "banner"
// the board's own epoch counter disowns (unchanged epoch — no reboot
// happened) downgrades honestly to corruption.
func TestBootBannerCorruptionDowngradeOnFlatEpoch(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	f := e.fakes["d75884"]
	f.on("hello", func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"protocol_ver": int64(8), "role": "controller",
			"fw": "x", "fw_build": "b", "fs": true, "boot_epoch": int64(151)}, nil
	})
	db := e.srv.supervisor(mkBoard("d75884", "xuansi-d75884.local"))
	prev := &bootSight{epoch: 151, at: time.Now().Add(-time.Minute)} // SAME epoch
	e.srv.bootCaptured("d75884")([]byte(realBannerBoard1))
	h, _ := f.Hello(false, "xuansi-daemon")
	e.srv.resolvePendingBanner("d75884", db, prev, h)
	recs := e.srv.bootRecords().Receipts("d75884")
	if len(recs) != 1 || recs[0].Arbiter != "corruption" {
		t.Fatalf("flat-epoch receipt: %+v", recs)
	}
}

var _ = context.Background
var _ = replclient.Event{}
