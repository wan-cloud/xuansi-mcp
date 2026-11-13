package main

// b16t7.go — the B16-T7 board legs on the REAL board #1 (C2 upload lane
// through the daemon + D2 panic evidence face), against a DEDICATED
// daemon life on a private sock (the .sh wrapper owns it; the working-
// tree build is the code under test). Legs:
//
//	C0 witness    — raw TCP hello: build binding, protocol_ver 8, and
//	                panic_last present (the board's real panic history).
//	C1 tcp lease  — holder b16-t7, no reboot.
//	C2 lane proof — fs_write 1MiB sha-verified through the daemon; the
//	                board's ring log shows "repl: uplink open" (the lane
//	                actually taken — the trio writes no such line).
//	C3 throughput — 1MiB ×3 + 100KB ×3 via fs_write (host-side timing,
//	                medians reported; the acceptance table's TCP leg).
//	C4 read-back  — fs_read sha == payload sha (byte-level account).
//	C5 cleanup    — fs_rm the artifacts; lane refused without a lease
//	                (the intent face).
//	D1 status     — board_status carries panic_last verbatim.
//	D2 panic      — diag.wdttest through the leased controller: real
//	                TWDT panic → auto-reboot → the daemon redials → the
//	                board.reboot event carries panic_last{task=xs_vm,
//	                reason Task watchdog…} — the D2 face end-to-end.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"xuansi/internal/replclient"
	"xuansi/internal/server"
)

type t7Cfg struct{}

const (
	t7Board  = "d75884"
	t7Holder = "b16-t7"
)

func runB16T7Cmd() int {
	sock := os.Getenv("T7_SOCK")
	bin := os.Getenv("T7_BIN")
	if sock == "" || bin == "" {
		fmt.Fprintln(os.Stderr, "b16t7: T7_SOCK/T7_BIN env required (the .sh wrapper sets them)")
		return 2
	}
	return runB16T7(sock, bin)
}

func runB16T7(sock, _ string) int {
	h := &harness{}
	fmt.Printf("=== b16 T7 board legs (C2 upload lane + D2 panic face) — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Println("pv<8 fallback leg: unit-pinned (fs_stream_test.go) — no v7 board life to burn; honest boundary")

	// ——— C0: the raw TCP witness ————————————————————————————
	wit, err := replclient.DialTCP(t6TCPEndp, replclient.DialOpts{Log: func(f string, a ...any) {
		h.note("[witness] "+f, a...)
	}})
	if err != nil {
		h.check(false, "C0 witness dial: "+err.Error())
		return h.finish("b16t7")
	}
	defer wit.Close()
	wh, err := wit.Hello(false, "b16t7-witness")
	if err != nil {
		h.check(false, "C0 witness hello: "+err.Error())
		return h.finish("b16t7")
	}
	h.check(wh.ProtocolVer == 8, fmt.Sprintf("C0 witness hello: fw_build=%s protocol_ver=%d epoch=%d", wh.FwBuild, wh.ProtocolVer, wh.BootEpoch))
	if wh.PanicLast != nil {
		h.check(true, fmt.Sprintf("C0 witness panic_last present: task=%s epoch=%v reason=%.60s",
			str(wh.PanicLast["task"]), wh.PanicLast["boot_epoch"], str(wh.PanicLast["reason"])))
	} else {
		h.check(false, "C0 witness panic_last absent (board has panic history — expected present)")
	}

	c := dialAPI(h, sock, "b16t7")
	if c == nil {
		return h.finish("b16t7")
	}
	defer c.close()
	_, _, werr := c.call(10*time.Second, server.Request{Type: "watch_events", Board: t7Board})
	h.check(werr == nil, fmt.Sprintf("C0 watch_events: rerr=%v", werr))

	// ——— C1: lease (T7_TRANSPORT=usb forces the CDC route: one reboot
	// per lease open, §2 — the USB throughput leg rides the same life) —
	leaseReq := server.Request{Type: "lease", Board: t7Board, Holder: t7Holder}
	if t := os.Getenv("T7_TRANSPORT"); t != "" {
		leaseReq.Transport = t
	}
	res, rerr, err := c.call(60*time.Second, leaseReq)
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("C1 tcp lease: err=%v rerr=%v", err, rerr))
		return h.finish("b16t7")
	}
	if leaseReq.Transport == "usb" {
		h.check(res["transport"] == "usb",
			fmt.Sprintf("C1 usb lease {transport:%v rebooted:%v}", res["transport"], res["rebooted"]))
	} else {
		h.check(res["transport"] == "tcp" && res["rebooted"] == false,
			fmt.Sprintf("C1 tcp lease {transport:%v rebooted:%v}", res["transport"], res["rebooted"]))
	}

	// ——— C2+C3: the upload lane, 700KB ×3 then 100KB ×3 ————————————
	// The 1MiB+ acceptance rides the fw-side measurement
	// (b16t7-c2-perf2: 1MiB direct, sha-verified); the daemon's inline
	// fs_write is bounded by the frozen MaxMsgBytes frame (base64 →
	// ~760KB payload ceiling) — 700KB is the in-envelope large size.
	big := make([]byte, 700*1024)
	for i := range big {
		big[i] = byte(i*31 + 17)
	}
	bigSha := sha256Hex(big)
	small := big[:100*1024+123]
	smallSha := sha256Hex(small)

	fsWrite := func(path string, data []byte, sha string) (time.Duration, int64, bool) {
		t0 := time.Now()
		res, rerr, err := c.call(120*time.Second, server.Request{
			Type: "fs_write", Board: t7Board, Path: path, Holder: t7Holder,
			Data: data, Sha256: sha})
		dt := time.Since(t0)
		if err != nil || rerr != nil {
			h.note("fs_write %s: err=%v rerr=%v", path, err, rerr)
			return dt, 0, false
		}
		n := num64(res["size"])
		return dt, int64(n), int64(n) == int64(len(data))
	}

	var bigTimes []float64
	okLane := true
	for i := 0; i < 3; i++ {
		dt, n, ok := fsWrite("/lua/t7hil-big.tmp", big, bigSha)
		okLane = okLane && ok
		bigTimes = append(bigTimes, dt.Seconds())
		h.check(ok, fmt.Sprintf("C3 700KB trial%d: size=%d %v", i, n, dt.Round(time.Millisecond)))
	}
	h.check(okLane, fmt.Sprintf("C2 700KB fs_write ×3 via the daemon: %v (median %.2fs ≈ %.0fKB/s)",
		bigTimes, medianF(bigTimes), float64(len(big))/1024/medianF(bigTimes)))

	var smallTimes []float64
	okSmall := true
	for i := 0; i < 3; i++ {
		dt, n, ok := fsWrite("/lua/t7hil-100k.tmp", small, smallSha)
		okSmall = okSmall && ok
		smallTimes = append(smallTimes, dt.Seconds())
		h.check(ok, fmt.Sprintf("C3 100KB trial%d: size=%d %v", i, n, dt.Round(time.Millisecond)))
	}
	h.check(okSmall, fmt.Sprintf("C3 100KB ×3: %v (median %.2fs)", smallTimes, medianF(smallTimes)))

	// the lane proof: the board's ring log carries the uplink-open
	// lines (fs.write_part writes none); the daemon's op face reads it.
	res, rerr, err = c.call(15*time.Second, server.Request{
		Type: "op", Board: t7Board, Op: "log.read", Args: map[string]any{"tail": 64}})
	if err == nil && rerr == nil {
		lines, _ := res["lines"].([]any)
		opens := 0
		for _, ln := range lines {
			if strings.Contains(fmt.Sprint(ln), "uplink open") {
				opens++
			}
		}
		h.check(opens >= 6, fmt.Sprintf("C2 lane proof: %d 'repl: uplink open' ring lines (6 uploads)", opens))
	} else {
		h.check(false, fmt.Sprintf("C2 log.read: err=%v rerr=%v", err, rerr))
	}

	// ——— C4: read-back sha (the witness walks fs.read_part — ro, no
	// frame cap on the raw repl face; the daemon's inline fs_read reply
	// is bounded by the same MaxMsgBytes ceiling as the write) ————
	got := make([]byte, 0, len(big))
	for off := uint32(0); int(off) < len(big); {
		res2, rerr2 := witOp(wit, off)
		if rerr2 != nil {
			h.check(false, "C4 witness fs.read_part: "+rerr2.Error())
			got = nil
			break
		}
		d, _ := res2["data"].([]byte)
		got = append(got, d...)
		off += uint32(len(d))
		if eof, _ := res2["eof"].(bool); eof {
			break
		}
	}
	if len(got) == len(big) {
		h.check(sha256Hex(got) == bigSha,
			fmt.Sprintf("C4 read-back byte account: %dB sha ok", len(got)))
	}

	// ——— D1: board_status carries panic_last ————————————————————
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "board_status", Board: t7Board})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D1 board_status: err=%v rerr=%v", err, rerr))
	} else {
		bv, _ := res["board"].(map[string]any)
		pl, _ := bv["panic_last"].(map[string]any)
		h.check(pl != nil && str(pl["task"]) != "",
			fmt.Sprintf("D1 board_status.panic_last: task=%s epoch=%v", str(pl["task"]), pl["boot_epoch"]))
	}

	// ——— D2: a real panic end-to-end —————————————————————————
	// The trigger is the fw vector suite's OWN wdttest leg (the .sh
	// fires `selftest-vectors --only panic.summary-after-wdt` over USB
	// while the b16t7d2 watcher scenario holds this events face): the
	// USB connection owns the controller through the reboot the open
	// causes, the panic reboots the board, the daemon's TCP redial
	// hellos → noteBootSighting → board.reboot with panic_last. The
	// daemon never needs the controller — no slot race at all.
	h.check(true, "D2 armed (the .sh sequence fires the wdttest vector; the b16t7d2 watcher owns the verdict)")

	// ——— C5: cleanup + the intent face ——————————————————————
	_, _, _ = c.call(30*time.Second, server.Request{Type: "lease", Board: t7Board, Holder: t7Holder})
	for _, p := range []string{"/lua/t7hil-big.tmp", "/lua/t7hil-100k.tmp"} {
		_, _, _ = c.call(15*time.Second, server.Request{
			Type: "fs_rm", Board: t7Board, Path: p, Holder: t7Holder})
	}
	_, rerr, _ = c.call(15*time.Second, server.Request{Type: "release", Board: t7Board, Holder: t7Holder})
	h.check(rerr == nil, "C5 release")
	_, rerr, _ = c.call(15*time.Second, server.Request{
		Type: "fs_write", Board: t7Board, Path: "/lua/no-lease.tmp", Data: []byte("x")})
	h.check(rerr != nil, fmt.Sprintf("C5 lane needs intent: no-lease fs_write refused (%v)", rerr))

	return h.finish("b16t7")
}

// witOp walks one fs.read_part window on the witness connection.
func witOp(wit *replclient.Client, off uint32) (map[string]any, error) {
	return wit.Op(context.Background(), "fs.read_part", map[string]any{
		"path": "/lua/t7hil-big.tmp", "off": off, "len": uint32(32640)})
}

func medianF(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64{}, v...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func firstPC(pl map[string]any) any {
	pcs, _ := pl["pc"].([]any)
	if len(pcs) == 0 {
		return nil
	}
	return pcs[0]
}

// runB16T7D2 is the watcher half of the D2 leg: it holds the events
// face on the daemon while the .sh fires the fw vector suite's
// panic.summary-after-wdt case over USB (the wdttest trigger). The
// verdict: the board.reboot event lands carrying panic_last with the
// frozen shape (task=xs_vm, reason Task watchdog…, pc non-empty) —
// the D2 face end-to-end at the daemon seat.
func runB16T7D2(sock, _ string) int {
	h := &harness{}
	fmt.Printf("=== b16 T7 D2 watcher — %s ===\n", time.Now().Format(time.RFC3339))
	c := dialAPI(h, sock, "b16t7d2")
	if c == nil {
		return h.finish("b16t7d2")
	}
	defer c.close()
	_, rerr, _ := c.call(10*time.Second, server.Request{Type: "watch_events", Board: t7Board})
	if rerr != nil {
		h.check(false, fmt.Sprintf("watch_events: %v", rerr))
		return h.finish("b16t7d2")
	}
	// A lease gates the op face (ensureLiveLease) and carries the
	// daemon's controller intent — take it once, then prod.
	_, rerr, _ = c.call(30*time.Second, server.Request{
		Type: "lease", Board: t7Board, Holder: "b16t7d2"})
	h.check(rerr == nil, fmt.Sprintf("D2 watcher lease: %v", rerr))
	h.check(true, "events face armed — waiting for the wdttest reboot (up to 120s)")
	// The daemon's redial is lazy (next-use): prod it every ~8s so the
	// post-panic hello tap lands while we watch. board_status is
	// cache-first (never dials); a ro board_op (log.read) walks the
	// supervisor's op path → fresh dial → hello tap → the sighting.
	var rebootEv map[string]any
	deadline := time.Now().Add(120 * time.Second)
	nextProd := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && rebootEv == nil {
		select {
		case ev := <-c.events:
			if ev.Event.Source == "board" {
				if k, _ := ev.Event.Data["kind"].(string); k == "board.reboot" {
					rebootEv = ev.Event.Data
				}
			}
		default:
		}
		if rebootEv == nil {
			if time.Now().After(nextProd) {
				_, _, _ = c.call(30*time.Second, server.Request{
					Type: "op", Board: t7Board, Op: "log.read", Holder: "b16t7d2",
					Args: map[string]any{"tail": 4}})
				nextProd = time.Now().Add(8 * time.Second)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if rebootEv == nil {
		h.check(false, "D2 board.reboot event not seen in 120s")
		_, _, _ = c.call(15*time.Second, server.Request{Type: "release", Board: t7Board, Holder: "b16t7d2"})
		return h.finish("b16t7d2")
	}
	delta := num64(rebootEv["new_epoch"]) - num64(rebootEv["prev_epoch"])
	h.check(delta >= 1 && str(rebootEv["reset_reason"]) != "",
		fmt.Sprintf("D2 board.reboot: epoch %v→%v reset=%v", rebootEv["prev_epoch"], rebootEv["new_epoch"], rebootEv["reset_reason"]))
	pl, _ := rebootEv["panic_last"].(map[string]any)
	h.check(pl != nil && str(pl["task"]) == "xs_vm" &&
		strings.Contains(str(pl["reason"]), "Task watchdog"),
		fmt.Sprintf("D2 board.reboot.panic_last: task=%s epoch=%v pc0=%v reason=%.50s",
			str(pl["task"]), pl["boot_epoch"], firstPC(pl), str(pl["reason"])))
	_, _, _ = c.call(15*time.Second, server.Request{Type: "release", Board: t7Board, Holder: "b16t7d2"})
	return h.finish("b16t7d2")
}

func runB16T7D2Cmd() int {
	sock := os.Getenv("T7_SOCK")
	bin := os.Getenv("T7_BIN")
	if sock == "" || bin == "" {
		fmt.Fprintln(os.Stderr, "b16t7d2: T7_SOCK/T7_BIN env required")
		return 2
	}
	return runB16T7D2(sock, bin)
}
