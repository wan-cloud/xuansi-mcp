package main

// b16t11.go — the B16 T11 board legs (the 2026-11-13 "minor都修" wave):
//
//	b16t11inject — T6-④ the deterministic corruption injector: a second
//	               plain file descriptor on the SAME CDC device feeds
//	               len==0 empty frames into the board during the
//	               daemon's drain window; the fw answers each with a
//	               NON-fatal protocol error reply (§7: empty frame =
//	               error reply, connection stays framed) which lands in
//	               the daemon's 4KB capture tail — constructed bytes
//	               that are neither ROM anchor nor skeleton match, so
//	               the classifier must say corruption (T2) while the
//	               epoch arbiter confirms the real reboot (+1). The
//	               connection itself survives: the daemon's hello
//	               still lands, the receipt books the verdict.
//	b16t11abort  — T9-② the M9 aux-abort watchdog: dual TCP
//	               connections dance the controller seat ×100 rounds
//	               (begin → yield → aux probe hits the M9 path-busy
//	               refusal → yield back → abort), counting every
//	               timeout — the single never-reproduced timeout of
//	               the T9 wave either recurs (root-cause time) or the
//	               N=100 zero-timeout account bounds it.
//	b16t11tput   — T7-④ the windowed pump's throughput leg: direct
//	               replclient FsStreamWrite over TCP, 1MiB ×3
//	               sha-verified (the fw-direct comparator shape).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"xuansi/internal/replclient"
	"xuansi/internal/server"
)

const (
	t11Board   = "d75884"
	t11Holder  = "b16-t11"
	t11USBByID = "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00"
)

// ---------------------------------------------------------------------------
// b16t11inject

func runB16T11InjectCmd() int {
	sock := os.Getenv("T11_SOCK")
	if sock == "" {
		fmt.Fprintln(os.Stderr, "b16t11inject: T11_SOCK env required (the .sh wrapper sets it)")
		return 2
	}
	return runB16T11Inject(sock)
}

// injectEmptyFrames is the injector leg: hold a plain write-only fd on
// the board's CDC device and feed len==0 empty-frame headers
// ([00 00 00 00]) through the daemon's drain window. Plain opens do
// not reboot the S3 (transition-shaped reset logic — the daemon's own
// dialer pulses RTS explicitly for exactly that reason); pre-repl-up
// writes are eaten by the fw's construction-window guard; every
// post-repl-up write draws one NON-fatal protocol error reply into the
// daemon's capture. The USB re-enumeration across the daemon's reboot
// kills the fd (EIO) — the by-id path re-resolves and the loop reopens.
func injectEmptyFrames(h *harness, phase time.Duration) (written, reopened int) {
	deadline := time.Now().Add(phase)
	writeEvery := 15 * time.Millisecond
	fd, err := os.OpenFile(t11USBByID, os.O_WRONLY, 0)
	if err != nil {
		h.note("injector: initial open: %v", err)
		return 0, 0
	}
	defer fd.Close()
	for t := range time.Tick(writeEvery) {
		if t.After(deadline) {
			return written, reopened
		}
		if _, err := fd.Write([]byte{0, 0, 0, 0}); err != nil {
			reopened++
			fd.Close()
			nfd, nerr := os.OpenFile(t11USBByID, os.O_WRONLY, 0)
			if nerr != nil {
				h.note("injector: reopen failed: %v", nerr)
				return written, reopened
			}
			fd = nfd
			continue
		}
		written++
	}
	return written, reopened
}

func runB16T11Inject(sock string) int {
	h := &harness{}
	fmt.Printf("=== b16 T11 corruption injector (board %s) — %s ===\n", t11Board, time.Now().Format(time.RFC3339))

	// the witness: board alive + the pre-injection epoch ground truth.
	wit, err := replclient.DialTCP(t6TCPEndp, replclient.DialOpts{})
	if err != nil {
		h.check(false, "witness dial "+t6TCPEndp+": "+err.Error())
		return h.finish("b16t11inject")
	}
	wh, err := wit.Hello(false, "b16t11-wit")
	if err != nil {
		wit.Close()
		h.check(false, "witness hello: "+err.Error())
		return h.finish("b16t11inject")
	}
	epoch0 := wh.BootEpoch
	h.check(wh.ProtocolVer == 8, fmt.Sprintf("witness hello: fw_build=%s protocol_ver=%d epoch=%d", wh.FwBuild, wh.ProtocolVer, epoch0))
	wit.Close()

	c := dialAPI(h, sock, "b16t11inject")
	if c == nil {
		return h.finish("b16t11inject")
	}
	defer c.close()
	_, _, werr := c.call(10*time.Second, server.Request{Type: "watch_events", Board: t11Board})
	h.check(werr == nil, fmt.Sprintf("watch_events: rerr=%v", werr))

	latest := func() map[string]any {
		res, rerr, err := c.call(15*time.Second, server.Request{Type: "boot_receipts", Board: t11Board})
		if err != nil || rerr != nil {
			return nil
		}
		rc, _ := res["receipts"].([]any)
		if len(rc) == 0 {
			return nil
		}
		m, _ := rc[len(rc)-1].(map[string]any)
		return m
	}
	usbLease := func() (map[string]any, bool) {
		res, rerr, err := c.call(60*time.Second, server.Request{
			Type: "lease", Board: t11Board, Holder: t11Holder, Transport: "usb"})
		if err != nil || rerr != nil {
			h.check(false, fmt.Sprintf("usb lease: err=%v rerr=%v", err, rerr))
			return nil, false
		}
		return res, true
	}

	// ——— S1: the baseline open — a clean banner receipt first (the
	// classifier needs an adopted skeleton for the corruption leg to
	// diverge from, and the epoch backstop needs a pre-open sighting).
	if _, ok := usbLease(); !ok {
		return h.finish("b16t11inject")
	}
	r1 := latest()
	h.check(r1 != nil && r1["kind"] == "banner",
		fmt.Sprintf("S1 baseline open: latest receipt kind=%v tier=%v arbiter=%v", r1["kind"], r1["tier"], r1["arbiter"]))
	c.call(15*time.Second, server.Request{Type: "release", Board: t11Board, Holder: t11Holder})

	// ——— S2: the injected open — the injector rides the SAME lease
	// request's reboot; timing is wall-clock from the request write
	// (open+pulse ≈0.5s, repl-up ≈3.6s, window ends ≈5.1s).
	type injRes struct{ written, reopened int }
	inj := make(chan injRes, 1)
	requestSent := time.Now()
	go func() { w, r := injectEmptyFrames(h, 5*time.Second); inj <- injRes{w, r} }()
	res, ok := usbLease()
	if !ok {
		return h.finish("b16t11inject")
	}
	// wait out the injector's tail so late writes cannot straddle the
	// drain window's end into the live frame stream.
	if delta := 5*time.Second - time.Since(requestSent); delta > 0 {
		time.Sleep(delta + 200*time.Millisecond)
	}
	ir := <-inj
	h.note("S2 injector: %d empty frames written, %d reopen(s) across the reboot", ir.written, ir.reopened)

	r2 := latest()
	okKind := r2 != nil && r2["kind"] == "corruption" && r2["tier"] == "T2"
	h.check(okKind, fmt.Sprintf("S2 injected capture verdict: kind=%v tier=%v arbiter=%v (want corruption/T2)", mget(r2, "kind"), mget(r2, "tier"), mget(r2, "arbiter")))
	h.check(r2 != nil && r2["arbiter"] == "confirmed",
		fmt.Sprintf("S2 epoch arbitration: arbiter=%v (the open's real reboot — +1 confirms; the disowning direction is unit-pinned)", mget(r2, "arbiter")))
	_, hasJunk := r2["junk_text"]
	h.check(r2 != nil && hasJunk, "S2 the T2 triad's junk_text leg present at the seat")
	c.call(15*time.Second, server.Request{Type: "release", Board: t11Board, Holder: t11Holder})

	// ——— S3: the board SURVIVED — witness re-hello, epoch advanced
	// past the injection reboot (≥ +2: baseline + injected).
	wit2, err := replclient.DialTCP(t6TCPEndp, replclient.DialOpts{})
	if err == nil {
		if wh2, herr := wit2.Hello(false, "b16t11-wit2"); herr == nil {
			h.check(wh2.BootEpoch >= epoch0+2 && wh2.ProtocolVer == 8,
				fmt.Sprintf("S3 board alive after injection: epoch %d → %d (≥+2), protocol %d", epoch0, wh2.BootEpoch, wh2.ProtocolVer))
		} else {
			h.check(false, "S3 witness2 hello: "+herr.Error())
		}
		wit2.Close()
	} else {
		h.check(false, "S3 witness2 dial: "+err.Error())
	}

	// ——— S4: the recovery open — a clean banner receipt again (the
	// injection left no lasting classifier damage).
	if res, ok = usbLease(); ok {
		r3 := latest()
		h.check(r3 != nil && r3["kind"] == "banner" && r3["arbiter"] == "confirmed",
			fmt.Sprintf("S4 recovery open: kind=%v tier=%v arbiter=%v (the clean banner re-adopted)", mget(r3, "kind"), mget(r3, "tier"), mget(r3, "arbiter")))
		c.call(15*time.Second, server.Request{Type: "release", Board: t11Board, Holder: t11Holder})
	}
	_ = res
	return h.finish("b16t11inject")
}

func mget(m map[string]any, k string) any {
	if m == nil {
		return "<nil>"
	}
	return m[k]
}

// ---------------------------------------------------------------------------
// b16t11abort

func runB16T11AbortCmd() int {
	return runB16T11Abort()
}

func runB16T11Abort() int {
	h := &harness{}
	fmt.Printf("=== b16 T11 M9 aux-abort stress ×100 (board %s over %s) — %s ===\n", t11Board, t6TCPEndp, time.Now().Format(time.RFC3339))

	ctx := context.Background()
	main, err := replclient.DialTCP(t6TCPEndp, replclient.DialOpts{Log: func(f string, a ...any) {
		h.note("[main] "+f, a...)
	}})
	if err != nil {
		h.check(false, "main dial: "+err.Error())
		return h.finish("b16t11abort")
	}
	defer main.Close()
	mh, err := main.Hello(false, "t11m9-main")
	if err != nil {
		h.check(false, "main hello: "+err.Error())
		return h.finish("b16t11abort")
	}
	epoch0 := mh.BootEpoch

	aux, err := replclient.DialTCP(t6TCPEndp, replclient.DialOpts{Log: func(f string, a ...any) {
		h.note("[aux] "+f, a...)
	}})
	if err != nil {
		h.check(false, "aux dial: "+err.Error())
		return h.finish("b16t11abort")
	}
	defer aux.Close()
	if _, err := aux.Hello(true, "t11m9-aux"); err != nil {
		h.check(false, "aux observer hello: "+err.Error())
		return h.finish("b16t11abort")
	}

	const rounds = 100
	const path = "/lua/t11m9.tmp"
	var timeouts, retakes, busyRefusals, aborted int
	roundMs := make([]float64, 0, rounds)
	to := func(err error) bool {
		return errors.Is(err, replclient.ErrOpTimeout) || errors.Is(err, context.DeadlineExceeded) ||
			strings.Contains(fmt.Sprint(err), "timeout")
	}
	t0 := time.Now()
	for i := 0; i < rounds; i++ {
		rt := time.Now()
		bad := false
		// begin on main (controller)
		if _, err := main.Op(ctx, "fs.stream_write", map[string]any{"path": path, "expected_len": 100}); err != nil {
			h.note("round %d: begin: %v", i, err)
			timeouts++
			bad = true
		}
		// yield → aux takes the seat
		if !bad {
			if _, err := main.Hello(true, "t11m9-main"); err != nil {
				h.note("round %d: main yield: %v", i, err)
				timeouts++
				bad = true
			}
		}
		if !bad {
			if _, err := aux.Hello(false, "t11m9-aux"); err != nil {
				h.note("round %d: aux take: %v", i, err)
				timeouts++
				bad = true
			}
		}
		// the M9 probe: aux's begin on the held path → path busy refusal
		if !bad {
			_, err := aux.Op(ctx, "fs.stream_write", map[string]any{"path": path, "expected_len": 6})
			var oe *replclient.OpError
			switch {
			case err == nil:
				h.note("round %d: aux begin ACCEPTED — M9 lock broken", i)
				bad = true
			case to(err):
				h.note("round %d: aux probe TIMED OUT: %v", i, err)
				timeouts++
				bad = true
			case errors.As(err, &oe) && strings.HasPrefix(oe.Msg, "path busy"):
				busyRefusals++
			default:
				h.note("round %d: aux probe unexpected: %v", i, err)
				bad = true
			}
		}
		// yield back → main retakes (the detach-race window may need a retry)
		if !bad {
			if _, err := aux.Hello(true, "t11m9-aux"); err != nil {
				h.note("round %d: aux yield: %v", i, err)
				timeouts++
				bad = true
			}
		}
		if !bad {
			took := false
			for k := 0; k < 10 && !took; k++ {
				if k > 0 {
					retakes++
				}
				hh, herr := main.Hello(false, "t11m9-main")
				if herr == nil && hh.Role == "controller" {
					took = true
				}
			}
			if !took {
				h.note("round %d: main never retook the seat", i)
				bad = true
			}
		}
		// the holder's abort frees the path
		if !bad {
			res, err := main.Op(ctx, "fs.stream_abort", map[string]any{"path": path})
			switch {
			case err == nil && res["aborted"] == true:
				aborted++
			case to(err):
				h.note("round %d: abort TIMED OUT: %v", i, err)
				timeouts++
				bad = true
			default:
				h.note("round %d: abort unexpected: %v", i, err)
				bad = true
			}
		}
		// a bad round still tries to leave the path freed for the next
		if bad {
			main.Hello(false, "t11m9-main")
			main.Op(ctx, "fs.stream_abort", map[string]any{"path": path})
			aux.Hello(true, "t11m9-aux")
		}
		roundMs = append(roundMs, float64(time.Since(rt).Milliseconds()))
	}
	dt := time.Since(t0)

	// residue + board-alive account
	main.Op(ctx, "fs.rm", map[string]any{"path": path})
	main.Op(ctx, "fs.rm", map[string]any{"path": path + ".part"})
	fh, ferr := main.Hello(false, "t11m9-main")

	h.check(timeouts == 0, fmt.Sprintf("×100 loop: %d timeout(s) across %d ops-shapes (the T9-② incident face)", timeouts, rounds*5))
	h.check(busyRefusals == rounds-aborted-timeouts && aborted+timeouts == rounds,
		fmt.Sprintf("M9 legs: %d path-busy refusals, %d holder aborts (%d rounds)", busyRefusals, aborted, rounds))
	h.check(retakes < rounds, fmt.Sprintf("seat retakes: %d retry re-hellos beyond the first (detach-race window)", retakes))
	h.check(ferr == nil && fh.BootEpoch == epoch0,
		fmt.Sprintf("board identity stable: epoch %d → %d (no reboot slipped in), hello err=%v", epoch0, numEpoch(fh), ferr))
	sort.Float64s(roundMs)
	h.note("round latency: median %.0fms p90 %.0fms max %.0fms — %d rounds in %.1fs",
		roundMs[len(roundMs)/2], roundMs[len(roundMs)*9/10], roundMs[len(roundMs)-1], rounds, dt.Seconds())
	h.check(true, fmt.Sprintf("T9-② watchdog account: N=%d, timeouts=%d (0 ⇒ not reproduced; the confidence boundary stays: a low-probability fw race is bounded, not excluded)", rounds, timeouts))
	return h.finish("b16t11abort")
}

func numEpoch(h *replclient.HelloInfo) uint32 {
	if h == nil {
		return 0
	}
	return h.BootEpoch
}

// ---------------------------------------------------------------------------
// b16t11tput

func runB16T11TputCmd() int {
	return runB16T11Tput()
}

func runB16T11Tput() int {
	h := &harness{}
	fmt.Printf("=== b16 T11 windowed-upload throughput (board %s over %s) — %s ===\n", t11Board, t6TCPEndp, time.Now().Format(time.RFC3339))

	c, err := replclient.DialTCP(t6TCPEndp, replclient.DialOpts{Log: func(f string, a ...any) {
		h.note("[conn] "+f, a...)
	}})
	if err != nil {
		h.check(false, "dial: "+err.Error())
		return h.finish("b16t11tput")
	}
	defer c.Close()
	hh, err := c.Hello(false, "b16t11-tput")
	if err != nil {
		h.check(false, "hello: "+err.Error())
		return h.finish("b16t11tput")
	}
	h.check(hh.ProtocolVer == 8, fmt.Sprintf("hello: fw_build=%s protocol_ver=%d (the v8 lane)", hh.FwBuild, hh.ProtocolVer))

	run := func(label, path string, data []byte) float64 {
		sha := sha256Hex(data)
		var times []float64
		okAll := true
		for i := 0; i < 3; i++ {
			t0 := time.Now()
			err := c.FsStreamWrite(context.Background(), path, data, sha)
			dt := time.Since(t0).Seconds()
			if err != nil {
				h.note("%s trial%d: %v", label, i, err)
				okAll = false
				continue
			}
			times = append(times, dt)
			h.check(true, fmt.Sprintf("%s trial%d: %dB sha-verified in %v (%.0fKB/s)", label, i, len(data), time.Duration(dt*float64(time.Second)), float64(len(data))/1024/dt))
		}
		if !okAll || len(times) == 0 {
			h.check(false, label+": trials failed")
			return 0
		}
		sort.Float64s(times)
		med := times[len(times)/2]
		h.check(true, fmt.Sprintf("%s: median %.2fs ≈ %.0fKB/s (trials %v)", label, med, float64(len(data))/1024/med, times))
		return float64(len(data)) / 1024 / med
	}

	mib := make([]byte, 1024*1024)
	for i := range mib {
		mib[i] = byte(i*29 + 7)
	}
	kb700 := mib[:700*1024]

	r1 := run("1MiB", "/lua/t11-1m.tmp", mib)
	r2 := run("700KB", "/lua/t11-700k.tmp", kb700)

	// read-back byte account on the 1MiB artifact (fs.read_part walk).
	got := make([]byte, 0, len(mib))
	for off := uint32(0); int(off) < len(mib); {
		res, err := c.Op(context.Background(), "fs.read_part", map[string]any{"path": "/lua/t11-1m.tmp", "off": off, "len": uint32(32640)})
		if err != nil {
			h.check(false, fmt.Sprintf("read-back @%d: %v", off, err))
			break
		}
		d, _ := res["data"].([]byte)
		got = append(got, d...)
		off += uint32(len(d))
		if eof, _ := res["eof"].(bool); eof {
			break
		}
	}
	if len(got) == len(mib) {
		h.check(sha256Hex(got) == sha256Hex(mib), fmt.Sprintf("read-back byte account: %dB sha ok", len(got)))
	}

	c.Op(context.Background(), "fs.rm", map[string]any{"path": "/lua/t11-1m.tmp"})
	c.Op(context.Background(), "fs.rm", map[string]any{"path": "/lua/t11-700k.tmp"})

	h.check(r1 > 100, fmt.Sprintf("the window carries: 1MiB %.0fKB/s vs the 84-97 strict baseline (fw-direct comparator 154-158)", r1))
	_ = r2
	return h.finish("b16t11tput")
}
