package main

// b16t6.go — the B16-T6 board legs on the REAL board #1 (C1 stream
// subscription + D1 boot-banner receipts), against a DEDICATED daemon
// life on a private sock (the .sh wrapper owns it). The producer is
// the daemon's OWN ops face (diag.streamtest, allowlisted for this
// purpose): the board direct-delivers the synthetic frames to the
// daemon's controller connection WHILE the armed ring captures them —
// and a raw TCP witness (observer; hello is ro) carries the epoch
// bypass leg. The drained frames verify byte-exact against the frozen
// synthetic template ((seq*7+idx*13+j)&0xFF — the same formula the
// vectors pin), which IS the direct-delivery payload.
//
// One connected story:
//
//	C0 prelude    — board's TCP face reachable (raw witness hello: the
//	                build binding d75884 + protocol_ver 8 + epoch N).
//	C1 lease      — TCP lease (holder b16-t6); no reboot.
//	C2 arm        — stream_read ch=254 first sight: arms + empty drain
//	                {frames:[], next unmoved, gap:0}.
//	C3 burst      — the witness fires diag.streamtest(3×64B): the ring
//	                captures (the direct frames land on the witness —
//	                byte-exact, the §16 consumer semantics unchanged);
//	                the `stream` notification reaches the events face.
//	C4 drain      — stream_read drains the 3 frames; ch/seq/idx/total
//	                and the PAYLOAD BYTES match the witness's direct
//	                frames exactly (ring replay == direct delivery).
//	C5 gap        — the witness fires 8×8192B: the 64KB ring drops the
//	                newest — stream_read answers 7 frames + gap:1
//	                (dropped frames countable, the acceptance line).
//	C6 oversize   — the witness fires 1×65526B: not ringed (entry cap),
//	                gap:1, zero frames — the honest loss column.
//	C7 end intent — release disarms; a lease-less stream_read refuses
//	                (intent ended with the lease); re-lease re-arms on
//	                the next read (first sight again).
//	D1 usb lease  — transport=usb: ONE CDC open = ONE reboot (§2); the
//	                drain window's tail captured (BootCapture).
//	D2 receipt    — boot_receipts: ≥1 receipt, arbiter=confirmed (the
//	                open's reboot is real), kind=banner tier=T0,
//	                reset_raw non-empty, rom_ver=20210327, chip_rev
//	                0.2, chip_match=true (esp32s3 ↔ ESP32-S3).
//	D3 witness    — the raw TCP witness re-hellos: epoch N+k (the
//	                reboot the receipt claims, seen from the bypass).
//	D4 accumulate — a second usb lease adds a second receipt (every
//	                open refreshes; last-8 bounded).
//	D5 seat       — the MCP face over stdio (go-sdk client riding an
//	                `xuansi mcp` subprocess): xuansi_stream_read and
//	                xuansi_boot_receipts answer their frozen shapes at
//	                the SEAT (the seat-freeze discipline: the judgment
//	                face is what the tools actually return).
//
//	Corruption injection (brief step 10's D1 leg): no deterministic
//	on-board injector exists without inventing evidence paths — the
//	three REAL corruption shapes (TWDT-junk prefix, torn head, F1
//	glue) are unit-pinned from the bench's own history
//	(bootbanner_test.go); recorded here as the honest boundary.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/replclient"
	"xuansi/internal/server"
)

const (
	t6Board   = "d75884"
	t6Holder  = "b16-t6"
	t6TCPEndp = "10.1.51.205:7777"
	t6USBPath = "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00"
)

func runB16T6Cmd() int {
	sock := os.Getenv("T6_SOCK")
	bin := os.Getenv("T6_BIN")
	if sock == "" || bin == "" {
		fmt.Fprintln(os.Stderr, "b16t6: T6_SOCK/T6_BIN env required (the .sh wrapper sets them)")
		return 2
	}
	return runB16T6(sock, bin)
}

func runB16T6(sock, bin string) int {
	h := &harness{}
	fmt.Printf("=== b16 T6 board legs (C1 stream + D1 receipts) — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Println("corruption injection: no deterministic on-board injector — the real corruption shapes are unit-pinned (bootbanner_test.go); honest boundary")

	// ——— C0: the raw TCP witness ————————————————————————————
	wit, err := replclient.DialTCP(t6TCPEndp, replclient.DialOpts{Log: func(f string, a ...any) {
		h.note("[witness] "+f, a...)
	}})
	if err != nil {
		h.check(false, "C0 witness dial "+t6TCPEndp+": "+err.Error())
		return h.finish("b16t6")
	}
	defer wit.Close()
	wh, err := wit.Hello(false, "b16t6-witness")
	if err != nil {
		h.check(false, "C0 witness hello: "+err.Error())
		return h.finish("b16t6")
	}
	h.check(wh.FwBuild != "" && wh.ProtocolVer == 8,
		fmt.Sprintf("C0 witness hello: fw_build=%s protocol_ver=%d epoch=%d chip=%s", wh.FwBuild, wh.ProtocolVer, wh.BootEpoch, wh.Chip))
	epoch0 := wh.BootEpoch

	c := dialAPI(h, sock, "b16t6")
	if c == nil {
		return h.finish("b16t6")
	}
	defer c.close()

	// the producer rides the daemon's ops face (the lease-gated
	// controller fires diag.streamtest; direct delivery lands on the
	// daemon's own conn, the ring captures beside it).
	burst := func(chunks, size int) bool {
		_, rerr, err := c.call(30*time.Second, server.Request{
			Type: "op", Board: t6Board, Op: "diag.streamtest",
			Args: map[string]any{"chunks": chunks, "size": size}})
		if err != nil || rerr != nil {
			h.note("streamtest(%d,%d): err=%v rerr=%v", chunks, size, err, rerr)
			return false
		}
		return true
	}

	// the events face: this conn watches the board (the notification
	// leg rides the watcher pushes).
	_, _, werr := c.call(10*time.Second, server.Request{Type: "watch_events", Board: t6Board})
	h.check(werr == nil, fmt.Sprintf("C1 watch_events: rerr=%v", werr))

	// ——— C1: TCP lease (no reboot) ————————————————————————————
	res, rerr, err := c.call(30*time.Second, server.Request{Type: "lease", Board: t6Board, Holder: t6Holder})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("C1 tcp lease: err=%v rerr=%v", err, rerr))
		return h.finish("b16t6")
	}
	h.check(res["transport"] == "tcp" && res["rebooted"] == false,
		fmt.Sprintf("C1 tcp lease {transport:%v rebooted:%v}", res["transport"], res["rebooted"]))

	// ——— C2: first sight arms; the empty drain ————————————————————
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "stream_read", Board: t6Board, Ch: 254})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("C2 stream_read#1: err=%v rerr=%v", err, rerr))
		return h.finish("b16t6")
	}
	frames1, _ := res["frames"].([]any)
	h.check(len(frames1) == 0 && num64(res["gap"]) == 0,
		fmt.Sprintf("C2 first-sight arm + empty drain: frames=%d gap=%v next=%v", len(frames1), res["gap"], res["next"]))

	// ——— C3: the burst + the notification ————————————————————
	if !h.check(burst(3, 64), "C3 witness fired diag.streamtest(3,64)") {
		return h.finish("b16t6")
	}
	var notified map[string]any
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev := <-c.events:
			if ev.Event.Source == "stream" {
				notified = ev.Event.Data
			}
		default:
		}
		if notified != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.check(notified != nil && num64(notified["ch"]) == 254 && num64(notified["pending"]) >= 1,
		fmt.Sprintf("C3 stream notification on the events face: %v", notified))

	// ——— C4: drain == the direct frames, byte-exact ————————————
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "stream_read", Board: t6Board, Ch: 254})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("C4 stream_read#2: err=%v rerr=%v", err, rerr))
		return h.finish("b16t6")
	}
	frames2, _ := res["frames"].([]any)
	same := len(frames2) == 3
	var seq0 float64 = -1
	if same {
		f0, _ := frames2[0].(map[string]any)
		seq0 = num64(f0["seq"])
	}
	if same {
		for i, el := range frames2 {
			m, _ := el.(map[string]any)
			if num64(m["ch"]) != 254 || num64(m["idx"]) != float64(i) || num64(m["total"]) != 3 {
				same = false
				break
			}
			want := make([]byte, 64)
			for j := range want {
				want[j] = byte((uint32(seq0)*7 + uint32(i)*13 + uint32(j)) & 0xFF)
			}
			if fmt.Sprint(m["data_b64"]) != b64of(want) {
				same = false
				break
			}
		}
	}
	h.check(same && num64(res["gap"]) == 0,
		fmt.Sprintf("C4 ring replay == the direct-delivery template (frames=%d byte-exact, seq=%d, gap=%v)", len(frames2), int(seq0), res["gap"]))

	// ——— C5: the gap account (8×8192 into a 64KB ring) ————————————
	if !h.check(burst(8, 8192), "C5 witness fired diag.streamtest(8,8192)") {
		return h.finish("b16t6")
	}
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "stream_read", Board: t6Board, Ch: 254})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("C5 stream_read: err=%v rerr=%v", err, rerr))
		return h.finish("b16t6")
	}
	frames3, _ := res["frames"].([]any)
	h.check(len(frames3) == 7 && num64(res["gap"]) == 1,
		fmt.Sprintf("C5 ring-full drops the NEWEST + gap counts it: frames=%d gap=%v next=%v", len(frames3), res["gap"], res["next"]))

	// ——— C6: the oversize entry (9+65526 > the 65400 entry cap) ————
	if !h.check(burst(1, 65526), "C6 witness fired diag.streamtest(1,65526)") {
		return h.finish("b16t6")
	}
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "stream_read", Board: t6Board, Ch: 254})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("C6 stream_read: err=%v rerr=%v", err, rerr))
		return h.finish("b16t6")
	}
	frames4, _ := res["frames"].([]any)
	h.check(len(frames4) == 0 && num64(res["gap"]) == 1,
		fmt.Sprintf("C6 oversize not ringed, same gap account: frames=%d gap=%v", len(frames4), res["gap"]))

	// ——— C7: release ends the intent ————————————————————————
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "release", Board: t6Board, Holder: t6Holder})
	h.check(err == nil && rerr == nil, fmt.Sprintf("C7 release: err=%v rerr=%v", err, rerr))
	_, rerr, err = c.call(15*time.Second, server.Request{Type: "stream_read", Board: t6Board, Ch: 254})
	h.check(err != nil || rerr != nil, "C7 lease-less stream_read refuses (intent ended with the lease)")

	// ——— D1: the USB lease (one CDC open = one reboot) ————————————
	res, rerr, err = c.call(40*time.Second, server.Request{Type: "lease", Board: t6Board, Holder: t6Holder, Transport: "usb"})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D1 usb lease: err=%v rerr=%v", err, rerr))
		return h.finish("b16t6")
	}
	h.check(res["transport"] == "usb" && res["rebooted"] == true,
		fmt.Sprintf("D1 usb lease {transport:%v rebooted:%v} — the CDC open captured the banner", res["transport"], res["rebooted"]))

	// ——— D2: the receipt view ————————————————————————————————
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "boot_receipts", Board: t6Board})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D2 boot_receipts: err=%v rerr=%v", err, rerr))
		return h.finish("b16t6")
	}
	rc, _ := res["receipts"].([]any)
	h.check(len(rc) >= 1, fmt.Sprintf("D2 receipts recorded: %d", len(rc)))
	if len(rc) >= 1 {
		last, _ := rc[len(rc)-1].(map[string]any)
		h.check(last["kind"] == "banner" && last["tier"] == "T0" && last["arbiter"] == "confirmed",
			fmt.Sprintf("D2 latest receipt kind=%v tier=%v arbiter=%v", last["kind"], last["tier"], last["arbiter"]))
		h.check(fmt.Sprint(last["reset_raw"]) != "" && fmt.Sprint(last["rom_ver"]) == "20210327" &&
			fmt.Sprint(last["chip_rev"]) == "0.2" && last["chip_match"] == true,
			fmt.Sprintf("D2 ground-truth columns reset_raw=%v rom=%v rev=%v chip_match=%v",
				last["reset_raw"], last["rom_ver"], last["chip_rev"], last["chip_match"]))
	}

	// ——— D3: the bypass witness sees the reboot the receipt claims ————
	wit2, err := replclient.DialTCP(t6TCPEndp, replclient.DialOpts{})
	if err == nil {
		wh2, herr := wit2.Hello(false, "b16t6-witness2")
		if herr == nil {
			h.check(wh2.BootEpoch > epoch0,
				fmt.Sprintf("D3 bypass witness: boot_epoch %d → %d (the receipt's confirmed reboot, seen from TCP)", epoch0, wh2.BootEpoch))
		} else {
			h.check(false, "D3 witness2 hello: "+herr.Error())
		}
		wit2.Close()
	} else {
		h.check(false, "D3 witness2 dial: "+err.Error())
	}

	// ——— D4: every open refreshes (a second receipt) ————————————
	// release first: the D1 CDC controller is WARM (release yields the
	// §12 slot per its own contract), so the next usb lease is a FRESH
	// open = a real reboot = a second capture.
	c.call(15*time.Second, server.Request{Type: "release", Board: t6Board, Holder: t6Holder})
	res, rerr, err = c.call(40*time.Second, server.Request{Type: "lease", Board: t6Board, Holder: t6Holder, Transport: "usb"})
	h.check(err == nil && rerr == nil && res["rebooted"] == true, fmt.Sprintf("D4 second usb lease: err=%v rerr=%v", err, rerr))
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "boot_receipts", Board: t6Board})
	if err == nil && rerr == nil {
		rc2, _ := res["receipts"].([]any)
		h.check(len(rc2) >= 2, fmt.Sprintf("D4 receipts accumulate per open: %d", len(rc2)))
	} else {
		h.check(false, fmt.Sprintf("D4 boot_receipts: err=%v rerr=%v", err, rerr))
	}
	c.call(15*time.Second, server.Request{Type: "release", Board: t6Board, Holder: t6Holder})

	// ——— D5: the MCP seat over stdio ————————————————————————
	seatD5(h, bin, sock, c)
	return h.finish("b16t6")
}

// seatD5 drives a REAL MCP client (go-sdk over os.Pipe stdio) against
// an `xuansi mcp` subprocess: xuansi_stream_read + xuansi_boot_receipts
// answer at the seat with their frozen shapes.
func seatD5(h *harness, bin, sock string, c *apiConn) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	inR, inW, _ := os.Pipe()
	outR, outW, _ := os.Pipe()
	cmd := exec.Command(bin, "mcp", "--sock", sock)
	cmd.Stdin, cmd.Stdout = inR, outW
	if err := cmd.Start(); err != nil {
		h.check(false, "D5 mcp subprocess: "+err.Error())
		return
	}
	defer func() { inW.Close(); cmd.Wait() }()
	if sr, err := cmd.StderrPipe(); err == nil {
		go io.Copy(io.Discard, sr)
	}

	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b16t6-seat", Version: "1.0"}, nil)
	cs, cerr := cl.Connect(ctx, &mcpsdk.IOTransport{Reader: outR, Writer: inW}, nil)
	if cerr != nil {
		h.check(false, "D5 MCP initialize: "+cerr.Error())
		return
	}

	// the seat's stream_read: arm on first sight through the MCP face,
	// the reply shape {board, ch, frames, next, gap, armed}.
	cctx, ccancel := context.WithTimeout(ctx, 30*time.Second)
	defer ccancel()
	// lease through the seat first (the arm needs it)
	if _, err := cs.CallTool(cctx, &mcpsdk.CallToolParams{Name: "xuansi_lease",
		Arguments: map[string]any{"board": t6Board, "holder": "b16-t6-seat"}}); err != nil {
		h.check(false, "D5 seat lease: "+err.Error())
		return
	}
	r1, err := cs.CallTool(cctx, &mcpsdk.CallToolParams{Name: "xuansi_stream_read",
		Arguments: map[string]any{"board": t6Board, "ch": 254}})
	if err != nil {
		h.check(false, "D5 seat stream_read: "+err.Error())
		return
	}
	text1 := seatText(r1)
	var m1 map[string]any
	if json.Unmarshal([]byte(text1), &m1) == nil {
		h.check(m1["board"] == t6Board && num64(m1["ch"]) == 254,
			fmt.Sprintf("D5 seat xuansi_stream_read: board=%v ch=%v gap=%v armed=%v", m1["board"], m1["ch"], m1["gap"], m1["armed"]))
	} else {
		h.check(false, "D5 seat stream_read reply not JSON: "+text1[:min(120, len(text1))])
	}

	r2, err := cs.CallTool(cctx, &mcpsdk.CallToolParams{Name: "xuansi_boot_receipts",
		Arguments: map[string]any{"board": t6Board}})
	if err != nil {
		h.check(false, "D5 seat boot_receipts: "+err.Error())
		return
	}
	text2 := seatText(r2)
	var m2 map[string]any
	if json.Unmarshal([]byte(text2), &m2) == nil {
		rc, _ := m2["receipts"].([]any)
		ok := len(rc) >= 2
		if ok {
			last, _ := rc[len(rc)-1].(map[string]any)
			ok = last["arbiter"] == "confirmed" && last["reset_raw"] != ""
		}
		h.check(ok, fmt.Sprintf("D5 seat xuansi_boot_receipts: %d receipts, latest arbiter confirmed (verbatim view)", len(rc)))
	} else {
		h.check(false, "D5 seat boot_receipts reply not JSON: "+text2[:min(120, len(text2))])
	}
	cs.Close()
}

func seatText(res *mcpsdk.CallToolResult) string {
	for _, c := range res.Content {
		if tc, ok := c.(*mcpsdk.TextContent); ok {
			return tc.Text
		}
	}
	return ""
}

func num64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	}
	return -1
}

func b64of(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
