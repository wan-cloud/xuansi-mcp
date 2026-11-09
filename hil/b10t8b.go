package main

// b10t8b.go — B10 task 8b HIL: reboot visibility, panic→agent
// notification. A board reboot while the daemon is away (panic
// auto-reboot included — the fw's boot_epoch is the cause-agnostic
// arbiter) must reach the AGENT as an event: source "board", kind
// board.reboot, riding the EXISTING MCP event face (events ring +
// xuansi_events_recent — zero new tools).
//
// The real panic path (diag.wdttest) FREEZES on the IDF 5.5.5
// panic-entry stall (carry item 15) — so the reboot here is the
// CONTROLLED one: the daemon's own USB route (replclient.DialUSB)
// pulses the board down (a plain CDC open does NOT reboot the S3 —
// the USB-JTAG reset logic is transition-shaped, so DialUSB pulses
// RTS explicitly; +1 epoch per cycle, fw-measured by
// tools/boot-epoch-b10t8b.py). The daemon holds the TCP controller
// across that reboot; the next daemon touch redials, its hello sights
// the CHANGED epoch, and the event fires. Panic-path detection is the
// same epoch-change code (unit-pinned in internal/server/reboot_test.go
// with reset_reason "panic"); only the reset_reason STRING differs
// board-side, and the mapping table is fw-pinned.
//
//	suite (S0..S5):
//	  S0  prelude: clean bench, board reachable over TCP, sandboxed
//	      HEAD serve (its startup audit = the FIRST sighting, baseline
//	      epoch E1) + mcp child + initialize
//	  S1  baseline: lease + xuansi_board_op diag.stats (allowlisted,
//	      lease-only poke) → E1 straight from the board; an independent
//	      observer hello binds fw_build == canonical; the agent face
//	      asserts NO board event yet (first sight is silent)
//	  S2  the reboot: DialUSB's RTS pulse takes the board down (the
//	      ONLY CDC touch — reset_reason "usb") and the harness waits
//	      for the TCP face to come back (wifi auto-reconnect)
//	  S3  detection: xuansi_board_op diag.stats again → the daemon
//	      redials → tap hello sights epoch E1+1 → board.reboot event;
//	      the op's own reply independently shows E1+1
//	  S4  the agent face: xuansi_events_recent {board, source_prefix
//	      "board"} → exactly ONE event, payload pinned: kind
//	      board.reboot, prev_epoch=E1, new_epoch=E1+1,
//	      reset_reason="usb", gap_ms>0, no fw keys (same build)
//	  S5  cleanup: release; board state verified clean (a reboot clears
//	      board-side claims; camera untouched), observer hello
//	      re-binds the canonical build, teardown
//
// Evidence discipline: ASCII only, verdicts in-file, honest attempts.

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/replclient"
)

const (
	b10t8bBoard   = "d75884"
	b10t8bFwBuild = "dcaa9ae" // B12-P2 fullclean canonical (protocol_ver 6); HOUSE RULE: same commit as the other hil constants
	b10t8bHolder  = "b10t8b-hil"
	b10t8bTCPEndp = "xuansi-d75884.local:7777"
	b10t8bUSBByID = "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00"
)

// staleServe checks no xuansi serve process holds the bench (the b10t5
// S0 pattern: a resident serve would own the controller slot and starve
// this run's HEAD daemon).
func staleServe(h *harness) {
	out, err := exec.Command("pgrep", "-af", "xuansi").CombinedOutput()
	if err != nil {
		h.note("pgrep: no xuansi processes found (clean bench)")
		return
	}
	stale := 0
	for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Fields(ln)
		if len(f) >= 3 && strings.Contains(filepath.Base(f[1]), "xuansi") && f[2] == "serve" {
			stale++
			fmt.Printf("  [note] stale xuansi serve process: %s\n", asciiLine(ln))
		}
	}
	h.check(stale == 0, fmt.Sprintf("no stale xuansi serve daemon on the bench (found %d)", stale))
}

func runB10T8B(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== b10 task-8b mini-HIL: reboot visibility - panic->agent notification via board.reboot - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("serving daemon: %s (freshly built at HEAD by the orchestrating script)\n", daemonBin)
	fmt.Printf("board: %s over TCP %s; fw expect %s (protocol_ver 5, hello boot_epoch/reset_reason)\n", b10t8bBoard, b10t8bTCPEndp, b10t8bFwBuild)
	fmt.Printf("reboot trigger: DialUSB RTS pulse via %s (controlled - the panic path freezes on the IDF 5.5.5 panic-entry stall, carry item 15)\n", b10t8bUSBByID)

	var serve, mcp *b6Proc
	defer func() { // die-safe: never leave processes behind
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int {
		code := h.finish("b10t8b")
		if h.fail == 0 {
			fmt.Printf("=== ALL PASS - controlled reboot round-trip: the daemon sighted the epoch change and the agent heard board.reboot (attempt %d, S0..S5) ===\n", attempt)
		}
		return code
	}

	// ---------- S0: prelude ----------
	fmt.Println("\n- S0: prelude - clean bench, board reachable, sandbox, HEAD serve+mcp up -")
	staleServe(h)
	if conn, err := net.DialTimeout("tcp", b10t8bTCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v", b10t8bTCPEndp, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only - no reboot)", b10t8bTCPEndp))
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b10-t8b boards.conf: no aliases - the mac6 selector is used directly\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}

	serve = startB6Daemon(h, daemonBin, sock, boardsCfg, "XDG_DATA_HOME="+xdg)
	if serve == nil {
		return finish()
	}
	ok, last := pollUntil(300*time.Millisecond, 15*time.Second, func() (bool, string) {
		if c, err := net.Dial("unix", sock); err == nil {
			c.Close()
			return true, "sock dialable"
		}
		return false, "sock not bound yet"
	})
	h.check(ok, fmt.Sprintf("HEAD daemon sock bound at %s (%s)", sock, last))
	// the startup audit dials the board and taps it: its hello is the
	// FIRST boot sighting (baseline E1, silent by design)
	ok, last = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return serve.has("startup audit board "+b10t8bBoard), "audit line"
	})
	h.check(ok, fmt.Sprintf("startup audit tapped the board (first sighting = baseline epoch): %s", last))

	mcp, mcpR, mcpW := startB6MCP(h, daemonBin, sock, "XDG_DATA_HOME="+xdg)
	if mcp == nil {
		return finish()
	}
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b10t8b-hil", Version: "1.0"},
		&mcpsdk.ClientOptions{
			ResourceUpdatedHandler: func(_ context.Context, _ *mcpsdk.ResourceUpdatedNotificationRequest) {},
			ToolListChangedHandler: func(context.Context, *mcpsdk.ToolListChangedRequest) {},
		})
	b := &b6Client{h: h}
	cs, cerr := cl.Connect(ctx, &mcpsdk.IOTransport{Reader: mcpR, Writer: mcpW}, nil)
	if cerr != nil {
		h.check(false, "MCP client initialize over the child's stdio pipes: "+cerr.Error())
		return finish()
	}
	b.cs = cs
	si := cs.InitializeResult()
	h.check(si != nil && si.ServerInfo.Name == "xuansi",
		fmt.Sprintf("initialize handshake ok: server=%s version=%s", initField(si, "name"), initField(si, "ver")))

	// ---------- S1: baseline ----------
	fmt.Println("\n- S1: baseline - lease + diag.stats poke: epoch E1 from the board; agent face silent -")
	m, _, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b10t8bBoard, "holder": b10t8bHolder})
	h.check(!isErr && b6Str(m, "board") == b10t8bBoard,
		fmt.Sprintf("W00 lease holder=%s (default TCP route, no reboot)", b10t8bHolder))

	m, text, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t8bBoard, "op": "diag.stats"})
	// board_op's structured content IS the board's op result map (no wrapper)
	e1, hasE1 := numField(m, "boot_epoch")
	rr1 := strField(m, "reset_reason")
	h.check(!isErr && hasE1 && e1 > 0,
		fmt.Sprintf("W01 diag.stats carries boot_epoch E1=%.0f (reset_reason=%q; text=%.60s)", e1, rr1, text))

	// independent sighting: observer hello (never the controller slot)
	// binds the fw canonical and re-reads the epoch
	if c, derr := replclient.DialTCP(b10t8bTCPEndp, replclient.DialOpts{}); derr == nil {
		if hi, herr := c.Hello(true, "b10t8b-base"); herr == nil {
			h.check(hi.FwBuild == b10t8bFwBuild && hi.ProtocolVer == 5,
				fmt.Sprintf("W02 fw BUILD BINDING: observer hello fw_build=%s protocol_ver=%d (expected %s)", hi.FwBuild, hi.ProtocolVer, b10t8bFwBuild))
			h.check(float64(hi.BootEpoch) == e1,
				fmt.Sprintf("W03 two independent sightings agree: observer hello epoch=%d == diag.stats E1=%.0f", hi.BootEpoch, e1))
		} else {
			h.check(false, "baseline observer hello: "+herr.Error())
		}
		c.Close()
	} else {
		h.check(false, "baseline observer dial: "+derr.Error())
	}

	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_events_recent",
		map[string]any{"board": b10t8bBoard, "source_prefix": "board", "limit": 50})
	evsRaw, _ := m["events"].([]any)
	h.check(!isErr && len(evsRaw) == 0,
		fmt.Sprintf("W04 agent face silent at baseline: events_recent(source=board) = %d events (first sighting never fires)", len(evsRaw)))

	// ---------- S2: the controlled reboot ----------
	fmt.Println("\n- S2: the reboot - the daemon's own USB route pulses the board down; the TCP face goes away and comes back -")
	cdcRebootAt := time.Now()
	// The daemon's own DialUSB is the proven controlled-reboot trigger
	// (replclient.dialUSBLink: a plain open does NOT reboot the S3 — the
	// USB-JTAG reset logic is transition-shaped — so it pulses RTS
	// explicitly, EN pulled, GPIO0 high -> run app). We take the link
	// and close it right away: the reboot has happened by the return.
	uc, err := replclient.DialUSB(b10t8bUSBByID, replclient.DialOpts{})
	if err != nil {
		h.check(false, "USB open+RTS pulse (reboots the board): "+err.Error())
		return finish()
	}
	uc.Close()
	h.check(true, "CDC opened (RTS-pulsed) + closed - board rebooted (reset_reason will read \"usb\"; epoch +1 per cycle, fw-measured)")

	ok, last = pollUntil(1*time.Second, 90*time.Second, func() (bool, string) {
		c, derr := net.DialTimeout("tcp", b10t8bTCPEndp, 2*time.Second)
		if derr == nil {
			c.Close()
			return true, "TCP face back"
		}
		return false, "still down (wifi auto-reconnect window)"
	})
	h.check(ok, fmt.Sprintf("board TCP face came back after the reboot (%s, %.0fs)", last, time.Since(cdcRebootAt).Seconds()))

	// ---------- S3: detection ----------
	fmt.Println("\n- S3: detection - the daemon touch redials; the tap hello sights E1+1; the event fires -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t8bBoard, "op": "diag.stats"})
	e2, hasE2 := numField(m, "boot_epoch")
	rr2 := strField(m, "reset_reason")
	h.check(!isErr && hasE2 && e2 == e1+1,
		fmt.Sprintf("W05 board reports epoch E2=%.0f == E1+1 after the reboot (reset_reason=%q; this very request rode the daemon redial; text=%.40s)", e2, rr2, text))

	// ---------- S4: the agent face ----------
	fmt.Println("\n- S4: the agent face - xuansi_events_recent carries the board.reboot event -")
	var evs []map[string]any
	ok, last = pollUntil(500*time.Millisecond, 15*time.Second, func() (bool, string) {
		m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_events_recent",
			map[string]any{"board": b10t8bBoard, "source_prefix": "board", "limit": 50})
		if isErr {
			return false, "events_recent rpc"
		}
		raw, _ := m["events"].([]any)
		evs = nil
		for _, e := range raw {
			if em, ok := e.(map[string]any); ok {
				evs = append(evs, em)
			}
		}
		if len(evs) >= 1 {
			return true, fmt.Sprintf("%d board event(s)", len(evs))
		}
		return false, "no board event in the ring yet"
	})
	h.check(ok, "events_recent(source=board) holds the reboot event ("+last+")")
	if ok {
		h.check(len(evs) == 1, fmt.Sprintf("W06 exactly ONE board event (got %d; redial != reboot means no spurious extras)", len(evs)))
		if len(evs) >= 1 {
			ev := evs[len(evs)-1]
			src, _ := ev["source"].(string)
			d, _ := ev["data"].(map[string]any)
			kind := strField(d, "kind")
			prev, _ := numField(d, "prev_epoch")
			next, _ := numField(d, "new_epoch")
			gapMs, _ := numField(d, "gap_ms")
			h.check(src == "board" && kind == "board.reboot",
				fmt.Sprintf("W07 event source=%q kind=%q (source board, kind board.reboot)", src, kind))
			h.check(strField(d, "board") == b10t8bBoard,
				fmt.Sprintf("W08 payload.board=%q", strField(d, "board")))
			h.check(prev == e1 && next == e2,
				fmt.Sprintf("W09 payload epochs: prev_epoch=%.0f == E1, new_epoch=%.0f == E2 (E1+1)", prev, next))
			h.check(strField(d, "reset_reason") == "usb",
				fmt.Sprintf("W10 payload.reset_reason=%q (CDC-open reboot row of the fw mapping table)", strField(d, "reset_reason")))
			h.check(gapMs > 0 && gapMs < 600000,
				fmt.Sprintf("W11 payload.gap_ms=%.0f (last-seen -> re-detected; %.0fs elapsed since the reboot)", gapMs, time.Since(cdcRebootAt).Seconds()))
			_, hasPrevFw := d["fw_build_prev"]
			_, hasCurFw := d["fw_build"]
			h.check(!hasPrevFw && !hasCurFw, "W12 same-build reboot carries NO fw keys (the fw pair only rides a changed build)")
		}
	}

	// ---------- S5: cleanup ----------
	fmt.Println("\n- S5: cleanup - release; board state verified clean; teardown -")
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_release", map[string]any{"board": b10t8bBoard, "holder": b10t8bHolder})
	h.check(!isErr, "lease released (board-end state: the reboot cleared board-side claims; camera was never touched)")

	m, _, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_status", map[string]any{"board": b10t8bBoard})
	connected := false
	if mm, ok := m["connected"].(bool); ok {
		connected = mm
	}
	h.check(!isErr && connected, "board_status connected after the round-trip (warm link lives; scripts/pins empty post-reboot)")

	if c, derr := replclient.DialTCP(b10t8bTCPEndp, replclient.DialOpts{}); derr == nil {
		if hi, herr := c.Hello(true, "b10t8b-verify"); herr == nil {
			h.check(hi.BootEpoch == uint32(e2) && hi.FwBuild == b10t8bFwBuild && hi.ResetReason == "usb",
				fmt.Sprintf("final observer hello: boot_epoch=%d (== E2, stable) fw_build=%s reset_reason=%s (canonical binding)", hi.BootEpoch, hi.FwBuild, hi.ResetReason))
		} else {
			h.check(false, "final observer hello: "+herr.Error())
		}
		c.Close()
	} else {
		h.check(false, "final observer dial: "+derr.Error())
	}

	return finish()
}

// numField reads one JSON number field out of a decoded map.
func numField(m map[string]any, k string) (float64, bool) {
	if m == nil {
		return 0, false
	}
	f, ok := m[k].(float64)
	return f, ok
}

// strField reads one JSON string field out of a decoded map.
func strField(m map[string]any, k string) string {
	if m == nil {
		return ""
	}
	s, _ := m[k].(string)
	return s
}
