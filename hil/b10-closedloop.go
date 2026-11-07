package main

// b10-closedloop.go — B10 task 7 HIL: the eyes+hands closed loop, the
// wave's capstone. ONE continuous MCP-driven capture drives the screen
// (st7789), watches it with the camera (GC2145), judges with ledwatch,
// and confirms electrically (GPIO1->2 wired pair) — fetch from the real
// public index to a board-clean teardown, all under one lease.
//
// Two entries (the aim iteration is interactive — the coordinator relays
// JPEGs to the owner who physically adjusts between rounds):
//
//	b10t7aim  one aim round: camera-ALONE sanity (the FPC differential:
//	          no module loaded, so a failure here is sensor hardware, not
//	          coexistence), then the full module pipeline with the border
//	          pattern up — shot + ledwatch analyze twice; convergence =
//	          dominant region detectable, roughly square (240x240 panel
//	          modulo perspective), stable across 2 consecutive captures.
//	          Saves the round's JPEGs to -outdir (2026-11-08-b10-t7-aim-*
//	          in the private evidence dir) and prints an AIM_OK /
//	          AIM_ADJUST / BLOCKED-FPC verdict with the numbers.
//	b10t7     the closed loop per the brief: fetch(ledwatch+st7789 from
//	          the live index) -> push x2 -> load x2 -> st7789 init+border
//	          drawn with the camera OFF -> camera on -> geometry pairing
//	          (ledwatch largest-region bbox on the persisted border vs
//	          the -ref reference screen bbox, tolerance -tol px) ->
//	          R/B verdict (pattern red_rect isolated-region r/b ratio;
//	          double evidence: full-screen clear RED / clear BLUE ratios +
//	          the quad luma-ordering pass) -> GPIO1<->2 electrical readback
//	          x5 -> teardown board-clean (camera off + 14 pins released,
//	          modules unloaded, /drv wasm files removed, budget default,
//	          no lease). Verdict line RB_VERDICT: NORMAL|SWAPPED|AMBIGUOUS
//	          <numbers> in-file (binding). Without -ref the geometry check
//	          fails honestly (the brief's step-2 red: run before pairing
//	          converges).
//
// DRAW-THEN-WATCH (the round-1 pivot, honestly recorded): the panel
// holds its own RAM, so patterns are drawn with the camera OFF and the
// camera then watches the PERSISTED pattern — a large spi_xfer (4KB
// DMA) while the DVP path is live hits the engine's rt refusal
// "xuansi.spi_xfer: slot 0 refused: DMA buffer OOM (rc=-10)" (the first
// boot on this bench with camera+SPI both truly live; the T6-fix1
// C-group never reached it — its camera was FPC-dead). Each R/B phase
// cycles camera.off -> draw -> power shot -> analyze. The aim round
// additionally probes one redraw WITH the camera live (A6) and records
// the frame-level coexistence datum verbatim either way.
//
// Quad honesty note (found at design time, recorded in-file): the st7789
// quad pattern fills the panel with four SEAM-ADJACENT quadrants, and
// ledwatch's 4-connected CCL merges adjacent lit regions — a per-quadrant
// r_mean/b_mean through ledwatch is unreachable (any threshold that
// lights red 76 also lights its touching neighbor green 149 and they
// merge). The swap question is therefore double-covered by red_rect
// (isolated rect on black — the single lit region IS the drawn red) and
// full-screen clear(RED)/clear(BLUE); the quad pass runs at threshold
// 100 where green(149)+white(254) light and red(76)+blue(29) stay dark —
// a luma-ORDERING + layout evidence pass (swap-invariant by construction),
// with the JPEG left for the owner's eyeball.
//
// Topology (b9/b10t5 house pattern): freshly-built HEAD daemon (serve
// --sock <run>/daemon.sock, XDG_DATA_HOME sandboxed) <- its own `xuansi
// mcp` child <- this go-sdk client over the child's stdio pipes. The
// RESIDENT npm 0.1.1 daemon is stopped/restored by the orchestrating
// .sh (it holds the controller slot; the HEAD binary has driver_fetch).
// The daemon never opens CDC — that reboots the board.
//
// Evidence discipline: ASCII only, verdicts in-file, honest attempts.

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	b10t7Board   = "d75884"
	b10t7FwBuild = "fbfef59" // B10-T8b canonical (hello boot identity; HOUSE RULE: same commit as the other hil constants)
	b10t7Holder  = "b10t7-hil"
	b10t7TCPEndp = "xuansi-d75884.local:7777"

	// pinned against the live public index (wan-cloud/xuansi-drivers);
	// released version dirs are immutable upstream.
	b10t7WatchSHA   = "3454105a7233f29dd405a2de4900534672269587a4232d2f244e450f0fe54f66"
	b10t7WatchByt   = 7547
	b10t7StSHA      = "3ab5f5149f0db79953a80404fcf19685b3f3b123f2a6155eaa352acfe1ccbf73"
	b10t7StByt      = 12538
	b10t7BudgetEval = 4000 // the fw's HARD ceiling (api: eval_ms out of range 0..4000, conn-task TWDT ceiling); analyze overruns fall back to the script.start VM-task path (the vec-suite long-op convention)
	b10t7BudgetEv   = 4000
)

// b10t7ScreenPins: the st7789 face's spi0 + control-pin occupancy (T6):
// sclk 41, mosi 42, miso 14, RES 47, DC 21. The as-found check wants all
// of them (plus the camera's 14 + gpio 1/2) absent from diag.pins after
// teardown.
var b10t7ScreenPins = []string{"GPIO14", "GPIO21", "GPIO41", "GPIO42", "GPIO47"}

// b10t7Reg is one ledwatch region row.
type b10t7Reg struct {
	x, y, w, h, area float64
	luma, r, g, b    float64
}

func (r b10t7Reg) String() string {
	return fmt.Sprintf("{x=%.0f y=%.0f w=%.0f h=%.0f area=%.0f luma=%.0f rgb=(%.0f,%.0f,%.0f)}",
		r.x, r.y, r.w, r.h, r.area, r.luma, r.r, r.g, r.b)
}

// b10t7Analyze runs one ledwatch analyze with cfg and decodes regions
// (area-descending contract: index 0 is the dominant region). If the
// direct conn-task eval busts the fw's 4000ms budget ceiling (a bright
// full-screen pattern can outrun it), the call retries through the
// script.start VM-task path (the vec-suite long-op convention: the
// wasm call runs in a resident script's timer callback — not under the
// conn-task eval budget — stores the result in _G, and the cheap poll
// eval reads it back).
func b10t7Analyze(b *b6Client, ctx context.Context, cfg string) ([]b10t7Reg, string, bool) {
	code := fmt.Sprintf("return bench.wasm.call('ledwatch','analyze',{%s})", cfg)
	m, text, isErr, _ := b.call(ctx, 40*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t7Board, "code": code})
	if isErr && (strings.Contains(text, "budget") || strings.Contains(text, "out of range")) {
		b.h.note("analyze busts the conn-task budget (%.90s) - falling back to the script.start VM-task path", text)
		return b10t7AnalyzeViaScript(b, ctx, cfg)
	}
	return b10t7DecodeRegs(m, text, isErr)
}

// b10t7AnalyzeViaScript is the budget-free path: a resident script
// under the LEASE session (hScriptStart pins it there — the same
// namespace the evals ride, so _G hands the result over), one analyze
// in a self-disarming timer callback, bounded poll.
func b10t7AnalyzeViaScript(b *b6Client, ctx context.Context, cfg string) ([]b10t7Reg, string, bool) {
	b.call(ctx, 15*time.Second, "xuansi_script_stop", map[string]any{"board": b10t7Board, "name": "t7an"})
	_, text, isErr, _ := b.call(ctx, 20*time.Second, "xuansi_script_start",
		map[string]any{"board": b10t7Board, "name": "t7an",
			"code": "_G.t7an=nil\n" +
				"bench.timer_ms(50, function()\n" +
				"  if _G.t7an ~= nil then return end\n" + // self-disarm: timer_ms is periodic
				"  local ok,e = pcall(bench.wasm.call,'ledwatch','analyze',{" + cfg + "})\n" +
				"  if ok then _G.t7an=e else _G.t7an={script_err=tostring(e)} end\n" +
				"end)\n" +
				"while true do local f=coroutine.yield() if f then f() end end"})
	if isErr {
		return nil, "script start: " + text, false
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(1500 * time.Millisecond)
		m, _, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_run_lua",
			map[string]any{"board": b10t7Board, "code": "return _G.t7an"})
		if !isErr && gAny(m, "value") != nil {
			b.call(ctx, 15*time.Second, "xuansi_script_stop", map[string]any{"board": b10t7Board, "name": "t7an"})
			av, _ := m["value"].(map[string]any)
			if _, has := av["script_err"]; has {
				return nil, fmt.Sprint(av["script_err"]), false
			}
			regs, _, ok := b10t7DecodeRegs(m, "", false)
			return regs, "script-path ok", ok
		}
	}
	b.call(ctx, 15*time.Second, "xuansi_script_stop", map[string]any{"board": b10t7Board, "name": "t7an"})
	return nil, "script-path poll timeout (30s)", false
}

// b10t7DecodeRegs decodes the run_lua reply of an analyze call.
func b10t7DecodeRegs(m map[string]any, text string, isErr bool) ([]b10t7Reg, string, bool) {
	if isErr {
		return nil, text, false
	}
	av, _ := m["value"].(map[string]any)
	raw, _ := av["regions"].([]any)
	var regs []b10t7Reg
	for _, e := range raw {
		rm, _ := e.(map[string]any)
		reg := b10t7Reg{}
		reg.x, _ = b6Num(rm, "x")
		reg.y, _ = b6Num(rm, "y")
		reg.w, _ = b6Num(rm, "w")
		reg.h, _ = b6Num(rm, "h")
		reg.area, _ = b6Num(rm, "area")
		reg.luma, _ = b6Num(rm, "luma_mean")
		reg.r, _ = b6Num(rm, "r_mean")
		reg.g, _ = b6Num(rm, "g_mean")
		reg.b, _ = b6Num(rm, "b_mean")
		regs = append(regs, reg)
	}
	return regs, text, true
}

// b10t7Lua evals one Lua snippet; returns (value-as-string, ok).
func b10t7Lua(b *b6Client, ctx context.Context, code string) (string, string, bool) {
	m, text, isErr, _ := b.call(ctx, 40*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t7Board, "code": code})
	if isErr {
		return "", text, false
	}
	return fmt.Sprint(gAny(m, "value")), text, true
}

// b10t7Shot takes one camera_shot (QVGA), verifies the JPEG, and copies
// it to dst (when non-empty) as evidence. The 35s deadline (not 60)
// detects a stalled sensor FAST - the fb_get/re-init stall wedges the
// conn task for minutes and the caller must await the unwind.
func b10t7Shot(h *harness, b *b6Client, ctx context.Context, dst string) bool {
	m, text, isErr, _ := b.call(ctx, 35*time.Second, "xuansi_camera_shot",
		map[string]any{"board": b10t7Board, "framesize": "QVGA"})
	if isErr {
		h.check(false, fmt.Sprintf("camera_shot QVGA failed (text=%.140s)", text))
		return false
	}
	files, _ := m["files"].([]any)
	if len(files) != 1 {
		h.check(false, fmt.Sprintf("camera_shot reply carries %d files, want 1 (text=%.80s)", len(files), text))
		return false
	}
	fm, _ := files[0].(map[string]any)
	ok := b9VerifyJPEG(h, fm, 320, 240)
	if dst != "" {
		if body, rerr := os.ReadFile(b6Str(fm, "path")); rerr == nil {
			if werr := os.WriteFile(dst, body, 0o644); werr != nil {
				h.note("copy JPEG to %s: %v", dst, werr)
			} else {
				h.note("JPEG evidence: %s (%d bytes)", dst, len(body))
			}
		} else {
			h.note("read landed JPEG %s: %v", b6Str(fm, "path"), rerr)
		}
	}
	return ok
}

// b10t7Prelude is the shared S0: stale-daemon audit, TCP dial, sandbox,
// HEAD serve+mcp up, initialize, fw binding, lease, budget raise, and a
// self-healing board sweep. Returns the live client plus a finish()
// that kills the subprocesses.
func b10t7Prelude(h *harness, ctx context.Context, attemptLabel, daemonBin, runDir string) (*b6Client, func() int, bool) {
	fmt.Printf("=== b10 task-7 HIL: eyes+hands closed loop (%s) - %s ===\n", attemptLabel, time.Now().Format(time.RFC3339))
	fmt.Printf("board: %s over TCP %s (daemon never opens CDC - that reboots); fw expect %s (protocol_ver 5)\n", b10t7Board, b10t7TCPEndp, b10t7FwBuild)
	fmt.Printf("index: the live public one (raw.githubusercontent.com/wan-cloud/xuansi-drivers/main)\n")
	fmt.Printf("discipline: evidence ASCII only; verdicts in-file; honest attempts\n")

	var serve, mcp *b6Proc
	finish := func() int {
		code := h.finish("b10-t7")
		mcp.kill("mcp child")
		serve.kill("daemon")
		return code
	}
	hardFail := func() (*b6Client, func() int, bool) {
		f := finish
		mcp = nil
		serve = nil
		return nil, f, false
	}

	if out, err := exec.Command("pgrep", "-af", "xuansi").CombinedOutput(); err == nil {
		stale := 0
		for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Fields(ln)
			if len(f) >= 3 && strings.Contains(filepath.Base(f[1]), "xuansi") && f[2] == "serve" {
				stale++
				fmt.Printf("  [note] stale xuansi serve process: %s\n", asciiLine(ln))
			}
		}
		h.check(stale == 0, fmt.Sprintf("no stale xuansi serve daemon on the bench (found %d - the orchestrating .sh stops the resident npm daemon first)", stale))
	} else {
		h.note("pgrep: no xuansi processes found (clean bench)")
	}
	if conn, err := net.DialTimeout("tcp", b10t7TCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v", b10t7TCPEndp, err))
		b, f, _ := hardFail()
		return b, f, false
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only)", b10t7TCPEndp))
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return hardFail()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b10-t7 boards.conf: no aliases - the mac6 selector is used directly\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return hardFail()
	}
	h.note("XDG sandbox: XDG_DATA_HOME=%s", xdg)

	serve = startB6Daemon(h, daemonBin, sock, boardsCfg, "XDG_DATA_HOME="+xdg)
	if serve == nil {
		return hardFail()
	}
	good, last := pollUntil(300*time.Millisecond, 15*time.Second, func() (bool, string) {
		if c, err := net.Dial("unix", sock); err == nil {
			c.Close()
			return true, "sock dialable"
		}
		return false, "sock not bound yet"
	})
	h.check(good, fmt.Sprintf("HEAD daemon sock bound at %s (%s)", sock, last))
	good, last = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return serve.has("initial discovery sweep: 1 board(s)"), "sweep line"
	})
	h.check(good, fmt.Sprintf("initial discovery sweep found 1 board (%s)", last))

	mcp, mcpR, mcpW := startB6MCP(h, daemonBin, sock, "XDG_DATA_HOME="+xdg)
	if mcp == nil {
		return hardFail()
	}
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b10t7-hil", Version: "1.0"},
		&mcpsdk.ClientOptions{
			ResourceUpdatedHandler: func(_ context.Context, _ *mcpsdk.ResourceUpdatedNotificationRequest) {},
			ToolListChangedHandler: func(context.Context, *mcpsdk.ToolListChangedRequest) {},
		})
	b := &b6Client{h: h}
	cs, cerr := cl.Connect(ctx, &mcpsdk.IOTransport{Reader: mcpR, Writer: mcpW}, nil)
	if cerr != nil {
		h.check(false, "MCP client initialize over the child's stdio pipes: "+cerr.Error())
		return hardFail()
	}
	b.cs = cs
	si := cs.InitializeResult()
	h.check(si != nil && si.ServerInfo.Name == "xuansi",
		fmt.Sprintf("initialize handshake ok: server=%s version=%s (protocol %s)", initField(si, "name"), initField(si, "ver"), initField(si, "proto")))

	// fw BUILD BINDING (house rule)
	good, last = pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		m, _, isErr, ok := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
		if !ok || isErr {
			return false, "list_boards trouble"
		}
		for _, e := range m["boards"].([]any) {
			if bm, yes := e.(map[string]any); yes && bm["mac6"] == b10t7Board {
				if fw, _ := bm["fw"].(string); fw != "" {
					return true, fw
				}
				return false, "fw not yet enriched"
			}
		}
		return false, "board not in list yet"
	})
	fw := last
	h.check(good && fw == b10t7FwBuild, fmt.Sprintf("fw BUILD BINDING: %q == expected %q", fw, b10t7FwBuild))

	m, text, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b10t7Board, "holder": b10t7Holder})
	leaseSess, _ := b6Num(m, "session")
	h.check(!isErr && b6Str(m, "board") == b10t7Board && b6Str(m, "holder") == b10t7Holder && leaseSess > 0,
		fmt.Sprintf("lease holder=%s session=%.0f (default TCP route, no reboot)", b10t7Holder, leaseSess))

	// budget raise (analyze is a multi-second op; a lit full-screen can exceed 4s)
	_, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t7Board, "op": "budget.set", "args": map[string]any{"eval_ms": b10t7BudgetEval, "event_ms": b10t7BudgetEv}})
	h.check(!isErr, fmt.Sprintf("budget.set eval_ms=%d event_ms=%d (restored at teardown; text=%.60s)", b10t7BudgetEval, b10t7BudgetEv, text))

	// self-healing sweep: whatever an earlier round left, converge to a
	// clean slate (idempotent; honest notes only — no checks)
	if _, t2, ok2 := b10t7Lua(b, ctx, "for _,n in ipairs(bench.wasm.list()) do bench.wasm.unload(n) end return 'cleaned'"); !ok2 {
		h.note("sweep unload-all: %.70s (tolerated)", t2)
	}
	for _, n := range []string{"ledwatch", "st7789"} {
		b.call(ctx, 15*time.Second, "xuansi_fs_rm", map[string]any{"board": b10t7Board, "path": "/drv/" + n + ".wasm"})
	}
	b.call(ctx, 30*time.Second, "xuansi_board_op", map[string]any{"board": b10t7Board, "op": "camera.off"})
	b.call(ctx, 15*time.Second, "xuansi_session_reset", map[string]any{"board": b10t7Board, "session": float64(0)})
	if v, _, ok2 := b10t7Lua(b, ctx, "return #bench.wasm.list()"); ok2 {
		h.check(v == "0", fmt.Sprintf("precondition: wasm list empty after the sweep (list=%s)", v))
	}
	return b, finish, true
}

// b10t7Teardown is the shared board-clean convergence: st7789 deinit,
// camera off + cold check, unload x2, fs_rm x2 + not-found verify,
// session reset, budget restore, diag.pins cleanliness, lease release.
// deep=false (early-failure path: nothing was pushed) tolerates the
// /drv steps as honest notes instead of checks.
func b10t7Teardown(h *harness, b *b6Client, ctx context.Context, deep bool) {
	fmt.Println("\n- Z: teardown - board clean: camera off + 14 pins, modules unloaded, /drv removed, budget default, no lease -")
	b10t7AwaitBoard(h, b, ctx, "before teardown (stall residue check)")
	if _, text, ok := b10t7Lua(b, ctx, "return bench.wasm.call('st7789','deinit')"); !ok {
		h.note("st7789 deinit: %.80s (tolerated - claims converge via session reset below)", text)
	}
	m, text, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t7Board, "op": "camera.off"})
	h.check(!isErr, fmt.Sprintf("camera.off (text=%.60s)", text))
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t7Board, "op": "camera.status"})
	h.check(!isErr && !bolAny(m, "inited"), fmt.Sprintf("camera.status cold after off (inited=%v)", bolAny(m, "inited")))

	for _, n := range []string{"ledwatch", "st7789"} {
		if _, text, ok := b10t7Lua(b, ctx, fmt.Sprintf("return bench.wasm.unload('%s')", n)); !ok {
			h.note("unload %s: %.60s (tolerated)", n, text)
		}
	}
	for _, n := range []string{"ledwatch", "st7789"} {
		_, text, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_fs_rm",
			map[string]any{"board": b10t7Board, "path": "/drv/" + n + ".wasm"})
		if isErr && !deep {
			h.note("fs_rm /drv/%s.wasm (shallow teardown - pipeline never completed): %.50s", n, text)
		} else {
			h.check(!isErr, fmt.Sprintf("fs_rm /drv/%s.wasm (direct fs_rm rather than driver_pull to keep this teardown lease-lean; driver_pull sweeps it too since B10-T8; text=%.50s)", n, text))
		}
	}
	if deep {
		_, text, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_fs_read",
			map[string]any{"board": b10t7Board, "path": "/drv/st7789.wasm"})
		h.check(isErr && strings.Contains(text, "not found"), fmt.Sprintf("/drv/st7789.wasm gone (text=%.60s)", text))
	}
	if v, _, ok := b10t7Lua(b, ctx, "return #bench.wasm.list()"); ok {
		h.check(v == "0", fmt.Sprintf("wasm list empty after unload x2 (list=%s)", v))
	}
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_session_reset",
		map[string]any{"board": b10t7Board, "session": float64(0)})
	h.check(!isErr, fmt.Sprintf("session_reset 0 (the wasm-claims convergence path; text=%.50s)", text))
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t7Board, "op": "budget.set", "args": map[string]any{"eval_ms": 200, "event_ms": 50}})
	h.check(!isErr, "budget restored to defaults (eval_ms=200 event_ms=50)")

	// pins: capture the diag.pins stdout dump (b9 S8 pattern)
	capCh := b.callAsync("xuansi_capture", map[string]any{"board": b10t7Board, "source": "stdout", "seconds": float64(4)})
	time.Sleep(700 * time.Millisecond)
	b.call(ctx, 30*time.Second, "xuansi_board_op", map[string]any{"board": b10t7Board, "op": "diag.pins"})
	cres := <-capCh
	capData, rdErr := os.ReadFile(b6Str(cres.m, "path"))
	if rdErr != nil {
		h.check(false, fmt.Sprintf("pins dump readable (%v)", rdErr))
		return
	}
	cont := asciiLine(string(capData))
	var busy []string
	for _, pin := range append(append([]string{}, b9CameraPins...), append(b10t7ScreenPins, "GPIO1", "GPIO2")...) {
		if strings.Contains(cont, pin) {
			busy = append(busy, pin)
		}
	}
	h.check(len(busy) == 0, fmt.Sprintf("diag.pins after teardown: none of the 14 camera + 5 screen + gpio1/2 pins held (busy=%v)", busy))

	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_release", map[string]any{"board": b10t7Board, "holder": b10t7Holder})
	h.check(!isErr, fmt.Sprintf("lease released (text=%.50s)", text))
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b10t7Board})
	_, hasLease := m["lease"]
	h.check(!isErr && !hasLease, fmt.Sprintf("board_status shows NO lease (lease key %v)", hasLease))
}

// b10t7FetchPushLoad is the S1 real-index pipeline (shared by aim+loop).
func b10t7FetchPushLoad(h *harness, b *b6Client, ctx context.Context) bool {
	all := true
	for _, spec := range []struct {
		name  string
		sha   string
		bytes int
	}{{"ledwatch", b10t7WatchSHA, b10t7WatchByt}, {"st7789", b10t7StSHA, b10t7StByt}} {
		m, text, isErr, _ := b.call(ctx, 60*time.Second, "xuansi_driver_fetch",
			map[string]any{"name": spec.name, "version": "1.0.0"})
		var fm map[string]any
		if fl, ok := m["files"].([]any); ok && len(fl) > 0 {
			fm, _ = fl[0].(map[string]any)
		}
		sha, byt := b6Str(fm, "sha256"), 0
		if n, ok := b6Num(fm, "bytes"); ok {
			byt = int(n)
		}
		all = h.check(!isErr && sha == spec.sha && byt == spec.bytes,
			fmt.Sprintf("W10 fetch %s@1.0.0 from the live index: sha=%.12s bytes=%d (pinned; text=%.50s)", spec.name, sha, byt, text)) && all
		m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
			map[string]any{"board": b10t7Board, "name": spec.name, "version": "1.0.0"})
		all = h.check(!isErr && b6Str(m, "path") == "/drv/"+spec.name+".wasm" && b6Str(m, "sha256") == spec.sha,
			fmt.Sprintf("W11 push %s@1.0.0 -> %s sha-verified (text=%.50s)", spec.name, b6Str(m, "path"), text)) && all
		v, text, okLua := b10t7Lua(b, ctx, fmt.Sprintf("return bench.wasm.load('%s')", spec.name))
		all = h.check(okLua && v == "true", fmt.Sprintf("W12 wasm.load('%s') -> %s (text=%.50s)", spec.name, v, text)) && all
	}
	return all
}

func absF(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// b10t7AwaitBoard waits (bounded) for the controller conn task to chew
// through a stalled op — a dead-sensor fb_get wedges the conn task well
// past the client deadlines, and every teardown op queued behind it
// would time out as collateral (aim round 1's honest finding).
func b10t7AwaitBoard(h *harness, b *b6Client, ctx context.Context, why string) {
	deadline := time.Now().Add(150 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, ok := b10t7Lua(b, ctx, "return 'alive'"); ok {
			h.note("board responsive again %s", why)
			return
		}
		time.Sleep(5 * time.Second)
	}
	h.note("board NOT responsive within 150s %s - proceeding; the teardown below may surface collateral timeouts (the stalled fb_get still holds the conn task; a CDC reboot is the hard recovery)", why)
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// b10t7RefBox parses the -ref geometry "x,y,w,h" (zeros when absent).
func b10t7RefBox(refGeom string) (float64, float64, float64, float64) {
	parts := strings.Split(refGeom, ",")
	if len(parts) != 4 {
		return 0, 0, 0, 0
	}
	v := make([]float64, 4)
	for i, p := range parts {
		v[i], _ = strconv.ParseFloat(strings.TrimSpace(p), 64)
	}
	return v[0], v[1], v[2], v[3]
}

// runB10T7Aim is one interactive aim round (task steps 1-2).
func runB10T7Aim(round int, daemonBin, runDir, outDir string) int {
	h := &harness{}
	ctx := context.Background()
	b, finish, ok := b10t7Prelude(h, ctx, fmt.Sprintf("aim round %d", round), daemonBin, runDir)
	if !ok {
		return finish()
	}

	fmt.Println("\n- A1: camera-ALONE sanity (the FPC differential - no module loaded) -")
	okShot := b10t7Shot(h, b, ctx, filepath.Join(outDir, fmt.Sprintf("2026-11-08-b10-t7-aim-%d-camonly.jpg", round)))
	h.check(okShot, fmt.Sprintf("A1 camera alone: QVGA shot ok (round %d) - a failure here is sensor/FPC hardware, NOT coexistence", round))
	if !okShot {
		fmt.Printf("AIM_ROUND %d VERDICT: BLOCKED-FPC | camera dead with NO module loaded - owner must re-check the FPC/connector\n", round)
		b10t7AwaitBoard(h, b, ctx, "after the stalled capture (fb_get unwind)")
		b10t7Teardown(h, b, ctx, false)
		return finish()
	}
	// park the camera: large spi_xfer (4KB DMA) hits the engine's
	// "DMA buffer OOM" refusal while the DVP path is live (round 1's
	// honest finding) - the panel holds its own RAM, so the loop draws
	// with the camera OFF and watches the PERSISTED pattern (see A6).
	_, text, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t7Board, "op": "camera.off"})
	h.check(!isErr, fmt.Sprintf("A1b camera parked (off) before any draw - draw-then-watch discipline (text=%.50s)", text))

	fmt.Println("\n- A2: the real pipeline - fetch both modules from the live index -> push x2 -> load x2 -")
	b10t7FetchPushLoad(h, b, ctx)

	fmt.Println("\n- A3: st7789 init + border pattern drawn with the camera OFF (the panel persists it) -")
	v, text, okLua := b10t7Lua(b, ctx, "return bench.wasm.call('st7789','init',{x_offset=0})")
	if !okLua {
		h.check(false, fmt.Sprintf("A3 st7789 init (text=%.100s)", text))
		b10t7Teardown(h, b, ctx, true)
		return finish()
	}
	h.check(strings.Contains(v, "ok"), fmt.Sprintf("A3 st7789 init {x_offset=0} -> %s", v))
	_, text, okLua = b10t7Lua(b, ctx, "return bench.wasm.call('st7789','pattern','border')")
	if !okLua {
		h.check(false, fmt.Sprintf("A3 border pattern (text=%.100s)", text))
		b10t7Teardown(h, b, ctx, true)
		return finish()
	}
	h.check(true, "A3 border pattern drawn (camera off - no DMA contention)")

	// A4/A5: power the camera on the PERSISTED border, then two
	// consecutive shot+analyze passes (convergence needs BOTH)
	var pass [2][]b10t7Reg
	var done [2]bool
	for p := 1; p <= 2; p++ {
		fmt.Printf("\n- A%d: capture pass %d on the persisted border + ledwatch analyze -\n", 3+p, p)
		if !b10t7Shot(h, b, ctx, filepath.Join(outDir, fmt.Sprintf("2026-11-08-b10-t7-aim-%d-border%d.jpg", round, p))) {
			b10t7AwaitBoard(h, b, ctx, fmt.Sprintf("after aim pass %d stalled shot", p))
			continue
		}
		regs, text, okA := b10t7Analyze(b, ctx, "min_area=800")
		if !okA {
			h.check(false, fmt.Sprintf("A%d analyze pass %d (text=%.120s)", 3+p, p, text))
			if strings.Contains(text, "context deadline exceeded") || strings.Contains(text, "timeout") {
				b10t7AwaitBoard(h, b, ctx, fmt.Sprintf("after aim pass %d stalled analyze", p))
			}
			continue
		}
		h.note("analyze pass %d: %d region(s)", p, len(regs))
		for i, r := range regs {
			h.note("  region[%d] %s", i, r.String())
		}
		pass[p-1], done[p-1] = regs, true
	}

	// A6: the deferred frame-level coexistence PROBE - one border
	// redraw attempt while the camera is LIVE. Round 1's honest answer
	// was the engine's rt refusal "xuansi.spi_xfer: slot 0 refused: DMA
	// buffer OOM (rc=-10)" (the first boot ever with camera+SPI both
	// truly live); recorded verbatim either way - this is the datum the
	// T6-fix1 C-group could not produce (its camera was FPC-dead).
	if _, text, okLua := b10t7Lua(b, ctx, "return bench.wasm.call('st7789','pattern','border')"); okLua {
		h.note("A6 frame-level coexistence probe: redraw with camera LIVE SUCCEEDED (border redrawn inside a live camera epoch)")
	} else {
		h.note("A6 frame-level coexistence probe: redraw with camera LIVE refused: %.120s", text)
	}

	verdict, detail := "AIM_ADJUST", "no dominant lit region detected - screen not in frame or not visible"
	largest := b10t7Reg{}
	if done[0] && len(pass[0]) > 0 {
		largest = pass[0][0] // area-descending contract
		ratio := largest.w / largest.h
		h.note("dominant region: %s  w/h=%.2f", largest.String(), ratio)
		switch {
		case largest.area < 1500:
			detail = fmt.Sprintf("dominant region too small (area=%.0f < 1500) - screen not resolving in frame", largest.area)
		case ratio < 0.5 || ratio > 2.0:
			detail = fmt.Sprintf("w/h=%.2f outside [0.5,2.0] - bad angle/distance or partial view", ratio)
		case !done[1] || len(pass[1]) == 0:
			detail = "second capture pass produced no regions (unstable)"
		default:
			l2 := pass[1][0]
			dx, dy := largest.x-l2.x, largest.y-l2.y
			dw, dh := largest.w-l2.w, largest.h-l2.h
			if absF(dx) > 15 || absF(dy) > 15 || absF(dw) > 15 || absF(dh) > 15 {
				detail = fmt.Sprintf("unstable across captures: d=(%.0f,%.0f,%.0f,%.0f) > 15px", dx, dy, dw, dh)
			} else {
				verdict = "AIM_OK"
				detail = fmt.Sprintf("stable: d=(%.0f,%.0f,%.0f,%.0f)px, w/h=%.2f, area=%.0f, luma=%.0f",
					dx, dy, dw, dh, ratio, largest.area, largest.luma)
				h.check(true, fmt.Sprintf("aim converged round %d: dominant region %s (w/h=%.2f) stable across 2 consecutive captures", round, largest.String(), ratio))
			}
		}
	}
	if verdict == "AIM_ADJUST" {
		h.check(false, fmt.Sprintf("aim round %d not converged: %s (JPEGs saved for the owner)", round, detail))
	}
	fmt.Printf("AIM_ROUND %d VERDICT: %s | %s | ref bbox=(%.0f,%.0f,%.0f,%.0f)\n",
		round, verdict, detail, largest.x, largest.y, largest.w, largest.h)

	b10t7Teardown(h, b, ctx, true)
	return finish()
}

// runB10T7Loop is the full closed loop (brief steps 2-5), ONE continuous
// MCP-driven capture.
func runB10T7Loop(attempt int, daemonBin, runDir, outDir, refGeom string, tolPx int) int {
	h := &harness{}
	ctx := context.Background()
	b, finish, ok := b10t7Prelude(h, ctx, fmt.Sprintf("closed loop, attempt %d", attempt), daemonBin, runDir)
	if !ok {
		return finish()
	}

	fmt.Println("\n- S1: fetch(ledwatch+st7789) from the live index -> push x2 -> load x2 -")
	b10t7FetchPushLoad(h, b, ctx)

	fmt.Println("\n- S2: st7789 init + clear(RED) drawn with the camera OFF (draw-then-watch: the panel persists; large spi_xfer while the DVP path is live hits the engine's DMA OOM refusal - round-1 finding). ONE cycle carries BOTH the geometry check (the full-screen region's bbox IS the screen rect - the same rect the aim round's border ring outlined) and R/B signal #1 (full-screen red: dilution-immune) - the camera is hardware-marginal (intermittent FPC) and every cycle counts -")
	var screen b10t7Reg
	geoOK := false
	v, text, okLua := b10t7Lua(b, ctx, "return bench.wasm.call('st7789','init',{x_offset=0})")
	h.check(okLua && strings.Contains(v, "ok"), fmt.Sprintf("S2 st7789 init {x_offset=0} -> %.60s", v))
	_, text, okLua = b10t7Lua(b, ctx, "return bench.wasm.call('st7789','clear',0xF800)")
	h.check(okLua, fmt.Sprintf("S2 clear(RED) 0xF800 full screen drawn (camera off; text=%.60s)", text))

	fmt.Printf("\n- S3: geometry pairing + R/B signal #1 - camera on (power shot) -> ledwatch region on the persisted RED screen vs the reference screen bbox (tol %dpx) + the full-red r/b ratio -\n", tolPx)
	stalled := false
	sigRed := -1 // -1 unknown, 1 red-dominant, 0 blue-dominant
	if !b10t7Shot(h, b, ctx, filepath.Join(outDir, fmt.Sprintf("2026-11-08-b10-t7-closedloop-attempt%d-clearred.jpg", attempt))) {
		b10t7AwaitBoard(h, b, ctx, "after the S3 power shot stall")
		stalled = true
	}
	regs, text, okA := b10t7Analyze(b, ctx, "min_area=800")
	if okA && len(regs) > 0 {
		// AWB settler: the second analyze is the measurement (attempt 7's
		// finding - a fresh monochrome field reads crushed on the first)
		if rs2, _, ok2 := b10t7Analyze(b, ctx, "min_area=800"); ok2 && len(rs2) > 0 {
			regs = rs2
			h.note("S3: AWB settler pass -> settled regions used")
		}
	}
	if !okA {
		h.check(false, fmt.Sprintf("S3 analyze on the red screen (text=%.120s)", text))
		if strings.Contains(text, "context deadline exceeded") || strings.Contains(text, "timeout") {
			b10t7AwaitBoard(h, b, ctx, "after the S3 analyze stall")
			stalled = true
		}
	} else {
		for i, r := range regs {
			h.note("  region[%d] %s", i, r.String())
		}
		if len(regs) == 0 {
			h.check(false, "S3 red screen produced 0 regions (screen not visible to ledwatch)")
		} else {
			screen = regs[0]
			if screen.r > 0 || screen.b > 0 {
				if screen.r >= 2*screen.b {
					sigRed = 1
				} else if screen.b >= 2*screen.r {
					sigRed = 0
				}
			}
			ratio := screen.w / screen.h
			h.check(ratio >= 0.5 && ratio <= 2.0, fmt.Sprintf("S3 screen w/h=%.2f within [0.5,2.0] (240x240 panel modulo perspective; luma=%.0f)", ratio, screen.luma))
			if refGeom == "" {
				h.check(false, fmt.Sprintf("S3 GEOMETRY PAIRING: no -ref reference given (pre-convergence honest red) - dominant region %s w/h=%.2f is the pairing candidate", screen.String(), ratio))
			} else {
				parts := strings.Split(refGeom, ",")
				if len(parts) != 4 {
					h.check(false, fmt.Sprintf("S3 -ref parse failed: %q (want x,y,w,h)", refGeom))
				} else {
					rx, _ := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
					ry, _ := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
					rw, _ := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
					rh, _ := strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
					dx, dy, dw, dh := absF(screen.x-rx), absF(screen.y-ry), absF(screen.w-rw), absF(screen.h-rh)
					geoOK = dx <= float64(tolPx) && dy <= float64(tolPx) && dw <= float64(tolPx) && dh <= float64(tolPx)
					h.check(geoOK,
						fmt.Sprintf("S3 GEOMETRY PAIRING: red-screen region (%.0f,%.0f,%.0f,%.0f) vs the border-ring reference (%.0f,%.0f,%.0f,%.0f): d=(%.0f,%.0f,%.0f,%.0f)px <= tol %dpx (a full bright screen can bloom a few px past the thin ring - the deltas are printed for judgment)",
							screen.x, screen.y, screen.w, screen.h, rx, ry, rw, rh, dx, dy, dw, dh, tolPx))
				}
			}
		}
	}

	// b10t7Phase is one draw-then-watch cycle: camera OFF + settle (the
	// sensor wants a discharge window before a re-power - a quick
	// off->draw->on wedged the conn task for minutes in loop attempt 1),
	// draw the pattern (SPI unconstrained), power shot (JPEG evidence +
	// sensor warm), then analyze down a threshold ladder (the pattern
	// persists on the panel - no redraw between rungs). On the first
	// resolving rung ONE more analyze runs and ITS region is the
	// measurement: the GC2145's AWB converges over seconds and a fresh
	// monochrome field reads crushed on the first analyze (attempt 7:
	// a full RED screen read r/g/b=(255,234,237)) - the settled pass
	// measures, the first settles. Region selection matches the
	// REFERENCE SCREEN BBOX (the area-dominant region can be an
	// AWB-wrecked background blob - attempt 7's S4b). A stall anywhere
	// awaits the conn-task unwind and reports the phase failed WITHOUT
	// poisoning the phases after it. Returns the screen region.
	phase := func(label, drawCode string, shotName string, thrs []int, baseCfg string) (b10t7Reg, int, bool) {
		if _, text, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_board_op",
			map[string]any{"board": b10t7Board, "op": "camera.off"}); isErr {
			h.note("%s: camera.off pre-draw: %.70s", label, text)
		}
		time.Sleep(3 * time.Second) // sensor settle before the re-power (attempt 1's honest finding)
		_, t, okD := b10t7Lua(b, ctx, drawCode)
		if !okD {
			h.check(false, fmt.Sprintf("%s draw (text=%.90s)", label, t))
			b10t7AwaitBoard(h, b, ctx, "after "+label)
			return b10t7Reg{}, 0, false
		}
		if !b10t7Shot(h, b, ctx, filepath.Join(outDir, shotName)) {
			// the documented transient class: esp_camera_init rc=-1 at
			// boot-churn margins retries clean (diag round: B9+B10 both,
			// ~1-2 per 15 cold inits) - one retry, honestly noted
			b10t7AwaitBoard(h, b, ctx, "after "+label+" stalled capture")
			h.note("%s: transient init-fail retry (documented class)", label)
			b.call(ctx, 30*time.Second, "xuansi_board_op", map[string]any{"board": b10t7Board, "op": "camera.off"})
			time.Sleep(2 * time.Second)
			if !b10t7Shot(h, b, ctx, filepath.Join(outDir, shotName)) {
				b10t7AwaitBoard(h, b, ctx, "after "+label+" retry also stalled")
				return b10t7Reg{}, 0, false
			}
		}
		pickScr := func(rs []b10t7Reg) b10t7Reg { // best overlap with the ref screen bbox
			if len(rs) == 0 {
				return b10t7Reg{}
			}
			rx, ry, rw, rh := b10t7RefBox(refGeom)
			best, bestOv := rs[0], -1.0
			for _, r := range rs {
				ox := maxF(0, minF(r.x+r.w, rx+rw)-maxF(r.x, rx))
				oy := maxF(0, minF(r.y+r.h, ry+rh)-maxF(r.y, ry))
				ov := (ox * oy) / maxF(1, r.w*r.h)
				if ov > bestOv {
					best, bestOv = r, ov
				}
			}
			return best
		}
		for _, thr := range thrs {
			rs, t, okA := b10t7Analyze(b, ctx, fmt.Sprintf("%s,threshold=%d", baseCfg, thr))
			if !okA {
				if strings.Contains(t, "context deadline exceeded") || strings.Contains(t, "timeout") {
					h.check(false, fmt.Sprintf("%s analyze thr=%d stalled (text=%.80s)", label, thr, t))
					b10t7AwaitBoard(h, b, ctx, "after "+label+" stalled analyze")
					return b10t7Reg{}, 0, false
				}
				h.check(false, fmt.Sprintf("%s analyze thr=%d (text=%.100s)", label, thr, t))
				return b10t7Reg{}, 0, false
			}
			if len(rs) == 0 {
				h.note("%s thr=%d: 0 regions (ladder continues)", label, thr)
				continue
			}
			settled := pickScr(rs)
			rs2, _, ok2 := b10t7Analyze(b, ctx, fmt.Sprintf("%s,threshold=%d", baseCfg, thr)) // AWB settler
			if ok2 && len(rs2) > 0 {
				settled = pickScr(rs2)
			}
			for i, r := range rs {
				h.note("  %s thr=%d region[%d] %s%s", label, thr, i, r.String(),
					map[bool]string{true: " <= screen", false: ""}[r == settled])
			}
			h.note("  %s settled measurement %s (settler pass %s)", label, settled.String(),
				map[bool]string{true: "used", false: "unavailable"}[ok2 && len(rs2) > 0])
			return settled, thr, true
		}
		h.check(false, fmt.Sprintf("%s: no rung of the threshold ladder %v resolved a region", label, thrs))
		return b10t7Reg{}, 0, false
	}

	fmt.Println("\n- S4: R/B verdict double evidence - red_rect (the brief's primary pattern) + clear BLUE (the mirror) + quad luma pass (draw-then-watch per phase; camera-cycle budget: the sensor is marginal (intermittent FPC) - the first stall ends camera work for the run; S3's full-red signal #1 already banked) -")
	rbVerdict, rbDetail := "AMBIGUOUS", ""
	rbRatio, evBlue := 0.0, 0.0
	evRed := 0.0
	var sigs []bool // DECISIVE signals (>=2x): true = faithful color mapping (drawn color measured as drawn)
	// B10-T7 fix1: the MEASURED settled ratio ALWAYS feeds evRed (the
	// fix-round finding: a decisive-only assignment left the 0.00
	// initializer in the machine line - reading as "zero red", a
	// swapped-direction number inside the exact line automation greps).
	// sigRed stays the decisiveness tri-state; the print marks the field.
	redTag := "(unmeasured)"
	if screen.r > 0 || screen.b > 0 {
		evRed = screen.r / maxF(screen.b, 1)
		if sigRed >= 0 {
			sigs = append(sigs, sigRed == 1)
			redTag = ""
			rbDetail = fmt.Sprintf("clearRED@S3 r/b=%.2f", evRed)
		} else {
			redTag = "(dir-only,tri-state)" // AWB-crushed: direction recorded, not counted
			rbDetail = fmt.Sprintf("clearRED@S3 r/b=%.2f(dir-only,tri-state)", evRed)
		}
	} else {
		rbDetail = "clearRED@S3 unmeasured"
	}
	// S4a: red_rect — the brief's primary: the lit region IS the drawn red
	// (black surround; a dark-room AE can wash the panel's black above the
	// threshold and dilute the ratio - that is why S3's full-red carries
	// signal #1). AWB whitens a SMALL field harder (attempt 8/9: the rect
	// reads ~(255,247,243), directional but not 2x-decisive) - only
	// DECISIVE orientations append a signal; directional-but-crushed is
	// noted, never counted (attempt 9's SWAPPED was this bug plus the
	// blue-mirror polarity inversion - both fixed, honest history kept).
	if !stalled {
		if r, thr, okP := phase("S4a red_rect", "return bench.wasm.call('st7789','pattern','red_rect')",
			fmt.Sprintf("2026-11-08-b10-t7-closedloop-attempt%d-redrect.jpg", attempt),
			[]int{128, 60, 40, 25, 15}, "min_area=500"); okP {
			if r.r > 0 || r.b > 0 {
				rbRatio = r.r / maxF(r.b, 1)
				rbDetail += fmt.Sprintf(" red_rect@%d r=%d b=%d", thr, int(r.r), int(r.b))
				switch {
				case r.r >= 2*r.b && r.r > 60:
					sigs = append(sigs, true)
					h.note("S4a signal: DECISIVE red-dominant")
				case r.b >= 2*r.r && r.b > 60:
					sigs = append(sigs, false)
					rbDetail += " RED-DRAWN MEASURES BLUE-DOMINANT"
					h.note("S4a signal: DECISIVE blue-dominant (red-drawn!)")
				default:
					h.note("S4a signal: directional r/b=%.2f but AWB-crushed (<2x) - noted, NOT counted", rbRatio)
				}
			}
		} else {
			stalled = true
		}
	} else {
		h.note("S4a red_rect skipped: the camera stalled earlier this run (cycle budget spent)")
	}
	// S4b: full-screen clear BLUE - the mirror signal: under NORMAL the
	// blue-drawn screen measures blue-dominant (attempt 9 settled:
	// (12,248,255), 21x separation); under SWAP it would measure
	// RED-dominant. blueDom IS the faithful-mapping signal (attempt 9's
	// machine-SWAPPED was the !blueDom polarity inversion - fixed).
	if !stalled {
		if r, thr, okP := phase("S4b clear-BLUE", "return bench.wasm.call('st7789','clear',0x001F)",
			fmt.Sprintf("2026-11-08-b10-t7-closedloop-attempt%d-clear-blue.jpg", attempt),
			[]int{60, 25, 15, 10}, "min_area=2000"); okP {
			evBlue = r.r / maxF(r.b, 1)
			blueDom := r.b >= 2*r.r
			if blueDom {
				sigs = append(sigs, true) // faithful: drawn blue measured blue
			} else if r.r >= 2*r.b {
				sigs = append(sigs, false) // drawn blue measured RED = swap
			}
			h.note("S4b clear-BLUE@%d: %s ratio r/b=%.2f blue-dominant=%v (faithful-mapping signal=%v)",
				thr, r.String(), evBlue, blueDom, blueDom)
		} else {
			stalled = true
		}
	} else {
		h.note("S4b clear-BLUE skipped: the camera stalled earlier this run (cycle budget spent)")
	}
	// S4c: quad luma-ordering pass (swap-invariant by construction) + JPEG
	if !stalled {
		if r, _, okP := phase("S4c quad@100", "return bench.wasm.call('st7789','pattern','quad')",
			fmt.Sprintf("2026-11-08-b10-t7-closedloop-attempt%d-quad.jpg", attempt),
			[]int{100}, "min_area=2000"); okP {
			h.note("S4c quad@100: the lit adjacent quadrants MERGE in ledwatch's CCL (seam-adjacent), so per-quadrant r/b is unreachable through ledwatch - evidence by ordering + the red_rect/clear passes above")
			h.check(r.area >= 2000, fmt.Sprintf("S4c quad pass: a dominant lit region resolves on the multi-color panel (%s)", r.String()))
		}
	} else {
		h.note("S4c quad skipped: the camera stalled earlier this run (cycle budget spent)")
	}
	// N-signal verdict. DECISIVE signals (>=2x either way) decide alone
	// when >=2 agree; a single decisive decides only with UNANIMOUS
	// directional corroboration (the GC2145's AWB clamps full-field red
	// to ~1.8x - measured twice - so the red mirror is often directional-
	// only; the blue mirror measured 21x. No inverted measurement may
	// stand: any conflict = AMBIGUOUS with all numbers recorded).
	allTrue, allFalse := len(sigs) >= 2, len(sigs) >= 2
	for _, s := range sigs {
		allTrue = allTrue && s
		allFalse = allFalse && !s
	}
	dirs := []bool{} // directional orientations: true = faithful (drawn color measured stronger)
	if evRed > 1 {
		dirs = append(dirs, true)
	} else if evRed > 0 && evRed < 1 {
		dirs = append(dirs, false)
	}
	if rbRatio > 1 {
		dirs = append(dirs, true)
	} else if rbRatio > 0 && rbRatio < 1 {
		dirs = append(dirs, false)
	}
	if evBlue > 0 {
		dirs = append(dirs, evBlue < 1) // blue mirror: faithful = r/b < 1
	}
	dirsAgree := len(dirs) > 0
	for _, d := range dirs {
		dirsAgree = dirsAgree && d
	}
	dirsConflict := false
	for _, d := range dirs {
		if !d {
			dirsConflict = true
		}
	}
	switch {
	case allTrue:
		rbVerdict = "NORMAL"
	case allFalse:
		rbVerdict = "SWAPPED"
	case len(sigs) == 1 && sigs[0] && dirsAgree && !dirsConflict:
		rbVerdict = "NORMAL"
	case len(sigs) == 1 && !sigs[0] && dirsConflict && !dirsAgree:
		rbVerdict = "SWAPPED"
	}
	h.check(rbVerdict != "AMBIGUOUS",
		fmt.Sprintf("S4 R/B VERDICT from %d decisive signal(s) %v + %d directional corroboration(s) (clearRED=%.2f red_rect=%.2f clearBLUE=%.2f): %s",
			len(sigs), sigs, len(dirs), evRed, rbRatio, evBlue, rbVerdict))
	fmt.Printf("RB_VERDICT: %s %s | red_rect r/b=%.2f | clear-RED r/b=%.2f%s | clear-BLUE r/b=%.2f\n",
		rbVerdict, rbDetail, rbRatio, evRed, redTag, evBlue)

	fmt.Println("\n- S5: hands - GPIO1(out) -> GPIO2(in) wired pair, drive+readback x5 -")
	v, text, okLua = b10t7Lua(b, ctx, `bench.gpio.mode(1,"out") bench.gpio.mode(2,"in") return "modes-set"`)
	if !okLua {
		h.check(false, fmt.Sprintf("S5 gpio modes (text=%.80s)", text))
	} else {
		h.check(v == "modes-set", "S5 gpio1 out + gpio2 in claimed")
		nOK := 0
		for i, want := range []int{1, 0, 1, 0, 1} {
			v, text, okLua := b10t7Lua(b, ctx, fmt.Sprintf("bench.gpio.write(1,%d) return bench.gpio.read(2)", want))
			if !okLua {
				h.check(false, fmt.Sprintf("S5 readback %d/5: write/read (text=%.80s)", i+1, text))
				continue
			}
			got, _ := strconv.Atoi(strings.TrimSpace(v))
			h.check(got == want, fmt.Sprintf("S5 electrical readback %d/5: gpio1=%d -> gpio2=%d", i+1, want, got))
			if got == want {
				nOK++
			}
		}
		h.check(nOK == 5, fmt.Sprintf("S5 electrical loop 5/5 matches (got %d/5)", nOK))
	}

	b10t7Teardown(h, b, ctx, true)
	fmt.Printf("=== b10-t7 closed loop attempt %d: geometry_pairing=%v rb_verdict=%s ===\n", attempt, geoOK, rbVerdict)
	return finish()
}
