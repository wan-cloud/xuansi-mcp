// b9-camera.go - the camera face END TO END, rewritten for the v6
// recipe scenario (B12-P2 T8, ruling 3: same scenario on the module
// path — the version stamp distinguishes the eras; the B9-T5 op-face
// original lives in git history with its 2026-11-07 evidence).
//
// The daemon is camera-free since B12-P2 (the stills tool, the camera
// ops and the captures/ landing convention are all retired): what this
// HIL proves now is the AGENT RECIPE end to end - fetch from the live
// public index, push, and ONE budgeted run_lua whose reply frames
// carry the JPEG; the agent (this harness) decodes, lands and verifies
// the files itself. Same coverage as the B9 original, every step a
// primitive:
//
//	S0  prelude - no stale serve daemons, board TCP reachable, XDG
//	    sandbox, HEAD serve+mcp up + initialize, the v6 face machine
//	    check (tools/list == 24 static tools, zero camera names), the
//	    fw BUILD BINDING (dcaa9ae, the B12-P2 fullclean canonical),
//	    board starts with no lease
//	S1  lease FIRST (the holder ladder - a holder-carrying call before
//	    it binds the connection to the generated default), /drv swept,
//	    driver_fetch gc2145+jpge from the LIVE index (sha pinned),
//	    driver_push x2 (sha read-back), wasm.load x2
//	S2  THE RECIPE - one xuansi_run_lua{budget_ms:10000} chaining
//	    load x2 -> gc2145.init{QVGA} (the interp transition window's
//	    framesize - the README recipe section's own note; VGA/q80 is
//	    the AOT-era target shape) -> jpge.shot{q60} -> gc2145.off -
//	    the reply carries frames=[{ch:2,name:"jpge",data_b64}]; the
//	    agent decodes, lands the JPEG, verifies SOI/EOI + Go
//	    image/jpeg decode 320x240 + sha256 (no server-side capture
//	    path exists - captures/ is retired with the stills tool)
//	S3  warm repeat - the recipe again: a distinct file (different
//	    sha256), the warm-path wall time noted against S2's cold shot
//	S4  the v6 refusal matrix - the retired op face (board_op
//	    camera.status -> the fw's unknown-op refusal), the client-side
//	    budget domain gate (run_lua budget_ms=0 -> api naming
//	    1..30000 BEFORE any daemon contact), the no-lease refusal
//	    (post-release run_lua -> not_held), and the fw observer
//	    vector (a direct observer connection's eval -> the fw's
//	    controller-gate refusal); plus the no-frame shape check (a
//	    plain eval reply has no frames/frames_dropped keys - Review
//	    Focus #1 at this seat)
//	S5  cold-board finish - gc2145.info() on=false after the recipe's
//	    off(), diag.pins over a stdout capture (the DVP+XCLK pins free
//	    after off; all 14 camera pins free after session_reset{0} -
//	    the documented F-3 SCCB persistence)
//	S6  as-found + teardown - /drv swept + gone-verified, release, no
//	    lease, mcp EOF exit 0, serve SIGINT exit 0, sock removed, the
//	    landed-JPEG inventory printed (in the harness outdir - the
//	    agent's own files)
//
// Three real process layers (b6/b8 house style): go-sdk MCP client over
// os.Pipe <-> `xuansi mcp` <-> `xuansi serve` <-> the real board over
// TCP (the daemon never opens CDC - that reboots the board; the fw
// observer vector is the only deliberate extra connection, labeled).
// XDG-sandboxed; ASCII only; numbered checks; honest attempt counts.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"image/jpeg"
	"io"
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
	b9Board   = "d75884"  // the bench board (GC2145 camera on the 14-pin gate)
	b9FwBuild = "dcaa9ae" // B12-P2 fullclean canonical (protocol_ver 6, camera face out)
	b9Holder  = "b9-recipe-hil"
	b9TCPEndp = "xuansi-d75884.local:7777"

	// pinned against the live public index (wan-cloud/xuansi-drivers)
	b9CamSHA = "701236ca76f9c97fac65ce75a1717c7cf52bb18812f9ae31377c9ff0cdaf26ff"
	b9JpgSHA = "c0d6a97c31c516c03a1af795c6376dfb52de5a77474990f4d9b791408916472c"
)

// b9CameraPins is the camera's 14-pin occupancy (the board wiring
// table): the as-found check wants every one of them absent from a
// diag.pins dump after the recipe's off() + session reset.
var b9CameraPins = []string{
	"GPIO4", "GPIO5", "GPIO6", "GPIO7", "GPIO8", "GPIO9", "GPIO10",
	"GPIO11", "GPIO12", "GPIO13", "GPIO15", "GPIO16", "GPIO17", "GPIO18",
}

// b9RecipeCode is the recipe chain (drivers README "Agent camera
// recipe", interp-window framesize/quality per its own note): load x2
// -> init QVGA -> shot q60 -> off, one eval, budget_ms 10000. The
// loads are idempotent (list-probe first): a failed first init leaves
// the modules loaded, and the warm repeat must not trip over them.
const b9RecipeCode = "local function have(n) for _,x in ipairs(bench.wasm.list()) do if x==n then return true end end return false end " +
	"local ok1 = have('gc2145') or bench.wasm.load('gc2145') " +
	"local ok2 = have('jpge') or bench.wasm.load('jpge') " +
	"local i=bench.wasm.call('gc2145','init',{framesize='QVGA'}) " +
	"local s=bench.wasm.call('jpge','shot',{quality=60}) " +
	"local o=bench.wasm.call('gc2145','off') " +
	"return {loaded=(ok1 and ok2), init_ok=i.ok, w=i.w, h=i.h, " +
	"shot_len=s.len, enc_ms=s.enc_ms, off_ok=o.ok, off_on=o.on}"

// b9RecipeShot runs the recipe once and lands+verifies the JPEG:
// frames[0] {ch:2, name:"jpge"} decoded, SOI/EOI, Go image/jpeg decode
// to 320x240, sha256 taken agent-side, file written to dst. Returns
// whether the whole product path held. Retry-armored (the i2c-stuck
// first-init-after-idle transient: pcall gc2145 off + cooldown +
// retry — the P1 mod_init convention; every attempt counted in-log).
func b9RecipeShot(h *harness, b *b6Client, ctx context.Context, dst string, label string) bool {
	t0 := time.Now()
	var m map[string]any
	var text string
	isErr := true
	for att := 1; att <= 3; att++ {
		var ok bool
		m, text, isErr, ok = b.call(ctx, 60*time.Second, "xuansi_run_lua",
			map[string]any{"board": b9Board, "code": b9RecipeCode, "budget_ms": float64(10000)})
		_ = ok
		if !isErr {
			break
		}
		h.note("%s: recipe attempt %d refused (%.110s) - off + cooldown, retry", label, att, text)
		b9Lua(b, ctx, "local ok,e=pcall(bench.wasm.call,'gc2145','off') return 'parked'")
		time.Sleep(12 * time.Second)
	}
	dur := time.Since(t0).Round(time.Millisecond)
	if isErr {
		h.check(false, fmt.Sprintf("%s: the recipe run_lua failed (text=%.160s)", label, text))
		return false
	}
	val, _ := m["value"].(map[string]any)
	frames, _ := m["frames"].([]any)
	ok := h.check(len(val) > 0 && gAny(val, "init_ok") == float64(1) && gAny(val, "w") == float64(320) &&
		gAny(val, "off_ok") == float64(1),
		fmt.Sprintf("%s: recipe map {init_ok=1, 320x240, off_ok=1} (enc_ms=%v, shot_len=%v)",
			label, gAny(val, "enc_ms"), gAny(val, "shot_len")))
	ok = h.check(len(frames) == 1,
		fmt.Sprintf("%s: reply carries exactly one frame (frames=%d; frames_dropped=%v)",
			label, len(frames), gAny(m, "frames_dropped"))) && ok
	if len(frames) != 1 {
		return false
	}
	fm, _ := frames[0].(map[string]any)
	chVal, hasCh := b6Num(fm, "ch")
	ok = h.check(hasCh && chVal == 2 && b6Str(fm, "name") == "jpge",
		fmt.Sprintf("%s: frame[0] == {ch:2, name:\"jpge\"} (got ch=%v name=%q)", label, gAny(fm, "ch"), b6Str(fm, "name"))) && ok
	body, derr := base64.StdEncoding.DecodeString(b6Str(fm, "data_b64"))
	if derr != nil {
		h.check(false, fmt.Sprintf("%s: frames[0].data_b64 decode: %v", label, derr))
		return false
	}
	if len(body) < 4 {
		h.check(false, fmt.Sprintf("%s: landed JPEG is %dB - too short for SOI+EOI", label, len(body)))
		return false
	}
	ok = h.check(body[0] == 0xFF && body[1] == 0xD8 && body[len(body)-2] == 0xFF && body[len(body)-1] == 0xD9,
		fmt.Sprintf("%s: SOI FFD8 / EOI FFD9 in place (head % X tail % X)", label, body[:2], body[len(body)-2:])) && ok
	cfg, jerr := jpeg.DecodeConfig(strings.NewReader(string(body)))
	ok = h.check(jerr == nil && cfg.Width == 320 && cfg.Height == 240,
		fmt.Sprintf("%s: Go image/jpeg decodes to 320x240 (got %dx%d, err=%v)", label, cfg.Width, cfg.Height, jerr)) && ok
	shotLen, _ := b6Num(val, "shot_len")
	ok = h.check(float64(len(body)) == shotLen,
		fmt.Sprintf("%s: landed bytes == reply shot_len (%d == %v)", label, len(body), shotLen)) && ok
	if werr := os.WriteFile(dst, body, 0o644); werr != nil {
		h.check(false, fmt.Sprintf("%s: write %s: %v", label, dst, werr))
		return false
	}
	sum := sha256.Sum256(body)
	h.note("%s: landed %s (%d bytes, sha256=%x, wall=%v - agent-side decode+verify+sha256, no server capture path)",
		label, dst, len(body), sum, dur)
	return ok
}

func runB9Camera(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== b9 recipe HIL (v6): the agent camera recipe end to end - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("board: %s over TCP %s (daemon never opens CDC - that reboots); fw expect %s (protocol_ver 6)\n", b9Board, b9TCPEndp, b9FwBuild)
	fmt.Printf("discipline: evidence ASCII only; the recipe's JPEGs land in THIS harness's outdir (the agent lands its own files - captures/ is retired)\n")

	var serve, mcp *b6Proc
	defer func() { // die-safe: never leave processes or a lease behind
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int {
		code := h.finish("b9-recipe")
		if h.fail == 0 {
			fmt.Printf("=== ALL PASS - the agent recipe lands openable JPEGs through budget_ms + frames primitives, refusals honest, board as found (attempt %d, S0..S6) ===\n", attempt)
		}
		return code
	}

	// ---------- S0: prelude ----------
	fmt.Println("\n- S0: prelude - clean bench, board reachable, sandbox, serve+mcp up, v6 face (24 tools, zero camera) -")
	if out, err := exec.Command("pgrep", "-af", "xuansi").CombinedOutput(); err == nil {
		stale := 0
		resident := 0
		for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Fields(ln)
			// pgrep -af lines LEAD WITH THE PID: the binary is field 1
			// and the subcommand field 2. `serve` processes are the
			// interference vector (a daemon owns the board's controller
			// slot). The USER-SESSION RESIDENT on the standard socket
			// (/run/user/.../xuansi/daemon.sock) is noted, not failed:
			// it is the environment's own daemon (idle during this run
			// - the sweep below proves the audit is not starved); a
			// stray on any OTHER socket is a real stale.
			if len(f) >= 3 && strings.Contains(filepath.Base(f[1]), "xuansi") && f[2] == "serve" {
				if strings.Contains(ln, "/run/user/") && strings.Contains(ln, "xuansi/daemon.sock") {
					resident++
					fmt.Printf("  [note] user-session resident daemon present (standard socket, idle - tolerated): %s\n", asciiLine(ln))
					continue
				}
				stale++
				fmt.Printf("  [note] stale xuansi serve process: %s\n", asciiLine(ln))
			}
		}
		h.check(stale == 0, fmt.Sprintf("no stale xuansi serve daemon on the bench (found %d; the resident user-session daemon is noted separately)", stale))
	} else {
		h.note("pgrep: no xuansi processes found (clean bench)")
	}
	if conn, err := net.DialTimeout("tcp", b9TCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v (USB fallback OUT of scope - opening it reboots the board)", b9TCPEndp, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only - no reboot, no USB open)", b9TCPEndp))
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b9-recipe boards.conf: no aliases - the mac6 selector is used directly\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}
	h.note("XDG sandbox: XDG_DATA_HOME=%s (the real user workspace untouched)", xdg)

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
	h.check(ok, fmt.Sprintf("daemon sock bound at %s (%s)", sock, last))
	ok, last = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return serve.has("initial discovery sweep: 1 board(s)"), "sweep line"
	})
	h.check(ok, fmt.Sprintf("initial discovery sweep found 1 board (%s)", last))

	mcp, mcpR, mcpW := startB6MCP(h, daemonBin, sock, "XDG_DATA_HOME="+xdg)
	if mcp == nil {
		return finish()
	}
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b9-recipe-hil", Version: "1.0"},
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
		fmt.Sprintf("initialize handshake ok: server=%s version=%s (protocol %s)", initField(si, "name"), initField(si, "ver"), initField(si, "proto")))

	names, terr := b.toolNames(ctx)
	servedCam := 0
	for _, n := range names {
		if strings.Contains(n, "camera") {
			servedCam++
		}
	}
	h.check(terr == nil && len(names) == 24 && servedCam == 0,
		fmt.Sprintf("the v6 face: tools/list serves exactly 24 static tools, zero camera names (got %d tools, %d camera-named)", len(names), servedCam))

	m, text, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
	_ = text
	ok2 := !isErr
	// fw BUILD BINDING + no-lease precondition (read-only legs)
	good, last := pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		m, _, isErr, ok := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
		if !ok || isErr {
			return false, "list_boards trouble"
		}
		for _, e := range m["boards"].([]any) {
			if bm, yes := e.(map[string]any); yes && bm["mac6"] == b9Board {
				if fw, _ := bm["fw"].(string); fw != "" {
					return true, fw
				}
				return false, "fw not yet enriched"
			}
		}
		return false, "board not in list yet"
	})
	_ = ok2
	fw := last
	h.check(good && fw == b9FwBuild, fmt.Sprintf("fw BUILD BINDING: %q == expected %q (B12-P2 fullclean canonical)", fw, b9FwBuild))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b9Board})
	_, hasLease := m["lease"]
	h.check(!isErr && !hasLease, fmt.Sprintf("precondition: board starts with NO lease (lease key %v)", hasLease))
	// ---------- S1: lease FIRST, then the module pipeline ----------
	fmt.Println("\n- S1: lease FIRST (the holder ladder) -> /drv swept -> fetch x2 (live index, pinned) -> push x2 -> load x2 -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b9Board, "holder": b9Holder})
	leaseSess, _ := b6Num(m, "session")
	h.check(!isErr && b6Str(m, "board") == b9Board && b6Str(m, "holder") == b9Holder && leaseSess > 0,
		fmt.Sprintf("lease holder=%s session=%.0f (default TCP route, no reboot)", b9Holder, leaseSess))
	h.check(b6Str(m, "transport") == "" && b6Str(m, "warning") == "",
		fmt.Sprintf("lease took the DEFAULT TCP route: transport=%q warning=%q", b6Str(m, "transport"), b6Str(m, "warning")))

	// the no-frame shape (Review Focus #1 at this seat; post-lease per the ladder)
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_run_lua",
		map[string]any{"board": b9Board, "code": "return 1+1"})
	noFrames := m != nil
	_, hasFrames := m["frames"]
	_, hasDropped := m["frames_dropped"]
	h.check(!isErr && noFrames && gAny(m, "value") == float64(2) && !hasFrames && !hasDropped,
		fmt.Sprintf("no-stream eval reply: value=2, frames/frames_dropped keys ABSENT (value=%v frames-key=%v dropped-key=%v; text=%.60s)",
			gAny(m, "value"), hasFrames, hasDropped, text))

	// board-state convergence: unload whatever a previous run left
	// (b10t7Prelude convention) - this harness owns its own residue
	v, _, _ := b9Lua(b, ctx, "for _,n in ipairs(bench.wasm.list()) do bench.wasm.unload(n) end return 'cleaned'")
	_ = v
	b9Lua(b, ctx, "local ok,e=pcall(bench.wasm.call,'gc2145','off') return 'parked'")
	b.call(ctx, 15*time.Second, "xuansi_session_reset", map[string]any{"board": b9Board})
	for _, nm := range []string{"gc2145", "jpge", "ledwatch", "st7789"} {
		b.call(ctx, 15*time.Second, "xuansi_fs_rm", map[string]any{"board": b9Board, "path": "/drv/" + nm + ".wasm"})
	}
	gone := true
	for _, nm := range []string{"gc2145", "jpge", "ledwatch", "st7789"} {
		_, t, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_fs_read",
			map[string]any{"board": b9Board, "path": "/drv/" + nm + ".wasm"})
		if !(isErr && strings.Contains(t, "not found")) {
			h.note("/drv/%s not verified gone (%.60s)", nm, t)
			gone = false
		}
	}
	h.check(gone, "/drv swept before the recipe (fs_read not-found x4 - fs.ls is not on the v6 ops allowlist)")

	for _, spec := range []struct {
		name string
		sha  string
	}{{"gc2145", b9CamSHA}, {"jpge", b9JpgSHA}} {
		m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_fetch",
			map[string]any{"name": spec.name}) // version omitted = latest
		var fm map[string]any
		if fl, ok := m["files"].([]any); ok && len(fl) > 0 {
			fm, _ = fl[0].(map[string]any)
		}
		h.check(!isErr && b6Str(m, "version") == "1.0.0" && b6Str(fm, "sha256") == spec.sha,
			fmt.Sprintf("fetch %s default-latest from the LIVE index -> 1.0.0 sha=%.12s (pinned; text=%.50s)", spec.name, b6Str(fm, "sha256"), text))
		m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
			map[string]any{"board": b9Board, "name": spec.name, "version": "1.0.0"})
		h.check(!isErr && b6Str(m, "path") == "/drv/"+spec.name+".wasm" && b6Str(m, "sha256") == spec.sha,
			fmt.Sprintf("push %s@1.0.0 -> %s sha-verified (text=%.50s)", spec.name, b6Str(m, "path"), text))
		v, _, okLua := b9Lua(b, ctx, fmt.Sprintf("return bench.wasm.load('%s')", spec.name))
		h.check(okLua && v == "true", fmt.Sprintf("wasm.load('%s') -> %s", spec.name, v))
	}
	v, _, _ = b9Lua(b, ctx, "return table.concat(bench.wasm.list(), ',')")
	h.check(v == "gc2145,jpge", fmt.Sprintf("wasm.list == gc2145,jpge (got %q)", v))

	// ---------- S2: the recipe ----------
	fmt.Println("\n- S2: THE RECIPE - one budgeted run_lua chains load x2 -> init QVGA -> shot q60 -> off; frames carry the JPEG; the agent lands + verifies -")
	outDir := filepath.Join(runDir, "recipe-jpegs")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		h.check(false, "mkdir outdir: "+err.Error())
		return finish()
	}
	b9RecipeShot(h, b, ctx, filepath.Join(outDir, fmt.Sprintf("recipe-v6-attempt%d-cold.jpg", attempt)), "S2 cold recipe")

	// ---------- S3: warm repeat ----------
	fmt.Println("\n- S3: warm repeat - the recipe again: a distinct file, the warm-path wall noted -")
	time.Sleep(3 * time.Second) // sensor settle between cycles (F-4)
	b9RecipeShot(h, b, ctx, filepath.Join(outDir, fmt.Sprintf("recipe-v6-attempt%d-warm.jpg", attempt)), "S3 warm recipe")
	a, _ := os.ReadFile(filepath.Join(outDir, fmt.Sprintf("recipe-v6-attempt%d-cold.jpg", attempt)))
	c, _ := os.ReadFile(filepath.Join(outDir, fmt.Sprintf("recipe-v6-attempt%d-warm.jpg", attempt)))
	h.check(len(a) > 0 && len(c) > 0 && sha256.Sum256(a) != sha256.Sum256(c),
		fmt.Sprintf("cold and warm JPEGs are distinct files (cold=%dB warm=%dB, sha256 differ)", len(a), len(c)))

	// cold-board finish datum (taken HERE, before the S4 refusal matrix:
	// a release tears the lease session down with session.reset, which
	// sweeps the instances the evals loaded — the recipe's own off() is
	// the state being asserted, and it must be read before that sweep)
	vInfo, tInfo, okInfo := b9Lua(b, ctx, "return bench.wasm.call('gc2145','info')")
	h.check(okInfo && (strings.Contains(vInfo, "on:0") || strings.Contains(vInfo, "on=0")),
		fmt.Sprintf("gc2145.info() after the recipe's off(): on=0 (info=%.60s text=%.40s)", vInfo, tInfo))
	time.Sleep(3 * time.Second) // sensor settle before S4's legs

	// ---------- S4: the v6 refusal matrix ----------
	fmt.Println("\n- S4: refusals honest at every gate - the retired op face, the client budget domain, no-lease, the fw observer gate -")
	_, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_op",
		map[string]any{"board": b9Board, "op": "camera.status"})
	h.check(isErr && (strings.Contains(text, "outside the ops-face allowlist") || strings.Contains(text, "unknown op")),
		fmt.Sprintf("the retired op face: board_op camera.status is refused at the DAEMON's ops-face allowlist (the v6 de-camerad gate — the op never reaches the fw; text=%.90s)", text))
	_, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_run_lua",
		map[string]any{"board": b9Board, "code": "return 1", "budget_ms": float64(30001)})
	h.check(isErr && strings.Contains(text, "1..30000"),
		fmt.Sprintf("budget_ms=30001 refused client-side naming 1..30000 BEFORE any daemon contact (0 rides omitted by design; text=%.90s)", text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release",
		map[string]any{"board": b9Board, "holder": b9Holder})
	h.check(!isErr && b6Str(m, "holder") == b9Holder, fmt.Sprintf("release the lease (text=%.50s)", text))
	_, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_run_lua",
		map[string]any{"board": b9Board, "code": "return 1"})
	h.check(isErr && strings.HasPrefix(text, "not_held:"),
		fmt.Sprintf("post-release run_lua refused not_held (eval needs the lease; text=%.80s)", text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b9Board, "holder": b9Holder})
	lease2, _ := b6Num(m, "session")
	h.check(!isErr && lease2 > 0,
		fmt.Sprintf("re-lease under the SAME holder adopts the lease back inside the grace TTL (session %.0f -> %.0f)", leaseSess, lease2))

	oc, oerr := replclient.DialTCP(b9TCPEndp, replclient.DialOpts{})
	if oerr != nil {
		h.check(false, fmt.Sprintf("observer dial %s: %v", b9TCPEndp, oerr))
	} else {
		oinfo, herr := oc.Hello(true, "b9-recipe-observer")
		if herr != nil {
			h.check(false, fmt.Sprintf("observer hello: %v", herr))
		} else {
			h.check(oinfo.Role == "observer" && oinfo.ProtocolVer == 6,
				fmt.Sprintf("direct TCP hello as observer granted role=%s ver=%d (the daemon's controller untouched)", oinfo.Role, oinfo.ProtocolVer))
			_, oerr := oc.Op(ctx, "eval", map[string]any{"code": "return 1"})
			var oe *replclient.OpError
			if errors.As(oerr, &oe) {
				h.check(oe.Class == "protocol",
					fmt.Sprintf("observer eval REFUSED by the fw gate: class=%s msg=%.80s (eval is controller-only)", oe.Class, oe.Msg))
			} else {
				h.check(false, fmt.Sprintf("observer eval err = %v, want the fw's protocol-class refusal", oerr))
			}
		}
		oc.Close()
	}

	// ---------- S5: cold-board finish ----------
	fmt.Println("\n- S5: cold-board finish - pins free (off + session reset; the info datum was read at S3's tail before the lease cycle) -")
	capCh := b.callAsync("xuansi_capture", map[string]any{"board": b9Board, "source": "stdout", "seconds": float64(4)})
	time.Sleep(700 * time.Millisecond)
	b.call(ctx, 30*time.Second, "xuansi_board_op", map[string]any{"board": b9Board, "op": "diag.pins"})
	cres := <-capCh
	capData, rdErr := os.ReadFile(b6Str(cres.m, "path"))
	if rdErr != nil {
		if cres.ok && b6Str(cres.m, "path") == "" {
			h.note("pins dump: capture returned no path (dump empty - zero claims; treated as clean)")
			capData = []byte{}
		} else {
			h.check(false, fmt.Sprintf("pins dump capture readable (%v)", rdErr))
		}
	}
	cont := asciiLine(string(capData))
	busy := []string{}
	for _, pin := range b9CameraPins {
		if strings.Contains(cont, pin) {
			busy = append(busy, pin)
		}
	}
	dvpBusy := []string{}
	for _, pin := range busy {
		if pin != "GPIO4" && pin != "GPIO5" {
			dvpBusy = append(dvpBusy, pin)
		}
	}
	h.check(len(dvpBusy) == 0,
		fmt.Sprintf("after off(): DVP+XCLK pins all free (busy=%v; GPIO4/5 persistence is the documented F-3 SCCB exception)", dvpBusy))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_session_reset", map[string]any{"board": b9Board})
	h.check(!isErr, fmt.Sprintf("session_reset{0} (the F-3 claims converge here; text=%.50s)", text))
	capCh = b.callAsync("xuansi_capture", map[string]any{"board": b9Board, "source": "stdout", "seconds": float64(4)})
	time.Sleep(700 * time.Millisecond)
	b.call(ctx, 30*time.Second, "xuansi_board_op", map[string]any{"board": b9Board, "op": "diag.pins"})
	cres = <-capCh
	capData, rdErr = os.ReadFile(b6Str(cres.m, "path"))
	if rdErr == nil {
		cont = asciiLine(string(capData))
		busy = busy[:0]
		for _, pin := range b9CameraPins {
			if strings.Contains(cont, pin) {
				busy = append(busy, pin)
			}
		}
		h.check(len(busy) == 0, fmt.Sprintf("after session_reset{0}: none of the 14 camera pins held (busy=%v)", busy))
	}

	// ---------- S6: as-found + teardown ----------
	fmt.Println("\n- S6: as-found + teardown - /drv swept, release, no lease, mcp EOF, serve SIGINT -")
	for _, nm := range []string{"gc2145", "jpge", "ledwatch", "st7789"} {
		b9Lua(b, ctx, fmt.Sprintf("pcall(bench.wasm.unload,'%s')", nm))
		_, text, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_fs_rm",
			map[string]any{"board": b9Board, "path": "/drv/" + nm + ".wasm"})
		h.check(!isErr || strings.Contains(text, "No such file"),
			fmt.Sprintf("fs_rm /drv/%s.wasm (removed or already gone; text=%.50s)", nm, text))
	}
	v, _, _ = b9Lua(b, ctx, "return #bench.wasm.list()")
	h.check(v == "0", fmt.Sprintf("wasm list empty after unload x2 (list=%s)", v))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_op",
		map[string]any{"board": b9Board, "op": "budget.set", "args": map[string]any{"eval_ms": 200, "event_ms": 50}})
	h.check(!isErr, "session budget at defaults 200/50 (the recipe never touched it - budget_ms is per-call)")
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_op",
		map[string]any{"board": b9Board, "op": "caps"})
	limits, _ := m["limits"].(map[string]any)
	ebVal, hasEB := b6Num(limits, "eval_budget_ms")
	h.check(!isErr && limits != nil && hasEB && ebVal == 200,
		fmt.Sprintf("caps limits.eval_budget_ms == 200 (per-call override left no session drift; got %v)", gAny(limits, "eval_budget_ms")))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release", map[string]any{"board": b9Board, "holder": b9Holder})
	h.check(!isErr, fmt.Sprintf("release the lease (text=%.50s)", text))
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b9Board})
	_, hasLease2 := m["lease"]
	h.check(!isErr && !hasLease2, fmt.Sprintf("post-release board_status shows NO lease (lease key %v)", hasLease2))

	if err := cs.Close(); err != nil {
		h.note("client session close: %v (proceeding to EOF wait)", err)
	}
	select {
	case <-mcp.exited:
		code, werr := mcp.exitStats()
		// code 0 = clean EOF; SIGPIPE-after-EOF = the child died writing
		// an in-flight notification (resource-updated push) into our
		// already-closed read end — a teardown race, not a leak (the
		// process IS gone); recorded honestly either way
		cleanEOF := code == 0 && werr == nil
		pipeRace := code == -1 && werr != nil && strings.Contains(werr.Error(), "broken pipe")
		h.check(cleanEOF || pipeRace,
			fmt.Sprintf("xuansi mcp exited on stdin EOF (code=%d waitErr=%v; %s)", code, werr,
				map[bool]string{true: "clean", false: "post-close notification SIGPIPE race - process gone, no leak"}[cleanEOF]))
	case <-time.After(8 * time.Second):
		h.check(false, "xuansi mcp did not exit within 8s of the client close (EOF on stdin)")
		mcp.kill("mcp EOF timeout")
	}
	if !serve.sigintAndWait(h, 20*time.Second) {
		return finish()
	}
	code, werr := serve.exitStats()
	h.check(code == 0 && werr == nil, fmt.Sprintf("daemon exited clean (code=%d waitErr=%v)", code, werr))
	h.check(serve.shutdownDone(), "daemon's own final log line: 'shutdown complete' (no goroutine-leak complaint)")
	_, serr := os.Stat(sock)
	h.check(os.IsNotExist(serr), fmt.Sprintf("sock file removed at exit (stat err=%v)", serr))

	jpegs := 0
	if entries, err := os.ReadDir(outDir); err == nil {
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".jpg") {
				continue
			}
			jpegs++
			p := filepath.Join(outDir, e.Name())
			body, _ := os.ReadFile(p)
			cfg, derr := jpeg.DecodeConfig(strings.NewReader(string(body)))
			dims := fmt.Sprintf("%dx%d", cfg.Width, cfg.Height)
			if derr != nil {
				dims = "decode-error"
			}
			h.note("  LANDED JPEG (agent-landed, left in place): %s  bytes=%d  sha256=%x  dims=%s", p, len(body), sha256.Sum256(body), dims)
		}
	}
	h.check(jpegs == 2, fmt.Sprintf("outdir holds exactly the two recipe JPEGs of S2/S3 (found %d) - the captures/ convention is retired; these are the agent's own files", jpegs))
	return finish()
}

// b9Lua evals one Lua snippet; returns (value-as-string, text, ok).
func b9Lua(b *b6Client, ctx context.Context, code string) (string, string, bool) {
	m, text, isErr, _ := b.call(ctx, 40*time.Second, "xuansi_run_lua",
		map[string]any{"board": b9Board, "code": code})
	if isErr {
		return "", text, false
	}
	return fmt.Sprint(gAny(m, "value")), text, true
}

// bolAny pulls a bool from a decoded JSON map.
func bolAny(m map[string]any, k string) bool {
	v, _ := m[k].(bool)
	return v
}

// silence the io import when image/jpeg's transitive needs change
var _ = io.Discard
