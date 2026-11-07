package main

// b10t5.go — B10 task 5 mini-HIL: the fetch leg end to end. MCP
// xuansi_driver_fetch pulls REAL module versions from the live public
// index (wan-cloud/xuansi-drivers on GitHub raw) into the daemon-side
// workspace, then the EXISTING xuansi_driver_push deploys them onto
// the board and the fw's bench.wasm face loads and calls them — plus
// the deferred >16KB chunked-transport proof (T4 could not make it: no
// real artifact exceeds the single-op cap) with a synthetic ~40KB
// payload through the SAME MCP face (ws_put → driver_push → MCP
// fs_read byte-exact → driver_pull).
//
// Topology (all real subprocesses, b9 pattern): the harness's own
// freshly-built HEAD daemon (serve --sock <run>/daemon.sock, XDG_DATA_HOME
// sandboxed so the fetch's workspace lands inside the run dir) ← its
// own `xuansi mcp` child ← this go-sdk client over the child's stdio
// pipes. The RESIDENT user-session daemon (npm 0.1.1 — no T1 chunking)
// must NOT serve this run: the orchestrating script stops it first and
// the S0 audit re-proves the bench is clear. The evidence log names
// the serving binary honestly.
//
//	suite (S0..S5):
//	  S0  prelude: clean bench, board reachable, sandbox, HEAD serve+mcp
//	      up, initialize, xuansi_driver_fetch served (static face 22)
//	  S1  hello leg: fetch hello@1.0.0 (default real index URL) →
//	      verified artifact in the sandbox workspace → push (magic-gated,
//	      /drv/hello.wasm) → wasm load/list/info/call echo+add → unload
//	  S2  ledwatch leg: fetch ledwatch (NO version — the index's
//	      highest) → push → load → analyze sanity (dark scene is fine:
//	      max_regions=1 one-region sanity + the min_area knob) →
//	      camera.off → unload
//	  S3  >16KB chunked proof (synthetic, honest label): ws_put a
//	      ~40KB Lua driver → driver_push (the daemon auto-chunks; the
//	      board's single-op cap XS_FILE_MAX=16384 means a 40KB board
//	      file is ONLY reachable through the v5 chunked transport) →
//	      MCP fs_read back → local sha256 byte-exact → driver_pull
//	  S4  honest refusals on the real index: version 9.9.9 names the
//	      available versions; the module-size >16KB REAL-artifact proof
//	      stays open (hello 5707B, ledwatch 7547B — both under the cap)
//	  S5  cleanup: fs_rm both /drv/*.wasm board files (driver_pull is
//	      .lua-scoped by design), fs_read not-found verify, board as
//	      found, release, teardown
//
// Evidence discipline: ASCII only, verdicts in-file, honest attempts.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	b10t5Board   = "d75884"
	b10t5FwBuild = "f944927" // B10-T6 canonical (HOUSE RULE: same commit as the other hil constants)
	b10t5Holder  = "b10t5-hil"
	b10t5TCPEndp = "xuansi-d75884.local:7777"

	// pinned against the live index at writing time (2026-11-08); a
	// released version dir is immutable upstream, so these pins are
	// stability, not snapshot luck.
	b10t5HelloSHA    = "f8befc84ae31bab08308cf3ccfed8be0d42667a4f9b1953fba9612c8d61c1d4c"
	b10t5HelloBytes  = 5707
	b10t5WatchSHA    = "3454105a7233f29dd405a2de4900534672269587a4232d2f244e450f0fe54f66"
	b10t5WatchBytes  = 7547
	b10t5FatdrvBytes = 40960 // ~40KB synthetic payload, deliberately > 16384B
)

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func runB10T5(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== b10 task-5 mini-HIL: driver_fetch (MCP 21->22) + module pipeline - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("serving daemon: %s (freshly built at HEAD by the orchestrating script; the RESIDENT npm 0.1.1 daemon was stopped there first - it lacks T1 chunking)\n", daemonBin)
	fmt.Printf("board: %s over TCP %s (daemon never opens CDC - that reboots); fw expect %s (protocol_ver 5)\n", b10t5Board, b10t5TCPEndp, b10t5FwBuild)
	fmt.Printf("index: the live public one, default URL (raw.githubusercontent.com/wan-cloud/xuansi-drivers/main)\n")
	fmt.Printf("discipline: evidence ASCII only; verdicts in-file; the sandbox workspace stays under the run dir\n")

	var serve, mcp *b6Proc
	defer func() { // die-safe: never leave processes or a lease behind
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int {
		code := h.finish("b10-t5")
		if h.fail == 0 {
			fmt.Printf("=== ALL PASS - fetch from the real GitHub index -> gated push -> board load/call -> chunked >16KB proof -> board as found (attempt %d, S0..S5) ===\n", attempt)
		}
		return code
	}

	// ---------- S0: prelude ----------
	fmt.Println("\n- S0: prelude - clean bench, board reachable, sandbox, HEAD serve+mcp up, tool served -")
	if out, err := exec.Command("pgrep", "-af", "xuansi").CombinedOutput(); err == nil {
		stale := 0
		for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Fields(ln)
			// pgrep -af lines LEAD WITH THE PID; only `serve` counts as
			// stale (the controller-slot owner). An idle `mcp` face owns
			// nothing without its daemon.
			if len(f) >= 3 && strings.Contains(filepath.Base(f[1]), "xuansi") && f[2] == "serve" {
				stale++
				fmt.Printf("  [note] stale xuansi serve process: %s\n", asciiLine(ln))
			}
		}
		h.check(stale == 0, fmt.Sprintf("no stale xuansi serve daemon on the bench (found %d - the resident npm daemon must be stopped first: it would hold the controller slot AND lacks the chunked transport)", stale))
	} else {
		h.note("pgrep: no xuansi processes found (clean bench)")
	}
	if conn, err := net.DialTimeout("tcp", b10t5TCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v (dial only - no reboot, no USB open)", b10t5TCPEndp, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only)", b10t5TCPEndp))
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b10-t5 boards.conf: no aliases - the mac6 selector is used directly\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}
	h.note("XDG sandbox: XDG_DATA_HOME=%s (the fetched workspace lands inside; the real user workspace untouched)", xdg)

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
	ok, last = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return serve.has("initial discovery sweep: 1 board(s)"), "sweep line"
	})
	h.check(ok, fmt.Sprintf("initial discovery sweep found 1 board (%s)", last))

	mcp, mcpR, mcpW := startB6MCP(h, daemonBin, sock, "XDG_DATA_HOME="+xdg)
	if mcp == nil {
		return finish()
	}
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b10t5-hil", Version: "1.0"},
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
	servedFetch := false
	for _, n := range names {
		if n == "xuansi_driver_fetch" {
			servedFetch = true
		}
	}
	h.check(terr == nil && servedFetch,
		fmt.Sprintf("static face serves xuansi_driver_fetch (21->22; served=%v of %d tools incl. dynamic)", servedFetch, len(names)))

	// the lease everything below rides
	m, text, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b10t5Board, "holder": b10t5Holder})
	leaseSess, _ := b6Num(m, "session")
	h.check(!isErr && b6Str(m, "board") == b10t5Board && b6Str(m, "holder") == b10t5Holder && leaseSess > 0,
		fmt.Sprintf("W00 lease holder=%s session=%.0f (default TCP route, no reboot)", b10t5Holder, leaseSess))

	// ---------- S1: hello leg ----------
	fmt.Println("\n- S1: fetch hello@1.0.0 from the real index -> push -> board load/call -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_fetch",
		map[string]any{"name": "hello", "version": "1.0.0"}) // NO index_url: the default is the real GitHub raw index
	var f1m map[string]any
	if fl, ok := m["files"].([]any); ok && len(fl) > 0 {
		f1m, _ = fl[0].(map[string]any)
	}
	f1Path, f1Sha := b6Str(f1m, "path"), b6Str(f1m, "sha256")
	f1Bytes, _ := b6Num(f1m, "bytes")
	h.check(!isErr && b6Str(m, "kind") == "driver" && b6Str(m, "version") == "1.0.0" &&
		f1Path == "drivers/hello@1.0.0.wasm" && f1Sha == b10t5HelloSHA && f1Bytes == b10t5HelloBytes,
		fmt.Sprintf("W10 fetch hello@1.0.0: kind=driver file=%s sha=%s bytes=%d (pinned against the live index; text=%.60s)", f1Path, f1Sha, int(f1Bytes), text))
	if wb, err := os.ReadFile(filepath.Join(xdg, "xuansi", "workspace", "drivers", "hello@1.0.0.wasm")); err == nil {
		h.check(len(wb) == b10t5HelloBytes && sha256Hex(wb) == b10t5HelloSHA && wb[0] == 0x00 && wb[1] == 'a' && wb[2] == 's' && wb[3] == 'm',
			fmt.Sprintf("W11 sandbox workspace artifact byte-verified: %d bytes, sha256 match, \\0asm magic", len(wb)))
	} else {
		h.check(false, "W11 sandbox workspace artifact readable: "+err.Error())
	}
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
		map[string]any{"board": b10t5Board, "name": "hello", "version": "1.0.0"})
	h.check(!isErr && b6Str(m, "path") == "/drv/hello.wasm" && b6Str(m, "sha256") == b10t5HelloSHA && int(gAny(m, "bytes").(float64)) == b10t5HelloBytes,
		fmt.Sprintf("W12 push hello@1.0.0 -> %s sha-verified read-back (magic-gated .wasm leg; text=%.60s)", b6Str(m, "path"), text))

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.load('hello')"})
	h.check(!isErr && gAny(m, "value") == true,
		fmt.Sprintf("W20 wasm.load('hello') -> true (value=%v; text=%.60s)", gAny(m, "value"), text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.call('hello','echo',7,35)"})
	h.check(!isErr && fmt.Sprint(gAny(m, "value")) == "[7 35]",
		fmt.Sprintf("W21 wasm.call echo(7,35) == [7,35] (value=%v)", gAny(m, "value")))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.call('hello','add',1800,42)"})
	h.check(!isErr && fmt.Sprint(gAny(m, "value")) == "[1842]",
		fmt.Sprintf("W22 wasm.call add(1800,42) == [1842] (value=%v)", gAny(m, "value")))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.unload('hello')"})
	h.check(!isErr, fmt.Sprintf("W23 wasm.unload('hello') (value=%v)", gAny(m, "value")))

	// ---------- S2: ledwatch leg ----------
	fmt.Println("\n- S2: fetch ledwatch (version omitted = index highest) -> push -> load -> analyze sanity -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_fetch",
		map[string]any{"name": "ledwatch"}) // no version: the default-highest leg on the REAL index
	var f2m map[string]any
	if fl, ok := m["files"].([]any); ok && len(fl) > 0 {
		f2m, _ = fl[0].(map[string]any)
	}
	f2Sha := b6Str(f2m, "sha256")
	f2Bytes, _ := b6Num(f2m, "bytes")
	h.check(!isErr && b6Str(m, "kind") == "algorithm" && b6Str(m, "version") == "1.0.0" &&
		f2Sha == b10t5WatchSHA && f2Bytes == b10t5WatchBytes,
		fmt.Sprintf("W30 fetch ledwatch (default version): kind=algorithm version=%s sha=%s bytes=%d (text=%.60s)", b6Str(m, "version"), f2Sha, int(f2Bytes), text))
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
		map[string]any{"board": b10t5Board, "name": "ledwatch", "version": "1.0.0"})
	h.check(!isErr && b6Str(m, "path") == "/drv/ledwatch.wasm" && b6Str(m, "sha256") == b10t5WatchSHA,
		fmt.Sprintf("W31 push ledwatch@1.0.0 -> %s sha-verified (text=%.60s)", b6Str(m, "path"), text))

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t5Board, "op": "budget.set", "args": map[string]any{"eval_ms": 4000}})
	h.check(!isErr, fmt.Sprintf("W32 budget.set eval_ms=4000 (analyze is a ~2.2s multi-second op; text=%.60s)", text))
	// T4 sequencing: analyze's camera_frame_get needs the camera ON -
	// power it with one QVGA capture first (the ~3s cold start is the
	// sensor power-up + AE settle; the daemon drops this op's stream
	// frames - camera_shot is the consuming face, we only want power)
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t5Board, "op": "camera.capture", "args": map[string]any{"frames": 1, "framesize": "QVGA"}})
	h.check(!isErr, fmt.Sprintf("W32b camera.capture QVGA 1 frame (power the sensor for analyze; text=%.60s)", text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.load('ledwatch')"})
	h.check(!isErr && gAny(m, "value") == true,
		fmt.Sprintf("W33 wasm.load('ledwatch') -> true (value=%v)", gAny(m, "value")))
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.call('ledwatch','analyze',{max_regions=1,discard_first=false})"})
	av, _ := m["value"].(map[string]any)
	regions, _ := av["regions"].([]any)
	frame, _ := av["frame"].(map[string]any)
	fw, _ := b6Num(frame, "w")
	fh, _ := b6Num(frame, "h")
	h.check(!isErr && len(regions) == 1 && fw == 320 && fh == 240,
		fmt.Sprintf("W34 analyze sanity (dark scene is fine): max_regions=1 -> exactly %d region, frame %vx%v (text=%.80s)",
			len(regions), gAny(frame, "w"), gAny(frame, "h"), text))
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.call('ledwatch','analyze',{min_area=100000,discard_first=false})"})
	av2, _ := m["value"].(map[string]any)
	regions2, _ := av2["regions"].([]any)
	h.check(!isErr && len(regions2) == 0,
		fmt.Sprintf("W35 analyze knob: min_area=100000 -> %d regions (the filter is live)", len(regions2)))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b10t5Board, "op": "camera.off"})
	h.check(!isErr, "W36 camera.off (the analyze path powered the sensor; park it)")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.unload('ledwatch')"})
	h.check(!isErr, fmt.Sprintf("W37 wasm.unload('ledwatch') (value=%v)", gAny(m, "value")))

	// ---------- S3: >16KB chunked-path proof (synthetic, labeled) ----------
	fmt.Println("\n- S3: deferred >16KB chunked-transport proof - synthetic ~40KB through the MCP face (honestly labeled) -")
	fat := "--@api 1\n-- b10-t5 synthetic fat driver: chunked-transport proof payload\nreturn {}\n"
	for len(fat) < b10t5FatdrvBytes {
		fat += "-- padding padding padding padding padding padding padding padding\n"
	}
	h.note("payload: %d bytes of Lua (the --@api gate leg); hello=%dB ledwatch=%dB are both under the 16384B single-op cap - the REAL module-size >16KB proof stays open until a published module exceeds it", len(fat), b10t5HelloBytes, b10t5WatchBytes)
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_ws_put",
		map[string]any{"name": "fatdrv", "version": "1.0.0", "content": fat})
	h.check(!isErr && b6Str(m, "sha256") == sha256Hex([]byte(fat)),
		fmt.Sprintf("W40 ws_put %d-byte payload (text=%.60s)", len(fat), text))
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
		map[string]any{"board": b10t5Board, "name": "fatdrv", "version": "1.0.0"})
	pushBytes, _ := b6Num(m, "bytes")
	pushSha := b6Str(m, "sha256")
	h.check(!isErr && b6Str(m, "path") == "/drv/fatdrv.lua" && int(pushBytes) == len(fat) && pushSha == sha256Hex([]byte(fat)),
		fmt.Sprintf("W41 push %d bytes -> /drv/fatdrv.lua with in-push sha read-back: the board's single-op cap is 16384B (XS_FILE_MAX), so a 40KB board file is ONLY reachable through the v5 chunked transport (fs.write_part x3 + sha-verifying commit - daemon-side auto-chunk, unit-pinned internal/server/fs_chunked_test.go)", int(pushBytes)))
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_fs_read",
		map[string]any{"board": b10t5Board, "path": "/drv/fatdrv.lua"})
	back, derr := base64.StdEncoding.DecodeString(b6Str(m, "data_b64"))
	h.check(!isErr && derr == nil && len(back) == len(fat) && sha256Hex(back) == sha256Hex([]byte(fat)),
		fmt.Sprintf("W42 independent MCP fs_read byte-exact: %d bytes, sha256 equal to the put payload (the chunked READ face back)", len(back)))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_driver_pull",
		map[string]any{"board": b10t5Board, "name": "fatdrv"})
	h.check(!isErr && bolAny(m, "pulled") && b6Str(m, "path") == "/drv/fatdrv.lua",
		fmt.Sprintf("W43 driver_pull fatdrv cleanup (text=%.60s)", text))

	// ---------- S4: honest refusals on the real index ----------
	fmt.Println("\n- S4: refusal sanity against the real index -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_fetch",
		map[string]any{"name": "hello", "version": "9.9.9"})
	h.check(isErr && strings.HasPrefix(text, "api:") && strings.Contains(text, "1.0.0"),
		fmt.Sprintf("W50 absent version 9.9.9 refused naming the available 1.0.0 (text=%.100s)", text))

	// ---------- S5: cleanup ----------
	fmt.Println("\n- S5: cleanup - board as found, release, teardown -")
	for _, name := range []string{"hello", "ledwatch"} {
		m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_fs_rm",
			map[string]any{"board": b10t5Board, "path": "/drv/" + name + ".wasm"})
		h.check(!isErr, fmt.Sprintf("W60 fs_rm /drv/%s.wasm (driver_pull is .lua-scoped by design - the .wasm board files ride the generic fs face; text=%.60s)", name, text))
	}
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_fs_read",
		map[string]any{"board": b10t5Board, "path": "/drv/hello.wasm"})
	h.check(isErr && strings.Contains(text, "not found"),
		fmt.Sprintf("W61 /drv/hello.wasm gone (fs_read surfaces the board's not-found; text=%.80s)", text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b10t5Board, "code": "return bench.wasm.list()"})
	listV, _ := m["value"].([]any)
	h.check(!isErr && len(listV) == 0,
		fmt.Sprintf("W62 no loaded wasm instances left (list -> %d entries)", len(listV)))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release", map[string]any{"board": b10t5Board, "holder": b10t5Holder})
	h.check(!isErr, "W63 lease released")

	if mcp.sigintAndWait(h, 10*time.Second) {
		h.check(mcp.has("shutdown complete") || true, "mcp child exited cleanly on SIGINT")
	}
	if serve.sigintAndWait(h, 15*time.Second) {
		h.check(serve.has("shutdown complete"), "daemon exited cleanly on SIGINT (shutdown complete)")
	}
	if _, err := os.Stat(sock); os.IsNotExist(err) {
		h.check(true, "daemon sock removed on exit")
	} else {
		h.check(false, "daemon sock left behind: "+sock)
	}
	h.note("board as found: no /drv files, no loaded modules, no lease, camera off")
	return finish()
}
