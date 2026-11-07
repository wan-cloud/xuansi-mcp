// b8-t1.go - batch 8 task 1 mini-HIL: the pipeline's REMOVAL path
// (fs_rm + xuansi_driver_pull) on the real bench board. The b7 full
// HIL's honest finding was that the MCP face could deploy and read
// back drivers but had NO way to clean /drv (an out-of-pipeline
// direct REPL connection had to do it); this capture proves the gap is
// closed end to end, in pipeline, and that the board is left as found.
//
// Three real process layers (b6 house style): go-sdk MCP client over
// os.Pipe <-> `xuansi mcp` <-> `xuansi serve` <-> the real board over
// TCP (xuansi-d75884.local:7777; the CDC port is NEVER opened - that
// reboots the board). XDG-sandboxed so the real user workspace is
// untouched.
//
// Steps:
//
//	S0  prelude - no stale processes, board TCP reachable, XDG sandbox,
//	    serve up + discovery sweep, mcp up + initialize, list_boards
//	    shows d75884 fw 775ecaa, and both /drv targets are absent
//	    (honest precondition, fs_read not-found)
//	S1  lease - holder b8-t1, default TCP route
//	S2  fs removal leg - fs_write /drv/b8tmp.lua (small --@api 1
//	    file) -> fs_read verifies the bytes -> xuansi_fs_rm removes
//	    it -> fs_read not-found is the GONE proof (the MCP face has
//	    no fs.ls; the board's api error is the evidence)
//	S3  whitelist negatives - traversal, the bare /drv root, a
//	    trailing slash and a non-whitelisted prefix are all refused
//	    CLIENT-SIDE with the api-class message
//	S4  driver lifecycle - ws_put b8pull@1.0.0 -> driver_push -> eval
//	    require works -> driver_pull -> ws_list: active ABSENT,
//	    version + sha still listed (history never lies) -> fs_read
//	    /drv/b8pull.lua not-found -> ROLLBACK: re-push 1.0.0
//	    (explicit - Active was cleared) -> hot-swap require works
//	    again -> driver_pull again
//	S5  cleanup + honesty - xuansi_fs_rm /drv/b8tmp.lua hits the
//	    board's api not-found VERBATIM (already removed at S2 - the
//	    passthrough proof on the real board), release, board_status
//	    shows no lease, mcp EOF exit 0, serve SIGINT exit 0,
//	    sandbox accounting
//
// Single capture, numbered PASS/FAIL per check, ASCII only (the
// batch-8 evidence discipline).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	b8Board   = "d75884"  // the bench board
	b8FwBuild = "db1fd12" // T8 fix round canonical (camera switch self-deadlock fix + settle + silent-panic + RTC WDT backstop)
	b8Holder  = "b8-t1"   // custom lease holder for this run
	b8TCPEndp = "xuansi-d75884.local:7777"
	b8TmpPath = "/drv/b8tmp.lua"  // the S2 fs-face target
	b8DrvName = "b8pull"          // the S4 driver-lifecycle name
	b8DrvPath = "/drv/b8pull.lua" // its effective board file
)

// b8TmpBody is the S2 fs-face payload: a small --@api 1 file (the
// driver-gate shape, though nothing requires it - it is just a tiny
// valid file).
const b8TmpBody = "--@api 1\n-- b8-t1 temporary fs_rm target\nreturn {}\n"

// b8DrvBody is the S4 driver: tiny but a real loadable module with a
// version marker the eval verifies.
const b8DrvBody = "--@api 1\n-- b8-t1 pull lifecycle driver\nlocal M = {}\nM.version = \"1.0.0\"\nfunction M.tag() return \"b8pull-ok\" end\nreturn M\n"

func b8SHA(s string) string {
	sum := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", sum)
}

// b8B64 is the plain standard base64 of s (the fs tools' data_b64).
func b8B64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func runB8T1(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== b8 task-1 mini-HIL: the removal path (fs_rm + driver_pull) - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("board: %s over TCP %s (CDC never opened - that reboots the board); fw expect %s\n", b8Board, b8TCPEndp, b8FwBuild)
	fmt.Printf("discipline: evidence ASCII only; the board is left AS FOUND (no /drv files, no lease)\n")

	var serve, mcp *b6Proc
	defer func() { // die-safe: never leave processes or a lease behind
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int {
		code := h.finish("b8-t1")
		if h.fail == 0 {
			fmt.Printf("=== ALL PASS - the pipeline can now REMOVE what it deploys: fs_rm through the MCP face, driver_pull with history kept, board as found (attempt %d, S0..S5) ===\n", attempt)
		}
		return code
	}

	// ---------- S0: prelude ----------
	fmt.Println("\n- S0: prelude - clean bench, board reachable, sandbox, serve+mcp up -")
	if out, err := exec.Command("pgrep", "-af", "xuansi").CombinedOutput(); err == nil {
		stale := 0
		for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Fields(ln)
			if len(f) >= 2 && strings.Contains(filepath.Base(f[0]), "xuansi") && (f[1] == "serve" || f[1] == "mcp") {
				stale++
				fmt.Printf("  [note] stale xuansi process: %s\n", asciiLine(ln))
			}
		}
		h.check(stale == 0, fmt.Sprintf("no stale xuansi serve/mcp processes on the bench (found %d)", stale))
	} else {
		h.note("pgrep: no xuansi processes found (clean bench)")
	}
	if conn, err := net.DialTimeout("tcp", b8TCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v (USB fallback OUT of scope - opening it reboots the board)", b8TCPEndp, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only - no reboot, no USB open)", b8TCPEndp))
	}
	h.note("payloads: b8tmp %dB sha=%.16s... / b8pull %dB sha=%.16s... (first lines --@api 1)",
		len(b8TmpBody), b8SHA(b8TmpBody), len(b8DrvBody), b8SHA(b8DrvBody))

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b8-t1 boards.conf: no aliases - the mac6 selector is used directly\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}
	h.note("XDG sandbox: XDG_DATA_HOME=%s (the mcp workspace + any captures land inside; the real user workspace untouched)", xdg)

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
	h.check(serve.has("daemon serving"), fmt.Sprintf("daemon reports serving (grace-ttl %s)", b6GraceTTL))
	ok, last = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return serve.has("initial discovery sweep: 1 board(s)"), "sweep line"
	})
	h.check(ok, fmt.Sprintf("initial discovery sweep found 1 board (%s)", last))

	mcp, mcpR, mcpW := startB6MCP(h, daemonBin, sock, "XDG_DATA_HOME="+xdg)
	if mcp == nil {
		return finish()
	}
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b8-hil", Version: "1.0"},
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

	// the two new tools are served
	names, terr := b.toolNames(ctx)
	servedFsRm, servedPull := false, false
	for _, n := range names {
		if n == "xuansi_fs_rm" {
			servedFsRm = true
		}
		if n == "xuansi_driver_pull" {
			servedPull = true
		}
	}
	h.check(terr == nil && servedFsRm && servedPull,
		fmt.Sprintf("static face serves xuansi_fs_rm + xuansi_driver_pull (fs_rm=%v pull=%v of %d tools)", servedFsRm, servedPull, len(names)))

	ok, last = pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		m, _, isErr, ok := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
		if !ok || isErr {
			return false, fmt.Sprintf("list_boards trouble (isErr=%v)", isErr)
		}
		for _, e := range m["boards"].([]any) {
			if bm, yes := e.(map[string]any); yes && bm["mac6"] == b8Board {
				if fw, _ := bm["fw"].(string); fw != "" {
					return true, "fw populated (hello-enriched)"
				}
				return false, "board listed, fw not yet enriched"
			}
		}
		return false, "board not in list yet"
	})
	h.check(ok, fmt.Sprintf("list_boards shows %s with fw populated (%s)", b8Board, last))
	m, text, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
	fw := ""
	for _, e := range m["boards"].([]any) {
		if bm, _ := e.(map[string]any); bm["mac6"] == b8Board {
			fw, _ = bm["fw"].(string)
		}
	}
	h.check(!isErr && fw == b8FwBuild, fmt.Sprintf("fw BUILD BINDING: %q == expected %q (text=%.60s)", fw, b8FwBuild, text))

	// honest precondition: both /drv targets absent (a leftover from an
	// older run is removed through the NEW tool - cleanup toward as-found)
	for _, p := range []string{b8TmpPath, b8DrvPath} {
		m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_fs_read", map[string]any{"board": b8Board, "path": p})
		if !isErr {
			h.note("precondition: %s PRESENT (older leftover) - removing it through xuansi_fs_rm before the run (text=%.40s)", p, text)
			m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_fs_rm", map[string]any{"board": b8Board, "path": p})
			h.check(!isErr, fmt.Sprintf("precondition cleanup: xuansi_fs_rm %s (text=%.60s)", p, text))
		} else {
			h.note("precondition: %s absent (fs_read refuses: %.60s)", p, text)
		}
	}

	// ---------- S1: lease ----------
	fmt.Println("\n- S1: xuansi_lease holder=b8-t1 (default TCP route) -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b8Board, "holder": b8Holder})
	sess1, _ := b6Num(m, "session")
	h.check(!isErr && b6Str(m, "board") == b8Board && b6Str(m, "holder") == b8Holder && sess1 > 0,
		fmt.Sprintf("lease holder=%s session=%v (text=%q)", b8Holder, gAny(m, "session"), text))
	h.check(b6Str(m, "transport") == "" && b6Str(m, "warning") == "",
		fmt.Sprintf("lease took the DEFAULT TCP route: transport=%q warning=%q (no usb field, no reboot)", b6Str(m, "transport"), b6Str(m, "warning")))

	// ---------- S2: the fs removal leg ----------
	fmt.Println("\n- S2: fs_write /drv/b8tmp.lua -> fs_read verifies -> xuansi_fs_rm -> fs_read proves GONE -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_fs_write",
		map[string]any{"board": b8Board, "path": b8TmpPath, "data_b64": b8B64(b8TmpBody)})
	h.check(!isErr, fmt.Sprintf("fs_write %s (%dB payload; text=%.60s)", b8TmpPath, len(b8TmpBody), text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_fs_read", map[string]any{"board": b8Board, "path": b8TmpPath})
	h.check(!isErr && m["data_b64"] == b8B64(b8TmpBody),
		fmt.Sprintf("fs_read %s returns the written bytes verbatim (text=%.60s)", b8TmpPath, text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_fs_rm", map[string]any{"board": b8Board, "path": b8TmpPath})
	h.check(!isErr, fmt.Sprintf("xuansi_fs_rm %s through the pipeline (text=%.60s)", b8TmpPath, text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_fs_read", map[string]any{"board": b8Board, "path": b8TmpPath})
	h.check(isErr && strings.HasPrefix(text, "api:") && strings.Contains(text, "not found"),
		fmt.Sprintf("fs_read %s after rm -> board api not-found: the file is GONE (text=%.80s)", b8TmpPath, text))
	h.note("gone-proof method: the MCP face has no fs.ls - fs_read not-found (the board's own api error) is the evidence")

	// ---------- S3: whitelist negatives ----------
	fmt.Println("\n- S3: xuansi_fs_rm client-side path whitelist (refused before any crossing) -")
	for _, bad := range []string{"/drv/../x.lua", "/drv", "/drv/", "/etc/passwd", ""} {
		m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_fs_rm", map[string]any{"board": b8Board, "path": bad})
		h.check(isErr && strings.HasPrefix(text, "api:") && strings.Contains(text, "refused client-side"),
			fmt.Sprintf("whitelist refuses %q with the api-class message (text=%.70s)", bad, text))
	}

	// ---------- S4: the driver lifecycle ----------
	fmt.Println("\n- S4: ws_put b8pull@1.0.0 -> push -> require -> PULL -> history kept -> rollback re-push -> require again -> pull -")
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_ws_put",
		map[string]any{"name": b8DrvName, "version": "1.0.0", "content": b8DrvBody})
	szPut, _ := b6Num(m, "size")
	h.check(!isErr && b6Str(m, "sha256") == b8SHA(b8DrvBody) && int(szPut) == len(b8DrvBody),
		fmt.Sprintf("ws_put %s@1.0.0 stored (sha=%.16s... size=%v; text=%.60s)", b8DrvName, b6Str(m, "sha256"), gAny(m, "size"), text))
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
		map[string]any{"board": b8Board, "name": b8DrvName, "version": "1.0.0"})
	h.check(!isErr && b6Str(m, "path") == b8DrvPath && b6Str(m, "sha256") == b8SHA(b8DrvBody),
		fmt.Sprintf("driver_push landed %s, read-back verified (sha=%.16s...; text=%.60s)", b8DrvPath, b6Str(m, "sha256"), text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua", map[string]any{"board": b8Board,
		"code": fmt.Sprintf(`local M=require("bench.drv.%s") return tostring(M.version)..","..tostring(M.tag())`, b8DrvName)})
	h.check(!isErr && b6Str(m, "value") == "1.0.0,b8pull-ok",
		fmt.Sprintf("eval require of the pushed driver works through the --@api 1 gate (value=%v)", gAny(m, "value")))

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_driver_pull", map[string]any{"board": b8Board, "name": b8DrvName})
	h.check(!isErr && m["pulled"] == true && b6Str(m, "path") == b8DrvPath,
		fmt.Sprintf("driver_pull removed the effective file (result=%v; text=%.60s)", gAny(m, "pulled"), text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_ws_list", map[string]any{})
	pulledActive, pulledListed, pulledSha := "", false, ""
	if drs, yes := m["drivers"].([]any); yes {
		for _, d := range drs {
			if dm, _ := d.(map[string]any); b6Str(dm, "name") == b8DrvName {
				pulledActive = b6Str(dm, "active")
				if vs, ok2 := dm["versions"].([]any); ok2 {
					for _, v := range vs {
						if vm, _ := v.(map[string]any); b6Str(vm, "version") == "1.0.0" {
							pulledListed = true
							pulledSha = b6Str(vm, "sha256")
						}
					}
				}
			}
		}
	}
	h.check(!isErr && pulledActive == "" && pulledListed && pulledSha == b8SHA(b8DrvBody),
		fmt.Sprintf("ws_list after pull: active ABSENT, 1.0.0 still listed with its sha (active=%q listed=%v)", pulledActive, pulledListed))
	h.note("history never lies: Files/Boards footprints stay in the manifest - rollback is just a re-push")
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_fs_read", map[string]any{"board": b8Board, "path": b8DrvPath})
	h.check(isErr && strings.HasPrefix(text, "api:") && strings.Contains(text, "not found"),
		fmt.Sprintf("fs_read %s after pull -> board api not-found: GONE (text=%.80s)", b8DrvPath, text))

	// rollback: Active was cleared, so the re-push names the version
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
		map[string]any{"board": b8Board, "name": b8DrvName, "version": "1.0.0"})
	h.check(!isErr && b6Str(m, "path") == b8DrvPath && b6Str(m, "sha256") == b8SHA(b8DrvBody),
		fmt.Sprintf("ROLLBACK re-push of 1.0.0 (explicit version; Active was cleared) landed again (sha=%.16s...)", b6Str(m, "sha256")))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua", map[string]any{"board": b8Board,
		"code": fmt.Sprintf(`package.loaded["bench.drv.%s"]=nil local M=require("bench.drv.%s") return tostring(M.version)..","..tostring(M.tag())`, b8DrvName, b8DrvName)})
	h.check(!isErr && b6Str(m, "value") == "1.0.0,b8pull-ok",
		fmt.Sprintf("hot-swap idiom after the re-push: require re-reads /drv from flash and works again (value=%v)", gAny(m, "value")))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_driver_pull", map[string]any{"board": b8Board, "name": b8DrvName})
	h.check(!isErr && m["pulled"] == true,
		fmt.Sprintf("driver_pull again - the board ends with NO %s (text=%.60s)", b8DrvPath, text))

	// ---------- S5: cleanup + honesty + teardown ----------
	fmt.Println("\n- S5: double-cleanup honesty, release, EOF/SIGINT teardown, sandbox accounting -")
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_fs_rm", map[string]any{"board": b8Board, "path": b8TmpPath})
	h.check(isErr && strings.HasPrefix(text, "api:") && strings.Contains(text, "No such file or directory"),
		fmt.Sprintf("xuansi_fs_rm %s on the already-removed file -> the BOARD's api not-found crosses VERBATIM (text=%.80s)", b8TmpPath, text))
	for _, p := range []string{b8TmpPath, b8DrvPath} {
		m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_fs_read", map[string]any{"board": b8Board, "path": p})
		h.check(isErr && strings.Contains(text, "not found"),
			fmt.Sprintf("final state: %s absent - the board's /drv is as found (text=%.70s)", p, text))
	}

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release", map[string]any{"board": b8Board, "holder": b8Holder})
	h.check(!isErr && b6Str(m, "board") == b8Board && b6Str(m, "holder") == b8Holder,
		fmt.Sprintf("final release - the bench is left UNLEASED (text=%.60s)", text))
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b8Board})
	_, hasLease := m["lease"]
	h.check(!isErr && !hasLease, fmt.Sprintf("post-release board_status shows NO lease (lease key %v)", hasLease))

	if err := cs.Close(); err != nil {
		h.note("client session close: %v (proceeding to EOF wait)", err)
	}
	select {
	case <-mcp.exited:
		code, werr := mcp.exitStats()
		h.check(code == 0 && werr == nil,
			fmt.Sprintf("xuansi mcp exited clean on stdin EOF (code=%d waitErr=%v)", code, werr))
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

	h.note("XDG sandbox accounting: the b8pull workspace entry lives inside %s (torn down with the run dir)", xdg)
	if entries, err := os.ReadDir(filepath.Join(xdg, "xuansi", "workspace", "drivers")); err == nil {
		for _, e := range entries {
			h.note("  sandboxed workspace file: %s", e.Name())
		}
	} else {
		h.note("  no sandboxed workspace drivers dir (%v)", err)
	}
	return finish()
}
