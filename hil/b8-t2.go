// b8-t2.go - batch 8 task 2 mini-HIL: xuansi_session_reset on the real
// bench board - the b7 S4b detour's proper fix, proven live. The b7
// finding: bare evals claim pins in board namespace 0 (owner _eval,
// shared across every eval), and the MCP face had NO in-pipeline
// release - the clumsy detour was release-lease -> re-lease (the
// daemon's teardown sweep did the resetting as a side effect). This
// capture proves the dedicated path: ONE lease held throughout, the
// ns-0 claims swept by the new tool alone, and the board left as found.
//
// Three real process layers (b6/b8-t1 house style): go-sdk MCP client
// over os.Pipe <-> `xuansi mcp` <-> `xuansi serve` <-> the real board
// over TCP (xuansi-d75884.local:7777; the CDC port is NEVER opened -
// that reboots the board). XDG-sandboxed so the real user workspace is
// untouched.
//
// Steps:
//
//	S0  prelude - no stale processes, board TCP reachable, XDG sandbox,
//	    serve up + sweep, mcp up + initialize, list_boards shows d75884
//	    fw 775ecaa, xuansi_session_reset served (20 static tools), and
//	    the board starts clean (no lease, no scripts)
//	S1  the b7 S4b SITUATION replayed under ONE lease - eval
//	    bench.i2c.setup(0,{sda=8,scl=9}) claims 8/9 into ns-0 -> a
//	    SECOND eval proves the claim persists across evals (the shared
//	    membership): a cross-prim bench.uart.setup(1,{tx=8,rx=9}) is
//	    refused by the fw's one-prim-per-pin gate naming both prims ->
//	    diag.pins (stdout capture cap-1) shows GPIO8/GPIO9 sess=0
//	    owner=_eval prim=i2c. The OLD escape (release + re-lease, the
//	    b7 S4b detour) is NOT taken.
//	S2  the proper fix - xuansi_session_reset {board} with session
//	    OMITTED (= ns-0) -> the board's {stopped:n} reply verbatim (n
//	    counts scripts; a pins-only ns-0 sweep honestly answers 0 -
//	    b5-t10 teardown log L50 precedent) -> diag.pins shows 8/9
//	    FREE -> the SAME cross-prim uart.setup now SUCCEEDS (the pin
//	    is truly released, not re-claimable-by-owner) -> a second
//	    session_reset sweeps the uart claim -> a FRESH i2c.setup +
//	    scan works (the b7 detour's re-lease leg replayed WITHOUT
//	    releasing: board_status still shows the S1 lease)
//	S3  the script-session rung - script_start b8t2res (claims GPIO12
//	    out/high, resident yield loop; runs under the LEASE's session
//	    namespace per the T10 ruling) -> board_status lists it ->
//	    session_reset {session:0} (EXPLICIT zero) does NOT stop it
//	    (namespacing: the script lives in the lease session, not 0)
//	    -> session_reset {session:<lease session>} -> {stopped:1},
//	    script gone from board_status, GPIO12 released
//	S4  honesty + as-found - final session_reset (omitted) sweeps the
//	    S2 i2c re-claim -> release -> brief second lease purely for
//	    capture cap-2 + diag.pins (b7 house pattern): every touched
//	    pin free -> release -> no lease -> mcp EOF exit 0, serve
//	    SIGINT exit 0, sandbox accounting
//
// Single capture, numbered PASS/FAIL per check, ASCII only (the
// batch-8 evidence discipline).
package main

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
)

const (
	b8t2Board   = "d75884"  // the bench board
	b8t2FwBuild = "775ecaa" // flashed fw (b6/b7/b8-t1 binding, unchanged)
	b8t2Holder  = "b8-t2"   // custom lease holder for this run
	b8t2TCPEndp = "xuansi-d75884.local:7777"
	b8t2Script  = "b8t2res" // the S3 resident script's name
)

// b8t2ResScript claims GPIO12 out/high and stays resident (the t10
// yield-loop idiom) - its claims land in the LEASE's session namespace
// (script_start stamps it daemon-side), not ns-0.
const b8t2ResScript = `bench.gpio.mode(12,"out") bench.gpio.write(12,1) print("b8t2res up, pin12 out/high") ` +
	`while true do local f = coroutine.yield(); if f then f() end end`

// b8t2I2CClaim claims 8/9 for ns-0 (bare eval = owner _eval).
const b8t2I2CClaim = `bench.i2c.setup(0,{sda=8,scl=9}) return "i2c-claimed"`

// b8t2UartProbe is the cross-prim probe: refused while i2c holds 8/9
// (one primitive per pin), succeeds once they are free.
const b8t2UartProbe = `bench.uart.setup(1,{tx=8,rx=9}) return "uart-claimed"`

// b8t2I2CReclaim re-claims 8/9 after the sweep and scans (no fixture:
// a clean bus answers 0 devices - the claim itself is the point).
const b8t2I2CReclaim = `bench.i2c.setup(0,{sda=8,scl=9,freq=100000}) local t=bench.i2c.scan(0) return "reclaim-ok,"..tostring(#t)`

// b8t2BusyPins reports which of the touched pins appear in a diag.pins
// dump (the b7 parser: substring per pin; "GPIO8" never matches
// "GPIO18" - the digits anchor it).
func b8t2BusyPins(dump string) []string {
	var busy []string
	for _, pin := range []string{"GPIO8", "GPIO9", "GPIO12"} {
		if strings.Contains(dump, pin) {
			busy = append(busy, pin)
		}
	}
	return busy
}

func runB8T2(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== b8 task-2 mini-HIL: xuansi_session_reset - the b7 S4b detour's proper fix - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("board: %s over TCP %s (CDC never opened - that reboots the board); fw expect %s\n", b8t2Board, b8t2TCPEndp, b8t2FwBuild)
	fmt.Printf("discipline: evidence ASCII only; ONE lease S1..S4 (no release/re-lease detour); board left AS FOUND\n")

	var serve, mcp *b6Proc
	defer func() { // die-safe: never leave processes or a lease behind
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int {
		code := h.finish("b8-t2")
		if h.fail == 0 {
			fmt.Printf("=== ALL PASS - ns-0 releases WITHOUT the release/re-lease detour: session_reset{omitted} sweeps eval pin claims, session_reset{lease-ns} stops its scripts, one lease held throughout, board as found (attempt %d, S0..S4) ===\n", attempt)
		}
		return code
	}

	// ---------- S0: prelude ----------
	fmt.Println("\n- S0: prelude - clean bench, board reachable, sandbox, serve+mcp up, tool served -")
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
	if conn, err := net.DialTimeout("tcp", b8t2TCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v (USB fallback OUT of scope - opening it reboots the board)", b8t2TCPEndp, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only - no reboot, no USB open)", b8t2TCPEndp))
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b8-t2 boards.conf: no aliases - the mac6 selector is used directly\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}
	h.note("XDG sandbox: XDG_DATA_HOME=%s (captures land inside; the real user workspace untouched)", xdg)

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

	// the new tool is served
	names, terr := b.toolNames(ctx)
	servedReset := false
	for _, n := range names {
		if n == "xuansi_session_reset" {
			servedReset = true
		}
	}
	h.check(terr == nil && servedReset,
		fmt.Sprintf("static face serves xuansi_session_reset (served=%v of %d tools)", servedReset, len(names)))

	ok, last = pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		m, _, isErr, ok := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
		if !ok || isErr {
			return false, "list_boards trouble"
		}
		for _, e := range m["boards"].([]any) {
			if bm, yes := e.(map[string]any); yes && bm["mac6"] == b8t2Board {
				if fw, _ := bm["fw"].(string); fw != "" {
					return true, "fw populated (hello-enriched)"
				}
				return false, "board listed, fw not yet enriched"
			}
		}
		return false, "board not in list yet"
	})
	h.check(ok, fmt.Sprintf("list_boards shows %s with fw populated (%s)", b8t2Board, last))
	m, text, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
	fw := ""
	for _, e := range m["boards"].([]any) {
		if bm, _ := e.(map[string]any); bm["mac6"] == b8t2Board {
			fw, _ = bm["fw"].(string)
		}
	}
	h.check(!isErr && fw == b8t2FwBuild, fmt.Sprintf("fw BUILD BINDING: %q == expected %q (text=%.60s)", fw, b8t2FwBuild, text))

	// honest precondition: no lease held, no scripts running
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b8t2Board})
	_, hasLease := m["lease"]
	h.check(!isErr && !hasLease, fmt.Sprintf("precondition: board starts with NO lease (lease key %v; text=%.50s)", hasLease, text))
	h.check(!isErr && !strings.Contains(fmt.Sprint(m["scripts"]), b8t2Script),
		fmt.Sprintf("precondition: no leftover %s script (scripts=%v)", b8t2Script, gAny(m, "scripts")))

	// ---------- S1: the b7 S4b situation, replayed under ONE lease ----------
	fmt.Println("\n- S1: eval claims ns-0 (8/9 i2c) -> claim PERSISTS across evals (the S4b situation; detour NOT taken) -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b8t2Board, "holder": b8t2Holder})
	leaseSess, _ := b6Num(m, "session")
	h.check(!isErr && b6Str(m, "board") == b8t2Board && b6Str(m, "holder") == b8t2Holder && leaseSess > 0,
		fmt.Sprintf("lease holder=%s session=%.0f (default TCP route; ONE lease for the whole run)", b8t2Holder, leaseSess))
	h.check(b6Str(m, "transport") == "" && b6Str(m, "warning") == "",
		fmt.Sprintf("lease took the DEFAULT TCP route: transport=%q warning=%q (no usb field, no reboot)", b6Str(m, "transport"), b6Str(m, "warning")))

	// cap-1: the stdout window collecting the THREE S1/S2 diag.pins dumps
	// (claimed -> released -> re-claimed); 20s covers the whole leg
	capCh := b.callAsync("xuansi_capture", map[string]any{"board": b8t2Board, "source": "stdout", "seconds": float64(20)})
	ok, last = pollUntil(300*time.Millisecond, 10*time.Second, func() (bool, string) {
		return serve.has("capture cap-1 started (" + b8t2Board + " stdout"), "daemon log: capture started"
	})
	h.check(ok, fmt.Sprintf("stdout capture cap-1 armed board-side through the MCP capture face (%s)", last))
	time.Sleep(400 * time.Millisecond) // let the subscription settle

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b8t2Board, "code": b8t2I2CClaim})
	h.check(!isErr && b6Str(m, "value") == "i2c-claimed",
		fmt.Sprintf("eval #1 bench.i2c.setup{8/9} claims ns-0 (value=%v; owner _eval)", gAny(m, "value")))

	// the shared-membership proof: a SECOND eval meets the FIRST's claim
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b8t2Board, "code": b8t2UartProbe})
	h.check(isErr && strings.HasPrefix(text, "api:") && strings.Contains(text, "already claimed by bench.i2c"),
		fmt.Sprintf("eval #2 cross-prim uart{tx=8} REFUSED - the eval-#1 claim PERSISTS across evals (text=%.90s)", text))
	h.note("this is the b7 S4b situation exactly: the OLD escape was release-lease + re-lease (the daemon teardown sweep) - NOT taken here")

	// the membership view: diag.pins dump lands on stdout, cap-1 collects it
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b8t2Board, "op": "diag.pins"})
	h.check(!isErr, fmt.Sprintf("board_op diag.pins #1 (claimed view; dump lands on stdout into cap-1; text=%.40s)", text))

	// ---------- S2: the proper fix ----------
	fmt.Println("\n- S2: xuansi_session_reset {session OMITTED} -> ns-0 swept -> 8/9 free -> cross-prim ok -> i2c re-claim ok -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_session_reset", map[string]any{"board": b8t2Board})
	stopped, hasStopped := b6Num(m, "stopped")
	h.check(!isErr && hasStopped,
		fmt.Sprintf("xuansi_session_reset {board} (session omitted = ns-0) -> the board's reply VERBATIM: {stopped:%v} (text=%q)", gAny(m, "stopped"), text))
	h.check(!isErr && stopped == 0,
		fmt.Sprintf("stopped == 0 is the HONEST pins-only answer (n counts scripts; ns-0 holds none - b5-t10 L50 precedent; actual %v)", stopped))

	// the released view BEFORE any re-claim: dump #2 must show 8/9 gone
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b8t2Board, "op": "diag.pins"})
	h.check(!isErr, fmt.Sprintf("board_op diag.pins #2 (released view; text=%.40s)", text))

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b8t2Board, "code": b8t2UartProbe})
	h.check(!isErr && b6Str(m, "value") == "uart-claimed",
		fmt.Sprintf("cross-prim uart{tx=8,rx=9} now SUCCEEDS - 8/9 truly RELEASED, not owner-reclaimable (value=%v)", gAny(m, "value")))

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_session_reset", map[string]any{"board": b8t2Board})
	h.check(!isErr, fmt.Sprintf("second session_reset sweeps the uart claim (text=%.60s)", text))

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b8t2Board, "code": b8t2I2CReclaim})
	h.check(!isErr && b6Str(m, "value") == "reclaim-ok,0",
		fmt.Sprintf("FRESH i2c.setup{8/9} + scan works (value=%v) - the b7 detour's re-lease leg replayed WITHOUT releasing", gAny(m, "value")))

	// dump #3: the re-claimed view (same lease, same ns-0, fresh claim)
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b8t2Board, "op": "diag.pins"})
	h.check(!isErr, fmt.Sprintf("board_op diag.pins #3 (re-claimed view; text=%.40s)", text))

	// the lease survived everything: no release, no re-lease, anywhere
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b8t2Board})
	leaseView, stillLeased := m["lease"].(map[string]any)
	holderNow := ""
	if stillLeased {
		holderNow, _ = leaseView["holder"].(string)
	}
	h.check(!isErr && stillLeased && holderNow == b8t2Holder,
		fmt.Sprintf("the S1 lease STILL held after two session_resets + re-claims (holder=%q) - the detour is obsolete", holderNow))

	cres := <-capCh
	h.check(cres.ok && !cres.isErr && b6Str(cres.m, "reason") == "limit",
		fmt.Sprintf("stdout capture cap-1 returned {path=%v bytes=%v reason=%v}", gAny(cres.m, "path"), gAny(cres.m, "bytes"), b6Str(cres.m, "reason")))
	capPath := b6Str(cres.m, "path")
	capsRoot := filepath.Join(xdg, "xuansi", "captures")
	var capData []byte
	var rdErr error
	if capPath != "" {
		capData, rdErr = os.ReadFile(capPath)
	}
	h.check(capPath != "" && strings.HasPrefix(capPath, capsRoot) && rdErr == nil,
		fmt.Sprintf("cap-1 file inside the XDG sandbox, readable from THIS HIL process (%s, %d bytes)", capPath, len(capData)))

	// the three dumps: split on "pins (" section starts (asciiLine
	// flattens newlines to "-", so there is no line anchor to ride on -
	// b7's "pins (0):-" head lines are that flattening)
	cont := asciiLine(string(capData))
	var idx []int
	for off := 0; off+len("pins (") <= len(cont); {
		j := strings.Index(cont[off:], "pins (")
		if j < 0 {
			break
		}
		idx = append(idx, off+j)
		off = off + j + len("pins (")
	}
	h.check(len(idx) == 3, fmt.Sprintf("cap-1 holds exactly the three diag.pins dumps (found %d sections)", len(idx)))
	section := func(n int) string {
		if len(idx) <= n {
			return ""
		}
		end := len(cont)
		if n+1 < len(idx) {
			end = idx[n+1]
		}
		return cont[idx[n]:end]
	}
	d1, d2, d3 := section(0), section(1), section(2)
	h.check(strings.Contains(d1, "GPIO8") && strings.Contains(d1, "GPIO9") &&
		strings.Contains(d1, "owner=_eval") && strings.Contains(d1, "prim=i2c"),
		fmt.Sprintf("dump #1: GPIO8/GPIO9 sess=0 owner=_eval prim=i2c - the ns-0 membership view (head: %.80s)", strings.TrimSpace(d1)))
	busy2 := b8t2BusyPins(d2)
	h.check(len(busy2) == 0,
		fmt.Sprintf("dump #2 (post-reset, pre-re-claim): 8/9 ABSENT - RELEASED by session_reset{omitted} alone (busy=%v; head: %.60s)", busy2, strings.TrimSpace(d2)))
	h.check(strings.Contains(d3, "GPIO8") && strings.Contains(d3, "GPIO9") && strings.Contains(d3, "prim=i2c"),
		fmt.Sprintf("dump #3: 8/9 back as prim=i2c - the fresh re-claim under the SAME lease (head: %.60s)", strings.TrimSpace(d3)))

	// ---------- S3: the script-session rung ----------
	fmt.Println("\n- S3: script_start b8t2res (lease ns) -> reset{0} does NOT touch it -> reset{lease-ns} stops it + releases pin 12 -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_script_start",
		map[string]any{"board": b8t2Board, "name": b8t2Script, "code": b8t2ResScript})
	h.check(!isErr, fmt.Sprintf("script_start %s - claims GPIO12 under the LEASE's session %.0f (text=%.60s)", b8t2Script, leaseSess, text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b8t2Board})
	h.check(!isErr && strings.Contains(fmt.Sprint(m["scripts"]), b8t2Script),
		fmt.Sprintf("board_status lists the resident script (scripts=%v)", gAny(m, "scripts")))

	// explicit ZERO does not stop it: the script lives in the lease ns
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_session_reset",
		map[string]any{"board": b8t2Board, "session": float64(0)})
	h.check(!isErr, fmt.Sprintf("session_reset {session:0} crossed fine (text=%.60s)", text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b8t2Board})
	h.check(!isErr && strings.Contains(fmt.Sprint(m["scripts"]), b8t2Script),
		fmt.Sprintf("namespacing negative: reset{0} did NOT stop %s (still in scripts=%v) - it lives in ns %.0f", b8t2Script, gAny(m, "scripts"), leaseSess))

	// the lease-ns reset stops it: {stopped:1}
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_session_reset",
		map[string]any{"board": b8t2Board, "session": leaseSess})
	stoppedS, _ := b6Num(m, "stopped")
	h.check(!isErr && stoppedS >= 1,
		fmt.Sprintf("session_reset {session:%.0f} -> {stopped:%v} (the script namespace; text=%q)", leaseSess, gAny(m, "stopped"), text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b8t2Board})
	h.check(!isErr && !strings.Contains(fmt.Sprint(m["scripts"]), b8t2Script),
		fmt.Sprintf("script GONE from board_status after its namespace reset (scripts=%v)", gAny(m, "scripts")))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b8t2Board, "code": `local ok=pcall(bench.gpio.mode, 12, "out") return "pin12-free,"..tostring(ok)`})
	h.check(!isErr && b6Str(m, "value") == "pin12-free,true",
		fmt.Sprintf("GPIO12 released by the script-ns reset - a bare eval can claim it now (value=%v)", gAny(m, "value")))
	// sweep that last eval claim so the honesty leg starts clean
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_session_reset", map[string]any{"board": b8t2Board})
	h.check(!isErr, fmt.Sprintf("sweep the pin12 eval claim too (text=%.60s)", text))

	// ---------- S4: honesty + as-found ----------
	fmt.Println("\n- S4: release -> second lease for cap-2 + final diag.pins -> all free -> release -> teardown -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release", map[string]any{"board": b8t2Board, "holder": b8t2Holder})
	h.check(!isErr && b6Str(m, "board") == b8t2Board && b6Str(m, "holder") == b8t2Holder,
		fmt.Sprintf("release the ONE lease (text=%.60s)", text))

	// the b7 house pattern: a brief second lease purely for cap-2 + diag.pins
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b8t2Board, "holder": b8t2Holder})
	h.check(!isErr, fmt.Sprintf("brief second lease for the final pins view (text=%.60s)", text))
	cap2Ch := b.callAsync("xuansi_capture", map[string]any{"board": b8t2Board, "source": "stdout", "seconds": float64(3)})
	time.Sleep(700 * time.Millisecond) // subscription settles
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b8t2Board, "op": "diag.pins"})
	h.check(!isErr, fmt.Sprintf("final board_op diag.pins (dump on stdout via cap-2; text=%.40s)", text))
	cres2 := <-cap2Ch
	h.check(cres2.ok && !cres2.isErr, fmt.Sprintf("cap-2 returned {path=%v reason=%v}", gAny(cres2.m, "path"), b6Str(cres2.m, "reason")))
	cap2Data, rd2Err := os.ReadFile(b6Str(cres2.m, "path"))
	h.check(rd2Err == nil, fmt.Sprintf("cap-2 file readable (%d bytes)", len(cap2Data)))
	finalBusy := b8t2BusyPins(asciiLine(string(cap2Data)))
	h.check(len(finalBusy) == 0,
		fmt.Sprintf("final diag.pins: GPIO8/9/12 ALL FREE - the board is as found (busy=%v; head: %.60s)", finalBusy, strings.TrimSpace(asciiLine(string(cap2Data)))))

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release", map[string]any{"board": b8t2Board, "holder": b8t2Holder})
	h.check(!isErr, fmt.Sprintf("final release - the bench is left UNLEASED (text=%.60s)", text))
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b8t2Board})
	_, hasLease2 := m["lease"]
	h.check(!isErr && !hasLease2, fmt.Sprintf("post-release board_status shows NO lease (lease key %v)", hasLease2))

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

	h.note("XDG sandbox accounting: the two captures live inside %s (torn down with the run dir)", xdg)
	if entries, err := os.ReadDir(filepath.Join(xdg, "xuansi", "captures")); err == nil {
		for _, e := range entries {
			h.note("  sandboxed capture file: %s", e.Name())
		}
	} else {
		h.note("  no sandboxed captures dir (%v)", err)
	}
	return finish()
}
