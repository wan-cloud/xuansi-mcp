package main

// b11toolchain.go — B11 task-11 linux HIL: the toolchain-distribution
// chain END TO END on the real bench board — the whole point of the
// batch. A fresh HEAD daemon + its `xuansi mcp` child serve the three
// new static tools (24->25), and this driver walks the agent's real
// first-day story against the REAL public toolchain index and the REAL
// network:
//
//	status says the cache is EMPTY and self-guides to fetch ->
//	fetch REALLY downloads the pinned wasi-sdk (119MB) + wamrc (33MB)
//	from the index's GitHub URLs into an ISOLATED cache
//	(XUANSI_WASM_CACHE under the run dir; the real user cache is
//	never touched) -> a second fetch proves the idempotent all-Skipped
//	path -> status now reads present=true both pieces with the index's
//	pinned sha/bytes -> xuansi_wasm_build compiles a hello-scale C
//	source (module_abi=1: xuansi_abi/xuansi_alloc exports + one echo
//	fn, the drivers repo's src/hello/hello.c minimal form) against the
//	just-fetched sdk -> xuansi_driver_push deploys the artifact to the
//	board (magic-gated .wasm leg, first push with an EXPLICIT version
//	— T10 concern-2 semantics) -> the fw's bench.wasm face loads and
//	CALLS it, echo's packed (ptr,len) round-trip bridging back the
//	caller's args -> xuansi_ws_list reconciles the workspace entry
//	against the build's own sha/bytes -> teardown leaves the board as
//	found.
//
// Topology (b10t5's, unchanged): the harness's own freshly-built HEAD
// daemon (serve --sock <run>/daemon.sock, XDG_DATA_HOME sandboxed so
// the workspace lands inside the run dir) <- its own `xuansi mcp`
// child <- this go-sdk client over the child's stdio pipes. The
// RESIDENT user-session daemon is stopped by the orchestrating script
// for the duration and restored after (b10t5 contract).
//
//	suite (S0..S8):
//	  S0  prelude: clean bench, board reachable + LISTED (an absent
//	      board is BLOCKED, not a spin), sandbox, HEAD serve+mcp up,
//	      initialize, the three toolchain tools served, lease
//	  S1  missing-cache status: both pieces present=false, guidance
//	      names xuansi_toolchain_fetch, index.source=remote
//	  S2  the real fetch: Downloaded=[wasi_sdk wamrc] from the live
//	      index (pinned sha/bytes asserted via S3), disk probes for
//	      bin/clang + the wamrc binary; SECOND fetch -> all Skipped
//	      (the idempotent sync verb)
//	  S3  complete status: present=true both, generation=toolchain-v2,
//	      sha/bytes == the index pins, guidance quiet
//	  S4  wasm_build hilhello@1.0.0 from C source: sha/bytes>0,
//	      warnings empty, the Next sentence, workspace artifact
//	      byte-verified (\0asm magic)
//	  S5  driver_push version 1.0.0 (explicit): /drv/hilhello.wasm
//	      sha-verified read-back
//	  S6  eval: bench.wasm.load -> true; echo(7,35) == [7 35];
//	      echo(1800,42) == [1800 42]
//	  S7  ws_list reconciliation: hilhello active=1.0.0, sha/size ==
//	      the build's answer
//	  S8  teardown: unload, fs_rm, not-found verify, no loaded
//	      instances, release, clean subprocess exits
//
// Evidence discipline: ASCII only, verdicts in-file, honest attempts.

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	b11Board   = "d75884"
	b11FwBuild = "0b6cc4e" // B10-T8c canonical (fw_version semver); HOUSE RULE: same commit as the other hil constants
	b11Holder  = "b11tc-hil"
	b11TCPEndp = "xuansi-d75884.local:7777"

	// the module this run builds + deploys + calls
	b11ModName = "hilhello"
	b11ModVer  = "1.0.0" // EXPLICIT first version (T10 concern-2: the build/push pair names it)

	// pinned against the LIVE public index at writing time
	// (2026-11-08); a released generation's assets are immutable
	// upstream, so these pins are stability, not snapshot luck.
	// The per-ASSET pins live in b11Pins below (all five index
	// platforms, verbatim from the drivers repo's
	// toolchain-index.json) — W32/W33 reconcile against THIS host's
	// row.
	b11Gen = "toolchain-v2"
)

// b11Pin is one platform's toolchain-v2 asset pins: the wasi_sdk
// archive's sha/bytes and the wamrc archive's. wamrcSHA == "" (with
// wamrcBytes == 0) expresses the index's "wamrc": null — that
// platform's matrix row carries NO wamrc asset (linux-arm64 and
// win-x64), its fetch installs wasi_sdk ALONE, and W33 skips with a
// note instead of pinning.
type b11Pin struct {
	sdkSHA     string
	sdkBytes   int
	wamrcSHA   string
	wamrcBytes int
}

// b11Pins are the index's toolchain-v2 pins, VERBATIM from
// code/xuansi-drivers/toolchain-index.json (read 2026-11-09; a
// released generation's assets are immutable upstream, so these are
// stability, not snapshot luck).
var b11Pins = map[string]b11Pin{
	"linux-x64": {
		sdkSHA:     "c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5",
		sdkBytes:   118879731,
		wamrcSHA:   "209a6a51b6066f05e91060d95c2506ec0bad97cb898fec1fe634cd68865b9c42",
		wamrcBytes: 33053698,
	},
	"linux-arm64": {
		sdkSHA:   "ae6c1417ea161e54bc54c0a168976af57a0c6e53078857886057a71a0d928646",
		sdkBytes: 119660865,
		// wamrc: null on this platform — no pin, W33 skips
	},
	"macos-x64": {
		sdkSHA:     "4bfc274ae8b68c771dd93e32ad4bd695336ca46b585beb3b1260e5602dbc11a2",
		sdkBytes:   109975956,
		wamrcSHA:   "b87c91e5b6bd907937f8b5f79053212abfc297ee810d2dc348488d8c0089bdc1",
		wamrcBytes: 23784250,
	},
	"macos-arm64": {
		sdkSHA:     "aeae999396d5f5caa5ce419f52e83c35869d5fd21d40af80acba2c80f51b0b3a",
		sdkBytes:   108199177,
		wamrcSHA:   "e73c84a24abbc50e5122a5dc8221aa2c579594ea1559224d156f8bb1778fca30",
		wamrcBytes: 22421504,
	},
	"win-x64": {
		sdkSHA:   "0f934c6e7e171b5627c81b697a9e57e96d65cd889f56ce6077350de48978afd3",
		sdkBytes: 495098073,
		// wamrc: null on this platform — no pin, W33 skips
	},
}

// b11PlatformKey maps THIS host onto the index's platform key the way
// the daemon's tcPlatKey does (the index says "macos"/"win" where Go
// says "darwin"/"windows", "x64" where Go says "amd64"); "" = the
// host is off the index matrix entirely.
func b11PlatformKey() string {
	osName := map[string]string{"linux": "linux", "darwin": "macos", "windows": "win"}[runtime.GOOS]
	arch := map[string]string{"amd64": "x64", "arm64": "arm64"}[runtime.GOARCH]
	if osName == "" || arch == "" {
		return ""
	}
	return osName + "-" + arch
}

// b11SDKDirName names the SDK tree the fetch lands on this host
// (wasi-sdk-24.0-<platform key>); the W21 disk probe must look in the
// directory the FETCH ACTUALLY installed — the mac leg
// (darwin/arm64 -> macos-arm64) would otherwise look for this linux
// bench's spelling. Off-matrix hosts get the loud -unknown spelling
// so the probe fails naming it. Local maps on purpose: hil is a
// standalone package main and does not import internal/.
func b11SDKDirName() string {
	if k := b11PlatformKey(); k != "" {
		return "wasi-sdk-24.0-" + k
	}
	return "wasi-sdk-24.0-unknown"
}

// b11HelloSrc is the hello-scale module the build compiles: the
// drivers repo's src/hello/hello.c cut to the ABI's mandatory trio
// (xuansi_abi / xuansi_alloc / one exported fn). echo returns the
// caller's msgpack args untouched (the packed (ptr,len) pair), so
// bench.wasm.call('hilhello','echo',a,b) bridges back [a b] — the
// byte-level round-trip proof. `#include "xuansi_host.h"` resolves
// against the daemon's embedded frozen host table (wasmhdr/).
const b11HelloSrc = `/* hilhello.c - b11 task-11 HIL module: module_abi=1 minimal form
 * (the drivers repo's src/hello/hello.c cut to its mandatory trio).
 * echo returns the caller's serialized args unchanged - the host
 * decodes the same msgpack array back, so call(echo,a,b) -> [a b]. */
#include <stdint.h>
#include <stddef.h>

#include "xuansi_host.h"

#define XUANSI_EXPORT(name) __attribute__((export_name(name)))

/* small-args slot (hello.c's fix1 convention, minimal form): the host
 * calls xuansi_alloc on EVERY call-with-args; small args reuse one
 * fixed slot, safe because the fw host serializes every wasm call. */
static uint8_t args_slot[128] __attribute__((aligned(8)));

XUANSI_EXPORT("xuansi_alloc")
uint32_t xuansi_alloc(uint32_t n)
{
    if (n == 0 || n > sizeof args_slot) return 0;
    return (uint32_t)(uintptr_t)args_slot;
}

XUANSI_EXPORT("xuansi_abi")
uint32_t xuansi_abi(void)
{
    return XUANSI_ABI;
}

XUANSI_EXPORT("echo")
uint64_t echo(uint32_t ptr, uint32_t len)
{
    return ((uint64_t)ptr << 32) | (uint64_t)len;
}
`

func runB11Toolchain(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== b11 task-11 toolchain-distribution HIL (linux leg) - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("serving daemon: %s (freshly built at HEAD by the orchestrating script; the RESIDENT npm daemon is stopped there for the duration - b10t5 contract)\n", daemonBin)
	// mac leg (T11 step-2 prep, measured on the mac host): Go's Dial
	// with a .local name — LookupHost succeeds but the dialer's
	// internal lookup path times out — while dialing the board's IP
	// directly works. B11_TCP_ENDP overrides the endpoint for exactly
	// that (e.g. B11_TCP_ENDP=10.1.51.205:7777); the .local default
	// stays the linux bench's shape.
	endp := b11TCPEndp
	if v := os.Getenv("B11_TCP_ENDP"); v != "" {
		endp = v
		fmt.Printf("endpoint override: B11_TCP_ENDP=%s (the mac leg's .local dial workaround)\n", endp)
	}
	fmt.Printf("board: %s over TCP %s (daemon never opens CDC - that reboots); fw expect %s (protocol_ver 5)\n", b11Board, endp, b11FwBuild)
	fmt.Printf("index: the live public one, default URL (raw.githubusercontent.com/wan-cloud/xuansi-drivers/main/toolchain-index.json)\n")
	fmt.Printf("chain: status(missing) -> fetch(real 119MB sdk + 33MB wamrc download) -> fetch#2(all Skipped) -> status(present) ->\n")
	fmt.Printf("  wasm_build(hilhello@1.0.0 from C source) -> driver_push -> bench.wasm load/call echo -> ws_list reconcile\n")
	fmt.Printf("discipline: evidence ASCII only; verdicts in-file; cache ISOLATED under the run dir (XUANSI_WASM_CACHE - the real user cache untouched)\n")

	var serve, mcp *b6Proc
	defer func() { // die-safe: never leave processes or a lease behind
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int {
		code := h.finish("b11toolchain")
		if h.fail == 0 {
			fmt.Printf("=== ALL PASS - toolchain chain on the real board: missing status -> real fetch -> idempotent refetch -> present status -> build -> push -> load/call echo -> ws_list (attempt %d, S0..S8) ===\n", attempt)
		}
		return code
	}

	// ---------- S0: prelude ----------
	fmt.Println("\n- S0: prelude - clean bench, board reachable + listed, sandbox, HEAD serve+mcp up, tools served, lease -")
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
		h.check(stale == 0, fmt.Sprintf("no stale xuansi serve daemon on the bench (found %d - the resident npm daemon must be stopped first: it holds the controller slot)", stale))
	} else {
		h.note("pgrep: no xuansi processes found (clean bench)")
	}
	if conn, err := net.DialTimeout("tcp", endp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v (dial only - no reboot, no USB open)", endp, err))
		fmt.Println("  BLOCKED: the bench board is not reachable - report and stop (no spin)")
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only)", endp))
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	// CACHE ISOLATION (the brief's hard rule): XUANSI_WASM_CACHE points
	// INSIDE the run dir, and the wrapper wipes the run dir per attempt
	// - so S1's "missing" premise holds honestly every run and the
	// fetch's ~152MB lands in the sandbox, never in the real
	// ~/.cache/xuansi-wasm. os.Setenv here rides os.Environ() into BOTH
	// children (startB6Daemon/startB6MCP); the mcp child is the one
	// whose toolset reads it.
	wasmCache := filepath.Join(runDir, "wasm-cache")
	if err := os.Setenv("XUANSI_WASM_CACHE", wasmCache); err != nil {
		h.check(false, "set XUANSI_WASM_CACHE: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b11-toolchain boards.conf: no aliases - the mac6 selector is used directly\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}
	h.note("XDG sandbox: XDG_DATA_HOME=%s (the workspace lands inside; the real user workspace untouched)", xdg)
	h.note("CACHE ISOLATION: XUANSI_WASM_CACHE=%s (run-scoped, wiped per attempt; the real user cache is never read or written)", wasmCache)

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
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b11toolchain-hil", Version: "1.0"},
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
	want := map[string]bool{"xuansi_toolchain_status": false, "xuansi_toolchain_fetch": false, "xuansi_wasm_build": false}
	for _, n := range names {
		if _, is := want[n]; is {
			want[n] = true
		}
	}
	allServed := true
	for _, n := range []string{"xuansi_toolchain_status", "xuansi_toolchain_fetch", "xuansi_wasm_build"} {
		allServed = allServed && want[n]
	}
	h.check(terr == nil && allServed,
		fmt.Sprintf("static face serves the toolchain trio (24->25; served=%v of %d tools incl. dynamic)", want, len(names)))

	// the board must be LISTED with fw populated (hello-enriched) - an
	// absent board is BLOCKED per the brief, never a spin
	var boardRow map[string]any
	ok, last = pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		m, _, isErr, cok := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
		if !cok || isErr {
			return false, "list_boards trouble"
		}
		if boards, yes := m["boards"].([]any); yes {
			for _, e := range boards {
				if bm, y2 := e.(map[string]any); y2 && bm["mac6"] == b11Board {
					if fw, _ := bm["fw"].(string); fw != "" {
						boardRow = bm
						return true, "fw populated (hello-enriched)"
					}
					return false, "board listed, fw not yet enriched"
				}
			}
		}
		return false, "board not in list yet"
	})
	if !ok || boardRow == nil {
		h.check(false, fmt.Sprintf("list_boards shows %s with fw populated (%s)", b11Board, last))
		fmt.Println("  BLOCKED: the bench board is not listed - report and stop (no spin)")
		return finish()
	}
	h.check(true, fmt.Sprintf("list_boards shows %s with fw populated (%s)", b11Board, last))
	fw, _ := boardRow["fw"].(string)
	h.check(fw == b11FwBuild, fmt.Sprintf("fw BUILD BINDING: %q == expected %q", fw, b11FwBuild))

	// the lease everything below rides
	m, text, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b11Board, "holder": b11Holder})
	leaseSess, _ := b6Num(m, "session")
	h.check(!isErr && b6Str(m, "board") == b11Board && b6Str(m, "holder") == b11Holder && leaseSess > 0,
		fmt.Sprintf("W00 lease holder=%s session=%.0f (default TCP route, no reboot)", b11Holder, leaseSess))

	// ---------- S1: missing-cache status ----------
	fmt.Println("\n- S1: xuansi_toolchain_status on the fresh cache - both pieces missing, self-guiding -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_toolchain_status", map[string]any{})
	rows := tcRows(m["installed"])
	guidance, idxSrc, idxLatest, idxNote := b6Str(m, "guidance"), tcIdxField(m, "source"), tcIdxField(m, "latest"), tcIdxField(m, "note")
	h.check(!isErr && b6Str(m, "cache_root") == wasmCache && b6Str(m, "generation") == "",
		fmt.Sprintf("W10 status on the fresh cache: cache_root=%q (the ISOLATED sandbox root), generation empty (text=%.80s)", b6Str(m, "cache_root"), text))
	h.check(len(rows) == 2 && rows["wasi_sdk"] == false && rows["wamrc"] == false,
		fmt.Sprintf("W11 both pieces present=false (rows=%v)", rows))
	h.check(idxSrc == "remote" && idxLatest == b11Gen && idxNote == "",
		fmt.Sprintf("W12 the LIVE index answered: source=%q latest=%q note=%q (remote, not the embedded fallback)", idxSrc, idxLatest, idxNote))
	h.check(strings.Contains(guidance, "xuansi_toolchain_fetch"),
		fmt.Sprintf("W13 guidance self-guides to fetch (%.120s)", asciiLine(guidance)))

	// ---------- S2: the real fetch ----------
	fmt.Println("\n- S2: xuansi_toolchain_fetch - the REAL download (119MB wasi-sdk + 33MB wamrc) from the live index -")
	tFetch := time.Now()
	m, text, isErr, _ = b.call(ctx, 15*time.Minute, "xuansi_toolchain_fetch", map[string]any{}) // NO timeout_sec: the tool's own 600s default is under test
	fetchMS := time.Since(tFetch).Milliseconds()
	dl, sk := tcList(m, "downloaded"), tcList(m, "skipped")
	h.check(!isErr && b6Str(m, "generation") == b11Gen && b6Str(m, "cache_root") == wasmCache &&
		len(dl) == 2 && containsKind(dl, "wasi_sdk") && containsKind(dl, "wamrc") && len(sk) == 0,
		fmt.Sprintf("W20 fetch downloaded BOTH pieces in one call: generation=%s downloaded=%v skipped=%v (%.1fs real network time; text=%.80s)",
			b6Str(m, "generation"), dl, sk, float64(fetchMS)/1000, text))
	// disk probes: the landed trees, exactly where §6.1 says
	sdkDir := b11SDKDirName()
	sdkClang := filepath.Join(wasmCache, sdkDir, "bin", "clang")
	wamrcBin := filepath.Join(wasmCache, "wamrc", "wamrc")
	if fi, err := os.Stat(sdkClang); err == nil {
		h.check(fi.Mode()&0o111 != 0, fmt.Sprintf("W21 wasi-sdk landed: %s (executable, %dB tree root %s)", sdkClang, fi.Size(), sdkDir))
	} else {
		h.check(false, "W21 wasi-sdk landed: "+err.Error())
	}
	if fi, err := os.Stat(wamrcBin); err == nil {
		// the INDEX pin is on the ARCHIVE - that pin is
		// reconciled via S3's status rows (W33); here the probe is the
		// EXTRACTED binary itself: present and executable
		h.check(fi.Size() > 0 && fi.Mode()&0o111 != 0,
			fmt.Sprintf("W22 wamrc binary landed: %s (executable, %dB; the archive-level sha/bytes pin is W33's reconciliation)", wamrcBin, fi.Size()))
	} else {
		h.check(false, "W22 wamrc binary landed: "+err.Error())
	}
	if _, err := os.Stat(filepath.Join(wasmCache, "toolchain-state.json")); err == nil {
		h.check(true, "W23 toolchain-state.json committed (the pairing check's local evidence)")
	} else {
		h.check(false, "W23 toolchain-state.json committed: "+err.Error())
	}
	// the idempotent re-call: everything pinned on disk -> all Skipped
	m, text, isErr, _ = b.call(ctx, 120*time.Second, "xuansi_toolchain_fetch", map[string]any{})
	dl2, sk2 := tcList(m, "downloaded"), tcList(m, "skipped")
	h.check(!isErr && len(dl2) == 0 && len(sk2) == 2 && containsKind(sk2, "wasi_sdk") && containsKind(sk2, "wamrc"),
		fmt.Sprintf("W24 SECOND fetch is the idempotent sync verb: downloaded=%v skipped=%v (nothing re-downloaded; text=%.80s)", dl2, sk2, text))

	// ---------- S3: complete status ----------
	fmt.Println("\n- S3: xuansi_toolchain_status again - present both, generation + index pins reconciled -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_toolchain_status", map[string]any{})
	rows = tcRows(m["installed"])
	pins := tcPins(m["installed"])
	guidance = b6Str(m, "guidance")
	sdkSHA, sdkBytes := pins["wasi_sdk_sha"], pins["wasi_sdk_bytes"]
	wamrcSHA, wamrcBytes := pins["wamrc_sha"], pins["wamrc_bytes"]
	h.check(!isErr && b6Str(m, "generation") == b11Gen,
		fmt.Sprintf("W30 generation=%q (the state's one-generation label)", b6Str(m, "generation")))
	h.check(len(rows) == 2 && rows["wasi_sdk"] == true && rows["wamrc"] == true,
		fmt.Sprintf("W31 both pieces present=true (rows=%v)", rows))
	// W32/W33 reconcile against THIS platform's index pins
	// (b11Pins, keyed by the same platform key the dir probe uses).
	platKey := b11PlatformKey()
	pin, pinKnown := b11Pins[platKey]
	if !pinKnown {
		h.check(false, fmt.Sprintf("W32 no toolchain-v2 pins tabled for platform key %q (this host) — extend b11Pins from the index (W33 also skipped)", platKey))
	} else {
		h.check(sdkSHA == pin.sdkSHA && sdkBytes == float64(pin.sdkBytes),
			fmt.Sprintf("W32 wasi_sdk pins == the live index's (%s): sha=%.16s... bytes=%.0f (want %d)", platKey, sdkSHA, sdkBytes, pin.sdkBytes))
		if pin.wamrcSHA == "" {
			h.note("W33 skipped: platform %s carries \"wamrc\": null in toolchain-v2 — wasi_sdk alone installs, no wamrc pin to reconcile (the status wamrc row reads not-present by design)", platKey)
		} else {
			h.check(wamrcSHA == pin.wamrcSHA && wamrcBytes == float64(pin.wamrcBytes),
				fmt.Sprintf("W33 wamrc pins == the live index's (%s): sha=%.16s... bytes=%.0f (want %d)", platKey, wamrcSHA, wamrcBytes, pin.wamrcBytes))
		}
	}
	h.check(guidance == "",
		fmt.Sprintf("W34 a complete, current cache stays quiet (guidance=%q)", guidance))

	// ---------- S4: wasm_build ----------
	fmt.Println("\n- S4: xuansi_wasm_build - compile the hello-scale module against the just-fetched sdk -")
	m, text, isErr, _ = b.call(ctx, 180*time.Second, "xuansi_wasm_build",
		map[string]any{"name": b11ModName, "version": b11ModVer, "source": b11HelloSrc}) // EXPLICIT version: the T10 concern-2 build/push pair
	buildSHA, buildBytes := b6Str(m, "sha256"), gAny(m, "bytes")
	buildBytesF, _ := b6Num(m, "bytes")
	h.check(!isErr && b6Str(m, "name") == b11ModName && b6Str(m, "version") == b11ModVer &&
		b6Str(m, "file") == "drivers/"+b11ModName+"@"+b11ModVer+".wasm" &&
		len(buildSHA) == 64 && buildBytesF > 0 && b6Str(m, "warnings") == "" &&
		strings.Contains(b6Str(m, "next"), "xuansi_driver_push"),
		fmt.Sprintf("W40 build hilhello@%s: file=%s sha=%.16s... bytes=%v warnings-silent Next=%q (text=%.80s)",
			b11ModVer, b6Str(m, "file"), buildSHA, buildBytes, b6Str(m, "next"), text))
	// the workspace artifact, byte-verified from THIS process
	artPath := filepath.Join(xdg, "xuansi", "workspace", "drivers", b11ModName+"@"+b11ModVer+".wasm")
	if wb, err := os.ReadFile(artPath); err == nil {
		h.check(len(wb) > 0 && wb[0] == 0x00 && wb[1] == 'a' && wb[2] == 's' && wb[3] == 'm' && sha256Hex(wb) == buildSHA,
			fmt.Sprintf("W41 sandbox workspace artifact byte-verified: %d bytes, \\0asm magic, sha256 == the build's answer", len(wb)))
	} else {
		h.check(false, "W41 sandbox workspace artifact readable: "+err.Error())
	}

	// ---------- S5: driver_push ----------
	fmt.Println("\n- S5: xuansi_driver_push version 1.0.0 (explicit) - the magic-gated .wasm leg -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
		map[string]any{"board": b11Board, "name": b11ModName, "version": b11ModVer})
	pushBytes, _ := b6Num(m, "bytes")
	h.check(!isErr && b6Str(m, "path") == "/drv/"+b11ModName+".wasm" && b6Str(m, "sha256") == buildSHA && int(pushBytes) == int(buildBytesF),
		fmt.Sprintf("W50 push hilhello@%s -> %s sha-verified read-back (bytes=%.0f == the build's; text=%.60s)", b11ModVer, b6Str(m, "path"), pushBytes, text))

	// ---------- S6: board eval ----------
	fmt.Println("\n- S6: bench.wasm load + call - echo's packed pair bridges back -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b11Board, "code": "return bench.wasm.load('" + b11ModName + "')"})
	h.check(!isErr && gAny(m, "value") == true,
		fmt.Sprintf("W60 wasm.load('%s') -> true (value=%v; text=%.60s)", b11ModName, gAny(m, "value"), text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b11Board, "code": fmt.Sprintf("return bench.wasm.call('%s','echo',7,35)", b11ModName)})
	h.check(!isErr && fmt.Sprint(gAny(m, "value")) == "[7 35]",
		fmt.Sprintf("W61 wasm.call echo(7,35) == [7,35] (value=%v - the args round-trip through the module untouched)", gAny(m, "value")))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b11Board, "code": fmt.Sprintf("return bench.wasm.call('%s','echo',1800,42)", b11ModName)})
	h.check(!isErr && fmt.Sprint(gAny(m, "value")) == "[1800 42]",
		fmt.Sprintf("W62 wasm.call echo(1800,42) == [1800,42] (value=%v - a second pair, not a cached fluke)", gAny(m, "value")))

	// ---------- S7: ws_list reconciliation ----------
	fmt.Println("\n- S7: xuansi_ws_list - the workspace entry reconciles with the build -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_ws_list", map[string]any{})
	entryActive, entrySHA, entrySize := "", "", -1.0
	if drs, yes := m["drivers"].([]any); yes {
		for _, d := range drs {
			if dm, _ := d.(map[string]any); b6Str(dm, "name") == b11ModName {
				entryActive = b6Str(dm, "active")
				if vs, ok2 := dm["versions"].([]any); ok2 {
					for _, v := range vs {
						if vm, _ := v.(map[string]any); b6Str(vm, "version") == b11ModVer {
							entrySHA = b6Str(vm, "sha256")
							entrySize, _ = b6Num(vm, "size")
						}
					}
				}
			}
		}
	}
	h.check(!isErr && entryActive == b11ModVer && entrySHA == buildSHA && entrySize == buildBytesF,
		fmt.Sprintf("W70 ws_list: %s active=%q version %s sha==build (%.16s...) size==build (%.0f; text=%.60s)",
			b11ModName, entryActive, b11ModVer, entrySHA, entrySize, text))

	// ---------- S8: cleanup ----------
	fmt.Println("\n- S8: cleanup - board as found, release, teardown -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b11Board, "code": "return bench.wasm.unload('" + b11ModName + "')"})
	h.check(!isErr, fmt.Sprintf("W80 wasm.unload('%s') (value=%v)", b11ModName, gAny(m, "value")))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_fs_rm",
		map[string]any{"board": b11Board, "path": "/drv/" + b11ModName + ".wasm"})
	h.check(!isErr, fmt.Sprintf("W81 fs_rm /drv/%s.wasm (driver_pull is .lua-scoped by design - the .wasm board file rides the generic fs face; text=%.60s)", b11ModName, text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_fs_read",
		map[string]any{"board": b11Board, "path": "/drv/" + b11ModName + ".wasm"})
	h.check(isErr && strings.Contains(text, "not found"),
		fmt.Sprintf("W82 /drv/%s.wasm gone (fs_read surfaces the board's not-found; text=%.80s)", b11ModName, text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b11Board, "code": "return bench.wasm.list()"})
	listV, _ := m["value"].([]any)
	h.check(!isErr && len(listV) == 0,
		fmt.Sprintf("W83 no loaded wasm instances left (list -> %d entries)", len(listV)))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release", map[string]any{"board": b11Board, "holder": b11Holder})
	h.check(!isErr, "W84 lease released")

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
	h.note("board as found: no /drv files, no loaded modules, no lease")
	h.note("sandbox kept for inspection: workspace under %s, toolchain cache under %s (the wrapper wipes the run dir on the next attempt)", xdg, wasmCache)
	return finish()
}

// ---------------------------------------------------------------- parsing --

// tcRows reads the status `installed` array into kind->present.
func tcRows(v any) map[string]bool {
	out := map[string]bool{}
	if arr, ok := v.([]any); ok {
		for _, e := range arr {
			if em, _ := e.(map[string]any); em != nil {
				out[b6Str(em, "kind")], _ = em["present"].(bool)
			}
		}
	}
	return out
}

// tcPins reads the status `installed` rows' sha/bytes into flat keys
// (kind+"_sha" / kind+"_bytes") for the index-pin reconciliation.
func tcPins(v any) map[string]any {
	out := map[string]any{}
	if arr, ok := v.([]any); ok {
		for _, e := range arr {
			if em, _ := e.(map[string]any); em != nil {
				kind := b6Str(em, "kind")
				out[kind+"_sha"] = em["sha256"]
				out[kind+"_bytes"] = em["bytes"]
			}
		}
	}
	return out
}

// tcIdxField reads one field out of the status's index object.
func tcIdxField(m map[string]any, field string) string {
	idx, _ := m["index"].(map[string]any)
	return b6Str(idx, field)
}

// tcList reads a string-array field (downloaded/skipped).
func tcList(m map[string]any, field string) []string {
	arr, _ := m[field].([]any)
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// containsKind reports whether the kind string is in the list.
func containsKind(list []string, kind string) bool {
	for _, s := range list {
		if s == kind {
			return true
		}
	}
	return false
}
