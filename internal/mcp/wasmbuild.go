package mcp

// wasmbuild.go — the wasm build face (B11 tasks 4+5, design
// §6.2/§6.3/§6.4): the internal core below, plus the xuansi_wasm_build
// handler that gates tool input, calls the core, and lands the
// artifact in the driver workspace — registration lives in
// tools_static.go with every static tool.
//
//   - tcBuildFlags: the FROZEN module-ABI §8 clang flag block,
//     single-sourced here (drivers build.sh's twin — T9's CI
//     cross-leg rebuilds the drivers modules through this function
//     and hash-compares, so a drift on either side is a red build).
//   - aotGate / fwGateWasm: the split-track pairing gates (§6.2).
//     Since P3-T7 toolchain-v2's aot domain is OPEN
//     (fw_compat.aot >=0.3.0): .aot is a real artifact lane — the
//     wamrc_path §6.4 override compiles the freshly-built .wasm one
//     step further with wamrc — and the aot gate's job is the EARLY
//     fw pairing refusal below the 0.3.0 floor (the 分發 spec's
//     配對紀律: refuse at BUILD time, not at board load). The wasm
//     track's domain stays the WIDE one (bytecode is version-less),
//     floored at the first fw with a wasm loader (0.2.0).
//   - wasmBuildInner: resolve the wasi-sdk (wasi_sdk_path override >
//     the cache state's landed record > the self-guiding fetch
//     refusal), expand the embedded xuansi_host.h into a temp include
//     dir, exec <sdk>/bin/clang, and return the .wasm bytes with
//     clang's stdout/stderr verbatim as warnings.
//   - wamrcExec: the aot lane's exec half (P3-T7: the not-yet stub
//     retired with the domain opening) — stage the .wasm, exec the
//     §6.4 wamrc with build.sh's frozen recipe, gate the output's
//     \0aot container magic, return the .aot bytes with wamrc's
//     transcript verbatim.
//   - wasmBuild (T5): the tool face — input gates, the gates above,
//     the compile, then drivers/<name>@<ver>.wasm (or .aot on the
//     wamrc_path lane) + one manifest cycle (kind "wasm", or
//     "wasm-dev" when ANY §6.4 override — wamrc_path or
//     wasi_sdk_path — is in play).

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// wasmHdrXuansiHost is the drivers repo's src/xuansi_host.h, embedded
// (the wasmhdr/ twin — provenance block inside the file). Expanded
// into the build's temp include dir so daemon-built modules see the
// same frozen host table the drivers repo compiles against.
//
//go:embed wasmhdr/xuansi_host.h
var wasmHdrXuansiHost []byte

// The ABI freeze's memory contract (module-ABI §8): the default
// initial linear memory is 4 pages (256 KiB — covers hello-scale
// modules; ledwatch's working set pins 11), every initial-memory a
// module asks for must be a whole wasm page, and the max-memory
// ceiling is frozen at 1 MiB (the linker flag mirrors tcWasmMaxMemory).
const (
	tcWasmInitialMemory int64 = 262144  // 4 x 64 KiB pages
	tcWasmPageSize      int64 = 65536   // the wasm page — the multiple rule
	tcWasmMaxMemory     int64 = 1048576 // 1 MiB frozen ceiling
)

// buildReq is one wasm build request's inner shape — T5's handler
// maps its tool input onto this after the tool-level gates
// (name/version regexes, source size cap). WamrcPath/WasiSDKPath are
// the §6.4 developer overrides: explicit, and (for WasiSDKPath)
// bypassing the cache-state pairing evidence on purpose.
type buildReq struct {
	Name, Source, Version      string
	InitialMemory              int64
	FW, WamrcPath, WasiSDKPath string
	WantAOT                    bool
}

// tcInitialMemory resolves the one ABI knob an explicit request may
// turn: <=0 (unset, or nonsense-negative) reads as the frozen
// 262144 default; an explicit value must be a whole multiple of the
// 64 KiB wasm page — wasm-ld refuses anything else, and this gate
// says so cheaper and clearer than the linker would. The max-memory
// ceiling needs no gate here: the frozen -Wl,--max-memory flag rides
// every build, and wasm-ld itself refuses initial > max.
func tcInitialMemory(n int64) (int64, error) {
	if n <= 0 {
		return tcWasmInitialMemory, nil
	}
	if n%tcWasmPageSize != 0 {
		return 0, errWs("xuansi_wasm_build: initial_memory %d is not a multiple of the 65536-byte wasm page (wasm-ld refuses it; the ABI freeze default is 262144, max 1048576)", n)
	}
	return n, nil
}

// tcBuildFlags is the FROZEN module-ABI §8 flag block — the entire
// clang argv except the trailing `-o <out> <src.c>` pair, which the
// exec'ing caller (wasmBuildInner) owns because it owns the temp
// paths. sdkRoot is the resolved wasi-sdk tree (state record or the
// wasi_sdk_path override): the sysroot derives from it either way.
// includeDir holds the expanded wasmhdr/xuansi_host.h.
//
// Order and spelling are build.sh's; the one deliberate divergence
// is build.sh's diagnostic tail (-Wall -Wextra
// -Wno-unused-parameter) — codegen-neutral warning flags the drivers
// CI wants for its own sources, omitted here because this face
// already surfaces clang's stderr verbatim and user source never
// signed up for -Wall noise.
//
// The initial-memory value resolves via tcInitialMemory: callers gate
// the refusal FIRST (wasmBuildInner does); with a gated request the
// error path is unreachable and the resolved value rides verbatim.
func tcBuildFlags(r buildReq, sdkRoot, includeDir string) []string {
	mem, _ := tcInitialMemory(r.InitialMemory)
	return []string{
		"--target=wasm32-wasi",
		"--sysroot=" + filepath.Join(sdkRoot, "share", "wasi-sysroot"),
		"-O2",
		"-fno-jump-tables",
		"-nostdlib",
		"-Wl,--no-entry",
		"-Wl,--allow-undefined",
		fmt.Sprintf("-Wl,--max-memory=%d", tcWasmMaxMemory),
		"-Wl,--global-base=1024",
		"-Wl,--strip-debug",
		"-Wl,--export-dynamic",
		fmt.Sprintf("-Wl,--initial-memory=%d", mem),
		"-I", includeDir,
	}
}

// tcClangName is the compiler binary's name inside the resolved
// wasi-sdk tree: the official windows SDK ships bin/clang.exe, every
// other platform bin/clang (final-review I1 — the stat probe and the
// exec must agree on ONE name, resolved once here). Parameterized by
// goos (not runtime.GOOS directly) so the test pins every platform's
// branch from any host.
func tcClangName(goos string) string {
	if goos == "windows" {
		return "clang.exe"
	}
	return "clang"
}

// aotFWFloor is the aot track's fw floor — 0.3.0, the first fw with
// an aot loader (internal-SRAM placement + the caps
// aot_exec_budget_bytes budget gate, T6). Hardcoded here exactly as
// fwGateWasm hardcodes its own floor: the build face never reads an
// index (the cache state IS the pairing evidence), and the value is
// locked to the index's fw_compat.aot by TestEmbeddedSnapshotParses
// (">=0.3.0") — the drift-lock's third leg. A future generation
// moving the floor moves all three in one red sweep.
const aotFWFloor = "0.3.0"

// aotGate is the split-track pairing gate's aot half (§6.2). Since
// P3-T7 toolchain-v2's aot domain is OPEN — the stock wamrc compiles
// .aot for fw 0.3.0+ — the gate's job is the EARLY pairing refusal
// the 分發 spec's 配對紀律 demands: an explicit fw below the 0.3.0
// floor refuses at BUILD time with the honest why (that firmware has
// no aot loader), never left to blow at board load. The lane itself
// needs a wamrc: the §6.4 wamrc_path override today (dev lane, kind
// wasm-dev); the cache-paired DEFAULT wamrc arrives with
// toolchain-v3 (P3-T12) — an internal WantAOT with no wamrc is
// refused here honestly, naming both.
func aotGate(r buildReq) error {
	if !r.WantAOT {
		return nil
	}
	if r.FW != "" && cmpVer(r.FW, aotFWFloor) < 0 {
		return errWs("xuansi_wasm_build: fw %q is below the aot track's fw floor 0.3.0 (fw_compat.aot=\">=0.3.0\") — this firmware has no aot loader; upgrade the board, or omit fw to build without the compat check", r.FW)
	}
	if r.WamrcPath == "" {
		return errWs("xuansi_wasm_build: the aot lane needs a wamrc — pass wamrc_path (the §6.4 dev override; the toolchain-v2 stock wamrc xuansi_toolchain_fetch installs compiles the .aot fw 0.3.0+ loads); the cache-paired default wamrc arrives with toolchain-v3")
	}
	return nil
}

// fwGateWasm is the split-track pairing gate's wasm half (§6.2): the
// wasm track's fw domain is the WIDE one — bytecode is version-less —
// floored at 0.2.0, the first fw with a wasm loader. An omitted fw
// passes (the explicit-declaration semantic belongs to T5's schema;
// this gate only judges what it is given). Comparison rides cmpVer
// (numeric segments, missing = 0), so a pre-release suffix like
// "0.2.0-rc1" compares on its numeric segments and reads as 0.2.0 —
// lenient on purpose: rc boards are expert territory, and this gate
// exists to catch the 0.1.x line, not to police suffixes.
func fwGateWasm(fw string) error {
	if fw == "" {
		return nil
	}
	if cmpVer(fw, "0.2.0") < 0 {
		return errWs("xuansi_wasm_build: fw %q is below the wasm track's fw floor 0.2.0 (fw_compat.wasm=\">=0.2.0\") — this firmware has no wasm loader; upgrade the board, or omit fw to build without the compat check", fw)
	}
	return nil
}

// wamrcExec is the aot lane's exec half — P3-T7 retired the not-yet
// stub when toolchain-v2's aot domain opened. Stages the freshly
// built .wasm bytes into one temp dir, execs the §6.4 wamrc with
// build.sh's frozen recipe VERBATIM (--target=xtensa --cpu=esp32s3
// --size-level=3 — the drivers repo's own invocation, so a
// daemon-built .aot is recipe-comparable with the repo's auxiliary
// artifacts), and returns the .aot bytes with wamrc's stdout/stderr
// verbatim as warnings. The probe gives a missing binary the
// broken-tree refusal (the clang twin's shape); a failing wamrc
// refuses carrying the transcript tail (wamrc prints its errors on
// STDOUT, so the tail rides the combined transcript); a "successful"
// run whose output lacks the \0aot container magic (aotMagic — the
// SAME byte-for-byte
// fw-loader gate driver_push enforces, workspace.go's B10 table) is
// refused too — wamrc exiting 0 with garbage must never land as an
// artifact. The compile cap rides tcRunCompile (tcCompileTimeout
// bounds ONE compiler run — clang's and wamrc's each get their own).
func wamrcExec(ctx context.Context, r buildReq, wasm []byte) (aotBytes []byte, warnings string, err error) {
	if _, serr := os.Stat(r.WamrcPath); serr != nil {
		return nil, "", errWs("xuansi_wasm_build: no wamrc at %s — the aot lane needs the toolchain-v2 stock wamrc (xuansi_toolchain_fetch installs it) or an explicit local build; manual steps: see the xuansi-drivers README's User-compiled modules section", r.WamrcPath)
	}
	base := "module"
	if r.Name != "" {
		if !wsNameRe.MatchString(r.Name) {
			return nil, "", errWs("xuansi_wasm_build: name must match [a-z0-9_-]+, got %q", r.Name)
		}
		base = r.Name
	}
	tmp, terr := os.MkdirTemp("", "xuansi-wamrc-*")
	if terr != nil {
		return nil, "", errWs("xuansi_wasm_build: wamrc staging dir: %v", terr)
	}
	defer os.RemoveAll(tmp)
	wasmPath := filepath.Join(tmp, base+".wasm")
	if err := os.WriteFile(wasmPath, wasm, 0o644); err != nil {
		return nil, "", errWs("xuansi_wasm_build: stage .wasm for wamrc: %v", err)
	}
	aotPath := filepath.Join(tmp, base+".aot")

	stdoutS, stderrS, runErr, timedOut := tcRunCompile(ctx, r.WamrcPath, []string{
		"--target=xtensa", "--cpu=esp32s3", "--size-level=3",
		"-o", aotPath, wasmPath,
	})
	warnings = stdoutS + stderrS
	if timedOut {
		return nil, warnings, runErr
	}
	if runErr != nil {
		// the TAIL rides the combined transcript (the seat check's
		// finding: wamrc prints ITS errors on STDOUT — "llvm get
		// target from triple ... failed" — unlike clang's stderr, so
		// an stderr-only tail like clang's refusal would arrive empty
		// exactly when it matters).
		return nil, warnings, errWs("xuansi_wasm_build: wamrc failed (%v): %s", runErr, tcWarnTail(warnings))
	}
	aot, rerr := os.ReadFile(aotPath)
	if rerr != nil {
		return nil, warnings, errWs("xuansi_wasm_build: wamrc reported success but no .aot landed: %v (transcript: %s)", rerr, tcWarnTail(warnings))
	}
	if len(aot) < len(aotMagic) || !bytes.HasPrefix(aot, aotMagic[:]) {
		got := aot
		if len(got) > 8 {
			got = got[:8]
		}
		return nil, warnings, errWs("xuansi_wasm_build: wamrc output is not an .aot container (magic % x, want \\0aot) — not a stock-wamrc xtensa build; the artifact is refused", got)
	}
	return aot, warnings, nil
}

// tcCompileTimeout bounds ONE clang compile (final-review M4: the
// tool call's ctx is caller-bounded and fetch has its timeout_sec
// discipline — the build had no internal bound, so a wedged compiler
// could hold the tool hostage indefinitely). A var (not a const)
// only as the timeout test's seam, tcExtractMax's discipline: no
// production path assigns it.
var tcCompileTimeout = 120 * time.Second

// tcRunCompile execs the compiler under the compile cap and returns
// its stdout/stderr VERBATIM (captured separately — one shared buffer
// would race exec's two copy goroutines) plus the run error. A cap
// firing is reported as timedOut=true with a shaped api refusal
// naming the timeout and the manual fallback.
func tcRunCompile(ctx context.Context, clang string, args []string) (stdout, stderr string, runErr error, timedOut bool) {
	cctx, cancel := context.WithTimeout(ctx, tcCompileTimeout)
	defer cancel()
	var out, errB bytes.Buffer
	cmd := exec.CommandContext(cctx, clang, args...)
	cmd.Stdout, cmd.Stderr = &out, &errB
	runErr = cmd.Run()
	stdout, stderr = out.String(), errB.String()
	if runErr != nil && cctx.Err() == context.DeadlineExceeded {
		return stdout, stderr, errWs("xuansi_wasm_build: compile timeout after %s (clang killed mid-compile — a <=1 MiB source compiles in seconds; retry, or build the module manually per the xuansi-drivers README's User-compiled modules section)", tcCompileTimeout), true
	}
	return stdout, stderr, runErr, false
}

// tcWarnTail bounds a compiler transcript to its tail — the part with
// the error and its carets. The full stdout/stderr rides warnings
// verbatim; this is only the refusal message's excerpt.
func tcWarnTail(s string) string {
	const max = 1200
	if len(s) <= max {
		return s
	}
	cut := s[len(s)-max:]
	if i := strings.IndexByte(cut, '\n'); i >= 0 && i+1 < len(cut) {
		cut = cut[i+1:] // resume at a line boundary when one is nearby
	}
	return "..." + cut
}

// wasmBuildInner compiles one C source to .wasm bytes — the build
// face's whole core (T5's handler adds the tool gates and the
// workspace landing). Resolution order (§6.4): WasiSDKPath override
// first — the developer's explicit "compile with my local tree",
// which deliberately bypasses the cache-state pairing evidence
// (T5's manifest marks the artifact dev — any §6.4 override, this
// one included, lands Kind "wasm-dev") and derives the
// sysroot from ITS root — then the cache state's landed wasi_sdk
// record (the T3 path contract: tcCacheRoot() + the record's Dir),
// then the self-guiding fetch refusal (§6.3 UX: the error carries
// the next step, always).
func (ts *toolset) wasmBuildInner(ctx context.Context, r buildReq) (wasmBytes []byte, warnings string, err error) {
	// the one ABI knob gates FIRST — a non-page-multiple memory is a
	// caller bug, refused before any filesystem or exec work.
	if _, err := tcInitialMemory(r.InitialMemory); err != nil {
		return nil, "", err
	}

	sdkRoot := r.WasiSDKPath
	if sdkRoot == "" {
		root := tcCacheRoot()
		if root == "" {
			return nil, "", errWs("xuansi_wasm_build: no toolchain cache root (XUANSI_WASM_CACHE unset and os.UserCacheDir failed) — call xuansi_toolchain_fetch (recommended) once anywhere on this host, set XUANSI_WASM_CACHE, or pass wasi_sdk_path")
		}
		tcMu.Lock()
		st, lerr := loadTCState(filepath.Join(root, toolchainStateFile))
		tcMu.Unlock()
		if lerr != nil {
			// a corrupt state is NOT "nothing installed": fetch would
			// refuse on the same bytes, so the guidance must not point
			// there — name the actual problem (loadTCState's
			// corrupt-refusal discipline, loadManifest's twin).
			return nil, "", errWs("xuansi_wasm_build: %v", lerr)
		}
		rec, ok := st.Installed["wasi_sdk"]
		if !ok || rec.Dir == "" || !tcPathExists(filepath.Join(root, rec.Dir)) {
			return nil, "", errWs("xuansi_wasm_build: no wasi-sdk installed in the toolchain cache — call xuansi_toolchain_fetch (recommended), or pass wasi_sdk_path to point at a local tree; manual steps: see the xuansi-drivers README's User-compiled modules section")
		}
		sdkRoot = filepath.Join(root, rec.Dir)
	}

	// the compiler itself: an absolute path inside the resolved tree,
	// never a PATH lookup — named for the HOST (bin/clang.exe on
	// windows, final-review I1; the probe and the exec share the one
	// tcClangName resolution). The probe gives the broken-tree case
	// (an override at a tree with no compiler, or a cache tree that
	// lost its) a refusal that names the missing binary — M3's
	// fetch-side shape gate, build-side twin.
	clang := filepath.Join(sdkRoot, "bin", tcClangName(runtime.GOOS))
	if _, serr := os.Stat(clang); serr != nil {
		return nil, "", errWs("xuansi_wasm_build: no clang at %s — the wasi-sdk tree is not usable (unexpected wasi-sdk layout); call xuansi_toolchain_fetch to reinstall, or point wasi_sdk_path at a tree with bin/clang; manual steps: see the xuansi-drivers README's User-compiled modules section", clang)
	}

	// staging: one temp dir holding the expanded host header, the
	// source, and the output. The source keeps its fixed name
	// (module.c — the compile is source-name-blind); the OUTPUT rides
	// the request's name when it carries one: wasm-ld embeds the -o
	// basename as the wasm module's name (the "name" custom section's
	// module-name subsection), so a daemon-built artifact must stage
	// its output as <name>.wasm to be byte-identical with the drivers
	// repo's own builds — T9's parity leg locks exactly this. The name
	// now reaches a path, so an EXPLICIT name is gated here too (the
	// tool layer's gate rides again as defense in depth); empty keeps
	// the legacy module.wasm staging (the inner stays name-optional).
	base := "module"
	if r.Name != "" {
		if !wsNameRe.MatchString(r.Name) {
			return nil, "", errWs("xuansi_wasm_build: name must match [a-z0-9_-]+, got %q", r.Name)
		}
		base = r.Name
	}
	tmp, terr := os.MkdirTemp("", "xuansi-wasm-build-*")
	if terr != nil {
		return nil, "", errWs("xuansi_wasm_build: staging dir: %v", terr)
	}
	defer os.RemoveAll(tmp)
	incDir := filepath.Join(tmp, "include")
	if err := os.MkdirAll(incDir, 0o755); err != nil {
		return nil, "", errWs("xuansi_wasm_build: include dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(incDir, "xuansi_host.h"), wasmHdrXuansiHost, 0o644); err != nil {
		return nil, "", errWs("xuansi_wasm_build: expand xuansi_host.h: %v", err)
	}
	srcPath := filepath.Join(tmp, "module.c")
	if err := os.WriteFile(srcPath, []byte(r.Source), 0o644); err != nil {
		return nil, "", errWs("xuansi_wasm_build: stage source: %v", err)
	}
	outPath := filepath.Join(tmp, base+".wasm")

	// compile (M4: under tcRunCompile's own cap — a wedged compiler
	// cannot hold the tool past it). stdout and stderr surface VERBATIM,
	// concatenated, as warnings; a failed clang repeats the stderr tail
	// inside the refusal so the error itself carries the diagnostic.
	args := append(tcBuildFlags(r, sdkRoot, incDir), "-o", outPath, srcPath)
	stdoutS, stderrS, runErr, timedOut := tcRunCompile(ctx, clang, args)
	warnings = stdoutS + stderrS
	if timedOut {
		return nil, warnings, runErr
	}
	if runErr != nil {
		return nil, warnings, errWs("xuansi_wasm_build: clang failed (%v): %s", runErr, tcWarnTail(stderrS))
	}

	wasm, rerr := os.ReadFile(outPath)
	if rerr != nil {
		return nil, warnings, errWs("xuansi_wasm_build: clang reported success but no .wasm landed: %v (transcript: %s)", rerr, tcWarnTail(warnings))
	}
	return wasm, warnings, nil
}

// ---------------------------------------------------------------------------
// the tool face (B11 task 5)

// wasmBuildSourceMax caps the C source one tool call may carry: 1 MiB,
// the internal wire's own frame ceiling — a module whose source runs
// past it belongs in the drivers repo (fetch its prebuilt artifact
// with xuansi_driver_fetch), and the refusal says so.
const wasmBuildSourceMax = 1 << 20

// wasmBuildFirstVersion is the omitted version's default: the
// first-build convention "0.0.1". driver_fetch's omitted version
// takes the index's highest for the name — build has no index to ask,
// so the default says "this is the module's first build"; a rebuilt
// module versions explicitly.
const wasmBuildFirstVersion = "0.0.1"

// wasmBuildNext is the out's fixed next-step sentence — the build
// verb's whole point is to hand the artifact to the deploy verb.
const wasmBuildNext = "push with xuansi_driver_push {board, name} then bench.wasm.load/call"

// wasmBuildIn is xuansi_wasm_build's input. No index_url: the build
// face never reads an index — the toolchain cache's state IS the
// pairing evidence (the plan's IndexURL row was driver_fetch's, not
// this tool's).
type wasmBuildIn struct {
	Name   string `json:"name" jsonschema:"module name, lowercase [a-z0-9_-]+ (e.g. \"hello\", \"ledwatch\") — the workspace file is drivers/<name>@<version>.wasm"`
	Source string `json:"source" jsonschema:"the module's full C source, at most 1 MiB (over is an api refusal naming the cap). #include \"xuansi_host.h\" resolves — the frozen host table every drivers-repo module compiles against"`
	// Version omitted = wasmBuildFirstVersion (the first-build
	// convention — there is no index whose highest to take).
	Version string `json:"version,omitempty" jsonschema:"version string, [a-z0-9.]+ (e.g. \"1.0.0\"); omit for \"0.0.1\", the first-build default"`
	// InitialMemory is the one ABI knob an explicit request may turn
	// (see tcInitialMemory): omitted = the frozen 262144 default.
	InitialMemory int64 `json:"initial_memory,omitempty" jsonschema:"the wasm linear memory's initial size in BYTES: omit for the ABI freeze default 262144 (4 x 64 KiB pages); an explicit value must be a whole multiple of the 65536-byte wasm page and at most the frozen 1048576 (1 MiB) ceiling"`
	// FW is an EXPLICIT pre-check, not a gate the interp track needs:
	// wasm bytecode is version-less and loads on any fw with a wasm
	// loader, so omitted builds with no check; fwGateWasm judges what
	// is given (floor 0.2.0).
	FW string `json:"fw,omitempty" jsonschema:"an EXPLICIT fw-compat pre-check, not a gate the interp track needs (wasm bytecode is version-less): supply the target board's fw version to refuse early below the 0.2.0 floor — the first fw with a wasm loader; omit to build with no check"`
	// WamrcPath / WasiSDKPath are the §6.4 developer overrides:
	// explicit, deliberately outside the toolchain cache's pairing
	// evidence — and EITHER one marks the manifest entry kind
	// "wasm-dev" (the artifact is visibly not an index-paired build,
	// however the unpaired compile was requested). WamrcPath IS the
	// aot lane (since P3-T7 the lane really execs wamrc — the frozen
	// recipe from build.sh — and lands a .aot for fw 0.3.0+);
	// WasiSDKPath compiles with a local wasi-sdk tree (bin/clang
	// inside it) instead of the cache's landed record.
	WamrcPath   string `json:"wamrc_path,omitempty" jsonschema:"the §6.4 developer override and the aot lane's knob: given, the freshly-built .wasm compiles one step further with this wamrc (frozen recipe: --target=xtensa --cpu=esp32s3 --size-level=3) and the artifact lands as drivers/<name>@<version>.aot, manifest kind \"wasm-dev\" — the toolchain-v2 stock wamrc (xuansi_toolchain_fetch) produces the .aot fw 0.3.0+ loads from internal SRAM (caps aot_exec_budget_bytes ceiling)"`
	WasiSDKPath string `json:"wasi_sdk_path,omitempty" jsonschema:"the §6.4 developer override: compile with this local wasi-sdk tree (its bin/clang, never a PATH lookup) instead of the toolchain cache's landed record — explicitly bypassing the index pairing evidence, and marks the manifest entry kind \"wasm-dev\""`
}

// wasmBuildOut is the build's answer: what landed, where, and the one
// next step.
type wasmBuildOut struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	File    string `json:"file" jsonschema:"the landed artifact's workspace-relative path (drivers/<name>@<version>.wasm, or .aot on the wamrc_path aot lane) — same presentation xuansi_ws_list/xuansi_driver_fetch use"`
	Sha256  string `json:"sha256" jsonschema:"sha256 of the compiled artifact (.wasm, or the .aot the wamrc lane produced) — the value the manifest's Files entry records"`
	Bytes   int    `json:"bytes" jsonschema:"compiled artifact bytes (.wasm, or .aot on the wamrc lane)"`
	// Warnings is the compiler transcripts verbatim — clang's
	// stdout+stderr, plus wamrc's on the aot lane (empty transcript =
	// absent). On a failed compile it is LOST — the refusal carries
	// the stderr tail instead, and nothing lands.
	Warnings string `json:"warnings,omitempty" jsonschema:"compiler stdout+stderr verbatim — clang's, plus wamrc's on the wamrc_path aot lane; absent when every compile was silent"`
	Next     string `json:"next" jsonschema:"the fixed next step: deploy the artifact, then exercise it"`
}

// wasmBuild is the xuansi_wasm_build handler — the toolchain face's
// build verb and the workspace's third storing path (ws_put stores
// Lua text, driver_fetch a prebuilt index artifact, this compiles C
// source into a fresh .wasm on this host — or, on the wamrc_path aot
// lane since P3-T7, the .aot the §6.4 wamrc compiles that .wasm
// into). Pipeline: input gates (name/version/source size/the one ABI
// knob, both its rules) → the split-track gates (§6.2) →
// wasmBuildInner (sdk resolution + the compile) → the landing
// (writeDurableFile + one manifest cycle, driverFetch's commit
// pattern under wsMu). No board, no lease — the compile is
// host-side, storing is not activating.
func (ts *toolset) wasmBuild(ctx context.Context, _ *mcpsdk.CallToolRequest, in wasmBuildIn) (*mcpsdk.CallToolResult, wasmBuildOut, error) {
	// input shape: the same name/version whitelist every workspace
	// path rides (ws_put's gates verbatim), the version default first
	// so the default lands in the manifest, never the raw "".
	if !wsNameRe.MatchString(in.Name) {
		return nil, wasmBuildOut{}, errWs("xuansi_wasm_build: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	ver := in.Version
	if ver == "" {
		ver = wasmBuildFirstVersion
	}
	if !wsVerRe.MatchString(ver) {
		return nil, wasmBuildOut{}, errWs("xuansi_wasm_build: version must match [a-z0-9.]+, got %q", in.Version)
	}
	if len(in.Source) > wasmBuildSourceMax {
		return nil, wasmBuildOut{}, errWs("xuansi_wasm_build: source is %d bytes, over the %d-byte (1 MiB) cap — split the module, or fetch a prebuilt artifact with xuansi_driver_fetch", len(in.Source), wasmBuildSourceMax)
	}
	// the one ABI knob gates at the TOOL edge with both rules clean
	// (T4 review note): a whole 64 KiB page multiple within the frozen
	// 1 MiB ceiling. The inner re-gates the multiple — harmless
	// repetition; this side's refusals name the contract before any
	// filesystem or exec work (wasm-ld would refuse the lawless
	// shapes later, less clearly).
	if _, err := tcInitialMemory(in.InitialMemory); err != nil {
		return nil, wasmBuildOut{}, err
	}
	if in.InitialMemory > tcWasmMaxMemory {
		return nil, wasmBuildOut{}, errWs("xuansi_wasm_build: initial_memory %d is over the frozen 1 MiB ceiling (the module-ABI freeze caps it at 1048576, the -Wl,--max-memory every build rides) — ask for at most 1048576", in.InitialMemory)
	}

	// the split-track gates (§6.2): the aot half rides the wamrc_path
	// lane (P3-T7: WantAOT = the §6.4 override in play; the aot
	// track's EARLY fw floor is 0.3.0 and the lane execs wamrc after
	// the compile); the wasm half judges an EXPLICIT fw only — its
	// 0.2.0 floor is dominated by the aot floor on the aot lane, so
	// the gate order is safe.
	wantAOT := in.WamrcPath != ""
	r := buildReq{Name: in.Name, Source: in.Source, Version: ver, InitialMemory: in.InitialMemory, FW: in.FW, WamrcPath: in.WamrcPath, WasiSDKPath: in.WasiSDKPath, WantAOT: wantAOT}
	if err := aotGate(r); err != nil {
		return nil, wasmBuildOut{}, err
	}
	if err := fwGateWasm(in.FW); err != nil {
		return nil, wasmBuildOut{}, err
	}
	wasm, warnings, err := ts.wasmBuildInner(ctx, r)
	if err != nil {
		return nil, wasmBuildOut{}, err
	}

	// the aot lane (P3-T7): the freshly-built .wasm compiles one step
	// further with the §6.4 wamrc — the artifact that LANDS is the
	// .aot, wamrc's transcript joining clang's in warnings (a wamrc
	// failure refuses carrying its stderr tail, and nothing lands).
	ext := "wasm"
	if wantAOT {
		var aot []byte
		var wwarn string
		aot, wwarn, err = wamrcExec(ctx, r, wasm)
		if err != nil {
			return nil, wasmBuildOut{}, err
		}
		warnings += wwarn
		wasm = aot
		ext = "aot"
	}

	// the landing: drivers/<name>@<ver>.wasm + one manifest cycle, the
	// whole commit phase under wsMu like driverFetch's (file and
	// manifest land as one unit against concurrent push/pull cycles;
	// the compile itself stayed outside the lock — it is the slow
	// half and touches only its temp dir).
	kind := "wasm"
	if in.WamrcPath != "" || in.WasiSDKPath != "" {
		// the §6.4 override lane, EITHER override (spec's symmetric
		// pair): the artifact was compiled outside the cache state's
		// pairing evidence, and the manifest says so — visibly not an
		// index-paired build.
		kind = "wasm-dev"
	}
	sha := sha256hex(wasm)
	wsMu.Lock()
	defer wsMu.Unlock()
	path := ts.ws.driverPath(in.Name, ver, ext)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, wasmBuildOut{}, errWs("xuansi_wasm_build: %v", err)
	}
	if err := writeDurableFile(path, wasm, 0o644); err != nil {
		return nil, wasmBuildOut{}, errWs("xuansi_wasm_build: %v", err)
	}
	m, err := loadManifest(ts.ws.manifestPath())
	if err != nil {
		return nil, wasmBuildOut{}, errWs("xuansi_wasm_build: %v", err)
	}
	e := m.entry(in.Name)
	e.Kind = kind
	fm := e.Files[ver] // push footprints (Boards) survive a re-build, like a re-put/re-fetch
	fm.Sha256, fm.Size = sha, len(wasm)
	// built-at: for put/fetch storing is NOT pushing and the field
	// stays zero; a build is the artifact's birth, and this face owns
	// that timestamp — the disk values win on a re-build, the same
	// single-source discipline driverPush applies to sha/size.
	fm.PushedAt = time.Now().UTC()
	e.Files[ver] = fm // Active deliberately untouched
	m.Drivers[in.Name] = e
	if err := writeManifestAtomic(ts.ws.manifestPath(), m); err != nil {
		return nil, wasmBuildOut{}, errWs("xuansi_wasm_build: %v", err)
	}
	return nil, wasmBuildOut{
		Name:     in.Name,
		Version:  ver,
		File:     "drivers/" + in.Name + "@" + ver + "." + ext,
		Sha256:   sha,
		Bytes:    len(wasm),
		Warnings: warnings,
		Next:     wasmBuildNext,
	}, nil
}
