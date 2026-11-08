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
//     The aot domain is EMPTY in P1's toolchain-v2 — .aot is refused
//     for every fw, the honest S3 ruling, with wamrc_path as the
//     §6.4 developer escape; the wasm track's domain is the WIDE one
//     (bytecode is version-less), floored at the first fw with a
//     wasm loader (0.2.0).
//   - wasmBuildInner: resolve the wasi-sdk (wasi_sdk_path override >
//     the cache state's landed record > the self-guiding fetch
//     refusal), expand the embedded xuansi_host.h into a temp include
//     dir, exec <sdk>/bin/clang, and return the .wasm bytes with
//     clang's stdout/stderr verbatim as warnings.
//   - wamrcExec: a stub — wamrc execution lands with A1/A3 runtime
//     work; the aot lane's override half dies there, explicitly.
//   - wasmBuild (T5): the tool face — input gates, the gates above,
//     the compile, then drivers/<name>@<ver>.wasm + one manifest
//     cycle (kind "wasm", or "wasm-dev" when ANY §6.4 override —
//     wamrc_path or wasi_sdk_path — is in play).

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

// aotGate is the split-track pairing gate's aot half (§6.2, spec
// wording verbatim): P1's toolchain-v2 carries an EMPTY aot domain —
// the current fw stack cannot load .aot at all (the S3 deficiency) —
// so the build gate refuses .aot for every fw, always, with the
// honest why and the one escape: wamrc_path, the §6.4 developer
// experimental lane (the gate then passes, and wamrcExec's stub
// answers the rest). A1 opens the index's aot domain; A3 the v3
// generation — neither is this daemon's today.
func aotGate(r buildReq) error {
	if r.WantAOT && r.WamrcPath == "" {
		return errWs("xuansi_wasm_build: toolchain-v2 aot domain empty (fw_compat.aot=null) — .aot needs the A1/A3 runtime; experimental: pass wamrc_path")
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

// wamrcExec is the .aot compile's exec seam. NOT in P1: wamrc
// execution lands with the A1/A3 runtime work (§6.2/§6.4). The stub
// keeps the override lane honest — aotGate PASSES an explicit
// wamrc_path (the §6.4 experimental override), and the first .aot
// build then dies HERE with the explicit not-yet refusal instead of
// silently shipping a .wasm where .aot was asked.
// TODO(T5+): the real wamrc exec + the aot artifact's own shape gate.
func wamrcExec(_ context.Context, _ buildReq, _ string) (aotBytes []byte, warnings string, err error) {
	return nil, "", errWs("xuansi_wasm_build: wamrc exec lands with A1 — .aot compilation is not in this daemon yet (the gate passed because wamrc_path was given; the runtime that would load .aot arrives with A1/A3)")
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
			return nil, "", errWs("xuansi_wasm_build: no wasi-sdk installed in the toolchain cache — call xuansi_toolchain_fetch (recommended), or pass wasi_sdk_path to point at a local tree")
		}
		sdkRoot = filepath.Join(root, rec.Dir)
	}

	// the compiler itself: an absolute path inside the resolved tree,
	// never a PATH lookup. The probe gives the broken-tree case (an
	// override at a tree with no bin/clang, or a cache tree that
	// lost its compiler) a refusal that names the missing binary —
	// M3's fetch-side shape gate, build-side twin.
	clang := filepath.Join(sdkRoot, "bin", "clang")
	if _, serr := os.Stat(clang); serr != nil {
		return nil, "", errWs("xuansi_wasm_build: no clang at %s — the wasi-sdk tree is not usable (unexpected wasi-sdk layout); call xuansi_toolchain_fetch to reinstall, or point wasi_sdk_path at a tree with bin/clang", clang)
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

	// compile. stdout and stderr are captured separately (exec copies
	// them from two goroutines — one shared buffer would race) and
	// surface VERBATIM, concatenated, as warnings; a failed clang
	// repeats the stderr tail inside the refusal so the error itself
	// carries the diagnostic.
	args := append(tcBuildFlags(r, sdkRoot, incDir), "-o", outPath, srcPath)
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, clang, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	runErr := cmd.Run()
	warnings = stdout.String() + stderr.String()
	if runErr != nil {
		return nil, warnings, errWs("xuansi_wasm_build: clang failed (%v): %s", runErr, tcWarnTail(stderr.String()))
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
	// however the unpaired compile was requested). WamrcPath is the
	// aot lane's knob (the .aot compile itself arrives with the A1
	// runtime); WasiSDKPath compiles with a local wasi-sdk tree
	// (bin/clang inside it) instead of the cache's landed record.
	WamrcPath   string `json:"wamrc_path,omitempty" jsonschema:"the §6.4 developer override: an explicit wamrc marks this a dev build — the manifest entry records kind \"wasm-dev\" instead of \"wasm\" (the .aot lane itself arrives with the A1 runtime)"`
	WasiSDKPath string `json:"wasi_sdk_path,omitempty" jsonschema:"the §6.4 developer override: compile with this local wasi-sdk tree (its bin/clang, never a PATH lookup) instead of the toolchain cache's landed record — explicitly bypassing the index pairing evidence, and marks the manifest entry kind \"wasm-dev\""`
}

// wasmBuildOut is the build's answer: what landed, where, and the one
// next step.
type wasmBuildOut struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	File    string `json:"file" jsonschema:"the landed artifact's workspace-relative path (drivers/<name>@<version>.wasm) — same presentation xuansi_ws_list/xuansi_driver_fetch use"`
	Sha256  string `json:"sha256" jsonschema:"sha256 of the compiled .wasm — the value the manifest's Files entry records"`
	Bytes   int    `json:"bytes" jsonschema:"compiled .wasm bytes"`
	// Warnings is clang's stdout+stderr verbatim (empty transcript =
	// absent). On a failed compile it is LOST — the refusal carries
	// the stderr tail instead, and nothing lands.
	Warnings string `json:"warnings,omitempty" jsonschema:"clang's stdout+stderr verbatim; absent when the compile was silent"`
	Next     string `json:"next" jsonschema:"the fixed next step: deploy the artifact, then exercise it"`
}

// wasmBuild is the xuansi_wasm_build handler — the toolchain face's
// build verb and the workspace's third storing path (ws_put stores
// Lua text, driver_fetch a prebuilt index artifact, this compiles C
// source into a fresh .wasm on this host). Pipeline: input gates
// (name/version/source size/the one ABI knob, both its rules) → the
// split-track gates (§6.2) → wasmBuildInner (sdk resolution + the
// compile) → the landing (writeDurableFile + one manifest cycle,
// driverFetch's commit pattern under wsMu). No board, no lease — the
// compile is host-side, storing is not activating.
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

	// the split-track gates (§6.2): the aot half can only fire the
	// day this face grows an aot knob (WantAOT is never set from tool
	// input today — the call keeps the pipeline whole for that day);
	// the wasm half judges an EXPLICIT fw only.
	r := buildReq{Name: in.Name, Source: in.Source, Version: ver, InitialMemory: in.InitialMemory, FW: in.FW, WamrcPath: in.WamrcPath, WasiSDKPath: in.WasiSDKPath}
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
	path := ts.ws.driverPath(in.Name, ver, "wasm")
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
		File:     "drivers/" + in.Name + "@" + ver + ".wasm",
		Sha256:   sha,
		Bytes:    len(wasm),
		Warnings: warnings,
		Next:     wasmBuildNext,
	}, nil
}
