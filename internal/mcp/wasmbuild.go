package mcp

// wasmbuild.go — the wasm build face's INTERNAL core (B11 task 4,
// design §6.2/§6.3/§6.4). Task 5's xuansi_wasm_build handler wraps
// exactly these pieces; nothing here registers a tool.
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

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
// (T5's manifest note marks the artifact dev) and derives the
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
	// source, and the output. Fixed names — r.Name is T5's to gate,
	// and an unvalidated name never reaches a path here.
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
	outPath := filepath.Join(tmp, "module.wasm")

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
