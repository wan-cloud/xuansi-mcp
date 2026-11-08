package mcp

// wasmbuild_test.go — B11 task 4: the wasm build face's internal
// core. The frozen flag block pinned flag-by-flag against the
// module-ABI §8 list (build.sh's twin — T9's CI cross-leg compiles
// the drivers modules through tcBuildFlags and hash-compares, so a
// drift either way is a red build); the split-track gates (aot
// domain empty / wasm fw floor) per design §6.2; and the sdk
// resolution order (wasi_sdk_path override > cache state > the
// self-guiding fetch refusal). The REAL compile (TestWasmBuildReal)
// is a smoke that only runs when a real wasi-sdk is on disk — the
// byte-level drivers cross-check is T9's CI leg.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestTCBuildFlagsFrozen pins the module-ABI §8 flag block exactly:
// every flag verbatim, in the frozen order, sysroot derived from the
// sdk root, initial-memory defaulted (262144) or carried verbatim —
// plus the one knob's rule: an explicit value must be a whole
// multiple of the 65536-byte wasm page or the build refuses api-class
// (wasm-ld demands the multiple; the refusal here is cheaper and
// clearer than the linker's).
func TestTCBuildFlagsFrozen(t *testing.T) {
	got := tcBuildFlags(buildReq{}, "/sdk/root", "/inc")
	want := []string{
		"--target=wasm32-wasi",
		"--sysroot=" + filepath.Join("/sdk/root", "share", "wasi-sysroot"),
		"-O2",
		"-fno-jump-tables",
		"-nostdlib",
		"-Wl,--no-entry",
		"-Wl,--allow-undefined",
		"-Wl,--max-memory=1048576",
		"-Wl,--global-base=1024",
		"-Wl,--strip-debug",
		"-Wl,--export-dynamic",
		"-Wl,--initial-memory=262144", // <=0 reads as the ABI freeze default (4 pages)
		"-I", "/inc",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("frozen flags:\n got  %v\n want %v", got, want)
	}

	// an explicit page-multiple memory rides verbatim (ledwatch's
	// 11-page 720896 is the build.sh precedent)
	got = tcBuildFlags(buildReq{InitialMemory: 720896}, "/s", "/i")
	found := false
	for _, f := range got {
		if f == "-Wl,--initial-memory=720896" {
			found = true
		}
	}
	if !found {
		t.Fatalf("explicit initial-memory: %v does not carry -Wl,--initial-memory=720896", got)
	}

	// the resolver: <=0 = default; multiples pass; a non-multiple is
	// an api-class refusal naming the page size.
	if m, err := tcInitialMemory(0); err != nil || m != 262144 {
		t.Errorf("tcInitialMemory(0) = %d, %v; want the 262144 default, nil", m, err)
	}
	if m, err := tcInitialMemory(-5); err != nil || m != 262144 {
		t.Errorf("tcInitialMemory(-5) = %d, %v; want the 262144 default, nil", m, err)
	}
	for _, ok := range []int64{65536, 262144, 720896, 1048576} {
		if m, err := tcInitialMemory(ok); err != nil || m != ok {
			t.Errorf("tcInitialMemory(%d) = %d, %v; want it back, nil", ok, m, err)
		}
	}
	_, err := tcInitialMemory(65537)
	tcAPIErr(t, err, "65536") // asserts api class + the page size named
}

// TestAOTGateRejectsV2 pins the P1 v2 ruling (design §6.2): the aot
// domain is EMPTY — .aot is refused for EVERY fw (the current stack
// cannot load it; the domain opens with A1/A3), the message says the
// honest why, and the ONLY pass is the §6.4 developer override
// (wamrc_path) — which the gate passes to wamrcExec's not-yet stub.
func TestAOTGateRejectsV2(t *testing.T) {
	for _, fw := range []string{"", "0.1.0", "0.2.0", "9.9.9"} {
		err := aotGate(buildReq{WantAOT: true, FW: fw})
		tcAPIErr(t, err, "aot domain empty")
		if !strings.Contains(err.Error(), "wamrc_path") {
			t.Errorf("refusal %v: want the wamrc_path experimental escape named", err)
		}
	}
	// the override passes the gate (aot dies later, in wamrcExec's stub)
	if err := aotGate(buildReq{WantAOT: true, WamrcPath: "/opt/wamrc"}); err != nil {
		t.Fatalf("wamrc_path override: want the gate to pass, got %v", err)
	}
	// wasm-only builds never see the aot gate
	if err := aotGate(buildReq{}); err != nil {
		t.Fatalf("wasm-only: want nil, got %v", err)
	}
	// and the stub the override lane lands in says not-yet, explicitly
	_, _, err := wamrcExec(context.Background(), buildReq{WantAOT: true, WamrcPath: "/opt/wamrc"}, "/opt/wamrc")
	tcAPIErr(t, err, "wamrc exec lands with A1")
}

// TestFWGateWasm pins the wasm track's fw floor (design §6.2): the
// WIDE domain — bytecode is version-less — floored at the first fw
// with a wasm loader. Omitted fw passes (this gate only judges what
// it is given); below 0.2.0 refuses api-class; 0.2.0 and up pass,
// with a pre-release suffix (0.2.0-rc1) reading as its numeric
// segments — lenient on purpose, noted in fwGateWasm's doc.
func TestFWGateWasm(t *testing.T) {
	for _, c := range []struct {
		fw      string
		wantErr bool
	}{
		{"", false},
		{"0.1.0", true},
		{"0.1.9", true},
		{"0.2.0", false},
		{"0.2.0-rc1", false},
		{"0.2.1", false},
		{"0.2", false}, // missing segment reads as 0
		{"0.3.1", false},
		{"1.0.0", false},
	} {
		err := fwGateWasm(c.fw)
		if c.wantErr {
			tcAPIErr(t, err, "0.2.0")
		} else if err != nil {
			t.Errorf("fwGateWasm(%q): want pass, got %v", c.fw, err)
		}
	}
}

// TestWasmBuildInnerMissingToolchain (brief-named): no wasi_sdk_path
// override and nothing usable in the cache → the self-guiding
// refusal (§6.3 UX) naming xuansi_toolchain_fetch. All three nothing
// shapes: a fresh cache (no state), a record whose tree vanished,
// and an override pointing at a tree with no bin/clang.
func TestWasmBuildInnerMissingToolchain(t *testing.T) {
	ts := &toolset{}
	src := "int main(void){return 0;}\n"

	// fresh cache: no state file at all
	cache := tcCacheEnv(t)
	_, _, err := ts.wasmBuildInner(context.Background(), buildReq{Source: src})
	tcAPIErr(t, err, "call xuansi_toolchain_fetch")

	// state records a wasi_sdk whose tree vanished: same guidance
	if err := saveTCState(filepath.Join(cache, toolchainStateFile), &tcState{
		Generation: "toolchain-v2",
		Installed: map[string]tcInstalled{
			"wasi_sdk": {SHA256: strings.Repeat("a", 64), Bytes: 1, Dir: "wasi-sdk-24.0-linux-x64"},
		},
	}); err != nil {
		t.Fatalf("plant vanished-tree state: %v", err)
	}
	_, _, err = ts.wasmBuildInner(context.Background(), buildReq{Source: src})
	tcAPIErr(t, err, "call xuansi_toolchain_fetch")

	// override at a tree without bin/clang: the layout refusal names
	// the actual clang path (M3's fetch-side shape gate, build-side twin)
	_, _, err = ts.wasmBuildInner(context.Background(), buildReq{Source: src, WasiSDKPath: t.TempDir()})
	tcAPIErr(t, err, "no clang at")
}

// TestWasmBuildInnerFakeClang: the whole inner pipeline against a
// FAKE sdk — bin/clang is a shell script that ignores its flags and
// writes fixed .wasm-magic bytes to the -o target, plus a line on
// stderr. Pins: the exec path and argv shape (the fake asserts the
// frozen flags arrive), stdout/stderr surfacing verbatim as warnings,
// and the output bytes returned (T5's fake-clang fixture, inner half).
func TestWasmBuildInnerFakeClang(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("/bin/sh absent: the fake clang is a shell script")
	}
	sdk := t.TempDir()
	if err := os.MkdirAll(filepath.Join(sdk, "bin"), 0o755); err != nil {
		t.Fatalf("fake sdk bin: %v", err)
	}
	// the fake: write wasm magic to the -o target, say one warning
	// line on stderr. It deliberately checks the arriving argv — a
	// frozen-flag drift turns this test red.
	script := `#!/bin/sh
out=""
prev=""
for a in "$@"; do
  case "$prev" in
    -I) prev="$a"; continue ;;
    -o) out="$a"; prev="$a"; continue ;;
  esac
  case "$a" in
    --target=wasm32-wasi|--sysroot=*|-O2|-fno-jump-tables|-nostdlib) ;;
    -Wl,--no-entry|-Wl,--allow-undefined|-Wl,--max-memory=1048576) ;;
    -Wl,--global-base=1024|-Wl,--strip-debug|-Wl,--export-dynamic) ;;
    -Wl,--initial-memory=262144|-I|-o|*.c) ;;
    *) echo "unexpected flag: $a" >&2; exit 2 ;;
  esac
  prev="$a"
done
printf '\000\141\163\155\000\000' > "$out"
echo "fake clang: 1 warning generated" >&2
`
	if err := os.WriteFile(filepath.Join(sdk, "bin", "clang"), []byte(script), 0o755); err != nil {
		t.Fatalf("plant fake clang: %v", err)
	}

	ts := &toolset{}
	wasm, warnings, err := ts.wasmBuildInner(context.Background(), buildReq{Source: "int x;\n", WasiSDKPath: sdk})
	if err != nil {
		t.Fatalf("fake build: %v", err)
	}
	if !bytes.HasPrefix(wasm, wasmMagic[:]) {
		t.Fatalf("wasm bytes % x: want the fake's \\0asm magic prefix", wasm)
	}
	if warnings != "fake clang: 1 warning generated\n" {
		t.Fatalf("warnings = %q: want the fake's stderr verbatim", warnings)
	}

	// failing clang: non-zero exit → api refusal carrying the stderr
	// tail; the transcript still rides warnings verbatim.
	if err := os.WriteFile(filepath.Join(sdk, "bin", "clang"), []byte("#!/bin/sh\necho 'src.c:3:1: error: boom' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("plant failing clang: %v", err)
	}
	_, warnings, err = ts.wasmBuildInner(context.Background(), buildReq{Source: "int x;\n", WasiSDKPath: sdk})
	tcAPIErr(t, err, "error: boom")
	if warnings != "src.c:3:1: error: boom\n" {
		t.Fatalf("failing warnings = %q: want the stderr verbatim", warnings)
	}

	// the initial-memory gate fires BEFORE any exec (a non-multiple
	// never reaches the compiler)
	if err := os.WriteFile(filepath.Join(sdk, "bin", "clang"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("plant no-op clang: %v", err)
	}
	_, _, err = ts.wasmBuildInner(context.Background(), buildReq{Source: "int x;\n", WasiSDKPath: sdk, InitialMemory: 65537})
	tcAPIErr(t, err, "65536")
}

// wasmRealSDKRoot finds a REAL wasi-sdk for the compile smoke: the
// cache state's landed record first (the product path), else any
// wasi-sdk-* tree under the cache root (the build.sh layout this dev
// box carries). "" = none — TestWasmBuildReal skips.
func wasmRealSDKRoot() string {
	root := tcCacheRoot()
	if root == "" {
		return ""
	}
	if st, err := loadTCState(filepath.Join(root, toolchainStateFile)); err == nil {
		if rec, ok := st.Installed["wasi_sdk"]; ok && rec.Dir != "" {
			cand := filepath.Join(root, rec.Dir)
			if tcPathExists(filepath.Join(cand, "bin", "clang")) {
				return cand
			}
		}
	}
	matches, _ := filepath.Glob(filepath.Join(root, "wasi-sdk-*"))
	for _, cand := range matches {
		if tcPathExists(filepath.Join(cand, "bin", "clang")) {
			return cand
		}
	}
	return ""
}

// TestWasmBuildReal compiles a real module through the real local
// wasi-sdk's clang — the daemon-side real-run smoke. Skipped unless
// that sdk is actually on disk (XUANSI_WASM_CACHE pointing at a
// landed cache, or a build.sh-laid ~/.cache/xuansi-wasm) and not in
// -short. The byte-level cross-check against the drivers repo's own
// builds (hash vs index.json) is T9's CI leg.
func TestWasmBuildReal(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode: the real clang compile is skipped")
	}
	sdkRoot := wasmRealSDKRoot()
	if sdkRoot == "" {
		t.Skip("no real wasi-sdk in the toolchain cache — land one with xuansi_toolchain_fetch (T9 CI runs this leg)")
	}
	ts := &toolset{}
	src := `#include <stdint.h>
#include "xuansi_host.h"

static uint32_t boot_ms;
void setup(void) { xuansi_millis(&boot_ms); }
`
	wasm, warnings, err := ts.wasmBuildInner(context.Background(), buildReq{Source: src, WasiSDKPath: sdkRoot})
	if err != nil {
		t.Fatalf("real build (sdk %s): %v (warnings: %s)", sdkRoot, err, warnings)
	}
	if !bytes.HasPrefix(wasm, wasmMagic[:]) {
		t.Fatalf("real wasm bytes % x: want the \\0asm magic prefix", wasm[:8])
	}
	if len(wasm) < 8 {
		t.Fatalf("real wasm: %d bytes is implausibly small", len(wasm))
	}
	t.Logf("real build: %d bytes, warnings %q", len(wasm), warnings)
}
