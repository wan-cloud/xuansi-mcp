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
	"time"
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

// TestAOTGateAOTFloor (P3-T7's gate, P3-T12's rewrite): the aot
// track's EARLY fw pairing refusal — an explicit fw below 0.3.0 (the
// floor, fw_compat.aot's twin in the index) refuses at BUILD time
// naming the floor, on BOTH lane knobs (aot=true and wamrc_path);
// at/above the floor and an omitted fw the lane passes; wasm-only
// builds never see the gate. The wamrc RESOLUTION question moved to
// resolveAOTWamrc (TestResolveAOTWamrc below) — the gate itself no
// longer knows how the lane's wamrc arrives.
func TestAOTGateAOTFloor(t *testing.T) {
	// explicit fw below the aot floor: refused EARLY, api-class, the
	// floor and the escape named — for every below-floor spelling,
	// on both knobs.
	for _, fw := range []string{"0.1.0", "0.2.0", "0.2.9", "0.2"} {
		for _, r := range []buildReq{
			{WantAOT: true, WamrcPath: "/opt/wamrc", FW: fw},
			{WantAOT: true, FW: fw}, // the default lane (aot=true): same floor
		} {
			err := aotGate(r)
			tcAPIErr(t, err, "0.3.0")
		}
	}
	// at/above the floor, and an omitted fw (this gate only judges
	// what it is given — fwGateWasm's aot-lane twin), the lane passes
	// with and without an explicit wamrc.
	for _, fw := range []string{"", "0.3.0", "0.3.1", "0.3.0-rc1", "9.9.9"} {
		for _, r := range []buildReq{
			{WantAOT: true, WamrcPath: "/opt/wamrc", FW: fw},
			{WantAOT: true, FW: fw},
		} {
			if err := aotGate(r); err != nil {
				t.Errorf("aotGate(%+v): want the lane open, got %v", r, err)
			}
		}
	}
	// wasm-only builds never see the aot gate
	if err := aotGate(buildReq{FW: "0.1.0"}); err != nil {
		t.Fatalf("wasm-only: want nil, got %v", err)
	}
}

// plantFakeWamrcCache lands a fake wamrc as the toolchain cache's OWN
// record — the aot default lane's PRODUCT path shape: XUANSI_WASM_CACHE
// points at a fresh cache whose state records the given generation and
// a wamrc whose Dir exists, so aot=true resolves the fake through the
// pairing evidence. Returns the cache root.
func plantFakeWamrcCache(t *testing.T, generation string) string {
	t.Helper()
	cache := tcCacheEnv(t)
	if err := os.MkdirAll(filepath.Join(cache, "wamrc"), 0o755); err != nil {
		t.Fatalf("plant wamrc dir: %v", err)
	}
	os.WriteFile(filepath.Join(cache, "wamrc", "wamrc"), []byte("#!/bin/sh\n"), 0o755)
	if err := saveTCState(filepath.Join(cache, toolchainStateFile), &tcState{
		Generation: generation,
		Installed: map[string]tcInstalled{
			"wamrc": {SHA256: strings.Repeat("b", 64), Bytes: 4096, Dir: "wamrc/wamrc"},
		},
	}); err != nil {
		t.Fatalf("plant wamrc state: %v", err)
	}
	return cache
}

// TestResolveAOTWamrc (P3-T12): the aot lane's DEFAULT wamrc
// resolution — the cache-paired half. The state's generation label is
// the pairing evidence: toolchain-v3 resolves the recorded Dir; any
// other label (v2's stock wamrc produces .aot that fw 0.4.0+ refuses
// to load — the internal-SRAM lane is retired; no silent
// substitution), a fresh cache, or a vanished wamrc tree refuses
// api-class naming fetch and the wamrc_path override.
func TestResolveAOTWamrc(t *testing.T) {
	ts := &toolset{}

	// fresh cache: nothing installed — the self-guiding refusal
	tcCacheEnv(t)
	_, err := ts.resolveAOTWamrc()
	tcAPIErr(t, err, "xuansi_toolchain_fetch")
	if !strings.Contains(err.Error(), aotDefaultGen) {
		t.Errorf("refusal %v: want the default generation %s named", err, aotDefaultGen)
	}

	// a v2 cache: honest refusal naming BOTH generations — the stock
	// wamrc is not silently substituted for the surgery default.
	plantFakeWamrcCache(t, "toolchain-v2")
	_, err = ts.resolveAOTWamrc()
	tcAPIErr(t, err, "toolchain-v2")
	if !strings.Contains(err.Error(), aotDefaultGen) || !strings.Contains(err.Error(), "wamrc_path") {
		t.Errorf("refusal %v: want %s and the wamrc_path override both named", err, aotDefaultGen)
	}

	// the v3 cache: the recorded Dir resolves to a real path.
	cache := plantFakeWamrcCache(t, aotDefaultGen)
	p, err := ts.resolveAOTWamrc()
	if err != nil {
		t.Fatalf("resolveAOTWamrc: %v", err)
	}
	if want := filepath.Join(cache, "wamrc", "wamrc"); p != want {
		t.Fatalf("resolved %q: want the state's recorded Dir %q", p, want)
	}

	// a v3 cache whose wamrc tree vanished: reinstall guidance.
	if err := os.RemoveAll(filepath.Join(cache, "wamrc")); err != nil {
		t.Fatalf("remove wamrc tree: %v", err)
	}
	_, err = ts.resolveAOTWamrc()
	tcAPIErr(t, err, "xuansi_toolchain_fetch")
}

// fakeWamrc plants a shell-script wamrc and returns its path: it
// asserts the frozen recipe's argv shape (--target=xtensa
// --cpu=esp32s3 --size-level=3 — build.sh's invocation verbatim) and
// writes an \0aot-magic fake .aot to the -o target, one line on
// stderr — the aot lane's fake-clang twin (the argv pins live HERE;
// the tool tests only exercise the landing).
func fakeWamrc(t *testing.T) string {
	t.Helper()
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("/bin/sh absent: the fake wamrc is a shell script")
	}
	dir := t.TempDir()
	script := `#!/bin/sh
out=""
prev=""
seen=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then out="$a"; fi
  case "$a" in
    --target=xtensa|--cpu=esp32s3|--size-level=3) seen="$seen $a" ;;
    -o|*.wasm|*.aot) ;;
    *) echo "unexpected wamrc argv: $a" >&2; exit 2 ;;
  esac
  prev="$a"
done
if [ "$seen" != " --target=xtensa --cpu=esp32s3 --size-level=3" ]; then
  echo "frozen recipe flags missing, got:$seen" >&2; exit 2
fi
printf '\000aot\004\000\000\000fake-aot-body' > "$out"
echo "fake wamrc: compile ok" >&2
`
	p := filepath.Join(dir, "wamrc")
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatalf("plant fake wamrc: %v", err)
	}
	return p
}

// fakeAotBody is the fake wamrc's exact output: the \0aot magic, the
// v4 container version (u32 LE — the version AOT carries since
// forever, deliberately NOT bumped for P3), then the marker body.
func fakeAotBody() []byte {
	return append([]byte{0x00, 'a', 'o', 't', 0x04, 0x00, 0x00, 0x00}, []byte("fake-aot-body")...)
}

// TestWamrcExecFake (P3-T7): the real wamrc exec seam against the
// fake wamrc — the happy lane returns the .aot bytes with the
// transcript verbatim; a failing wamrc refuses carrying the stderr
// tail; a missing binary refuses naming the path; and a "successful"
// run whose output is not an \0aot container is refused by the shape
// gate (wasmMagic's aot twin — wamrc exit 0 with garbage must never
// land as an artifact).
func TestWamrcExecFake(t *testing.T) {
	wamrc := fakeWamrc(t)
	r := buildReq{Name: "hello", WamrcPath: wamrc}
	wasm := fakeWasmBody()

	aot, warnings, err := wamrcExec(context.Background(), r, wasm)
	if err != nil {
		t.Fatalf("wamrcExec: %v", err)
	}
	if want := fakeAotBody(); !bytes.Equal(aot, want) {
		t.Fatalf("aot bytes % x: want the fake's \\0aot output % x", aot, want)
	}
	if warnings != "fake wamrc: compile ok\n" {
		t.Fatalf("warnings = %q: want the fake's stderr verbatim", warnings)
	}

	// failing wamrc, STDERR shape: non-zero exit → api refusal carrying
	// the transcript tail; the transcript itself still rides warnings
	// verbatim.
	if err := os.WriteFile(wamrc, []byte("#!/bin/sh\necho 'wamrc: error: bad module' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("plant failing wamrc: %v", err)
	}
	_, warnings, err = wamrcExec(context.Background(), r, wasm)
	tcAPIErr(t, err, "wamrc failed")
	if !strings.Contains(err.Error(), "bad module") {
		t.Errorf("refusal %v: want wamrc's own transcript carried verbatim", err)
	}
	if warnings != "wamrc: error: bad module\n" {
		t.Fatalf("failing warnings = %q: want the stderr verbatim", warnings)
	}

	// failing wamrc, STDOUT shape (P3-T12, the T7-② rider): the REAL
	// wamrc prints its errors on STDOUT ("llvm get target from triple
	// ... failed" — the T7 seat-check finding), unlike clang. The
	// refusal's tail rides the COMBINED transcript, so the stdout
	// error must arrive exactly when it matters — an stderr-only tail
	// would be empty at this moment and swallow the diagnostic.
	if err := os.WriteFile(wamrc, []byte("#!/bin/sh\necho 'wamrc: error: llvm get target from triple failed'\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("plant stdout-failing wamrc: %v", err)
	}
	_, warnings, err = wamrcExec(context.Background(), r, wasm)
	tcAPIErr(t, err, "wamrc failed")
	if !strings.Contains(err.Error(), "llvm get target from triple failed") {
		t.Errorf("refusal %v: want wamrc's STDOUT error carried in the combined-transcript tail", err)
	}
	if warnings != "wamrc: error: llvm get target from triple failed\n" {
		t.Fatalf("stdout-failing warnings = %q: want the stdout verbatim", warnings)
	}

	// missing binary: the probe refusal names the path (the clang
	// twin's broken-tree shape).
	_, _, err = wamrcExec(context.Background(), buildReq{Name: "hello", WamrcPath: filepath.Join(t.TempDir(), "no-wamrc")}, wasm)
	tcAPIErr(t, err, "no wamrc at")

	// shape gate: exit 0 but the output is not an \0aot container —
	// refused (a .wasm-magic file would be wamrc refusing to convert
	// while still exiting 0, or a wrong binary entirely).
	if err := os.WriteFile(wamrc, []byte("#!/bin/sh\nout=\"\"; prev=\"\"\nfor a in \"$@\"; do\n  if [ \"$prev\" = \"-o\" ]; then out=\"$a\"; fi\n  prev=\"$a\"\ndone\nprintf '\\000asm-not-aot' > \"$out\"\n"), 0o755); err != nil {
		t.Fatalf("plant wrong-magic wamrc: %v", err)
	}
	_, _, err = wamrcExec(context.Background(), r, wasm)
	tcAPIErr(t, err, "not an .aot container")
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

// ---------------------------------------------------------------------------
// xuansi_wasm_build (B11 task 5) — the tool handler, handler-direct
// (the toolchain-test pattern: the build face crosses no daemon wire,
// so there is nothing to script — the workspace root is injected and
// the sdk resolution rides a fake clang fixture: as the wasi_sdk_path
// override target, as the cache state's landed record, or a fresh
// cache for the refusal shapes).

// plantFakeClang plants the fixture compiler under dir (bin/clang, a
// shell script that writes fixed .wasm-magic bytes to the -o target
// and one line on stderr) — the tool tests' compile stand-in (the
// argv-shape pins live in TestWasmBuildInnerFakeClang; here only the
// landing is under test).
func plantFakeClang(t *testing.T, dir string) {
	t.Helper()
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("/bin/sh absent: the fake clang is a shell script")
	}
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatalf("fake sdk bin: %v", err)
	}
	script := `#!/bin/sh
out=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then out="$a"; fi
  prev="$a"
done
printf '\000\141\163\155\000\000fake-module-body' > "$out"
echo "fake clang: ok" >&2
`
	if err := os.WriteFile(filepath.Join(dir, "bin", "clang"), []byte(script), 0o755); err != nil {
		t.Fatalf("plant fake clang: %v", err)
	}
}

// fakeWasmSDK plants the fixture in its own temp dir and returns the
// root — the wasi_sdk_path override target (the §6.4 lane: the inner
// execs <root>/bin/clang directly, no state/bin gate on this path).
func fakeWasmSDK(t *testing.T) string {
	t.Helper()
	sdk := t.TempDir()
	plantFakeClang(t, sdk)
	return sdk
}

// plantFakeSDKCache lands the fixture as the toolchain cache's OWN
// record — the PRODUCT path: XUANSI_WASM_CACHE points at a fresh
// cache whose state records a wasi_sdk whose Dir exists, so a build
// with NO override resolves the fake tree through the pairing
// evidence (kind stays "wasm").
func plantFakeSDKCache(t *testing.T) {
	t.Helper()
	cache := tcCacheEnv(t)
	dir := "wasi-sdk-24.0-fake"
	plantFakeClang(t, filepath.Join(cache, dir))
	if err := saveTCState(filepath.Join(cache, toolchainStateFile), &tcState{
		Generation: "toolchain-v2",
		Installed: map[string]tcInstalled{
			"wasi_sdk": {SHA256: strings.Repeat("a", 64), Bytes: 1, Dir: dir},
		},
	}); err != nil {
		t.Fatalf("plant landed sdk state: %v", err)
	}
}

// fakeWasmBody is the fake clang's exact output: the \0asm magic, two
// zero bytes, then the marker body — the bytes the landing must store
// byte-for-byte and the manifest's FileMeta must describe.
func fakeWasmBody() []byte {
	b := append([]byte{}, wasmMagic[:]...)
	b = append(b, 0x00, 0x00)
	return append(b, []byte("fake-module-body")...)
}

// wasmBuildRun runs the handler once under a test timeout, with the
// workspace root injected (ts.ws) — the landing target the test then
// inspects through the same wsState paths.
func wasmBuildRun(t *testing.T, ts *toolset, in wasmBuildIn) (wasmBuildOut, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, out, err := ts.wasmBuild(ctx, nil, in)
	return out, err
}

// TestWasmBuildToolLargeSource (brief-named): a source over the 1 MiB
// cap is an api refusal naming the cap, fired at the TOOL edge before
// any sdk resolution or landing — the workspace stays empty (the
// zero-write proof).
func TestWasmBuildToolLargeSource(t *testing.T) {
	ts := &toolset{ws: newWSState(t.TempDir())}
	tcCacheEnv(t) // fresh cache: the size gate must fire, never the tc guidance
	big := strings.Repeat("// padding padding\n", (1<<20)/18+2)
	if len(big) <= 1<<20 {
		t.Fatalf("fixture source is %d bytes, want over 1048576", len(big))
	}
	_, err := wasmBuildRun(t, ts, wasmBuildIn{Name: "hello", Source: big})
	tcAPIErr(t, err, "1048576") // the cap named in the refusal
	if !strings.Contains(err.Error(), "1 MiB") {
		t.Errorf("refusal %v: want the 1 MiB cap named in human units too", err)
	}
	if _, serr := os.Stat(ts.ws.manifestPath()); !os.IsNotExist(serr) {
		t.Errorf("over-cap source: manifest exists — nothing should have landed")
	}
}

// TestWasmBuildToolMissingTCGuidance (brief-named): no override, no
// cache → the self-guiding refusal names the fetch tool (§6.3 UX),
// through the tool handler this time (the inner's guidance, verbatim).
func TestWasmBuildToolMissingTCGuidance(t *testing.T) {
	tcCacheEnv(t)
	ts := &toolset{ws: newWSState(t.TempDir())}
	_, err := wasmBuildRun(t, ts, wasmBuildIn{Name: "hello", Source: "int x;\n"})
	tcAPIErr(t, err, "xuansi_toolchain_fetch")
}

// TestWasmBuildToolLandsWorkspace (brief-named): the whole tool path
// against the fake sdk landed as the toolchain cache's OWN record —
// the PRODUCT lane (fix round 1: no override input, the build
// resolves the tree through the pairing evidence, exactly what a
// user with a fetched toolchain rides) — the artifact lands at
// drivers/<name>@<ver>.wasm with the fake's exact bytes, the
// manifest entry records kind "wasm" plus the FileMeta (sha/size of
// the built bytes, built-at time, Active untouched), and every out
// field answers (File the workspace-relative path, Next the fixed
// push sentence, Warnings the transcript verbatim).
func TestWasmBuildToolLandsWorkspace(t *testing.T) {
	plantFakeSDKCache(t)
	root := t.TempDir()
	ts := &toolset{ws: newWSState(root)}
	out, err := wasmBuildRun(t, ts, wasmBuildIn{
		Name: "hello", Version: "1.2.0", Source: "int x;\n",
		InitialMemory: 131072, // an explicit 2-page value rides through the gates
		// no wamrc_path/wasi_sdk_path: the cache state's landed record
		// compiles, kind stays "wasm" — the index-paired lane
	})
	if err != nil {
		t.Fatalf("wasmBuild: %v", err)
	}
	body := fakeWasmBody()
	if out.Name != "hello" || out.Version != "1.2.0" {
		t.Fatalf("out identity = %+v: want hello@1.2.0", out)
	}
	if out.File != "drivers/hello@1.2.0.wasm" {
		t.Errorf("out.File = %q: want the workspace-relative landed path", out.File)
	}
	if out.Sha256 != sha256hex(body) || out.Bytes != len(body) {
		t.Errorf("out sha/bytes = %s/%d: want %s/%d (the compiled artifact's own)", out.Sha256, out.Bytes, sha256hex(body), len(body))
	}
	if out.Warnings != "fake clang: ok\n" {
		t.Errorf("out.Warnings = %q: want the transcript verbatim", out.Warnings)
	}
	if out.Next != wasmBuildNext {
		t.Errorf("out.Next = %q: want the fixed push sentence %q", out.Next, wasmBuildNext)
	}

	// the landed file: exact bytes at the driverPath the manifest names
	got, rerr := readFileShared(filepath.Join(root, "drivers", "hello@1.2.0.wasm"))
	if rerr != nil {
		t.Fatalf("landed artifact: %v", rerr)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("landed bytes % x: want the fake clang's output % x", got, body)
	}

	// the manifest entry: kind wasm, FileMeta of the built bytes,
	// built-at recorded, no activation, no push footprints
	m, merr := loadManifest(filepath.Join(root, "manifest.json"))
	if merr != nil {
		t.Fatalf("loadManifest: %v", merr)
	}
	e, ok := m.Drivers["hello"]
	if !ok {
		t.Fatal("manifest: no hello entry after the build")
	}
	if e.Kind != "wasm" {
		t.Errorf("kind = %q: want wasm (the cache state's landed record compiled — no override in play)", e.Kind)
	}
	if e.Active != "" {
		t.Errorf("active = %q: storing is NOT activating (a push moves it)", e.Active)
	}
	fm, ok := e.Files["1.2.0"]
	if !ok {
		t.Fatal("manifest: no files[1.2.0] entry after the build")
	}
	if fm.Sha256 != sha256hex(body) || fm.Size != len(body) {
		t.Errorf("files[1.2.0] = %+v: want sha %s size %d", fm, sha256hex(body), len(body))
	}
	if fm.PushedAt.IsZero() {
		t.Error("files[1.2.0].pushed_at: want the built-at timestamp recorded")
	}
	if len(fm.Boards) != 0 {
		t.Errorf("files[1.2.0].boards = %v: want none (a build pushes nowhere)", fm.Boards)
	}
}

// TestWasmBuildToolVersionGate (brief-named): the tool-edge input
// gates — name/version regex refusals (the omitted version's 0.0.1
// default lands in the dev test below) and the one ABI knob's frozen
// ceiling — all api-class, all BEFORE sdk resolution (a fresh cache
// never reaches the toolchain guidance on these shapes).
func TestWasmBuildToolVersionGate(t *testing.T) {
	tcCacheEnv(t)
	ts := &toolset{ws: newWSState(t.TempDir())}
	for _, c := range []struct {
		name, ver, want string
	}{
		{"Hello", "", "name must match"},   // uppercase outside [a-z0-9_-]+
		{"../evil", "", "name must match"}, // a path-shaped name never reaches one
		{"hello", "1.0-beta", "version must match"},
		{"hello", "V2", "version must match"},
		{"hello", "1.0/x", "version must match"},
	} {
		_, err := wasmBuildRun(t, ts, wasmBuildIn{Name: c.name, Version: c.ver, Source: "int x;\n"})
		tcAPIErr(t, err, c.want)
	}

	// the frozen 1 MiB memory ceiling (T4 review note): a page
	// MULTIPLE over the cap is the tool edge's own refusal naming the
	// legal max — wasm-ld would refuse it later; this says it before
	// any exec.
	_, err := wasmBuildRun(t, ts, wasmBuildIn{Name: "hello", Source: "int x;\n", InitialMemory: 1114112})
	tcAPIErr(t, err, "1048576")

	// and the non-multiple still refuses at the edge (the inner's
	// gate re-fired from the tool, message naming the page size)
	_, err = wasmBuildRun(t, ts, wasmBuildIn{Name: "hello", Source: "int x;\n", InitialMemory: 65537})
	tcAPIErr(t, err, "65536")
}

// TestWasmBuildToolDevOverrideKind (the added cases, fix round 1's
// ruling): ANY §6.4 override marks the manifest entry kind
// "wasm-dev" — the artifact is visibly not an index-paired build,
// however the unpaired compile was requested. Both halves: wamrc_path
// (fired over the cache state's OWN landed record — the mark rides
// the request's override, not the sdk the build resolved; since P3-T7
// this lane REALLY execs wamrc, so the landed artifact is the fake
// wamrc's .aot at drivers/<name>@<ver>.aot), and wasi_sdk_path alone
// (a local tree compiling with no cache in play). The omitted version
// lands the first-build default 0.0.1.
func TestWasmBuildToolDevOverrideKind(t *testing.T) {
	// half 1: wamrc_path over the PAIRED cache lane — the request's
	// override marks, even though the sdk resolved through the
	// pairing evidence. The aot lane: clang builds the .wasm, the
	// fake wamrc compiles it one step further, the .aot lands.
	plantFakeSDKCache(t)
	ts := &toolset{ws: newWSState(t.TempDir())}
	out, err := wasmBuildRun(t, ts, wasmBuildIn{
		Name: "hello", Source: "int x;\n",
		WamrcPath: fakeWamrc(t), // no Version: the 0.0.1 default
	})
	if err != nil {
		t.Fatalf("wasmBuild (wamrc dev): %v", err)
	}
	if out.Version != wasmBuildFirstVersion {
		t.Fatalf("out.Version = %q: want the %s first-build default", out.Version, wasmBuildFirstVersion)
	}
	if out.File != "drivers/hello@"+wasmBuildFirstVersion+".aot" {
		t.Errorf("out.File = %q: want the aot lane's .aot path", out.File)
	}
	aotBody := fakeAotBody()
	if out.Sha256 != sha256hex(aotBody) || out.Bytes != len(aotBody) {
		t.Errorf("out sha/bytes = %s/%d: want the compiled .aot's own %s/%d", out.Sha256, out.Bytes, sha256hex(aotBody), len(aotBody))
	}
	m, merr := loadManifest(ts.ws.manifestPath())
	if merr != nil {
		t.Fatalf("loadManifest: %v", merr)
	}
	e := m.Drivers["hello"]
	if e.Kind != "wasm-dev" {
		t.Fatalf("kind = %q: want wasm-dev (wamrc_path marks the override lane)", e.Kind)
	}
	fm, ok := e.Files[wasmBuildFirstVersion]
	if !ok || fm.Sha256 != out.Sha256 || fm.Size != out.Bytes {
		t.Fatalf("files[%s] = %+v (ok=%v): want the built artifact's meta matching out", wasmBuildFirstVersion, fm, ok)
	}
	// P3-T13: the aot lane records its target — the frozen recipe's
	// --cpu=esp32s3 is the manifest's aot.esp32s3 claim (a later .wasm
	// build of the same version pairs with it on a matching board)
	if slot, aok := fm.Aot["esp32s3"]; !aok || slot.Sha256 != out.Sha256 || slot.Size != out.Bytes {
		t.Fatalf("files[%s].Aot[esp32s3] = %+v (ok=%v): want the built .aot's own sha/size", wasmBuildFirstVersion, slot, aok)
	}

	// half 2: wasi_sdk_path ALONE (probe-22's symmetric pair) — a
	// local tree compiles with no cache record at all, and the
	// manifest must not pass the artifact off as index-paired.
	tcCacheEnv(t) // fresh cache: the override is the only resolution
	ts2 := &toolset{ws: newWSState(t.TempDir())}
	if _, err := wasmBuildRun(t, ts2, wasmBuildIn{
		Name: "hello", Version: "2.0.0", Source: "int x;\n",
		WasiSDKPath: fakeWasmSDK(t),
	}); err != nil {
		t.Fatalf("wasmBuild (sdk dev): %v", err)
	}
	m2, merr2 := loadManifest(ts2.ws.manifestPath())
	if merr2 != nil {
		t.Fatalf("loadManifest (sdk dev): %v", merr2)
	}
	if e2 := m2.Drivers["hello"]; e2.Kind != "wasm-dev" {
		t.Fatalf("kind = %q: want wasm-dev (wasi_sdk_path alone marks the override lane too)", e2.Kind)
	}
}

// TestWasmBuildToolAOTLaneFloor (P3-T7): the aot lane's EARLY fw
// pairing refusal through the tool handler — an explicit fw below the
// 0.3.0 floor refuses before any exec (a 0.2.x board has no aot
// loader; the build must not discover that at push time), while the
// same call at the floor proceeds into the lane.
func TestWasmBuildToolAOTLaneFloor(t *testing.T) {
	plantFakeSDKCache(t)
	ts := &toolset{ws: newWSState(t.TempDir())}
	wamrc := fakeWamrc(t)

	_, err := wasmBuildRun(t, ts, wasmBuildIn{
		Name: "hello", Source: "int x;\n", FW: "0.2.0", WamrcPath: wamrc,
	})
	tcAPIErr(t, err, "0.3.0")
	if !strings.Contains(err.Error(), "aot") {
		t.Errorf("refusal %v: want the aot track named", err)
	}
	// nothing landed (the manifest may exist from the plant, but no
	// hello entry — the refusal fired before any compile or landing).
	m, merr := loadManifest(ts.ws.manifestPath())
	if merr == nil {
		if _, ok := m.Drivers["hello"]; ok {
			t.Fatal("aot floor refusal: a hello entry landed anyway")
		}
	}

	// at the floor the lane proceeds: the .aot lands.
	out, err := wasmBuildRun(t, ts, wasmBuildIn{
		Name: "hello", Version: "1.0.0", Source: "int x;\n", FW: "0.3.0", WamrcPath: wamrc,
	})
	if err != nil {
		t.Fatalf("wasmBuild (aot lane at floor): %v", err)
	}
	if out.File != "drivers/hello@1.0.0.aot" {
		t.Errorf("out.File = %q: want the .aot landing", out.File)
	}
	// both transcripts ride: clang's then wamrc's, in lane order.
	if !strings.Contains(out.Warnings, "fake clang: ok") || !strings.Contains(out.Warnings, "fake wamrc: compile ok") {
		t.Errorf("out.Warnings = %q: want BOTH transcripts (clang then wamrc)", out.Warnings)
	}
}

// plantFakeV3ToolchainCache lands BOTH pieces as the toolchain
// cache's OWN records under generation toolchain-v3: the fake clang
// as the wasi_sdk record and the fake wamrc as the wamrc record — the
// aot DEFAULT lane's whole product path (aot=true resolves the wamrc
// through the same pairing evidence the .wasm compile's sdk rides).
func plantFakeV3ToolchainCache(t *testing.T) string {
	t.Helper()
	cache := tcCacheEnv(t)
	plantFakeClang(t, filepath.Join(cache, "wasi-sdk-24.0-fake"))
	if err := os.MkdirAll(filepath.Join(cache, "wamrc"), 0o755); err != nil {
		t.Fatalf("plant wamrc dir: %v", err)
	}
	if err := copyFakeWamrc(t, filepath.Join(cache, "wamrc", "wamrc")); err != nil {
		t.Fatalf("plant cache wamrc: %v", err)
	}
	if err := saveTCState(filepath.Join(cache, toolchainStateFile), &tcState{
		Generation: aotDefaultGen,
		Installed: map[string]tcInstalled{
			"wasi_sdk": {SHA256: strings.Repeat("a", 64), Bytes: 1, Dir: "wasi-sdk-24.0-fake"},
			"wamrc":    {SHA256: strings.Repeat("b", 64), Bytes: 4096, Dir: "wamrc/wamrc"},
		},
	}); err != nil {
		t.Fatalf("plant v3 state: %v", err)
	}
	return cache
}

// copyFakeWamrc plants fakeWamrc's script at path (fakeWamrc returns
// its own temp dir's path; the cache plant needs the script AT the
// record's Dir).
func copyFakeWamrc(t *testing.T, path string) error {
	t.Helper()
	b, err := os.ReadFile(fakeWamrc(t))
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o755)
}

// TestWasmBuildToolAOTDefaultLane (P3-T12): aot=true is the aot
// lane's DEFAULT knob — no wamrc_path. The whole product path rides
// the toolchain cache's pairing evidence: the fake clang compiles the
// .wasm, the cache's OWN wamrc record (the fake, resolved through
// resolveAOTWamrc) compiles the .aot, the artifact lands as
// drivers/<name>@<version>.aot with manifest kind "wasm" (the
// cache-paired build — NOT wasm-dev: no override is in play). A cache
// whose generation is not toolchain-v3 refuses BEFORE any compile.
func TestWasmBuildToolAOTDefaultLane(t *testing.T) {
	plantFakeV3ToolchainCache(t)
	ts := &toolset{ws: newWSState(t.TempDir())}

	out, err := wasmBuildRun(t, ts, wasmBuildIn{
		Name: "hello", Version: "1.0.0", Source: "int x;\n",
		Aot: true, // no wamrc_path: the cache-paired default lane
	})
	if err != nil {
		t.Fatalf("wasmBuild (aot default lane): %v", err)
	}
	if out.File != "drivers/hello@1.0.0.aot" {
		t.Fatalf("out.File = %q: want the aot lane's .aot landing", out.File)
	}
	aotBody := fakeAotBody()
	if out.Sha256 != sha256hex(aotBody) || out.Bytes != len(aotBody) {
		t.Fatalf("out sha/bytes = %s/%d: want the fake wamrc's .aot %s/%d", out.Sha256, out.Bytes, sha256hex(aotBody), len(aotBody))
	}
	if !strings.Contains(out.Warnings, "fake clang: ok") || !strings.Contains(out.Warnings, "fake wamrc: compile ok") {
		t.Fatalf("out.Warnings = %q: want BOTH transcripts (clang then the cache-resolved wamrc)", out.Warnings)
	}
	m, merr := loadManifest(ts.ws.manifestPath())
	if merr != nil {
		t.Fatalf("loadManifest: %v", merr)
	}
	e := m.Drivers["hello"]
	if e.Kind != "wasm" {
		t.Fatalf("kind = %q: want wasm (aot=true alone is the cache-paired lane; only the §6.4 overrides mark wasm-dev)", e.Kind)
	}
	if fm, ok := e.Files["1.0.0"]; !ok || fm.Sha256 != out.Sha256 {
		t.Fatalf("files[1.0.0] = %+v (ok=%v): want the built .aot's meta", fm, ok)
	}

	// the generation pairing refusal, through the tool handler: a v2
	// cache (the stock wamrc is NOT silently substituted) refuses
	// BEFORE any compile — nothing lands.
	plantFakeWamrcCache(t, "toolchain-v2")
	ts2 := &toolset{ws: newWSState(t.TempDir())}
	_, err = wasmBuildRun(t, ts2, wasmBuildIn{
		Name: "hello", Source: "int x;\n", Aot: true,
	})
	tcAPIErr(t, err, aotDefaultGen)
	if !strings.Contains(err.Error(), "toolchain-v2") {
		t.Errorf("refusal %v: want the cache's own generation named", err)
	}
	if _, serr := os.Stat(ts2.ws.manifestPath()); !os.IsNotExist(serr) {
		t.Errorf("generation refusal: manifest exists — the refusal must fire before any compile")
	}

	// the early fw floor rides the default lane too: aot=true with an
	// explicit 0.2.x fw refuses before the cache is even read.
	_, err = wasmBuildRun(t, ts2, wasmBuildIn{
		Name: "hello", Source: "int x;\n", Aot: true, FW: "0.2.0",
	})
	tcAPIErr(t, err, "0.3.0")
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

// TestTCClangName (final-review I1): the compiler binary's name is
// resolved per-host — bin/clang.exe on windows (the official SDK's
// shape there), bin/clang everywhere else. Parameterized by goos so
// every branch is pinned from ANY host (the linux leg proves the
// windows branch's spelling, and vice versa); wasmBuildInner's stat
// probe and its exec share the one resolution.
func TestTCClangName(t *testing.T) {
	for goos, want := range map[string]string{
		"linux":   "clang",
		"darwin":  "clang",
		"windows": "clang.exe",
		"freebsd": "clang", // off-matrix hosts keep the bare name
	} {
		if got := tcClangName(goos); got != want {
			t.Errorf("tcClangName(%q) = %q, want %q", goos, got, want)
		}
	}
}

// TestRunCompileTimeout (final-review M4): one compile is internally
// capped — a "compiler" that outlives tcCompileTimeout is killed and
// the refusal names the cap. The child is this test binary itself
// (the os/exec self-exec pattern), so the real kill path runs on
// every CI leg, windows included.
func TestRunCompileTimeout(t *testing.T) {
	old := tcCompileTimeout
	tcCompileTimeout = 150 * time.Millisecond
	t.Cleanup(func() { tcCompileTimeout = old })
	// arm the sleeper child: exec.CommandContext with a nil Env
	// inherits OUR environ, so the var rides to the child; unset on
	// the way out so later tests in this process never see it
	os.Setenv("GO_WANT_COMPILE_SLEEPER", "1")
	t.Cleanup(func() { os.Unsetenv("GO_WANT_COMPILE_SLEEPER") })

	stdout, stderr, err, timedOut := tcRunCompile(context.Background(), os.Args[0],
		[]string{"-test.run=TestCompileSleeperHelper"})
	if !timedOut {
		t.Fatalf("want the compile cap to fire (timedOut=false err=%v stdout=%q stderr=%q)", err, stdout, stderr)
	}
	tcAPIErr(t, err, "compile timeout")
}

// TestCompileSleeperHelper is the self-exec child of
// TestRunCompileTimeout: armed by env var, it sleeps past any sane
// cap; unarmed (a normal test run) it skips.
func TestCompileSleeperHelper(t *testing.T) {
	if os.Getenv("GO_WANT_COMPILE_SLEEPER") != "1" {
		t.Skip("helper: only acts as the self-exec child of TestRunCompileTimeout")
	}
	time.Sleep(10 * time.Second)
}
