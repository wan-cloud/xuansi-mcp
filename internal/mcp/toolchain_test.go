package mcp

// toolchain_test.go — B11 task 1's toolchain-face foundations (the
// index ver gate, the embedded snapshot, the state round trip) plus
// task 2's xuansi_toolchain_status: local installed state x index
// latest (present = state record AND files on disk, the §6.1 layout),
// a remote-unreachable index degrading to the embedded snapshot
// without error, a REACHABLE index failing the ver gate refused (not
// degraded), file:// index URLs refused client-side, and missing
// pieces self-guiding to xuansi_toolchain_fetch (spec §6.3 UX).
// Task 3 adds xuansi_toolchain_fetch's six brief-named pins (sha
// mismatch zero-write, tar-slip, mid-failure atomicity, idempotent
// skip, index/asset URL gates, concurrent temp uniqueness) plus the
// MB follow-up (present fully local via the state's Dir records) and
// the timeout clamp — all fixtures are REAL tar.gz bodies built
// in-process and served over httptest, no network, no disk fixtures.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/server"
)

// TestParseToolchainIndexVerGate pins the version gate: index_ver 2 is
// refused as an api-class error (the whole toolchain face refuses with
// errWs, never a bare fmt error), and index_ver 1 parses with the
// fw_compat SPLIT TRACKS intact — since P3-T7 the aot track's domain
// is OPEN (">=0.3.0": toolchain-v2's stock wamrc against the fw line
// with an aot loader); "" or a missing key still reads as an empty
// domain for any generation that honestly carries one.
func TestParseToolchainIndexVerGate(t *testing.T) {
	if _, err := parseToolchainIndex([]byte(`{"index_ver":2,"latest":"toolchain-v3"}`)); err == nil {
		t.Fatal("index_ver 2: want refusal, got nil")
	} else {
		var rerr *server.RPCError
		if !errors.As(err, &rerr) || rerr.Class != "api" {
			t.Fatalf("index_ver 2: want api-class refusal, got %v", err)
		}
	}

	// T1 review M3: a body MISSING index_ver unmarshals to 0 — the same
	// gate must refuse it (an unversioned document is not a version 1;
	// only the explicit-ver case was pinned before).
	if _, err := parseToolchainIndex([]byte(`{"latest":"toolchain-v2"}`)); err == nil {
		t.Fatal("missing index_ver: want refusal (unmarshals to 0), got nil")
	}

	ix, err := parseToolchainIndex([]byte(`{
		"index_ver": 1,
		"latest": "toolchain-v2",
		"generations": {
			"toolchain-v2": {
				"kind": "stock-wamrc",
				"fw_compat": {"wasm": ">=0.2.0", "aot": ">=0.3.0"},
				"wasi_sdk": "24.0",
				"platforms": {}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("index_ver 1: want pass, got %v", err)
	}
	gen := ix.Generations["toolchain-v2"]
	if gen.FwCompat["aot"] != ">=0.3.0" {
		t.Fatalf("fw_compat aot: want the P3-T7 open domain \">=0.3.0\", got %q", gen.FwCompat["aot"])
	}
	if gen.FwCompat["wasm"] != ">=0.2.0" {
		t.Fatalf("fw_compat wasm: want \">=0.2.0\", got %q", gen.FwCompat["wasm"])
	}
}

// TestEmbeddedSnapshotDriftLock (B11 task 1's TestEmbeddedSnapshotParses,
// grown the T8 pin verbatim lock and the P3-T12 dual-generation +
// mechanical cross-assertion legs): pins the embedded snapshot's shape,
// every real pin verbatim — all five wasi_sdk entries and the built
// wamrc assets of BOTH generations (v2: linux-x64 + both mac legs;
// v3: the same three legs) against hardcoded truth — and, the T7-③
// debt closed, MECHANICALLY cross-asserts the build face's hardcoded
// constants against the snapshot itself: every generation that opens
// an aot lane pins exactly ">=" + aotFWFloor (a future floor move
// turns this red in every test run, moving all the legs in one
// sweep), and the snapshot's latest IS aotDefaultGen (the default
// aot lane resolves a generation the shipped snapshot actually
// names). A snapshot that no longer mirrors the drivers repo's
// published toolchain-index.json (stale copy, bad manual
// transcription) fails HERE, in every test run, instead of at a
// user's fetch. linux-arm64/win-x64 wamrc stay honest nulls in both
// generations (no leg built; the win best-effort leg failed its
// MSVC probe — tracked P2).
func TestEmbeddedSnapshotDriftLock(t *testing.T) {
	ix := embeddedToolchainIndex()
	if ix.IndexVer != 1 { // trivially true past the gate; pins that the gate ran
		t.Fatalf("embedded index_ver: want 1, got %d", ix.IndexVer)
	}
	if ix.Latest != "toolchain-v3" {
		t.Fatalf("embedded latest: want toolchain-v3 (the P3-T12 surgery generation), got %q", ix.Latest)
	}

	// ---- v2 (kept): head + pins unchanged since the T8 sync ----
	v2, ok := ix.Generations["toolchain-v2"]
	if !ok {
		t.Fatal("embedded generations: toolchain-v2 missing")
	}
	if v2.Kind != "stock-wamrc" || v2.WasiSDK != "24.0" {
		t.Fatalf("embedded v2 head: want stock-wamrc/24.0, got %q/%q", v2.Kind, v2.WasiSDK)
	}
	wamrcAssetURL := func(gen, file string) string {
		return "https://github.com/wan-cloud/xuansi-drivers/releases/download/" + gen + "/" + file
	}
	wantV2Wamrc := map[string]struct {
		url, sha string
		bytes    int64
	}{
		"linux-x64":   {wamrcAssetURL("toolchain-v2", "wamrc-2.4.0-xtensa-18.1.2-linux-x64.tar.gz"), "209a6a51b6066f05e91060d95c2506ec0bad97cb898fec1fe634cd68865b9c42", 33053698},
		"macos-x64":   {wamrcAssetURL("toolchain-v2", "wamrc-2.4.0-xtensa-18.1.2-macos-x64.tar.gz"), "b87c91e5b6bd907937f8b5f79053212abfc297ee810d2dc348488d8c0089bdc1", 23784250},
		"macos-arm64": {wamrcAssetURL("toolchain-v2", "wamrc-2.4.0-xtensa-18.1.2-macos-arm64.tar.gz"), "e73c84a24abbc50e5122a5dc8221aa2c579594ea1559224d156f8bb1778fca30", 22421504},
	}
	for plat, want := range wantV2Wamrc {
		w := v2.Platforms[plat].Wamrc
		if w == nil {
			t.Errorf("embedded v2 %s wamrc: want asset, got null", plat)
			continue
		}
		if w.URL != want.url || w.SHA256 != want.sha || w.Bytes != want.bytes {
			t.Errorf("embedded v2 %s wamrc: got url=%s sha=%s bytes=%d, want url=%s sha=%s bytes=%d", plat, w.URL, w.SHA256, w.Bytes, want.url, want.sha, want.bytes)
		}
	}

	// ---- v3 (the latest): the surgery generation ----
	v3, ok := ix.Generations["toolchain-v3"]
	if !ok {
		t.Fatal("embedded generations: toolchain-v3 missing")
	}
	if v3.Kind != "xuansi-surgery-wamrc" || v3.WasiSDK != "24.0" {
		t.Fatalf("embedded v3 head: want xuansi-surgery-wamrc/24.0, got %q/%q", v3.Kind, v3.WasiSDK)
	}
	wantV3Wamrc := map[string]struct {
		url, sha string
		bytes    int64
	}{
		"linux-x64":   {wamrcAssetURL("toolchain-v3", "wamrc-2.4.0-xuansi-surgery-cf84bd31-linux-x64.tar.gz"), "a69642458b4f78427d9bd2759b76aad6405fb609d7b8cb49ed384c890e3aad73", 33059704},
		"macos-x64":   {wamrcAssetURL("toolchain-v3", "wamrc-2.4.0-xuansi-surgery-cf84bd31-macos-x64.tar.gz"), "a2e8ee4ea6ee31f3130f49e837fb92d1bd39f9e8383f727d4112f4c61eca18bf", 23788868},
		"macos-arm64": {wamrcAssetURL("toolchain-v3", "wamrc-2.4.0-xuansi-surgery-cf84bd31-macos-arm64.tar.gz"), "8c5c7f097aed438e496ceeff28f12c20466afc088b9a01c1c027fd87b2ecf9b8", 22425173},
	}
	for plat, want := range wantV3Wamrc {
		w := v3.Platforms[plat].Wamrc
		if w == nil {
			t.Errorf("embedded v3 %s wamrc: want asset, got null", plat)
			continue
		}
		if w.URL != want.url || w.SHA256 != want.sha || w.Bytes != want.bytes {
			t.Errorf("embedded v3 %s wamrc: got url=%s sha=%s bytes=%d, want url=%s sha=%s bytes=%d", plat, w.URL, w.SHA256, w.Bytes, want.url, want.sha, want.bytes)
		}
	}

	// ---- shared: five platform keys, wasi_sdk pins verbatim (both
	// generations pin the SAME five official sdk entries) ----
	wantURLs := map[string]string{
		"linux-x64":   "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-x86_64-linux.tar.gz",
		"linux-arm64": "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-arm64-linux.tar.gz",
		"macos-x64":   "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-x86_64-macos.tar.gz",
		"macos-arm64": "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-arm64-macos.tar.gz",
		"win-x64":     "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-x86_64-windows.tar.gz",
	}
	wantSDK := map[string]struct {
		sha   string
		bytes int64
	}{
		"linux-x64":   {"c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5", 118879731},
		"linux-arm64": {"ae6c1417ea161e54bc54c0a168976af57a0c6e53078857886057a71a0d928646", 119660865},
		"macos-x64":   {"4bfc274ae8b68c771dd93e32ad4bd695336ca46b585beb3b1260e5602dbc11a2", 109975956},
		"macos-arm64": {"aeae999396d5f5caa5ce419f52e83c35869d5fd21d40af80acba2c80f51b0b3a", 108199177},
		"win-x64":     {"0f934c6e7e171b5627c81b697a9e57e96d65cd889f56ce6077350de48978afd3", 495098073},
	}
	for genName, gen := range map[string]tcGen{"toolchain-v2": v2, "toolchain-v3": v3} {
		if len(gen.Platforms) != len(wantURLs) {
			t.Fatalf("embedded %s platforms: want %d keys, got %d (%v)", genName, len(wantURLs), len(gen.Platforms), gen.Platforms)
		}
		for plat, url := range wantURLs {
			p, ok := gen.Platforms[plat]
			if !ok {
				t.Fatalf("embedded %s platforms: %s missing", genName, plat)
			}
			if p.WasiSDK == nil || p.WasiSDK.URL != url || p.WasiSDK.SHA256 != wantSDK[plat].sha || p.WasiSDK.Bytes != wantSDK[plat].bytes {
				t.Errorf("embedded %s %s wasi_sdk: got %+v, want url=%s sha=%s bytes=%d", genName, plat, p.WasiSDK, url, wantSDK[plat].sha, wantSDK[plat].bytes)
			}
		}
		for _, plat := range []string{"linux-arm64", "win-x64"} {
			if gen.Platforms[plat].Wamrc != nil {
				t.Fatalf("embedded %s %s wamrc: want null (no leg built; win best-effort leg failed — tracked P2), got %+v", genName, plat, gen.Platforms[plat].Wamrc)
			}
		}
	}

	// ---- the mechanical cross-assertions (T7-③'s debt closed: the
	// build face's hardcoded constants are locked to the SNAPSHOT, in
	// both directions) ----
	for genName, gen := range map[string]tcGen{"toolchain-v2": v2, "toolchain-v3": v3} {
		if aot, ok := gen.FwCompat["aot"]; ok && aot != "" {
			// an OPEN aot domain must be exactly the hardcoded floor
			if want := ">=" + aotFWFloor; aot != want {
				t.Errorf("%s fw_compat.aot = %q, want exactly %q (aotFWFloor=%s — a floor move must land here red)", genName, aot, want, aotFWFloor)
			}
		}
		if gen.FwCompat["wasm"] != ">=0.2.0" {
			t.Errorf("%s fw_compat.wasm = %q, want the wide \">=0.2.0\" domain", genName, gen.FwCompat["wasm"])
		}
	}
	// the default aot lane's generation is the snapshot's latest — the
	// resolver's hardcoded label can never drift from what this daemon
	// serves as current
	if ix.Latest != aotDefaultGen {
		t.Errorf("snapshot latest %q != aotDefaultGen %q (resolveAOTWamrc's default generation drifted from the served index)", ix.Latest, aotDefaultGen)
	}
	// and the latest generation really is the surgery kind the default
	// lane assumes
	if k := ix.Generations[aotDefaultGen].Kind; k != "xuansi-surgery-wamrc" {
		t.Errorf("aotDefaultGen %s kind = %q, want xuansi-surgery-wamrc", aotDefaultGen, k)
	}
}

// TestToolchainDescAOTDomain (P3-T7, extended P3-T12): the
// toolchain/build Descriptions are the face's fw-compat pairing
// surface, and they must speak the DUAL-GENERATION read — aot >=0.3.0
// with toolchain-v3 (the latest/default: the surgery wamrc, PSRAM
// exec) and toolchain-v2 (the stock wamrc, fw-internal SRAM within
// aot_exec_budget_bytes) — while every retired future-tense marker
// ("aot = null", "arrives with the A1 runtime", "refused until A1",
// "arrives later with toolchain-v3") must be GONE: v3 is the present,
// not a horizon. The drift this pins is the T8-minor⑤ staleness
// class: a Desc that describes a mechanism the product no longer has
// (or does not yet have) is a documentation defect the seat carries
// to every agent.
func TestToolchainDescAOTDomain(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	desc := map[string]string{}
	for _, tool := range lr.Tools {
		desc[tool.Name] = tool.Description
	}

	// status: the pairing matrix sentence, present tense, the four
	// ruling-mandated elements + BOTH generations of the dual-gen read.
	st := desc["xuansi_toolchain_status"]
	for _, want := range []string{
		"aot >=0.3.0",           // the open domain + its floor
		"aot_exec_budget_bytes", // v2's ceiling's caps key (T6 caps face)
		"INTERNAL SRAM",         // v2 placement: fw-internal, not PSRAM
		"honestly refused",      // over-budget semantics: no silent swap
		"toolchain-v3",          // the latest generation
		"toolchain-v2",          // the kept stock generation
		"xuansi-surgery-wamrc",  // v3's kind
		"PSRAM exec",            // v3's placement lane
	} {
		if !strings.Contains(st, want) {
			t.Errorf("toolchain_status Desc: want %q (the dual-generation aot read), missing from:\n%s", want, st)
		}
	}

	// the retired future-tense markers must be gone from ALL three —
	// including T12's own retiree: "arrives later with toolchain-v3"
	// (v3 IS the present now).
	for name, d := range desc {
		switch name {
		case "xuansi_toolchain_status", "xuansi_toolchain_fetch", "xuansi_wasm_build":
		default:
			continue
		}
		for _, stale := range []string{"aot = null", "aot empty", "refused until A1", "arrives with the A1 runtime", "empty domain", "arrives later with toolchain-v3", "arrives with toolchain-v3"} {
			if strings.Contains(d, stale) {
				t.Errorf("%s Desc: retired future-tense marker %q still present:\n%s", name, stale, d)
			}
		}
	}

	// fetch: the PAIRING sentence states the dual generations.
	fx := desc["xuansi_toolchain_fetch"]
	for _, want := range []string{"aot >=0.3.0", "toolchain-v3", "PSRAM exec"} {
		if !strings.Contains(fx, want) {
			t.Errorf("toolchain_fetch Desc: want %q in the dual-generation pairing sentence, missing from:\n%s", want, fx)
		}
	}

	// build: the aot lane's TWO knobs + the frozen recipe named.
	bd := desc["xuansi_wasm_build"]
	for _, want := range []string{"--target=xtensa", "--cpu=esp32s3", "drivers/<name>@<version>.aot", "wasm-dev", "aot=true"} {
		if !strings.Contains(bd, want) {
			t.Errorf("wasm_build Desc: want %q (the dual-knob aot lane), missing from:\n%s", want, bd)
		}
	}

	// build input schema: the aot knob's Desc carries the lane
	// semantics; the wamrc_path field Desc keeps the dev-override
	// semantics (the seat a client actually reads first).
	for _, tool := range lr.Tools {
		if tool.Name != "xuansi_wasm_build" {
			continue
		}
		m, ok := tool.InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("wasm_build input schema = %#v, want a JSON object", tool.InputSchema)
		}
		props, _ := m["properties"].(map[string]any)
		for _, field := range [...]string{"aot", "wamrc_path", "fw"} {
			f, ok := props[field].(map[string]any)
			if !ok {
				t.Fatalf("wasm_build input schema: no %s property", field)
			}
			fd, _ := f["description"].(string)
			switch field {
			case "aot":
				for _, want := range []string{".aot", "toolchain-v3"} {
					if !strings.Contains(fd, want) {
						t.Errorf("aot schema Desc: want %q, missing from:\n%s", want, fd)
					}
				}
			case "wamrc_path":
				for _, want := range []string{".aot", "wasm-dev"} {
					if !strings.Contains(fd, want) {
						t.Errorf("wamrc_path schema Desc: want %q, missing from:\n%s", want, fd)
					}
				}
			case "fw":
				// T7-①'s debt: the fw Desc must state BOTH floors —
				// 0.2.0 (wasm track) and 0.3.0 (aot lane).
				for _, want := range []string{"0.2.0", "0.3.0", "aot"} {
					if !strings.Contains(fd, want) {
						t.Errorf("fw schema Desc: want the dual-floor wording (%s), missing from:\n%s", want, fd)
					}
				}
			}
		}
	}
}

// TestTCStateRoundTripAtomic pins the state file's contract: a
// save→load round trip is lossless, saving through a bad path lets the
// write error cross untouched (writeDurableFile's own refusal — these
// are raw-error helpers; the TOOL layer wraps with errWs), a missing
// file loads as a fresh cache with non-nil maps for caller mutation,
// and a corrupt file REFUSES rather than being silently treated as
// empty (the state is the pairing check's evidence — loadManifest's
// discipline, state twin).
func TestTCStateRoundTripAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "toolchain-state.json")
	want := &tcState{
		Generation: "toolchain-v2",
		Installed: map[string]tcInstalled{
			"wasi_sdk": {SHA256: "c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5", Bytes: 118879731, Dir: "wasi-sdk-24.0-linux-x64"},
			"wamrc":    {SHA256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", Bytes: 4096, Dir: "wamrc/wamrc"},
		},
	}
	if err := saveTCState(path, want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := loadTCState(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip: got %+v, want %+v", got, want)
	}

	if err := saveTCState(filepath.Join(dir, "no-such-dir", "state.json"), want); err == nil {
		t.Fatal("bad path: want error, got nil")
	}

	fresh, err := loadTCState(filepath.Join(dir, "absent.json"))
	if err != nil {
		t.Fatalf("missing file: want fresh state, got %v", err)
	}
	if fresh.Generation != "" || fresh.Installed == nil || len(fresh.Installed) != 0 {
		t.Fatalf("missing file: want fresh empty state, got %+v", fresh)
	}

	if err := os.WriteFile(path, []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("plant corrupt: %v", err)
	}
	if _, err := loadTCState(path); err == nil {
		t.Fatal("corrupt file: want refusal, got nil")
	}
}

// ---------------------------------------------------------------------------
// xuansi_toolchain_status (B11 task 2)

// tcStatusOK runs the status handler directly (no daemon, no
// workspace — the tool reads only the cache root and the index URL)
// and fails the test on any error.
func tcStatusOK(t *testing.T, indexURL string) tcStatusOut {
	t.Helper()
	ts := &toolset{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, out, err := ts.toolchainStatus(ctx, nil, tcStatusIn{IndexURL: indexURL})
	if err != nil {
		t.Fatalf("toolchainStatus: %v", err)
	}
	return out
}

// tcStatusErr runs the handler expecting a refusal, returning it.
func tcStatusErr(t *testing.T, indexURL string) error {
	t.Helper()
	ts := &toolset{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := ts.toolchainStatus(ctx, nil, tcStatusIn{IndexURL: indexURL})
	if err == nil {
		t.Fatalf("toolchainStatus(%q): want refusal, got nil", indexURL)
	}
	return err
}

// TestTCStatusAllPresentRemoteIndex: a FULL cache (state records both
// pieces, both exist on disk in the §6.1 layout — wasi-sdk-<ver>-<plat>
// dir, wamrc/wamrc file) against a reachable index → source=remote,
// both rows present with the state's pins carried, exactly one HTTP
// hit (the index; status downloads nothing else), and NO guidance
// (nothing missing). The fake index serves the embedded snapshot's own
// body — the exact document task 6 lands upstream.
func TestTCStatusAllPresentRemoteIndex(t *testing.T) {
	plat := tcPlatKey(runtime.GOOS, runtime.GOARCH)
	if plat == "" {
		t.Skipf("host %s/%s has no index platform key — the present-path needs a matrix leg", runtime.GOOS, runtime.GOARCH)
	}
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write(toolchainSnapshotJSON)
	}))
	defer srv.Close()

	cache := t.TempDir()
	t.Setenv("XUANSI_WASM_CACHE", cache)
	sdkPin := tcInstalled{SHA256: "c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5", Bytes: 118879731, Dir: "wasi-sdk-24.0-" + plat}
	wamrcPin := tcInstalled{SHA256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", Bytes: 4096, Dir: "wamrc/wamrc"}
	if err := saveTCState(filepath.Join(cache, toolchainStateFile), &tcState{
		Generation: "toolchain-v3",
		Installed:  map[string]tcInstalled{"wasi_sdk": sdkPin, "wamrc": wamrcPin},
	}); err != nil {
		t.Fatalf("plant state: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(cache, "wasi-sdk-24.0-"+plat), 0o755); err != nil {
		t.Fatalf("plant wasi-sdk dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(cache, "wamrc"), 0o755); err != nil {
		t.Fatalf("plant wamrc dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cache, "wamrc", "wamrc"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("plant wamrc binary: %v", err)
	}

	out := tcStatusOK(t, srv.URL)
	if out.Index.Source != "remote" || out.Index.Latest != "toolchain-v3" || out.Index.Note != "" {
		t.Fatalf("index = %+v, want remote / toolchain-v3 (the P3-T12 latest) / no note", out.Index)
	}
	if out.Generation != "toolchain-v3" {
		t.Fatalf("generation = %q, want the state's toolchain-v3", out.Generation)
	}
	if out.CacheRoot != cache {
		t.Fatalf("cache_root = %q, want the XUANSI_WASM_CACHE override %q", out.CacheRoot, cache)
	}
	if len(out.Installed) != 2 {
		t.Fatalf("installed rows = %d (%+v), want 2", len(out.Installed), out.Installed)
	}
	for _, row := range out.Installed {
		if !row.Present {
			t.Errorf("row %+v: want present", row)
		}
		switch row.Kind {
		case "wasi_sdk":
			if row.SHA256 != sdkPin.SHA256 || row.Bytes != sdkPin.Bytes {
				t.Errorf("wasi_sdk row = %+v, want the state's pins %+v", row, sdkPin)
			}
		case "wamrc":
			if row.SHA256 != wamrcPin.SHA256 || row.Bytes != wamrcPin.Bytes {
				t.Errorf("wamrc row = %+v, want the state's pins %+v", row, wamrcPin)
			}
		default:
			t.Errorf("unexpected row kind %q", row.Kind)
		}
	}
	if out.Guidance != "" {
		t.Errorf("guidance = %q, want empty (nothing missing)", out.Guidance)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("HTTP hits = %d, want exactly 1 (the index; status downloads no assets)", n)
	}

	// the dual-generation upgrade line (P3-T12): the SAME complete
	// cache re-labeled toolchain-v2 against the v3-latest index gains
	// exactly the upgrade guidance — both pieces still present, the
	// pairing question answered honestly.
	if err := saveTCState(filepath.Join(cache, toolchainStateFile), &tcState{
		Generation: "toolchain-v2",
		Installed:  map[string]tcInstalled{"wasi_sdk": sdkPin, "wamrc": wamrcPin},
	}); err != nil {
		t.Fatalf("re-plant v2 state: %v", err)
	}
	out = tcStatusOK(t, srv.URL)
	if !strings.Contains(out.Guidance, "toolchain-v2") || !strings.Contains(out.Guidance, "toolchain-v3") || !strings.Contains(out.Guidance, "xuansi_toolchain_fetch") {
		t.Errorf("v2-cache guidance = %q: want the upgrade line naming both generations and fetch", out.Guidance)
	}
}

// TestTCStatusRemoteUnreachableDegradesToEmbedded: a dead index URL
// with a fresh cache is NOT an error — source=embedded, the snapshot's
// latest, a non-empty note saying why, both rows absent, and guidance
// naming xuansi_toolchain_fetch (the offline pin's whole point).
func TestTCStatusRemoteUnreachableDegradesToEmbedded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.Close() // dead port: connection refused

	t.Setenv("XUANSI_WASM_CACHE", t.TempDir())
	out := tcStatusOK(t, srv.URL)
	if out.Index.Source != "embedded" || out.Index.Latest != "toolchain-v3" {
		t.Fatalf("index = %+v, want embedded / toolchain-v3 (the snapshot's values)", out.Index)
	}
	if out.Index.Note == "" {
		t.Fatal("note: want non-empty (why the remote index did not answer)")
	}
	if out.Generation != "" {
		t.Fatalf("generation = %q, want empty (fresh cache)", out.Generation)
	}
	for _, row := range out.Installed {
		if row.Present {
			t.Errorf("row %+v: want present=false on a fresh cache", row)
		}
		if row.SHA256 != "" || row.Bytes != 0 {
			t.Errorf("row %+v: want no pins without a state record", row)
		}
	}
	if !strings.Contains(out.Guidance, "xuansi_toolchain_fetch") {
		t.Errorf("guidance = %q, want it to name xuansi_toolchain_fetch", out.Guidance)
	}
}

// TestTCStatusRemoteVerGateRefused: a REACHABLE index whose body fails
// the ver gate is a hard api-class refusal, NOT a silent degrade — the
// degrade path is for UNAVAILABILITY only; "a newer index needs a
// newer daemon" is a compatibility signal status must not paper over
// with the older snapshot's latest.
func TestTCStatusRemoteVerGateRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"index_ver":2,"latest":"toolchain-v3"}`))
	}))
	defer srv.Close()

	t.Setenv("XUANSI_WASM_CACHE", t.TempDir())
	err := tcStatusErr(t, srv.URL)
	var rerr *server.RPCError
	if !errors.As(err, &rerr) || rerr.Class != "api" {
		t.Fatalf("index_ver 2 index: want api-class refusal, got %v", err)
	}
	if !strings.Contains(err.Error(), "index_ver 2") {
		t.Errorf("refusal %v: want the gate's newer-daemon message verbatim", err)
	}
}

// TestTCStatusFileURLRefused: an explicit non-http(s) index URL is
// refused client-side before any request — driverFetch's gate shape
// (file:// must not read local paths).
func TestTCStatusFileURLRefused(t *testing.T) {
	t.Setenv("XUANSI_WASM_CACHE", t.TempDir())
	err := tcStatusErr(t, "file:///etc/passwd")
	var rerr *server.RPCError
	if !errors.As(err, &rerr) || rerr.Class != "api" {
		t.Fatalf("file:// index_url: want api-class refusal, got %v", err)
	}
	if !strings.Contains(err.Error(), "http(s)") {
		t.Errorf("refusal %v: want it to demand an http(s) URL", err)
	}
}

// TestTCStatusGuidanceOnMissing (brief-named): an empty cache with a
// dead index → guidance non-empty and pointing at fetch — the status
// answer always carries the next step (spec §6.3 UX).
func TestTCStatusGuidanceOnMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.Close()

	t.Setenv("XUANSI_WASM_CACHE", t.TempDir())
	out := tcStatusOK(t, srv.URL)
	if out.Guidance == "" {
		t.Fatal("guidance: want non-empty on an empty cache")
	}
	if !strings.Contains(out.Guidance, "xuansi_toolchain_fetch") {
		t.Errorf("guidance = %q, want the fetch pointer", out.Guidance)
	}
}

// TestTCStatusPlatKeyMapping: Go's runtime names → the index's
// platform-key vocabulary ("macos" not "darwin", "x64" not "amd64"),
// compositional over the two tables; anything off the matrix maps to
// "" (win-arm64 composes — no index carries the key, and the
// Platforms lookup is the asset-leg gate, not the naming).
func TestTCStatusPlatKeyMapping(t *testing.T) {
	for _, c := range []struct{ goos, goarch, want string }{
		{"linux", "amd64", "linux-x64"},
		{"linux", "arm64", "linux-arm64"},
		{"darwin", "amd64", "macos-x64"},
		{"darwin", "arm64", "macos-arm64"},
		{"windows", "amd64", "win-x64"},
		{"windows", "arm64", "win-arm64"},
		{"freebsd", "amd64", ""},
		{"linux", "riscv64", ""},
	} {
		if got := tcPlatKey(c.goos, c.goarch); got != c.want {
			t.Errorf("tcPlatKey(%s,%s) = %q, want %q", c.goos, c.goarch, got, c.want)
		}
	}
}

// TestTCStatusPresentFullyLocal (T2 review MB): present is decided by
// the STATE's own Dir records + disk probes alone — a generation the
// served index does not even know still reports present when the
// records and the trees are there (status never needs the index to
// interpret its own cache). A record WITHOUT Dir (the pre-T3 shape)
// cannot prove its target and reads not-present — a re-fetch heals it.
// The unknown-generation guidance rides the revised wording.
func TestTCStatusPresentFullyLocal(t *testing.T) {
	plat := tcPlatKey(runtime.GOOS, runtime.GOARCH)
	if plat == "" {
		t.Skipf("host %s/%s has no index platform key — the Dir-record path needs a matrix leg", runtime.GOOS, runtime.GOARCH)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.Close() // dead port → embedded snapshot, which does NOT know the planted generation

	cache := t.TempDir()
	t.Setenv("XUANSI_WASM_CACHE", cache)
	if err := saveTCState(filepath.Join(cache, toolchainStateFile), &tcState{
		Generation: "toolchain-custom-not-in-index",
		Installed: map[string]tcInstalled{
			"wasi_sdk": {SHA256: "c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5", Bytes: 118879731, Dir: "wasi-sdk-24.0-" + plat},
			"wamrc":    {SHA256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", Bytes: 4096}, // no Dir: legacy shape
		},
	}); err != nil {
		t.Fatalf("plant state: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(cache, "wasi-sdk-24.0-"+plat), 0o755); err != nil {
		t.Fatalf("plant wasi-sdk dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(cache, "wamrc"), 0o755); err != nil {
		t.Fatalf("plant wamrc dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cache, "wamrc", "wamrc"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("plant wamrc binary: %v", err)
	}

	out := tcStatusOK(t, srv.URL)
	if out.Generation != "toolchain-custom-not-in-index" {
		t.Fatalf("generation = %q, want the state's own label", out.Generation)
	}
	for _, row := range out.Installed {
		switch row.Kind {
		case "wasi_sdk":
			if !row.Present {
				t.Errorf("wasi_sdk row %+v: want present — the Dir record + the tree are local evidence, the index is not needed", row)
			}
		case "wamrc":
			if row.Present {
				t.Errorf("wamrc row %+v: want present=false — a record without Dir cannot prove its target", row)
			}
		}
	}
	if !strings.Contains(out.Guidance, "unknown to the index") {
		t.Errorf("guidance = %q, want the unknown-generation wording", out.Guidance)
	}
	if !strings.Contains(out.Guidance, "xuansi_toolchain_fetch") {
		t.Errorf("guidance = %q, want the fetch pointer for the Dir-less wamrc record", out.Guidance)
	}
}

// ---------------------------------------------------------------------------
// xuansi_toolchain_fetch (B11 task 3)

// tcHostPlat is the host's index platform key, skipping on off-matrix
// hosts (the fetch's asset selection needs a leg; CI runs linux-x64).
func tcHostPlat(t *testing.T) string {
	t.Helper()
	plat := tcPlatKey(runtime.GOOS, runtime.GOARCH)
	if plat == "" {
		t.Skipf("host %s/%s has no index platform key — fetch tests need a matrix leg", runtime.GOOS, runtime.GOARCH)
	}
	return plat
}

// tcEnt is one tar.gz fixture member.
type tcEnt struct {
	name string
	body string
	mode int64
	dir  bool
	// link members (the official wasi-sdk's own shape): TypeSymlink /
	// TypeLink with the target in linkname. 0 = regular/dir by dir.
	link     byte
	linkname string
}

// tcTarGz builds a REAL (small) tar.gz in-process — stdlib archive/tar
// + compress/gzip, no disk fixtures, exactly the shapes the extract
// path must handle (dir members, exec-bit files, nested paths, and
// the wasi-sdk tarball's relative symlink/hardlink members).
func tcTarGz(t *testing.T, ents ...tcEnt) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range ents {
		hdr := &tar.Header{Name: e.name, Mode: e.mode}
		switch {
		case e.link != 0:
			hdr.Typeflag = e.link
			hdr.Linkname = e.linkname
		case e.dir:
			hdr.Typeflag = tar.TypeDir
		default:
			hdr.Typeflag = tar.TypeReg
			hdr.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("fixture tar header %q: %v", e.name, err)
		}
		if e.link == 0 && !e.dir {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("fixture tar body %q: %v", e.name, err)
			}
		}
	}
	tw.Close()
	gz.Close()
	return buf.Bytes()
}

// tcSDKTar is the wasi-sdk tarball's shape: ONE top-level dir holding
// bin/ executables (0755 — the exec bit the toolchain needs) and
// share/ data (0644).
func tcSDKTar(t *testing.T) []byte {
	return tcTarGz(t,
		tcEnt{name: "wasi-sdk-24.0-x86_64-linux", dir: true, mode: 0o755},
		tcEnt{name: "wasi-sdk-24.0-x86_64-linux/bin/clang", body: "#!/bin/sh\nfake clang\n", mode: 0o755},
		tcEnt{name: "wasi-sdk-24.0-x86_64-linux/share/misc.txt", body: "share\n", mode: 0o644},
	)
}

// tcWamrcTar is the wamrc tarball's shape: one top-level dir holding
// the wamrc binary itself.
func tcWamrcTar(t *testing.T) []byte {
	return tcTarGz(t,
		tcEnt{name: "wamrc-2.4.0-xtensa-18.1.2", dir: true, mode: 0o755},
		tcEnt{name: "wamrc-2.4.0-xtensa-18.1.2/wamrc", body: "fake wamrc\n", mode: 0o755},
	)
}

// tcTestIndex builds one index body: generation toolchain-v2 (sdk
// 24.0, the embedded snapshot's shape) carrying the given platform's
// asset pair.
func tcTestIndex(t *testing.T, plat string, sdk, wamrc *tcAsset) []byte {
	t.Helper()
	b, err := json.Marshal(toolchainIndex{
		IndexVer: 1,
		Latest:   "toolchain-v2",
		Generations: map[string]tcGen{
			"toolchain-v2": {
				Kind: "stock-wamrc", FwCompat: map[string]string{"wasm": ">=0.2.0", "aot": ">=0.3.0"},
				WasiSDK:   "24.0",
				Platforms: map[string]tcPlat{plat: {WasiSDK: sdk, Wamrc: wamrc}},
			},
		},
	})
	if err != nil {
		t.Fatalf("fixture index: %v", err)
	}
	return b
}

// tcPairServer serves /index plus /sdk and /wamrc with REAL pins
// (sha256 + bytes computed from the served bodies) and counts asset
// hits — the idempotency test's "second call downloads no asset
// bytes" evidence. delay (when >0) slows asset serves to widen the
// concurrency window; wamrcCode (when != 0) makes /wamrc fail with
// that status instead of serving.
type tcPairServer struct {
	*httptest.Server
	sdkSHA, wamrcSHA   string
	sdkHits, wamrcHits atomic.Int32
}

func tcPairServerStart(t *testing.T, plat string, delay time.Duration, wamrcCode int) *tcPairServer {
	t.Helper()
	sdkBody, wamrcBody := tcSDKTar(t), tcWamrcTar(t)
	ps := &tcPairServer{sdkSHA: sha256hex(sdkBody), wamrcSHA: sha256hex(wamrcBody)}
	// the asset URLs are placeholders rewritten per request — the
	// index body must name THIS server, which does not exist yet at
	// build time
	index := tcTestIndex(t, plat,
		&tcAsset{URL: "__SDK__", SHA256: ps.sdkSHA, Bytes: int64(len(sdkBody))},
		&tcAsset{URL: "__WAMRC__", SHA256: ps.wamrcSHA, Bytes: int64(len(wamrcBody))},
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/index", func(w http.ResponseWriter, r *http.Request) {
		body := bytes.ReplaceAll(index, []byte("__SDK__"), []byte("http://"+r.Host+"/sdk"))
		body = bytes.ReplaceAll(body, []byte("__WAMRC__"), []byte("http://"+r.Host+"/wamrc"))
		_, _ = w.Write(body)
	})
	serve := func(body []byte, hits *atomic.Int32) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			if delay > 0 {
				time.Sleep(delay)
			}
			hits.Add(1)
			_, _ = w.Write(body)
		}
	}
	mux.HandleFunc("/sdk", serve(sdkBody, &ps.sdkHits))
	mux.HandleFunc("/wamrc", func(w http.ResponseWriter, r *http.Request) {
		if wamrcCode != 0 {
			w.WriteHeader(wamrcCode)
			return
		}
		serve(wamrcBody, &ps.wamrcHits)(w, r)
	})
	ps.Server = httptest.NewServer(mux)
	t.Cleanup(ps.Close)
	return ps
}

// tcFetchRun runs the fetch handler directly (no daemon, no lease)
// and returns its refusal — callers assert; no t.Fatalf inside, so
// the concurrency test can call it from goroutines.
func tcFetchRun(in tcFetchIn) (tcFetchOut, error) {
	ts := &toolset{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, out, err := ts.toolchainFetch(ctx, nil, in)
	return out, err
}

// tcNoResidue asserts the cache root holds NOTHING — no state file,
// no piece dirs, no .tmp-fetch-* staging: the atomic sequence's
// zero-residue promise after any refusal.
func tcNoResidue(t *testing.T, root string) {
	t.Helper()
	ents, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("read cache root %s: %v", root, err)
	}
	if len(ents) != 0 {
		names := make([]string, 0, len(ents))
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("cache root residue: want empty, got %v", names)
	}
}

// tcCacheEnv points the cache root at a fresh temp dir.
func tcCacheEnv(t *testing.T) string {
	t.Helper()
	cache := t.TempDir()
	t.Setenv("XUANSI_WASM_CACHE", cache)
	return cache
}

// tcAPIErr asserts err is an api-class RPCError containing want.
func tcAPIErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want an api-class refusal containing %q, got nil", want)
	}
	var rerr *server.RPCError
	if !errors.As(err, &rerr) || rerr.Class != "api" {
		t.Fatalf("want an api-class refusal, got %v", err)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("refusal %v: want it to contain %q", err, want)
	}
}

// tcExecOK is the platform-honest exec assertion for extracted files:
// off-windows the exec bit must survive extraction (perm&0o111 — the
// toolchain runs these); on windows NTFS a Go file mode carries only
// the read-only bit (0666 writable / 0444 read-only, exec is
// unrepresentable), so the honest pin there is a landed, writable
// file — existence plus non-read-only semantics, no skip.
func tcExecOK(fi os.FileInfo) bool {
	if runtime.GOOS == "windows" {
		return fi.Mode().Perm()&0o222 != 0
	}
	return fi.Mode().Perm()&0o111 != 0
}

// TestFetchTCArchiveShaMismatch (brief-named): a bad sha pin refuses
// — at the unit level the error names the sha mismatch, and at the
// pipeline level the refusal leaves the CACHE untouched (zero
// writes: no state file, no piece dirs, no temp staging).
func TestFetchTCArchiveShaMismatch(t *testing.T) {
	body := []byte("a tiny archive body\n")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// unit: wrong sha → error naming the mismatch; w is a scratch
	// buffer only (the real caller writes into .tmp-fetch-*, which the
	// atomic sequence discards wholesale — THAT is the zero-write
	// guarantee the pipeline half pins below).
	var buf bytes.Buffer
	err := fetchTCArchive(ctx, srv.URL, strings.Repeat("0", 64), int64(len(body)), int64(len(body))+8, &buf)
	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("wrong sha: want a sha256 mismatch error, got %v", err)
	}
	// unit: byte-count mismatch → error naming the counts
	buf.Reset()
	err = fetchTCArchive(ctx, srv.URL, sha256hex(body), int64(len(body))-1, int64(len(body))+8, &buf)
	if err == nil || !strings.Contains(err.Error(), "index says") {
		t.Fatalf("byte mismatch: want a got-vs-index error, got %v", err)
	}
	// unit: over the per-archive cap → error naming the cap
	buf.Reset()
	err = fetchTCArchive(ctx, srv.URL, sha256hex(body), int64(len(body)), int64(len(body))-1, &buf)
	if err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("over cap: want a cap error, got %v", err)
	}
	// unit: the happy pin passes and lands the exact bytes
	buf.Reset()
	if err := fetchTCArchive(ctx, srv.URL, sha256hex(body), int64(len(body)), int64(len(body))+8, &buf); err != nil {
		t.Fatalf("good pin: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), body) {
		t.Fatal("good pin: w must receive the archive bytes verbatim")
	}

	// pipeline: an index pinning the WRONG sha → api refusal, zero
	// residue in the cache root.
	plat := tcHostPlat(t)
	sdk := tcSDKTar(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/index", func(w http.ResponseWriter, r *http.Request) {
		body := bytes.ReplaceAll(tcTestIndex(t, plat,
			&tcAsset{URL: "__SDK__", SHA256: strings.Repeat("0", 64), Bytes: int64(len(sdk))},
			nil), []byte("__SDK__"), []byte("http://"+r.Host+"/sdk"))
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/sdk", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(sdk) })
	srv2 := httptest.NewServer(mux)
	defer srv2.Close()
	cache := tcCacheEnv(t)
	_, err = tcFetchRun(tcFetchIn{IndexURL: srv2.URL + "/index"})
	tcAPIErr(t, err, "sha256")
	tcNoResidue(t, cache)
}

// TestTCPathHostile: the shared tar-slip shape gate, pinned
// LEXICALLY — the same refusal set on every host (final-review C1:
// the old IsAbs-only checks had per-platform blind spots — "/tmp/x"
// passes IsAbs on neither... on windows IsAbs("/tmp/x")=false, and
// on unix IsAbs("C:/x")=false with an empty VolumeName). This pin
// runs on BOTH CI legs, so each platform proves the forms the OTHER
// platform's natives would miss.
func TestTCPathHostile(t *testing.T) {
	hostile := []string{
		"", "/tmp/tc-evil-abs", "C:/evil", "c:/evil", `C:\evil`,
		"../evil", "a/../../evil2", `back\slash`, "nul\x00byte",
		"C:", // a bare drive letter colon is never a member shape
	}
	for _, p := range hostile {
		if !tcPathHostile(p) {
			t.Errorf("tcPathHostile(%q) = false, want true (the refusal set must be platform-blind)", p)
		}
	}
	safe := []string{"top", "top/bin/tool", "wasi-sdk-24.0-x86_64-linux/bin/clang", "clang", "llvm-objcopy", "share/wasi-sysroot/include/stdio.h"}
	for _, p := range safe {
		if tcPathHostile(p) {
			t.Errorf("tcPathHostile(%q) = true, want false (a real toolchain member shape)", p)
		}
	}
}

// TestExtractTarGzSlip: hostile tar members — ../ traversal, absolute
// paths, backslash names, WINDOWS-SHAPED absolutes (leading "/", volume
// names "C:/evil" and "C:\evil" — filepath.IsAbs is false for these on
// windows, and VolumeName is empty on unix, so BOTH platforms need the
// explicit forms), and hostile link targets — are all refused, and
// nothing escapes the extraction root. A safe member (control)
// extracts fine.
func TestExtractTarGzSlip(t *testing.T) {
	base := t.TempDir()
	dst := filepath.Join(base, "out")

	// control: safe members extract with the tree's modes
	safe := tcTarGz(t, tcEnt{name: "top", dir: true, mode: 0o755}, tcEnt{name: "top/bin/tool", body: "x\n", mode: 0o755})
	if err := extractTarGz(bytes.NewReader(safe), dst); err != nil {
		t.Fatalf("safe archive: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "top", "bin", "tool")); err != nil || string(b) != "x\n" {
		t.Fatalf("safe extract: %q, %v", b, err)
	}
	if fi, err := os.Stat(filepath.Join(dst, "top", "bin", "tool")); err != nil || !tcExecOK(fi) {
		t.Fatalf("exec member must keep an exec bit (windows: landed + writable), got %v, %v", fi, err)
	}

	for _, evil := range []tcEnt{
		{name: "../evil", body: "x", mode: 0o644},
		{name: "/tmp/tc-evil-abs", body: "x", mode: 0o644},
		{name: "a/../../evil2", body: "x", mode: 0o644},
		{name: `back\slash`, body: "x", mode: 0o644},
		// windows-shaped absolutes: on windows filepath.IsAbs("/tmp/…")
		// is FALSE (no volume) and IsAbs("C:/evil") is true; on unix
		// VolumeName("C:/evil") is EMPTY — so on BOTH platforms at
		// least one of these two used to slip past a plain IsAbs gate
		// (final-review C1: the leading-"/" form on windows, the
		// volume form on linux — landing a garbage "C:" tree inside
		// the cache even when contained)
		{name: "C:/evil-vol", body: "x", mode: 0o644},
		{name: `C:\evil-vol-bs`, body: "x", mode: 0o644},
	} {
		if err := extractTarGz(bytes.NewReader(tcTarGz(t, evil)), filepath.Join(base, "slip")); err == nil {
			t.Errorf("member %q: want refusal, got nil", evil.name)
		}
	}

	// hostile link TARGETS, both plain-absolute and windows-shaped:
	// since the controlled-landing fix (B11 task-11 fix round) link
	// members with contained RELATIVE targets land — these fixtures
	// are refused for their TARGETS ("/etc/passwd" is a path-shape
	// escape on every platform: an absolute on unix, and on windows a
	// root-relative pointer the OS resolves against the current
	// drive's root — OUTSIDE the cache; "C:/Windows/evil" likewise).
	for _, evil := range []struct{ name, target string }{
		{"lnk", "/etc/passwd"},          // absolute (unix) / drive-root-relative (windows)
		{"lnk-c", "C:/Windows/evil"},    // windows volume-absolute — IsAbs=false on unix
		{"lnk-c-bs", `C:\Windows\evil`}, // backslash + volume
	} {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		tw := tar.NewWriter(gz)
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeSymlink, Name: evil.name, Linkname: evil.target}); err != nil {
			t.Fatalf("fixture link header %q: %v", evil.name, err)
		}
		tw.Close()
		gz.Close()
		if err := extractTarGz(bytes.NewReader(buf.Bytes()), filepath.Join(base, "slip")); err == nil {
			t.Errorf("symlink member %q -> %q: want refusal, got nil", evil.name, evil.target)
		}
	}

	// and nothing escaped the roots
	for _, p := range []string{"evil", "evil2", "tc-evil-abs"} {
		if _, err := os.Lstat(filepath.Join(base, p)); !os.IsNotExist(err) {
			t.Errorf("member %q escaped the extraction root", p)
		}
	}
}

// TestExtractTarGzLinks: the OFFICIAL wasi-sdk tarball's own shape —
// relative symlink members (bin/llvm-strip -> llvm-objcopy,
// bin/clang++ -> clang &c) and hardlink members — LAND, resolve
// inside the extraction root, and run; hostile link targets (absolute,
// ".."-escaping, backslash) stay refused. Found by the B11 task-11
// HIL: the real wasi-sdk-24.0-x86_64-linux.tar.gz carries ~20 relative
// symlinks, and the blanket link refusal made the fetch of the REAL
// artifact impossible — the "wasi-sdk tarballs carry none" assumption
// was wrong.
func TestExtractTarGzLinks(t *testing.T) {
	base := t.TempDir()
	dst := filepath.Join(base, "out")

	// the wasi-sdk shape: one top dir, a real binary, a same-dir
	// relative symlink onto it, another, and a hardlink onto an
	// earlier member.
	good := tcTarGz(t,
		tcEnt{name: "wasi-sdk-x", dir: true, mode: 0o755},
		tcEnt{name: "wasi-sdk-x/bin", dir: true, mode: 0o755},
		// FORWARD REFERENCE (L1): this link member precedes its
		// target in the member stream — legal tar, and exactly why
		// links land in a DEFERRED pass rather than in-loop (a
		// refactor back to in-loop linking breaks this pin).
		tcEnt{name: "wasi-sdk-x/bin/wasm-ld", mode: 0o755, link: tar.TypeSymlink, linkname: "lld"},
		tcEnt{name: "wasi-sdk-x/bin/llvm-objcopy", body: "objcopy-bytes\n", mode: 0o755},
		tcEnt{name: "wasi-sdk-x/bin/llvm-strip", mode: 0o755, link: tar.TypeSymlink, linkname: "llvm-objcopy"},
		tcEnt{name: "wasi-sdk-x/bin/clang", body: "clang-bytes\n", mode: 0o755},
		tcEnt{name: "wasi-sdk-x/bin/clang++", mode: 0o755, link: tar.TypeSymlink, linkname: "clang"},
		// the forward-referenced target, listed AFTER its link
		tcEnt{name: "wasi-sdk-x/bin/lld", body: "lld-bytes\n", mode: 0o755},
		tcEnt{name: "wasi-sdk-x/bin/clang-hardlink", mode: 0o755, link: tar.TypeLink, linkname: "wasi-sdk-x/bin/clang"},
	)
	if err := extractTarGz(bytes.NewReader(good), dst); err != nil {
		t.Fatalf("wasi-sdk-shaped archive (relative links): %v", err)
	}
	// the symlinks exist as symlinks and resolve to the real bytes —
	// including the FORWARD-REFERENCED one
	for ln, want := range map[string]string{
		"wasi-sdk-x/bin/llvm-strip": "objcopy-bytes\n",
		"wasi-sdk-x/bin/clang++":    "clang-bytes\n",
		"wasi-sdk-x/bin/wasm-ld":    "lld-bytes\n",
	} {
		p := filepath.Join(dst, filepath.FromSlash(ln))
		if _, err := os.Lstat(p); err != nil {
			t.Fatalf("link member %s did not land: %v", ln, err)
		}
		fi, err := os.Stat(p) // follows the link: the target's bytes
		if err != nil || fi.IsDir() {
			t.Fatalf("link %s must resolve to a file (fi=%v err=%v)", ln, fi, err)
		}
		if b, rerr := os.ReadFile(p); rerr != nil || string(b) != want {
			t.Fatalf("link %s resolved bytes = %q (want %q), err=%v", ln, b, want, rerr)
		}
	}
	// the hardlink shares the target's bytes
	if b, err := os.ReadFile(filepath.Join(dst, "wasi-sdk-x", "bin", "clang-hardlink")); err != nil || string(b) != "clang-bytes\n" {
		t.Fatalf("hardlink bytes = %q err=%v", b, err)
	}

	// hostile link TARGETS stay refused (the target, not the member
	// type, is the vector): absolute, ..-escaping, backslash.
	for _, evil := range []tcEnt{
		{name: "lnk-abs", link: tar.TypeSymlink, linkname: "/etc/passwd"},
		{name: "lnk-up", link: tar.TypeSymlink, linkname: "../../escape"},
		{name: "lnk-empty", link: tar.TypeSymlink, linkname: ""},
		{name: `lnk\bs`, link: tar.TypeSymlink, linkname: `win\path`},
		{name: "hl-abs", link: tar.TypeLink, linkname: "/etc/passwd"},
		{name: "hl-up", link: tar.TypeLink, linkname: "../escape"},
	} {
		if err := extractTarGz(bytes.NewReader(tcTarGz(t, evil)), filepath.Join(base, "slip")); err == nil {
			t.Errorf("link member %q (target %q): want refusal, got nil", evil.name, evil.linkname)
		}
	}
	// and nothing escaped
	for _, p := range []string{"escape"} {
		if _, err := os.Stat(filepath.Join(base, p)); !os.IsNotExist(err) {
			t.Errorf("link target %q escaped the extraction root", p)
		}
	}
}

// TestExtractTarGzFallbackPath (T11a M1): the symlink COPY FALLBACK
// reads the correctly-resolved target path — pinned by driving
// tcLandLink directly with an injected link primitive that fails the
// way windows denies symlinks (EACCES without developer mode). On
// unix os.Symlink just succeeds, so the copy leg is unreachable in
// production there — which is exactly how the original bug survived a
// green linux HIL: absTarget used to prefix dst a SECOND time for
// symlinks (dst/dst/...), the fallback's ReadFile ENOENT'd, and every
// windows fetch — where the fallback fires for EVERY symlink member
// of the real wasi-sdk tarball — would have died at extraction.
func TestExtractTarGzFallbackPath(t *testing.T) {
	base := t.TempDir()
	dst := filepath.Join(base, "out")
	if err := os.MkdirAll(filepath.Join(dst, "bin"), 0o755); err != nil {
		t.Fatalf("fixture bin dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dst, "bin", "clang"), []byte("clang-bytes\n"), 0o755); err != nil {
		t.Fatalf("fixture target: %v", err)
	}

	// the symlink leg: EACCES from the primitive (the windows shape)
	// forces the copy fallback, which must read EXACTLY dst/bin/clang
	// — the seam observes the resolved path it was handed, so a
	// doubled dst prefix fails loudly right here.
	want := filepath.Join(dst, "bin", "clang")
	sl := tcLink{name: "bin/clang++", target: "clang"}
	saw := ""
	n, err := tcLandLink(dst, sl, func(resolved, linkPath string) error {
		saw = resolved
		return syscall.EACCES
	}, tcExtractMax)
	if err != nil {
		t.Fatalf("symlink copy fallback: %v (resolved %q; this is the M1 shape when the path is wrong)", err, saw)
	}
	if saw != want {
		t.Errorf("symlink fallback reads %q, want %q (the M1 bug doubled the dst prefix)", saw, want)
	}
	if n != int64(len("clang-bytes\n")) {
		t.Errorf("fallback byte count = %d, want %d", n, len("clang-bytes\n"))
	}
	// the fallback's artifact: a REAL FILE (not a link) with the
	// target's bytes and its exec bit kept
	fb, rerr := os.ReadFile(filepath.Join(dst, "bin", "clang++"))
	if rerr != nil || string(fb) != "clang-bytes\n" {
		t.Fatalf("fallback artifact bytes = %q err=%v", fb, rerr)
	}
	if fi, ferr := os.Lstat(filepath.Join(dst, "bin", "clang++")); ferr != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("fallback artifact must be a regular file (fi=%v err=%v)", fi, ferr)
	} else if !tcExecOK(fi) {
		t.Fatalf("fallback artifact must keep the target's exec bit (mode=%v)", fi.Mode())
	}

	// the hardlink leg: the target is ARCHIVE-ROOT-relative and must
	// anchor at dst (never double-prefixed); EXDEV (cross-device
	// os.Link) forces the copy fallback there too
	hl := tcLink{name: "bin/cc", target: "bin/clang", hard: true}
	saw = ""
	if _, err := tcLandLink(dst, hl, func(resolved, linkPath string) error {
		saw = resolved
		return syscall.EXDEV
	}, tcExtractMax); err != nil {
		t.Fatalf("hardlink copy fallback: %v (resolved %q)", err, saw)
	}
	if saw != want {
		t.Errorf("hardlink fallback reads %q, want %q", saw, want)
	}
	if hb, herr := os.ReadFile(filepath.Join(dst, "bin", "cc")); herr != nil || string(hb) != "clang-bytes\n" {
		t.Fatalf("hardlink fallback bytes = %q err=%v", hb, herr)
	}

	// a NON-fallback-worthy failure (ENOENT — a dangling link) stays
	// an error rather than silently vanishing
	if _, err := tcLandLink(dst, tcLink{name: "bin/dangling", target: "nope"}, func(string, string) error {
		return syscall.ENOENT
	}, tcExtractMax); err == nil {
		t.Error("dangling link: want an error, got nil")
	}

	// the copy fallback respects the remaining extraction budget
	if _, err := tcLandLink(dst, sl, func(string, string) error { return syscall.EACCES },
		int64(len("clang-bytes\n")-1)); err == nil {
		t.Error("budget-exceeded copy: want an error, got nil")
	}
}

// TestToolchainFetchAtomicMidFailure (brief-named): two assets, the
// SECOND fails mid-fetch (HTTP 500) → api refusal, and the FIRST
// piece — downloaded and staged fine — leaves NOTHING in the cache:
// no target dir, no state file, no temp staging.
func TestToolchainFetchAtomicMidFailure(t *testing.T) {
	plat := tcHostPlat(t)
	ps := tcPairServerStart(t, plat, 0, http.StatusInternalServerError)
	cache := tcCacheEnv(t)

	_, err := tcFetchRun(tcFetchIn{IndexURL: ps.URL + "/index"})
	tcAPIErr(t, err, "500")

	// the atomicity pin: the FIRST piece (fully downloaded+staged
	// before the second failed) must not survive anywhere.
	if _, serr := os.Stat(filepath.Join(cache, "wasi-sdk-24.0-"+plat)); !os.IsNotExist(serr) {
		t.Fatalf("first piece must not land when the second fails (stat err = %v)", serr)
	}
	tcNoResidue(t, cache)
	if n := ps.sdkHits.Load(); n != 1 {
		t.Errorf("sdk hits = %d, want 1 (staged once, committed never)", n)
	}
}

// TestToolchainFetchIdempotent (brief-named): a first call installs
// both pieces (flattened sdk dir with exec bits, wamrc binary, state
// with Dir records); the SECOND call downloads no asset bytes and
// reports everything Skipped, the state file byte-identical.
func TestToolchainFetchIdempotent(t *testing.T) {
	plat := tcHostPlat(t)
	ps := tcPairServerStart(t, plat, 0, 0)
	cache := tcCacheEnv(t)

	out, err := tcFetchRun(tcFetchIn{IndexURL: ps.URL + "/index"})
	if err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	if out.Generation != "toolchain-v2" || out.CacheRoot != cache {
		t.Fatalf("first out = %+v, want generation toolchain-v2 at the cache root", out)
	}
	if !reflect.DeepEqual(out.Downloaded, []string{"wasi_sdk", "wamrc"}) || len(out.Skipped) != 0 {
		t.Fatalf("first out downloaded/skipped = %v/%v, want [wasi_sdk wamrc]/[]", out.Downloaded, out.Skipped)
	}
	if out.Note != "" {
		t.Fatalf("first out note = %q, want empty (both legs exist)", out.Note)
	}

	// layout: the tarball's top dir flattened into wasi-sdk-24.0-<plat>,
	// the exec bit the toolchain needs intact; wamrc at wamrc/wamrc.
	clang := filepath.Join(cache, "wasi-sdk-24.0-"+plat, "bin", "clang")
	if fi, ferr := os.Stat(clang); ferr != nil || !tcExecOK(fi) {
		t.Fatalf("sdk bin/clang: %v (mode %v) — want present + exec bit (windows: landed + writable)", ferr, fi)
	}
	wamrcBin := filepath.Join(cache, "wamrc", "wamrc")
	if fi, ferr := os.Stat(wamrcBin); ferr != nil || !tcExecOK(fi) {
		t.Fatalf("wamrc/wamrc: %v (mode %v) — want present + exec bit (windows: landed + writable)", ferr, fi)
	}

	statePath := filepath.Join(cache, toolchainStateFile)
	st, err := loadTCState(statePath)
	if err != nil {
		t.Fatalf("state after first: %v", err)
	}
	if st.Generation != "toolchain-v2" {
		t.Fatalf("state generation = %q", st.Generation)
	}
	if rec := st.Installed["wasi_sdk"]; rec.SHA256 != ps.sdkSHA || rec.Dir != "wasi-sdk-24.0-"+plat {
		t.Fatalf("state wasi_sdk record = %+v (sdk sha %s)", rec, ps.sdkSHA)
	}
	if rec := st.Installed["wamrc"]; rec.SHA256 != ps.wamrcSHA || rec.Dir != "wamrc/wamrc" {
		t.Fatalf("state wamrc record = %+v (wamrc sha %s)", rec, ps.wamrcSHA)
	}
	state1, _ := os.ReadFile(statePath)
	if n := ps.sdkHits.Load(); n != 1 || ps.wamrcHits.Load() != 1 {
		t.Fatalf("asset hits after first = %d/%d, want 1/1", n, ps.wamrcHits.Load())
	}

	// second call: everything Skipped, no asset bytes moved, state
	// byte-identical.
	out2, err := tcFetchRun(tcFetchIn{IndexURL: ps.URL + "/index"})
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if len(out2.Downloaded) != 0 || !reflect.DeepEqual(out2.Skipped, []string{"wasi_sdk", "wamrc"}) {
		t.Fatalf("second out downloaded/skipped = %v/%v, want []/[wasi_sdk wamrc]", out2.Downloaded, out2.Skipped)
	}
	if n := ps.sdkHits.Load(); n != 1 || ps.wamrcHits.Load() != 1 {
		t.Fatalf("asset hits after second = %d/%d, want 1/1 (idempotent skip downloads nothing)", n, ps.wamrcHits.Load())
	}
	state2, _ := os.ReadFile(statePath)
	if !bytes.Equal(state1, state2) {
		t.Fatal("state file changed across an all-skip fetch")
	}
}

// TestToolchainFetchIndexURLGate (brief-named): file:// index URLs
// are refused client-side, AND an index-picked ASSET url with a
// non-http(s) scheme re-fails the same gate (driver_fetch fix1's
// threat model, asset twin) — both with zero cache residue.
func TestToolchainFetchIndexURLGate(t *testing.T) {
	cache := tcCacheEnv(t)
	_, err := tcFetchRun(tcFetchIn{IndexURL: "file:///tmp/nope.json"})
	tcAPIErr(t, err, "http(s)")
	tcNoResidue(t, cache)

	plat := tcHostPlat(t)
	sdk := tcSDKTar(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/index", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(tcTestIndex(t, plat,
			&tcAsset{URL: "file:///etc/passwd", SHA256: sha256hex(sdk), Bytes: int64(len(sdk))},
			nil))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	_, err = tcFetchRun(tcFetchIn{IndexURL: srv.URL + "/index"})
	tcAPIErr(t, err, "http(s)") // the ASSET url's gate, not the index's
	if !strings.Contains(err.Error(), "wasi_sdk") {
		t.Fatalf("refusal %v: want it to name the offending asset kind", err)
	}
	tcNoResidue(t, cache)
}

// TestToolchainFetchConcurrentTempUnique (brief-named): two fetches
// racing on one cache root — both succeed (the random .tmp-fetch-*
// staging dirs never collide), and the final cache is coherent: the
// exact entry set (state file + the two piece trees), correct pins,
// no staging leftovers.
func TestToolchainFetchConcurrentTempUnique(t *testing.T) {
	plat := tcHostPlat(t)
	ps := tcPairServerStart(t, plat, 40*time.Millisecond, 0) // slow serves → real overlap
	cache := tcCacheEnv(t)

	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = tcFetchRun(tcFetchIn{IndexURL: ps.URL + "/index"})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent fetch %d: %v", i, err)
		}
	}

	st, err := loadTCState(filepath.Join(cache, toolchainStateFile))
	if err != nil {
		t.Fatalf("final state: %v", err)
	}
	if st.Generation != "toolchain-v2" || len(st.Installed) != 2 {
		t.Fatalf("final state = %+v, want toolchain-v2 with both pieces", st)
	}
	if rec := st.Installed["wasi_sdk"]; rec.SHA256 != ps.sdkSHA || rec.Dir != "wasi-sdk-24.0-"+plat {
		t.Fatalf("final wasi_sdk record = %+v", rec)
	}
	if rec := st.Installed["wamrc"]; rec.SHA256 != ps.wamrcSHA || rec.Dir != "wamrc/wamrc" {
		t.Fatalf("final wamrc record = %+v", rec)
	}

	// exact entry set: the two trees + the state file — no duplicate
	// dirs, no .tmp-fetch-* residue.
	ents, err := os.ReadDir(cache)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	got := map[string]bool{}
	for _, e := range ents {
		got[e.Name()] = true
	}
	want := map[string]bool{"toolchain-state.json": true, "wasi-sdk-24.0-" + plat: true, "wamrc": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("final cache entries = %v, want exactly %v", got, want)
	}
	if _, err := os.Stat(filepath.Join(cache, "wamrc", "wamrc")); err != nil {
		t.Fatalf("final wamrc binary: %v", err)
	}
}

// TestToolchainFetchTBDAssetRefused: an index entry with placeholder
// pins (the embedded snapshot's pre-T6 "TBD-BY-T6"/0-bytes shape, and
// the general bytes=0 case) is an EXPLICIT refusal — 0 bytes must
// never pass the fetch's sha/bytes對拍.
func TestToolchainFetchTBDAssetRefused(t *testing.T) {
	plat := tcHostPlat(t)
	for _, tc := range []struct {
		asset tcAsset
	}{
		{tcAsset{URL: "http://x/sdk", SHA256: "TBD-BY-T6", Bytes: 0}},
		{tcAsset{URL: "http://x/sdk", SHA256: strings.Repeat("a", 64), Bytes: 0}},
	} {
		cache := tcCacheEnv(t)
		mux := http.NewServeMux()
		mux.HandleFunc("/index", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(tcTestIndex(t, plat, &tc.asset, nil))
		})
		srv := httptest.NewServer(mux)
		_, err := tcFetchRun(tcFetchIn{IndexURL: srv.URL + "/index"})
		srv.Close()
		tcAPIErr(t, err, "index entry incomplete (sha pending T6)")
		tcNoResidue(t, cache)
	}
}

// TestToolchainFetchNoWamrcLeg: a generation with NO wamrc asset for
// this platform (the mac/win reality until T8) installs wasi_sdk
// alone and says so in the note — T2 status's symmetric honesty.
func TestToolchainFetchNoWamrcLeg(t *testing.T) {
	plat := tcHostPlat(t)
	sdk := tcSDKTar(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/index", func(w http.ResponseWriter, r *http.Request) {
		body := bytes.ReplaceAll(tcTestIndex(t, plat,
			&tcAsset{URL: "__SDK__", SHA256: sha256hex(sdk), Bytes: int64(len(sdk))},
			nil), []byte("__SDK__"), []byte("http://"+r.Host+"/sdk"))
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/sdk", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(sdk) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	cache := tcCacheEnv(t)

	out, err := tcFetchRun(tcFetchIn{IndexURL: srv.URL + "/index"})
	if err != nil {
		t.Fatalf("no-wamrc-leg fetch: %v", err)
	}
	if !reflect.DeepEqual(out.Downloaded, []string{"wasi_sdk"}) || len(out.Skipped) != 0 {
		t.Fatalf("downloaded/skipped = %v/%v, want [wasi_sdk]/[]", out.Downloaded, out.Skipped)
	}
	if !strings.Contains(out.Note, "wamrc") {
		t.Fatalf("note = %q, want it to explain the missing wamrc leg", out.Note)
	}

	st, err := loadTCState(filepath.Join(cache, toolchainStateFile))
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if len(st.Installed) != 1 {
		t.Fatalf("installed = %+v, want wasi_sdk only", st.Installed)
	}
	if _, err := os.Stat(filepath.Join(cache, "wamrc")); !os.IsNotExist(err) {
		t.Fatalf("no wamrc dir may exist without a leg (stat err = %v)", err)
	}
}

// TestToolchainFetchGenerationSelection: generation omitted = the
// index's latest; an explicit known generation is honored (and a
// repeat against the same cache is all-Skipped); an unknown
// generation is refused listing what the index does have.
func TestToolchainFetchGenerationSelection(t *testing.T) {
	plat := tcHostPlat(t)
	ps := tcPairServerStart(t, plat, 0, 0)
	tcCacheEnv(t)

	out, err := tcFetchRun(tcFetchIn{IndexURL: ps.URL + "/index"}) // omitted → latest
	if err != nil || out.Generation != "toolchain-v2" {
		t.Fatalf("omitted generation: %v, %+v", err, out)
	}
	out, err = tcFetchRun(tcFetchIn{Generation: "toolchain-v2", IndexURL: ps.URL + "/index"})
	if err != nil {
		t.Fatalf("explicit generation: %v", err)
	}
	if len(out.Downloaded) != 0 || !reflect.DeepEqual(out.Skipped, []string{"wasi_sdk", "wamrc"}) {
		t.Fatalf("explicit repeat: downloaded/skipped = %v/%v, want all-skipped", out.Downloaded, out.Skipped)
	}

	_, err = tcFetchRun(tcFetchIn{Generation: "toolchain-zz", IndexURL: ps.URL + "/index"})
	tcAPIErr(t, err, "toolchain-v2") // the refusal lists what the index has
}

// TestTCFetchTimeoutClamp: timeout_sec clamping — <=0 (unset or
// nonsense-negative) reads as the 600s default, values above 3600
// clamp down to 3600.
func TestTCFetchTimeoutClamp(t *testing.T) {
	for _, c := range []struct{ in, want int }{
		{-5, 600}, {0, 600}, {1, 1}, {599, 599}, {600, 600}, {3600, 3600}, {3601, 3600}, {7200, 3600},
	} {
		if got := tcClampTimeoutSec(c.in); got != c.want {
			t.Errorf("tcClampTimeoutSec(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestExtractTarGzBoundedWrite (T3 review M1): the extract cap bounds
// the WRITE, not just the after-the-fact total — a member past the
// remaining budget is cut AT the budget on disk (never fully landed)
// and the extraction refuses immediately; later members never
// extract. Exactly-at-cap still passes (a ceiling, not an off-by-one).
func TestExtractTarGzBoundedWrite(t *testing.T) {
	old := tcExtractMax // test seam: the var exists so this can lower it
	tcExtractMax = 1024
	t.Cleanup(func() { tcExtractMax = old })
	base := t.TempDir()

	// 600 + 600 bytes of members against a 1024-byte cap: member b
	// must be cut at the remaining budget (425 = 1024-600+1) and the
	// whole extraction refused.
	past := tcTarGz(t,
		tcEnt{name: "top", dir: true, mode: 0o755},
		tcEnt{name: "top/a", body: strings.Repeat("a", 600), mode: 0o644},
		tcEnt{name: "top/b", body: strings.Repeat("b", 600), mode: 0o644},
	)
	if err := extractTarGz(bytes.NewReader(past), filepath.Join(base, "x")); err == nil || !strings.Contains(err.Error(), "extraction cap") {
		t.Fatalf("past-cap archive: want the extraction-cap refusal, got %v", err)
	}
	b, berr := os.ReadFile(filepath.Join(base, "x", "top", "b"))
	if berr != nil {
		t.Fatalf("cut member b: %v", berr)
	}
	if int64(len(b)) > tcExtractMax-600+1 {
		t.Fatalf("member b landed %d bytes; the bounded write must cut at the remaining budget %d", len(b), tcExtractMax-600+1)
	}

	// exactly at cap: one member of exactly cap bytes passes...
	at := tcTarGz(t, tcEnt{name: "c", body: strings.Repeat("c", 1024), mode: 0o644})
	if err := extractTarGz(bytes.NewReader(at), filepath.Join(base, "y")); err != nil {
		t.Fatalf("at-cap archive: want pass, got %v", err)
	}
	// ...one byte over is refused.
	over := tcTarGz(t, tcEnt{name: "d", body: strings.Repeat("d", 1025), mode: 0o644})
	if err := extractTarGz(bytes.NewReader(over), filepath.Join(base, "z")); err == nil || !strings.Contains(err.Error(), "extraction cap") {
		t.Fatalf("one-byte-over archive: want the extraction-cap refusal, got %v", err)
	}
}

// tcSDKTarNoBin is the wasi-sdk shape WITHOUT bin/ — the M3 fixture:
// a "sdk" tarball whose top dir holds only share/ data, no compiler.
func tcSDKTarNoBin(t *testing.T) []byte {
	return tcTarGz(t,
		tcEnt{name: "wasi-sdk-24.0-x86_64-linux", dir: true, mode: 0o755},
		tcEnt{name: "wasi-sdk-24.0-x86_64-linux/share/misc.txt", body: "share\n", mode: 0o644},
	)
}

// TestToolchainFetchSDKLayoutGate (T3 review M3): a wasi-sdk tarball
// that lands without bin/ under its root is refused at the staging
// door — "unexpected wasi-sdk layout" — with zero cache residue (the
// sha pin cannot see SHAPE; wamrc's no-binary check, sdk twin).
func TestToolchainFetchSDKLayoutGate(t *testing.T) {
	plat := tcHostPlat(t)
	sdk := tcSDKTarNoBin(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/index", func(w http.ResponseWriter, r *http.Request) {
		body := bytes.ReplaceAll(tcTestIndex(t, plat,
			&tcAsset{URL: "__SDK__", SHA256: sha256hex(sdk), Bytes: int64(len(sdk))},
			nil), []byte("__SDK__"), []byte("http://"+r.Host+"/sdk"))
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/sdk", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(sdk) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	cache := tcCacheEnv(t)

	_, err := tcFetchRun(tcFetchIn{IndexURL: srv.URL + "/index"})
	tcAPIErr(t, err, "unexpected wasi-sdk layout")
	tcNoResidue(t, cache)
}
