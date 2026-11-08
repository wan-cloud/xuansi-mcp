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
	"testing"
	"time"

	"xuansi/internal/server"
)

// TestParseToolchainIndexVerGate pins the version gate: index_ver 2 is
// refused as an api-class error (the whole toolchain face refuses with
// errWs, never a bare fmt error), and index_ver 1 parses with the
// fw_compat SPLIT TRACKS intact — "aot" as the EMPTY domain ("") is
// the P1 v2 honest-refusal semantic every downstream gate reads.
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
				"fw_compat": {"wasm": ">=0.2.0", "aot": ""},
				"wasi_sdk": "24.0",
				"platforms": {}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("index_ver 1: want pass, got %v", err)
	}
	gen := ix.Generations["toolchain-v2"]
	if gen.FwCompat["aot"] != "" {
		t.Fatalf("fw_compat aot: want empty-domain \"\", got %q", gen.FwCompat["aot"])
	}
	if gen.FwCompat["wasm"] != ">=0.2.0" {
		t.Fatalf("fw_compat wasm: want \">=0.2.0\", got %q", gen.FwCompat["wasm"])
	}
}

// TestEmbeddedSnapshotParses pins the embedded snapshot's shape — the
// mirror T6 cross-checks the drivers repo's index first version
// against. The four non-linux platforms' sha256/bytes ride TBD-BY-T6
// placeholders (the parse layer deliberately does not validate asset
// fields — verification is the fetch tool's job), so this test pins
// URLs (real, official) but not those placeholder values; linux-x64
// carries its already-known real pins.
func TestEmbeddedSnapshotParses(t *testing.T) {
	ix := embeddedToolchainIndex()
	if ix.IndexVer != 1 { // trivially true past the gate; pins that the gate ran
		t.Fatalf("embedded index_ver: want 1, got %d", ix.IndexVer)
	}
	if ix.Latest != "toolchain-v2" {
		t.Fatalf("embedded latest: want toolchain-v2, got %q", ix.Latest)
	}
	gen, ok := ix.Generations["toolchain-v2"]
	if !ok {
		t.Fatal("embedded generations: toolchain-v2 missing")
	}
	if gen.Kind != "stock-wamrc" || gen.WasiSDK != "24.0" {
		t.Fatalf("embedded gen head: want stock-wamrc/24.0, got %q/%q", gen.Kind, gen.WasiSDK)
	}
	if gen.FwCompat["aot"] != "" || gen.FwCompat["wasm"] != ">=0.2.0" {
		t.Fatalf("embedded fw_compat: want wasm >=0.2.0 + empty aot, got %v", gen.FwCompat)
	}

	// Five platform keys, each with a wasi-sdk asset at its official
	// URL (arm64 — NOT aarch64 — is the linux asset name; the exact
	// spot a wrong guess would plant a dead URL only T6 would catch).
	wantURLs := map[string]string{
		"linux-x64":   "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-x86_64-linux.tar.gz",
		"linux-arm64": "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-arm64-linux.tar.gz",
		"macos-x64":   "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-x86_64-macos.tar.gz",
		"macos-arm64": "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-arm64-macos.tar.gz",
		"win-x64":     "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-24/wasi-sdk-24.0-x86_64-windows.tar.gz",
	}
	if len(gen.Platforms) != len(wantURLs) {
		t.Fatalf("embedded platforms: want %d keys, got %d (%v)", len(wantURLs), len(gen.Platforms), gen.Platforms)
	}
	for plat, url := range wantURLs {
		p, ok := gen.Platforms[plat]
		if !ok {
			t.Fatalf("embedded platforms: %s missing", plat)
		}
		if p.WasiSDK == nil || p.WasiSDK.URL != url {
			t.Fatalf("embedded %s wasi_sdk: want URL %s, got %+v", plat, url, p.WasiSDK)
		}
	}

	// The one fully-pinned asset: linux-x64 wasi-sdk carries real
	// sha256 + bytes (the numbers T6 must reproduce byte-for-byte).
	if ws := gen.Platforms["linux-x64"].WasiSDK; ws.SHA256 != "c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5" || ws.Bytes != 118879731 {
		t.Fatalf("embedded linux-x64 wasi_sdk pins: got %+v", ws)
	}

	// wamrc matrix: linux-x64 only in P1 (mac legs land with T8, win is
	// best-effort); every other platform's slot is an honest null.
	if gen.Platforms["linux-x64"].Wamrc == nil {
		t.Fatal("embedded linux-x64 wamrc: want asset, got null")
	}
	// T1 review M1: the linux-x64 wamrc URL pinned verbatim — the one
	// self-built asset the P1 fetch downloads; a guessed URL would
	// plant a dead release link only task 6's cross-check would catch.
	if w := gen.Platforms["linux-x64"].Wamrc; w.URL != "https://github.com/wan-cloud/xuansi-drivers/releases/download/toolchain-v2/wamrc-2.4.0-xtensa-18.1.2-linux-x64.tar.gz" {
		t.Fatalf("embedded linux-x64 wamrc URL = %q, want the toolchain-v2 release URL verbatim", w.URL)
	}
	for _, plat := range []string{"linux-arm64", "macos-x64", "macos-arm64", "win-x64"} {
		if gen.Platforms[plat].Wamrc != nil {
			t.Fatalf("embedded %s wamrc: want null (P1 matrix), got %+v", plat, gen.Platforms[plat].Wamrc)
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
		Generation: "toolchain-v2",
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
	if out.Index.Source != "remote" || out.Index.Latest != "toolchain-v2" || out.Index.Note != "" {
		t.Fatalf("index = %+v, want remote / toolchain-v2 / no note", out.Index)
	}
	if out.Generation != "toolchain-v2" {
		t.Fatalf("generation = %q, want the state's toolchain-v2", out.Generation)
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
	if out.Index.Source != "embedded" || out.Index.Latest != "toolchain-v2" {
		t.Fatalf("index = %+v, want embedded / toolchain-v2 (the snapshot's values)", out.Index)
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
}

// tcTarGz builds a REAL (small) tar.gz in-process — stdlib archive/tar
// + compress/gzip, no disk fixtures, exactly the shapes the extract
// path must handle (dir members, exec-bit files, nested paths).
func tcTarGz(t *testing.T, ents ...tcEnt) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range ents {
		hdr := &tar.Header{Name: e.name, Mode: e.mode}
		if e.dir {
			hdr.Typeflag = tar.TypeDir
		} else {
			hdr.Typeflag = tar.TypeReg
			hdr.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("fixture tar header %q: %v", e.name, err)
		}
		if !e.dir {
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
				Kind: "stock-wamrc", FwCompat: map[string]string{"wasm": ">=0.2.0", "aot": ""},
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

// TestExtractTarGzSlip: hostile tar members — ../ traversal, absolute
// paths, backslash names, symlinks — are all refused, and nothing
// escapes the extraction root. A safe member (control) extracts fine.
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
	if fi, err := os.Stat(filepath.Join(dst, "top", "bin", "tool")); err != nil || fi.Mode().Perm()&0o111 == 0 {
		t.Fatalf("exec member must keep an exec bit, got %v, %v", fi, err)
	}

	for _, evil := range []tcEnt{
		{name: "../evil", body: "x", mode: 0o644},
		{name: "/tmp/tc-evil-abs", body: "x", mode: 0o644},
		{name: "a/../../evil2", body: "x", mode: 0o644},
		{name: `back\slash`, body: "x", mode: 0o644},
	} {
		if err := extractTarGz(bytes.NewReader(tcTarGz(t, evil)), filepath.Join(base, "slip")); err == nil {
			t.Errorf("member %q: want refusal, got nil", evil.name)
		}
	}

	// symlink member: refused (TypeSymlink is not a landable type)
	var sbuf bytes.Buffer
	sgz := gzip.NewWriter(&sbuf)
	stw := tar.NewWriter(sgz)
	if err := stw.WriteHeader(&tar.Header{Typeflag: tar.TypeSymlink, Name: "lnk", Linkname: "/etc/passwd"}); err != nil {
		t.Fatalf("fixture symlink header: %v", err)
	}
	stw.Close()
	sgz.Close()
	if err := extractTarGz(bytes.NewReader(sbuf.Bytes()), filepath.Join(base, "slip")); err == nil {
		t.Error("symlink member: want refusal, got nil")
	}

	// and nothing escaped the roots
	for _, p := range []string{"evil", "evil2", "tc-evil-abs"} {
		if _, err := os.Stat(filepath.Join(base, p)); !os.IsNotExist(err) {
			t.Errorf("member %q escaped the extraction root", p)
		}
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
	if fi, ferr := os.Stat(clang); ferr != nil || fi.Mode().Perm()&0o111 == 0 {
		t.Fatalf("sdk bin/clang: %v (mode %v) — want present + exec bit", ferr, fi)
	}
	wamrcBin := filepath.Join(cache, "wamrc", "wamrc")
	if fi, ferr := os.Stat(wamrcBin); ferr != nil || fi.Mode().Perm()&0o111 == 0 {
		t.Fatalf("wamrc/wamrc: %v (mode %v) — want present + exec bit", ferr, fi)
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
