package mcp

// toolchain_test.go — B11 task 1's toolchain-face foundations: the
// index ver gate (index_ver!=1 refuses — a newer index needs a newer
// daemon, the driver fetch's fetchIndex gate discipline, toolchain
// twin), the embedded known-good snapshot (parses past the gate and
// mirrors the drivers repo's index first version: the toolchain-v2
// generation, five-platform wasi-sdk matrix, wamrc on linux-x64 only),
// and the local state round trip (save→load equal over the atomic
// writeDurableFile, a bad path's error crossing untouched, missing
// file = fresh cache, corrupt file = refusal).

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

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
			"wasi_sdk": {SHA256: "c6c38aab56e5de88adf6c1ebc9c3ae8da72f88ec2b656fb024eda8d4167a0bc5", Bytes: 118879731},
			"wamrc":    {SHA256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", Bytes: 4096},
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
