package mcp

// toolchain.go — the toolchain face's data foundations (B11 task 1,
// design §6.1): the toolchain index types (the driver fetch's index,
// toolchain twin — same index_ver gate discipline), the embedded
// known-good snapshot (the fallback when the remote index is
// unreachable, and the offline pin — a compile-time constant the
// design's fallback reads instead of the network), and the local
// installed-state file (~/.cache/xuansi-wasm/toolchain-state.json —
// the pairing check's local half: which generation this cache
// installed, and each landed piece's pinned sha).
//
// Placeholder discipline: the snapshot mirrors the drivers repo's
// index FIRST VERSION; task 6 cross-checks it against the release
// manifest and fills the real asset values. The TBD-BY-T6 sha256
// placeholders (and 0 bytes) are deliberately harmless here — the
// parse layer gates the version only, and asset verification is the
// fetch tool's job (task 3), so a placeholder can never pass a fetch.

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
)

//go:embed toolchain_snapshot.json
var toolchainSnapshotJSON []byte

// toolchainIndex is the whole toolchain index (design §6.1 schema):
// the version gate field, the generation the face should install by
// default, and one entry per toolchain generation. Structurally the
// fetchIndex discipline (tools_static.go): a versioned remote
// document the daemon parses defensively, never trusts past its gate.
type toolchainIndex struct {
	IndexVer    int              `json:"index_ver"`
	Latest      string           `json:"latest"`
	Generations map[string]tcGen `json:"generations"`
}

// tcGen is one toolchain generation: how its wamrc was built, the fw
// compatibility domains per artifact track, the wasi-sdk version it
// pins, and the per-platform asset matrix. FwCompat is SPLIT-TRACK
// (design §6.2): "wasm" carries a wide semver domain (bytecode is
// version-less), "aot" an empty string "" = EMPTY domain — the P1 v2
// honest refusal (the current fw stack cannot load .aot; the domain
// opens with the A1 runtime).
type tcGen struct {
	Kind      string            `json:"kind"`      // "stock-wamrc" (v2) / "fork-surgery" (v3, P2)
	FwCompat  map[string]string `json:"fw_compat"` // per-track domains; "" (or a missing key) = empty domain
	WasiSDK   string            `json:"wasi_sdk"`  // pinned wasi-sdk version ("24.0")
	Platforms map[string]tcPlat `json:"platforms"` // linux-x64 / linux-arm64 / macos-x64 / macos-arm64 / win-x64
}

// tcPlat is one platform's asset pair. WasiSDK is always present (the
// official wasi-sdk ships all five platforms); Wamrc is nil wherever
// the wamrc matrix has no leg yet — an honest null, not a missing
// file (P1: linux-x64 only; mac lands with task 8, win is
// best-effort).
type tcPlat struct {
	WasiSDK *tcAsset `json:"wasi_sdk"`
	Wamrc   *tcAsset `json:"wamrc"`
}

// tcAsset is one pinned artifact: where to fetch it, the sha256 it
// must hash to, and its exact size — the fetch's pin discipline
// (bit-drift refusal) in data form.
type tcAsset struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// parseToolchainIndex decodes one index body, gating index_ver: only
// 1 parses (the fetchIndex gate, toolchain twin) — a NEWER index
// needs a newer daemon, and this face never shipped an older shape.
// Asset fields are NOT validated here: the fetch tool verifies
// sha256/bytes against what it downloads, which keeps TBD placeholders
// (pre-task-6 snapshot sync) and forward-compatible extra fields
// harmless to every reader.
func parseToolchainIndex(b []byte) (*toolchainIndex, error) {
	var ix toolchainIndex
	if err := json.Unmarshal(b, &ix); err != nil {
		return nil, errWs("toolchain index: %v", err)
	}
	if ix.IndexVer != 1 {
		return nil, errWs("toolchain index: index_ver %d unsupported (this daemon speaks 1) — a newer index needs a newer daemon", ix.IndexVer)
	}
	return &ix, nil
}

// embeddedToolchainIndex returns the embedded known-good snapshot,
// parsed FRESH per call so every caller owns its copy (no shared
// mutable maps across the face; the body is ~2 KiB, the parse is
// noise). The snapshot is compiled in, so a parse failure here is a
// build bug, not a runtime condition — it panics loudly at first use,
// and TestEmbeddedSnapshotParses pins it green in every test run.
func embeddedToolchainIndex() *toolchainIndex {
	ix, err := parseToolchainIndex(toolchainSnapshotJSON)
	if err != nil {
		panic(fmt.Sprintf("toolchain_snapshot.json no longer parses: %v", err))
	}
	return ix
}

// tcInstalled is one landed piece's record — the pairing check's
// local evidence: the sha the piece was verified against at fetch
// time (before anything was written), and its size. Keyed by kind
// ("wasi_sdk" / "wamrc") in tcState.Installed.
type tcInstalled struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// tcState is the whole toolchain-state.json: the ONE generation this
// cache installed, and per-piece records. One generation per cache
// keeps the pairing question binary (does the cache's generation
// match what the build gate demands?) — installing a different
// generation replaces the state whole.
type tcState struct {
	Generation string                 `json:"generation"`
	Installed  map[string]tcInstalled `json:"installed"`
}

// loadTCState reads toolchain-state.json. A missing file is a fresh
// cache (empty state, maps non-nil for caller mutation — loadManifest's
// fresh-workspace read, state twin); a corrupt one REFUSES rather than
// being treated as empty and silently overwritten on the next save —
// the state is the pairing check's evidence, not a disposable cache
// entry (loadManifest's corrupt-refusal discipline, same reasoning).
func loadTCState(path string) (*tcState, error) {
	b, err := readFileShared(path) // share-delete read: must not block a concurrent atomic replace
	if os.IsNotExist(err) {
		return &tcState{Installed: map[string]tcInstalled{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var s tcState
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("toolchain state %s: %w", path, err)
	}
	if s.Installed == nil {
		s.Installed = map[string]tcInstalled{}
	}
	return &s, nil
}

// saveTCState writes s atomically — the same temp+fsync+rename+
// dir-fsync writeDurableFile performs (writeManifestAtomic's twin):
// a concurrent reader ever only sees the whole old or whole new
// state, never a torn pairing record, and a crash mid-write leaves
// the previous state intact. Raw-error helper: the tool layer wraps
// with errWs, exactly as the workspace tools wrap loadManifest.
func saveTCState(path string, s *tcState) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return writeDurableFile(path, b, 0o600)
}
