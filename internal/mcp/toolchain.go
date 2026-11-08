package mcp

// toolchain.go — the toolchain face (B11 tasks 1-2, design §6.1/§6.3):
// task 1 laid the data foundations — the toolchain index types (the
// driver fetch's index, toolchain twin — same index_ver gate
// discipline), the embedded known-good snapshot (the fallback when the
// remote index is unreachable, and the offline pin — a compile-time
// constant the design's fallback reads instead of the network), and
// the local installed-state file (~/.cache/xuansi-wasm/
// toolchain-state.json — the pairing check's local half: which
// generation this cache installed, and each landed piece's pinned
// sha). Task 2 adds the read-only status face below: local installed
// state x index latest in one view, missing pieces self-guiding to
// xuansi_toolchain_fetch.
//
// Placeholder discipline: the snapshot mirrors the drivers repo's
// index FIRST VERSION; task 6 cross-checks it against the release
// manifest and fills the real asset values. The TBD-BY-T6 sha256
// placeholders (and 0 bytes) are deliberately harmless here — the
// parse layer gates the version only, and asset verification is the
// fetch tool's job (task 3), so a placeholder can never pass a fetch.

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
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

// Concurrency handoff (B11 task 2, T1 review M2): loadTCState and
// saveTCState themselves take no lock — the TOOL layer owns every
// load→mutate→save cycle, and each such critical section runs under
// tcMu (declared in the status section below): task 3's fetch holds it
// exclusively across its whole cycle, status shares it for its local
// read (load + existence probes) so a status never interleaves with a
// fetch's commit phase. The network stays OUTSIDE the lock.
//
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

// ---------------------------------------------------------------------------
// xuansi_toolchain_status (B11 task 2 — the toolchain face's read-only
// half, spec §6.3: local installed state x index latest x pairing
// matrix in one view; missing pieces self-guide to fetch)

// defaultToolchainIndexURL is the toolchain index's home — the drivers
// repo, the driver index's twin hub (plan task 6 lands the file). The
// embedded snapshot above is its known-good shadow.
const defaultToolchainIndexURL = "https://raw.githubusercontent.com/wan-cloud/xuansi-drivers/main/toolchain-index.json"

// toolchainStateFile is the state file's name inside the cache root
// (design §6.1: ~/.cache/xuansi-wasm/toolchain-state.json).
const toolchainStateFile = "toolchain-state.json"

// tcMu serializes the toolchain cache's state-file cycles at the tool
// layer (T1 review M2 — the load/save helpers are deliberately
// lock-free): task 3's fetch wraps its WHOLE loadTCState→mutate→
// saveTCState critical section in it, and status takes it for its
// local read (state load + existence probes) so a status never
// interleaves with a fetch's commit. Local reads are fast; network
// fetches stay outside the lock.
var tcMu sync.Mutex

// tcCacheRoot is the toolchain cache root (design §6.1):
// XUANSI_WASM_CACHE overrides (tests, non-standard hosts), else
// os.UserCacheDir()/xuansi-wasm. "" = no root available at all (no
// override and UserCacheDir refused) — the tool layer turns that into
// an actionable refusal rather than guessing a relative path.
func tcCacheRoot() string {
	if d := os.Getenv("XUANSI_WASM_CACHE"); d != "" {
		return d
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(base, "xuansi-wasm")
}

// tcOSNames/tcArchNames translate Go's runtime names into the index's
// platform-key vocabulary — the index says "macos" where Go says
// "darwin", "x64" where Go says "amd64". Off-matrix values map to ""
// (honest: no asset can exist for this host).
var tcOSNames = map[string]string{"linux": "linux", "darwin": "macos", "windows": "win"}
var tcArchNames = map[string]string{"amd64": "x64", "arm64": "arm64"}

// tcPlatKey names goos/goarch the index's way ("linux-x64" &c.); ""
// means the host is off the matrix entirely.
func tcPlatKey(goos, goarch string) string {
	os, arch := tcOSNames[goos], tcArchNames[goarch]
	if os == "" || arch == "" {
		return ""
	}
	return os + "-" + arch
}

// tcPathExists is the present-check's existence probe — file or
// directory, no content validation: the fetch pinned the bytes at
// install time (state's sha), and re-verifying installed trees is not
// status's job.
func tcPathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

type tcStatusIn struct {
	IndexURL string `json:"index_url,omitempty" jsonschema:"the toolchain-index.json URL; omit for the default https://raw.githubusercontent.com/wan-cloud/xuansi-drivers/main/toolchain-index.json (http(s) only - file:// and other schemes are refused client-side)"`
}

// tcInstalledOut is one piece's status row. Present = the state file
// records the piece AND its directory/file still exists under
// cache_root (a record whose files vanished is a broken cache — the
// row says so by carrying the pin without present). The pins ride
// whenever a record exists: they say exactly what a re-fetch must
// reproduce.
type tcInstalledOut struct {
	Kind    string `json:"kind" jsonschema:"wasi_sdk or wamrc"`
	Present bool   `json:"present" jsonschema:"true when toolchain-state.json records the piece AND its directory/file still exists under cache_root"`
	SHA256  string `json:"sha256,omitempty" jsonschema:"the pinned sha256 the state recorded (carried whenever a record exists, present or not)"`
	Bytes   int64  `json:"bytes,omitempty" jsonschema:"the pinned byte count the state recorded"`
}

// tcIndexOut reports which index answered: the fetched remote one, or
// the daemon's embedded snapshot (served when the remote index is
// unreachable — a note then says why, and latest is what THIS daemon
// knows, not necessarily what exists upstream).
type tcIndexOut struct {
	Source string `json:"source" jsonschema:"remote (the fetched index) or embedded (the daemon's compiled-in snapshot)"`
	Latest string `json:"latest" jsonschema:"the generation the answered index says is latest"`
	Note   string `json:"note,omitempty" jsonschema:"non-empty when source=embedded: why the remote index did not answer"`
}

type tcStatusOut struct {
	CacheRoot  string           `json:"cache_root" jsonschema:"the toolchain cache root (XUANSI_WASM_CACHE override > os.UserCacheDir()/xuansi-wasm)"`
	Generation string           `json:"generation" jsonschema:"the generation toolchain-state.json says this cache installed (empty = fresh cache, nothing installed)"`
	Installed  []tcInstalledOut `json:"installed" jsonschema:"one row per piece (wasi_sdk, wamrc): state-recorded AND on disk"`
	Index      tcIndexOut       `json:"index"`
	Guidance   string           `json:"guidance,omitempty" jsonschema:"non-empty when something is missing or stale: the next step, always naming xuansi_toolchain_fetch when installing helps"`
}

// toolchainStatus answers "can I build wasm here, and if not, what do
// I call next" in one read-only shot: no lease, no daemon crossing.
func (ts *toolset) toolchainStatus(ctx context.Context, _ *mcpsdk.CallToolRequest, in tcStatusIn) (*mcpsdk.CallToolResult, tcStatusOut, error) {
	// index URL gate, driverFetch's shape exactly: omitted = the
	// default, explicit = must parse to http(s) with a host (file://
	// and friends refused before any request is attempted).
	raw := in.IndexURL
	if raw == "" {
		raw = defaultToolchainIndexURL
	}
	iu, uerr := url.Parse(raw)
	if uerr != nil || (iu.Scheme != "http" && iu.Scheme != "https") || iu.Host == "" {
		return nil, tcStatusOut{}, errWs("xuansi_toolchain_status: index_url must be an http(s) URL, got %q", raw)
	}

	// The index: remote first. UNREACHABLE degrades to the embedded
	// snapshot with a note — a status answer must not die on the
	// network; the offline pin exists for exactly this (design §6.1).
	// A body that arrives but fails the ver gate REFUSES instead:
	// that is not unavailability, it is the gate's hard signal (a
	// newer index needs a newer daemon) and status must not paper
	// over it by silently serving the older snapshot's "latest".
	idx, src, note := embeddedToolchainIndex(), "embedded", ""
	if body, gerr := fetchGet(ctx, iu.String(), fetchIndexMax); gerr != nil {
		note = fmt.Sprintf("remote index unreachable (%v) — serving the daemon's embedded snapshot; latest is what THIS daemon knows, not necessarily what exists upstream", gerr)
	} else {
		ridx, perr := parseToolchainIndex(body)
		if perr != nil {
			return nil, tcStatusOut{}, errWs("xuansi_toolchain_status: index %s: %v", raw, perr)
		}
		idx, src = ridx, "remote"
	}

	root := tcCacheRoot()
	if root == "" {
		return nil, tcStatusOut{}, errWs("xuansi_toolchain_status: no toolchain cache root (XUANSI_WASM_CACHE unset and os.UserCacheDir failed) — set XUANSI_WASM_CACHE")
	}
	plat := tcPlatKey(runtime.GOOS, runtime.GOARCH)

	// The local half, one coherent view under tcMu (shared with task
	// 3's future write cycles): the state read plus the existence
	// probes, so a status never sees a half-committed cache.
	out := tcStatusOut{CacheRoot: root, Index: tcIndexOut{Source: src, Latest: idx.Latest, Note: note}, Installed: make([]tcInstalledOut, 0, 2)}
	var missing []string
	tcMu.Lock()
	st, err := loadTCState(filepath.Join(root, toolchainStateFile))
	if err != nil {
		tcMu.Unlock()
		return nil, tcStatusOut{}, errWs("xuansi_toolchain_status: %v", err)
	}
	out.Generation = st.Generation
	// gen: the cache's OWN generation (the wasi-sdk dir on disk was
	// laid out by it — unknown when the cache is fresh). target/tgen:
	// what a fetch would install here — the cache's generation when it
	// holds one (upgrade/replace), else the index's latest (a fresh
	// fetch installs latest by default); the guidance's "is there an
	// asset for me" question belongs to the TARGET, not the cache.
	gen, genKnown := idx.Generations[st.Generation] // zero tcGen when the cache is fresh or the generation is unknown to the served index
	target := st.Generation
	if target == "" {
		target = idx.Latest
	}
	tgen, tknown := idx.Generations[target]
	for _, kind := range [...]string{"wasi_sdk", "wamrc"} {
		rec, recorded := st.Installed[kind]
		present := false
		switch kind {
		case "wasi_sdk":
			// dir wasi-sdk-<generation's pinned sdk>-<plat key> — the
			// §6.1 layout, the platform key in the INDEX's vocabulary
			if recorded && genKnown && plat != "" {
				present = tcPathExists(filepath.Join(root, "wasi-sdk-"+gen.WasiSDK+"-"+plat))
			}
		case "wamrc":
			present = recorded && tcPathExists(filepath.Join(root, "wamrc", "wamrc"))
		}
		row := tcInstalledOut{Kind: kind, Present: present}
		if recorded {
			row.SHA256, row.Bytes = rec.SHA256, rec.Bytes
		}
		out.Installed = append(out.Installed, row)
		if !present {
			missing = append(missing, kind)
		}
	}
	tcMu.Unlock()

	// Guidance (spec §6.3 UX): every gap names its next step; a
	// complete, current cache stays quiet (guidance omitted).
	var g []string
	if plat == "" {
		g = append(g, fmt.Sprintf("this host (%s/%s) has no platform key in the index matrix — no toolchain asset can install here", runtime.GOOS, runtime.GOARCH))
	}
	if st.Generation != "" && !genKnown {
		g = append(g, fmt.Sprintf("cache generation %q is not in the served index (source=%s) — call xuansi_toolchain_fetch to replace it with a known generation", st.Generation, src))
	}
	for _, kind := range missing {
		hasAsset := false
		if tknown && plat != "" {
			p := tgen.Platforms[plat]
			switch kind {
			case "wasi_sdk":
				hasAsset = p.WasiSDK != nil
			case "wamrc":
				hasAsset = p.Wamrc != nil
			}
		}
		if hasAsset {
			g = append(g, fmt.Sprintf("%s missing — install: call xuansi_toolchain_fetch", kind))
		} else {
			g = append(g, fmt.Sprintf("%s missing and this index generation carries no %s asset — nothing to install yet (the platform matrix grows with later toolchain releases)", kind, plat))
		}
	}
	if st.Generation != "" && genKnown && st.Generation != idx.Latest {
		g = append(g, fmt.Sprintf("cache holds %s, index latest is %s — call xuansi_toolchain_fetch to upgrade", st.Generation, idx.Latest))
	}
	out.Guidance = strings.Join(g, "; ")
	return nil, out, nil
}
