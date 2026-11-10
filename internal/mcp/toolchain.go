package mcp

// toolchain.go — the toolchain face (B11 tasks 1-3, design §6.1/§6.3):
// task 1 laid the data foundations — the toolchain index types (the
// driver fetch's index, toolchain twin — same index_ver gate
// discipline), the embedded known-good snapshot (the fallback when the
// remote index is unreachable, and the offline pin — a compile-time
// constant the design's fallback reads instead of the network), and
// the local installed-state file (~/.cache/xuansi-wasm/
// toolchain-state.json — the pairing check's local half: which
// generation this cache installed, and each landed piece's pinned
// sha). Task 2 added the read-only status face: local installed
// state x index latest in one view, missing pieces self-guiding to
// xuansi_toolchain_fetch. Task 3 adds that write half — the fetch
// tool below: sha-pinned streaming downloads, tar extraction with
// hostile-member refusal, and an all-or-nothing staging→re-verify→
// rename→state commit whose any failure leaves the cache untouched.
//
// Placeholder discipline: the snapshot mirrored the drivers repo's
// index FIRST VERSION with TBD-BY-T6 sha256 placeholders (0 bytes) —
// deliberately harmless to status, but the FETCH tool refuses them
// explicitly (an unpinned asset must never pass the sha/bytes pin
// check), so a placeholder can never install. Task 6 cross-checked
// the wasi-sdk values; task 8 synced the snapshot to the published
// index verbatim (all five wasi_sdk entries real; wamrc real on
// linux-x64 + both mac legs — snapshot == index, byte-identical).
//
// P3-T12: the snapshot carries TWO generations — toolchain-v3 (the
// latest: kind xuansi-surgery-wamrc, the fork surgery wamrc whose
// .aot rides PSRAM exec on fw 0.3.0+; the default generation the
// wasm build's aot lane resolves via resolveAOTWamrc) alongside the
// kept toolchain-v2 (stock wamrc, the fw-internal-SRAM lane). The
// drift-lock (TestEmbeddedSnapshotDriftLock) cross-asserts every
// served pin verbatim AND the hardcoded build-face constants
// (aotFWFloor, aotDefaultGen) against the snapshot mechanically.

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

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
// version-less); "aot" has been OPEN (">=0.3.0") since P3-T7 — and
// since P3-T12 the index carries TWO generations over that domain
// with different PLACEMENT lanes: toolchain-v3 (the latest, kind
// xuansi-surgery-wamrc — the fork surgery wamrc; .aot into PSRAM
// exec) and toolchain-v2 (stock-wamrc — .aot into fw-internal SRAM
// within caps aot_exec_budget_bytes). "" (or a missing key) still
// reads as an empty domain — the honest shape for any future
// generation whose aot lane is genuinely closed.
type tcGen struct {
	Kind      string            `json:"kind"`      // "stock-wamrc" (v2) / "xuansi-surgery-wamrc" (v3, P3-T12)
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
// time (before anything was written), its size, and where it landed.
// Keyed by kind ("wasi_sdk" / "wamrc") in tcState.Installed.
type tcInstalled struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	// Dir is the piece's landing path relative to the cache root —
	// the wasi-sdk directory ("wasi-sdk-24.0-linux-x64") or the wamrc
	// binary's path ("wamrc/wamrc"). Recorded by fetch at commit time
	// (B11 task 3, T2 review MB): present is decided by the STATE's
	// own records plus a disk probe, never by the served index — a
	// pre-T3 record without Dir cannot prove its target and reads
	// not-present (a re-fetch heals it).
	Dir string `json:"dir,omitempty"`
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
// lock-free). Task 3's fetch runs TWO locked cycles — the PLAN (state
// load + idempotency probes) and the COMMIT (renames + state save) —
// with the network downloads and tar extraction in between touching
// only the random .tmp-fetch-* staging dir, outside the lock: a
// concurrent status never blocks behind a long download, and status's
// own local read (state load + existence probes) never interleaves
// with a fetch's commit. Local reads are fast; network fetches stay
// outside the lock.
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
	// genKnown answers one guidance question only (below): does the
	// served index know the cache's generation label? Present itself
	// is fully local since T2 review MB: each record carries its own
	// Dir, and the probe rides the state — the index is not needed to
	// interpret the cache's own contents. target/tgen: what a fetch
	// would install here — the cache's generation when it holds one
	// (upgrade/replace), else the index's latest (a fresh fetch
	// installs latest by default); the guidance's "is there an asset
	// for me" question belongs to the TARGET, not the cache.
	_, genKnown := idx.Generations[st.Generation]
	target := st.Generation
	if target == "" {
		target = idx.Latest
	}
	tgen, tknown := idx.Generations[target]
	for _, kind := range [...]string{"wasi_sdk", "wamrc"} {
		rec, recorded := st.Installed[kind]
		present := recorded && rec.Dir != "" && tcPathExists(filepath.Join(root, rec.Dir))
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
		g = append(g, fmt.Sprintf("cache generation %q is unknown to the index this answer served (source=%s) — call xuansi_toolchain_fetch to replace it with a known generation", st.Generation, src))
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
		switch {
		case hasAsset:
			g = append(g, fmt.Sprintf("%s missing — install: call xuansi_toolchain_fetch (manual steps: the xuansi-drivers README's User-compiled modules section)", kind))
		case plat == "":
			g = append(g, fmt.Sprintf("%s missing and this host has no index platform key, so no asset can exist for it", kind))
		default:
			g = append(g, fmt.Sprintf("%s missing and this index generation carries no %s asset — nothing to install yet (the platform matrix grows with later toolchain releases)", kind, plat))
		}
	}
	if st.Generation != "" && genKnown && st.Generation != idx.Latest {
		g = append(g, fmt.Sprintf("cache holds %s, index latest is %s — call xuansi_toolchain_fetch to upgrade", st.Generation, idx.Latest))
	}
	out.Guidance = strings.Join(g, "; ")
	return nil, out, nil
}

// ---------------------------------------------------------------------------
// xuansi_toolchain_fetch (B11 task 3 — the toolchain face's write half,
// design §6.1: sha-pinned downloads, tar extraction, and an atomic
// staging→verify→rename→state commit)

// Asset caps (brief): one archive may be up to 700 MiB (the win
// wasi-sdk ~495MB passes with headroom), all declared assets of one
// fetch together may not exceed 1.5 GiB (disk-bomb refusal), and one
// tarball may not EXPAND beyond 4 GiB on extraction (the download is
// sha-pinned, so this is defense-in-depth against a gzip bomb the
// index itself would have to carry — generous vs the ~1 GiB a real
// wasi-sdk extracts to).
const (
	tcArchiveMax      int64 = 700 << 20 // 700 MiB per archive
	tcFetchTotalMax   int64 = 3 << 29   // 1.5 GiB across one fetch's declared assets
	tcTimeoutDefaultS int   = 600       // timeout_sec default
	tcTimeoutMaxS     int   = 3600      // timeout_sec clamp ceiling
)

// tcExtractMax is the 4 GiB uncompressed-per-tarball ceiling — a var
// (its const block siblings stay const) only as the bounded-write
// test's seam, so it can lower the cap to a fixture-sized budget; no
// production path ever assigns it (T3 review M1).
var tcExtractMax int64 = 4 << 30

// tcHTTP is the toolchain assets' client — deliberately NOT
// fetchHTTP (the 60s index/artifact client): a wasi-sdk archive is
// ~120MB (linux) to ~495MB (win) and a slow link legitimately takes
// minutes. No client-level Timeout at all; the ONLY bound is the ctx
// deadline toolchainFetch derives from timeout_sec (default 600s,
// clamped to <=3600s), which cancels mid-body when it fires.
var tcHTTP = &http.Client{}

// tcClampTimeoutSec: timeout_sec <=0 (unset, or nonsense-negative)
// reads as the 600s default; anything above 3600 clamps down to it.
func tcClampTimeoutSec(sec int) int {
	if sec <= 0 {
		return tcTimeoutDefaultS
	}
	if sec > tcTimeoutMaxS {
		return tcTimeoutMaxS
	}
	return sec
}

type tcFetchIn struct {
	Generation string `json:"generation,omitempty" jsonschema:"the toolchain generation to install (e.g. \"toolchain-v3\", the latest — the surgery wamrc; \"toolchain-v2\" stays installable as the stock lane); omit for the index's latest"`
	IndexURL   string `json:"index_url,omitempty" jsonschema:"the toolchain-index.json URL; omit for the default https://raw.githubusercontent.com/wan-cloud/xuansi-drivers/main/toolchain-index.json (http(s) only - file:// and other schemes are refused client-side, and every ASSET url the index hands out re-passes the same gate)"`
	TimeoutSec int    `json:"timeout_sec,omitempty" jsonschema:"bounds the WHOLE fetch (index + every asset download), seconds; default 600, clamped to at most 3600; 0 or negative reads as the default"`
}

type tcFetchOut struct {
	Generation string   `json:"generation" jsonschema:"the generation now installed in this cache (the one asked for, or the index's latest when omitted)"`
	Downloaded []string `json:"downloaded" jsonschema:"pieces this call downloaded, sha-verified and committed (kinds: wasi_sdk, wamrc)"`
	Skipped    []string `json:"skipped" jsonschema:"pieces already installed with the same pin and left untouched (idempotent re-call)"`
	CacheRoot  string   `json:"cache_root" jsonschema:"the toolchain cache root the pieces landed under"`
	Note       string   `json:"note,omitempty" jsonschema:"non-empty when something deserves saying: the index was unreachable (the daemon's embedded snapshot answered), or this platform's generation carries no wamrc asset yet (wasi_sdk alone was installed — the matrix grows with later releases)"`
}

// tcPinComplete: a usable pin = a 64-char hex sha256 AND bytes > 0.
// The pre-T6 snapshot placeholders ("TBD-BY-T6" + 0 bytes) failed
// here ON PURPOSE — an unpinned asset must never reach the
// sha/bytes pin check (0 bytes would "pass" a 0-byte expectation);
// the T6/T8 syncs turned the entries real and this gate opened for
// them without a code change. A remote index carrying placeholders
// still gets the same refusal.
func tcPinComplete(a *tcAsset) bool {
	if len(a.SHA256) != 64 || a.Bytes <= 0 {
		return false
	}
	for _, c := range a.SHA256 {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// fetchTCArchive streams one archive from url into w while hashing:
// sha256 is computed IN the stream (memory stays flat for the ~495MB
// win asset), the byte count must equal wantBytes exactly, and the
// stream is refused past maxBytes. NOTE the zero-write contract's
// level: w is always a file inside the fetch's random .tmp-fetch-*
// staging dir, which the atomic sequence discards WHOLESALE on any
// failure — on a mismatch w may hold the bad bytes for a moment, but
// the CACHE (the rename targets, the state file) never receives one.
func fetchTCArchive(ctx context.Context, rawurl, wantSHA string, wantBytes, maxBytes int64, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if err != nil {
		return err
	}
	res, err := tcHTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", res.StatusCode)
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), io.LimitReader(res.Body, maxBytes+1))
	if err != nil {
		return err
	}
	if n > maxBytes {
		return fmt.Errorf("stream exceeds the %d-byte per-archive cap", maxBytes)
	}
	if n != wantBytes {
		return fmt.Errorf("got %d bytes, index says %d", n, wantBytes)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != strings.ToLower(wantSHA) {
		return fmt.Errorf("sha256 %s != index %s", got, wantSHA)
	}
	return nil
}

// tcFilePerm is the extraction's file mode: 0644 as the rule, with
// the tar header's exec bits propagated (0755). A blanket 0644 would
// strip the exec bit off wasi-sdk's bin/clang and the wamrc binary
// itself — both must RUN — so the header's x-bits ride; everything
// else stays non-executable.
func tcFilePerm(mode int64) os.FileMode {
	if mode&0o111 != 0 {
		return 0o755
	}
	return 0o644
}

// tcPathHostile is the tar-slip gate's shape test, shared by the
// member-name gate and the link-target gate (final-review C1: the
// two gates had drifted into separate IsAbs-only checks, and
// filepath.IsAbs is platform-LOADEN — on windows it says false for
// "/tmp/x" (no volume) and on unix it says false for "C:/x" (no
// volumes at all), so each platform had its own blind spot). The
// refusal set is evaluated LEXICALLY so it means the same on every
// host: empty, NUL, backslash (a path separator on windows), "..", a
// leading "/" (absolute on unix, drive-root-relative on windows), a
// windows volume prefix ("C:/x", "c:x" — filepath.VolumeName catches
// it on windows; tcVolumePrefix keeps it refused on unix too, where a
// drive-letter colon is never a legitimate toolchain member and the
// literal "C:" tree it would land inside the cache is windows-shaped
// malice either way), and the host's own filepath.IsAbs as the belt
// behind all of it.
func tcPathHostile(p string) bool {
	return p == "" || strings.ContainsAny(p, "\x00\\") ||
		strings.HasPrefix(p, "/") || strings.Contains(p, "..") ||
		filepath.IsAbs(p) || filepath.VolumeName(p) != "" ||
		tcVolumePrefix(p)
}

// tcVolumePrefix reports the windows drive-letter form "X:" at the
// start of p (single ASCII letter + colon). On windows
// filepath.VolumeName already rejects it; on unix — where VolumeName
// is identically empty — this keeps the refusal set platform-blind
// (final-review C1: "C:/evil" used to land on linux as a contained
// but garbage "C:" tree, and on windows "/etc/passwd" as a symlink
// the OS resolves against the current drive's root — outside the
// cache).
func tcVolumePrefix(p string) bool {
	return len(p) >= 2 && p[1] == ':' &&
		((p[0] >= 'A' && p[0] <= 'Z') || (p[0] >= 'a' && p[0] <= 'z'))
}

// extractTarGz unpacks a .tar.gz stream into dst, refusing hostile
// members outright (tcPathHostile, platform-blind): absolute paths —
// including the windows-shaped leading-"/" and "C:/x" volume forms —
// any "..", backslashes (a path separator on windows — the
// off-matrix member a unix tar would shrug at), NUL bytes, and
// everything that resolves outside dst after cleaning (belt and
// braces: the resolved path is re-checked relative to dst).
// Directories land 0755, files 0644 + header exec bits.
//
// Link members (tar.TypeSymlink / tar.TypeLink) LAND when their
// target is a contained RELATIVE path — the official wasi-sdk
// tarball's own shape: it ships ~20 same-dir relative symlinks
// (bin/llvm-strip -> llvm-objcopy, bin/clang++ -> clang &c; found by
// the B11 task-11 HIL — the earlier blanket refusal assumed "real
// toolchain tarballs carry none", which the real artifact disproved,
// so the fetch of the actual wasi-sdk could never complete). The
// target gate is the member-name gate verbatim plus containment of
// the LEXICALLY resolved target: an absolute, ".."-escaping,
// backslash, or NUL-bearing target is refused — a link member is an
// escape vector exactly when its target escapes. Links land in a
// DEFERRED pass after every regular member, so the windows-friendly
// copy fallback (os.Symlink denied without developer mode) always
// finds its target on disk; unix keeps true symlinks because the
// toolchain's binaries exec through them. Special members (devices,
// fifos &c) stay refused.
func extractTarGz(src io.Reader, dst string) error {
	gz, err := gzip.NewReader(src)
	if err != nil {
		return fmt.Errorf("not gzip: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var total int64
	var links []tcLink
	// linkTargetOK gates one link target: a non-empty RELATIVE path
	// whose shape passes tcPathHostile and whose resolution (relative
	// to the link's own directory for symlinks, to the archive root
	// for hardlinks) stays lexically inside dst.
	linkTargetOK := func(name, target string, hard bool) bool {
		if tcPathHostile(target) {
			return false
		}
		resolved := target
		if !hard { // a symlink's target resolves from the link's dir
			resolved = filepath.Join(filepath.Dir(name), target)
		}
		rel, rerr := filepath.Rel(dst, filepath.Join(dst, resolved))
		return rerr == nil && rel != ".." && !filepath.IsAbs(rel) &&
			!strings.HasPrefix(rel, ".."+string(filepath.Separator))
	}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar stream: %w", err)
		}
		name := hdr.Name
		if tcPathHostile(name) {
			return fmt.Errorf("member %q is not a safe relative path (absolute — including windows leading-slash and volume forms — \"..\", or backslash members are refused)", name)
		}
		target := filepath.Join(dst, name)
		if rel, rerr := filepath.Rel(dst, target); rerr != nil || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("member %q escapes the extraction root", name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, ferr := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, tcFilePerm(hdr.Mode))
			if ferr != nil {
				return ferr
			}
			// T3 review M1: the cap bounds the WRITE itself — the copy
			// stops at the remaining budget (+1 to detect the byte that
			// crosses it), so a bomb member is cut on disk at the budget
			// and the refusal fires immediately, mid-member; members
			// past it never extract at all.
			n, cerr := io.Copy(f, io.LimitReader(tr, tcExtractMax-total+1))
			if cerr == nil {
				total += n
				if total > tcExtractMax {
					cerr = fmt.Errorf("expands beyond the %d-byte extraction cap", tcExtractMax)
				}
			}
			if cerr != nil {
				f.Close()
				return cerr
			}
			if cerr = f.Close(); cerr != nil {
				return cerr
			}
		case tar.TypeSymlink, tar.TypeLink:
			if !linkTargetOK(name, hdr.Linkname, hdr.Typeflag == tar.TypeLink) {
				return fmt.Errorf("member %q links to %q — an absolute, \"..\", backslash, or escaping link target is refused", name, hdr.Linkname)
			}
			links = append(links, tcLink{name: name, target: hdr.Linkname, hard: hdr.Typeflag == tar.TypeLink})
		case tar.TypeXGlobalHeader:
			continue // pax global metadata — no filesystem member
		default:
			return fmt.Errorf("member %q has unsupported type (special members — devices, fifos &c — are refused; link members land when their target is a contained relative path)", name)
		}
	}
	// the deferred link pass: every regular member is on disk, so a
	// link's target exists — even when the link member PRECEDES its
	// target in the stream (legal tar; TestExtractTarGzLinks pins the
	// forward-reference shape). The landing itself lives in tcLandLink
	// behind an injectable link primitive — the fallback's own test
	// seam (T11a M1).
	for _, l := range links {
		mk := func(_, linkPath string) error { return os.Symlink(l.target, linkPath) }
		if l.hard {
			mk = os.Link
		}
		n, lerr := tcLandLink(dst, l, mk, tcExtractMax-total)
		if lerr != nil {
			return lerr
		}
		total += n
	}
	return nil
}

// tcLink is one deferred link member: the member's path relative to
// the extraction root, its VERBATIM link target, and whether it is a
// hardlink (else symlink). Package-level so the fallback test can
// drive tcLandLink directly.
type tcLink struct {
	name, target string
	hard         bool
}

// absTarget resolves one link's TARGET to its absolute path under
// dst: a symlink's target resolves from the link's OWN DIRECTORY —
// Join(dst, name) is already absolute, so that join ends the
// computation (T11a M1: prefixing dst a SECOND time doubled the path,
// the copy fallback ENOENT'd, and the windows fetch — where
// os.Symlink is denied and the fallback fires for every symlink —
// could never complete); a hardlink's target is archive-root-relative
// and anchors at dst.
func (l tcLink) absTarget(dst string) string {
	if l.hard {
		return filepath.Join(dst, l.target)
	}
	return filepath.Join(filepath.Dir(filepath.Join(dst, l.name)), l.target)
}

// tcLandLink lands ONE deferred link member: the native link through
// mk (os.Link for hardlinks, os.Symlink else — the resolved target
// rides along for the assertion-shaped seams), and — when the OS
// refuses in the fallback-worthy way — a BYTE COPY of the resolved
// target, budget-checked against the remaining extraction cap.
// Returns the fallback's byte count (0 when the native link landed).
//
// mk is the injectable seam the fallback test rides: on unix
// os.Symlink just succeeds, so the copy leg is unreachable in
// production there — exactly how the T11a-M1 double-dst-join bug
// survived a green linux HIL and would have killed the windows fetch
// (where os.Symlink is denied without developer mode and the
// fallback fires for EVERY symlink member). The test injects the
// denial and pins the exact path the fallback reads.
func tcLandLink(dst string, l tcLink, mk func(resolved, linkPath string) error, budget int64) (int64, error) {
	p := filepath.Join(dst, l.name)
	resolved := l.absTarget(dst)
	lerr := mk(resolved, p)
	if lerr == nil {
		return 0, nil
	}
	// the windows-friendly fallback (and the cross-device guard for
	// hardlinks): a symlink denied (no developer mode) or a
	// cross-device os.Link becomes a BYTE COPY of the target — the
	// toolchain runs the same either way; any other failure (a
	// genuinely dangling link — the real tarballs carry none) stays
	// an error rather than a silent gap.
	if !(os.IsPermission(lerr) || errors.Is(lerr, syscall.EXDEV) || errors.Is(lerr, syscall.EPERM) || errors.Is(lerr, syscall.EACCES)) {
		return 0, fmt.Errorf("member %q: link to %q: %w", l.name, l.target, lerr)
	}
	b, rerr := os.ReadFile(resolved)
	if rerr != nil {
		return 0, fmt.Errorf("member %q: link to %q: %w (copy fallback could not read the resolved target %s: %v)", l.name, l.target, lerr, resolved, rerr)
	}
	if int64(len(b)) > budget {
		return 0, fmt.Errorf("member %q: copy fallback expands beyond the %d-byte extraction budget", l.name, budget)
	}
	mode := os.FileMode(0o644)
	if fi, serr := os.Stat(resolved); serr == nil && fi.Mode()&0o111 != 0 {
		mode = 0o755
	}
	if werr := os.WriteFile(p, b, mode); werr != nil {
		return 0, fmt.Errorf("member %q: link to %q: %w (copy fallback write: %v)", l.name, l.target, lerr, werr)
	}
	return int64(len(b)), nil
}

// tcPiece is one asset this fetch manages: its index pin, its rename
// destination under the cache root, and the path the state records
// (§6.1 layout — the sdk lands as its own directory named by the
// generation's pinned sdk version + platform key; the wamrc binary
// lands under wamrc/).
type tcPiece struct {
	kind   string
	asset  *tcAsset
	target string // rename destination, relative to the cache root
	record string // Dir the state records (wamrc's names the BINARY)
}

// tcKinds lists the pieces' kinds, in the fetch's fixed order, that
// appear in set — deterministic Downloaded/Skipped ordering.
func tcKinds(pieces []tcPiece, set map[string]tcInstalled) []string {
	out := make([]string, 0, len(set))
	for _, p := range pieces {
		if _, ok := set[p.kind]; ok {
			out = append(out, p.kind)
		}
	}
	return out
}

// tcJoinNotes joins the non-empty notes with "; " (degraded index +
// missing wamrc leg can both apply).
func tcJoinNotes(notes ...string) string {
	var out []string
	for _, n := range notes {
		if n != "" {
			out = append(out, n)
		}
	}
	return strings.Join(out, "; ")
}

// toolchainFetch installs this host's toolchain pieces from the
// index into the local cache. The atomic sequence (spec §6.1): every
// needed piece downloads into a RANDOM <cache>/.tmp-fetch-*/ staging
// dir (two concurrent processes never touch each other's bytes, and
// every rename stays inside the cache root's volume), each staged
// archive is sha-re-verified, then the trees rename into place and
// ONE state file lands — any failure at any step removes the staging
// dir and the cache holds exactly what it held before (zero
// residue). Idempotency: a piece whose state record pins the same
// sha AND whose recorded Dir still exists is Skipped untouched —
// re-calling is the safe "sync" verb.
//
// Locking (T1 review M2's handoff, resolved): two locked cycles —
// PLAN (state load + idempotency probes) and COMMIT (renames + state
// save) — with the network and extraction in between outside tcMu,
// so a concurrent status never blocks behind a long download. Two
// concurrent fetches in THIS process serialize at the commit; two
// PROCESSES are last-writer-wins by design (the random staging keeps
// their bytes apart, and whichever state lands is self-consistent:
// both pieces from one generation, Dir records pointing at real
// trees — MB's fully-local present reads it without the index).
func (ts *toolset) toolchainFetch(ctx context.Context, _ *mcpsdk.CallToolRequest, in tcFetchIn) (*mcpsdk.CallToolResult, tcFetchOut, error) {
	// index URL gate, status's shape exactly: omitted = the default,
	// explicit = must parse to http(s) with a host.
	raw := in.IndexURL
	if raw == "" {
		raw = defaultToolchainIndexURL
	}
	iu, uerr := url.Parse(raw)
	if uerr != nil || (iu.Scheme != "http" && iu.Scheme != "https") || iu.Host == "" {
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: index_url must be an http(s) URL, got %q", raw)
	}

	// the WHOLE fetch (index + every asset) shares one deadline.
	ctx, cancel := context.WithTimeout(ctx, time.Duration(tcClampTimeoutSec(in.TimeoutSec))*time.Second)
	defer cancel()

	// The index: remote first, unreachable degrades to the embedded
	// snapshot with a note (the offline pin, design §6.1); a body
	// that arrives but fails the ver gate REFUSES — the same ruling
	// as status (T2 concern #1, controller-endorsed): a newer index
	// needs a newer daemon, and the older snapshot's "latest" must
	// not paper over that hard signal.
	idx, note := embeddedToolchainIndex(), ""
	if body, gerr := fetchGet(ctx, iu.String(), fetchIndexMax); gerr != nil {
		note = fmt.Sprintf("remote index unreachable (%v) — fetched against the daemon's embedded snapshot; the pins are what THIS daemon knows", gerr)
	} else {
		ridx, perr := parseToolchainIndex(body)
		if perr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: index %s: %v", raw, perr)
		}
		idx = ridx
	}

	// generation selection: omitted = the index's latest.
	genName := in.Generation
	if genName == "" {
		if idx.Latest == "" {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: index names no latest generation and none was given")
		}
		genName = idx.Latest
	}
	gen, gok := idx.Generations[genName]
	if !gok {
		var have []string
		for g := range idx.Generations {
			have = append(have, g)
		}
		sort.Strings(have)
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: generation %q not in the index (have: %s)", genName, strings.Join(have, ", "))
	}

	root := tcCacheRoot()
	if root == "" {
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: no toolchain cache root (XUANSI_WASM_CACHE unset and os.UserCacheDir failed) — set XUANSI_WASM_CACHE")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: %v", err)
	}
	plat := tcPlatKey(runtime.GOOS, runtime.GOARCH)
	if plat == "" {
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: this host (%s/%s) has no platform key in the index matrix — no toolchain asset can install here", runtime.GOOS, runtime.GOARCH)
	}
	p, pok := gen.Platforms[plat]
	if !pok || p.WasiSDK == nil {
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: generation %s carries no wasi_sdk asset for %s — wasi_sdk is the mandatory piece (the platform matrix grows with later toolchain releases)", genName, plat)
	}
	if gen.WasiSDK == "" {
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: generation %s pins no wasi_sdk version — its cache directory name cannot be derived", genName)
	}

	// The piece table + per-asset gates, ALL before any download
	// (fail fast — driverFetch's stage-everything discipline): every
	// index-picked URL re-passes the http(s) gate (driver_fetch fix1's
	// threat model, asset twin — a hostile index must not steer
	// file:// or other schemes), pins must be complete (the TBD
	// refusal), and declared sizes must fit the caps.
	sdkDir := "wasi-sdk-" + gen.WasiSDK + "-" + plat
	var pieces []tcPiece
	var wamrcLegNote string
	total := int64(0)
	for _, m := range []struct {
		kind         string
		a            *tcAsset
		target, recd string
	}{
		{"wasi_sdk", p.WasiSDK, sdkDir, sdkDir},
		{"wamrc", p.Wamrc, "wamrc", "wamrc/wamrc"},
	} {
		if m.a == nil {
			if m.kind == "wamrc" {
				wamrcLegNote = fmt.Sprintf("wamrc: this index generation carries no wamrc asset for %s — nothing to install yet (the platform matrix grows with later toolchain releases); wasi_sdk alone was installed", plat)
			}
			continue
		}
		au, aerr := url.Parse(m.a.URL)
		if aerr != nil || (au.Scheme != "http" && au.Scheme != "https") || au.Host == "" {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: index asset URL %q must be an http(s) URL — a hostile index must not steer other schemes", m.kind, m.a.URL)
		}
		if !tcPinComplete(m.a) {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: index entry incomplete (sha pending T6) — sha256=%q bytes=%d is not a usable pin; refusing to fetch unpinned bytes", m.kind, m.a.SHA256, m.a.Bytes)
		}
		if m.a.Bytes > tcArchiveMax {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: index pins %d bytes, over the %d-byte single-archive cap", m.kind, m.a.Bytes, int64(tcArchiveMax))
		}
		total += m.a.Bytes
		if total > tcFetchTotalMax {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: declared assets total %d bytes, over the %d-byte whole-fetch cap", total, int64(tcFetchTotalMax))
		}
		pieces = append(pieces, tcPiece{kind: m.kind, asset: m.a, target: m.target, record: m.recd})
	}

	// PLAN (locked): state load + idempotency probes. Skip = the
	// state records the piece with the SAME sha AND its Dir exists on
	// disk (target present + state sha same → Skipped).
	statePath := filepath.Join(root, toolchainStateFile)
	tcMu.Lock()
	st, err := loadTCState(statePath)
	if err != nil {
		tcMu.Unlock()
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: %v", err)
	}
	skipped := map[string]tcInstalled{}
	var toFetch []tcPiece
	for _, piece := range pieces {
		if rec, ok := st.Installed[piece.kind]; ok && rec.Dir != "" && strings.EqualFold(rec.SHA256, piece.asset.SHA256) && tcPathExists(filepath.Join(root, rec.Dir)) {
			skipped[piece.kind] = rec
		} else {
			toFetch = append(toFetch, piece)
		}
	}
	if len(toFetch) == 0 {
		// Everything pinned on disk already — still re-write the
		// state's generation LABEL: the pairing check reads the
		// label, and a fetch of a generation whose pins match the
		// cache's bytes is exactly "this cache now holds it".
		fresh := &tcState{Generation: genName, Installed: map[string]tcInstalled{}}
		for kind, rec := range skipped {
			fresh.Installed[kind] = rec
		}
		if serr := saveTCState(statePath, fresh); serr != nil {
			tcMu.Unlock()
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: %v", serr)
		}
		tcMu.Unlock()
		return nil, tcFetchOut{Generation: genName, Downloaded: []string{}, Skipped: tcKinds(pieces, skipped), CacheRoot: root, Note: tcJoinNotes(note, wamrcLegNote)}, nil
	}
	tcMu.Unlock()

	// STAGE (no lock): everything lands in one random temp dir first.
	tmp, err := os.MkdirTemp(root, ".tmp-fetch-")
	if err != nil {
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: staging dir: %v", err)
	}
	// Always swept: on failure NOTHING may remain (zero residue); on
	// success the renamed-out trees leave only the archives and empty
	// scaffolding behind, which go too.
	defer os.RemoveAll(tmp)

	type tcStaged struct {
		piece tcPiece
		tree  string // the extracted tree's effective root
	}
	var stagedList []tcStaged
	for _, piece := range toFetch {
		archPath := filepath.Join(tmp, piece.kind+".tar.gz")
		af, aerr := os.Create(archPath)
		if aerr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", piece.kind, aerr)
		}
		ferr := fetchTCArchive(ctx, piece.asset.URL, piece.asset.SHA256, piece.asset.Bytes, tcArchiveMax, af)
		af.Close()
		if ferr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", piece.kind, ferr)
		}
		stageDir := filepath.Join(tmp, piece.kind+".x")
		if merr := os.MkdirAll(stageDir, 0o755); merr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", piece.kind, merr)
		}
		sf, serr := os.Open(archPath)
		if serr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", piece.kind, serr)
		}
		xerr := extractTarGz(sf, stageDir)
		sf.Close()
		if xerr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", piece.kind, xerr)
		}
		// The effective tree root: both real tarballs (official
		// wasi-sdk, our wamrc) carry ONE top-level dir — that inner
		// dir is the tree to rename. Any other shape (flat tarball)
		// renames the stage dir itself.
		ents, rerr := os.ReadDir(stageDir)
		if rerr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", piece.kind, rerr)
		}
		if len(ents) == 0 {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: archive is empty — nothing to install", piece.kind)
		}
		tree := stageDir
		if len(ents) == 1 && ents[0].IsDir() {
			tree = filepath.Join(stageDir, ents[0].Name())
		}
		if piece.kind == "wamrc" {
			if _, werr := os.Stat(filepath.Join(tree, "wamrc")); werr != nil {
				return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch wamrc: the tarball carries no wamrc binary at its root — refusing a toolchain with no compiler (the index pin and the tarball's shape disagree)")
			}
		}
		// T3 review M3: the flattened sdk tree must carry bin/ — a
		// wasi-sdk without bin/ holds no compiler, and the sha pin
		// cannot see SHAPE (it hashed the tarball, not the layout);
		// wamrc's no-binary check, sdk twin.
		if piece.kind == "wasi_sdk" {
			if _, serr := os.Stat(filepath.Join(tree, "bin")); serr != nil {
				return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch wasi_sdk: unexpected wasi-sdk layout (no bin/ under the archive's root) — refusing a toolchain with no compiler; the index pin and the tarball's shape disagree")
			}
		}
		stagedList = append(stagedList, tcStaged{piece: piece, tree: tree})
	}

	// Re-verify every staged archive's sha (brief: 逐件 sha 複驗) —
	// the bytes ON DISK in staging are what the commit renames in,
	// so they are re-hashed against the index pin one last time.
	for _, s := range stagedList {
		archPath := filepath.Join(tmp, s.piece.kind+".tar.gz")
		f, oerr := os.Open(archPath)
		if oerr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", s.piece.kind, oerr)
		}
		h := sha256.New()
		_, cerr := io.Copy(h, f)
		f.Close()
		if cerr != nil {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", s.piece.kind, cerr)
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != strings.ToLower(s.piece.asset.SHA256) {
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: staged archive re-verify: sha256 %s != index %s", s.piece.kind, got, s.piece.asset.SHA256)
		}
	}

	// COMMIT (locked): renames + ONE state write. Replacing an
	// existing target (a stale tree, or a concurrent process's fresh
	// commit) removes it first — under tcMu in-process commits never
	// interleave, and cross-process is last-writer-wins; a crash in
	// the remove→rename window leaves a cache status honestly reads
	// as not-present, and a re-fetch heals.
	tcMu.Lock()
	defer tcMu.Unlock()
	fresh := &tcState{Generation: genName, Installed: map[string]tcInstalled{}}
	for kind, rec := range skipped {
		fresh.Installed[kind] = rec
	}
	for _, s := range stagedList {
		target := filepath.Join(root, s.piece.target)
		if tcPathExists(target) {
			if rerr := os.RemoveAll(target); rerr != nil {
				return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", s.piece.kind, rerr)
			}
		}
		if rerr := os.Rename(s.tree, target); rerr != nil {
			// T3 review M2: same narrow window as a crash between the
			// remove and the rename — the target is already gone and
			// the state not yet saved, so status honestly reads
			// not-present and a re-fetch heals; this fetch dies whole,
			// never half-committed.
			return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch %s: %v", s.piece.kind, rerr)
		}
		fresh.Installed[s.piece.kind] = tcInstalled{SHA256: strings.ToLower(s.piece.asset.SHA256), Bytes: s.piece.asset.Bytes, Dir: s.piece.record}
	}
	if serr := saveTCState(statePath, fresh); serr != nil {
		return nil, tcFetchOut{}, errWs("xuansi_toolchain_fetch: %v", serr)
	}

	out := tcFetchOut{
		Generation: genName,
		Downloaded: make([]string, 0, len(stagedList)),
		Skipped:    tcKinds(pieces, skipped),
		CacheRoot:  root,
		Note:       tcJoinNotes(note, wamrcLegNote),
	}
	for _, s := range stagedList {
		out.Downloaded = append(out.Downloaded, s.piece.kind)
	}
	return nil, out, nil
}
