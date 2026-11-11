package mcp

// workspace.go — the daemon-side driver workspace (batch 6 task 11,
// design §7 "daemon 端版本管理"): $XDG_DATA_HOME/xuansi/workspace with
//
//	drivers/<name>@<ver>.lua   ← versioned store = the history
//	manifest.json              ← 生效版本 (Active), checksums, boards
//
// Deploy = push a version file onto the board's effective file
// /drv/<name>.lua. Rollback = push the older version explicitly;
// fleet = push each board — both emergent from the same primitive, no
// extra tools. The fw loads a driver ONLY when its first line is
// exactly "--@api N" with N equal to the board's bench.api_version
// (one axis, no semver): xuansi_driver_push enforces the SAME gate
// daemon-side BEFORE the flash write, so a mismatched driver is
// refused without wasting board I/O. The board's api_version comes
// from the dynamic refresh's caps cache when fresh (tools_dynamic.go
// re-asks every DynRefreshDefault), else ONE holder-less op caps
// exactly like the refresh's own call.
//
// Four static tools (numbers 13-16): xuansi_ws_put stores a version
// (any size — caps are a BOARD concern, checked at push
// time, so a fat version can sit in the workspace until it slims),
// xuansi_ws_list / xuansi_ws_get read the store back, and
// xuansi_driver_push runs the gated pipeline
//
//	read file → load gate (B10 task 5, branched by artifact: .lua →
//	--@api vs bench.api_version; .wasm/.aot → module magic) →
//	fs_write carrying the payload's sha256 (B15 §4.5②: the verified
//	path — the board hashes the chunks as they land and its
//	fs.write_commit verdict IS the verification; the post-push byte
//	read-back is retired, halving the transfer) → manifest
//
// The manifest records the push ONLY after the commit verdict verifies
// — a corrupt board write never becomes history. B8 task 1 appends
// the removal half, xuansi_driver_pull (fs_rm of the effective file +
// Active cleared, footprints kept), after the capture tool.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/paths"
	"xuansi/internal/server"
)

// ---------------------------------------------------------------------------
// root, paths, sanitization

// wsRoot is the default workspace root: $XDG_DATA_HOME/xuansi/workspace,
// or ~/.local/share/xuansi/workspace when XDG_DATA_HOME is unset — the
// XDG data convention, os.UserConfigDir's data-dir analogue; the
// per-OS data-dir resolution lives in internal/paths (byte-identical
// on Linux). Tests inject their root through Options.WorkspaceRoot
// instead of mutating the environment.
func wsRoot() string {
	return filepath.Join(paths.DataDir(), "workspace")
}

// wsMu serializes every manifest read-modify-write cycle in-process
// (the package's one workspace mutex): puts and pushes merge their
// entries/footprints instead of racing a lost update — concurrent
// pushes last-write-win on Active while every Boards entry survives.
// Cross-process, the atomic rename (manifest.go) keeps each file
// whole and the versioned driver files never collide at all.
var wsMu sync.Mutex

// wsState is the workspace root handle. Every filesystem path the
// workspace touches is built by its two methods from wsNameRe /
// wsVerRe-validated parts — nothing else constructs a path, so
// "../evil" & friends never reach the filesystem.
type wsState struct{ root string }

func newWSState(root string) *wsState {
	if root == "" {
		root = wsRoot()
	}
	return &wsState{root: root}
}

func (w *wsState) manifestPath() string { return filepath.Join(w.root, "manifest.json") }

// driverPath is drivers/<name>@<ver>.<ext> on sanitized parts only —
// ext is one of the fixed artifact set (lua/wasm/aot, pushExts), never
// raw input, so "../evil" & friends never reach the filesystem.
func (w *wsState) driverPath(name, ver, ext string) string {
	return filepath.Join(w.root, "drivers", name+"@"+ver+"."+ext)
}

// drvBoardPath is the board's EFFECTIVE driver file the push
// overwrites — always /drv/<sanitized name>.<ext>, never raw input.
// B10 task 5: the extension is the artifact's own (a fetched .wasm
// deploys to /drv/<name>.wasm — the fw wasm loader's lookup path).
func drvBoardPath(name, ext string) string { return "/drv/" + name + "." + ext }

// name/version sanitization (the path-traversal guard): name
// [a-z0-9_-]+, version [a-z0-9.]+, aot target [a-z0-9-]+ (the §8-6
// namespace's own vocabulary — esp32s3 style; a target is a manifest
// key, not a path, but the gate keeps hostile indexes from planting
// arbitrary keys); anything else is a local api-class refusal.
var (
	wsNameRe   = regexp.MustCompile(`^[a-z0-9_-]+$`)
	wsVerRe    = regexp.MustCompile(`^[a-z0-9.]+$`)
	wsTargetRe = regexp.MustCompile(`^[a-z0-9-]+$`)
)

// chipTargets maps the fw hello's chip string (surfaced as the board
// list's chip column — daemon-cached hello) to the aot target
// namespace's key (§8-6: "esp32s3" today; classic/C6 keys arrive later
// without migration). An unmapped chip pairs NOTHING — the conservative
// .wasm single (ruling 1: never guess an arch, never claim a pair that
// did not happen).
var chipTargets = map[string]string{
	"ESP32-S3": "esp32s3",
}

// boardTarget answers the board's aot target key from its hello chip.
func boardTarget(chip string) (string, bool) {
	t, ok := chipTargets[chip]
	return t, ok
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// capsFreshTTL bounds how stale the dynamic refresh's cached caps may
// be when gating a push: production re-asks every DynRefreshDefault
// (5s), so 30s rides out a missed cycle without letting a mid-session
// api_version flip go unnoticed for long.
const capsFreshTTL = 30 * time.Second

// apiPrefix heads the fw gate's demanded first line: "--@api N" with
// N EXACTLY equal to the board's bench.api_version (design §7: one
// compatibility axis, no semver ranges).
const apiPrefix = "--@api "

// parseAPIDirective pulls the --@api N the driver file's FIRST line
// must carry: prefix, then digits only (CRLF tolerated). Missing,
// displaced or malformed directives are errors; the caller names the
// file in its own refusal.
func parseAPIDirective(content string) (int, error) {
	line := content
	if i := strings.IndexByte(content, '\n'); i >= 0 {
		line = content[:i]
	}
	line = strings.TrimRight(line, "\r")
	if !strings.HasPrefix(line, apiPrefix) {
		return 0, fmt.Errorf("first line is not %q (got %q)", apiPrefix+"N", firstWords(line))
	}
	digits := line[len(apiPrefix):]
	if digits == "" || strings.Trim(digits, "0123456789") != "" {
		return 0, fmt.Errorf("first line %q does not carry an integer N after %q", line, apiPrefix)
	}
	n, err := strconv.Atoi(digits)
	if err != nil {
		return 0, fmt.Errorf("first line %q: %v", line, err)
	}
	return n, nil
}

// firstWords quotes a line for an error message without dumping a
// whole fat driver into it.
func firstWords(line string) string {
	if len(line) > 40 {
		return line[:40] + "…"
	}
	return line
}

// B10 module magics, byte-for-byte the fw wasm loader's own gate
// (xs_wasm.c k_wasm_magic/k_aot_magic) AND the real artifacts under
// wan-cloud/xuansi-drivers modules/ (hello.aot/ledwatch.aot open
// 00 61 6f 74; every .wasm opens 00 61 73 6d): a wasm module starts
// "\0asm", a WAMR AOT file starts "\0aot".
var (
	wasmMagic = [4]byte{0x00, 0x61, 0x73, 0x6d} // \0asm
	aotMagic  = [4]byte{0x00, 0x61, 0x6f, 0x74} // \0aot (WAMR AOT header)
)

// gateModuleMagic is the B10 push gate for binary artifacts (task 5:
// the .aot leg the brief names, plus its .wasm sibling): the fw's
// wasm loader refuses any /drv/<name>.wasm|.aot whose first four
// bytes are not the module magic (xs_wasm.c read_one_module_file), so
// the write is refused daemon-side BEFORE the flash cycle — the same
// waste-avoidance the --@api gate gives the Lua track. The Lua track
// itself rides parseAPIDirective unchanged.
func gateModuleMagic(ext string, content []byte, file string) error {
	var want *[4]byte
	switch ext {
	case "wasm":
		want = &wasmMagic
	case "aot":
		want = &aotMagic
	default:
		return nil // .lua rides the --@api directive gate instead
	}
	head := content
	if len(head) > 4 {
		head = head[:4]
	}
	if len(content) < 4 || !bytes.Equal(head, want[:]) {
		return errWs("xuansi_driver_push: %s: first bytes % x are not the .%s module magic % x — the board's wasm loader refuses to load it", file, ext, head, want[:])
	}
	return nil
}

// pushExts is the fixed artifact-extension probe order a push (and a
// fetched version's manifest FileMeta) resolves with (B10 task 5):
// classic .lua first — every pre-B10 workspace and manifest behaves
// exactly as before — then the delivery tracks: .wasm (INTERP, what
// the fetch stores; the fw loader's fallback) and .aot
// (meta-auxiliary; this stack's loader honestly refuses AOT execution
// but the file still deploys whole and commit-verified).
var pushExts = []string{"lua", "wasm", "aot"}

// errWs is the workspace's local api-class refusal (the fs tools'
// client-side refusal convention, errors.go).
func errWs(format string, args ...any) error {
	return &server.RPCError{Class: "api", Msg: fmt.Sprintf(format, args...)}
}

// ---------------------------------------------------------------------------
// tool I/O shapes

type wsPutIn struct {
	Name    string `json:"name" jsonschema:"driver name, lowercase [a-z0-9_-]+ (e.g. \"ads1115\") — the board's effective file is /drv/<name>.lua"`
	Version string `json:"version" jsonschema:"version string, [a-z0-9.]+ (e.g. \"1.3.0\") — the workspace file is drivers/<name>@<version>.lua"`
	Content string `json:"content" jsonschema:"the driver Lua module's full source. Storing does NOT activate anything — nothing reaches a board until xuansi_driver_push"`
}

type wsPutOut struct {
	Sha256 string `json:"sha256" jsonschema:"sha256 of the stored content"`
	Size   int    `json:"size" jsonschema:"stored bytes (any size storable; the push rides the chunked transport)"`
}

type wsListIn struct{}

type wsListOut struct {
	Drivers []wsDriverSummary `json:"drivers" jsonschema:"one entry per driver, sorted by name"`
}

type wsDriverSummary struct {
	Name   string `json:"name"`
	Active string `json:"active,omitempty" jsonschema:"the version a bare xuansi_driver_push deploys; absent when none was ever pushed"`
	// Kind (B15 W1.T4, vocabulary reconciled W2.T4) surfaces the
	// RELEASE CLASS the fetch index carried in its `kind` field —
	// the post-restructure index vocabulary (production/test-canary),
	// one meaning at every seat face; hand-put versions and a kindless
	// index leave it honestly absent. The per-version module facts
	// (its own release claim, the dev-lane marker) ride each version
	// row instead.
	Kind     string          `json:"kind,omitempty" jsonschema:"the RELEASE CLASS the fetch index carried for this driver (production / test-canary — the index's kind field, verbatim; persisted in the manifest); absent for hand-put versions and kindless indexes. A built version makes no release-class claim — its dev-lane marker rides the version rows"`
	Versions []wsVersionInfo `json:"versions" jsonschema:"every stored version, sorted"`
}

type wsVersionInfo struct {
	Version string `json:"version"`
	Size    int    `json:"size"`
	Sha256  string `json:"sha256"`
	// Aot (P3-T13) surfaces the version's per-target .aot records —
	// the pairing claims a bare push acts on. Absent = a bare push
	// deploys the single resolved artifact on every board.
	Aot map[string]AotSlot `json:"aot,omitempty" jsonschema:"per-target .aot records the version's manifest carries (key = aot target, e.g. \"esp32s3\"; value = sha256+size of the .aot): present = a bare xuansi_driver_push to a board whose chip maps to that key (ESP32-S3 → esp32s3) deploys the .wasm + .aot PAIR; absent = the single resolved artifact"`
	// Dev (B15 W2-T4) marks a version built through a §6.4 override
	// lane (wasm_build's wamrc_path/wasi_sdk_path) — visibly a dev
	// artifact, not a cache-paired build. The former "wasm-dev" entry
	// kind, moved to where the fact lives.
	Dev bool `json:"dev,omitempty" jsonschema:"true when this version was built through a §6.4 override lane (xuansi_wasm_build's wamrc_path or wasi_sdk_path) — a dev artifact compiled outside the toolchain cache's pairing evidence"`
}

type wsGetIn struct {
	Name    string `json:"name" jsonschema:"driver name, lowercase [a-z0-9_-]+"`
	Version string `json:"version" jsonschema:"version string, [a-z0-9.]+"`
}

type wsGetOut struct {
	Content  string `json:"content" jsonschema:"the stored artifact's content: verbatim text for a .lua driver; standard base64 for a binary .wasm/.aot artifact (encoding says which)"`
	Encoding string `json:"encoding,omitempty" jsonschema:"absent for a .lua driver (content is verbatim text); \"base64\" when the stored artifact is binary (.wasm/.aot — content is standard base64, sha256 of the DECODED bytes, matching the manifest footprint)"`
	Artifact string `json:"artifact,omitempty" jsonschema:"the workspace file that served the read (drivers/<name>@<ver>.<lua|wasm|aot>) — same artifact priority a bare xuansi_driver_push deploys"`
	Sha256   string `json:"sha256" jsonschema:"sha256 of the stored bytes"`
}

type driverPushIn struct {
	Board   string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Name    string `json:"name" jsonschema:"driver name, lowercase [a-z0-9_-]+ — the board's effective file /drv/<name>.<ext> is overwritten (a paired push overwrites BOTH .wasm and .aot; an explicit single-artifact push overwrites its own ext and EMPTIES the sibling slots)"`
	Version string `json:"version,omitempty" jsonschema:"omit to re-deploy the manifest's ACTIVE version (error when none is active). Rollback = push the older version explicitly; fleet = push each board"`
	// Artifact (P3-T13, the F-5 kill): push EXACTLY one binary
	// artifact and sweep every other extension slot empty — a stale
	// .aot can never shadow a fresh .wasm (the loader prefers .aot).
	// Omitted = the manifest-driven default (pairing, see the tool
	// Desc).
	Artifact string `json:"artifact,omitempty" jsonschema:"\"wasm\" or \"aot\": push exactly that artifact AND fs_rm every sibling slot (/drv/<name>.<lua|wasm|aot> minus the pushed one — the F-5 sweep, driver_pull's mirror; an already-empty sibling is expected not-found noise). The kill for the stale-shield foot-gun: the loader PREFERS .aot, so redeploying a .wasm while an old .aot sits in its slot would silently keep running the old compiled code — artifact \"wasm\" removes it. Omit for the manifest-driven default: .wasm + .aot PAIR when the version's manifest record aot.<target> matches the board's chip (see the tool Desc), else the single resolved artifact (.lua → .wasm → .aot priority). NOT \"lua\": a Lua driver just pushes bare (it has no binary sibling to shield)"`
	Holder   string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type driverPushOut struct {
	Board  string `json:"board" jsonschema:"the board the push landed on: resolved mac6 when the local board list knows it, else the raw selector"`
	Path   string `json:"path" jsonschema:"the board's effective driver file that was overwritten (the .wasm deliverable's path on a paired push; the pushed artifact's on a single push)"`
	Sha256 string `json:"sha256" jsonschema:"sha256 of the pushed bytes at path — the digest the board's fs.write_commit verdict verified"`
	Bytes  int    `json:"bytes" jsonschema:"bytes written at path"`
	// VerifiedBy (B15 §4.5②) names the verification axis: the push's
	// fs_write carries the payload's sha256 and the board's own
	// commit verdict (its chunk-by-chunk ledger) is what verified the
	// landing — the post-push byte read-back is retired.
	VerifiedBy string `json:"verified_by" jsonschema:"the verification axis: \"commit-sha256\" — the board's fs.write_commit verified the payload digest chunk-by-chunk as the file landed (the former post-push fs_read-back is retired; fs_read stays an independent tool)"`
	// Kind (B15 W1.T4, vocabulary reconciled W2.T4) surfaces the
	// RELEASE CLASS on the deploy seat — the index's kind field the
	// fetch persisted (production/test-canary), else the pushed
	// version's own meta release claim when the index carried none;
	// absent for hand-put versions. One vocabulary at every face.
	Kind string `json:"kind,omitempty" jsonschema:"the RELEASE CLASS for this push (production / test-canary): the fetch index's kind field, else the version's own meta release claim; absent for hand-put versions and kindless sources"`
	// Paired (P3-T13): the honest pair claim. True only when BOTH legs
	// of a .wasm + .aot pair landed commit-verified; false on every
	// single-artifact push — the reply never claims a pair that did
	// not happen (unknown chip, no manifest record, degraded file).
	Paired    bool     `json:"paired" jsonschema:"true when the .wasm + .aot PAIR landed (the version's manifest aot.<target> matched the board's chip; the .aot leg verified FIRST, then the .wasm) — false on every single-artifact push: never a pairing claim that did not happen"`
	AotPath   string   `json:"aot_path,omitempty" jsonschema:"the paired push's .aot board path (present only when paired)"`
	AotSha256 string   `json:"aot_sha256,omitempty" jsonschema:"sha256 of the paired .aot leg, verified by its own commit verdict (present only when paired)"`
	Swept     []string `json:"swept,omitempty" jsonschema:"sibling board paths an explicit artifact push EMPTIED (fs_rm; the F-5 sweep) — absent on paired and bare pushes (they overwrite every slot they touch), and lists nothing when the siblings were already empty"`
}

type driverPullIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Name   string `json:"name" jsonschema:"driver name, lowercase [a-z0-9_-]+ — the board's files /drv/<name>.<lua|wasm|aot> are removed"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type driverPullOut struct {
	Pulled bool   `json:"pulled" jsonschema:"true when a board file was removed and the manifest's Active cleared"`
	Path   string `json:"path" jsonschema:"the first board driver file that was removed (/drv/<name>.<lua|wasm|aot> — every extension slot a push could have written is swept)"`
}

// ---------------------------------------------------------------------------
// handlers

// wsPut stores one version: the file under drivers/ plus its manifest
// Files entry (checksum + size). Storing is NOT activating — Active
// only moves on a verified push, so a stored-but-broken version can
// never become the default deploy.
func (ts *toolset) wsPut(_ context.Context, _ *mcpsdk.CallToolRequest, in wsPutIn) (*mcpsdk.CallToolResult, wsPutOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, wsPutOut{}, errWs("xuansi_ws_put: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	if !wsVerRe.MatchString(in.Version) {
		return nil, wsPutOut{}, errWs("xuansi_ws_put: version must match [a-z0-9.]+, got %q", in.Version)
	}
	path := ts.ws.driverPath(in.Name, in.Version, "lua")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, wsPutOut{}, errWs("xuansi_ws_put: %v", err)
	}
	content := []byte(in.Content)
	if err := writeDurableFile(path, content, 0o644); err != nil {
		return nil, wsPutOut{}, errWs("xuansi_ws_put: %v", err)
	}
	sum := sha256hex(content)

	wsMu.Lock()
	defer wsMu.Unlock()
	m, err := loadManifest(ts.ws.manifestPath())
	if err != nil {
		return nil, wsPutOut{}, errWs("xuansi_ws_put: %v", err)
	}
	e := m.entry(in.Name)
	fm := e.Files[in.Version] // push footprints survive a re-put of the same version
	fm.Sha256, fm.Size = sum, len(content)
	e.Files[in.Version] = fm // Active deliberately untouched
	m.Drivers[in.Name] = e
	if err := writeManifestAtomic(ts.ws.manifestPath(), m); err != nil {
		return nil, wsPutOut{}, errWs("xuansi_ws_put: %v", err)
	}
	return nil, wsPutOut{Sha256: sum, Size: len(content)}, nil
}

// wsList reports the store: per driver the active version and every
// stored version with size + sha256 — the deploy menu, history
// included (rollback picks from here).
func (ts *toolset) wsList(_ context.Context, _ *mcpsdk.CallToolRequest, _ wsListIn) (*mcpsdk.CallToolResult, wsListOut, error) {
	wsMu.Lock()
	m, err := loadManifest(ts.ws.manifestPath())
	wsMu.Unlock()
	if err != nil {
		return nil, wsListOut{}, errWs("xuansi_ws_list: %v", err)
	}
	names := make([]string, 0, len(m.Drivers))
	for n := range m.Drivers {
		names = append(names, n)
	}
	sort.Strings(names)
	out := wsListOut{Drivers: make([]wsDriverSummary, 0, len(names))}
	for _, n := range names {
		e := m.Drivers[n]
		vers := make([]string, 0, len(e.Files))
		for v := range e.Files {
			vers = append(vers, v)
		}
		sort.Strings(vers)
		s := wsDriverSummary{Name: n, Active: e.Active, Kind: e.Kind, Versions: make([]wsVersionInfo, 0, len(vers))}
		for _, v := range vers {
			fm := e.Files[v]
			s.Versions = append(s.Versions, wsVersionInfo{Version: v, Size: fm.Size, Sha256: fm.Sha256, Aot: fm.Aot, Dev: fm.Dev})
		}
		out.Drivers = append(out.Drivers, s)
	}
	return nil, out, nil
}

// wsGet reads one stored version back — the workspace keeps every
// version, so rollback's older file stays retrievable. The artifact
// resolves with the SAME first-extension-wins priority a bare push
// deploys with (pushExts: .lua → .wasm → .aot): a .lua returns
// verbatim text (the pre-B10 face byte-identical), while a binary
// .wasm/.aot — storable only through xuansi_driver_fetch — rides
// standard base64 (a JSON string would mangle invalid UTF-8; fs_read's
// data_b64 is the precedent), with encoding marking the shape and
// sha256 staying of the DECODED bytes, the value the manifest's
// footprint records.
func (ts *toolset) wsGet(_ context.Context, _ *mcpsdk.CallToolRequest, in wsGetIn) (*mcpsdk.CallToolResult, wsGetOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, wsGetOut{}, errWs("xuansi_ws_get: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	if !wsVerRe.MatchString(in.Version) {
		return nil, wsGetOut{}, errWs("xuansi_ws_get: version must match [a-z0-9.]+, got %q", in.Version)
	}
	for _, ext := range pushExts {
		b, err := readFileShared(ts.ws.driverPath(in.Name, in.Version, ext)) // share-delete read (windows replace contract)
		if err == nil {
			out := wsGetOut{Artifact: "drivers/" + in.Name + "@" + in.Version + "." + ext, Sha256: sha256hex(b)}
			if ext == "lua" {
				out.Content = string(b)
			} else {
				out.Content = base64.StdEncoding.EncodeToString(b)
				out.Encoding = "base64"
			}
			return nil, out, nil
		}
		if !os.IsNotExist(err) {
			return nil, wsGetOut{}, errWs("xuansi_ws_get: %v", err)
		}
	}
	return nil, wsGetOut{}, errWs("xuansi_ws_get: %s@%s has no stored artifact (.lua/.wasm/.aot) — xuansi_ws_put stores a Lua driver, xuansi_driver_fetch stores .wasm/.aot", in.Name, in.Version)
}

// driverPush deploys one workspace version onto a board (the pipeline
// is the file comment's gated chain). The daemon crossings: a
// holder-less op caps (like the refresh's own call — never pre-binds
// the connection), then a HELD fs_write of the artifact carrying its
// sha256 (the verified path). Only the board's commit verdict updates
// the manifest.
func (ts *toolset) driverPush(ctx context.Context, _ *mcpsdk.CallToolRequest, in driverPushIn) (*mcpsdk.CallToolResult, driverPushOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	if in.Version != "" && !wsVerRe.MatchString(in.Version) {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: version must match [a-z0-9.]+, got %q", in.Version)
	}
	// the artifact key's enum (P3-T13): a garbage value must refuse as
	// a BAD KEY, not fall through to the missing-artifact error (the
	// seat check's S6 catch — a typo'd "AOT" read as a missing file
	// sends the agent looking for a fetch instead of fixing the key).
	if in.Artifact != "" && in.Artifact != "wasm" && in.Artifact != "aot" {
		return nil, driverPushOut{}, errWs(`xuansi_driver_push: artifact must be "wasm" or "aot", got %q — omit it for the manifest-driven default (the .wasm + .aot pair on a matching board)`, in.Artifact)
	}

	// best-known board identity: the refresh-fed board list resolves
	// the selector to mac6 (the manifest keys footprints by it); a
	// board the local list does not know (static-only face, a board
	// newer than the last refresh) falls back to the raw selector —
	// the daemon resolves authoritatively either way.
	board, resolved := in.Board, false
	if mac6, rerr := ts.ev.resolve(in.Board); rerr == nil {
		board, resolved = mac6, true
	}

	// version default: the manifest's ACTIVE one (rollback states its
	// version explicitly; a bare push re-deploys what is active).
	ver := in.Version
	wsMu.Lock()
	m, err := loadManifest(ts.ws.manifestPath())
	wsMu.Unlock()
	if err != nil {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %v", err)
	}
	if ver == "" {
		if act := m.Drivers[in.Name].Active; act != "" {
			ver = act
		} else {
			return nil, driverPushOut{}, errWs("xuansi_driver_push %s: no active version in the manifest — supply version, or push one explicitly to activate it", in.Name)
		}
	}
	if !wsVerRe.MatchString(ver) { // Active came from the manifest file: re-gate it too
		return nil, driverPushOut{}, errWs("xuansi_driver_push: version must match [a-z0-9.]+, got %q", ver)
	}

	// artifact resolution (B10 task 5): an EXPLICIT artifact key (P3's
	// F-5) pins exactly one extension; else the first extension with
	// a stored file wins (.lua → .wasm → .aot, pushExts) — a fetched
	// binary module pushes exactly like a stored Lua one, to the
	// extension-matching board path.
	var ext string
	var content []byte
	if in.Artifact != "" {
		ext = in.Artifact
		b, rerr := readFileShared(ts.ws.driverPath(in.Name, ver, ext)) // share-delete read (windows replace contract)
		if os.IsNotExist(rerr) {
			return nil, driverPushOut{}, errWs("xuansi_driver_push: %s.%s is not in the workspace — the explicit artifact push deploys exactly that file (xuansi_driver_fetch / xuansi_wasm_build land .wasm/.aot)", in.Name+"@"+ver, ext)
		}
		if rerr != nil {
			return nil, driverPushOut{}, errWs("xuansi_driver_push: %v", rerr)
		}
		content = b
	} else {
		for _, cand := range pushExts {
			b, rerr := readFileShared(ts.ws.driverPath(in.Name, ver, cand)) // share-delete read (windows replace contract)
			if rerr == nil {
				ext, content = cand, b
				break
			}
			if !os.IsNotExist(rerr) {
				return nil, driverPushOut{}, errWs("xuansi_driver_push: %v", rerr)
			}
		}
	}
	file := in.Name + "@" + ver
	if ext == "" {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %s.lua is not in the workspace — xuansi_ws_put it first (no .wasm/.aot artifact either; xuansi_driver_fetch stores those)", file)
	}
	file += "." + ext

	// the pairing decision (P3-T13, F-5): a BARE .wasm push whose
	// version carries an aot.<target> record for the BOARD's mapped
	// chip deploys the .wasm + .aot PAIR. The architecture source is
	// the hello-cached chip on the refresh-fed board list — no fresh
	// crossing (ruling 3): a board the list does not carry, or a chip
	// the map does not know, or a version without the record pairs
	// NOTHING — the conservative .wasm single, exactly the pre-T13
	// behavior, and the reply's paired=false never claims otherwise.
	// A record whose sha disagrees with the artifact on disk is a
	// REFUSAL (the corrupt-manifest discipline), not a guess.
	var aotContent []byte
	paired := false
	if in.Artifact == "" && ext == "wasm" && resolved {
		if target, ok := boardTarget(ts.ev.chip(board)); ok {
			if slot, ok := m.entry(in.Name).Files[ver].Aot[target]; ok {
				b, rerr := readFileShared(ts.ws.driverPath(in.Name, ver, "aot"))
				switch {
				case os.IsNotExist(rerr):
					// the record's file is gone: nothing to pair — the
					// .wasm single, honestly (ws_get/ws_list would not
					// see the file either)
				case rerr != nil:
					return nil, driverPushOut{}, errWs("xuansi_driver_push: %s.aot: %v", in.Name+"@"+ver, rerr)
				default:
					if got := sha256hex(b); got != slot.Sha256 {
						return nil, driverPushOut{}, errWs("xuansi_driver_push: manifest aot.%s says sha256 %s but %s.aot on disk hashes %s — the record and the artifact disagree (hand-edited file or manifest?); re-fetch the version (xuansi_driver_fetch) to resync; refusing to push an unverified pair", target, slot.Sha256, in.Name+"@"+ver, got)
					}
					aotContent = b
					paired = true
				}
			}
		}
	}

	// the load gate, enforced BEFORE the flash write, branched by
	// artifact (task 5): the Lua track keeps its §7 gate — first line
	// --@api N with N EXACTLY the board's bench.api_version — while a
	// binary module (.wasm/.aot) gates on the module magic the fw's
	// wasm loader itself checks (its compatibility axis is the module
	// ABI the loader validates, NOT bench.api_version, so no caps
	// crossing happens on this leg).
	var path string
	switch ext {
	case "lua":
		api, perr := parseAPIDirective(string(content))
		if perr != nil {
			return nil, driverPushOut{}, errWs("xuansi_driver_push: %s: %v — the board only loads a driver whose first line is \"--@api N\" matching its bench.api_version", file, perr)
		}
		want, cerr := ts.boardAPIVersion(ctx, board, resolved)
		if cerr != nil {
			return nil, driverPushOut{}, cerr
		}
		if api != want {
			return nil, driverPushOut{}, errWs("xuansi_driver_push: %s declares --@api %d but board %s reports bench.api_version %d — the board refuses to load it (exact match, no semver)", file, api, board, want)
		}
		path = drvBoardPath(in.Name, "lua")
	default:
		if gerr := gateModuleMagic(ext, content, file); gerr != nil {
			return nil, driverPushOut{}, gerr
		}
		path = drvBoardPath(in.Name, ext)
	}

	// the paired .aot gates on its own magic too — both legs are
	// refused BEFORE the first flash write (the whole pair or nothing).
	if paired {
		if gerr := gateModuleMagic("aot", aotContent, in.Name+"@"+ver+".aot"); gerr != nil {
			return nil, driverPushOut{}, gerr
		}
	}

	// write, verify (the board's own commit verdict), then (and only
	// then) the manifest. The WHOLE cycle rides wsMu (not just the
	// manifest half): a concurrent driver_pull's fs_rm of the same
	// path must not land between this fs_write and its manifest
	// record — pull/push serialize as one unit per board file, the
	// contract the B8 race test pins. P3-T13: a paired push rides the
	// SAME serialization for BOTH legs plus its rollback rm — the
	// pair lands or reverts as one unit against a pull too.
	sum := sha256hex(content)
	aotPath := drvBoardPath(in.Name, "aot")
	wsMu.Lock()
	defer wsMu.Unlock()

	if paired {
		// ruling 2's order: the .aot leg FIRST (the smaller failure
		// face) — a failed .aot leg leaves no residue at its path (a
		// refused or verdict-failed commit lands NOTHING; only the
		// fw's .part staging file can remain, kept by its own retry
		// contract, never shadowing the real path); then the .wasm,
		// whose failure rolls the WHOLE pair back out (the verified
		// .aot always goes; the .wasm's slot was never touched —
		// §4.5② made unverified-at-path impossible).
		if err := ts.pushArtifact(ctx, board, aotPath, aotContent, in.Holder); err != nil {
			return nil, driverPushOut{}, err
		}
		if err := ts.pushArtifact(ctx, board, path, content, in.Holder); err != nil {
			// rollback (ruling 2: pre-push slot state, honestly): the
			// .aot half goes — it landed verified moments ago; the
			// .wasm half never landed (an unverified commit refuses
			// the rename; at most its .part remains, which the fw
			// deliberately keeps for a from-off=0 retry).
			var stuck []string
			if _, rerr := ts.callHeld(ctx, server.Request{Type: "fs_rm", Board: board, Path: aotPath, Holder: in.Holder}); rerr != nil && !notFoundIsh(rerr) {
				stuck = append(stuck, fmt.Sprintf("the verified .aot at %s (%v)", aotPath, rerr))
			}
			if len(stuck) > 0 {
				return nil, driverPushOut{}, errWs("xuansi_driver_push: the paired push's .wasm leg failed (%v) AND the rollback could not clear %s — the board may hold the verified .aot half of this push; xuansi_driver_pull the driver before retrying; manifest untouched", err, strings.Join(stuck, "; "))
			}
			return nil, driverPushOut{}, errWs("xuansi_driver_push: the paired push's .wasm leg failed (%v) — the pair was rolled back to the pre-push slots (fs_rm %s; the .wasm leg itself never landed: a refused commit leaves only the fw's .part staging file, which the next push's off=0 reopen truncates); the board holds neither half of the new pair, manifest untouched", err, aotPath)
		}
	} else if err := ts.pushArtifact(ctx, board, path, content, in.Holder); err != nil {
		return nil, driverPushOut{}, err
	}

	// the F-5 sweep (an explicit single-artifact push): empty every
	// sibling slot a push could have written — driver_pull's mirror.
	// A stale .aot would SHADOW the fresh .wasm (the loader prefers
	// .aot), the foot-gun this kills; an already-empty sibling answers
	// the fw's api not-found and is expected noise, not a failure. A
	// REAL sweep failure refuses honestly with the manifest untouched
	// (error ⇒ manifest untouched, the verdict contract's shape) —
	// the error says the shield may still be armed.
	var swept []string
	if in.Artifact != "" {
		for _, sib := range pushExts {
			if sib == ext {
				continue
			}
			sp := drvBoardPath(in.Name, sib)
			if _, rerr := ts.callHeld(ctx, server.Request{Type: "fs_rm", Board: board, Path: sp, Holder: in.Holder}); rerr != nil {
				if notFoundIsh(rerr) {
					continue // already empty: the desired end state
				}
				return nil, driverPushOut{}, errWs("xuansi_driver_push: %s landed and verified, but the sibling sweep failed at %s (%v) — a stale artifact there may still shadow the pushed one (the loader prefers .aot); re-run the push or xuansi_driver_pull the driver; manifest untouched", path, sp, rerr)
			}
			swept = append(swept, sp)
		}
	}

	now := time.Now().UTC()
	m, err = loadManifest(ts.ws.manifestPath())
	if err != nil {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %v", err)
	}
	e := m.entry(in.Name)
	fmPush := e.Files[ver]
	// ALWAYS the disk-at-push-time values (single source): a file
	// modified after its put must not leave a stale put-time checksum —
	// the push just verified THESE bytes on the board, so these are the
	// bytes the history records (B7 minors item 1; the old fill-only-
	// when-empty guard desynced manifest vs disk). The version's own
	// aot.<target> record rides through untouched (it describes the
	// workspace artifact, not the board legs).
	fmPush.Sha256, fmPush.Size = sum, len(content)
	fmPush.PushedAt = now
	if fmPush.Boards == nil {
		fmPush.Boards = map[string]time.Time{}
	}
	fmPush.Boards[board] = now
	e.Files[ver] = fmPush
	e.Active = ver
	m.Drivers[in.Name] = e
	if err := writeManifestAtomic(ts.ws.manifestPath(), m); err != nil {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %v", err)
	}
	out := driverPushOut{Board: board, Path: path, Sha256: sum, Bytes: len(content), VerifiedBy: "commit-sha256", Swept: swept}
	// the kind ladder (one vocabulary at the seat): the index-level
	// Kind the fetch persisted, else the pushed version's own meta
	// release claim; hand-put versions carry neither — honest absence.
	if k := m.Drivers[in.Name].Kind; k != "" {
		out.Kind = k
	} else {
		out.Kind = m.entry(in.Name).Files[ver].Release
	}
	if paired {
		out.Paired, out.AotPath, out.AotSha256 = true, aotPath, sha256hex(aotContent)
	}
	return nil, out, nil
}

// pushArtifact is one board leg of a push: the held fs_write carrying
// the payload AND its sha256 — the verified path (B15 §4.5②). The
// daemon lands the file through fs.write_part × N + one
// fs.write_commit carrying this digest; the board hashes chunk-by-
// chunk as it writes and its commit verdict IS the verification (the
// B8-era post-push byte read-back is retired — half the transfer). An
// error here means the leg did NOT land: a board refusal crosses
// verbatim (the commit's own msg), and a verdict mismatch leaves only
// the fw's .part staging file (kept deliberately for a from-off=0
// retry, never shadowing the real path). The CALLER owns
// serialization (wsMu) and any rollback or sweep around the leg.
func (ts *toolset) pushArtifact(ctx context.Context, board, path string, content []byte, holder string) error {
	_, err := ts.callHeld(ctx, server.Request{
		Type: "fs_write", Board: board, Path: path, Data: content,
		Sha256: sha256hex(content), Holder: holder,
	})
	return err
}

// notFoundIsh recognizes the fw's absent-file vocabularies in an fs_rm
// refusal: a slot that never held the artifact is already the sweep's
// desired end state, the same expected noise driverPull tolerates on its
// un-held slots. Two spellings are real on the wire: the READ path says
// "'path' not found (No such file or directory)" (xs_fs.c's stat branch)
// while the REMOVE path — the one fs_rm actually rides — says
// "rm 'path' failed (No such file or directory)" (xs_fs_remove's unlink
// branch, POSIX strerror). The remove spelling carries no "not found"
// substring at all; the P3-T15 real-board seat check caught the first
// version of this helper matching only the read-path text (the unit-test
// fake board had embedded that spelling), which made every explicit-
// artifact push on a clean board fail its sweep AFTER landing the
// artifact. Both spellings are tolerated; anything else is a true error.
func notFoundIsh(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "not found") ||
		strings.Contains(s, "No such file or directory")
}

// boardAPIVersion answers the board's bench.api_version for the push
// gate: the dynamic refresh's cached caps when one is fresh, else ONE
// holder-less op caps — the same crossing the refresh itself makes
// (tools_dynamic.go: holder-less keeps the task-8 grace-adopt story;
// the daemon rides the connection's binding). Only a locally-resolved
// board (resolved) can hit the mac6-keyed cache or feed it.
func (ts *toolset) boardAPIVersion(ctx context.Context, board string, resolved bool) (int, error) {
	if resolved {
		if v, ok := ts.dyn.capsFresh(board); ok {
			return v, nil
		}
	}
	res, err := ts.call(ctx, server.Request{Type: "op", Board: board, Op: "caps"})
	if err != nil {
		return 0, err
	}
	ver, _, ok := parseCaps(res)
	if !ok {
		return 0, errWs("xuansi_driver_push %s: caps reply is malformed (no api_version) — cannot gate the driver", board)
	}
	if resolved {
		ts.dyn.capsStore(board, ver)
	}
	return ver, nil
}

// driverPull removes the board's driver files through the fs_rm
// request (B8: the pipeline's removal path — the b7 HIL cleaned /drv
// out-of-pipeline; B10-T8: generalized off the .lua-only scope) and
// clears the manifest's Active: the board has no effective driver to
// deploy anymore. The fs_rm path is type-agnostic board-side, so the
// pull walks EVERY extension slot a push could have written
// (pushExts: .lua → .wasm → .aot) — a push only ever wrote the
// resolved artifact's ext, so at most one slot holds the file and the
// other slots' api not-found refusals are expected noise, tolerated
// when any slot removed; the FIRST removed path is the reported one.
// History is never rewritten — the Files entries and per-board push
// footprints STAY (rollback = re-push the version explicitly), and
// the whole removal batch + manifest cycle rides wsMu like every
// other manifest cycle, so a pull racing a push serializes instead of
// tearing the file. A driver whose board files are ALL already absent
// surfaces the board's api not-found VERBATIM (the .lua slot's, the
// first probed) and rewrites nothing (the pull did not remove
// anything, so Active stays).
func (ts *toolset) driverPull(ctx context.Context, _ *mcpsdk.CallToolRequest, in driverPullIn) (*mcpsdk.CallToolResult, driverPullOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, driverPullOut{}, errWs("xuansi_driver_pull: name must match [a-z0-9_-]+, got %q", in.Name)
	}

	// the removals first: only a file the board actually removed clears
	// Active (when no slot held the file, the FIRST slot's error — the
	// api not-found on an absent file — crosses verbatim and the
	// manifest below never runs). The batch rides wsMu TOGETHER with
	// the manifest cycle: a concurrent driver_push's verified fs_write
	// of the same path must not interleave with these fs_rms — the
	// whole board-file mutation + manifest cycle is one serialized
	// unit (the contract TestDriverPullConcurrentWithPush pins).
	wsMu.Lock()
	defer wsMu.Unlock()
	var removed string
	var firstErr error
	for _, ext := range pushExts {
		path := drvBoardPath(in.Name, ext)
		if _, err := ts.callHeld(ctx, server.Request{Type: "fs_rm", Board: in.Board, Path: path, Holder: in.Holder}); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if removed == "" {
			removed = path
		}
	}
	if removed == "" {
		return nil, driverPullOut{}, firstErr
	}

	m, err := loadManifest(ts.ws.manifestPath())
	if err != nil {
		return nil, driverPullOut{}, errWs("xuansi_driver_pull: %v", err)
	}
	if _, exists := m.Drivers[in.Name]; exists {
		e := m.entry(in.Name)
		e.Active = "" // footprints deliberately kept — history never lies
		m.Drivers[in.Name] = e
		if err := writeManifestAtomic(ts.ws.manifestPath(), m); err != nil {
			return nil, driverPullOut{}, errWs("xuansi_driver_pull: %v", err)
		}
	}
	// no manifest entry: the board file is gone and there was no Active
	// to clear — writing an empty entry would be noise, not history.
	return nil, driverPullOut{Pulled: true, Path: removed}, nil
}
