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
// (any size — the 16 KiB cap is a BOARD concern, checked at push
// time, so a fat version can sit in the workspace until it slims),
// xuansi_ws_list / xuansi_ws_get read the store back, and
// xuansi_driver_push runs the gated pipeline
//
//	read file → --@api gate → caps → 16384B cap → fs_write
//	/drv/<name>.lua → fs_read back → sha256 compare → manifest
//
// The manifest records the push ONLY after the read-back verifies
// byte-exact — a corrupt board write never becomes history. B8 task 1
// appends the removal half, xuansi_driver_pull (fs_rm of the effective
// file + Active cleared, footprints kept), after the capture tool.

import (
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

// driverPath is drivers/<name>@<ver>.lua on sanitized parts only.
func (w *wsState) driverPath(name, ver string) string {
	return filepath.Join(w.root, "drivers", name+"@"+ver+".lua")
}

// drvBoardPath is the board's EFFECTIVE driver file the push
// overwrites — always /drv/<sanitized name>.lua, never raw input.
func drvBoardPath(name string) string { return "/drv/" + name + ".lua" }

// name/version sanitization (the path-traversal guard): name
// [a-z0-9_-]+, version [a-z0-9.]+; anything else is a local api-class
// refusal.
var (
	wsNameRe = regexp.MustCompile(`^[a-z0-9_-]+$`)
	wsVerRe  = regexp.MustCompile(`^[a-z0-9.]+$`)
)

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
	Size   int    `json:"size" jsonschema:"stored bytes (the board's 16384B cap applies at push time, not here)"`
}

type wsListIn struct{}

type wsListOut struct {
	Drivers []wsDriverSummary `json:"drivers" jsonschema:"one entry per driver, sorted by name"`
}

type wsDriverSummary struct {
	Name     string          `json:"name"`
	Active   string          `json:"active,omitempty" jsonschema:"the version a bare xuansi_driver_push deploys; absent when none was ever pushed"`
	Versions []wsVersionInfo `json:"versions" jsonschema:"every stored version, sorted"`
}

type wsVersionInfo struct {
	Version string `json:"version"`
	Size    int    `json:"size"`
	Sha256  string `json:"sha256"`
}

type wsGetIn struct {
	Name    string `json:"name" jsonschema:"driver name, lowercase [a-z0-9_-]+"`
	Version string `json:"version" jsonschema:"version string, [a-z0-9.]+"`
}

type wsGetOut struct {
	Content string `json:"content" jsonschema:"the stored driver source, verbatim"`
	Sha256  string `json:"sha256" jsonschema:"sha256 of the returned content"`
}

type driverPushIn struct {
	Board   string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Name    string `json:"name" jsonschema:"driver name, lowercase [a-z0-9_-]+ — the board's effective file /drv/<name>.lua is overwritten"`
	Version string `json:"version,omitempty" jsonschema:"omit to re-deploy the manifest's ACTIVE version (error when none is active). Rollback = push the older version explicitly; fleet = push each board"`
	Holder  string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type driverPushOut struct {
	Board  string `json:"board" jsonschema:"the board the push landed on: resolved mac6 when the local board list knows it, else the raw selector"`
	Path   string `json:"path" jsonschema:"the board's effective driver file that was overwritten"`
	Sha256 string `json:"sha256" jsonschema:"sha256 of the pushed bytes, verified by fs_read-back"`
	Bytes  int    `json:"bytes" jsonschema:"bytes written"`
}

type driverPullIn struct {
	Board  string `json:"board" jsonschema:"board selector: mac6 / alias / hostname unique prefix (e.g. \"d75884\" or \"bench1\"), resolved daemon-side; an ambiguous or unknown prefix returns an error listing the candidates"`
	Name   string `json:"name" jsonschema:"driver name, lowercase [a-z0-9_-]+ — the board's effective file /drv/<name>.lua is removed"`
	Holder string `json:"holder,omitempty" jsonschema:"lease holder identity. Omit to ride this process's holder (a generated \"mcp-…\" id): the first holder used on the daemon connection binds it and every later call rides it. Supply your own ONLY to adopt a lease across sessions/processes (grace-adopt) — supply it on the FIRST call (e.g. xuansi_lease), not midway"`
}

type driverPullOut struct {
	Pulled bool   `json:"pulled" jsonschema:"true when the board file was removed and the manifest's Active cleared"`
	Path   string `json:"path" jsonschema:"the board's effective driver file that was removed"`
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
	path := ts.ws.driverPath(in.Name, in.Version)
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
		s := wsDriverSummary{Name: n, Active: e.Active, Versions: make([]wsVersionInfo, 0, len(vers))}
		for _, v := range vers {
			fm := e.Files[v]
			s.Versions = append(s.Versions, wsVersionInfo{Version: v, Size: fm.Size, Sha256: fm.Sha256})
		}
		out.Drivers = append(out.Drivers, s)
	}
	return nil, out, nil
}

// wsGet reads one stored version back — the workspace keeps every
// version, so rollback's older file stays retrievable.
func (ts *toolset) wsGet(_ context.Context, _ *mcpsdk.CallToolRequest, in wsGetIn) (*mcpsdk.CallToolResult, wsGetOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, wsGetOut{}, errWs("xuansi_ws_get: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	if !wsVerRe.MatchString(in.Version) {
		return nil, wsGetOut{}, errWs("xuansi_ws_get: version must match [a-z0-9.]+, got %q", in.Version)
	}
	b, err := os.ReadFile(ts.ws.driverPath(in.Name, in.Version))
	if os.IsNotExist(err) {
		return nil, wsGetOut{}, errWs("xuansi_ws_get: %s@%s.lua is not in the workspace — xuansi_ws_put it first", in.Name, in.Version)
	}
	if err != nil {
		return nil, wsGetOut{}, errWs("xuansi_ws_get: %v", err)
	}
	return nil, wsGetOut{Content: string(b), Sha256: sha256hex(b)}, nil
}

// driverPush deploys one workspace version onto a board (the pipeline
// is the file comment's gated chain). The daemon crossings: a
// holder-less op caps (like the refresh's own call — never pre-binds
// the connection), a HELD fs_write of /drv/<name>.lua (the lease the
// agent took with xuansi_lease), a holder-less fs_read back. Only a
// byte-exact read-back updates the manifest.
func (ts *toolset) driverPush(ctx context.Context, _ *mcpsdk.CallToolRequest, in driverPushIn) (*mcpsdk.CallToolResult, driverPushOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	if in.Version != "" && !wsVerRe.MatchString(in.Version) {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: version must match [a-z0-9.]+, got %q", in.Version)
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

	file := in.Name + "@" + ver + ".lua"
	content, err := os.ReadFile(ts.ws.driverPath(in.Name, ver))
	if os.IsNotExist(err) {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %s is not in the workspace — xuansi_ws_put it first", file)
	}
	if err != nil {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %v", err)
	}

	// the fw's load gate, enforced BEFORE the flash write (§7): first
	// line --@api N with N EXACTLY the board's bench.api_version — the
	// board would refuse to load anything else, so we refuse to write
	// it and the flash cycle is not wasted.
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
	path := drvBoardPath(in.Name)
	if len(content) > fsWriteMax {
		return nil, driverPushOut{}, errFsTooLarge(path, len(content))
	}

	// write, read back, verify — then (and only then) the manifest.
	// The WHOLE cycle rides wsMu (not just the manifest half): a
	// concurrent driver_pull's fs_rm of the same path must not land
	// between this fs_write and its fs_read-back (the read-back would
	// see the removal and fail the sha check) — pull/push serialize as
	// one unit per board file, the contract the B8 race test pins.
	sum := sha256hex(content)
	wsMu.Lock()
	defer wsMu.Unlock()
	if _, err := ts.callHeld(ctx, server.Request{Type: "fs_write", Board: board, Path: path, Data: content, Holder: in.Holder}); err != nil {
		return nil, driverPushOut{}, err
	}
	readRes, rerr := ts.call(ctx, server.Request{Type: "fs_read", Board: board, Path: path})
	if rerr != nil {
		return nil, driverPushOut{}, rerr
	}
	back, derr := base64.StdEncoding.DecodeString(str(readRes, "data"))
	got := sha256hex(back)
	if derr != nil || got != sum {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %s on board %s: read-back sha256 %s != pushed %s — the board's file differs; manifest untouched", path, board, got, sum)
	}

	now := time.Now().UTC()
	m, err = loadManifest(ts.ws.manifestPath())
	if err != nil {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %v", err)
	}
	e := m.entry(in.Name)
	fm := e.Files[ver]
	// ALWAYS the disk-at-push-time values (single source): a file
	// modified after its put must not leave a stale put-time checksum —
	// the push just verified THESE bytes on the board, so these are the
	// bytes the history records (B7 minors item 1; the old fill-only-
	// when-empty guard desynced manifest vs disk).
	fm.Sha256, fm.Size = sum, len(content)
	fm.PushedAt = now
	if fm.Boards == nil {
		fm.Boards = map[string]time.Time{}
	}
	fm.Boards[board] = now
	e.Files[ver] = fm
	e.Active = ver
	m.Drivers[in.Name] = e
	if err := writeManifestAtomic(ts.ws.manifestPath(), m); err != nil {
		return nil, driverPushOut{}, errWs("xuansi_driver_push: %v", err)
	}
	return nil, driverPushOut{Board: board, Path: path, Sha256: sum, Bytes: len(content)}, nil
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

// driverPull removes the board's effective /drv/<name>.lua through the
// fs_rm request (B8: the pipeline's removal path — the b7 HIL cleaned
// /drv out-of-pipeline) and clears the manifest's Active: the board has
// no effective driver to deploy anymore. History is never rewritten —
// the Files entries and per-board push footprints STAY (rollback =
// re-push the version explicitly), and the whole read-modify-write
// rides wsMu like every other manifest cycle, so a pull racing a push
// serializes instead of tearing the file. A board file that is already
// absent surfaces the board's api not-found VERBATIM and rewrites
// nothing (the pull did not remove anything, so Active stays).
func (ts *toolset) driverPull(ctx context.Context, _ *mcpsdk.CallToolRequest, in driverPullIn) (*mcpsdk.CallToolResult, driverPullOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, driverPullOut{}, errWs("xuansi_driver_pull: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	path := drvBoardPath(in.Name)

	// the removal first: only a file the board actually removed clears
	// Active (its error — the api not-found on an absent file — crosses
	// verbatim and the manifest below never runs). The removal rides
	// wsMu TOGETHER with the manifest cycle: a concurrent driver_push's
	// fs_write + fs_read-back of the same path must not interleave with
	// this fs_rm (the read-back would see the removal and fail) — the
	// whole board-file mutation + manifest cycle is one serialized unit
	// (the contract TestDriverPullConcurrentWithPush pins).
	wsMu.Lock()
	defer wsMu.Unlock()
	if _, err := ts.callHeld(ctx, server.Request{Type: "fs_rm", Board: in.Board, Path: path, Holder: in.Holder}); err != nil {
		return nil, driverPullOut{}, err
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
	return nil, driverPullOut{Pulled: true, Path: path}, nil
}
