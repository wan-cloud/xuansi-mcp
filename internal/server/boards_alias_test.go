package server

// boards_alias_test.go — the alias write face at the internal API:
// boards_alias {board, alias} → {aliased, alias} names a board in the
// daemon's boards.conf (live immediately, durable across a restart),
// and boards_unalias {board} → {unaliased} removes a pure alias line.
// Every environment here runs on a REAL temp boards.conf through the
// production wiring shape — the file seeds the registry AND is the
// file the handlers edit — so each assertion about the file is about
// the bytes a restart reads back.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"xuansi/internal/discover"
	"xuansi/internal/testutil"
)

// aliasEnv is a serving daemon over a real boards file: the registry
// is seeded from that file the way New seeds it in production and is
// told where the file lives, so the alias handlers edit the very file
// a restart re-reads.
type aliasEnv struct {
	*env
	reg  *discover.Registry
	conf string
}

// newAliasEnv writes conf to a fresh temp boards.conf and serves over it.
func newAliasEnv(t *testing.T, conf string, boards ...discover.Board) *aliasEnv {
	t.Helper()
	path := filepath.Join(t.TempDir(), "boards.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	return newAliasEnvAt(t, path, boards...)
}

// newAliasEnvAt serves a daemon over an EXISTING boards file — a
// restart is one more call with the same path.
func newAliasEnvAt(t *testing.T, conf string, boards ...discover.Board) *aliasEnv {
	t.Helper()
	reg := fileRegistry(t, conf, boards)
	return &aliasEnv{env: newEnvReg(t, 10*time.Second, 0, reg, boards...), reg: reg, conf: conf}
}

// fileRegistry is the production seed shape (New's default-registry
// branch): the file's three maps into a registry that knows its file.
func fileRegistry(t *testing.T, conf string, boards []discover.Board) *discover.Registry {
	t.Helper()
	aliases, pins, models, err := discover.LoadBoardsFile(conf)
	if err != nil {
		t.Fatalf("LoadBoardsFile: %v", err)
	}
	return discover.NewRegistry(aliases, pins,
		discover.WithModels(models),
		discover.WithBoardsFile(conf),
		discover.WithUSB(func(context.Context) ([]discover.Board, error) { return boards, nil }),
		discover.WithMDNS(func(context.Context) ([]discover.Board, error) { return boards, nil }),
	)
}

// TestBoardsAliasSetUnaliasSurviveRestart: the whole chain on a real
// file — set → list_boards shows it at once → the alias is a live
// selector → unalias deletes the pure line → a RESTARTED daemon reads
// the same world (the removal held) → a later set survives its own
// restart. The file's other line — an endpoint pin plus a model= token
// — is byte-identical from start to finish.
func TestBoardsAliasSetUnaliasSurviveRestart(t *testing.T) {
	const header = "# bench boards\ncam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\n"
	const conf = header + "bench=d75884\n"
	boards := []discover.Board{
		mkBoard("d75884", "xuansi-d75884.local"),
		mkBoard("1a2b3c", "xuansi-1a2b3c.local"),
		mkBoard("d75885", "xuansi-d75885.local"),
	}
	confPath := filepath.Join(t.TempDir(), "boards.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	read := func() string {
		t.Helper()
		b, err := os.ReadFile(confPath)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	// a restart: a new daemon over the same file
	boot := func() (*aliasEnv, *sockClient) {
		e := newAliasEnvAt(t, confPath, boards...)
		return e, dialSock(t, e.sock)
	}

	e, c := boot()

	// set: the reply echoes the resolved board and the alias as written
	rep, _ := c.call(1, map[string]any{"type": "boards_alias", "board": "d75884", "alias": "left"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("boards_alias: %#v", rep)
	}
	res := asMap(rep["result"])
	if asStr(res["aliased"]) != "d75884" || asStr(res["alias"]) != "left" {
		t.Fatalf("boards_alias reply = %#v, want {aliased:d75884, alias:left}", res)
	}
	// live immediately — no sweep, no restart
	if got := asStr(boardRowOf(t, c, 2, "d75884")["alias"]); got != "left" {
		t.Fatalf("alias right after boards_alias = %q, want left", got)
	}
	cam := boardRowOf(t, c, 3, "1a2b3c")
	if asStr(cam["alias"]) != "cam" || asStr(cam["model"]) != "esp32s3-dev" || asInt(cam["port"]) != 7777 {
		t.Fatalf("the other board's row moved: %#v", cam)
	}
	// the alias is a selector right away
	if got := asStr(statusBoardOf(t, c, 4, "left")["mac6"]); got != "d75884" {
		t.Fatalf("board_status(left) resolved %q", got)
	}
	if got := read(); got != header+"left=d75884\n" {
		t.Fatalf("file = %q", got)
	}

	// rename, addressed by the alias being replaced
	rep, _ = c.call(5, map[string]any{"type": "boards_alias", "board": "left", "alias": "left2"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("rename through the alias: %#v", rep)
	}
	if got := read(); got != header+"left2=d75884\n" {
		t.Fatalf("file after the rename = %q", got)
	}

	// unalias: the pure line goes
	rep, _ = c.call(6, map[string]any{"type": "boards_unalias", "board": "left2"})
	if asMap(rep["error"]) != nil || asStr(asMap(rep["result"])["unaliased"]) != "d75884" {
		t.Fatalf("boards_unalias: %#v", rep)
	}
	if got := asStr(boardRowOf(t, c, 7, "d75884")["alias"]); got != "" {
		t.Fatalf("alias after boards_unalias = %q, want none", got)
	}
	if got := read(); got != header {
		t.Fatalf("file after unalias = %q, want %q", got, header)
	}
	// …and the selector that was the alias is gone
	rep, _ = c.call(8, map[string]any{"type": "board_status", "board": "left2"})
	if asStr(errOf(t, rep)["class"]) != "no_board" {
		t.Fatalf("board_status(left2) after the unalias: %#v", rep)
	}

	// RESTART: a new daemon over the same file reads the same world
	e.srv.Close()
	e, c = boot()
	if got := asStr(boardRowOf(t, c, 1, "d75884")["alias"]); got != "" {
		t.Fatalf("a restart resurrected the cleared alias: %q", got)
	}
	checkModelPair(t, boardRowOf(t, c, 2, "1a2b3c"), "list_boards 1a2b3c after the restart", "esp32s3-dev", "conf")

	// a set, then a restart: still there
	rep, _ = c.call(3, map[string]any{"type": "boards_alias", "board": "d75885", "alias": "spare"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("boards_alias after the restart: %#v", rep)
	}
	if got := read(); got != header+"spare=d75885\n" {
		t.Fatalf("file = %q", got)
	}
	e.srv.Close()
	_, c = boot()
	if got := asStr(boardRowOf(t, c, 1, "d75885")["alias"]); got != "spare" {
		t.Fatalf("a restart lost the alias: %q", got)
	}
	if got := read(); got != header+"spare=d75885\n" {
		t.Fatalf("file after the second restart = %q", got)
	}
}

// TestBoardsAliasRefusals: every write-time refusal, its wire class
// and its coach — and the proof that a refusal wrote nothing (the file
// keeps its bytes AND its identity: no rewrite, rename-replace
// included) and moved nothing in the world.
func TestBoardsAliasRefusals(t *testing.T) {
	const conf = "# bench\nbench=d75884 model=goouuu-s3cam-v1.3\ncam=1a2b3c@10.1.11.204:7777\n"
	e := newAliasEnv(t, conf,
		mkBoard("d75884", "xuansi-d75884.local"),
		mkBoard("1a2b3c", "xuansi-1a2b3c.local"),
		mkBoard("d75885", "xuansi-d75885.local"))
	c := dialSock(t, e.sock)
	before, err := os.ReadFile(e.conf)
	if err != nil {
		t.Fatal(err)
	}
	beforeInfo, err := os.Stat(e.conf)
	if err != nil {
		t.Fatal(err)
	}
	assertUntouched := func(what string) {
		t.Helper()
		after, err := os.ReadFile(e.conf)
		if err != nil {
			t.Fatal(err)
		}
		afterInfo, err := os.Stat(e.conf)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(after, before) {
			t.Fatalf("%s: the file changed:\n before %q\n after  %q", what, before, after)
		}
		if !os.SameFile(beforeInfo, afterInfo) {
			t.Fatalf("%s: the file was rewritten onto a new file — a request that writes nothing must not rewrite", what)
		}
	}

	type refusal struct {
		name       string
		req        map[string]any
		class, why string
	}
	for _, tc := range []refusal{
		{"an unknown board", map[string]any{"type": "boards_alias", "board": "nope", "alias": "x"}, "no_board", "no board matches"},
		{"an ambiguous selector", map[string]any{"type": "boards_unalias", "board": "xuansi"}, "ambiguous", "ambiguous"},
		{"an alias outside the charset", map[string]any{"type": "boards_alias", "board": "d75885", "alias": "Bad Alias"}, "server", "^[a-z0-9][a-z0-9_-]{0,31}$"},
		{"an alias another board holds", map[string]any{"type": "boards_alias", "board": "d75885", "alias": "cam"}, "server", "is already board 1a2b3c's alias"},
		{"an alias that is a mac6 prefix", map[string]any{"type": "boards_alias", "board": "d75885", "alias": "d7"}, "server", "is a prefix of known mac6 d75884"},
		{"an alias that is a hostname prefix", map[string]any{"type": "boards_alias", "board": "d75885", "alias": "xuansi"}, "server", `is a prefix of board 1a2b3c's hostname "xuansi-1a2b3c.local"`},
		{"unaliasing a line that carries a model=", map[string]any{"type": "boards_unalias", "board": "d75884"}, "server", "rename the name to the board's mac6"},
		{"unaliasing a line that carries an @endpoint", map[string]any{"type": "boards_unalias", "board": "cam"}, "server", "rename the name to the board's mac6"},
	} {
		rep, _ := c.call(1, tc.req)
		er := errOf(t, rep)
		if asStr(er["class"]) != tc.class {
			t.Errorf("%s: class = %q, want %q (%#v)", tc.name, er["class"], tc.class, er)
		}
		if !strings.Contains(asStr(er["msg"]), tc.why) {
			t.Errorf("%s: msg = %q, want it to name %q", tc.name, er["msg"], tc.why)
		}
	}
	assertUntouched("after the refusals")
	for mac6, alias := range map[string]string{"d75884": "bench", "1a2b3c": "cam"} {
		if got := asStr(boardRowOf(t, c, 2, mac6)["alias"]); got != alias {
			t.Errorf("a refusal moved board %s's alias to %q, want %q", mac6, got, alias)
		}
	}

	// idempotent-shaped: unaliasing a board that was never aliased
	// answers success — and writes nothing
	rep, _ := c.call(3, map[string]any{"type": "boards_unalias", "board": "d75885"})
	if asMap(rep["error"]) != nil || asStr(asMap(rep["result"])["unaliased"]) != "d75885" {
		t.Fatalf("unalias of a never-aliased board: %#v", rep)
	}
	assertUntouched("after the idempotent unalias")
}

// TestBoardsUnaliasCoachNamesTheSafeEdit: a boards.conf line needs its
// name — without it the line's @endpoint and model= parse to nothing
// at the next start. So the unalias refusal must coach the ONE edit
// that clears an alias off a payload line and keeps the payload
// (rename the name to the board's own mac6), never an edit that loses
// it — and that edit is one boards_alias call away, with the result
// identical after a restart.
func TestBoardsUnaliasCoachNamesTheSafeEdit(t *testing.T) {
	const payload = "@10.1.11.204:7777 model=esp32s3-dev"
	conf := "# rig\ncam=1a2b3c" + payload + "\n"
	board := mkBoard("1a2b3c", "xuansi-1a2b3c.local")
	e := newAliasEnv(t, conf, board)
	c := dialSock(t, e.sock)

	rep, _ := c.call(1, map[string]any{"type": "boards_unalias", "board": "cam"})
	er := errOf(t, rep)
	msg := asStr(er["msg"])
	if asStr(er["class"]) != "server" {
		t.Fatalf("class = %q, want server", er["class"])
	}
	lower := strings.ToLower(msg)
	for _, bad := range []string{"delete the name", "delete the line", "delete it", "remove the name", "remove the line", "by deleting", "deleting the name", "deleting the line", "deleting one"} {
		if strings.Contains(lower, bad) {
			t.Errorf("the refusal instructs the destructive edit (%q):\n %q", bad, msg)
		}
	}
	for _, safe := range []string{"rename the name to the board's mac6", `"cam" → "1a2b3c"`, "or by setting the alias to 1a2b3c", e.conf} {
		if !strings.Contains(msg, safe) {
			t.Errorf("the refusal lacks the safe recipe %q:\n %q", safe, msg)
		}
	}
	if got, _ := os.ReadFile(e.conf); !bytes.Equal(got, []byte(conf)) {
		t.Fatalf("the refusal wrote: %q", got)
	}

	// the recipe, from the same seat: the alias set to the board's own mac6
	rep, _ = c.call(2, map[string]any{"type": "boards_alias", "board": "cam", "alias": "1a2b3c"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("boards_alias{board: cam, alias: 1a2b3c}: %#v", rep)
	}
	if got, _ := os.ReadFile(e.conf); !bytes.Equal(got, []byte("# rig\n1a2b3c=1a2b3c"+payload+"\n")) {
		t.Fatalf("file = %q, want the name renamed and the payload untouched", got)
	}
	check := func(c *sockClient, id int64, where string) {
		t.Helper()
		row := boardRowOf(t, c, id, "1a2b3c")
		if asStr(row["alias"]) != "1a2b3c" || asInt(row["port"]) != 7777 || asStr(row["ip"]) != "10.1.11.204" {
			t.Errorf("%s: row = %#v, want the board answering to its mac6 with the pin intact", where, row)
		}
		checkModelPair(t, row, where, "esp32s3-dev", "conf")
	}
	check(c, 3, "running daemon")

	// …and the recipe is the END of it: a line already named by the
	// mac6 has no alias left to clear. boards_unalias is then the
	// idempotent success the Desc promises — never the refusal again
	// with a coach that goes nowhere (`"1a2b3c" → "1a2b3c"` was a
	// terminal loop for an agent following it) — and nothing is
	// written: the line keeps its payload, and the board goes on
	// answering to its mac6 exactly as before the call.
	renamed := "# rig\n1a2b3c=1a2b3c" + payload + "\n"
	for id := int64(4); id <= 5; id++ { // twice: the second answers the same
		rep, _ = c.call(id, map[string]any{"type": "boards_unalias", "board": "1a2b3c"})
		if asMap(rep["error"]) != nil || asStr(asMap(rep["result"])["unaliased"]) != "1a2b3c" {
			t.Fatalf("boards_unalias after the recipe (call %d) = %#v, want {unaliased: 1a2b3c}", id, rep)
		}
		if got, _ := os.ReadFile(e.conf); !bytes.Equal(got, []byte(renamed)) {
			t.Fatalf("the idempotent unalias wrote: %q", got)
		}
	}
	check(c, 6, "running daemon, after the idempotent unalias")

	// a restart reads exactly the same
	e.srv.Close()
	e2 := newAliasEnvAt(t, e.conf, board)
	check(dialSock(t, e2.sock), 1, "after a restart")
}

// TestBoardsAliasNeedsABoardsFile: a registry that was never told
// where its file lives cannot write an alias — an honest refusal
// naming the flag, never a silent in-memory-only alias.
func TestBoardsAliasNeedsABoardsFile(t *testing.T) {
	e := newEnv(t, nil, mkBoard("d75884", "xuansi-d75884.local"))
	c := dialSock(t, e.sock)
	for _, req := range []map[string]any{
		{"type": "boards_alias", "board": "d75884", "alias": "bench"},
		{"type": "boards_unalias", "board": "d75884"},
	} {
		rep, _ := c.call(1, req)
		er := errOf(t, rep)
		if asStr(er["class"]) != "server" || !strings.Contains(asStr(er["msg"]), "--boards-file") {
			t.Errorf("%s: error = %#v, want a server-class refusal naming --boards-file", req["type"], er)
		}
	}
	if got := asStr(boardRowOf(t, c, 2, "d75884")["alias"]); got != "" {
		t.Errorf("a refused alias reached the board view: %q", got)
	}
}

// TestNewWiresBoardsFileForAliasWrites: the production path —
// Opts.Registry nil, the `serve` shape. New resolves the boards file,
// seeds the registry from it AND hands the registry the path; a
// dropped WithBoardsFile on that line would leave every
// injected-registry test green while no production alias write could
// ever land. No Start, so no discovery runs: a runtime Pin lands the
// entry.
func TestNewWiresBoardsFileForAliasWrites(t *testing.T) {
	dir := t.TempDir()
	conf := filepath.Join(dir, "boards.conf")
	if err := os.WriteFile(conf, []byte("bench=d75884\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts := Opts{
		BoardsFile:       conf,
		SockPath:         filepath.Join(testutil.ShortSockDir(t), "d.sock"),
		BootRecordsDir:   filepath.Join(dir, "bootrec"),
		VaultDir:         filepath.Join(dir, "vault"),
		CaptureRoot:      filepath.Join(dir, "captures"),
		DisableProvision: true,
		Log:              newTestLogf(t, "srv: "),
	}
	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(srv.Close)
	if got := srv.reg.BoardsFile(); got != conf {
		t.Fatalf("the registry's boards file = %q, want the resolved Opts.BoardsFile %q", got, conf)
	}
	if err := srv.reg.Pin("d75884", "10.1.11.204", 7777); err != nil {
		t.Fatalf("Pin: %v", err)
	}

	res, err := srv.hBoardsAlias(&Request{Board: "bench", Alias: "left"}) // addressed by the file's own alias
	if err != nil {
		t.Fatalf("hBoardsAlias: %v", err)
	}
	if m := asMap(res); asStr(m["aliased"]) != "d75884" || asStr(m["alias"]) != "left" {
		t.Fatalf("reply = %#v", res)
	}
	if got, _ := os.ReadFile(conf); !bytes.Equal(got, []byte("left=d75884\n")) {
		t.Fatalf("file = %q", got)
	}

	// a restart, still through the production wiring: the file is what counts
	srv.Close()
	restarted, err := New(opts)
	if err != nil {
		t.Fatalf("New (restart): %v", err)
	}
	t.Cleanup(restarted.Close)
	if err := restarted.reg.Pin("d75884", "10.1.11.204", 7777); err != nil {
		t.Fatal(err)
	}
	b, err := restarted.reg.Resolve("left")
	if err != nil || b.MAC6 != "d75884" {
		t.Fatalf("the restarted daemon resolves left = %v, %v; want d75884", b.MAC6, err)
	}
	if _, err := restarted.hBoardsUnalias(&Request{Board: "left"}); err != nil {
		t.Fatalf("hBoardsUnalias: %v", err)
	}
	if got, _ := os.ReadFile(conf); len(got) != 0 {
		t.Fatalf("file after unalias = %q, want empty", got)
	}
}
