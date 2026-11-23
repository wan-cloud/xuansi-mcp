package mcp

// boards_alias_test.go — the alias write face's MCP half: the two
// static tools' request crossing and reply shape, the required-field
// split, the daemon's refusal crossing as the tool's error text — and
// the seat's own restart proof: REAL daemons over a REAL temp
// boards.conf, driven only through the MCP tools (what an agent at
// this seat would see, end to end).

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/discover"
	"xuansi/internal/server"
	"xuansi/internal/testutil"
)

// TestBoardsAliasToolCrossing: the tool carries {board, alias} to the
// daemon as a holder-less boards_alias and answers the reply's
// {aliased, alias} verbatim.
func TestBoardsAliasToolCrossing(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"aliased": "d75884",
		"alias":   "bench",
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_boards_alias", map[string]any{"board": "d75884", "alias": "bench"})
	if res.IsError {
		t.Fatalf("boards_alias: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "boards_alias" || req.Board != "d75884" || req.Alias != "bench" {
		t.Errorf("request = %+v, want boards_alias/board d75884/alias bench", req)
	}
	if req.Holder != "" {
		t.Errorf("holder = %q on a holder-less daemon-config call, want empty", req.Holder)
	}
	m := structured(t, res)
	if m["aliased"] != "d75884" || m["alias"] != "bench" {
		t.Errorf("result = %#v, want the daemon's {aliased, alias} verbatim", m)
	}
}

// TestBoardsUnaliasToolCrossing: {board} crosses, {unaliased} answers.
func TestBoardsUnaliasToolCrossing(t *testing.T) {
	e, reqs := newEnv(t, []step{{result: map[string]any{
		"unaliased": "d75884",
	}}}, Options{Holder: "mcp-fixed01"})

	res := e.call("xuansi_boards_unalias", map[string]any{"board": "bench"})
	if res.IsError {
		t.Fatalf("boards_unalias: %s", resultText(t, res))
	}
	req := wantReq(t, reqs)
	if req.Type != "boards_unalias" || req.Board != "bench" || req.Holder != "" {
		t.Errorf("request = %+v, want a holder-less boards_unalias/board bench", req)
	}
	if m := structured(t, res); m["unaliased"] != "d75884" {
		t.Errorf("result = %#v, want unaliased d75884", m)
	}
}

// TestBoardsAliasUnaliasSchema: board+alias are required on alias
// (board alone on unalias); the descs state the alias contract —
// where the alias lives, what the daemon may touch, what it refuses,
// and the honest limits (uniqueness is a now-check; a hand edit
// applies at the next daemon start and races a write
// last-writer-wins). The payload refusal names the SAFE edit, and
// neither desc names the destructive one.
func TestBoardsAliasUnaliasSchema(t *testing.T) {
	e, _ := newEnv(t, nil, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]mcpsdk.Tool{}
	for _, tool := range lr.Tools {
		byName[tool.Name] = *tool
	}
	required := func(name string) map[string]bool {
		out := map[string]bool{}
		schema, ok := byName[name].InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("tool %s input schema = %#v, want a JSON object", name, byName[name].InputSchema)
		}
		if r, ok := schema["required"].([]any); ok {
			for _, x := range r {
				if s, ok := x.(string); ok {
					out[s] = true
				}
			}
		}
		return out
	}
	if r := required("xuansi_boards_alias"); !r["board"] || !r["alias"] || len(r) != 2 {
		t.Errorf("boards_alias required = %v, want exactly board+alias", r)
	}
	if r := required("xuansi_boards_unalias"); !r["board"] || len(r) != 1 {
		t.Errorf("boards_unalias required = %v, want exactly board", r)
	}
	for name, wants := range map[string][]string{
		"xuansi_boards_alias": {
			"boards.conf",                // where the alias lives
			"restart",                    // …and that it survives one
			"^[a-z0-9][a-z0-9_-]{0,31}$", // the charset, verbatim
			"name field",                 // what the daemon touches on an existing line
			"NOW",                        // uniqueness is a now-check
			"last writer",                // the hand-edit race, told straight
			"next daemon start",          // …and when a hand edit applies
			"No lease",                   // holder-less, no board contact
		},
		"xuansi_boards_unalias": {
			"boards.conf",
			"restart",
			"pure", // only a pure alias line is deleted
			// the payload refusal coaches the SAFE edit — rename the
			// name to the board's own mac6 — and says how, from here
			"parses to NOTHING",
			"rename the name to the board's own mac6",
			"xuansi_boards_alias{board, alias: <its mac6>}",
			"keeps the endpoint and the model",
			"next daemon start",
			"idempotent", // a never-aliased board answers success
			"No lease",
		},
	} {
		d := byName[name].Description
		if d == "" {
			t.Errorf("tool %s has an empty description", name)
		}
		for _, w := range wants {
			if !strings.Contains(d, w) {
				t.Errorf("%s description lacks %q: %q", name, w, d)
			}
		}
	}

	// Neither Desc may instruct the destructive edit: a boards.conf
	// line needs its name, and one without it (or no line at all)
	// parses to nothing — the endpoint and the model= are lost at the
	// next start, with only a warning in the log.
	for _, name := range []string{"xuansi_boards_alias", "xuansi_boards_unalias"} {
		lower := strings.ToLower(byName[name].Description)
		for _, bad := range []string{"delete the name", "delete the line", "delete it", "remove the name", "remove the line", "by deleting", "deleting the name", "deleting the line"} {
			if strings.Contains(lower, bad) {
				t.Errorf("%s description instructs the destructive edit %q: %q", name, bad, byName[name].Description)
			}
		}
	}
}

// TestBoardsAliasDaemonRefusalCrossing: the daemon's refusal crosses
// as the tool's error text unaltered — the validator's words, coach
// and file path included, not a client-side re-wording. (The line is
// a copy of the registry's real refusal format; the format itself is
// pinned beside the code that produces it, in the discover package.)
func TestBoardsAliasDaemonRefusalCrossing(t *testing.T) {
	const daemonLine = `discover: unalias d75884: clearing the alias would take more than the alias with it: /path/to/boards.conf names it as "cam=d75884@10.1.11.204:7777 model=esp32s3-dev" — a boards.conf line needs its name (without it the @endpoint and model= on that line are lost at the next start), so the daemon deletes only a pure alias=mac6 line; to clear this alias, rename the name to the board's mac6 (here "cam" → "d75884", the rest of the line unchanged) — by hand, or by setting the alias to d75884`
	e, _ := newEnv(t, []step{{rerr: &server.RPCError{Class: "server", Msg: daemonLine}}}, Options{})
	res := e.call("xuansi_boards_unalias", map[string]any{"board": "d75884"})
	if !res.IsError {
		t.Fatalf("unalias of a payload line succeeded: %#v", res)
	}
	if got, want := resultText(t, res), "server: "+daemonLine; got != want {
		t.Errorf("refusal text = %q, want the daemon's line verbatim %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// the restart proof: real daemons, a real boards file, only MCP tools
// ---------------------------------------------------------------------------

// startAliasDaemon serves a REAL daemon over a REAL boards file: the
// registry seeded from the file exactly as `serve` seeds it and told
// where the file lives; the boards sighted over USB only, so the
// startup audit never dials anything (no controller, no board
// contact — this face never needs one). The caller closes it: a
// restart is one more call with the same file.
func startAliasDaemon(t *testing.T, sock, conf string, boards ...discover.Board) *server.Server {
	t.Helper()
	aliases, pins, models, err := discover.LoadBoardsFile(conf)
	if err != nil {
		t.Fatalf("LoadBoardsFile: %v", err)
	}
	reg := discover.NewRegistry(aliases, pins,
		discover.WithModels(models),
		discover.WithBoardsFile(conf),
		discover.WithUSB(func(context.Context) ([]discover.Board, error) { return boards, nil }),
		discover.WithMDNS(func(context.Context) ([]discover.Board, error) { return nil, nil }),
	)
	dir := t.TempDir()
	srv, err := server.New(server.Opts{
		SockPath:         sock,
		Registry:         reg,
		GraceTTL:         10 * time.Second,
		RefreshInterval:  -1,
		BootRecordsDir:   filepath.Join(dir, "bootrec"),
		VaultDir:         filepath.Join(dir, "vault"),
		CaptureRoot:      filepath.Join(dir, "captures"),
		DisableProvision: true,
		Log:              func(string, ...any) {},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(srv.Close)
	// the initial sweep lands the boards (it runs async inside Start)
	deadline := time.Now().Add(5 * time.Second)
	for len(reg.Snapshot()) < len(boards) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(reg.Snapshot()); got != len(boards) {
		t.Fatalf("the initial sweep landed %d of %d board(s)", got, len(boards))
	}
	return srv
}

// listedBoard answers one board's list_boards row as the seat serves
// it (absent keys stay absent — that is the point of omitempty).
func listedBoard(t *testing.T, e *env, mac6 string) map[string]any {
	t.Helper()
	res := e.call("xuansi_list_boards", nil)
	if res.IsError {
		t.Fatalf("list_boards: %s", resultText(t, res))
	}
	raw, _ := structured(t, res)["boards"].([]any)
	for _, b := range raw {
		if m, ok := b.(map[string]any); ok && m["mac6"] == mac6 {
			return m
		}
	}
	t.Fatalf("board %s is not listed: %#v", mac6, raw)
	return nil
}

// TestBoardsAliasRestartPersistence: the single-source claim, seen
// from the seat. Generation 1 sets an alias (list_boards shows it at
// once, board_status resolves by it) and unaliases it again;
// generation 2 — a restarted daemon over the same file — reads the
// removal, still refuses the payload line — with the safe recipe,
// which is then followed from the seat: the alias set to the board's
// own mac6 renames the name and keeps endpoint and model — where the
// recipe ENDS: unalias is then the idempotent success (the loop of the
// fix wave, closed) — and sets another alias; generation 3 reads both.
// The file's payload line differs from start to finish in its name
// alone.
func TestBoardsAliasRestartPersistence(t *testing.T) {
	const header = "# bench boards\ncam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\n"
	conf := filepath.Join(t.TempDir(), "boards.conf")
	if err := os.WriteFile(conf, []byte(header), 0o644); err != nil {
		t.Fatal(err)
	}
	boards := []discover.Board{
		{MAC6: "d75884", Sources: []string{discover.SourceUSB}, USBPath: "/dev/serial/by-id/fake-d75884"},
		{MAC6: "1a2b3c", Sources: []string{discover.SourceUSB}, USBPath: "/dev/serial/by-id/fake-1a2b3c"},
	}
	sock := filepath.Join(testutil.ShortSockDir(t), "d.sock")

	// generation 1: set → seen at once → unalias
	srv := startAliasDaemon(t, sock, conf, boards...)
	if !t.Run("generation 1", func(t *testing.T) {
		e := wireMCPSock(t, sock, Options{}, nil)

		res := e.call("xuansi_boards_alias", map[string]any{"board": "d75884", "alias": "bench"})
		if res.IsError {
			t.Fatalf("boards_alias: %s", resultText(t, res))
		}
		if m := structured(t, res); m["aliased"] != "d75884" || m["alias"] != "bench" {
			t.Fatalf("boards_alias reply = %#v", m)
		}
		// live immediately — the very next list_boards carries it
		if row := listedBoard(t, e, "d75884"); row["alias"] != "bench" {
			t.Fatalf("list_boards right after the alias = %#v, want alias bench", row)
		}
		// …and it is a selector already
		res = e.call("xuansi_board_status", map[string]any{"board": "bench"})
		if res.IsError {
			t.Fatalf("board_status(bench): %s", resultText(t, res))
		}
		if b, _ := structured(t, res)["board"].(map[string]any); b["mac6"] != "d75884" {
			t.Fatalf("board_status(bench) = %#v", b)
		}

		// unalias, addressed by the alias itself
		res = e.call("xuansi_boards_unalias", map[string]any{"board": "bench"})
		if res.IsError {
			t.Fatalf("boards_unalias: %s", resultText(t, res))
		}
		if m := structured(t, res); m["unaliased"] != "d75884" {
			t.Fatalf("boards_unalias reply = %#v", m)
		}
		if row := listedBoard(t, e, "d75884"); row["alias"] != nil {
			t.Fatalf("list_boards right after the unalias = %#v, want no alias key", row)
		}
	}) {
		return
	}
	srv.Close()

	// generation 2: the restart reads the removal; a set lands again
	srv = startAliasDaemon(t, sock, conf, boards...)
	if !t.Run("generation 2 (restarted)", func(t *testing.T) {
		e := wireMCPSock(t, sock, Options{}, nil)

		if row := listedBoard(t, e, "d75884"); row["alias"] != nil {
			t.Fatalf("the restart resurrected the cleared alias: %#v", row)
		}
		// the payload line rode through untouched: alias, pin and model as written
		if row := listedBoard(t, e, "1a2b3c"); row["alias"] != "cam" || row["model"] != "esp32s3-dev" || row["model_source"] != "conf" {
			t.Fatalf("the payload line's board = %#v", row)
		}
		// …and it is the line unalias refuses, coach and path included
		res := e.call("xuansi_boards_unalias", map[string]any{"board": "cam"})
		if !res.IsError {
			t.Fatalf("unalias of a payload line succeeded: %#v", res)
		}
		// …with the SAFE recipe (rename the name to the board's own
		// mac6; the line keeps its payload) and the path — never an
		// instruction that loses the payload
		got := resultText(t, res)
		for _, safe := range []string{`rename the name to the board's mac6`, `"cam" → "1a2b3c"`, "or by setting the alias to 1a2b3c", conf} {
			if !strings.Contains(got, safe) {
				t.Errorf("refusal lacks the safe recipe %q: %q", safe, got)
			}
		}
		for _, bad := range []string{"delete the name", "delete the line", "remove the name", "by deleting"} {
			if strings.Contains(strings.ToLower(got), bad) {
				t.Errorf("refusal instructs the destructive edit %q: %q", bad, got)
			}
		}
		if b, _ := os.ReadFile(conf); !bytes.Equal(b, []byte(header)) {
			t.Fatalf("the refusal wrote: %q", b)
		}

		// the recipe, followed from the seat: one call renames the
		// name; endpoint and model stay, live and after the restart
		res = e.call("xuansi_boards_alias", map[string]any{"board": "cam", "alias": "1a2b3c"})
		if res.IsError {
			t.Fatalf("boards_alias{board: cam, alias: 1a2b3c}: %s", resultText(t, res))
		}
		if m := structured(t, res); m["aliased"] != "1a2b3c" || m["alias"] != "1a2b3c" {
			t.Fatalf("boards_alias reply = %#v", m)
		}
		if row := listedBoard(t, e, "1a2b3c"); row["alias"] != "1a2b3c" || row["model"] != "esp32s3-dev" || row["model_source"] != "conf" {
			t.Fatalf("after the recipe the board = %#v, want alias = its mac6 with the conf model intact", row)
		}

		// …and the recipe is the END of it: unalias is then the
		// idempotent success the Desc promises — the terminal loop of
		// the fix wave ("the same refusal again, with `"1a2b3c" →
		// "1a2b3c"`") is closed. Nothing is written, and the board goes
		// on answering to its mac6 as it did: the line keeps the mac6
		// as its name, so list_boards keeps showing it in the alias
		// column — live here, and after the restart below.
		res = e.call("xuansi_boards_unalias", map[string]any{"board": "1a2b3c"})
		if res.IsError {
			t.Fatalf("boards_unalias after the recipe (the loop's end): %s", resultText(t, res))
		}
		if m := structured(t, res); m["unaliased"] != "1a2b3c" {
			t.Fatalf("boards_unalias reply = %#v", m)
		}
		if row := listedBoard(t, e, "1a2b3c"); row["alias"] != "1a2b3c" || row["model"] != "esp32s3-dev" || row["model_source"] != "conf" {
			t.Fatalf("after the idempotent unalias the board = %#v, want the mac6 as its name, pin and model intact", row)
		}
		if b, _ := os.ReadFile(conf); !bytes.Equal(b, []byte(strings.Replace(header, "cam=1a2b3c@", "1a2b3c=1a2b3c@", 1))) {
			t.Fatalf("the idempotent unalias wrote: %q", b)
		}

		res = e.call("xuansi_boards_alias", map[string]any{"board": "d75884", "alias": "spare"})
		if res.IsError {
			t.Fatalf("boards_alias after the restart: %s", resultText(t, res))
		}
	}) {
		return
	}
	srv.Close()

	// generation 3: the alias set in generation 2 is still there, and
	// so is the renamed payload line — endpoint and model included
	srv = startAliasDaemon(t, sock, conf, boards...)
	t.Run("generation 3 (restarted again)", func(t *testing.T) {
		e := wireMCPSock(t, sock, Options{}, nil)
		if row := listedBoard(t, e, "d75884"); row["alias"] != "spare" {
			t.Fatalf("the restart lost the alias: %#v", row)
		}
		if row := listedBoard(t, e, "1a2b3c"); row["alias"] != "1a2b3c" || row["model"] != "esp32s3-dev" || row["model_source"] != "conf" {
			t.Fatalf("the renamed payload line after the restart: %#v", row)
		}
	})
	srv.Close()

	// the file, byte for byte: the payload line with ONLY its name
	// changed (the safe recipe), one line appended
	renamed := strings.Replace(header, "cam=1a2b3c@", "1a2b3c=1a2b3c@", 1)
	if got, err := os.ReadFile(conf); err != nil || string(got) != renamed+"spare=d75884\n" {
		t.Fatalf("boards.conf = %q (%v)\n            want %q", got, err, renamed+"spare=d75884\n")
	}
}

// TestInlineCommentAliasAtTheSeat: the ruled inline-comment grammar
// (owner ruling 2026-11-24) as an agent at this seat meets it — a real
// daemon over a real file, only MCP tools. A hand-written
// `bench1=d75884 # inline note` names the board (before the ruling
// that line was malformed: skipped with a warning, its alias dropped
// at every start); a rename through xuansi_boards_alias moves the name
// and leaves the comment byte for byte; the line is still not
// deletable — unalias is refused with the safe recipe and writes
// nothing — and once the recipe names it by the mac6, unalias is the
// idempotent success, the comment still in place.
func TestInlineCommentAliasAtTheSeat(t *testing.T) {
	conf := filepath.Join(t.TempDir(), "boards.conf")
	const header = "# bench boards\nbench1=d75884 # inline note\n"
	if err := os.WriteFile(conf, []byte(header), 0o644); err != nil {
		t.Fatal(err)
	}
	boards := []discover.Board{
		{MAC6: "d75884", Sources: []string{discover.SourceUSB}, USBPath: "/dev/serial/by-id/fake-d75884"},
	}
	sock := filepath.Join(testutil.ShortSockDir(t), "d.sock")
	srv := startAliasDaemon(t, sock, conf, boards...)
	e := wireMCPSock(t, sock, Options{}, nil)

	// the alias is registered: listed, and a selector already
	if row := listedBoard(t, e, "d75884"); row["alias"] != "bench1" {
		t.Fatalf("an alias-only line with an inline note = %#v, want alias bench1 (the comment is not part of the line's meaning)", row)
	}
	res := e.call("xuansi_board_status", map[string]any{"board": "bench1"})
	if res.IsError {
		t.Fatalf("board_status(bench1): %s", resultText(t, res))
	}
	if b, _ := structured(t, res)["board"].(map[string]any); b["mac6"] != "d75884" {
		t.Fatalf("board_status(bench1) = %#v", b)
	}

	// the rename: the name moves, the comment stays byte for byte
	res = e.call("xuansi_boards_alias", map[string]any{"board": "bench1", "alias": "bench1x"})
	if res.IsError {
		t.Fatalf("boards_alias{board: bench1, alias: bench1x}: %s", resultText(t, res))
	}
	const renamed = "# bench boards\nbench1x=d75884 # inline note\n"
	if b, _ := os.ReadFile(conf); string(b) != renamed {
		t.Fatalf("after the rename boards.conf = %q\n             want %q (the comment as written)", b, renamed)
	}
	if row := listedBoard(t, e, "d75884"); row["alias"] != "bench1x" {
		t.Fatalf("after the rename the board = %#v, want alias bench1x", row)
	}

	// unalias: the line carries a comment, so it is not deleted
	res = e.call("xuansi_boards_unalias", map[string]any{"board": "bench1x"})
	if !res.IsError {
		t.Fatalf("unalias of a comment-bearing line succeeded: %#v", res)
	}
	for _, want := range []string{`"bench1x" → "d75884"`, "rename the name to the board's mac6", conf} {
		if got := resultText(t, res); !strings.Contains(got, want) {
			t.Errorf("refusal lacks the safe recipe %q: %q", want, got)
		}
	}
	if b, _ := os.ReadFile(conf); string(b) != renamed {
		t.Fatalf("the refusal wrote: %q", b)
	}

	// the recipe on that line, then the clear: idempotent success, the
	// comment still in place
	res = e.call("xuansi_boards_alias", map[string]any{"board": "bench1x", "alias": "d75884"})
	if res.IsError {
		t.Fatalf("boards_alias{board: bench1x, alias: d75884}: %s", resultText(t, res))
	}
	const recipe = "# bench boards\nd75884=d75884 # inline note\n"
	if b, _ := os.ReadFile(conf); string(b) != recipe {
		t.Fatalf("after the recipe boards.conf = %q\n             want %q", b, recipe)
	}
	res = e.call("xuansi_boards_unalias", map[string]any{"board": "d75884"})
	if res.IsError {
		t.Fatalf("boards_unalias after the recipe: %s", resultText(t, res))
	}
	if m := structured(t, res); m["unaliased"] != "d75884" {
		t.Fatalf("boards_unalias reply = %#v", m)
	}
	if b, _ := os.ReadFile(conf); string(b) != recipe {
		t.Fatalf("the idempotent unalias wrote: %q", b)
	}
	if row := listedBoard(t, e, "d75884"); row["alias"] != "d75884" {
		t.Fatalf("after the idempotent unalias the board = %#v, want the mac6 as its name", row)
	}
	srv.Close()
}
