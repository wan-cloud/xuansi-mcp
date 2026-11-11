package mcp

// lua_lint_module_doc_test.go — the two W2.T4 tool faces' seat tests:
// the lint tool over the REAL embedded bench index (findings cross
// with positions; the tokenize refusal points at lua.check), and the
// doc tool's workspace-first / GitHub-fallback resolution with the
// release-class kind ladder.

import (
	"strings"
	"testing"

	"xuansi/internal/mcp/lualint"
)

func TestLuaLintToolFindings(t *testing.T) {
	e, reqs := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_lua_lint", map[string]any{
		"source": "local m = require('bench.drv.mod')\n" +
			"bench.events.on('t', function() bench.i2c.scan(0) end)\n" +
			"return m.total / m.n\n" +
			"coroutine.yield()\n" +
			"x = bench.wasm.hotload\n",
	})
	if res.IsError {
		t.Fatalf("lua_lint failed: %s", resultText(t, res))
	}
	noContact(t, reqs) // the lint is purely local
	m := structured(t, res)
	fs, _ := m["findings"].([]any)
	var rules []string
	for _, f := range fs {
		fm, _ := f.(map[string]any)
		rules = append(rules, fm["rule"].(string))
	}
	want := []string{"R1", "R2", "R3", "R4"}
	// order is source order: R3 (line 2) fires before R1 (line 3)
	wantOrder := []string{"R3", "R1", "R2", "R4"}
	if strings.Join(rules, ",") != strings.Join(wantOrder, ",") {
		t.Errorf("findings = %v, want %v (source order)", rules, wantOrder)
	}
	for _, r := range want {
		found := false
		for _, x := range rules {
			if x == r {
				found = true
			}
		}
		if !found {
			t.Errorf("rule %s missing from %v", r, rules)
		}
	}
	// each finding carries the full shape
	f0, _ := fs[0].(map[string]any)
	if f0["msg"] == nil || f0["hint"] == nil || f0["line"] == nil || f0["col"] == nil {
		t.Errorf("finding = %v, want {rule,line,col,msg,hint} complete", f0)
	}
}

func TestLuaLintToolCleanAndRefusal(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	// clean: empty findings array, never null
	res := e.call("xuansi_lua_lint", map[string]any{"source": "local x = 1\nreturn x\n"})
	if res.IsError {
		t.Fatalf("clean lint failed: %s", resultText(t, res))
	}
	fs, _ := structured(t, res)["findings"].([]any)
	if len(fs) != 0 {
		t.Errorf("clean findings = %v, want empty", fs)
	}
	// the R4 table is the REAL embedded index: a real member is clean
	res = e.call("xuansi_lua_lint", map[string]any{"source": "bench.wasm.plan({})\nbench.millis()\n"})
	if res.IsError {
		t.Fatalf("real-member lint failed: %s", resultText(t, res))
	}
	fs, _ = structured(t, res)["findings"].([]any)
	if len(fs) != 0 {
		t.Errorf("real-member findings = %v, want empty (the embedded index knows the full surface)", fs)
	}
	// a tokenize failure refuses with the lua.check pointer
	res = e.call("xuansi_lua_lint", map[string]any{"source": "x = \"open"})
	if !res.IsError {
		t.Fatal("unterminated string must refuse")
	}
	if txt := resultText(t, res); !strings.Contains(txt, "lua.check") {
		t.Errorf("refusal = %q, want the lua.check ground-truth pointer", txt)
	}
}

// moduleDocKindLadder pins the seat's kind vocabulary: the index-level
// kind (release class) wins; the version's own meta release is the
// fallback when the index carried none.
func TestModuleDocKindLadder(t *testing.T) {
	wbody := fetchWasmBody(48)

	// leg 1: workspace-first with an index kind — the INDEX vocabulary
	// answers (test-canary)
	e, _, _ := newWsEnv(t, nil)
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "metamod", kind: "test-canary", version: "1.0.0",
		files:    []fetchFileSpec{fetchSpec(t, "modules/metamod/1.0.0/metamod.wasm", wbody, "")},
		metaBody: metaFixtureJSON(t, "production"),
	}})
	if res := e.call("xuansi_driver_fetch", map[string]any{"name": "metamod", "version": "1.0.0", "index_url": url}); res.IsError {
		t.Fatalf("fetch: %s", resultText(t, res))
	}
	res := e.call("xuansi_module_doc", map[string]any{"name": "metamod"})
	if res.IsError {
		t.Fatalf("module_doc: %s", resultText(t, res))
	}
	m := structured(t, res)
	if m["source"] != "workspace" || m["version"] != "1.0.0" || m["kind"] != "test-canary" {
		t.Errorf("workspace doc = %v, want source=workspace/1.0.0/kind=test-canary (index vocabulary wins)", m)
	}
	ops, _ := m["ops"].([]any)
	if len(ops) != 2 {
		t.Fatalf("ops = %v, want the meta's two rows", ops)
	}
	op0, _ := ops[0].(map[string]any)
	if op0["name"] != "echo" || op0["semantics"] != "round-trip" {
		t.Errorf("ops[0] = %v, want the echo row", op0)
	}
	meas, _ := m["measured"].(map[string]any)
	if meas["initial_memory_bytes"] != float64(262144) {
		t.Errorf("measured = %v, want the meta's map", meas)
	}

	// leg 2: a KINDLESS index whose meta carries release — the META
	// release answers (the fallback half of the ladder)
	e2, _, _ := newWsEnv(t, nil)
	url2, _ := newFetchIndex(t, []fetchModSpec{{
		name: "metamod", kind: "", version: "1.0.0",
		files:    []fetchFileSpec{fetchSpec(t, "modules/metamod/1.0.0/metamod.wasm", wbody, "")},
		metaBody: metaFixtureJSON(t, "production"),
	}})
	if res := e2.call("xuansi_driver_fetch", map[string]any{"name": "metamod", "version": "1.0.0", "index_url": url2}); res.IsError {
		t.Fatalf("fetch (kindless): %s", resultText(t, res))
	}
	res = e2.call("xuansi_module_doc", map[string]any{"name": "metamod", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("module_doc (kindless): %s", resultText(t, res))
	}
	if k := structured(t, res)["kind"]; k != "production" {
		t.Errorf("kind = %v, want production (the meta release fallback)", k)
	}
}

// TestModuleDocGitHubFallback: a version the workspace does not hold
// answers from the index + raw meta with source=github; a name known
// NOWHERE refuses naming both legs.
func TestModuleDocGitHubFallback(t *testing.T) {
	e, _, _ := newWsEnv(t, nil)
	wbody := fetchWasmBody(40)
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "ghostmod", kind: "production", version: "2.0.0",
		files:    []fetchFileSpec{fetchSpec(t, "modules/ghostmod/2.0.0/ghostmod.wasm", wbody, "")},
		metaBody: metaFixtureJSON(t, "production"),
	}})
	// ghostmod's meta fixture writes name "metamod" — the doc face
	// serves by REQUEST name regardless (the body is a fixture).

	res := e.call("xuansi_module_doc", map[string]any{"name": "ghostmod", "version": "2.0.0", "index_url": url})
	if res.IsError {
		t.Fatalf("github fallback failed: %s", resultText(t, res))
	}
	m := structured(t, res)
	if m["source"] != "github" || m["version"] != "2.0.0" {
		t.Errorf("github doc = %v, want source=github/2.0.0", m)
	}
	// version omitted resolves the index's highest
	res = e.call("xuansi_module_doc", map[string]any{"name": "ghostmod", "index_url": url})
	if res.IsError {
		t.Fatalf("github highest failed: %s", resultText(t, res))
	}
	if v := structured(t, res)["version"]; v != "2.0.0" {
		t.Errorf("resolved version = %v, want the index's highest 2.0.0", v)
	}
	// known nowhere: the refusal names both legs
	res = e.call("xuansi_module_doc", map[string]any{"name": "nowhere", "index_url": url})
	if !res.IsError {
		t.Fatal("unknown module must refuse")
	}
	if txt := resultText(t, res); !strings.Contains(txt, "workspace") {
		t.Errorf("refusal = %q, want both legs named", txt)
	}
}

// TestModuleDocWorkspaceVersionResolution: the omitted version
// resolves the workspace ACTIVE first, else the highest stored — the
// driver_fetch rule, workspace flavor.
func TestModuleDocWorkspaceVersionResolution(t *testing.T) {
	e, _, _ := newWsEnv(t, nil)
	wbody := fetchWasmBody(40)
	url, _ := newFetchIndex(t, []fetchModSpec{{ // the workspace leg serves these
		name: "metamod", kind: "production", version: "1.0.0",
		files:    []fetchFileSpec{fetchSpec(t, "modules/metamod/1.0.0/metamod.wasm", wbody, "")},
		metaBody: metaFixtureJSON(t, "production"),
	}})
	for _, v := range []string{"1.0.0", "1.1.0"} {
		if res := e.call("xuansi_driver_fetch", map[string]any{"name": "metamod", "version": v, "index_url": url}); res.IsError {
			// 1.1.0 is not in the fixture index — put it by hand with a
			// hand-planted manifest doc entry instead
			if v == "1.1.0" {
				wsMu.Lock()
				m, err := loadManifest(e.ts.ws.manifestPath())
				if err != nil {
					t.Fatal(err)
				}
				ent := m.entry("metamod")
				ent.Files["1.1.0"] = FileMeta{Doc: &DocMeta{Ops: []DocOp{{Name: "newop"}}}}
				m.Drivers["metamod"] = ent
				if err := writeManifestAtomic(e.ts.ws.manifestPath(), m); err != nil {
					t.Fatal(err)
				}
				wsMu.Unlock()
				continue
			}
			t.Fatalf("fetch %s: %s", v, resultText(t, res))
		}
	}
	// ACTIVE is empty → highest stored (1.1.0)
	res := e.call("xuansi_module_doc", map[string]any{"name": "metamod", "index_url": url})
	if res.IsError {
		t.Fatalf("module_doc: %s", resultText(t, res))
	}
	if v := structured(t, res)["version"]; v != "1.1.0" {
		t.Errorf("resolved = %v, want the highest stored 1.1.0", v)
	}
}

// compile-time: the tool output must stay the lualint finding shape.
var _ []lualint.Finding
