package mcp

// admission_test.go — driver_push's wasm memory preflight (B15 §4.12,
// W2-T4): the UNION arithmetic (an already-loaded module counts once
// — planning a loaded name projects zero growth), the fits/over
// verdict, the degraded leg (eval refused / board unreachable), the
// NEVER-a-gate contract (an over verdict still lands the push), and
// the honest absence for Lua pushes.

import (
	"fmt"
	"strings"
	"testing"

	"xuansi/internal/server"
)

// footprintMetaJSON builds one meta.json body whose wasm footprint
// declares the given initial.
func footprintMetaJSON(initial int64) []byte {
	return []byte(fmt.Sprintf(`{
	  "name": "metamod", "kind": "driver", "release": "production", "version": "1.0.0", "abi": 1,
	  "doc": {"ops": [{"name": "echo", "semantics": "round-trip"}], "measured": {"initial_memory_bytes": %d}},
	  "footprint": {"wasm": {"initial_bytes": %d, "max_bytes": 1048576}}
	}`, initial, initial))
}

// planStep builds the admission eval's scripted reply: the plan table
// (loaded/budget) and the loaded names list.
func planStep(loaded, budget int64, names ...string) step {
	ns := make([]any, len(names))
	for i, n := range names {
		ns[i] = n
	}
	return step{result: map[string]any{
		"out": "",
		"value": map[string]any{
			"plan": map[string]any{
				"loaded_bytes": float64(loaded), "total_bytes": float64(loaded),
				"budget_bytes": float64(budget), "fits": true,
			},
			"names": ns,
		},
	}}
}

// TestAdmissionFitsUnionArithmetic: the projection uses the plan
// UNION — a fresh candidate ADDS its footprint, a name already in the
// loaded list adds NOTHING (it is already inside loaded_bytes), and
// the numbers cross verbatim.
func TestAdmissionFitsUnionArithmetic(t *testing.T) {
	wbody := fetchWasmBody(48)

	// leg 1: fresh candidate — loaded 262144 + candidate 262144 fits
	e, reqs, _ := newWsEnv(t, []step{
		planStep(262144, 2097152, "othermod"),
		{result: map[string]any{"ok": true}},
	})
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "metamod", kind: "production", version: "1.0.0",
		files:    []fetchFileSpec{fetchSpec(t, "modules/metamod/1.0.0/metamod.wasm", wbody, "")},
		metaBody: footprintMetaJSON(262144),
	}})
	if res := e.call("xuansi_driver_fetch", map[string]any{"name": "metamod", "version": "1.0.0", "index_url": url}); res.IsError {
		t.Fatalf("fetch: %s", resultText(t, res))
	}
	res := e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "metamod", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("push: %s", resultText(t, res))
	}
	m := structured(t, res)
	adm, _ := m["admission"].(map[string]any)
	if adm == nil {
		t.Fatalf("no admission object: %v", m)
	}
	if adm["projected"] != true || adm["verdict"] != "fits" {
		t.Errorf("admission = %v, want projected/fits", adm)
	}
	if adm["loaded_bytes"] != float64(262144) || adm["candidate_bytes"] != float64(262144) || adm["budget_bytes"] != float64(2097152) {
		t.Errorf("admission numbers = %v, want 262144/262144/2097152", adm)
	}
	// the eval rode HELD (the push's own holder) and preceded the write
	req := wantReq(t, reqs)
	if req.Type != "eval" || req.Holder != "mcp-fixed01" || !strings.Contains(req.Code, "bench.wasm.plan") {
		t.Errorf("preflight eval = %+v, want the held plan+list eval", req)
	}
	if req = wantReq(t, reqs); req.Type != "fs_write" || req.Sha256 == "" {
		t.Errorf("fs_write = %+v, want the verified write after the preflight", req)
	}

	// leg 2: the union — the module's own name already loaded: the
	// candidate adds NOTHING (total stays loaded; the board's own plan
	// semantics, not a sum)
	e2, reqs2, _ := newWsEnv(t, []step{
		planStep(262144, 2097152, "metamod"),
		{result: map[string]any{"ok": true}},
	})
	url2, _ := newFetchIndex(t, []fetchModSpec{{
		name: "metamod", kind: "production", version: "1.0.0",
		files:    []fetchFileSpec{fetchSpec(t, "modules/metamod/1.0.0/metamod.wasm", wbody, "")},
		metaBody: footprintMetaJSON(262144),
	}})
	if res := e2.call("xuansi_driver_fetch", map[string]any{"name": "metamod", "version": "1.0.0", "index_url": url2}); res.IsError {
		t.Fatalf("fetch (union): %s", resultText(t, res))
	}
	res = e2.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "metamod", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("push (union): %s", resultText(t, res))
	}
	adm, _ = structured(t, res)["admission"].(map[string]any)
	if adm == nil || adm["projected"] != true || adm["verdict"] != "fits" {
		t.Fatalf("union admission = %v, want projected/fits", adm)
	}
	// the loaded account ALONE stands: 262144 (already inside it), the
	// candidate counted once — the seat surfaces the candidate number
	// AND the loaded number; the verdict is the union, not the sum
	if adm["loaded_bytes"] != float64(262144) || adm["candidate_bytes"] != float64(262144) {
		t.Errorf("union numbers = %v, want loaded 262144 with the candidate counted once", adm)
	}
	if req := wantReq(t, reqs2); req.Type != "eval" {
		t.Errorf("union preflight = %+v, want the eval", req)
	}
	if req := wantReq(t, reqs2); req.Type != "fs_write" {
		t.Errorf("union write = %+v, want the verified write", req)
	}
	noContact(t, reqs2)
}

// TestAdmissionOverStillPushes: the over verdict NEVER blocks — the
// write lands, the manifest records it, and the reply reports the
// projection honestly (the board's load gate is the authority).
func TestAdmissionOverStillPushes(t *testing.T) {
	wbody := fetchWasmBody(48)
	e, reqs, root := newWsEnv(t, []step{
		planStep(1900000, 2097152, "fatmod"), // 1900000 + 262144 > budget
		{result: map[string]any{"ok": true}},
	})
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "metamod", kind: "production", version: "1.0.0",
		files:    []fetchFileSpec{fetchSpec(t, "modules/metamod/1.0.0/metamod.wasm", wbody, "")},
		metaBody: footprintMetaJSON(262144),
	}})
	if res := e.call("xuansi_driver_fetch", map[string]any{"name": "metamod", "version": "1.0.0", "index_url": url}); res.IsError {
		t.Fatalf("fetch: %s", resultText(t, res))
	}
	res := e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "metamod", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("an over projection must NOT block the push: %s", resultText(t, res))
	}
	adm, _ := structured(t, res)["admission"].(map[string]any)
	if adm == nil || adm["verdict"] != "over" || adm["projected"] != true {
		t.Fatalf("admission = %v, want projected/over", adm)
	}
	if req := wantReq(t, reqs); req.Type != "eval" {
		t.Fatalf("first crossing = %+v, want the preflight eval", req)
	}
	if req := wantReq(t, reqs); req.Type != "fs_write" {
		t.Fatalf("second crossing = %+v, want the write (over never blocks)", req)
	}
	// the manifest recorded the push (the preflight is not a gate)
	if m := wsManifest(t, root); m.Drivers["metamod"].Active != "1.0.0" {
		t.Errorf("Active = %q, want 1.0.0 — an over projection must not block the manifest either", m.Drivers["metamod"].Active)
	}
}

// TestAdmissionDegradedLeg: a board leg that cannot answer (a
// pre-wave-2 board without plan, an eval refusal, a dead link)
// degrades honestly — projected:false, loaded -1, the reference
// budget, and the candidate-vs-budget-alone verdict.
func TestAdmissionDegradedLeg(t *testing.T) {
	wbody := fetchWasmBody(48)
	e, reqs, _ := newWsEnv(t, []step{
		{rerr: &server.RPCError{Class: "rt", Msg: "attempt to call a nil value (field 'plan')"}}, // pre-wave-2 board
		{result: map[string]any{"ok": true}},
	})
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "metamod", kind: "production", version: "1.0.0",
		files:    []fetchFileSpec{fetchSpec(t, "modules/metamod/1.0.0/metamod.wasm", wbody, "")},
		metaBody: footprintMetaJSON(262144),
	}})
	if res := e.call("xuansi_driver_fetch", map[string]any{"name": "metamod", "version": "1.0.0", "index_url": url}); res.IsError {
		t.Fatalf("fetch: %s", resultText(t, res))
	}
	res := e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "metamod", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("degraded preflight must not block the push: %s", resultText(t, res))
	}
	adm, _ := structured(t, res)["admission"].(map[string]any)
	if adm == nil || adm["projected"] != false {
		t.Fatalf("admission = %v, want projected:false", adm)
	}
	if adm["loaded_bytes"] != float64(-1) || adm["budget_bytes"] != float64(2097152) {
		t.Errorf("degraded numbers = %v, want loaded -1 / the reference budget 2097152", adm)
	}
	if adm["verdict"] != "fits" || adm["candidate_bytes"] != float64(262144) {
		t.Errorf("degraded verdict = %v, want fits on 262144 vs the reference budget alone", adm)
	}
	if req := wantReq(t, reqs); req.Type != "eval" {
		t.Errorf("first crossing = %+v, want the (refused) preflight eval", req)
	}
	if req := wantReq(t, reqs); req.Type != "fs_write" {
		t.Errorf("second crossing = %+v, want the write (degraded never blocks)", req)
	}
	noContact(t, reqs)
}

// TestAdmissionLuaPushCarriesNone: a Lua driver push carries no
// admission (nothing to admit) and no preflight eval crosses.
func TestAdmissionLuaPushCarriesNone(t *testing.T) {
	e, reqs, _ := newWsPushEnv(t, 1, false)
	luaSrc := drvBody("--@api 1", 32)
	mustPut(t, e, "plainlua", "1.0.0", luaSrc)
	res := e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "plainlua", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("lua push: %s", resultText(t, res))
	}
	if _, has := structured(t, res)["admission"]; has {
		t.Error("a Lua push must carry no admission object")
	}
	// the push pipeline's own crossings drain; any EVAL among them fails
	for {
		var req server.Request
		select {
		case r := <-reqs:
			req = r
		default:
			return // drained: no eval crossed
		}
		if req.Type == "eval" {
			t.Errorf("a Lua push crossed an eval: %+v", req)
		}
	}
}

// TestAdmissionAbsentWithoutFootprint: a binary push whose version
// has NO recorded footprint meta (a hand-planted file, a
// pre-restructure fetch) carries no admission — absent, never guessed.
func TestAdmissionAbsentWithoutFootprint(t *testing.T) {
	wbody := fetchWasmBody(48)
	e, reqs, _ := newWsEnv(t, []step{
		{result: map[string]any{"ok": true}}, // just the fs_write
	})
	// fetch WITHOUT a meta body → no footprint recorded
	url, _ := newFetchIndex(t, []fetchModSpec{{
		name: "metamod", kind: "production", version: "1.0.0",
		files: []fetchFileSpec{fetchSpec(t, "modules/metamod/1.0.0/metamod.wasm", wbody, "")},
	}})
	if res := e.call("xuansi_driver_fetch", map[string]any{"name": "metamod", "version": "1.0.0", "index_url": url}); res.IsError {
		t.Fatalf("fetch: %s", resultText(t, res))
	}
	res := e.call("xuansi_driver_push", map[string]any{"board": "d75884", "name": "metamod", "version": "1.0.0"})
	if res.IsError {
		t.Fatalf("push: %s", resultText(t, res))
	}
	if _, has := structured(t, res)["admission"]; has {
		t.Error("a push without footprint meta must carry no admission")
	}
	if req := wantReq(t, reqs); req.Type != "fs_write" {
		t.Errorf("first crossing = %+v, want the write (no preflight eval without meta)", req)
	}
}
