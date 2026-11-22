package mcp

// abi_reference_test.go — the embedded reference's locks (B15 §4.14,
// W2-T4): the document parses (every run), the tool serves the
// sections + the section filter, the bench index R4 rides is complete
// against the fw registration table, and — wherever a drivers
// checkout is visible — the checked-in copy byte-compares against the
// upstream generated file (the wasmhdr/drivers-parity precedent:
// XUANSI_DRIVERS_DIR names it, else the sibling ../xuansi-drivers of
// checkout is probed; the CI drivers-parity job sets the
// env so the lock has teeth there too).

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/mcp/lualint"
)

func TestAbiReferenceEmbedsParses(t *testing.T) {
	d, err := parseAbiReference(abiReferenceJSON)
	if err != nil {
		t.Fatalf("embedded abi-reference.json does not parse: %v", err)
	}
	if v, _ := d.ABI["version"].(float64); v != 1 {
		t.Errorf("abi.version = %v, want 1 (the recarved single axis)", d.ABI["version"])
	}
	for _, s := range abiSectionNames {
		raw, ok := d.abiSectionRaw(s)
		if !ok || len(raw) == 0 {
			t.Errorf("section %s missing/empty in the embedded copy", s)
		}
	}
	if d.Provenance["header_sha256"] == nil || d.Provenance["sidecar_sha256"] == nil {
		t.Errorf("provenance lacks the header/sidecar sha pins: %v", d.Provenance)
	}
}

func TestAbiReferenceToolServesSections(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})

	// full: all eight sections ride
	res := e.call("xuansi_abi_reference", nil)
	if res.IsError {
		t.Fatalf("abi_reference failed: %s", resultText(t, res))
	}
	m := structured(t, res)
	secs, _ := m["sections"].([]any)
	if len(secs) != len(abiSectionNames) {
		t.Fatalf("sections = %v, want all eight", secs)
	}
	content, _ := m["content"].(map[string]any)
	for _, s := range abiSectionNames {
		if _, ok := content[s]; !ok {
			t.Errorf("full reply missing section %s", s)
		}
	}
	// the caps_keys section carries the 21-key account (final review
	// fix #2: 20 numeric + adc2_note — the la/ota additions in)
	ck, _ := content["caps_keys"].(map[string]any)
	keys, _ := ck["keys"].([]any)
	if len(keys) != 21 {
		t.Errorf("caps_keys.keys = %d entries, want 21", len(keys))
	}

	// filtered: exactly the named section
	res = e.call("xuansi_abi_reference", map[string]any{"section": "lua_boundaries"})
	if res.IsError {
		t.Fatalf("abi_reference(section) failed: %s", resultText(t, res))
	}
	m = structured(t, res)
	content, _ = m["content"].(map[string]any)
	if len(content) != 1 || content["lua_boundaries"] == nil {
		t.Fatalf("filtered content = %v, want exactly lua_boundaries", content)
	}
	lb, _ := content["lua_boundaries"].([]any)
	if len(lb) != 8 {
		t.Errorf("lua_boundaries = %d entries, want the eight", len(lb))
	}

	// unknown section: the api refusal lists the vocabulary
	res = e.call("xuansi_abi_reference", map[string]any{"section": "nope"})
	if !res.IsError {
		t.Fatal("unknown section must refuse")
	}
	if txt := resultText(t, res); !strings.Contains(txt, "host_table") || !strings.Contains(txt, "caps_keys") {
		t.Errorf("refusal = %q, want the section vocabulary listed", txt)
	}
}

func TestBenchIndexCoversTheRegistrationTable(t *testing.T) {
	idx, err := benchIndex()
	if err != nil {
		t.Fatalf("benchIndex: %v", err)
	}
	// the families table's eight + wasm
	for _, fam := range []string{"gpio", "uart", "i2c", "spi", "pwm", "adc", "rmt", "events", "wasm"} {
		if !idx.Members[fam] {
			t.Errorf("family %s missing from the bench index", fam)
		}
	}
	// wasm's seven ops (incl. the wave-2 plan/xfer)
	for _, op := range []string{"load", "call", "unload", "list", "info", "xfer", "plan"} {
		if !opIn(idx, "wasm", op) {
			t.Errorf("bench.wasm.%s missing from the bench index", op)
		}
	}
	// the bare members (timer_ms joined the generated table with the
	// W2.T5 seat finding — pinned here so a sidecar regression fails)
	for _, m := range []string{"millis", "timer_ms", "caps", "gc", "api_version"} {
		if !idx.Members[m] {
			t.Errorf("bare member bench.%s missing from the index", m)
		}
	}
	// family op spot-checks from the families table
	if !opIn(idx, "i2c", "scan") || !opIn(idx, "events", "next") || !opIn(idx, "gpio", "on") {
		t.Error("family op spot-check failed (i2c.scan / events.next / gpio.on)")
	}

	// the per-op face (2026-11-23 seat-debt audit): every family op in
	// the families lists carries a signature+semantics entry in ops —
	// the replacement face for the retired bench dynamic tools, locked
	// so a regeneration cannot silently drop it back to name lists
	// (families/ops values ride RawMessage: both carry a "_note" prose
	// key that is not a list)
	var sigs struct {
		Families map[string]json.RawMessage `json:"families"`
		Ops      map[string]json.RawMessage `json:"ops"`
	}
	raw, _ := abiReference().abiSectionRaw("bench_signatures")
	if err := json.Unmarshal(raw, &sigs); err != nil {
		t.Fatal(err)
	}
	for fam, famRaw := range sigs.Families {
		if fam == "_note" {
			continue
		}
		var want []string
		if err := json.Unmarshal(famRaw, &want); err != nil {
			t.Fatalf("families.%s: %v", fam, err)
		}
		var entries []struct {
			Sig string `json:"sig"`
		}
		if err := json.Unmarshal(sigs.Ops[fam], &entries); err != nil {
			t.Fatalf("ops.%s: %v", fam, err)
		}
		got := map[string]bool{}
		for _, e := range entries {
			name := strings.SplitN(strings.TrimPrefix(e.Sig, "bench."+fam+"."), "(", 2)[0]
			got[name] = true
		}
		if len(got) != len(want) {
			t.Errorf("ops.%s carries %d entries, want %d (one per family op)", fam, len(got), len(want))
		}
		for _, op := range want {
			if !got[op] {
				t.Errorf("ops.%s lacks the %s %s signature entry", fam, fam, op)
			}
		}
	}
}

func opIn(idx lualint.BenchIndex, fam, op string) bool {
	for _, o := range idx.Families[fam] {
		if o == op {
			return true
		}
	}
	return false
}

func TestRefWasmBudgetBytes(t *testing.T) {
	if b := refWasmBudgetBytes(); b != 2097152 {
		t.Errorf("refWasmBudgetBytes = %d, want 2097152 (the caps_keys constant)", b)
	}
}

// TestAbiReferenceQueryFilters (2026-11-23 seat-debt batch): query=
// narrows the served sections to the entries that mention the
// substring — case-insensitive, over signatures/semantics/keys/notes;
// no hit is an EMPTY content, never an error; an empty query is
// exactly the unfiltered behavior.
func TestAbiReferenceQueryFilters(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})

	// "i2c": bench_signatures rides with the families table narrowed
	// to exactly the i2c family, and host_table rides with exactly the
	// i2c host symbols
	res := e.call("xuansi_abi_reference", map[string]any{"query": "i2c"})
	if res.IsError {
		t.Fatalf("query=i2c failed: %s", resultText(t, res))
	}
	content, _ := structured(t, res)["content"].(map[string]any)
	bs, _ := content["bench_signatures"].(map[string]any)
	fams, _ := bs["families"].(map[string]any)
	if fams["i2c"] == nil || fams["gpio"] != nil {
		t.Errorf("bench_signatures.families after query=i2c = %v, want exactly the i2c family riding", fams)
	}
	ht, _ := content["host_table"].(map[string]any)
	syms, _ := ht["symbols"].([]any)
	if len(syms) == 0 {
		t.Fatal("host_table carried no symbols after query=i2c — the i2c host family must ride")
	}
	for _, s := range syms {
		sm, _ := s.(map[string]any)
		if sym, _ := sm["symbol"].(string); !strings.Contains(sym, "i2c") {
			t.Errorf("symbol %q rode an i2c query", sym)
		}
	}
	// a section with no i2c fact anywhere does not ride
	if _, ok := content["lua_boundaries"]; ok {
		t.Error("lua_boundaries rode an i2c query — no boundary mentions i2c")
	}

	// case-insensitive: I2C serves the same sections
	res = e.call("xuansi_abi_reference", map[string]any{"query": "I2C"})
	if res.IsError {
		t.Fatalf("query=I2C failed: %s", resultText(t, res))
	}
	upper, _ := structured(t, res)["content"].(map[string]any)
	if len(upper) != len(content) {
		t.Errorf("query=I2C served %d sections, query=i2c served %d — matching must be case-insensitive", len(upper), len(content))
	}

	// section + query compose: within bench_signatures, "scan" keeps
	// the i2c family's op list narrowed to the matching op
	res = e.call("xuansi_abi_reference", map[string]any{"section": "bench_signatures", "query": "scan"})
	if res.IsError {
		t.Fatalf("section+query failed: %s", resultText(t, res))
	}
	content, _ = structured(t, res)["content"].(map[string]any)
	if len(content) != 1 {
		t.Fatalf("section+query content = %v, want exactly bench_signatures", content)
	}
	bs, _ = content["bench_signatures"].(map[string]any)
	fams, _ = bs["families"].(map[string]any)
	if len(fams) != 1 {
		t.Errorf("families after query=scan = %v, want the i2c list alone", fams)
	} else if ops, _ := fams["i2c"].([]any); len(ops) != 1 || ops[0] != "scan" {
		t.Errorf("families.i2c after query=scan = %v, want exactly [scan]", ops)
	}

	// no hit: an EMPTY content, not an error
	res = e.call("xuansi_abi_reference", map[string]any{"query": "zzz-no-such-face"})
	if res.IsError {
		t.Fatalf("a no-hit query must not error: %s", resultText(t, res))
	}
	if m, _ := structured(t, res)["content"].(map[string]any); len(m) != 0 {
		t.Errorf("no-hit query content = %d sections, want the empty map", len(m))
	}

	// empty query = the unfiltered behavior: all eight sections ride
	res = e.call("xuansi_abi_reference", map[string]any{"query": ""})
	if res.IsError {
		t.Fatalf("empty query failed: %s", resultText(t, res))
	}
	if m, _ := structured(t, res)["content"].(map[string]any); len(m) != len(abiSectionNames) {
		t.Errorf("empty query content = %d sections, want all %d (exactly the no-query behavior)", len(m), len(abiSectionNames))
	}
}

// TestAbiReferenceQueryLeavesDocShape: a filtered reply keeps the
// document's envelope — the FULL section vocabulary in document order,
// the provenance pins, the abi block — with only content narrowed.
func TestAbiReferenceQueryLeavesDocShape(t *testing.T) {
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	res := e.call("xuansi_abi_reference", map[string]any{"query": "i2c"})
	if res.IsError {
		t.Fatalf("query=i2c failed: %s", resultText(t, res))
	}
	m := structured(t, res)
	secs, _ := m["sections"].([]any)
	if len(secs) != len(abiSectionNames) {
		t.Fatalf("sections = %v, want the full vocabulary even under a query", secs)
	}
	for i, s := range secs {
		if s != abiSectionNames[i] {
			t.Errorf("sections[%d] = %v, want %v (document order unchanged by the filter)", i, s, abiSectionNames[i])
		}
	}
	prov, _ := m["provenance"].(map[string]any)
	if prov["header_sha256"] == nil || prov["sidecar_sha256"] == nil {
		t.Errorf("provenance pins absent from a filtered reply: %v", prov)
	}
	if abi, _ := m["abi"].(map[string]any); abi["version"] == nil {
		t.Errorf("abi block absent from a filtered reply: %v", m["abi"])
	}
}

// TestAbiReferenceDriftLock — the sync twin, byte-for-byte. The
// upstream is the drivers repo's generated abi-reference.json; this
// checked-in copy must match it exactly (a regeneration upstream
// without a copy here is a red build — the generated bytes are the
// single source the seat embeds). XUANSI_DRIVERS_DIR names a checkout
// (the CI drivers-parity job); absent, the sibling ../xuansi-drivers
// checkout is probed; neither visible → skip.
func TestAbiReferenceDriftLock(t *testing.T) {
	repo := os.Getenv("XUANSI_DRIVERS_DIR")
	if repo == "" {
		sib := filepath.Join("..", "..", "..", "xuansi-drivers")
		if fi, err := os.Stat(sib); err == nil && fi.IsDir() {
			repo = sib
		}
	}
	if repo == "" {
		t.Skip("no drivers checkout visible (XUANSI_DRIVERS_DIR unset, no ../xuansi-drivers sibling) — the CI drivers-parity job runs the drift lock")
	}
	up, err := os.ReadFile(filepath.Join(repo, "abi-reference.json"))
	if err != nil {
		if os.IsNotExist(err) {
			t.Fatalf("%s carries no abi-reference.json — the generated file is the daemon copy's upstream", repo)
		}
		t.Fatalf("read upstream: %v", err)
	}
	if string(up) != string(abiReferenceJSON) {
		t.Errorf("the embedded abi-reference.json drifted from the drivers repo's generated copy (upstream %d bytes vs embedded %d) — re-copy the generated file into internal/mcp/ (the drivers file is the single source)", len(up), len(abiReferenceJSON))
	}
}

// TestCapsKeysAccount21 (final review fix #2): caps.limits is the
// 21-key account (20 numeric + adc2_note) — the section carries the
// three post-B15 additions (la_ring_budget_kb, ota_slots,
// ota_slot_bytes) in the fw's frozen three-exit order between
// wasm_inst_slots and adc2_note, the note states the count, and the
// two coaching Descs (board_op's caps reading, abi_reference's
// section list) say 21 — the whole MCP face agrees on the account.
func TestCapsKeysAccount21(t *testing.T) {
	raw, ok := abiReference().abiSectionRaw("caps_keys")
	if !ok {
		t.Fatal("no caps_keys section")
	}
	var ck struct {
		Note string `json:"note"`
		Keys []struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(raw, &ck); err != nil {
		t.Fatal(err)
	}
	if len(ck.Keys) != 21 {
		t.Fatalf("caps_keys serves %d keys, want 21 (20 numeric + adc2_note)", len(ck.Keys))
	}
	byName := map[string]int{}
	for i, k := range ck.Keys {
		byName[k.Key] = i
	}
	for _, k := range []string{"la_ring_budget_kb", "ota_slots", "ota_slot_bytes"} {
		if _, ok := byName[k]; !ok {
			t.Errorf("caps_keys lacks %q (the fw's limits table carries it)", k)
		}
	}
	// the fw's frozen order (xs_caps.c's three-exit single source):
	// the additions ride between wasm_inst_slots and adc2_note.
	if !(byName["wasm_inst_slots"] < byName["la_ring_budget_kb"] &&
		byName["la_ring_budget_kb"] < byName["ota_slots"] &&
		byName["ota_slots"] < byName["ota_slot_bytes"] &&
		byName["ota_slot_bytes"] < byName["adc2_note"]) {
		t.Errorf("caps_keys order drifted from the fw's three-exit order (indices: %v)", byName)
	}
	if !strings.Contains(ck.Note, "21 keys") || !strings.Contains(ck.Note, "20 numeric") {
		t.Errorf("note = %q, want the 21-key / 20-numeric account", ck.Note)
	}

	// the two Descs agree on 21
	e, _ := newEnv(t, nil, Options{Holder: "mcp-fixed01"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lr, err := e.cl.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatal(err)
	}
	desc := map[string]string{}
	for _, tool := range lr.Tools {
		desc[tool.Name] = tool.Description
	}
	if d := desc["xuansi_board_op"]; !strings.Contains(d, "21-key") || !strings.Contains(d, "ota_slots") {
		t.Errorf("board_op Desc caps account drifted: want the 21-key account naming ota_slots")
	}
	if d := desc["xuansi_abi_reference"]; !strings.Contains(d, "21-key") {
		t.Errorf("abi_reference Desc caps_keys line drifted: want 21-key")
	}
}
