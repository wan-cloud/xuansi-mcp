package mcp

// abi_reference.go — the seat's ABI reference face (B15 §4.14, W2-T4):
// the drivers repo's GENERATED single-source (abi-reference.json —
// tools/gen-abi-reference/gen.py reading the fw host header + the
// curated sidecar), embedded byte-for-byte and served by the
// xuansi_abi_reference tool. The checked-in copy is a SYNC TWIN, not
// a fork: the drivers file is the upstream, the drift-lock test
// (abi_reference_test.go, TestAbiReferenceDriftLock — the wasmhdr/
// drivers-parity precedent) byte-compares the two wherever a drivers
// checkout is visible (XUANSI_DRIVERS_DIR, or the sibling
// ../xuansi-drivers checkout), and the CI
// drivers-parity job runs the leg against the real checkout.

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/mcp/lualint"
)

//go:embed abi-reference.json
var abiReferenceJSON []byte

// abiSections is the generated document's sections block, each section
// kept as raw JSON so the seat serves it VERBATIM (the generated
// bytes are the single source; no re-typing can drift).
type abiSections struct {
	HostTable       json.RawMessage `json:"host_table"`
	CallConvention  json.RawMessage `json:"call_convention"`
	ValueDomain     json.RawMessage `json:"value_domain"`
	Lifecycle       json.RawMessage `json:"lifecycle"`
	ErrorFaces      json.RawMessage `json:"error_faces"`
	LuaBoundaries   json.RawMessage `json:"lua_boundaries"`
	BenchSignatures json.RawMessage `json:"bench_signatures"`
	CapsKeys        json.RawMessage `json:"caps_keys"`
}

// abiReferenceDoc is the whole generated document.
type abiReferenceDoc struct {
	Provenance map[string]any `json:"provenance"`
	ABI        map[string]any `json:"abi"`
	Sections   abiSections    `json:"sections"`
}

// abiSectionNames is the fixed section vocabulary, in the generated
// document's own order (host_table → caps_keys).
var abiSectionNames = []string{
	"host_table", "call_convention", "value_domain", "lifecycle",
	"error_faces", "lua_boundaries", "bench_signatures", "caps_keys",
}

// abiSectionRaw returns one section's raw JSON by name.
func (d *abiReferenceDoc) abiSectionRaw(name string) (json.RawMessage, bool) {
	switch name {
	case "host_table":
		return d.Sections.HostTable, true
	case "call_convention":
		return d.Sections.CallConvention, true
	case "value_domain":
		return d.Sections.ValueDomain, true
	case "lifecycle":
		return d.Sections.Lifecycle, true
	case "error_faces":
		return d.Sections.ErrorFaces, true
	case "lua_boundaries":
		return d.Sections.LuaBoundaries, true
	case "bench_signatures":
		return d.Sections.BenchSignatures, true
	case "caps_keys":
		return d.Sections.CapsKeys, true
	}
	return nil, false
}

var (
	abiRefOnce sync.Once
	abiRefDoc  *abiReferenceDoc
	abiRefErr  error
)

// abiReference parses the embedded document once (it is compiled in,
// so a parse failure is a build bug — it panics loudly at first use
// and TestAbiReferenceEmbedsParses pins it green in every run; the
// toolchain_snapshot.json embeddedToolchainIndex precedent).
func abiReference() *abiReferenceDoc {
	abiRefOnce.Do(func() {
		abiRefDoc, abiRefErr = parseAbiReference(abiReferenceJSON)
		if abiRefErr != nil {
			panic(fmt.Sprintf("abi-reference.json no longer parses: %v", abiRefErr))
		}
	})
	return abiRefDoc
}

func parseAbiReference(b []byte) (*abiReferenceDoc, error) {
	var d abiReferenceDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// ---------------------------------------------------------------------------
// the bench index (R4's ground truth) + the reference wasm budget

// benchSigsDoc is the bench_signatures section's shape (the per-family
// op lists mirror the fw registration table; "_note" keys are prose).
type benchSigsDoc struct {
	WASM []struct {
		Sig string `json:"sig"`
	} `json:"wasm"`
	Clock []struct {
		Sig string `json:"sig"`
	} `json:"clock"`
	Meta []struct {
		Sig string `json:"sig"`
	} `json:"meta"`
	Families map[string]json.RawMessage `json:"families"`
}

// capsKeysDoc is the caps_keys section's shape (18 keys; live keys
// carry "value":"live", numeric keys carry the number).
type capsKeysDoc struct {
	Keys []struct {
		Key   string `json:"key"`
		Value any    `json:"value"`
		Note  string `json:"note"`
	} `json:"keys"`
}

// benchIndex builds the lualint R4 index from the embedded reference
// — the SAME single source the tool serves, so the lint's known-member
// table can never drift from the seat's documentation of it.
func benchIndex() (lualint.BenchIndex, error) {
	var idx lualint.BenchIndex
	raw, ok := abiReference().abiSectionRaw("bench_signatures")
	if !ok {
		return idx, fmt.Errorf("abi-reference.json: no bench_signatures section")
	}
	var sigs benchSigsDoc
	if err := json.Unmarshal(raw, &sigs); err != nil {
		return idx, fmt.Errorf("bench_signatures: %w", err)
	}
	members := map[string]bool{}
	families := map[string][]string{}
	// bench.wasm.<op> — the wasm family rides its sig list
	for _, s := range sigs.WASM {
		m, ops := parseBenchSig(s.Sig)
		if m == "" {
			continue
		}
		members[m] = true
		if len(ops) == 2 {
			families[ops[0]] = append(families[ops[0]], ops[1])
		}
	}
	// clock/meta: bare members (millis, caps, gc, api_version)
	for _, s := range sigs.Clock {
		if m, _ := parseBenchSig(s.Sig); m != "" {
			members[m] = true
		}
	}
	for _, s := range sigs.Meta {
		if m, _ := parseBenchSig(s.Sig); m != "" {
			members[m] = true
		}
	}
	// the primitives families table (skip the "_note" prose key)
	for fam, rawOps := range sigs.Families {
		if fam == "_note" {
			continue
		}
		var ops []string
		if err := json.Unmarshal(rawOps, &ops); err != nil {
			return idx, fmt.Errorf("bench_signatures families.%s: %w", fam, err)
		}
		members[fam] = true
		families[fam] = append(families[fam], ops...)
	}
	return lualint.BenchIndex{Members: members, Families: families}, nil
}

// parseBenchSig splits "bench.wasm.load(name) -> true" into the first
// member ("wasm") and, when dotted, the (family, op) pair. A bare
// "bench.caps()" / "bench.api_version" yields the member alone.
func parseBenchSig(sig string) (member string, pair []string) {
	const p = "bench."
	if len(sig) <= len(p) || sig[:len(p)] != p {
		return "", nil
	}
	rest := sig[len(p):]
	// cut at the first '(' (calls) or take the whole (fields)
	for i := 0; i < len(rest); i++ {
		if rest[i] == '(' {
			rest = rest[:i]
			break
		}
	}
	for i := 0; i < len(rest); i++ {
		if rest[i] == '.' {
			return rest[:i], []string{rest[:i], rest[i+1:]}
		}
	}
	return rest, nil
}

// refWasmBudgetBytes answers caps_keys' wasm_memory_budget_bytes — the
// static wave-2 number the admission preflight's degraded leg falls
// back to when the live board account cannot be read (the caps_keys
// note names it live-maintained board-side; the checked-in value is
// the reference constant, honestly labeled projected:false).
func refWasmBudgetBytes() int64 {
	raw, ok := abiReference().abiSectionRaw("caps_keys")
	if !ok {
		return 0
	}
	var keys capsKeysDoc
	if err := json.Unmarshal(raw, &keys); err != nil {
		return 0
	}
	for _, k := range keys.Keys {
		if k.Key == "wasm_memory_budget_bytes" {
			if f, isNum := k.Value.(float64); isNum {
				return int64(f)
			}
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// the tool

type abiReferenceIn struct {
	Section string `json:"section,omitempty" jsonschema:"one section to serve: host_table, call_convention, value_domain, lifecycle, error_faces, lua_boundaries, bench_signatures, caps_keys; omit for the whole reference"`
}

// abiReferenceOut serves the generated document verbatim: the abi
// block (version, the recarve note, the load gate and memory-freeze
// sentences), the provenance (the header/sidecar sha256s the generator
// pinned — how stale this copy is, at a glance), the section
// vocabulary, and the requested section(s) as decoded JSON (the
// generated bytes are the single source; serving the parsed value with
// no re-typing cannot drift).
type abiReferenceOut struct {
	ABI        map[string]any `json:"abi" jsonschema:"the ABI block: version (1 — the 2026-11-11 recarve: module ABI and bench api_version are the SAME axis), the wave-2 load gate {1,2}, the initial==max memory freeze"`
	Provenance map[string]any `json:"provenance" jsonschema:"the generator's pins: the fw host header's sha256, the curated sidecar's sha256, and the regeneration command — this embedded copy was built from exactly those bytes"`
	Sections   []string       `json:"sections" jsonschema:"the full section vocabulary, in document order"`
	Content    map[string]any `json:"content" jsonschema:"the requested section(s), verbatim generated JSON — one key per section name"`
}

// registerAbiReference adds the reference tool (the register seam
// lives in tools_static.go; this face's implementation rides here,
// the tools_capture.go file-per-face precedent).
func (ts *toolset) registerAbiReference(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_abi_reference",
		Description: "The module-ABI reference — generated single-source from the firmware's host header (xuansi_host.h) plus a curated sidecar, embedded in this daemon and drift-locked byte-for-byte against the drivers repo's generated copy (the provenance block names the exact header/sidecar sha256s it was built from; regenerate there with tools/gen-abi-reference/gen.py). Sections: host_table (the xuansi.* import surface: every symbol, prototype, wasm view, semantics — plus the retired names), call_convention (module shape, the msgpack args/result packing, out-param convention, the 8192B/depth-8/256KB limits), value_domain (nil/bool/int64/string/array/map/float64; NaN/±Inf refused; integers exact to |v|<=2^53), lifecycle (load/unload/session_reset/vm_reopen/reboot claims semantics — unload does NOT release claims), error_faces (the seven classes and what each carries), lua_boundaries (the EIGHT boundary conditions learned-by-refusal otherwise — budget wall-clock, value domain, yield-vs-require, args bumps, timers/queue drops, script events, sync-op silence, unload-vs-claims), bench_signatures (the Lua bench.* table the dynamic tools and lua_lint's R4 are built from), caps_keys (the 18-key numeric caps.limits table — which values are live and which frozen). Read a section by name or the whole document. This is the WRITING-MODULES manual: consult it before writing a wasm module or a resident Lua driver, and check xuansi_lua_lint's findings against lua_boundaries.",
	}, ts.abiReferenceTool)
}

func (ts *toolset) abiReferenceTool(_ context.Context, _ *mcpsdk.CallToolRequest, in abiReferenceIn) (*mcpsdk.CallToolResult, abiReferenceOut, error) {
	doc := abiReference()
	if in.Section != "" {
		if _, ok := doc.abiSectionRaw(in.Section); !ok {
			return nil, abiReferenceOut{}, errWs("xuansi_abi_reference: unknown section %q (have: %v)", in.Section, abiSectionNames)
		}
	}
	names := abiSectionNames
	if in.Section != "" {
		names = []string{in.Section}
	}
	content := map[string]any{}
	for _, n := range names {
		raw, _ := doc.abiSectionRaw(n)
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, abiReferenceOut{}, errWs("xuansi_abi_reference: section %s does not decode: %v", n, err)
		}
		content[n] = v
	}
	return nil, abiReferenceOut{
		ABI:        doc.ABI,
		Provenance: doc.Provenance,
		Sections:   abiSectionNames,
		Content:    content,
	}, nil
}
