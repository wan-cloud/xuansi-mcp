package mcp

// module_doc.go — the module documentation face (B15 §4.18, W2-T4):
// one module version's ops table, measured list and release class,
// WORKSPACE FIRST (the manifest's FileMeta.Doc/Release the fetch
// recorded from the index artifacts' meta.json) with the GitHub
// index+meta fallback riding driver_fetch's existing machinery. The
// seat's kind axis is the release class everywhere: the index entry's
// kind when it carries one, else the meta's release field.

import (
	"context"
	"encoding/json"
	"net/url"
	"sort"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

type moduleDocIn struct {
	Name     string `json:"name" jsonschema:"module name, lowercase [a-z0-9_-]+ (e.g. \"hello\", \"ledwatch\")"`
	Version  string `json:"version,omitempty" jsonschema:"omit to resolve: the workspace's ACTIVE version, else its highest stored, else the index's highest for the name (driver_fetch's rule)"`
	IndexURL string `json:"index_url,omitempty" jsonschema:"the index.json URL for the GitHub fallback leg; omit for the default https://raw.githubusercontent.com/wan-cloud/xuansi-drivers/main/index.json"`
}

// moduleDocOut is the frozen seat shape: identity, the release class,
// the ops table, the measured list, and which source answered.
type moduleDocOut struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	// Kind is the RELEASE CLASS (production / test-canary) — the index
	// entry's kind field, else the meta's release claim; absent when
	// neither source carries one.
	Kind      string         `json:"kind,omitempty" jsonschema:"the RELEASE CLASS (production / test-canary): the index entry's kind field, else the version's own meta release claim — the same one vocabulary every seat face speaks; absent when neither carries one"`
	Ops       []DocOp        `json:"ops,omitempty" jsonschema:"the version's exported ops table (name/args/result/semantics per row — the drivers repo's per-version doc.md, extractor-locked into the meta)"`
	Semantics string         `json:"semantics,omitempty" jsonschema:"a module-level semantics summary, present only when the source meta carries one (today's metas carry per-op semantics only — each row's own semantics field)"`
	Measured  map[string]any `json:"measured,omitempty" jsonschema:"the version's measured list (byte counts, footprint sizes, timing measurements — free-form keys, read by presence)"`
	Source    string         `json:"source" jsonschema:"\"workspace\" (the daemon-side manifest's recorded meta — what THIS daemon fetched) or \"github\" (the index + raw meta, the fallback when the workspace holds no doc for the version)"`
}

// registerModuleDoc adds the doc tool (the register seam lives in
// tools_static.go).
func (ts *toolset) registerModuleDoc(srv *mcpsdk.Server) {
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        "xuansi_module_doc",
		Description: "One module version's documentation: the ops table (each row's name/args/result/semantics), the measured list (footprint sizes, timings — free-form keys), and the release class — the single source is the drivers repo's per-version doc.md, machine-read into the index artifacts' meta.json. WORKSPACE FIRST: a version this daemon fetched carries its recorded meta in the workspace manifest (source:\"workspace\") — the doc describes exactly what xuansi_driver_push would deploy here. GITHUB FALLBACK: a version the workspace does not hold (or holds without meta — pre-restructure fetches) answers from the public index + raw meta (source:\"github\"), the same fetch machinery xuansi_driver_fetch rides. kind is the RELEASE CLASS everywhere (production / test-canary): the index entry's kind, else the version meta's release. version omitted resolves like driver_fetch: the workspace ACTIVE, else its highest stored, else the index's highest. Pair with xuansi_abi_reference (the host table and Lua boundaries an op's semantics assume) and xuansi_lua_lint (check your driver source before pushing).",
	}, ts.moduleDocTool)
}

func (ts *toolset) moduleDocTool(ctx context.Context, _ *mcpsdk.CallToolRequest, in moduleDocIn) (*mcpsdk.CallToolResult, moduleDocOut, error) {
	if !wsNameRe.MatchString(in.Name) {
		return nil, moduleDocOut{}, errWs("xuansi_module_doc: name must match [a-z0-9_-]+, got %q", in.Name)
	}
	if in.Version != "" && !wsVerRe.MatchString(in.Version) {
		return nil, moduleDocOut{}, errWs("xuansi_module_doc: version must match [a-z0-9.]+, got %q", in.Version)
	}
	raw := in.IndexURL
	if raw == "" {
		raw = defaultIndexURL
	}
	iu, uerr := url.Parse(raw)
	if uerr != nil || (iu.Scheme != "http" && iu.Scheme != "https") || iu.Host == "" {
		return nil, moduleDocOut{}, errWs("xuansi_module_doc: index_url must be an http(s) URL, got %q", raw)
	}

	// workspace first: resolve the version, then serve its recorded
	// meta when the version carries one.
	if out, ok := ts.moduleDocWorkspace(in.Name, in.Version); ok {
		return nil, out, nil
	}

	// GitHub fallback: the index (version resolution + the release
	// class) + the raw per-version meta.
	out, err := ts.moduleDocGitHub(ctx, iu, in.Name, in.Version)
	if err != nil {
		return nil, moduleDocOut{}, err
	}
	return nil, out, nil
}

// moduleDocWorkspace resolves name+version against the manifest and
// serves the recorded meta when present. ok=false falls through to
// the GitHub leg (version unknown here, or stored without meta).
func (ts *toolset) moduleDocWorkspace(name, version string) (moduleDocOut, bool) {
	wsMu.Lock()
	m, err := loadManifest(ts.ws.manifestPath())
	wsMu.Unlock()
	if err != nil {
		return moduleDocOut{}, false // a broken manifest is the fallback's problem to report
	}
	e, exists := m.Drivers[name]
	if !exists {
		return moduleDocOut{}, false
	}
	ver := version
	if ver == "" {
		if e.Active != "" {
			ver = e.Active
		} else {
			for v := range e.Files {
				if ver == "" || cmpVer(v, ver) > 0 {
					ver = v
				}
			}
		}
	}
	fm, has := e.Files[ver]
	if !has || fm.Doc == nil {
		return moduleDocOut{}, false // not stored here, or stored pre-meta
	}
	kind := e.Kind // the index vocabulary first…
	if kind == "" {
		kind = fm.Release // …else the version's own release claim
	}
	ops := append([]DocOp(nil), fm.Doc.Ops...)
	return moduleDocOut{
		Name: name, Version: ver, Kind: kind,
		Ops: ops, Measured: fm.Doc.Measured, Source: "workspace",
	}, true
}

// moduleDocGitHub resolves through the public index and reads the
// version's raw meta.json (driver_fetch's index machinery, read-only:
// index for existence/version/kind, the conventional
// modules/<n>/<v>/<n>.meta.json path for the doc payload).
func (ts *toolset) moduleDocGitHub(ctx context.Context, iu *url.URL, name, version string) (moduleDocOut, error) {
	body, gerr := fetchGet(ctx, iu.String(), fetchIndexMax)
	if gerr != nil {
		return moduleDocOut{}, errWs("xuansi_module_doc: index %s: %v", iu.String(), gerr)
	}
	var idx fetchIndex
	if err := json.Unmarshal(body, &idx); err != nil {
		return moduleDocOut{}, errWs("xuansi_module_doc: index %s: not valid JSON: %v", iu.String(), err)
	}
	if idx.IndexVer != 1 {
		return moduleDocOut{}, errWs("xuansi_module_doc: index %s: unsupported index_ver %d (want 1)", iu.String(), idx.IndexVer)
	}
	var vers []string
	var chosen *fetchModule
	for i := range idx.Modules {
		mod := &idx.Modules[i]
		if mod.Name != name {
			continue
		}
		vers = append(vers, mod.Version)
		switch {
		case version != "":
			if mod.Version == version {
				chosen = mod
			}
		case chosen == nil || cmpVer(mod.Version, chosen.Version) > 0:
			chosen = mod
		}
	}
	if chosen == nil {
		if len(vers) == 0 {
			return moduleDocOut{}, errWs("xuansi_module_doc: module %q not in index %s and not in the workspace manifest", name, iu.String())
		}
		sort.Strings(vers)
		return moduleDocOut{}, errWs("xuansi_module_doc %s: version %q not in index (have %s) and not in the workspace manifest", name, version, strings.Join(vers, ", "))
	}
	out := moduleDocOut{Name: name, Version: chosen.Version, Kind: chosen.Kind, Source: "github"}

	// the raw meta (same best-effort contract as the fetch leg:
	// absence tolerated, garbage refused)
	meta, merr := fetchModuleMeta(ctx, iu, name, chosen.Version)
	if merr != nil {
		return moduleDocOut{}, merr
	}
	if meta != nil {
		if out.Kind == "" {
			out.Kind = meta.Release
		}
		if meta.Doc != nil {
			for _, op := range meta.Doc.Ops {
				out.Ops = append(out.Ops, DocOp{Name: op.Name, Args: op.Args, Result: op.Result, Semantics: op.Semantics})
			}
			out.Measured = meta.Doc.Measured
		}
	}
	if out.Ops == nil {
		return moduleDocOut{}, errWs("xuansi_module_doc %s@%s: no ops table anywhere — neither the workspace manifest nor the index's meta.json carries one", name, chosen.Version)
	}
	return out, nil
}
