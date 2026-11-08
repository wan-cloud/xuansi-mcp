package mcp

// wasmparity_test.go — B11 task 9: the drivers-parity leg. The
// daemon's frozen flag block (tcBuildFlags) and its embedded host
// header (wasmhdr/xuansi_host.h) claim to be single-source twins of
// the drivers repo's build.sh CFLAGS and src/xuansi_host.h. This leg
// locks the claim at byte level: rebuild EVERY module of a drivers
// checkout through wasmBuildInner and sha256-compare against the
// repo's own committed modules/<n>/<v>/<n>.wasm (and index.json's
// pins). Any drift in either direction — daemon flags move, build.sh
// moves, or the header twins diverge — is a red build.
//
// Per-module initial memory mirrors build.sh init_mem_for's table
// (the same-source values; a drift there also lands red here because
// the rebuilt bytes stop matching the committed artifact).
//
// TestDriversParity is the real leg (env-gated: XUANSI_DRIVERS_DIR +
// a real wasi-sdk on disk; the CI drivers-parity job supplies both).
// TestDriversParityFixture pins the comparator hermetically — a fake
// clang, a planted repo shape, one wrong committed artifact that MUST
// read red — so the real leg's green cannot be a vacuous pass.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// parityInitialMemory is build.sh init_mem_for's table: modules whose
// --initial-memory is not the frozen 262144 default. Same-source
// values; the parity hash is what actually locks them to build.sh.
var parityInitialMemory = map[string]int64{
	"ledwatch": 720896, // 11 pages: the CCL working set + the 64KB wasm-ld stack
}

// parityIndex is index.json's minimal shape — the module table whose
// files[] pins are the third leg of the comparison.
type parityIndexMod struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Files   []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
		Bytes  int64  `json:"bytes"`
	} `json:"files"`
}

type parityIndex struct {
	Modules []parityIndexMod `json:"modules"`
}

// driversParity walks a drivers-repo checkout shape, rebuilds every
// module through wasmBuildInner, and compares three ways: rebuilt sha
// vs the committed artifact's sha, and both against index.json's pin
// when the module has an entry. Returns the problem lines (empty =
// parity holds) and per-module ok lines for the caller's log.
func driversParity(repoDir, sdkRoot string) (problems, okLines []string) {
	// Module discovery mirrors build.sh all_names(): every src/<name>/
	// dir carrying src/<name>/<name>.c. The test_*.c siblings are
	// host-side unity tests, never modules.
	ents, err := os.ReadDir(filepath.Join(repoDir, "src"))
	if err != nil {
		return []string{fmt.Sprintf("drivers parity: read src/: %v", err)}, nil
	}
	var names []string
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		if fi, serr := os.Stat(filepath.Join(repoDir, "src", e.Name(), e.Name()+".c")); serr == nil && !fi.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return []string{"drivers parity: no modules discovered under src/ — checkout shape drift?"}, nil
	}

	// index.json: absent = the two-way compare stands alone (noted per
	// module); present but unparseable = a broken deliverable, red.
	idx := map[string]*parityIndexMod{}
	if b, ierr := os.ReadFile(filepath.Join(repoDir, "index.json")); ierr == nil {
		var p parityIndex
		if uerr := json.Unmarshal(b, &p); uerr != nil {
			return []string{fmt.Sprintf("drivers parity: index.json: %v", uerr)}, nil
		}
		for i := range p.Modules {
			idx[p.Modules[i].Name] = &p.Modules[i]
		}
	} else if !os.IsNotExist(ierr) {
		return []string{fmt.Sprintf("drivers parity: read index.json: %v", ierr)}, nil
	}

	ts := &toolset{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for _, name := range names {
		verB, verr := os.ReadFile(filepath.Join(repoDir, "src", name, "VERSION"))
		if verr != nil {
			problems = append(problems, fmt.Sprintf("drivers parity: %s: read VERSION: %v", name, verr))
			continue
		}
		ver := strings.TrimSpace(string(verB))
		if ver == "" || !wsVerRe.MatchString(ver) {
			problems = append(problems, fmt.Sprintf("drivers parity: %s: bad VERSION %q (build.sh refuses anything outside [a-z0-9.])", name, ver))
			continue
		}
		src, serr := os.ReadFile(filepath.Join(repoDir, "src", name, name+".c"))
		if serr != nil {
			problems = append(problems, fmt.Sprintf("drivers parity: %s@%s: read source: %v", name, ver, serr))
			continue
		}
		mem := int64(tcWasmInitialMemory) // the frozen default
		if m, ok := parityInitialMemory[name]; ok {
			mem = m
		}
		wasm, warnings, berr := ts.wasmBuildInner(ctx, buildReq{Name: name, Source: string(src), InitialMemory: mem, WasiSDKPath: sdkRoot})
		if berr != nil {
			problems = append(problems, fmt.Sprintf("drivers parity: %s@%s: rebuild failed: %v (warnings tail: %s)", name, ver, berr, tcWarnTail(warnings)))
			continue
		}
		got := sha256hex(wasm)

		wantPath := filepath.Join(repoDir, "modules", name, ver, name+".wasm")
		wantBytes, rerr := os.ReadFile(wantPath)
		if rerr != nil {
			problems = append(problems, fmt.Sprintf("drivers parity: %s@%s: committed artifact missing: %v", name, ver, rerr))
			continue
		}
		want := sha256hex(wantBytes)
		if got != want {
			problems = append(problems, fmt.Sprintf("drivers parity: %s@%s: rebuilt sha256 %s != committed %s (%d vs %d bytes) — frozen-flag or host-header drift (daemon tcBuildFlags/wasmhdr vs drivers build.sh/src)", name, ver, got, want, len(wasm), len(wantBytes)))
			continue
		}
		line := fmt.Sprintf("drivers parity: %s@%s: sha256 %s, %d bytes — identical", name, ver, got, len(wasm))

		// the index leg: a pinned entry must agree with the committed
		// artifact (and so, transitively, with the rebuild above); an
		// absent entry is a note, not a red — new modules land their
		// committed .wasm before the index grows.
		if m, ok := idx[name]; ok {
			if m.Version != ver {
				problems = append(problems, fmt.Sprintf("drivers parity: %s: VERSION %q but index.json pins version %q", name, ver, m.Version))
				continue
			}
			var pinSHA string
			pinBytes := int64(-1)
			for _, f := range m.Files {
				if f.Path == filepath.ToSlash(filepath.Join("modules", name, ver, name+".wasm")) {
					pinSHA, pinBytes = f.SHA256, f.Bytes
					break
				}
			}
			if pinSHA == "" {
				problems = append(problems, fmt.Sprintf("drivers parity: %s@%s: index.json entry carries no pin for modules/%s/%s/%s.wasm", name, ver, name, ver, name))
				continue
			}
			if !strings.EqualFold(want, pinSHA) {
				problems = append(problems, fmt.Sprintf("drivers parity: %s@%s: committed sha256 %s != index.json pin %s", name, ver, want, pinSHA))
				continue
			}
			if pinBytes != int64(len(wantBytes)) {
				problems = append(problems, fmt.Sprintf("drivers parity: %s@%s: committed %d bytes != index.json pin %d", name, ver, len(wantBytes), pinBytes))
				continue
			}
			line += " (index-pinned)"
		} else {
			line += " (no index.json entry — noted)"
		}
		okLines = append(okLines, line)
	}
	return problems, okLines
}

// TestDriversParity — the real leg. XUANSI_DRIVERS_DIR names the
// drivers checkout (absent = skip; set-but-broken = fatal: a
// misconfiguration must be loud, never a silent skip), and a real
// wasi-sdk must be resolvable (wasmRealSDKRoot's skip contract, the
// TestWasmBuildReal twin).
func TestDriversParity(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode: the real parity compile is skipped")
	}
	repo := os.Getenv("XUANSI_DRIVERS_DIR")
	if repo == "" {
		t.Skip("XUANSI_DRIVERS_DIR not set — no drivers checkout to parity-check (the CI drivers-parity job runs this leg)")
	}
	if fi, err := os.Stat(repo); err != nil || !fi.IsDir() {
		t.Fatalf("XUANSI_DRIVERS_DIR=%q is not a readable directory: %v", repo, err)
	}
	sdkRoot := wasmRealSDKRoot()
	if sdkRoot == "" {
		t.Skip("no real wasi-sdk in the toolchain cache — land one with xuansi_toolchain_fetch (the CI drivers-parity job runs this leg)")
	}
	problems, okLines := driversParity(repo, sdkRoot)
	for _, line := range okLines {
		t.Log(line)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// TestDriversParityFixture pins the comparator hermetically: no real
// toolchain (the fake-clang sdk), a planted repo shape with three
// modules — hello's committed bytes deliberately WRONG (must read
// red, naming the sha mismatch), ledwatch right (and riding the
// 720896 initial-memory table leg), probe right but absent from
// index.json (the no-entry note path). A comparator that vacuously
// passes — empty walk, swallowed error — fails here, so the real
// leg's green means something.
func TestDriversParityFixture(t *testing.T) {
	sdk := fakeWasmSDK(t)
	repo := t.TempDir()
	body := fakeWasmBody()

	// plant plants one module's checkout shape: src/<n>/<n>.c,
	// src/<n>/VERSION, modules/<n>/<v>/<n>.wasm.
	plant := func(name, ver string, committed []byte) {
		t.Helper()
		for p, b := range map[string][]byte{
			filepath.Join("src", name, name+".c"):             []byte("int " + name + ";\n"),
			filepath.Join("src", name, "VERSION"):             []byte(ver + "\n"),
			filepath.Join("modules", name, ver, name+".wasm"): committed,
		} {
			if err := os.MkdirAll(filepath.Join(repo, filepath.Dir(p)), 0o755); err != nil {
				t.Fatalf("plant %s: %v", p, err)
			}
			if err := os.WriteFile(filepath.Join(repo, p), b, 0o644); err != nil {
				t.Fatalf("plant %s: %v", p, err)
			}
		}
	}
	bad := append([]byte{}, body...)
	bad[len(bad)-1] ^= 0xff // hello's committed artifact: wrong final byte
	plant("hello", "1.0.0", bad)
	plant("ledwatch", "1.0.0", append([]byte{}, body...))
	plant("probe", "0.2.1", append([]byte{}, body...))

	// index.json pins hello's wrong committed bytes (self-consistent
	// wrongness: the rebuild must disagree with BOTH legs) and
	// ledwatch's right ones; probe stays absent on purpose.
	idxJSON := fmt.Sprintf(`{"index_ver":1,"modules":[
	  {"name":"hello","version":"1.0.0","files":[
	    {"path":"modules/hello/1.0.0/hello.wasm","sha256":%q,"bytes":%d}]},
	  {"name":"ledwatch","version":"1.0.0","files":[
	    {"path":"modules/ledwatch/1.0.0/ledwatch.wasm","sha256":%q,"bytes":%d}]}
	]}`, sha256hex(bad), len(bad), sha256hex(body), len(body))
	if err := os.WriteFile(filepath.Join(repo, "index.json"), []byte(idxJSON), 0o644); err != nil {
		t.Fatalf("plant index.json: %v", err)
	}

	problems, okLines := driversParity(repo, sdk)

	// red leg: hello named, with the sha-mismatch wording.
	if len(problems) == 0 {
		t.Fatalf("drivers parity: hello's wrong committed artifact read GREEN — the comparator is vacuous (problems=%v ok=%v)", problems, okLines)
	}
	redHello := false
	for _, p := range problems {
		if strings.Contains(p, "hello@1.0.0") && strings.Contains(p, "rebuilt sha256") {
			redHello = true
		}
	}
	if !redHello {
		t.Errorf("drivers parity: want hello@1.0.0 named with a sha mismatch, got problems: %v", problems)
	}
	// green legs: ledwatch and probe ok, never in problems.
	for _, p := range problems {
		if strings.Contains(p, "ledwatch") || strings.Contains(p, "probe") {
			t.Errorf("drivers parity: right module named in problems: %s", p)
		}
	}
	for _, want := range []string{"ledwatch@1.0.0", "probe@0.2.1"} {
		found := false
		for _, line := range okLines {
			if strings.Contains(line, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("drivers parity: %s missing from the ok lines: %v", want, okLines)
		}
	}
}
