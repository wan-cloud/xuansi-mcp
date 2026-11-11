package mcp

// manifest.go — the driver workspace's manifest (batch 6 task 11,
// design §7 daemon-side version management): $WS/manifest.json records
// per driver name the ACTIVE version (the last one pushed to a board —
// the version a bare xuansi_driver_push re-deploys) and one FileMeta
// per version (checksum, size, last push time, per-board push
// footprints). The versioned files under drivers/ ARE the history; the
// manifest is the index plus audit trail (生效版本、checksum、來源).
//
// All writes are atomic (temp file + rename, the temp fsynced, the
// directory fsynced after the rename — crash durability) and every
// read-modify-write cycle is serialized in-process by wsMu
// (workspace.go): a reader ever only sees a whole manifest, and
// concurrent pushes merge their footprints instead of losing them
// (last-writer-wins on Active, every Boards entry kept).
//
// Single-writer expectation (final review): wsMu serializes the RMW
// in-process ONLY — there is no cross-process lock, so two concurrent
// `xuansi mcp` sessions sharing one workspace are each free to
// read-modify-write manifest.json from the last on-disk state and can
// drop one another's Active/footprint entries (classic lost update).
// The blast radius is bounded: manifest.json itself always stays whole
// (atomic rename), versioned driver files never collide (their names
// are unique by construction), and board state stays verified — the
// manifest records a push ONLY after the commit verdict verifies, so a
// corrupt board write never becomes history (B15 §4.5②; pre-§4.5 the
// same guarantee rode a post-push byte read-back). The worst case is
// a stale Active or a missing audit entry, never a corrupt file or an
// unverified push. Don't run parallel MCP sessions against one
// workspace.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Manifest is the whole manifest.json: driver name → entry.
type Manifest struct {
	Drivers map[string]DriverEntry `json:"drivers"`
}

// DriverEntry is one driver's record: the active version ("" until a
// push activates one) and the per-version file metadata. Kind (B10
// task 5, passthrough widened B15 W1.T4, vocabulary reconciled W2.T4)
// carries the fetch index's `kind` VERBATIM — the post-restructure
// index speaks the RELEASE-CLASS vocabulary there
// ("production"/"test-canary"), and the seat's `kind` axis is exactly
// that vocabulary, one meaning everywhere (fetch/push/ws_list/module_
// doc): a release class, never a module type and never a build lane.
// The per-version module-type and dev-lane facts live in FileMeta
// (Release/Dev) where they belong — a version-scoped fact never
// masquerades as the driver's release class. Hand-put versions and a
// kindless index leave it empty; the daemon never judges the value.
type DriverEntry struct {
	Active string              `json:"active,omitempty"`
	Kind   string              `json:"kind,omitempty"`
	Files  map[string]FileMeta `json:"files"`
}

// FileMeta describes one stored version: content checksum and size,
// when it was last pushed to ANY board (the build face records its
// built-at there), and per board (mac6 when the local board list
// resolved it, else the raw selector) when it was last pushed THERE
// — the fleet audit trail behind the design's
// "艦隊 = 推 N 片 + 對 checksum" loop.
type FileMeta struct {
	Sha256   string               `json:"sha256"`
	Size     int                  `json:"size"`
	PushedAt time.Time            `json:"pushed_at,omitempty"`
	Boards   map[string]time.Time `json:"boards,omitempty"`

	// Aot (P3-T13) is the version's per-target .aot record — the
	// pairing CLAIM a bare driver_push acts on: target key → the .aot
	// artifact's sha/size (the §8-6 namespace, "esp32s3" today,
	// classic/C6 keys later without migration). Populated by the fetch
	// (the index files[] target field) and the build aot lane (the
	// frozen --cpu=esp32s3 recipe); additive — pre-T13 manifests load
	// unchanged and simply never pair. A record whose sha disagrees
	// with drivers/<name>@<ver>.aot on disk is a refusal, not a guess.
	Aot map[string]AotSlot `json:"aot,omitempty"`

	// Release (B15 W2-T4) is the release class the version's own
	// per-version meta declared (the drivers meta.json `release`
	// field: production/test-canary) — the per-version twin of the
	// index-level Kind. The seat's module_doc maps kind from the entry
	// Kind first (the index vocabulary), falling back to this when the
	// index carried none. Absent for versions stored before the meta
	// leg or from kindless sources.
	Release string `json:"release,omitempty"`

	// Dev (B15 W2-T4) marks a wasm-build version whose compile rode a
	// §6.4 override (wamrc_path / wasi_sdk_path) — visibly a dev
	// artifact, not a cache-paired build. Formerly this rode the entry
	// Kind as "wasm-dev", polluting the seat's kind vocabulary with a
	// build lane; the reconciliation moved it here, a version-scoped
	// boolean surfaced by ws_list.
	Dev bool `json:"dev,omitempty"`

	// Doc (B15 W2-T4) is the per-version module documentation the
	// fetch records from the index artifact's meta.json `doc` field
	// (the drivers repo's doc.md, extractor-managed): the ops table
	// and the measured list xuansi_module_doc serves. Absent for
	// versions fetched from pre-restructure indexes (their doc answer
	// falls back to GitHub).
	Doc *DocMeta `json:"doc,omitempty"`

	// Footprint (B15 W2-T4) is the version's static memory footprint
	// the meta.json `footprint` block declared — the admission
	// preflight's candidate number (the memory account bills the
	// linear-memory initial only; text/pool are the .aot lane's info
	// columns). Absent for pre-restructure versions (their pushes
	// carry no admission projection, honestly).
	Footprint *FootprintMeta `json:"footprint,omitempty"`
}

// DocMeta is the per-version documentation payload (meta.json doc).
type DocMeta struct {
	Ops      []DocOp        `json:"ops,omitempty"`
	Measured map[string]any `json:"measured,omitempty"`
}

// DocOp is one exported op's row (doc.md's Ops table, extractor-locked).
type DocOp struct {
	Name      string `json:"name"`
	Args      string `json:"args,omitempty"`
	Result    string `json:"result,omitempty"`
	Semantics string `json:"semantics,omitempty"`
}

// FootprintMeta is the version's static footprint: the .wasm lane's
// linear memory (initial==max under the ABI 1 freeze) and, when the
// version carries .aot artifacts, their per-target trio (the initial
// the account bills + text/pool as info columns).
type FootprintMeta struct {
	Wasm FootprintWasm           `json:"wasm"`
	Aot  map[string]FootprintAot `json:"aot,omitempty"`
}

type FootprintWasm struct {
	Initial int64 `json:"initial_bytes"`
	Max     int64 `json:"max_bytes,omitempty"`
}

type FootprintAot struct {
	Initial int64 `json:"initial_bytes"`
	Text    int64 `json:"text_bytes,omitempty"`
	Pool    int64 `json:"pool_bytes,omitempty"`
}

// AotSlot is one target's .aot record inside FileMeta.Aot.
type AotSlot struct {
	Sha256 string `json:"sha256"`
	Size   int    `json:"size"`
}

// entry returns the driver's entry with Files non-nil — manifests
// from older versions (or hand edits) may miss maps, and callers
// mutate what they get back.
func (m *Manifest) entry(name string) DriverEntry {
	e, ok := m.Drivers[name]
	if !ok {
		return DriverEntry{Files: map[string]FileMeta{}}
	}
	if e.Files == nil {
		e.Files = map[string]FileMeta{}
	}
	return e
}

// loadManifest reads manifest.json. A missing file is an empty
// manifest (a fresh workspace); a corrupt one REFUSES rather than
// being overwritten on the next write — the workspace's history is
// not disposable.
func loadManifest(path string) (*Manifest, error) {
	b, err := readFileShared(path) // share-delete on windows: must not block a concurrent atomic replace
	if os.IsNotExist(err) {
		return &Manifest{Drivers: map[string]DriverEntry{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	if m.Drivers == nil {
		m.Drivers = map[string]DriverEntry{}
	}
	for name, e := range m.Drivers { // nil-guard nested maps the same way
		if e.Files == nil {
			e.Files = map[string]FileMeta{}
			m.Drivers[name] = e
		}
	}
	return &m, nil
}

// writeDurableFile writes data to path ATOMICALLY (B8 item 5): a temp
// file in the SAME directory (the rename stays on one filesystem),
// written, fsynced, chmodded to perm, closed, and renamed over the
// target (on windows via replaceFile — a bounded retry past transient
// sharing violations from concurrent readers, see rename_windows.go),
// then the parent directory fsynced (unix; on windows fsyncDir is a
// no-op — no POSIX dir-fsync exists there, see fsync_windows.go) — the
// crash-durability pair (without the file fsync the bytes may be lost
// on power failure; without the directory fsync the fresh rename can
// vanish). A concurrent reader
// ever only sees the old or the new whole file, never a torn mid-write
// (the pre-B8 in-place O_TRUNC write was durable but tearable: caught
// mid-write, the manifest's old sha disagreed with the half-written
// driver until a push re-verified) — that rename semantics half is
// pinned by TestWriteDurableFileNeverTorn; the crash half stays
// code-review-only (no crash simulation).
func writeDurableFile(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".durable-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil { // CreateTemp's 0600 → the caller's perm
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceFile(tmpName, path); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(path))
}

// fsyncDir flushes one directory's entries (create/rename) to stable
// storage — the other half of writeDurableFile, and of an atomic
// rename: without it the rename itself may be unrecorded after a
// crash. Same code-review-only verification note as writeDurableFile.
// Platform split: see fsync_unix.go / fsync_windows.go (Windows has no
// POSIX dir-fsync — the first CI matrix run caught the unix body
// failing there with Access is denied).

// writeManifestAtomic writes m to path atomically — the same
// temp+Sync+rename+dir-fsync writeDurableFile performs (B8 item 5
// generalized it from the manifest's private helper to the one atomic
// writer both files ride): a concurrent reader ever only sees the old
// or the new whole manifest, never a torn write.
func writeManifestAtomic(path string, m *Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return writeDurableFile(path, b, 0o600)
}
