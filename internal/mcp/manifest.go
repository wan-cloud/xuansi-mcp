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
// are unique by construction), and board state stays read-back-verified
// (sha256 read-back precedes any manifest write) — the worst case is a
// stale Active or a missing audit entry, never a corrupt file or an
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
// push activates one) and the per-version file metadata.
type DriverEntry struct {
	Active string              `json:"active,omitempty"`
	Files  map[string]FileMeta `json:"files"`
}

// FileMeta describes one stored version: content checksum and size,
// when it was last pushed to ANY board, and per board (mac6 when the
// local board list resolved it, else the raw selector) when it was
// last pushed THERE — the fleet audit trail behind the design's
// "艦隊 = 推 N 片 + 對 checksum" loop.
type FileMeta struct {
	Sha256   string               `json:"sha256"`
	Size     int                  `json:"size"`
	PushedAt time.Time            `json:"pushed_at,omitempty"`
	Boards   map[string]time.Time `json:"boards,omitempty"`
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
	b, err := os.ReadFile(path)
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

// writeDurableFile writes data to path and fsyncs BOTH the file and
// its parent directory — the crash-durability pair (B7 minors item 2):
// without the file fsync the bytes may be lost on power failure, and
// without the directory fsync a fresh directory entry can vanish while
// the data sits in an orphaned block. Verified by code review only —
// fsync semantics cannot be unit-proven without crash simulation
// (controller ruling: implement + note, no unit red/green possible).
func writeDurableFile(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(path))
}

// fsyncDir flushes one directory's entries (create/rename) to stable
// storage — the other half of writeDurableFile, and of an atomic
// rename: without it the rename itself may be unrecorded after a
// crash. Same code-review-only verification note as writeDurableFile.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// writeManifestAtomic writes m to path atomically: a temp file in the
// SAME directory (the rename stays on one filesystem), written,
// fsynced and renamed over the target, then the directory fsynced so
// the rename itself survives a crash — a concurrent reader ever only
// sees the old or the new whole manifest, never a torn write.
func writeManifestAtomic(path string, m *Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(path), ".manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(b); err != nil {
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
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(path))
}
