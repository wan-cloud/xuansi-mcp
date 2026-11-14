// Package la lands the LA (logic-analyzer) capture objects on the
// daemon host and pulls them back losslessly from the board's listen
// ring. One capture is a directory captures/la/<cap_id>/ holding the
// frozen-format data.bin plus a meta.json account; PullCapture drives
// the v9 la.* op quintet (arm → done event → stop → info → read
// windows → verify → disarm) with the checksum-identity assertion as
// part of the pull itself — a pull that cannot prove the landed bytes
// equal the ring's is an error, never a warning.
package la

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"xuansi/internal/paths"
)

// DefaultQuota is the capture store's FIFO retention quota (the plan's
// frozen 512MB): Retention() deletes the oldest landed objects until
// the store sits under it.
const DefaultQuota = 512 << 20

// LARoot is the frozen capture-object root: DataDir()/captures/la —
// the la subdirectory of the existing captures root (the same
// paths.DataDir landing discipline internal/server's capture face
// rides; the daemon's own filesystem takes the host separator).
func LARoot() string { return filepath.Join(paths.DataDir(), "captures", "la") }

// Meta is the capture object's meta.json — the frozen column set of
// the LA wave plus the honest ledger mirrored from the fw's la.info.
// The mode-alternative columns (pins for edge/samples captures,
// rate_hz for sample rate, res_hz for edge timestamp resolution,
// runs/samples record counts) are omitempty: a listen capture carries
// port/pin/idle_syms/chunks, the later modes add their own.
type Meta struct {
	CapID    string `json:"cap_id"` // the daemon-side object id (directory name)
	Board    string `json:"board"`  // mac6 identity recorded by the puller
	Mode     string `json:"mode"`   // "listen" (this wave; "edge"/"samples" land with T10/T12)
	Port     int    `json:"port"`
	Pin      int    `json:"pin"`
	Pins     []int  `json:"pins,omitempty"`
	Baud     int    `json:"baud"` // resolved baud (auto → the measured value)
	IdleSyms int    `json:"idle_syms,omitempty"`
	RateHz   int    `json:"rate_hz,omitempty"`
	ResHz    int    `json:"res_hz,omitempty"`
	TStartNS uint64 `json:"t_start_ns"` // first chunk's start_ns (0 when empty)
	TEndNS   uint64 `json:"t_end_ns"`   // last chunk's start_ns (0 when empty)
	Bytes    int64  `json:"bytes"`      // payload bytes: Σ chunk n parsed from data.bin
	Chunks   int    `json:"chunks"`
	Runs     int    `json:"runs,omitempty"`
	Samples  int64  `json:"samples,omitempty"`
	Reason   string `json:"reason"` // full | stopped | limit

	// The fw-side ledger mirrored at pull time (la.stop / la.info);
	// zeros are omitted — an incomplete landing carries only what the
	// stopped engine could account.
	RingKB        int   `json:"ring_kb,omitempty"`
	MaxMS         int   `json:"max_ms,omitempty"`
	StreamBytes   int64 `json:"stream_bytes"` // len(data.bin) — the ring stream verbatim
	ChunksDropped int64 `json:"chunks_dropped,omitempty"`
	BytesDropped  int64 `json:"bytes_dropped,omitempty"`
	HwOvf         int64 `json:"hw_ovf,omitempty"`
	ArmedNS       int64 `json:"armed_ns,omitempty"`
	StoppedNS     int64 `json:"stopped_ns,omitempty"`

	SHA256     string    `json:"sha256"` // data.bin's digest (pass-1 == pass-2 identity)
	Incomplete bool      `json:"incomplete,omitempty"`
	PulledAt   time.Time `json:"pulled_at"`
}

// CaptureRef is a landed (or partially landed) capture object: the id,
// its two files, and the parsed meta. An Incomplete meta rides a ref
// returned together with an error — the object is forensics, not a
// lossless capture.
type CaptureRef struct {
	ID       string
	Dir      string
	DataPath string
	MetaPath string
	Meta     Meta
}

// Store is the capture-object root plus its retention policy. Tests
// inject Root/Quota/now; NewStore answers the frozen production
// defaults.
type Store struct {
	Root  string
	Quota int64
	now   func() time.Time

	// inFlight tracks reserved-but-not-yet-landed object dirs,
	// refcounted against same-second id collisions: Retention's litter
	// sweep skips them, because between reserveID and writeMeta a live
	// pull's dir is meta-less and would otherwise read as litter to a
	// concurrent sweep (fix round 1's TOCTOU — one pull's finishing
	// sweep unlinked another's data.bin mid-read). In-memory by
	// design: a process death leaves no marker behind, so the orphaned
	// dir reverts to ordinary litter the next sweep collects — a
	// durable pending marker would need an age rule to tell
	// crash-litter from live, and buys nothing on a single host.
	mu       sync.Mutex
	inFlight map[string]int
}

// NewStore answers the default store: the frozen LARoot and the 512MB
// FIFO quota.
func NewStore() *Store { return &Store{Root: LARoot(), Quota: DefaultQuota} }

func (s *Store) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// validID guards the id-derived paths against traversal: an id is a
// bare directory name.
func validID(id string) error {
	if id == "" || id != filepath.Base(id) || id == "." || id == ".." || strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("la: invalid capture id %q", id)
	}
	return nil
}

func (s *Store) dir(id string) string { return filepath.Join(s.Root, id) }

// ListCaptures answers the landed objects, newest first (PulledAt
// descending, id ascending as the tiebreak). Directories without a
// loadable meta.json are litter and stay invisible to the listing —
// Retention() sweeps them.
func (s *Store) ListCaptures() ([]Meta, error) {
	ents, err := os.ReadDir(s.Root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil // nothing landed yet
		}
		return nil, err
	}
	var out []Meta
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		m, err := loadMeta(filepath.Join(s.Root, e.Name()))
		if err != nil {
			continue // litter (crashed pull): invisible to the listing
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].PulledAt.Equal(out[j].PulledAt) {
			return out[i].PulledAt.After(out[j].PulledAt)
		}
		return out[i].CapID < out[j].CapID
	})
	return out, nil
}

// Info answers one object's meta.
func (s *Store) Info(id string) (Meta, error) {
	if err := validID(id); err != nil {
		return Meta{}, err
	}
	return loadMeta(s.dir(id))
}

// Delete removes one object (the whole directory, data.bin included).
func (s *Store) Delete(id string) error {
	if err := validID(id); err != nil {
		return err
	}
	if err := os.RemoveAll(s.dir(id)); err != nil {
		return err
	}
	return nil
}

// Retention enforces the FIFO quota: meta-less directories (crashed
// pulls' litter) go first, then the oldest objects by pulled_at until
// the store sits under Quota. The newest object is never deleted — a
// single object over quota stays (and surfaces via the next call).
// The removed ids are answered in deletion order.
func (s *Store) Retention() ([]string, error) {
	ents, err := os.ReadDir(s.Root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var removed []string
	quota := s.Quota
	if quota <= 0 {
		quota = DefaultQuota
	}

	// Pass 1: litter — a dir without a loadable meta is a crashed or
	// severed pull; it sweeps regardless of age and never counts
	// against the quota math. A RESERVED dir is not litter: it is a
	// live pull inside its reserve→writeMeta window (fix round 1).
	var metas []Meta
	for _, e := range ents {
		if !e.IsDir() || s.reserved(e.Name()) {
			continue
		}
		m, err := loadMeta(filepath.Join(s.Root, e.Name()))
		if err != nil {
			if err := os.RemoveAll(filepath.Join(s.Root, e.Name())); err != nil {
				return removed, err
			}
			removed = append(removed, e.Name())
			continue
		}
		metas = append(metas, m)
	}
	if len(metas) == 0 {
		return removed, nil
	}

	// Pass 2: FIFO — oldest pulled_at first (id ascending as the
	// tiebreak), until the store fits or only the newest remains.
	sort.Slice(metas, func(i, j int) bool {
		if !metas[i].PulledAt.Equal(metas[j].PulledAt) {
			return metas[i].PulledAt.Before(metas[j].PulledAt)
		}
		return metas[i].CapID < metas[j].CapID
	})
	var total int64
	for _, m := range metas {
		total += objSize(s.dir(m.CapID))
	}
	for i, m := range metas {
		if total <= quota || i == len(metas)-1 {
			break
		}
		size := objSize(s.dir(m.CapID))
		if err := os.RemoveAll(s.dir(m.CapID)); err != nil {
			return removed, err
		}
		removed = append(removed, m.CapID)
		total -= size
	}
	return removed, nil
}

// reserveID carves the object's directory under Root: <UTC ts>-<board>
// -la-<mode>-<n>, n stepping past same-second collisions (the dir's
// exclusive create is the reservation). The claim and the create ride
// one lock hold so a concurrent Retention can never observe the dir
// meta-less-but-unclaimed; a failed create rolls the claim back
// (refcount-aware — a colliding pull's claim stays).
func (s *Store) reserveID(board, mode string) (id string, dir string, err error) {
	if err := os.MkdirAll(s.Root, 0o700); err != nil {
		return "", "", err
	}
	ts := s.clock().UTC().Format("20060102T150405")
	for n := 1; ; n++ {
		id = fmt.Sprintf("%s-%s-la-%s-%d", ts, board, mode, n)
		dir = filepath.Join(s.Root, id)
		s.mu.Lock()
		s.claim(id)
		err := os.Mkdir(dir, 0o700)
		if err != nil {
			s.unclaim(id)
			s.mu.Unlock()
			if !errors.Is(err, os.ErrExist) {
				return "", "", err
			}
			continue // same-second collision: step n past the landed object
		}
		s.mu.Unlock()
		return id, dir, nil
	}
}

// claim marks id in-flight (refcounted: concurrent same-id reserves
// share the marker until the last exits). Caller holds mu.
func (s *Store) claim(id string) {
	if s.inFlight == nil {
		s.inFlight = map[string]int{}
	}
	s.inFlight[id]++
}

// unclaim rolls one claim back (the floor is zero — an unclaim of an
// unclaimed id, e.g. a collision loser, takes nothing). Caller holds mu.
func (s *Store) unclaim(id string) {
	if s.inFlight[id] > 1 {
		s.inFlight[id]--
	} else {
		delete(s.inFlight, id)
	}
}

// unreserve releases a landed (or abandoned) pull's claim — PullCapture's
// defer; after this an unlanded dir is honest litter again.
func (s *Store) unreserve(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unclaim(id)
}

// reserved reports whether id belongs to a pull still inside the
// reserve→writeMeta window.
func (s *Store) reserved(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inFlight[id] > 0
}

// writeMeta lands meta.json inside the object dir (exclusive create —
// a meta never rewrites an existing one).
func (s *Store) writeMeta(m Meta) (string, error) {
	dir := s.dir(m.CapID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	p := filepath.Join(dir, "meta.json")
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(append(buf, '\n')); err != nil {
		f.Close()
		return "", err
	}
	return p, f.Close()
}

// loadMeta reads one object's meta.json.
func loadMeta(dir string) (Meta, error) {
	var m Meta
	buf, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(buf, &m); err != nil {
		return m, fmt.Errorf("la: %s: malformed meta.json: %v", dir, err)
	}
	return m, nil
}

// objSize sums one object directory's file sizes (data.bin + meta.json
// — the honest disk account Retention budgets against).
func objSize(dir string) int64 {
	var total int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total
}
