package la

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"xuansi/internal/paths"
)

var fixedNow = time.Date(2026, 11, 15, 10, 15, 30, 0, time.UTC)

func testStore(t *testing.T) *Store {
	t.Helper()
	return &Store{Root: t.TempDir(), Quota: DefaultQuota, now: func() time.Time { return fixedNow }}
}

// land writes one synthetic object: data.bin of size bytes plus its
// meta (pulled_at backing the FIFO order).
func land(t *testing.T, s *Store, id string, size int, pulled time.Time) {
	t.Helper()
	dir := filepath.Join(s.Root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.bin"), make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	m := Meta{CapID: id, Mode: "listen", PulledAt: pulled, StreamBytes: int64(size), Bytes: int64(size)}
	p, err := s.writeMeta(m)
	if err != nil {
		t.Fatal(err)
	}
	if p != filepath.Join(dir, "meta.json") {
		t.Fatalf("writeMeta path = %q", p)
	}
}

func TestLARootFrozen(t *testing.T) {
	want := filepath.Join(paths.DataDir(), "captures", "la")
	if LARoot() != want {
		t.Fatalf("LARoot() = %q, want %q (DataDir()/captures/la — the wave's frozen path)", LARoot(), want)
	}
	if ns := NewStore(); ns.Root != want || ns.Quota != DefaultQuota {
		t.Fatalf("NewStore = %+v", ns)
	}
}

func TestStoreMetaRoundTrip(t *testing.T) {
	s := testStore(t)
	in := Meta{
		CapID: "rt-1", Board: "aa11bb", Mode: "listen", Port: 1, Pin: 38,
		Baud: 115200, IdleSyms: 4,
		TStartNS: 111, TEndNS: 222, Bytes: 42, Chunks: 2, Reason: "limit",
		RingKB: 128, MaxMS: 5000, StreamBytes: 62,
		BytesDropped: 7, ChunksDropped: 1, HwOvf: 2, ArmedNS: 1000, StoppedNS: 2000,
		SHA256: "deadbeef", PulledAt: fixedNow,
	}
	if _, err := s.writeMeta(in); err != nil {
		t.Fatal(err)
	}
	got, err := s.Info("rt-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, in)
	}
}

func TestStoreListInfoDelete(t *testing.T) {
	s := testStore(t)
	t1 := fixedNow
	t2 := fixedNow.Add(time.Minute)
	t3 := fixedNow.Add(2 * time.Minute)
	land(t, s, "c-old", 10, t1)
	land(t, s, "c-new", 10, t3)
	land(t, s, "c-mid", 10, t2)

	ms, err := s.ListCaptures()
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{"c-new", "c-mid", "c-old"} // newest first
	if len(ms) != 3 {
		t.Fatalf("ListCaptures len = %d", len(ms))
	}
	for i, w := range wantOrder {
		if ms[i].CapID != w {
			t.Errorf("ListCaptures[%d] = %s, want %s", i, ms[i].CapID, w)
		}
	}

	// litter: a crashed pull's dir without meta stays invisible
	if err := os.MkdirAll(filepath.Join(s.Root, "litter"), 0o700); err != nil {
		t.Fatal(err)
	}
	if ms, _ = s.ListCaptures(); len(ms) != 3 {
		t.Fatalf("litter leaked into the listing: %d", len(ms))
	}

	if _, err := s.Info("nope"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Info(nope) err = %v, want ErrNotExist", err)
	}
	if err := s.Delete("c-mid"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Info("c-mid"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Info after Delete err = %v, want ErrNotExist", err)
	}
	if _, err := s.Info("../escape"); err == nil {
		t.Fatal("Info traversal id accepted")
	}
	if err := s.Delete("../escape"); err == nil {
		t.Fatal("Delete traversal id accepted")
	}
}

func TestStoreRetentionFIFO(t *testing.T) {
	s := testStore(t)
	land(t, s, "r1", 40, fixedNow)
	land(t, s, "r2", 40, fixedNow.Add(time.Minute))
	land(t, s, "r3", 40, fixedNow.Add(2*time.Minute))
	// quota sized against the real object sizes (data.bin + meta.json):
	// exactly the two newest fit — the oldest must go.
	s.Quota = objSize(s.dir("r2")) + objSize(s.dir("r3"))
	// litter goes first, regardless of age
	if err := os.MkdirAll(filepath.Join(s.Root, "litter"), 0o700); err != nil {
		t.Fatal(err)
	}

	removed, err := s.Retention()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"litter", "r1"}
	if len(removed) != len(want) {
		t.Fatalf("removed = %v, want %v", removed, want)
	}
	for i := range want {
		if removed[i] != want[i] {
			t.Fatalf("removed = %v, want %v", removed, want)
		}
	}
	if _, err := s.Info("r1"); !errors.Is(err, os.ErrNotExist) {
		t.Error("r1 survived retention")
	}
	for _, keep := range []string{"r2", "r3"} {
		if _, err := s.Info(keep); err != nil {
			t.Errorf("%s evicted: %v", keep, err)
		}
	}
}

func TestStoreRetentionNeverDeletesNewestAlone(t *testing.T) {
	s := testStore(t)
	s.Quota = 10
	land(t, s, "only", 40, fixedNow) // alone over quota — stays
	removed, err := s.Retention()
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none (the newest is never deleted)", removed)
	}
	if _, err := s.Info("only"); err != nil {
		t.Fatal("the lone over-quota object was deleted")
	}
}
