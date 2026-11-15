package la

// pull_edge_test.go — the edge pull (T11) against the scripted board:
// the op sequence + verbatim arm args, the edge meta mirror (pins,
// trig, t0_ns), the run-form identity (runs×9 == total == stop
// bytes), and the broken-pull forensics landing.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"xuansi/internal/replclient"
)

func edgeRing() []byte {
	return encRuns(srun{1, 200_000}, srun{0, 13_000}, srun{1, 4_513_000})
}

func TestPullEdgeHappyPath(t *testing.T) {
	ring := edgeRing()
	sum := sha256.Sum256(ring)
	b := newBoard(ring)
	b.edge = true
	b.edgePins = []any{int64(2), int64(39)}
	b.stopB = int64(len(ring))
	b.stopR = "trig"
	b.pushDone = true
	b.edgeMeta = map[string]any{
		"res_hz": 80_000_000, "trig": map[string]any{
			"pin": int64(2), "kind": "edge_r", "pre_kb": int64(1)},
		"trig_fired": true, "runs": int64(3), "runs_dropped": int64(0),
		"hw_ovf": int64(0), "ring_kb": int64(64), "max_ms": int64(4000),
		"reason": "trig", "armed_ns": int64(111), "stopped_ns": int64(222),
		"t0_ns": int64(5_000_000_000),
	}
	s := testStore(t)

	ref, err := s.PullEdge(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, EdgeArmArgs{
		Pins: []int{2, 39}, RingKB: 64, MaxMS: 4000,
		Trig: &EdgeTrig{Pin: 2, Kind: "edge_r", PreKB: 1},
	})
	if err != nil {
		t.Fatalf("PullEdge: %v", err)
	}

	// the op sequence: subscribe precedes arm, stop/info after the
	// done, both read passes, disarm last; the arm args cross
	// verbatim (res_hz always the pinned 80M).
	wantSeq := []rec{
		{"events.subscribe", map[string]any{"source": "la"}},
		{"la.edge_arm", map[string]any{
			"pins": []any{int64(2), int64(39)}, "res_hz": int64(80_000_000),
			"ring_kb": int64(64), "max_ms": int64(4000),
			"trig": map[string]any{"pin": int64(2), "kind": "edge_r", "pre_kb": int64(1)}}},
		{"la.stop", map[string]any{"cap": int64(7)}},
		{"la.info", map[string]any{"cap": int64(7)}},
		{"la.read", map[string]any{"cap": int64(7), "off": int64(0), "len": int64(ReadWindow)}},
		{"la.read", map[string]any{"cap": int64(7), "off": int64(0), "len": int64(ReadWindow)}},
		{"la.disarm", map[string]any{"cap": int64(7)}},
	}
	got := b.snapshot()
	if len(got) != len(wantSeq) {
		t.Fatalf("op sequence:\n got %+v\nwant %+v", got, wantSeq)
	}
	for i, w := range wantSeq {
		if got[i].op != w.op || !reflect.DeepEqual(got[i].args, w.args) {
			t.Errorf("op %d:\n got %s %v\nwant %s %v", i, got[i].op, got[i].args, w.op, w.args)
		}
	}

	// landed object: data.bin byte-identical + the edge meta columns
	disk, err := os.ReadFile(ref.DataPath)
	if err != nil || string(disk) != string(ring) {
		t.Fatalf("data.bin = %d bytes err=%v, want the ring verbatim", len(disk), err)
	}
	if ref.ID != "20261115T101530-97c1ac-la-edge-1" {
		t.Errorf("ID = %q (the edge mode suffix)", ref.ID)
	}
	span := uint64(200_000 + 13_000 + 4_513_000)
	wantMeta := Meta{
		CapID: ref.ID, Board: "97c1ac", Mode: "edge",
		Pins: []int{2, 39}, ResHz: 80_000_000,
		TStartNS: 5_000_000_000, TEndNS: 5_000_000_000 + span,
		Bytes: int64(len(ring)), Runs: 3, Reason: "trig",
		Trig:        &TrigMeta{Pin: 2, Kind: "edge_r", PreKB: 1},
		TrigFired:   true,
		T0NS:        5_000_000_000,
		RunsDropped: 0,
		RingKB:      64, MaxMS: 4000,
		StreamBytes: int64(len(ring)),
		ArmedNS:     111, StoppedNS: 222,
		SHA256:   hex.EncodeToString(sum[:]),
		PulledAt: fixedNow,
	}
	if !reflect.DeepEqual(ref.Meta, wantMeta) {
		t.Fatalf("ref.Meta:\n got %+v\nwant %+v", ref.Meta, wantMeta)
	}

	// the landed object answers the analysis seats (the whole chain:
	// pull → read → analyze)
	runs, err := Runs(ref, 0, 0, 0)
	if err != nil || runs.NRuns != 3 || runs.T0NS != 5_000_000_000 {
		t.Fatalf("Runs on the landed object = %+v err=%v", runs, err)
	}
}

// The identity assertion: a stop ledger that disagrees with the
// parsed runs is ErrChecksum — the object lands as forensics.
func TestPullEdgeStopIdentity(t *testing.T) {
	ring := edgeRing()
	b := newBoard(ring)
	b.edge = true
	b.edgePins = []any{int64(2)}
	b.stopB = int64(len(ring)) - 9 // one run short
	b.stopR = "full"
	b.pushDone = true
	b.edgeMeta = map[string]any{"t0_ns": int64(5), "ring_kb": int64(64)}
	s := testStore(t)

	ref, err := s.PullEdge(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, EdgeArmArgs{Pins: []int{2}, RingKB: 64})
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
	if ref.ID == "" || !ref.Meta.Incomplete {
		t.Fatalf("the broken pull must land the incomplete object, got %+v", ref.Meta)
	}
	if ref.Meta.Runs != 3 { // the parse account rode the forensics landing
		t.Errorf("forensics runs = %d, want 3", ref.Meta.Runs)
	}
}

// An arm refusal crosses verbatim with nothing landed; a mid-read
// death lands the incomplete object with the tolerant run account.
func TestPullEdgeRefusalAndIncomplete(t *testing.T) {
	b := newBoard(nil)
	b.edge = true
	b.armErr = &replclient.OpError{Op: "la.edge_arm", Class: "api",
		Msg: "la.edge_arm: pin 45 is banned (XS_PIN_BANNED: strapping/USB/flash/PSRAM bus)"}
	s := testStore(t)
	ref, err := s.PullEdge(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, EdgeArmArgs{Pins: []int{45}, RingKB: 64})
	if err == nil || ref.ID != "" {
		t.Fatalf("arm refusal: ref=%+v err=%v, want nothing landed", ref, err)
	}
	if got := err.Error(); !strings.Contains(got, "banned") {
		t.Errorf("refusal text = %q, want the fw coach verbatim", got)
	}

	// a landed torn tail: pass 1 delivers the ring (its own bytes are
	// the board's truth), but the stream does not parse whole — the
	// checksum identity fails and the object lands as forensics with
	// the tolerant run account.
	stream := append(edgeRing(), encRun(1, 5)[:4]...)
	b2 := newBoard(stream)
	b2.edge = true
	b2.edgePins = []any{int64(2)}
	b2.stopB = int64(len(stream))
	b2.pushDone = true
	b2.edgeMeta = map[string]any{"t0_ns": int64(9), "ring_kb": int64(64)}
	ref2, err := s.PullEdge(context.Background(), PullBoard{ID: "97c1ac", Conn: b2}, EdgeArmArgs{Pins: []int{2}, RingKB: 64})
	if !errors.Is(err, ErrChecksum) || ref2.ID == "" || !ref2.Meta.Incomplete {
		t.Fatalf("torn landing: err=%v ref=%+v, want ErrChecksum forensics", err, ref2.Meta)
	}
	st, perr := parseRunStats(ref2.DataPath, true)
	if perr != nil || st.Runs != 3 || st.Stream != 27 {
		t.Errorf("tolerant account = %+v err=%v, want the 3 clean runs", st, perr)
	}

	// a mid-read link death: ErrIncomplete, same forensics shape
	b3 := newBoard(edgeRing())
	b3.edge = true
	b3.edgePins = []any{int64(2)}
	b3.stopB = int64(len(edgeRing()))
	b3.pushDone = true
	b3.edgeMeta = map[string]any{"t0_ns": int64(9), "ring_kb": int64(64)}
	b3.dieAt = 5 // the first la.read dies
	ref3, err := s.PullEdge(context.Background(), PullBoard{ID: "97c1ac", Conn: b3}, EdgeArmArgs{Pins: []int{2}, RingKB: 64})
	if !errors.Is(err, ErrIncomplete) || ref3.ID == "" || !ref3.Meta.Incomplete {
		t.Fatalf("mid-read death: err=%v ref=%+v, want incomplete forensics", err, ref3.Meta)
	}
}

// landIncomplete's edge branch fills the absolute axis only when the
// t0 ledger landed (the axis needs it).
func TestLandIncompleteEdgeAxis(t *testing.T) {
	s := testStore(t)
	id, dir, err := s.reserveID("97c1ac", "edge")
	if err != nil {
		t.Fatal(err)
	}
	defer s.unreserve(id)
	p := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(p, edgeRing(), 0o600); err != nil {
		t.Fatal(err)
	}
	meta := Meta{CapID: id, Board: "97c1ac", Mode: "edge", T0NS: 7_000}
	ref, err := s.landIncomplete(CaptureRef{ID: id, DataPath: p, Meta: meta}, meta, newBoard(nil), 1, ErrIncomplete)
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("err = %v", err)
	}
	if ref.Meta.TStartNS != 7_000 || ref.Meta.TEndNS != 7_000+4_726_000 || ref.Meta.Runs != 3 {
		t.Errorf("edge axis = %+v, want t0-absolute span", ref.Meta)
	}
}
