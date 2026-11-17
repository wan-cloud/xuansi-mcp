package la

// pull_stream_test.go — the wave C live sliding-window pulls (listen
// and edge) against the scripted board's stream face, plus the wave D
// daemon-face pins that ride the same fixtures: the kind=module trig
// mirror (module name + fire_ns, the 255 sentinels) and la.stop's
// "err" reason crossing verbatim (the pull never whitelists it).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// streamSeq pins the wave C op order: subscribe precedes arm, the
// live reads advance the cursor in ReadWindow strides, stop/info ride
// the done, and the tail + verify passes walk from the cursor (the
// recycled prefix is never re-asked for), disarm last.
func streamSeq(armArgs map[string]any, armOp string, liveReads []int64, tailFrom int64) []rec {
	seq := []rec{{"events.subscribe", map[string]any{"source": "la"}}}
	seq = append(seq, rec{armOp, armArgs})
	for _, off := range liveReads {
		seq = append(seq, rec{"la.read", map[string]any{
			"cap": int64(7), "off": off, "len": int64(ReadWindow)}})
	}
	seq = append(seq,
		rec{"la.stop", map[string]any{"cap": int64(7)}},
		rec{"la.info", map[string]any{"cap": int64(7)}},
		rec{"la.read", map[string]any{"cap": int64(7), "off": tailFrom, "len": int64(ReadWindow)}},
		rec{"la.read", map[string]any{"cap": int64(7), "off": tailFrom, "len": int64(ReadWindow)}},
		rec{"la.disarm", map[string]any{"cap": int64(7)}})
	return seq
}

func TestPullCaptureStreamHappyPath(t *testing.T) {
	ring := goldenRing() // 65587B: three live windows, the big u16-continuation chunk inside
	b := newBoard(ring)
	b.stream = true // streamGrow 0 = the whole prefix reveals on the first read
	b.stopB = goldenPayload()
	b.stopR = "limit"
	b.infoMeta = map[string]any{
		"baud": int64(115200), "auto": false, "ring_kb": int64(128),
		"idle_syms": int64(4), "max_ms": int64(5000), "chunks": int64(3),
		"chunks_dropped": int64(0), "bytes_dropped": int64(0),
		"hw_ovf": int64(0), "reason": "limit",
		"armed_ns": int64(111), "stopped_ns": int64(222),
	}
	s := testStore(t)

	ref, err := s.PullCaptureStream(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{
		Port: 1, Pin: 38, Baud: "auto", RingKB: 128, IdleSyms: 4, MaxMS: 5000,
	})
	if err != nil {
		t.Fatalf("PullCaptureStream: %v", err)
	}

	// op sequence: the live loop drains the ring in three windows
	// (0/32640/65280 — the last carries the 307B tail), then the
	// quintet's stop/info/tail/verify/disarm; the tail walks start at
	// the cursor (65587 — nothing left, one eof probe per pass).
	want := streamSeq(
		map[string]any{"port": 1, "pin": 38, "baud": "auto",
			"ring_kb": 128, "idle_syms": 4, "max_ms": 5000},
		"la.listen_arm", []int64{0, 32640, 65280}, 65587)
	got := b.snapshot()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("op sequence:\n got %v\nwant %v", got, want)
	}

	// the landed object: the whole stream, verified, honestly marked.
	if ref.Meta.StreamBytes != int64(len(ring)) || ref.Meta.Chunks != 3 ||
		ref.Meta.Bytes != goldenPayload() || ref.Meta.Reason != "limit" {
		t.Errorf("meta account = %+v", ref.Meta)
	}
	if !ref.Meta.Streamed {
		t.Error("a live pull must carry meta.streamed (the prefix's honest verification story)")
	}
	if ref.Meta.Incomplete {
		t.Error("a verified stream pull must not carry incomplete")
	}
	if raw, rerr := os.ReadFile(ref.DataPath); rerr != nil || len(raw) != len(ring) {
		t.Errorf("data.bin = %d bytes (err %v), want the whole ring's %d", len(raw), rerr, len(ring))
	}
}

// TestPullCaptureStreamFullFallback pins the reason=full account: the
// reader drained the live prefix, the ring closed the account — the
// tail read assembles the whole stream and the identities hold.
func TestPullCaptureStreamFullFallback(t *testing.T) {
	ring := goldenRing()
	b := newBoard(ring)
	b.stream = true
	b.streamCap = 40000 // the live window fills short of the stream: reason full
	b.stopB = goldenPayload()
	b.stopR = "full"
	b.infoMeta = map[string]any{
		"ring_kb": int64(128), "max_ms": int64(5000), "chunks": int64(3),
		"chunks_dropped": int64(0), "bytes_dropped": int64(0),
		"hw_ovf": int64(0), "reason": "full",
		"armed_ns": int64(111), "stopped_ns": int64(222),
	}
	s := testStore(t)

	ref, err := s.PullCaptureStream(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{
		Port: 1, Pin: 38, Baud: 115200, RingKB: 128, MaxMS: 5000,
	})
	if err != nil {
		t.Fatalf("PullCaptureStream: %v", err)
	}
	if ref.Meta.Reason != "full" {
		t.Errorf("reason = %q, want the ring-closing full account", ref.Meta.Reason)
	}
	// assembled = live prefix [0,40000) + ring tail [40000,65587):
	// the parse + stop-bytes identity hold over the WHOLE stream.
	if ref.Meta.StreamBytes != int64(len(ring)) || ref.Meta.Bytes != goldenPayload() {
		t.Errorf("meta account = %+v — the fallback must still land the whole stream", ref.Meta)
	}
	if raw, rerr := os.ReadFile(ref.DataPath); rerr != nil || len(raw) != len(ring) {
		t.Errorf("data.bin = %d bytes (err %v), want the assembled whole ring %d", len(raw), rerr, len(ring))
	}
	// the live reads advanced exactly to the cap before the done.
	reads := []int64{}
	for _, c := range b.snapshot() {
		if c.op == "la.read" {
			reads = append(reads, argU(c.args["off"]))
		}
	}
	wantReads := []int64{0, 32640, 40000, 40000} // live 0/32640, tail+verify at the cap
	if !reflect.DeepEqual(reads, wantReads) {
		t.Errorf("read offsets = %v, want %v", reads, wantReads)
	}
}

// TestPullCaptureStreamIncomplete: a link that dies mid-stream lands
// the forensics object — incomplete, whatever arrived, no sha claim.
func TestPullCaptureStreamIncomplete(t *testing.T) {
	ring := goldenRing()
	b := newBoard(ring)
	b.stream = true
	b.streamGrow = 32640 // partial reveal, then the link dies mid-loop
	b.stopB = goldenPayload()
	b.dieAt = 4          // subscribe, arm, read1, read2 → dead
	s := testStore(t)

	ref, err := s.PullCaptureStream(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{
		Port: 1, Pin: 38, Baud: 115200, RingKB: 128, MaxMS: 5000,
	})
	if err == nil || !errors.Is(err, ErrIncomplete) {
		t.Fatalf("err = %v, want ErrIncomplete", err)
	}
	if ref.ID == "" {
		t.Fatal("the incomplete object must land and be returned")
	}
	if !ref.Meta.Incomplete || ref.Meta.SHA256 != "" {
		t.Errorf("incomplete meta = %+v — no sha claim on a broken stream", ref.Meta)
	}
	if fi, ferr := os.Stat(ref.DataPath); ferr != nil || fi.Size() == 0 {
		t.Errorf("data.bin should hold the streamed prefix (stat %v %v)", fi, ferr)
	}
}

// TestPullCaptureStreamMetaCapID pins the R-D1 chain end to end: a
// stream pull's meta must carry the object's own id. An empty CapID
// lands the meta at the store ROOT (dir("")), the object dir reads as
// litter (invisible to la_list, swept by Retention), and the NEXT
// stream pull dies on the root meta.json's exclusive create. Two
// listen pulls and one edge pull over one store: every object listed,
// every meta inside its captures/la/<cap_id>/ dir, nothing in the
// store root but object directories.
func TestPullCaptureStreamMetaCapID(t *testing.T) {
	ring := goldenRing()
	s := testStore(t)

	streamBoard := func() *scriptedBoard {
		b := newBoard(ring)
		b.stream = true
		b.stopB = goldenPayload()
		b.stopR = "limit"
		b.infoMeta = map[string]any{
			"baud": int64(115200), "ring_kb": int64(128),
			"idle_syms": int64(4), "max_ms": int64(5000), "chunks": int64(3),
			"reason": "limit", "armed_ns": int64(111), "stopped_ns": int64(222),
		}
		return b
	}

	// Pull 1 — the object must be visible and self-located: the meta
	// inside captures/la/<cap_id>/, la_list answering it by that id.
	ref1, err := s.PullCaptureStream(context.Background(),
		PullBoard{ID: "aa11bb", Conn: streamBoard()}, ArmArgs{
			Port: 1, Pin: 38, Baud: 115200, RingKB: 128, MaxMS: 5000,
		})
	if err != nil {
		t.Fatalf("PullCaptureStream #1: %v", err)
	}
	if ref1.Meta.CapID != ref1.ID {
		t.Errorf("meta.cap_id = %q, want the object id %q", ref1.Meta.CapID, ref1.ID)
	}
	if got, lerr := s.ListCaptures(); lerr != nil || len(got) != 1 || got[0].CapID != ref1.ID {
		t.Errorf("la_list after pull 1 = %v (err %v) — the object must be visible as %s",
			got, lerr, ref1.ID)
	}
	if _, serr := os.Stat(filepath.Join(s.Root, ref1.ID, "meta.json")); serr != nil {
		t.Errorf("meta.json must land inside captures/la/<cap_id>/: %v", serr)
	}

	// Pull 2 — a second stream capture on the same board must land its
	// own fresh id, not die on the root meta.json's O_EXCL.
	ref2, err := s.PullCaptureStream(context.Background(),
		PullBoard{ID: "aa11bb", Conn: streamBoard()}, ArmArgs{
			Port: 1, Pin: 38, Baud: 115200, RingKB: 128, MaxMS: 5000,
		})
	if err != nil {
		t.Fatalf("PullCaptureStream #2: %v (the second stream capture must land, not die on an exclusive-create collision)", err)
	}
	if ref2.ID == ref1.ID {
		t.Errorf("pull 2 id = pull 1 id (%s) — a fresh capture needs a fresh id", ref2.ID)
	}

	// Pull 3 — the edge stream closure rides the same core; its meta
	// carries the id too.
	ering := edgeRing()
	eb := newBoard(ering)
	eb.edge = true
	eb.edgePins = []any{int64(2)}
	eb.stream = true
	eb.stopB = int64(len(ering))
	eb.stopR = "stopped"
	eb.edgeMeta = map[string]any{
		"res_hz": 80_000_000, "runs": int64(3), "runs_dropped": int64(0),
		"hw_ovf": int64(0), "ring_kb": int64(64), "max_ms": int64(4000),
		"reason": "stopped", "armed_ns": int64(111), "stopped_ns": int64(222),
		"t0_ns": int64(5_000_000_000),
	}
	ref3, err := s.PullEdgeStream(context.Background(),
		PullBoard{ID: "aa11bb", Conn: eb}, EdgeArmArgs{Pins: []int{2}, RingKB: 64, MaxMS: 4000})
	if err != nil {
		t.Fatalf("PullEdgeStream: %v", err)
	}
	if ref3.Meta.CapID != ref3.ID {
		t.Errorf("edge meta.cap_id = %q, want %q", ref3.Meta.CapID, ref3.ID)
	}

	// The store at the end: three object dirs, la_list sees all three,
	// and NOTHING else — no root meta.json garbage, no litter dirs.
	if list, lerr := s.ListCaptures(); lerr != nil || len(list) != 3 {
		t.Errorf("la_list = %d objects (err %v), want 3", len(list), lerr)
	}
	ents, rerr := os.ReadDir(s.Root)
	if rerr != nil {
		t.Fatal(rerr)
	}
	dirs := 0
	for _, e := range ents {
		if !e.IsDir() {
			t.Errorf("store root garbage: %q is not an object directory (the R-D1 root meta.json)", e.Name())
			continue
		}
		switch e.Name() {
		case ref1.ID, ref2.ID, ref3.ID:
			dirs++
		default:
			t.Errorf("unexpected dir %q under the store root", e.Name())
		}
	}
	if dirs != 3 {
		t.Errorf("store root holds %d object dirs, want 3", dirs)
	}
}

// TestPullEdgeStreamRunIdentity: the edge stream pull over the run
// form — runs×9 == total == stop bytes over the assembled stream.
func TestPullEdgeStreamRunIdentity(t *testing.T) {
	ring := edgeRing() // 27B = 3 runs
	b := newBoard(ring)
	b.edge = true
	b.edgePins = []any{int64(2)}
	b.stream = true
	b.stopB = int64(len(ring))
	b.stopR = "stopped"
	b.edgeMeta = map[string]any{
		"res_hz": 80_000_000, "trig_fired": false,
		"runs": int64(3), "runs_dropped": int64(0), "hw_ovf": int64(0),
		"ring_kb": int64(64), "max_ms": int64(4000), "reason": "stopped",
		"armed_ns": int64(111), "stopped_ns": int64(222),
		"t0_ns": int64(5_000_000_000),
	}
	s := testStore(t)

	ref, err := s.PullEdgeStream(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, EdgeArmArgs{
		Pins: []int{2}, RingKB: 64, MaxMS: 4000,
	})
	if err != nil {
		t.Fatalf("PullEdgeStream: %v", err)
	}
	if ref.Meta.Runs != 3 || ref.Meta.StreamBytes != 27 || ref.Meta.Bytes != 27 {
		t.Errorf("run identity meta = %+v", ref.Meta)
	}
	if !ref.Meta.Streamed || ref.Meta.T0NS != 5_000_000_000 {
		t.Errorf("stream mark/t0 = %v/%d", ref.Meta.Streamed, ref.Meta.T0NS)
	}
	// one live window drains the whole run stream; tail passes probe
	// at 27.
	reads := []int64{}
	for _, c := range b.snapshot() {
		if c.op == "la.read" {
			reads = append(reads, argU(c.args["off"]))
		}
	}
	if !reflect.DeepEqual(reads, []int64{0, 27, 27}) {
		t.Errorf("read offsets = %v, want the single live window + two tail probes", reads)
	}
}

// ---- wave D daemon-face pins over the same fixtures ----

// TestMirrorEdgeModuleTrig: the fw's kind=module info mirror crosses —
// module name, fire_ns (iff fired), the pin 255 sentinel — and the
// guardrail-② dual timestamp lands top-level (fire_ns + stopped_ns).
func TestMirrorEdgeModuleTrig(t *testing.T) {
	ring := edgeRing()
	b := newBoard(ring)
	b.edge = true
	b.edgePins = []any{int64(2)}
	b.stopB = int64(len(ring))
	b.stopR = "trig"
	b.pushDone = true
	b.edgeMeta = map[string]any{
		"res_hz": 80_000_000, "trig_fired": true,
		"trig": map[string]any{
			"pin": int64(255), "kind": "module", "module": "trigpat",
			"pre_kb": int64(0), "fire_ns": int64(123_456)},
		"runs": int64(3), "runs_dropped": int64(0), "hw_ovf": int64(0),
		"ring_kb": int64(64), "max_ms": int64(4000), "reason": "trig",
		"armed_ns": int64(111), "stopped_ns": int64(222),
		"t0_ns": int64(5_000_000_000),
	}
	s := testStore(t)

	ref, err := s.PullEdge(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, EdgeArmArgs{
		Pins: []int{2}, RingKB: 64, MaxMS: 4000,
		Trig: &EdgeTrig{Kind: "module", Module: "trigpat"},
	})
	if err != nil {
		t.Fatalf("PullEdge: %v", err)
	}
	tr := ref.Meta.Trig
	if tr == nil || tr.Kind != "module" || tr.Module != "trigpat" ||
		tr.Pin != 255 || tr.FireNS != 123_456 || tr.PreKB != 0 {
		t.Errorf("module trig mirror = %+v (want kind/module/pin255/fire_ns)", tr)
	}
	if ref.Meta.FireNS != 123_456 || ref.Meta.StoppedNS != 222 {
		t.Errorf("dual timestamps = fire %d / stopped %d, want 123456/222",
			ref.Meta.FireNS, ref.Meta.StoppedNS)
	}
	// the module arm wire carries NO pin (the fw's whole-window form).
	var armArgs map[string]any
	for _, c := range b.snapshot() {
		if c.op == "la.edge_arm" {
			armArgs = c.args
		}
	}
	if trig, _ := armArgs["trig"].(map[string]any); trig == nil ||
		trig["module"] != "trigpat" || trig["kind"] != "module" {
		t.Errorf("module arm trig wire = %v, want {kind:module, module:trigpat} with no pin", armArgs["trig"])
	}
}

// TestMirrorSampleModuleTrig: the sample side's kind=module mirror —
// ch 255 sentinel, module, fire_ns (present iff fired).
func TestMirrorSampleModuleTrig(t *testing.T) {
	ring := make([]byte, 64)
	b := newBoard(ring)
	b.sample = true
	b.stopB = int64(len(ring))
	b.stopR = "trig"
	b.pushDone = true
	b.rateHz = 1_000_000
	b.sampleMeta = map[string]any{
		"rate_hz": int64(1_000_000), "samples": int64(64),
		"trig_fired": true,
		"trig": map[string]any{
			"ch": int64(255), "kind": "module", "module": "trigpat",
			"pre_kb": int64(0), "fire_ns": int64(987)},
		"hw_ovf": int64(0), "ring_kb": int64(64), "max_ms": int64(4000),
		"reason": "trig", "armed_ns": int64(111), "stopped_ns": int64(222),
		"t0_ns": int64(5_000_000_000),
	}
	s := testStore(t)

	ref, err := s.PullSample(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, SampleArmArgs{
		RateHz: 1_000_000, RingKB: 64, MaxMS: 4000,
		Trig: &SampleTrig{Kind: "module", Module: "trigpat"},
	})
	if err != nil {
		t.Fatalf("PullSample: %v", err)
	}
	tr := ref.Meta.Trig
	if tr == nil || tr.Kind != "module" || tr.Module != "trigpat" ||
		tr.Ch != 255 || tr.FireNS != 987 {
		t.Errorf("sample module trig mirror = %+v (want ch255/module/fire_ns)", tr)
	}
	if ref.Meta.FireNS != 987 {
		t.Errorf("top-level fire_ns = %d, want 987", ref.Meta.FireNS)
	}
	// unfired: fire_ns absent from BOTH the mirror and the top level.
	b2 := newBoard(ring)
	b2.sample = true
	b2.stopB = int64(len(ring))
	b2.stopR = "stopped"
	b2.pushDone = true
	b2.sampleMeta = map[string]any{
		"trig_fired": false,
		"trig": map[string]any{
			"ch": int64(255), "kind": "module", "module": "trigpat", "pre_kb": int64(0)},
	}
	ref2, err := s.PullSample(context.Background(), PullBoard{ID: "97c1ac", Conn: b2}, SampleArmArgs{
		RateHz: 1_000_000, RingKB: 64, MaxMS: 4000,
		Trig: &SampleTrig{Kind: "module", Module: "trigpat"},
	})
	if err != nil {
		t.Fatalf("PullSample (unfired): %v", err)
	}
	if tr2 := ref2.Meta.Trig; tr2 == nil || tr2.FireNS != 0 {
		t.Errorf("unfired module trig mirror = %+v, want fire_ns absent (0)", tr2)
	}
	if ref2.Meta.FireNS != 0 {
		t.Errorf("unfired top-level fire_ns = %d, want absent", ref2.Meta.FireNS)
	}
}

// TestPullEdgeReasonErrPassthrough: kind=module captures can end
// reason "err" (the wasm failure) — the pull crosses it verbatim, no
// whitelist refuses the landing of an honestly-ended capture.
func TestPullEdgeReasonErrPassthrough(t *testing.T) {
	ring := edgeRing()
	b := newBoard(ring)
	b.edge = true
	b.edgePins = []any{int64(2)}
	b.stopB = int64(len(ring))
	b.stopR = "err" // the module's honest ending
	b.pushDone = true
	b.edgeMeta = map[string]any{
		"trig": map[string]any{
			"pin": int64(255), "kind": "module", "module": "trigpat", "pre_kb": int64(0)},
		"trig_fired": false, "reason": "err",
		"runs": int64(3), "t0_ns": int64(5_000_000_000),
	}
	s := testStore(t)

	ref, err := s.PullEdge(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, EdgeArmArgs{
		Pins: []int{2}, RingKB: 64, MaxMS: 4000,
		Trig: &EdgeTrig{Kind: "module", Module: "trigpat"},
	})
	if err != nil {
		t.Fatalf("PullEdge: %v (an err-ended capture still lands verified)", err)
	}
	if ref.Meta.Reason != "err" {
		t.Errorf("reason = %q, want the verbatim err", ref.Meta.Reason)
	}
	if ref.Meta.Incomplete || ref.Meta.SHA256 == "" {
		t.Errorf("an err-ended capture is still a verified capture: %+v", ref.Meta)
	}
}
