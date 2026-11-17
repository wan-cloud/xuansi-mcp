package la

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/replclient"
)

// ---------------------------------------------------------------------------
// scriptedBoard: a fake Board scripting the v9 la.* quintet. It serves
// la.read windows out of a scripted ring, pushes the "la" done event
// at arm time (or never), and can die mid-pull or flip verify-pass
// bytes — the shapes the pull's red cases pin.

type rec struct {
	op   string
	args map[string]any
}

type evReg struct {
	prefix string
	fn     func(replclient.Event)
}

type scriptedBoard struct {
	mu       sync.Mutex
	calls    []rec
	regs     map[int]evReg
	nextHID  int
	ring     []byte         // the stream la.read serves
	capID    int64          // arm-assigned cap number
	baud     int64          // arm reply's resolved baud
	stopB    int64          // stop reply's bytes (payload ledger)
	stopR    string         // stop reply's reason
	stopGate chan struct{}  // non-nil: la.stop signals stopHere then blocks until closed
	stopHere chan struct{}  // closed once la.stop is parked at the gate
	infoMeta map[string]any // la.info's 12-column meta (nil = none)
	armErr   error          // scripted arm refusal — crosses verbatim
	dieAt    int            // Op calls ≥ dieAt (after recording) answer ErrDead (−1 = never)
	flipAt   int            // la.read calls > flipAt serve a flipped byte (−1 = never)
	ops      int            // total Op calls taken
	pushDone bool           // push the done event inside the arm reply
	pushJunk bool           // push a foreign-cap done + a progress first

	// the edge face (T11): edge=true serves la.edge_arm/{cap,res_hz}
	// and la.info's edge shape (edgeMeta + top-level pins).
	edge     bool
	edgeMeta map[string]any
	edgePins []any

	// the samples face (T13): sample=true serves la.sample_arm/
	// {cap,rate_hz} and la.info's sample shape (sampleMeta).
	sample     bool
	sampleMeta map[string]any
	rateHz     int64

	// the wave C stream face: stream=true flips la.read into LIVE
	// serving — the published prefix grows to liveCap (streamGrow per
	// read; 0 = reveal the whole liveCap on the first read), and the
	// first caught-up read AT the cap pushes the done event and freezes
	// the capture (post-stop reads then serve [off, len(ring)) with
	// total=len(ring) — the ring closing the account, reason=full's
	// shape). liveCap==len(ring) models a self-ended capture the live
	// loop drains completely.
	stream    bool
	streamCap int
	streamGrow int
	liveRevealed int
	liveEnded bool
}

func newBoard(ring []byte) *scriptedBoard {
	return &scriptedBoard{
		regs: map[int]evReg{}, ring: ring, capID: 7, baud: 115200,
		stopB: 0, stopR: "stopped", dieAt: -1, flipAt: -1, rateHz: 1_000_000,
	}
}

func (b *scriptedBoard) snapshot() []rec {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]rec, len(b.calls))
	copy(out, b.calls)
	return out
}

func (b *scriptedBoard) opNames() []string {
	var out []string
	for _, c := range b.snapshot() {
		out = append(out, c.op)
	}
	return out
}

func (b *scriptedBoard) push(source string, data map[string]any) {
	b.mu.Lock()
	regs := make([]evReg, 0, len(b.regs))
	for _, r := range b.regs {
		regs = append(regs, r)
	}
	b.mu.Unlock()
	for _, r := range regs {
		if strings.HasPrefix(source, r.prefix) {
			r.fn(replclient.Event{Source: source, Data: data})
		}
	}
}

func argU(v any) int64 {
	switch v := v.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	}
	return -1
}

func (b *scriptedBoard) Op(ctx context.Context, op string, args map[string]any) (map[string]any, error) {
	b.mu.Lock()
	b.ops++
	n := b.ops
	b.calls = append(b.calls, rec{op: op, args: args})
	die := b.dieAt >= 0 && n >= b.dieAt
	b.mu.Unlock()
	if die {
		return nil, fmt.Errorf("%w: link gone mid-pull", replclient.ErrDead)
	}
	switch op {
	case "events.subscribe":
		return map[string]any{"ok": true}, nil
	case "la.listen_arm":
		if b.armErr != nil {
			return nil, b.armErr
		}
		if b.pushJunk {
			b.push("la", map[string]any{"event": "progress", "cap": b.capID, "bytes": int64(1024)})
			b.push("la", map[string]any{"event": "done", "cap": int64(99), "bytes": int64(1), "reason": "full"})
		}
		if b.pushDone {
			b.push("la", map[string]any{"event": "done", "cap": b.capID, "bytes": b.stopB, "reason": b.stopR})
		}
		return map[string]any{"cap": b.capID, "baud": b.baud}, nil
	case "la.edge_arm":
		if b.armErr != nil {
			return nil, b.armErr
		}
		if b.pushDone {
			b.push("la", map[string]any{"event": "done", "cap": b.capID, "bytes": b.stopB, "reason": b.stopR})
		}
		return map[string]any{"cap": b.capID, "res_hz": int64(80_000_000)}, nil
	case "la.sample_arm":
		if b.armErr != nil {
			return nil, b.armErr
		}
		if b.pushDone {
			b.push("la", map[string]any{"event": "done", "cap": b.capID, "bytes": b.stopB, "reason": b.stopR})
		}
		return map[string]any{"cap": b.capID, "rate_hz": b.rateHz}, nil
	case "la.stop":
		if b.stopGate != nil {
			close(b.stopHere)
			<-b.stopGate
		}
		return map[string]any{"bytes": b.stopB, "reason": b.stopR}, nil
	case "la.info":
		if b.sample {
			meta := b.sampleMeta
			if meta == nil {
				meta = map[string]any{}
			}
			return map[string]any{"caps": []any{map[string]any{
				"cap": b.capID, "mode": "sample", "state": "stopped",
				"bytes": b.stopB, "total": int64(len(b.ring)),
				"meta": meta,
			}}}, nil
		}
		if b.edge {
			meta := b.edgeMeta
			if meta == nil {
				meta = map[string]any{}
			}
			row := map[string]any{
				"cap": b.capID, "mode": "edge", "state": "stopped",
				"bytes": b.stopB, "total": int64(len(b.ring)),
				"meta": meta,
			}
			if b.edgePins != nil {
				row["pins"] = b.edgePins
			}
			return map[string]any{"caps": []any{row}}, nil
		}
		meta := b.infoMeta
		if meta == nil {
			meta = map[string]any{}
		}
		return map[string]any{"caps": []any{map[string]any{
			"cap": b.capID, "mode": "listen", "port": int64(1), "pin": int64(38),
			"state": "stopped", "bytes": b.stopB, "total": int64(len(b.ring)),
			"meta": meta,
		}}}, nil
	case "la.read":
		b.mu.Lock()
		flip := b.flipAt >= 0 && n > b.flipAt
		live := b.stream && !b.liveEnded
		b.mu.Unlock()
		if live {
			// wave C live serving: the published prefix reveals toward
			// streamCap (streamGrow per read; 0 = the whole cap at
			// once). The first caught-up read AT the cap ends the
			// capture — done pushes and serving freezes to the
			// post-stop shape (total = len(ring): streamCap < len(ring)
			// models reason=full with the ring closing the account;
			// streamCap == len(ring) a capture the live loop drains
			// whole).
			b.mu.Lock()
			cap := b.streamCap
			if cap == 0 {
				cap = len(b.ring)
			}
			if b.streamGrow > 0 {
				if b.liveRevealed < cap {
					b.liveRevealed += b.streamGrow
					if b.liveRevealed > cap {
						b.liveRevealed = cap
					}
				}
			} else {
				b.liveRevealed = cap
			}
			revealed := b.liveRevealed
			b.mu.Unlock()
			off := argU(args["off"])
			ln := argU(args["len"])
			if off < 0 || ln < 0 || off > int64(revealed) {
				return nil, &replclient.OpError{Op: op, Class: "api",
					Msg: "la.read: off outside live window"}
			}
			end := off + ln
			if end > int64(revealed) {
				end = int64(revealed)
			}
			eof := end >= int64(revealed)
			if eof && revealed == cap {
				b.mu.Lock()
				b.liveEnded = true
				b.mu.Unlock()
				b.push("la", map[string]any{"event": "done", "cap": b.capID,
					"bytes": b.stopB, "reason": b.stopR})
			}
			data := append([]byte(nil), b.ring[off:end]...)
			return map[string]any{"data": data, "total": int64(revealed),
				"eof": eof}, nil
		}
		off := argU(args["off"])
		ln := argU(args["len"])
		if off < 0 || ln < 0 || off > int64(len(b.ring)) {
			return nil, &replclient.OpError{Op: op, Class: "api", Msg: "la.read: bad window"}
		}
		end := off + ln
		if end > int64(len(b.ring)) {
			end = int64(len(b.ring))
		}
		data := append([]byte(nil), b.ring[off:end]...)
		if flip && len(data) > 0 {
			data[7] ^= 0xFF // transient corruption: pass-2 bytes differ
		}
		return map[string]any{"data": data, "total": int64(len(b.ring)), "eof": end >= int64(len(b.ring))}, nil
	case "la.disarm":
		return map[string]any{"cap": b.capID}, nil
	}
	return nil, &replclient.OpError{Op: op, Class: "api", Msg: "unknown op " + op}
}

func (b *scriptedBoard) OnEvent(sourcePrefix string, fn func(replclient.Event)) (cancel func()) {
	b.mu.Lock()
	id := b.nextHID
	b.nextHID++
	b.regs[id] = evReg{prefix: sourcePrefix, fn: fn}
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		delete(b.regs, id)
		b.mu.Unlock()
	}
}

func (b *scriptedBoard) Dead() bool { return false }

var _ Board = (*scriptedBoard)(nil)

// ---------------------------------------------------------------------------
// golden ring: 3 chunks crossing the read window (stream 65587 >
// 2×32640 → three windows), the big one at the u16 ceiling — the
// continuation shape the fw's fix1 lands.

var (
	ringA   = []byte("hello-listen")
	ringBig = make([]byte, 65535)
	ringC   = []byte("tail-bytes")
)

func goldenRing() []byte {
	for i := range ringBig {
		ringBig[i] = byte('A' + i%26)
	}
	s := append(encChunk(1_000_000, ringA), encChunk(2_000_000, ringBig)...)
	return append(s, encChunk(3_000_000, ringC)...)
}

func goldenPayload() int64 { return int64(len(ringA) + len(ringBig) + len(ringC)) }

func TestPullCaptureHappyPath(t *testing.T) {
	ring := goldenRing()
	sum := sha256.Sum256(ring)
	b := newBoard(ring)
	b.stopB = goldenPayload()
	b.stopR = "limit"
	b.pushDone = true
	b.pushJunk = true // foreign-cap done + progress must not end the wait
	b.infoMeta = map[string]any{
		"baud": int64(115200), "auto": false, "ring_kb": int64(128), "idle_syms": int64(4),
		"max_ms": int64(5000), "chunks": int64(3), "chunks_dropped": int64(0),
		"bytes_dropped": int64(0), "hw_ovf": int64(0), "reason": "limit",
		"armed_ns": int64(111), "stopped_ns": int64(222),
	}
	s := testStore(t)

	ref, err := s.PullCapture(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{
		Port: 1, Pin: 38, Baud: "auto", RingKB: 128, IdleSyms: 4, MaxMS: 5000,
	})
	if err != nil {
		t.Fatalf("PullCapture: %v", err)
	}

	// op sequence + verbatim arm args: subscribe precedes arm (miss-no-
	// done discipline), stop/info after the done, read windows in both
	// passes, disarm last.
	wantSeq := []rec{
		{"events.subscribe", map[string]any{"source": "la"}},
		{"la.listen_arm", map[string]any{"port": 1, "pin": 38, "baud": "auto", "ring_kb": 128, "idle_syms": 4, "max_ms": 5000}},
		{"la.stop", map[string]any{"cap": int64(7)}},
		{"la.info", map[string]any{"cap": int64(7)}},
		{"la.read", map[string]any{"cap": int64(7), "off": int64(0), "len": int64(ReadWindow)}},
		{"la.read", map[string]any{"cap": int64(7), "off": int64(ReadWindow), "len": int64(ReadWindow)}},
		{"la.read", map[string]any{"cap": int64(7), "off": int64(2 * ReadWindow), "len": int64(ReadWindow)}},
		{"la.read", map[string]any{"cap": int64(7), "off": int64(0), "len": int64(ReadWindow)}},
		{"la.read", map[string]any{"cap": int64(7), "off": int64(ReadWindow), "len": int64(ReadWindow)}},
		{"la.read", map[string]any{"cap": int64(7), "off": int64(2 * ReadWindow), "len": int64(ReadWindow)}},
		{"la.disarm", map[string]any{"cap": int64(7)}},
	}
	got := b.snapshot()
	if !reflect.DeepEqual(got, wantSeq) {
		t.Fatalf("op sequence:\n got %+v\nwant %+v", got, wantSeq)
	}

	// landed object: data.bin byte-identical to the ring
	if ref.ID != "20261115T101530-aa11bb-la-listen-1" {
		t.Errorf("ID = %q", ref.ID)
	}
	if ref.Dir != filepath.Join(s.Root, ref.ID) || ref.DataPath != filepath.Join(ref.Dir, "data.bin") ||
		ref.MetaPath != filepath.Join(ref.Dir, "meta.json") {
		t.Errorf("ref paths = %+v", ref)
	}
	disk, err := os.ReadFile(ref.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disk, ring) {
		t.Fatalf("data.bin ≠ ring: %d vs %d bytes", len(disk), len(ring))
	}

	// meta.json: the full column account
	wantMeta := Meta{
		CapID: ref.ID, Board: "aa11bb", Mode: "listen", Port: 1, Pin: 38,
		Baud: 115200, IdleSyms: 4,
		TStartNS: 1_000_000, TEndNS: 3_000_000,
		Bytes: goldenPayload(), Chunks: 3, Reason: "limit",
		RingKB: 128, MaxMS: 5000,
		StreamBytes: int64(len(ring)),
		ArmedNS:     111, StoppedNS: 222,
		SHA256:   hex.EncodeToString(sum[:]),
		PulledAt: fixedNow,
	}
	if !reflect.DeepEqual(ref.Meta, wantMeta) {
		t.Fatalf("ref.Meta:\n got %+v\nwant %+v", ref.Meta, wantMeta)
	}
	loaded, err := s.Info(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, wantMeta) {
		t.Fatalf("meta.json round trip:\n got %+v\nwant %+v", loaded, wantMeta)
	}
}

func TestPullCaptureArmRefusalPassthrough(t *testing.T) {
	coach := "la.listen_arm: ring budget 4096KB exceeded: active 3968KB + request 512KB (caps la_ring_budget_kb) — stop or disarm an armed capture first, or shrink ring_kb"
	b := newBoard(nil)
	b.armErr = &replclient.OpError{Op: "la.listen_arm", Class: "api", Msg: coach}
	s := testStore(t)

	ref, err := s.PullCapture(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{Port: 1, Pin: 38, Baud: 9600, RingKB: 512})
	if err == nil {
		t.Fatal("arm refusal: no error")
	}
	var oe *replclient.OpError
	if !errors.As(err, &oe) || oe.Msg != coach || oe.Class != "api" {
		t.Fatalf("refusal not verbatim: %v", err)
	}
	if ref.ID != "" {
		t.Errorf("refusal left a capture object: %q", ref.ID)
	}
	// nothing beyond subscribe+arm touched the board
	if names := b.opNames(); !reflect.DeepEqual(names, []string{"events.subscribe", "la.listen_arm"}) {
		t.Errorf("ops after refusal = %v", names)
	}
	ents, _ := os.ReadDir(s.Root)
	if len(ents) != 0 {
		t.Errorf("refusal left %d entries under the store root", len(ents))
	}
}

func TestPullCaptureMidPullDisconnectLandsIncomplete(t *testing.T) {
	ring := goldenRing()
	b := newBoard(ring)
	b.stopB = goldenPayload()
	b.stopR = "full"
	b.pushDone = true
	b.dieAt = 6 // ops 1-5 (subscribe/arm/stop/info/read w1) live; read w2 dies
	s := testStore(t)

	ref, err := s.PullCapture(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{Port: 1, Pin: 38, Baud: 115200, RingKB: 128})
	if err == nil {
		t.Fatal("mid-pull death: no error")
	}
	if !errors.Is(err, ErrIncomplete) || !errors.Is(err, replclient.ErrDead) {
		t.Fatalf("err = %v, want ErrIncomplete wrapping ErrDead", err)
	}
	if ref.ID == "" {
		t.Fatal("no partial object landed")
	}
	m, err := s.Info(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Incomplete {
		t.Error("meta.incomplete not set")
	}
	// the partial stream: window 1's 32640 bytes landed, tolerant parse
	// counted only the clean chunks inside it (ringA whole; ringBig torn)
	if m.StreamBytes != ReadWindow {
		t.Errorf("stream_bytes = %d, want %d", m.StreamBytes, ReadWindow)
	}
	if m.Chunks != 1 || m.Bytes != int64(len(ringA)) || m.TStartNS != 1_000_000 || m.TEndNS != 1_000_000 {
		t.Errorf("partial account = %+v", m)
	}
	disk, err := os.ReadFile(ref.DataPath)
	if err != nil || len(disk) != ReadWindow {
		t.Fatalf("data.bin = %d bytes (err %v), want %d", len(disk), err, ReadWindow)
	}
	if !reflect.DeepEqual(disk, ring[:ReadWindow]) {
		t.Error("partial data.bin ≠ ring prefix")
	}
}

func TestPullCaptureChecksumMismatchIsError(t *testing.T) {
	ring := goldenRing()
	b := newBoard(ring)
	b.stopB = goldenPayload()
	b.stopR = "limit"
	b.pushDone = true
	b.flipAt = 7 // pass 2's windows (calls 8-10) serve flipped bytes
	s := testStore(t)

	ref, perr := s.PullCapture(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{Port: 1, Pin: 38, Baud: 115200, RingKB: 128})
	if !errors.Is(perr, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", perr)
	}
	if ref.ID == "" {
		t.Fatal("tainted object missing")
	}
	m, err := s.Info(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Incomplete {
		t.Error("checksum failure meta not marked incomplete")
	}
	if !strings.Contains(perr.Error(), "sha256") {
		t.Errorf("error lacks the digest account: %v", perr)
	}
}

func TestPullCaptureStopLedgerShortfallFailsIdentity(t *testing.T) {
	ring := goldenRing()
	b := newBoard(ring)
	b.stopB = goldenPayload() - 3 // ledger disagrees with the ring stream
	b.stopR = "full"
	b.pushDone = true
	s := testStore(t)
	_, err := s.PullCapture(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{Port: 1, Pin: 38, Baud: 115200, RingKB: 128})
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum (payload accounting identity)", err)
	}
}

func TestPullCaptureEmptyCapture(t *testing.T) {
	b := newBoard(nil)
	b.stopB = 0
	b.stopR = "stopped"
	b.pushDone = true
	s := testStore(t)
	ref, err := s.PullCapture(context.Background(), PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{Port: 2, Pin: 39, Baud: 9600, RingKB: 64})
	if err != nil {
		t.Fatalf("empty capture: %v", err)
	}
	m := ref.Meta
	if m.Bytes != 0 || m.Chunks != 0 || m.TStartNS != 0 || m.TEndNS != 0 || m.Reason != "stopped" || m.StreamBytes != 0 {
		t.Fatalf("empty meta = %+v", m)
	}
	if m.SHA256 != hex.EncodeToString(sha256Sum(nil)) {
		t.Errorf("empty sha256 = %q", m.SHA256)
	}
	// arm args cross omitted optionals at zero
	arm := b.snapshot()[1]
	if _, ok := arm.args["idle_syms"]; ok {
		t.Errorf("idle_syms crossed at zero: %v", arm.args)
	}
	if _, ok := arm.args["max_ms"]; ok {
		t.Errorf("max_ms crossed at zero: %v", arm.args)
	}
}

func TestPullCaptureNoDoneWaitsThenCtxCancelLandsIncomplete(t *testing.T) {
	b := newBoard(nil) // no pushDone: the capture never ends
	b.stopB = 0
	b.stopR = "stopped"
	s := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	ref, err := s.PullCapture(ctx, PullBoard{ID: "aa11bb", Conn: b}, ArmArgs{Port: 1, Pin: 38, Baud: 115200, RingKB: 64})
	if !errors.Is(err, ErrIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want ErrIncomplete wrapping DeadlineExceeded", err)
	}
	m, err := s.Info(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Incomplete {
		t.Error("cancel landing not marked incomplete")
	}
	// end-of-intent: the still-armed capture was stopped best-effort
	names := b.opNames()
	if !containsOp(names, "la.stop") {
		t.Errorf("cancel path left the capture armed: %v", names)
	}
}

// pullRes carries one goroutine's PullCapture outcome.
type pullRes struct {
	ref CaptureRef
	err error
}

// TestRetentionSweepsLitterNotInFlightPull pins the fix-round-1 TOCTOU:
// between reserveID and writeMeta a live pull's dir is meta-less —
// indistinguishable from litter to a concurrent Retention(). Pull B
// parks at la.stop (deterministically inside that window, via the
// fake's gate); a direct sweep and a completing pull A's embedded
// sweep both run while B is parked. True litter (a dead process's
// meta-less dir) must go; B's dir must survive and B must land
// complete — zero phantoms.
func TestRetentionSweepsLitterNotInFlightPull(t *testing.T) {
	s := testStore(t)

	// Pull B: done pushes at arm, then la.stop parks — dir + data.bin
	// on disk, meta not yet written.
	bB := newBoard(nil)
	bB.stopB = 0
	bB.stopR = "stopped"
	bB.pushDone = true
	bB.stopGate = make(chan struct{})
	bB.stopHere = make(chan struct{})
	bDone := make(chan pullRes, 1)
	go func() {
		ref, err := s.PullCapture(context.Background(), PullBoard{ID: "bb22cc", Conn: bB}, ArmArgs{Port: 1, Pin: 38, Baud: 9600, RingKB: 64})
		bDone <- pullRes{ref, err}
	}()
	select {
	case <-bB.stopHere:
	case <-time.After(5 * time.Second):
		t.Fatal("pull B never reached la.stop")
	}

	bID := "20261115T101530-bb22cc-la-listen-1"
	bDir := filepath.Join(s.Root, bID)
	if fi, err := os.Stat(bDir); err != nil || !fi.IsDir() {
		t.Fatalf("pre-condition: in-flight dir missing at the window: %v", err)
	}
	if _, err := os.Stat(filepath.Join(bDir, "meta.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-condition: meta.json already present (%v) — the window was not hit", err)
	}

	// True litter — a dead process's abandoned pull dir — must still
	// sweep while B is in flight.
	if err := os.MkdirAll(filepath.Join(s.Root, "dead-pull-litter"), 0o700); err != nil {
		t.Fatal(err)
	}
	removed, err := s.Retention()
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != "dead-pull-litter" {
		t.Fatalf("direct sweep removed = %v, want exactly the dead-pull litter", removed)
	}
	if _, err := os.Stat(bDir); err != nil {
		t.Fatalf("direct sweep deleted the in-flight pull's dir: %v", err)
	}

	// A completing pull on the shared store runs its embedded retention
	// sweep while B is still parked — same guarantee.
	bA := newBoard(nil)
	bA.stopB = 0
	bA.stopR = "stopped"
	bA.pushDone = true
	if _, err := s.PullCapture(context.Background(), PullBoard{ID: "aa11bb", Conn: bA}, ArmArgs{Port: 1, Pin: 38, Baud: 9600, RingKB: 64}); err != nil {
		t.Fatalf("pull A: %v", err)
	}
	if _, err := os.Stat(bDir); err != nil {
		t.Fatalf("pull A's embedded sweep deleted the in-flight pull's dir: %v", err)
	}

	// Release B: it must land complete — the raced window left no hole.
	close(bB.stopGate)
	res := <-bDone
	if res.err != nil {
		t.Fatalf("pull B after the race window: %v", res.err)
	}
	if res.ref.ID != bID {
		t.Fatalf("pull B id = %q, want %q", res.ref.ID, bID)
	}
	ms, err := s.ListCaptures()
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 {
		t.Fatalf("ListCaptures = %d objects, want exactly A and B", len(ms))
	}
	// zero phantoms: every listed object has both files on disk
	for _, m := range ms {
		for _, f := range []string{"data.bin", "meta.json"} {
			if _, err := os.Stat(filepath.Join(s.Root, m.CapID, f)); err != nil {
				t.Errorf("phantom listing %s/%s: %v", m.CapID, f, err)
			}
		}
	}
}

func TestPullCaptureDeadLinkDuringWaitLandsIncomplete(t *testing.T) {
	b := newBoard(nil)
	b.pushDone = false
	b.stopB = 0
	b.stopR = "stopped"
	b.dieAt = 3 // subscribe+arm live; every later op dies (the link is gone)
	s := testStore(t)

	// Dead() is polled in the wait: a link that reports dead ends the
	// wait without a done event — no hang until ctx.
	dead := &deadBoard{scriptedBoard: b}
	dead.on = true

	ref, err := s.PullCapture(context.Background(), PullBoard{ID: "aa11bb", Conn: dead}, ArmArgs{Port: 1, Pin: 38, Baud: 115200, RingKB: 64})
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("err = %v, want ErrIncomplete", err)
	}
	m, err := s.Info(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Incomplete {
		t.Error("dead-link landing not marked incomplete")
	}
}

type deadBoard struct {
	*scriptedBoard
	on bool
}

func (d *deadBoard) Dead() bool { return d.on }

func containsOp(names []string, op string) bool {
	for _, n := range names {
		if n == op {
			return true
		}
	}
	return false
}

func sha256Sum(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

// ---------------------------------------------------------------------------
// readAll's defensive branches + the evQueue drop discipline (T5
// minor①): every identity the window walk checks on each reply, and
// the never-block rule the event handler owes the connection's read
// goroutine — none of these had a scripted test.

// readFake scripts ONLY la.read: each call answers from page().
type readFake struct {
	mu    sync.Mutex
	calls int
	page  func(call int, off int64) (data []byte, total int64, eof bool)
}

func (f *readFake) Op(ctx context.Context, op string, args map[string]any) (map[string]any, error) {
	if op != "la.read" {
		return nil, fmt.Errorf("readFake: unexpected op %s", op)
	}
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	data, total, eof := f.page(n, argU(args["off"]))
	return map[string]any{"data": data, "total": total, "eof": eof}, nil
}

func (f *readFake) OnEvent(string, func(replclient.Event)) func() { return func() {} }
func (f *readFake) Dead() bool                                    { return false }

var _ Board = (*readFake)(nil)

func TestReadAllTotalMutatedMidRead(t *testing.T) {
	f := &readFake{page: func(call int, off int64) ([]byte, int64, bool) {
		if call == 1 {
			return make([]byte, 16), 100, false
		}
		return make([]byte, 16), 101, false // the ring "grew": not stopped
	}}
	_, err := readAll(context.Background(), f, 7, func(int64, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "mutated mid-read") {
		t.Fatalf("err = %v, want the mutated-total refusal", err)
	}
}

func TestReadAllEmptyWindowWithBytesToGo(t *testing.T) {
	f := &readFake{page: func(call int, off int64) ([]byte, int64, bool) {
		if call == 1 {
			return make([]byte, 32), 100, false
		}
		return nil, 100, false // a stalled empty window, 68 bytes to go
	}}
	_, err := readAll(context.Background(), f, 7, func(int64, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "empty window") {
		t.Fatalf("err = %v, want the empty-window refusal", err)
	}
}

func TestReadAllEofLagConverges(t *testing.T) {
	// the accounting identities converge at off==total while eof lags
	// one reply — the walk answers done, not another spin.
	f := &readFake{page: func(call int, off int64) ([]byte, int64, bool) {
		return make([]byte, 40), 40, false
	}}
	var seen int
	total, err := readAll(context.Background(), f, 7, func(int64, []byte) error { seen++; return nil })
	if err != nil || total != 40 || seen != 1 {
		t.Fatalf("readAll = (%d, %v) sink=%d, want (40, nil) sink=1 — eof lag must converge", total, err, seen)
	}
}

func TestReadAllShortPullAtEof(t *testing.T) {
	f := &readFake{page: func(call int, off int64) ([]byte, int64, bool) {
		return make([]byte, 10), 100, true // eof claimed at 10 of 100
	}}
	_, err := readAll(context.Background(), f, 7, func(int64, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "short pull") {
		t.Fatalf("err = %v, want the short-pull refusal", err)
	}
}

// TestSubscribeLADropsRatherThanBlocks: the pull's local "la" handler
// must NEVER block the connection's read goroutine — a full queue
// drops (the default branch), it does not wait.
func TestSubscribeLADropsRatherThanBlocks(t *testing.T) {
	b := newBoard(nil)
	_, cancel := subscribeLA(b)
	defer cancel()
	b.mu.Lock()
	var fn func(replclient.Event)
	for _, r := range b.regs {
		fn = r.fn
	}
	b.mu.Unlock()
	if fn == nil {
		t.Fatal("no la handler registered")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < evQueue+64; i++ { // capacity plus overflow
			fn(replclient.Event{Source: "la", Data: map[string]any{"event": "progress"}})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler blocked on a full evQueue — the read goroutine would stall")
	}
}
