package la

// pull_sample_test.go — the samples pull (T13) against the scripted
// board: the op sequence + verbatim arm args (rate/ring/trig bits
// form), the sample ledger mirror (rate_hz, samples, t0_ns, the trig
// ch/mask/value/width), the byte-form identity (file == total ==
// stop's bytes), and the broken-pull forensics landing.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"xuansi/internal/replclient"
)

// sampleStream: 3 idle ticks + one CS-framed mode-0 byte — enough
// shape for the decode to read the landed object back.
func sampleStream() []byte {
	var b tickBuf
	spiIdle(&b, 0, 3)
	spiEmit(&b, 0, []byte{0x5A}, []byte{0xA5}, 2, 1)
	spiIdle(&b, 0, 3)
	return b.buf
}

func TestPullSampleHappyPath(t *testing.T) {
	ring := sampleStream()
	sum := sha256.Sum256(ring)
	b := newBoard(ring)
	b.sample = true
	b.rateHz = 20_000_000
	b.stopB = int64(len(ring))
	b.stopR = "trig"
	b.pushDone = true
	b.sampleMeta = map[string]any{
		"rate_hz": int64(20_000_000), "samples": int64(len(ring)),
		"trig": map[string]any{"ch": int64(3), "kind": "bits",
			"mask": int64(8), "value": int64(0), "width": int64(2), "pre_kb": int64(8)},
		"trig_fired": true, "hw_ovf": int64(0), "ring_kb": int64(512),
		"max_ms": int64(8000), "reason": "trig", "armed_ns": int64(11),
		"stopped_ns": int64(22), "t0_ns": int64(9_000_000_000),
	}
	s := testStore(t)

	ref, err := s.PullSample(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, SampleArmArgs{
		RateHz: 20_000_000, RingKB: 512, MaxMS: 8000,
		Trig: &SampleTrig{Ch: 3, Kind: "bits", Mask: 8, Value: 0, Width: 2, PreKB: 8},
	})
	if err != nil {
		t.Fatalf("PullSample: %v", err)
	}

	// the op sequence: subscribe precedes arm, stop/info after the
	// done, both read passes, disarm last; the arm args cross
	// verbatim.
	wantSeq := []rec{
		{"events.subscribe", map[string]any{"source": "la"}},
		{"la.sample_arm", map[string]any{
			"rate_hz": int64(20_000_000), "ring_kb": int64(512), "max_ms": int64(8000),
			"trig": map[string]any{"ch": int64(3), "kind": "bits", "mask": int64(8),
				"value": int64(0), "width": int64(2), "pre_kb": int64(8)}}},
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

	disk, err := os.ReadFile(ref.DataPath)
	if err != nil || string(disk) != string(ring) {
		t.Fatalf("data.bin = %d bytes err=%v, want the ring verbatim", len(disk), err)
	}
	if !strings.Contains(ref.ID, "-la-sample-") {
		t.Errorf("ID = %q (the sample mode suffix)", ref.ID)
	}
	tickNS := uint64(1_000_000_000 / 20_000_000)
	wantMeta := Meta{
		CapID: ref.ID, Board: "97c1ac", Mode: "samples",
		Pins:    SampleBusPins(),
		RateHz:  20_000_000,
		TStartNS: 9_000_000_000,
		TEndNS:  9_000_000_000 + uint64(len(ring))*tickNS,
		Bytes:   int64(len(ring)), Samples: int64(len(ring)), Reason: "trig",
		Trig:      &TrigMeta{Ch: 3, Kind: "bits", Mask: 8, Value: 0, Width: 2, PreKB: 8},
		TrigFired: true, T0NS: 9_000_000_000,
		RingKB: 512, MaxMS: 8000,
		StreamBytes: int64(len(ring)),
		ArmedNS:     11, StoppedNS: 22,
		SHA256:   hex.EncodeToString(sum[:]),
		PulledAt: fixedNow,
	}
	if !reflect.DeepEqual(ref.Meta, wantMeta) {
		t.Fatalf("ref.Meta:\n got %+v\nwant %+v", ref.Meta, wantMeta)
	}

	// the landed object answers the bus decode (the whole chain)
	res, derr := DecodeBus(ref, "spi", DecodeBusParams{Mode: 0})
	if derr != nil || res.NTxn != 1 || res.Txns[0].MOSI != "5a" || res.Txns[0].MISO != "a5" {
		t.Fatalf("DecodeBus on the landed object = %+v err=%v", res, derr)
	}
}

// The byte-form identity: a stop ledger that disagrees with the
// landed bytes is ErrChecksum — forensics only.
func TestPullSampleStopIdentity(t *testing.T) {
	ring := sampleStream()
	b := newBoard(ring)
	b.sample = true
	b.rateHz = 5_000_000
	b.stopB = int64(len(ring)) - 4
	b.stopR = "full"
	b.pushDone = true
	b.sampleMeta = map[string]any{"rate_hz": int64(5_000_000), "t0_ns": int64(5), "ring_kb": int64(64)}
	s := testStore(t)
	ref, err := s.PullSample(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, SampleArmArgs{RateHz: 5_000_000, RingKB: 64})
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
	if ref.ID == "" || !ref.Meta.Incomplete {
		t.Fatalf("the broken pull must land the incomplete object, got %+v", ref.Meta)
	}
	if ref.Meta.Samples != int64(len(ring)) {
		t.Errorf("forensics samples = %d, want %d", ref.Meta.Samples, len(ring))
	}
}

// An arm refusal crosses verbatim with nothing landed; a mid-read
// death lands the incomplete object.
func TestPullSampleRefusalAndIncomplete(t *testing.T) {
	b := newBoard(nil)
	b.sample = true
	b.armErr = &replclient.OpError{Op: "la.sample_arm", Class: "api",
		Msg: "la.sample_arm: camera active; camera.off first (DVP engine holds the LCD_CAM/GDMA lane — stream or xclk live)"}
	s := testStore(t)
	ref, err := s.PullSample(context.Background(), PullBoard{ID: "97c1ac", Conn: b}, SampleArmArgs{RateHz: 1_000_000, RingKB: 64})
	if err == nil || ref.ID != "" {
		t.Fatalf("arm refusal: ref=%+v err=%v, want nothing landed", ref, err)
	}
	if !strings.Contains(err.Error(), "camera active") {
		t.Errorf("refusal text = %q, want the fw coach verbatim", err)
	}

	b2 := newBoard(sampleStream())
	b2.sample = true
	b2.stopB = int64(len(sampleStream()))
	b2.stopR = "trig"
	b2.pushDone = true
	b2.sampleMeta = map[string]any{"rate_hz": int64(1_000_000), "t0_ns": int64(9), "ring_kb": int64(64)}
	b2.dieAt = 5 // the first la.read dies
	ref2, err := s.PullSample(context.Background(), PullBoard{ID: "97c1ac", Conn: b2}, SampleArmArgs{RateHz: 1_000_000, RingKB: 64})
	if !errors.Is(err, ErrIncomplete) || ref2.ID == "" || !ref2.Meta.Incomplete {
		t.Fatalf("mid-read death: err=%v ref=%+v, want incomplete forensics", err, ref2.Meta)
	}
}
