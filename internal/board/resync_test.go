package board

import (
	"context"
	"testing"
)

func TestParseScriptList(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"scripts (0):", nil},
		{"scripts (1): cnt", []string{"cnt"}},
		{"scripts (2): a b", []string{"a", "b"}},
		{"scripts (3): t10keep t10adopt t10resync", []string{"t10keep", "t10adopt", "t10resync"}},
		{"garbage from a foreign build", nil},
	} {
		got := parseScriptList(tc.in)
		if len(got) != len(tc.want) {
			t.Fatalf("parseScriptList(%q) = %v, want %v", tc.in, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("parseScriptList(%q) = %v, want %v", tc.in, got, tc.want)
			}
		}
	}
}

func TestParsePinsDump(t *testing.T) {
	if got := pinsHeaderCount(samplePinsDump); got != 2 {
		t.Fatalf("header count = %d, want 2", got)
	}
	claims := parsePinsDump(samplePinsDump)
	if len(claims) != 2 {
		t.Fatalf("claims = %+v, want 2", claims)
	}
	c0, c1 := claims[0], claims[1]
	if c0.GPIO != 1 || c0.Session != 0 || c0.Owner != "_eval" || c0.Prim != "gpio" || c0.Mode != "out" || c0.Watch != "none" {
		t.Fatalf("claim0 = %+v", c0)
	}
	if c1.GPIO != 2 || c1.Session != 1792000001 || c1.Owner != "t10keep" || c1.Watch != "rising" {
		t.Fatalf("claim1 = %+v", c1)
	}
	// partial dump (chunked): no header yet → -1, terminator logic
	if got := pinsHeaderCount("GPIO1   sess=0 owner"); got != -1 {
		t.Fatalf("headerless partial dump count = %d, want -1", got)
	}
}

func TestResyncColdStartListsOrphansAndNeverKills(t *testing.T) {
	ctl := newFakeCtl()
	ctl.listOut = "scripts (2): orphan1 orphan2"
	ctl.pinsChunks = []string{samplePinsDump}
	sup, _ := newFakeSup(ctl)
	defer sup.Close()

	rep, err := sup.Resync(context.Background())
	if err != nil {
		t.Fatalf("resync: %v", err)
	}
	if !rep.ColdStart {
		t.Fatal("first resync must be ColdStart (empty baseline — daemon-startup orphan audit)")
	}
	if len(rep.Scripts) != 2 || rep.Scripts[0] != "orphan1" || rep.Scripts[1] != "orphan2" {
		t.Fatalf("scripts = %v", rep.Scripts)
	}
	if len(rep.Survived) != 0 || len(rep.Gone) != 0 || len(rep.Started) != 0 {
		t.Fatalf("cold-start diff must be empty, got %+v", rep)
	}
	if len(rep.Pins) != 2 || rep.Pins[0].Owner != "_eval" || rep.Pins[1].Owner != "t10keep" {
		t.Fatalf("pins = %+v", rep.Pins)
	}
	if rep.Wifi["state"] != "up" {
		t.Fatalf("wifi = %v", rep.Wifi)
	}
	if res := ctl.resetCalls(); len(res) != 0 {
		t.Fatalf("orphan audit MUST NOT kill (session.reset calls=%v)", res)
	}
}

func TestResyncAfterLinkLossHonestDiff(t *testing.T) {
	ctl1 := newFakeCtl()
	ctl1.listOut = "scripts (2): t10keep t10resync"
	ctl1.pinsChunks = []string{samplePinsDump}
	sup, _ := newFakeSup(ctl1)
	if _, err := sup.Resync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// link loss: old controller dies, a fresh one sees a mutated board
	ctl2 := newFakeCtl()
	ctl2.listOut = "scripts (2): t10resync t10new" // t10keep died, t10new appeared
	ctl2.pinsChunks = []string{"pins (1):\n" +
		"  GPIO2   sess=1792000001 owner=t10resync prim=gpio mode=in watch=rising\n"}
	ctl1.markDead()
	// re-dial seam: the supervisor's next Connect must hand out ctl2
	sup.opts.DialTCP = func() (Controller, error) { return ctl2, nil }

	rep, err := sup.Resync(context.Background())
	if err != nil {
		t.Fatalf("resync after link loss: %v", err)
	}
	if rep.ColdStart {
		t.Fatal("second resync is a diff, not a cold start")
	}
	if len(rep.Survived) != 1 || rep.Survived[0] != "t10resync" {
		t.Fatalf("survived = %v, want [t10resync] (fw keeps scripts through link loss)", rep.Survived)
	}
	if len(rep.Gone) != 1 || rep.Gone[0] != "t10keep" {
		t.Fatalf("gone = %v, want [t10keep]", rep.Gone)
	}
	if len(rep.Started) != 1 || rep.Started[0] != "t10new" {
		t.Fatalf("started = %v, want [t10new] (found, not started by us)", rep.Started)
	}
	if len(rep.PinsGone) != 1 || rep.PinsGone[0].GPIO != 1 {
		t.Fatalf("pins gone = %+v, want GPIO1 (_eval claim released)", rep.PinsGone)
	}
	if len(rep.Pins) != 1 || rep.Pins[0].Owner != "t10resync" {
		t.Fatalf("pins now = %+v", rep.Pins)
	}
}
