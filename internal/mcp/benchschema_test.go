package mcp

// benchschema_test.go — the transcription self-check: BenchSchema(1)
// must match the frozen v1 bench API page entry-by-entry (§7 batch-2
// primitives + §3.2 gpio; docs/資料/v1-bench-API.md, api_version=1).
// The expected table below is transcribed from the page INDEPENDENTLY
// of benchschema.go — a shared typo survives only if both copies are
// wrong the same way, and the per-prim op lists are additionally
// pinned to the §8 caps schema's ops arrays.

import (
	"fmt"
	"strings"
	"testing"
)

// capsOpsV1 is §8's primitives.<prim>.ops, verbatim (the caps schema
// the daemon's dynamic expansion consumes).
var capsOpsV1 = map[string][]string{
	"gpio":   {"mode", "write", "read", "on"},
	"uart":   {"setup", "write", "read", "on"},
	"i2c":    {"setup", "write", "read", "wreg", "rreg", "scan"},
	"spi":    {"setup", "xfer", "write", "read"},
	"pwm":    {"setup", "duty", "stop"},
	"adc":    {"setup", "read", "read_mv"},
	"rmt":    {"setup", "tx", "rx"},
	"events": {"on", "off", "next"},
}

// capsPrimOrderV1 is §8's primitives key order.
var capsPrimOrderV1 = []string{"gpio", "uart", "i2c", "spi", "pwm", "adc", "rmt", "events"}

// expectedV1 is the §7 (+§3.2 gpio) signature table: prim, op, and the
// parameter tuple with kinds (param:kind, enum[a|b|…], trailing ? =
// optional), in the page's per-prim op order.
var expectedV1 = []struct{ prim, op, params string }{
	{"gpio", "mode", "pin:int, mode:enum[out|in|in_pu|in_pd]"},
	{"gpio", "write", "pin:int, level:int"},
	{"gpio", "read", "pin:int"},
	{"gpio", "on", "pin:int, edge:enum[rising|falling|both], fn:str"},

	{"uart", "setup", "port:int, cfg:table"},
	{"uart", "write", "port:int, data:bin"},
	{"uart", "read", "port:int, max:int, timeout_ms:int?"},
	{"uart", "on", "port:int, fn:str"},

	{"i2c", "setup", "port:int, cfg:table"},
	{"i2c", "write", "port:int, addr:int, data:bin"},
	{"i2c", "read", "port:int, addr:int, n:int"},
	{"i2c", "wreg", "port:int, addr:int, reg:int, data:bin"},
	{"i2c", "rreg", "port:int, addr:int, reg:int, n:int"},
	{"i2c", "scan", "port:int"},

	{"spi", "setup", "port:int, cfg:table"},
	{"spi", "xfer", "port:int, data:bin"},
	{"spi", "write", "port:int, data:bin"},
	{"spi", "read", "port:int, n:int"},

	{"pwm", "setup", "pin:int, cfg:table"},
	{"pwm", "duty", "pin:int, duty:int"},
	{"pwm", "stop", "pin:int"},

	{"adc", "setup", "pin:int, cfg:table"},
	{"adc", "read", "pin:int"},
	{"adc", "read_mv", "pin:int"},

	{"rmt", "setup", "pin:int, cfg:table"},
	{"rmt", "tx", "pin:int, items:table, timeout_ms:int"},
	{"rmt", "rx", "pin:int, timeout_ms:int"},

	{"events", "on", "source:str, fn:str?"},
	{"events", "off", "source:str"},
	{"events", "next", "timeout_ms:int"},
}

// renderParams formats a BenchFn's params the way expectedV1 spells
// them, so a mismatch reports as a readable diff line.
func renderParams(fn BenchFn) string {
	var parts []string
	for _, p := range fn.Params {
		s := p.Name + ":" + p.Kind
		if p.Kind == "enum" {
			s = p.Name + ":enum[" + strings.Join(p.Enum, "|") + "]"
		}
		if p.Optional {
			s += "?"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

// TestBenchSchemaV1Transcription: BenchSchema(1) is exactly the 30
// frozen signatures — prim/op sequence in caps order, every parameter
// tuple (name/kind/enum/optional) matching §7, every Desc non-empty.
func TestBenchSchemaV1Transcription(t *testing.T) {
	got := BenchSchema(1)
	if got == nil {
		t.Fatal("BenchSchema(1) = nil, want the 30 frozen signatures")
	}
	if len(got) != len(expectedV1) {
		t.Fatalf("BenchSchema(1) has %d entries, want %d", len(got), len(expectedV1))
	}
	seen := map[string]bool{}
	for i, fn := range got {
		want := expectedV1[i]
		if fn.Prim != want.prim || fn.Op != want.op {
			t.Errorf("entry %d = %s.%s, want %s.%s", i, fn.Prim, fn.Op, want.prim, want.op)
			continue
		}
		if seen[fn.Prim+"."+fn.Op] {
			t.Errorf("duplicate (prim,op) %s.%s", fn.Prim, fn.Op)
		}
		seen[fn.Prim+"."+fn.Op] = true
		if params := renderParams(fn); params != want.params {
			t.Errorf("%s.%s params = %q, want %q", fn.Prim, fn.Op, params, want.params)
		}
		if strings.TrimSpace(fn.Desc) == "" {
			t.Errorf("%s.%s has an empty Desc", fn.Prim, fn.Op)
		}
		for _, p := range fn.Params {
			switch p.Kind {
			case "int", "str", "bool", "bin", "table":
				if p.Enum != nil {
					t.Errorf("%s.%s param %s: kind %s must carry no Enum", fn.Prim, fn.Op, p.Name, p.Kind)
				}
			case "enum":
				if len(p.Enum) == 0 {
					t.Errorf("%s.%s param %s: enum kind with empty Enum", fn.Prim, fn.Op, p.Name)
				}
			default:
				t.Errorf("%s.%s param %s: unknown kind %q", fn.Prim, fn.Op, p.Name, p.Kind)
			}
		}
	}

	// per-prim op lists match the §8 caps schema, in order
	perPrim := map[string][]string{}
	for _, fn := range got {
		perPrim[fn.Prim] = append(perPrim[fn.Prim], fn.Op)
	}
	for _, prim := range capsPrimOrderV1 {
		want := capsOpsV1[prim]
		gotOps := perPrim[prim]
		if len(gotOps) != len(want) || fmt.Sprint(gotOps) != fmt.Sprint(want) {
			t.Errorf("prim %s ops = %v, want %v (§8 caps order)", prim, gotOps, want)
		}
	}
	if extra := len(got) - 30; extra != 0 {
		t.Errorf("BenchSchema(1) carries %d entries beyond the frozen 30", extra)
	}
}

// TestBenchSchemaUnsupportedVersions: anything but api_version 1
// disables expansion (nil) — the expansion engine surfaces a notice
// instead of guessing at unknown signatures.
func TestBenchSchemaUnsupportedVersions(t *testing.T) {
	for _, v := range []int{0, 2, 3, -1} {
		if got := BenchSchema(v); got != nil {
			t.Errorf("BenchSchema(%d) = %d entries, want nil (expansion disabled)", v, len(got))
		}
	}
}

// TestBenchSchemaDescFaithfulness: spot-checks that the Desc lines
// carry the §7 semantics an agent needs (bounds, enums, exceptions).
func TestBenchSchemaDescFaithfulness(t *testing.T) {
	got := BenchSchema(1)
	byName := map[string]BenchFn{}
	for _, fn := range got {
		byName[fn.Prim+"."+fn.Op] = fn
	}
	spots := []struct{ fn, want string }{
		{"gpio.write", `"out"`},        // must-be-output gate
		{"uart.read", "partial"},       // timeout-returns-partial exception
		{"uart.setup", "tx=42"},        // port2 default pins
		{"i2c.scan", "0x08"},           // scan range
		{"i2c.rreg", "repeated-start"}, // rreg's repeated-start read
		{"i2c.wreg", "8-bit reg"},      // wreg's register write
		{"spi.read", "0xFF"},           // dummy-clock semantics
		{"adc.setup", "2.5"},           // atten enum with the fractional member
		{"rmt.setup", "resolution_actual"},
		{"rmt.rx", "empty"},
		{"pwm.setup", "2^resolution"}, // duty bound
		{"events.on", "polling"},
		{"events.next", "0..5000"},
		// the callback-registering ops must name the events face's
		// arming path (final review): the sources flow only after
		// xuansi_capture arms them board-side.
		{"gpio.on", "xuansi_capture"},
		{"uart.on", "xuansi_capture"},
		{"events.on", "xuansi_capture"},
	}
	for _, s := range spots {
		fn, ok := byName[s.fn]
		if !ok {
			t.Fatalf("spot-check %s: not in BenchSchema(1)", s.fn)
		}
		if !strings.Contains(fn.Desc, s.want) {
			t.Errorf("%s Desc = %q, want it to mention %q", s.fn, fn.Desc, s.want)
		}
	}
}
