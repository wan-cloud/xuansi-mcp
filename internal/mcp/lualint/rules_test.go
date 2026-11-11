// rules_test.go — R1–R4 positive AND negative cases each, plus the
// closed-vocabulary refusal shape (a LexError surfaces as an error,
// never as a fake finding) and the BenchIndex wiring R4 rides.
package lualint

import (
	"strings"
	"testing"
)

// testBench mirrors the generated bench_signatures table's shape (the
// mcp package builds the real one from the embedded abi-reference).
func testBench() BenchIndex {
	return BenchIndex{
		Members: map[string]bool{
			"gpio": true, "uart": true, "i2c": true, "spi": true, "pwm": true,
			"adc": true, "rmt": true, "events": true, "wasm": true,
			"millis": true, "caps": true, "gc": true, "api_version": true,
		},
		Families: map[string][]string{
			"gpio":   {"mode", "write", "read", "on"},
			"uart":   {"setup", "write", "read", "on"},
			"i2c":    {"setup", "write", "read", "wreg", "rreg", "scan"},
			"wasm":   {"load", "call", "unload", "list", "info", "xfer", "plan"},
			"events": {"on", "off", "next"},
		},
	}
}

// rules returns the rules of one lint run, failing on a LexError.
func rules(t *testing.T, src string) []Finding {
	t.Helper()
	f, err := Lint(src, testBench())
	if err != nil {
		t.Fatalf("Lint(%q): %v", src, err)
	}
	return f
}

// hasRule reports whether the findings carry one of the rule ids.
func hasRule(fs []Finding, rule string) bool {
	for _, f := range fs {
		if f.Rule == rule {
			return true
		}
	}
	return false
}

// oneOf returns the first finding with the rule (nil when absent).
func oneOf(fs []Finding, rule string) *Finding {
	for i := range fs {
		if fs[i].Rule == rule {
			return &fs[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// R1

func TestR1Positive(t *testing.T) {
	cases := []string{
		"return a / b",
		"return 2^31",
		"return 1.5",
		"return #(t)/2",
		"return (x)/(y)",
		"function f() return total / n end",
	}
	for _, src := range cases {
		if f := oneOf(rules(t, src), "R1"); f == nil {
			t.Errorf("Lint(%q): no R1 finding", src)
		} else if f.Line != 1 && !strings.Contains(src, "\n") {
			t.Errorf("Lint(%q): R1 line = %d, want the return's line", src, f.Line)
		}
	}
}

func TestR1Negative(t *testing.T) {
	cases := []string{
		"return a",
		"return a // b", // floor division on integers stays integer
		"return a % b",
		"return math.floor(a / b)", // the coached fix: division inside floor
		"return 42",
		"return 0x10",
		"x = a / b", // division outside a return
		"local q = require",
		"return", // bare return
	}
	for _, src := range cases {
		if hasRule(rules(t, src), "R1") {
			t.Errorf("Lint(%q): unexpected R1 finding", src)
		}
	}
}

func TestR1SpanEndsAtBlockKeywords(t *testing.T) {
	// the hazard AFTER the closing keyword belongs to the next block
	src := "if c then return 1 else x = a / b end"
	if hasRule(rules(t, src), "R1") {
		t.Errorf("Lint(%q): division past else must not fire R1", src)
	}
	src = "while r do return 1 end\nx = a / b"
	if hasRule(rules(t, src), "R1") {
		t.Errorf("Lint(%q): division past end must not fire R1", src)
	}
	// …but a division inside the returned expression fires wherever it sits
	src = "return f(a / b)"
	if !hasRule(rules(t, src), "R1") {
		t.Errorf("Lint(%q): nested division must fire R1", src)
	}
}

// ---------------------------------------------------------------------------
// R2

func TestR2Positive(t *testing.T) {
	src := `local m = require("bench.drv.mod")
while true do
  coroutine.yield()
end`
	f := oneOf(rules(t, src), "R2")
	if f == nil {
		t.Fatalf("no R2 finding for require-then-yield")
	}
	if f.Line != 3 {
		t.Errorf("R2 line = %d, want 3 (the yield)", f.Line)
	}
}

func TestR2Negative(t *testing.T) {
	cases := []string{
		"while true do coroutine.yield() end", // no require
		"local m = require('x')",              // require, no yield
	}
	for _, src := range cases {
		if hasRule(rules(t, src), "R2") {
			t.Errorf("Lint(%q): unexpected R2 finding", src)
		}
	}
	// yield BEFORE the require is the legal order
	src := "coroutine.yield()\nlocal m = require('x')"
	if hasRule(rules(t, src), "R2") {
		t.Errorf("yield-then-require must not fire R2")
	}
}

// ---------------------------------------------------------------------------
// R3

func TestR3Positive(t *testing.T) {
	cases := []string{
		// function literal as an argument
		`bench.events.on("gpio", function() bench.i2c.scan(0) end)`,
		// assigned literal invoked later — token level cannot tell, included
		`local cb = function() delay_ms(200) end`,
		// uart.read WITH timeout (3 args)
		`register(function() local d = bench.uart.read(1, 256, 500) end)`,
		// dotted camera spelling
		`bench.events.on("t", function() bench.cam.frame_get(0, 0, 2048) end)`,
		// fused camera spelling
		`bench.events.on("t", function() bench.cam_frame_get() end)`,
		// nested deeper inside the literal body
		`cb(function() if x then bench.i2c.scan(1) end end)`,
	}
	for _, src := range cases {
		if !hasRule(rules(t, src), "R3") {
			t.Errorf("Lint(%q): no R3 finding", src)
		}
	}
}

func TestR3Negative(t *testing.T) {
	cases := []string{
		// statement-level function declaration: not a callback literal
		`function worker() bench.i2c.scan(0) end`,
		// blocking call OUTSIDE the literal
		`bench.i2c.scan(0)`,
		`local d = bench.uart.read(1, 256, 500)`,
		`delay_ms(100)`,
		// uart.read with NO timeout (the non-blocking 2-arg poll)
		`bench.events.on("t", function() local d = bench.uart.read(1, 256) end)`,
		// literal body with no blocking call
		`bench.events.on("gpio", function() x = x + 1 end)`,
		// method-style declaration after a dot (function a.b() end is a
		// declaration, and the preceding token is a Name)
		`function mod.tick() delay_ms(10) end`,
	}
	for _, src := range cases {
		if hasRule(rules(t, src), "R3") {
			t.Errorf("Lint(%q): unexpected R3 finding", src)
		}
	}
}

func TestR3ReportAtBlockingCall(t *testing.T) {
	src := `bench.events.on("t", function()
  delay_ms(50)
end)`
	f := oneOf(rules(t, src), "R3")
	if f == nil || f.Line != 2 || f.Col != 3 {
		t.Fatalf("R3 = %+v, want line 2 col 3 (the delay_ms)", f)
	}
}

// ---------------------------------------------------------------------------
// R4

func TestR4Positive(t *testing.T) {
	cases := []string{
		"bench.frobnicate(1)",
		"bench.gpio.frobnicate(4)",
		"x = bench.wasm.hotload",
	}
	for _, src := range cases {
		fs := rules(t, src)
		if !hasRule(fs, "R4") {
			t.Errorf("Lint(%q): no R4 finding", src)
		}
		if f := oneOf(fs, "R4"); f != nil && f.Hint == "" {
			t.Errorf("Lint(%q): R4 hint empty", src)
		}
	}
}

func TestR4Negative(t *testing.T) {
	cases := []string{
		"bench.gpio.write(4, 1)",
		"bench.wasm.plan({})",
		"bench.millis()",
		"bench.caps()",
		"x = bench.api_version",
		"bench.gc('collect')",
		"bench.i2c.scan(0)",  // scan is real; R3 is its rule, not R4
		"local bench2 = 1",   // a different name entirely
		"benchx.write(4, 1)", // bench-prefixed name is not bench
	}
	for _, src := range cases {
		if hasRule(rules(t, src), "R4") {
			t.Errorf("Lint(%q): unexpected R4 finding", src)
		}
	}
}

// ---------------------------------------------------------------------------
// wiring shapes

func TestLintLexErrorIsAnErrorNotAFakeFinding(t *testing.T) {
	// an unterminated string refuses the whole lint with the closed
	// vocabulary intact — the tool surfaces lua.check as the truth
	if _, err := Lint(`x = "open`, testBench()); err == nil {
		t.Fatal("LexError must surface as an error, not findings")
	}
	// nil BenchIndex disables R4 only
	f, err := Lint("bench.nothing.here()", BenchIndex{})
	if err != nil {
		t.Fatalf("Lint: %v", err)
	}
	if hasRule(f, "R4") {
		t.Error("R4 fired with an empty BenchIndex")
	}
}

func TestLintFindingsCarryPositionsAndText(t *testing.T) {
	fs := rules(t, "return 1/2")
	f := oneOf(fs, "R1")
	if f == nil {
		t.Fatal("no R1")
	}
	if f.Line != 1 || f.Col != 1 || f.Msg == "" || f.Hint == "" {
		t.Errorf("finding = %+v, want full shape at 1:1", f)
	}
}
