package la

// export_vcd_test.go — la_export (LA 增強波 A): the VCD writer's
// goldens (header shape, dumpvars, the change columns at exact
// timestamps), the samples popcount path, the idempotent overwrite,
// the 2^22 change cap, and the refusals (listen, empty, no-dir).
// The fixtures are COPIED into temp object dirs — the committed
// testdata stays pristine (the writer writes into ref.Dir).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// objCopy lands a fixture as a writable object dir (Dir set — the
// exporter's path domain).
func objCopy(t *testing.T, fixture string) CaptureRef {
	t.Helper()
	src := fixtureRef(t, fixture)
	dir := t.TempDir()
	data, err := os.ReadFile(src.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	m := src.Meta
	m.CapID = fixture + "-copy"
	return CaptureRef{ID: m.CapID, Dir: dir, DataPath: p, Meta: m}
}

func TestExportVCDEdgeGolden(t *testing.T) {
	ref := objCopy(t, "edgepwm")
	got, err := ExportVCD(ref)
	if err != nil {
		t.Fatalf("ExportVCD: %v", err)
	}
	if got.Path != filepath.Join(ref.Dir, "export.vcd") {
		t.Fatalf("path = %q, want the object dir's export.vcd", got.Path)
	}
	if got.Changes != 39 || got.Truncated {
		t.Fatalf("changes = %d trunc %v, want 39 (the 40-run stream) / false", got.Changes, got.Truncated)
	}
	raw, err := os.ReadFile(got.Path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	join := strings.Join(lines, "\n")
	for _, want := range []string{
		"$comment xuansi-la capture edgepwm-copy mode edge absolute t0_ns 4000000000 $end",
		"$timescale 1ns $end",
		"$scope module la $end",
		"$var wire 1 ! pin40 $end",
		"$enddefinitions $end",
	} {
		if !strings.Contains(join, want) {
			t.Errorf("VCD lacks %q", want)
		}
	}
	// dumpvars carries the opening level (mask 1 → high); the first
	// change is the +250µs fall, the second the +1ms rise
	if i := indexOf(lines, "$dumpvars"); i < 0 || lines[i+1] != "1!" {
		t.Errorf("dumpvars = %q after %d, want 1! (the opening high)", lines[i+1], i)
	}
	if i := indexOf(lines, "#250000"); i < 0 || lines[i+1] != "0!" {
		t.Errorf("#250000 block = %q, want 0!", lines[i+1])
	}
	if i := indexOf(lines, "#1000000"); i < 0 || lines[i+1] != "1!" {
		t.Errorf("#1000000 block = %q, want 1!", lines[i+1])
	}
	// the last change: run 39 starts at +19.25ms (a fall)
	if i := indexOf(lines, "#19250000"); i < 0 || lines[i+1] != "0!" {
		t.Errorf("#19250000 block = %q, want the final 0!", lines[i+1])
	}
	if got.Bytes != int64(len(raw)) {
		t.Errorf("bytes = %d, want the stat %d", got.Bytes, len(raw))
	}
}

func TestExportVCDSamplesAndIdempotent(t *testing.T) {
	ref := objCopy(t, "sampwm")
	got, err := ExportVCD(ref)
	if err != nil || got.Changes != 30 {
		t.Fatalf("samples export = %+v err %v, want 30 single-channel flips", got, err)
	}
	raw, _ := os.ReadFile(got.Path)
	lines := strings.Split(string(raw), "\n")
	// eight vars D0..D7 on the ids ! # $ % & ' ( ); the initial byte
	// 0x00 → all-low dumpvars; the first flips: +5µs D0 rises (id !),
	// +6µs D1 rises (id #), +10µs D0 falls
	if !strings.Contains(string(raw), "$var wire 1 & D4 $end") {
		t.Errorf("vars lack D4 (id &)")
	}
	if i := indexOf(lines, "#5000"); i < 0 || lines[i+1] != "1!" {
		t.Errorf("#5000 = %q, want 1! (D0 rise)", lines[i+1])
	}
	if i := indexOf(lines, "#6000"); i < 0 || lines[i+1] != "1#" {
		t.Errorf("#6000 = %q, want 1# (D1 rise)", lines[i+1])
	}
	if i := indexOf(lines, "#10000"); i < 0 || lines[i+1] != "0!" {
		t.Errorf("#10000 = %q, want 0! (D0 fall)", lines[i+1])
	}
	// idempotent: a second run overwrites byte-identically
	first := raw
	got2, err := ExportVCD(ref)
	if err != nil {
		t.Fatalf("re-export: %v", err)
	}
	second, _ := os.ReadFile(got2.Path)
	if len(first) != len(second) || got2.Changes != got.Changes {
		t.Errorf("re-export = %d bytes (was %d) changes %d (was %d) — not idempotent", len(second), len(first), got2.Changes, got.Changes)
	}
	if _, err := os.Stat(filepath.Join(ref.Dir, "export.vcd.tmp")); err == nil {
		t.Error("the tmp landing file survived the rename")
	}
}

func TestExportVCDTruncationCap(t *testing.T) {
	// 600k alternating ticks × 8 channels/bit = 4.8M changes > the
	// 2^22 cap → the exact cap written, the flag set
	dir := t.TempDir()
	samp := make([]byte, 600_000)
	for i := range samp {
		if i%2 == 0 {
			samp[i] = 0xFF
		}
	}
	p := filepath.Join(dir, "data.bin")
	os.WriteFile(p, samp, 0o600)
	ref := CaptureRef{ID: "big", Dir: dir, DataPath: p,
		Meta: Meta{Mode: "samples", RateHz: 40_000_000, T0NS: 9}}
	got, err := ExportVCD(ref)
	if err != nil {
		t.Fatalf("big export: %v", err)
	}
	if !got.Truncated || got.Changes != 1<<22 {
		t.Fatalf("changes = %d trunc %v, want exactly 2^22 / true", got.Changes, got.Truncated)
	}
	if fi, _ := os.Stat(got.Path); fi.Size() != got.Bytes {
		t.Fatalf("bytes = %d, stat %d", got.Bytes, fi.Size())
	}
}

func TestExportVCDRefusals(t *testing.T) {
	// listen refuses: VCD is the level domain
	mod := fixtureRef(t, "modbus")
	if _, err := ExportVCD(mod); err == nil || !strings.Contains(err.Error(), "LEVEL") {
		t.Errorf("listen: err = %v, want the LEVEL coach", err)
	}
	// empty level capture refuses
	dir := t.TempDir()
	p := filepath.Join(dir, "data.bin")
	os.WriteFile(p, nil, 0o600)
	ref := CaptureRef{ID: "empty", Dir: dir, DataPath: p,
		Meta: Meta{Mode: "samples", RateHz: 1_000_000, T0NS: 9}}
	if _, err := ExportVCD(ref); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty: err = %v, want the empty coach", err)
	}
	// a ref without an object directory refuses (the store always
	// hands one; the library does not guess a location)
	noDir := fixtureRef(t, "edgepwm")
	noDir.Dir = ""
	if _, err := ExportVCD(noDir); err == nil || !strings.Contains(err.Error(), "object directory") {
		t.Errorf("no dir: err = %v, want the directory coach", err)
	}
}

// indexOf is the lines helper (first hit, −1 absent).
func indexOf(lines []string, want string) int {
	for i, l := range lines {
		if l == want {
			return i
		}
	}
	return -1
}
