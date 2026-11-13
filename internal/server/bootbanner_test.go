package server

// bootbanner_test.go — B16-T6 D1: the boot-banner death classifier's
// deterministic layers (spec §4D). Real samples are transcribed from the
// bench's own evidence corpus (raw previews captured 2026-11-08 + the
// CDC-frame corruption diagnostics of 2026-11-12; the private mgmt repo
// is the canonical evidence home — this public repo carries none) — the
// corruption shapes are the bench's own history, not inventions.

import (
	"strings"
	"testing"
)

// realBannerBoard1 is a real boot banner tail (board #1 d75884, the CDC
// open's reboot; b10-t8-diag-e1 w2 sample, efuse line from the current
// build's own banner).
const realBannerBoard1 = "ESP-ROM:esp32s3-20210327\r\n" +
	"Build:Mar 27 2021\r\n" +
	"rst:0x15 (USB_UART_CHIP_RESET),boot:0x8 (SPI_FAST_FLASH_BOOT)\r\n" +
	"Saved PC:0x40374283\r\n" +
	"SPIWP:0xee\r\n" +
	"mode:DIO, clock div:1\r\n" +
	"load:0x3fce2820,len:0x14f0\r\n" +
	"load:0x403c8700,len:0xd24\r\n" +
	"load:0x403cb700,len:0x2eec\r\n" +
	"entry 0x403c8924\r\n" +
	"I (24) boot: ESP-IDF v5.5.5 2nd stage bootloader\r\n" +
	"I (24) boot: compile time Nov  8 2026 02:38:20\r\n" +
	"I (25) boot: Multicore bootloader\r\n" +
	"I (25) boot: chip revision: v0.2\r\n" +
	"I (27) boot: efuse block revision: v1.3\r\n" +
	"I (31) boot.esp32s3: Boot SPI Speed : 80MHz\r\n" +
	"I (35) boot.esp32s3: SPI Mode       : DIO\r\n" +
	"I (39) boot.esp32s3: SPI Flash Size : 16MB\r\n" +
	"I (43) boot: Enabling RNG early entropy source...\r\n" +
	"I (47) boot: Partition Table:\r\n" +
	"I (50) boot: ## Label            Usage          Type ST Offset   Length\r\n"

// realCorruptTWDT: a REAL corruption shape (TWDT print torn mid-line,
// then the ROM banner glued after it — the drain window caught the panic
// print of the PREVIOUS session followed by this boot's banner).
const realCorruptTWDT = "E (207488) task_wdt: Task watchdog got triggered. The following " +
	realBannerBoard1

// realCorruptTornHead: a REAL corruption shape (the drain window opened
// late and cut the ROM prefix — the banner starts at Build:, no ESP-ROM
// line at all; b10-t8-diag q01/w3 sample shape).
var realCorruptTornHead = strings.TrimPrefix(realBannerBoard1, "ESP-ROM:esp32s3-20210327\r\n")

// realCorruptGlued: a REAL corruption shape (the F1 disease: a torn
// esp_image printf glued to the ROM banner's first line — the captured
// window starts mid-print; b10-t8-fix-breadcrumb crumb sample shape).
const realCorruptGlued = "I (351) esp_image: segment 3: paddr=001627c8 vaddr=3fca9484 size" +
	realBannerBoard1

func TestNormalizeBannerConstructiveFuzz(t *testing.T) {
	lines := NormalizeBanner([]byte(realBannerBoard1))
	if len(lines) == 0 {
		t.Fatal("NormalizeBanner: no lines")
	}
	// Digit runs collapse to # everywhere (times/addresses/hex/versions).
	for _, ln := range lines {
		if strings.ContainsAny(ln, "0123456789") {
			t.Errorf("normalized line still has digits: %q", ln)
		}
	}
	// Cause enumerations become the (<R>) placeholder.
	if lines[2] != "rst:#x# (<R>),boot:#x# (<R>)" {
		t.Errorf("rst line normalized = %q", lines[2])
	}
	// The ROM first line keeps its shape with ALL digit runs fuzzed —
	// including the family's own digits (identity lives in the T0
	// anchor over the RAW capture, not the skeleton).
	if lines[0] != "ESP-ROM:esp#s#-#" {
		t.Errorf("rom line normalized = %q", lines[0])
	}
}

func TestAnchorT0GenericShapeAndFamily(t *testing.T) {
	// The generic shape anchor: any ESP32 family + 8-digit ROM date.
	fam, rom, hit := AnchorT0([]byte(realBannerBoard1))
	if !hit || fam != "esp32s3" || rom != "20210327" {
		t.Fatalf("AnchorT0 = %q %q %v", fam, rom, hit)
	}
	// Unknown future family still anchors (day-one determinism).
	fam, rom, hit = AnchorT0([]byte("ESP-ROM:esp32c6-20250102\r\nrst:0x1 (POWERON)\r\n"))
	if !hit || fam != "esp32c6" || rom != "20250102" {
		t.Fatalf("AnchorT0 future family = %q %q %v", fam, rom, hit)
	}
	// Offset-0 rule: a banner buried under junk does NOT anchor
	// (the T0 constant-anchor's whole point — soft corruption with a
	// transplanted ROM line deeper in the buffer must not pass T0).
	if _, _, hit = AnchorT0([]byte(realCorruptTWDT)); hit {
		t.Error("AnchorT0 must require the ROM prefix at offset 0")
	}
}

func TestSeedFamilyFleetGrowth(t *testing.T) {
	tb := NewFamilyTable()
	// Seeded from the fleet's real captures (both bench boards are
	// esp32s3-20210327).
	if _, ok := tb["esp32s3"]; !ok {
		t.Fatal("family table missing the seeded esp32s3 entry")
	}
	// First capture of an unseen family enters the table (fleet
	// knowledge grows without code changes).
	SeedFamily(tb, "esp32c6", "20250102")
	if tb["esp32c6"] != "20250102" {
		t.Fatalf("seeding did not take: %#v", tb)
	}
	// Re-capture of the same family with a DIFFERENT rev keeps the
	// first-seen value (determinism; per-board receipts carry the
	// precise rev anyway).
	SeedFamily(tb, "esp32c6", "20280101")
	if tb["esp32c6"] != "20250102" {
		t.Fatalf("seeding not idempotent-first: %#v", tb)
	}
}

func TestSkeletonMatchLCSAndFirstLine(t *testing.T) {
	base := Skeleton(NormalizeBanner([]byte(realBannerBoard1)))
	// Identical → match.
	if !SkeletonMatch(base, base, 0.8) {
		t.Fatal("identical skeletons must match")
	}
	// Line-subset tolerance: drop a few IDF lines (~10% of the set) —
	// still ≥80% LCS.
	cur := append([]string{}, base[:2]...)
	cur = append(cur, base[4:]...)
	if !SkeletonMatch(base, cur, 0.8) {
		t.Fatal("line-subset drift (a few missing lines) must match ≥80%")
	}
	// First-line exact match carries even a heavily drifted body
	// (IDF line-set churn across fw upgrades).
	drifted := append([]string{}, base[0], "totally new line", "another new line")
	if !SkeletonMatch(base, drifted, 0.8) {
		t.Fatal("first-line exact must match regardless of body drift")
	}
	// Wildly different skeleton → no match.
	if SkeletonMatch(base, Skeleton(NormalizeBanner([]byte("garbage\r\nlines\r\nonly\r\n"))), 0.8) {
		t.Fatal("unrelated skeletons must not match")
	}
}

func TestParseReceiptDeterministicColumns(t *testing.T) {
	r := ParseReceipt([]byte(realBannerBoard1))
	if !r.OK {
		t.Fatal("ParseReceipt: real banner must parse")
	}
	if r.ResetRaw != "0x15" {
		t.Errorf("reset_raw = %q", r.ResetRaw)
	}
	if r.RomVer != "20210327" {
		t.Errorf("rom_ver = %q", r.RomVer)
	}
	if r.Family != "esp32s3" {
		t.Errorf("family = %q, want esp32s3 (the anchor parsed)", r.Family)
	}
	if r.ChipRev != "0.2" {
		t.Errorf("chip_rev = %q", r.ChipRev)
	}
	// Ground-truth columns are regex-extracted ONLY — never an LLM path
	// (code-review red line; this test pins the parsers' outputs).
	if r.ResetRaw != "0x15" || r.ChipRev != "0.2" {
		t.Fatal("deterministic columns must be exact")
	}
	// Missing rst line → absent column, honest (no invention) — the
	// ROM anchor may still have parsed (OK covers ANY ground-truth
	// column; reset_raw itself stays empty).
	r2 := ParseReceipt([]byte("ESP-ROM:esp32s3-20210327\r\nnothing here\r\n"))
	if r2.ResetRaw != "" {
		t.Fatalf("absent rst must leave reset_raw empty: %+v", r2)
	}
}

func TestClassifyThreeTiers(t *testing.T) {
	base := Skeleton(NormalizeBanner([]byte(realBannerBoard1)))
	tb := NewFamilyTable()

	// T0: clean banner at offset 0.
	v := Classify([]byte(realBannerBoard1), [][]string{base}, tb)
	if v.Tier != "T0" || v.Kind != "banner" {
		t.Fatalf("clean banner = %s/%s", v.Tier, v.Kind)
	}
	if v.Receipt.ResetRaw != "0x15" {
		t.Errorf("receipt reset_raw = %q", v.Receipt.ResetRaw)
	}

	// T1: torn-head banner (no ROM line) still classifies as banner via
	// the skeleton — honest tier demotion, receipt columns still parse.
	v = Classify([]byte(realCorruptTornHead), [][]string{base}, tb)
	if v.Kind != "banner" || v.Tier != "T1" {
		t.Fatalf("torn head = %s/%s", v.Tier, v.Kind)
	}
	if v.Notes == "" {
		t.Error("torn-head banner must carry a drift note")
	}

	// T1: junk-glued banner (TWDT prefix / F1 glue) — skeleton carries.
	v = Classify([]byte(realCorruptTWDT), [][]string{base}, tb)
	if v.Kind != "banner" || v.Tier != "T1" {
		t.Fatalf("twdt-glued = %s/%s", v.Tier, v.Kind)
	}

	// No baseline + no anchor: corruption (T2 junk_text for the LLM
	// seat — daemon never calls out).
	v = Classify([]byte("qt\x00\x01\x02 not a banner at all\x7f\xff"), nil, tb)
	if v.Kind != "corruption" {
		t.Fatalf("junk = %s/%s", v.Tier, v.Kind)
	}
	if v.JunkText == "" {
		t.Error("corruption verdict must carry junk_text for the T2 seat")
	}
	if len(v.JunkText) > 600 {
		t.Errorf("junk_text bounded (~256B hex/ASCII side-by-side): %d chars", len(v.JunkText))
	}

	// First capture (no baseline) with a clean anchor: banner, honest
	// first-capture note (baseline-establishing, no drift claim).
	v = Classify([]byte(realBannerBoard1), nil, tb)
	if v.Kind != "banner" {
		t.Fatalf("first capture = %s/%s", v.Tier, v.Kind)
	}
	if !strings.Contains(v.Notes, "first capture") {
		t.Errorf("first-capture note = %q", v.Notes)
	}
}

func TestEpochArbitration(t *testing.T) {
	// +1 (or more — unseen reboots while away): confirmed.
	if got := ArbitrateEpoch(4, 5); got != "confirmed" {
		t.Errorf("+1 = %q", got)
	}
	if got := ArbitrateEpoch(4, 9); got != "confirmed" {
		t.Errorf("+N = %q", got)
	}
	// Unchanged: honest corruption downgrade (the capture window saw a
	// banner the board's own epoch counter disowns).
	if got := ArbitrateEpoch(5, 5); got != "corruption" {
		t.Errorf("unchanged = %q", got)
	}
	// No prior baseline: honest "baseline" (no claim either way).
	if got := ArbitrateEpoch(0, 5, true); got != "baseline" {
		t.Errorf("first sighting = %q", got)
	}
}

func TestChipCrossCheck(t *testing.T) {
	if !ChipMatch("esp32s3", "ESP32-S3") {
		t.Error("esp32s3 ↔ ESP32-S3 must cross-check")
	}
	if ChipMatch("esp32c3", "ESP32-S3") {
		t.Error("family/chip mismatch must be flagged")
	}
	if !ChipMatch("esp32s3", "") {
		t.Error("absent hello chip (pre-first-hello) is not a mismatch")
	}
}

// TestBootReceiptTriadLegsPersisted (fix-round Important-1): the T2
// triad's middle and baseline legs ride the receipt itself — a drifted
// capture persists the concrete machine-diff lines plus the baseline
// text they were taken against; a first capture carries neither
// (nothing to compare against, honestly absent).
func TestBootReceiptTriadLegsPersisted(t *testing.T) {
	dir := t.TempDir()
	rec := NewBootRecordStore(dir)

	// First capture: no baseline → diff/baseline absent.
	if err := rec.Record("d75884", []byte(realBannerBoard1), "confirmed"); err != nil {
		t.Fatalf("record#1: %v", err)
	}
	first := rec.Receipts("d75884")[0]
	if len(first.Diff) != 0 || len(first.Baseline) != 0 {
		t.Fatalf("first capture must carry no diff/baseline: %+v", first)
	}

	// Drifted second capture: a replaced IDF line + a new line (the
	// anchor head stays — drift, not corruption).
	drifted := strings.Replace(realBannerBoard1,
		"I (25) boot: Multicore bootloader\r\n",
		"I (25) boot: Single core bootloader\r\n", 1) +
		"I (99) boot: brand new boot line\r\n"
	if err := rec.Record("d75884", []byte(drifted), "confirmed"); err != nil {
		t.Fatalf("record#2: %v", err)
	}
	rs := rec.Receipts("d75884")
	second := rs[1]
	if len(second.Diff) == 0 {
		t.Fatal("drifted capture must persist the machine diff lines")
	}
	var plus, minus int
	for _, ln := range second.Diff {
		switch {
		case strings.HasPrefix(ln, "+ "):
			plus++
		case strings.HasPrefix(ln, "- "):
			minus++
		default:
			t.Errorf("diff line not +/- shaped: %q", ln)
		}
	}
	if plus < 2 || minus < 1 {
		t.Errorf("diff lines = %d+/%d- (want ≥2+/≥1-: the new line, the normalized new pair, the replaced line)", plus, minus)
	}
	if !containsStr(second.Diff, "- I (<R>) boot: Multicore bootloader") {
		t.Errorf("diff must name the replaced baseline line:\n%v", second.Diff)
	}
	if len(second.Baseline) == 0 {
		t.Fatal("drifted capture must persist the compared-against baseline text")
	}
	if !containsStr(second.Baseline, "I (<R>) boot: Multicore bootloader") {
		t.Errorf("baseline must be the PREVIOUS skeleton (with the old line):\n%v", second.Baseline)
	}
	// Durable: a reload answers the same triad legs.
	rec2 := NewBootRecordStore(dir)
	got := rec2.Receipts("d75884")[1]
	if len(got.Diff) != len(second.Diff) || len(got.Baseline) != len(second.Baseline) {
		t.Fatalf("triad legs not durable: diff %d vs %d, baseline %d vs %d",
			len(got.Diff), len(second.Diff), len(got.Baseline), len(second.Baseline))
	}
}

func containsStr(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func TestBootRecordPersistence(t *testing.T) {
	dir := t.TempDir()
	rec := NewBootRecordStore(dir)
	raw := []byte(realBannerBoard1)
	if err := rec.Record("d75884", raw, "confirmed"); err != nil {
		t.Fatalf("record: %v", err)
	}
	got := rec.Receipts("d75884")
	if len(got) != 1 || got[0].ResetRaw != "0x15" {
		t.Fatalf("receipts after one record: %+v", got)
	}
	// last-8 bounded.
	for k := 0; k < 12; k++ {
		if err := rec.Record("d75884", raw, "confirmed"); err != nil {
			t.Fatalf("record %d: %v", k, err)
		}
	}
	if n := len(rec.Receipts("d75884")); n != 8 {
		t.Fatalf("last-8 bound: %d", n)
	}
	// The adopted skeleton persists (the T1 baseline survives restarts).
	if sk := rec.Skeleton("d75884"); len(sk) == 0 {
		t.Fatal("skeleton baseline not persisted")
	}
	// A reload of the same directory sees the same history (durable
	// across daemon restarts — the in-context baseline supply).
	rec2 := NewBootRecordStore(dir)
	if n := len(rec2.Receipts("d75884")); n != 8 {
		t.Fatalf("durable receipts: %d", n)
	}
}
