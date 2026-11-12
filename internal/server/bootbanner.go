package server

// bootbanner.go — B16-T6 D1: the boot-banner death classifier + receipt
// bookkeeping (spec §4D). The CDC open's reboot makes the drain window's
// tail NECESSARILY this boot's banner (the zero-false-positive capture
// source) — that capture feeds three deterministic layers plus one LLM
// seat input; the daemon itself never calls out.
//
// Layers (spec §4D vocabulary):
//   T0 — the ROM-prefix offset-0 constant anchor (soft corruption with a
//        transplanted ROM line deeper in the buffer does NOT pass): one
//        generic shape regex covering every ESP32 family including
//        future silicon, a small known-family table for chip identity,
//        and capture seeding (an unseen family's first capture enters
//        the table — fleet knowledge grows without code changes).
//   T1 — the deterministic skeleton (format-agnostic structural fuzz):
//        every digit run collapses to '#', parenthesized cause
//        enumerations to '(<R>)', then a line-subset LCS match (≥80%)
//        against the per-board baseline history — or a first-line exact
//        match — absorbs IDF line-set drift across fw upgrades. Covers
//        the soft-reboot classes whose banners carry no ROM prefix.
//   T2 — bounded junk_text (~256B hex/ASCII side-by-side) for the agent
//        (LLM) seat: the triad input the daemon assembles is baseline
//        history + a machine diff + the anomaly body. The daemon decides
//        nothing beyond the deterministic tiers.
//
// Backstop — epoch arbitration (never wrong, mechanical): the post-open
// hello's boot_epoch vs the pre-open sighting confirms or refutes any
// verdict (+1 = the open's reboot is real; unchanged = honest corruption
// downgrade — the window's "banner" is something the board disowns).
//
// Receipts (free boot intelligence, structured columns): reset_raw (the
// ROM register's raw rst:0xNN — the hard-reset classification ground
// truth, complementary to hello's IDF-classified reset_reason, NVS-free)
// + rom_ver + chip_rev (board-level hardware identity) + the skeleton
// line-set/timing drift signal. Receipt parsing is deterministic
// regex ONLY — the ground-truth columns never pass through any LLM path
// (the review red line; the tests pin the parsers byte-for-byte).

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// normalization (the T1 skeleton form)

// digitRun collapses every decimal run; parenGroup collapses balanced
// parenthesized groups (cause enumerations: "(USB_UART_CHIP_RESET)",
// "(SPI_FAST_FLASH_BOOT)") to the placeholder.
var (
	digitRun  = regexp.MustCompile(`[0-9]+`)
	parenOpen = regexp.MustCompile(`\(([^()]*)\)`)
)

const parenPlaceholder = "(<R>)"

// NormalizeBanner reduces a raw capture to its skeleton lines: CRLF/CR
// split, per line digit-runs→'#' and parenthesized groups→'(<R>)',
// outer whitespace trimmed, empty lines dropped. Format-agnostic by
// construction — it does not need to know WHICH fields vary.
func NormalizeBanner(raw []byte) []string {
	s := strings.ReplaceAll(string(raw), "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	var out []string
	for _, ln := range strings.Split(s, "\n") {
		ln = parenOpen.ReplaceAllString(ln, parenPlaceholder)
		ln = digitRun.ReplaceAllString(ln, "#")
		ln = strings.TrimSpace(ln)
		if ln != "" {
			out = append(out, ln)
		}
	}
	return out
}

// Skeleton is the T1 comparison form — the normalized lines as-is (the
// name exists for the seat's vocabulary; normalization IS the skeleton).
func Skeleton(lines []string) []string { return lines }

// SkeletonMatch answers whether cur matches the baseline under the
// frozen rule: a line-subset LCS of ≥ threshold of the BASELINE's line
// count, or an exact first-line match (the ROM/Build head is stable even
// when IDF churns the body). cur may carry extra leading junk (the
// torn-print glue shapes) — LCS counts the baseline's lines present in
// cur, order-confirmed.
func SkeletonMatch(base, cur []string, threshold float64) bool {
	if len(base) == 0 || len(cur) == 0 {
		return false
	}
	if base[0] == cur[0] {
		return true
	}
	lcs := longestCommonSubsequence(base, cur)
	return float64(lcs)/float64(len(base)) >= threshold
}

func longestCommonSubsequence(a, b []string) int {
	// Banner line sets are tens of lines; the classic DP table is fine.
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			if a[i-1] == b[j-1] {
				cur[j] = prev[j-1] + 1
			} else if prev[j] >= cur[j-1] {
				cur[j] = prev[j]
			} else {
				cur[j] = cur[j-1]
			}
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

// ---------------------------------------------------------------------------
// T0 — the ROM anchor

// romAnchor matches the ROM banner's first line AT OFFSET 0:
// "ESP-ROM:<family>-<8-digit date>". One generic shape covers the whole
// ESP32 family space including future silicon (esp32c6, esp32h2, …).
var romAnchor = regexp.MustCompile(`\AESP-ROM:(esp32[a-z0-9]*)-(\d{8})`)

// AnchorT0 reports the capture's ROM family and rev when the ROM prefix
// sits at offset 0 (the constant-anchor position). A banner buried under
// junk (torn prints glued ahead of it — the bench's real corruption
// history) does NOT anchor: that is the entire point of T0.
func AnchorT0(raw []byte) (family, romVer string, hit bool) {
	m := romAnchor.FindSubmatch(raw)
	if m == nil {
		return "", "", false
	}
	return string(m[1]), string(m[2]), true
}

// seedFamilies is the known-family table's initial content: ROM revs
// this fleet has actually captured (both bench boards are
// esp32s3-20210327 — transcribed from the fw evidence corpus). The table
// grows by capture seeding (SeedFamily) without code changes.
var seedFamilies = map[string]string{
	"esp32s3": "20210327",
}

// NewFamilyTable returns the known-family table (seeded copy — callers
// own their instance).
func NewFamilyTable() map[string]string {
	tb := make(map[string]string, len(seedFamilies))
	for k, v := range seedFamilies {
		tb[k] = v
	}
	return tb
}

// SeedFamily enters an unseen family's first capture into the table.
// Idempotent-first: a later capture of the same family with a different
// rev does not overwrite (determinism; per-board receipts carry the
// precise rev regardless).
func SeedFamily(tb map[string]string, family, romVer string) {
	if family == "" || romVer == "" {
		return
	}
	if _, ok := tb[family]; ok {
		return
	}
	tb[family] = romVer
}

// ---------------------------------------------------------------------------
// receipts — deterministic columns (regex ONLY; zero LLM paths)

var (
	rstRawRe  = regexp.MustCompile(`rst:0x([0-9A-Fa-f]+)`)
	chipRevRe = regexp.MustCompile(`chip revision:\s*v([0-9]+\.[0-9]+)`)
)

// BootReceipt is one captured banner's structured columns (the seat's
// ground-truth face). ResetRaw is the ROM register's raw value ("0x15")
// — the hard-reset classification truth, complementary to hello's
// IDF-classified reset_reason; RomVer/ChipRev are the board-level
// hardware identity the hello's single chip word does not carry.
type BootReceipt struct {
	At        string `json:"at"`                  // capture wall time (RFC3339)
	ResetRaw  string `json:"reset_raw"`           // rst:0xNN raw register value ("" honest-absent)
	RomVer    string `json:"rom_ver"`             // ESP-ROM rev ("20210327")
	Family    string `json:"family"`              // anchor family ("esp32s3")
	ChipRev   string `json:"chip_rev"`            // "0.2" ("" honest-absent)
	Kind      string `json:"kind"`                // banner | corruption
	Tier      string `json:"tier"`                // T0 | T1 | T2
	Arbiter   string `json:"arbiter"`             // confirmed | corruption | baseline | ""
	Notes     string `json:"notes"`               // drift/first-capture honest notes
	SkeletonN int    `json:"skeleton_n"`          // adopted skeleton line count
	DriftN    int    `json:"drift_n"`             // lines added/missing vs previous baseline
	JunkText  string `json:"junk_text,omitempty"` // corruption: bounded payload for the LLM seat
	OK        bool   `json:"-"`                   // any ground-truth column parsed
}

// ParseReceipt extracts the deterministic columns from a raw capture.
// OK is false when not even the anchor nor the rst line is present —
// the caller still records the capture (corruption), it just carries no
// ground-truth columns to invent.
func ParseReceipt(raw []byte) (r BootReceipt) {
	if m := rstRawRe.FindSubmatch(raw); m != nil {
		r.ResetRaw = "0x" + strings.ToLower(string(m[1]))
	}
	if fam, rom, hit := AnchorT0(raw); hit {
		r.Family, r.RomVer = fam, rom
	}
	if m := chipRevRe.FindSubmatch(raw); m != nil {
		r.ChipRev = string(m[1])
	}
	r.OK = r.ResetRaw != "" || r.RomVer != ""
	return r
}

// ---------------------------------------------------------------------------
// T2 — bounded junk payload (the LLM seat input)

// JunkText renders the capture's head as ~256B hex/ASCII side-by-side —
// bounded, printable, copy-pasteable into an agent conversation. The
// daemon ships it verbatim; interpreting it is the LLM seat's job.
func JunkText(raw []byte) string {
	const n = 256
	head := raw
	if len(head) > n {
		head = head[:n]
	}
	var hexs []string
	for _, b := range head {
		hexs = append(hexs, fmt.Sprintf("%02x", b))
	}
	ascii := make([]byte, len(head))
	for i, b := range head {
		if b >= 0x20 && b < 0x7f {
			ascii[i] = b
		} else {
			ascii[i] = '.'
		}
	}
	return fmt.Sprintf("hex:   %s\nascii: %s", strings.Join(hexs, " "), ascii)
}

// ---------------------------------------------------------------------------
// the classifier

// Verdict is a capture's classification plus everything the T2 triad
// input needs (baseline history + machine diff + anomaly body).
type Verdict struct {
	Kind     string // banner | corruption
	Tier     string // T0 | T1 | T2
	Notes    string
	Receipt  BootReceipt
	JunkText string
	Diff     []string // machine diff vs the previous baseline (new/missing lines)
}

// Classify runs the three deterministic tiers over a capture.
// baseline is the per-board adopted skeleton history (nil/empty on the
// first capture — the verdict then honestly says "first capture").
func Classify(raw []byte, baseline [][]string, tb map[string]string) Verdict {
	v := Verdict{Receipt: ParseReceipt(raw)}
	cur := NormalizeBanner(raw)

	_, _, anchored := AnchorT0(raw)
	switch {
	case anchored && len(baseline) > 0:
		v.Tier, v.Kind = "T0", "banner"
		SeedFamily(tb, v.Receipt.Family, v.Receipt.RomVer)
		if !SkeletonMatch(baseline[len(baseline)-1], cur, 0.8) {
			v.Notes = "skeleton drift (anchor held; line set changed — adopting)"
		}
	case anchored:
		v.Tier, v.Kind = "T0", "banner"
		v.Notes = "first capture (baseline established)"
		SeedFamily(tb, v.Receipt.Family, v.Receipt.RomVer)
	case len(baseline) > 0 && SkeletonMatch(baseline[len(baseline)-1], cur, 0.8):
		v.Tier, v.Kind = "T1", "banner"
		v.Notes = "soft-reboot class (no ROM prefix; skeleton carried)"
		if !strings.HasPrefix(string(raw), "ESP-ROM:") {
			if v.Notes == "" || !strings.Contains(v.Notes, "head") {
				v.Notes += " — torn/junk head"
			}
		}
	default:
		v.Tier, v.Kind = "T2", "corruption"
		v.JunkText = JunkText(raw)
		v.Notes = "no anchor, no skeleton match"
	}

	// The machine diff vs the previous baseline (T2's second triad leg).
	if len(baseline) > 0 {
		v.Diff = lineDiff(baseline[len(baseline)-1], cur)
	}
	return v
}

// lineDiff lists added/missing lines vs base (bounded to the first 16
// each way — the machine finds the differences, the LLM reads them).
func lineDiff(base, cur []string) []string {
	baseSet := make(map[string]int, len(base))
	for _, l := range base {
		baseSet[l]++
	}
	curSet := make(map[string]int, len(cur))
	for _, l := range cur {
		curSet[l]++
	}
	var out []string
	add, miss := 0, 0
	for _, l := range cur {
		if curSet[l] > baseSet[l] && add < 16 {
			out = append(out, "+ "+l)
			add++
			curSet[l] = baseSet[l] // count each surplus once
		}
	}
	for _, l := range base {
		if baseSet[l] > curSet[l] && miss < 16 {
			out = append(out, "- "+l)
			miss++
			baseSet[l] = curSet[l]
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// the epoch backstop

// ArbitrateEpoch is the mechanical never-wrong backstop: prev is the
// last pre-open sighting's boot_epoch, now the post-open hello's.
// noBaseline marks a first-ever sighting (no claim either way).
func ArbitrateEpoch(prev, now uint32, noBaseline ...bool) string {
	if len(noBaseline) > 0 && noBaseline[0] {
		return "baseline"
	}
	switch {
	case now > prev:
		return "confirmed" // the open's reboot is real (+1 exactly, or +N unseen boots)
	case now == prev:
		return "corruption" // the board's own counter disowns the capture
	default:
		return "corruption" // monotonic counter went backwards — disown
	}
}

// ChipMatch cross-checks the anchor family against hello's chip word
// (the anomaly signal of spec §4D): "esp32s3" ↔ "ESP32-S3". An absent
// chip word (pre-first-hello) is not a mismatch — nothing to compare.
func ChipMatch(family, helloChip string) bool {
	if helloChip == "" || family == "" {
		return true
	}
	norm := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(helloChip, "-", ""), "_", ""))
	return strings.Contains(norm, family)
}

// ---------------------------------------------------------------------------
// the per-board record (persistence: last-8 + the adopted skeleton)

// bootRecordFile is one board's durable record: the last-8 receipts and
// the currently adopted skeleton baseline (the in-context baseline
// supply that survives daemon restarts — spec §4D "基準優先判讀").
type bootRecordFile struct {
	Schema   int            `json:"schema"`
	Receipts []*BootReceipt `json:"receipts"`
	Skeleton []string       `json:"skeleton"`
}

// BootRecordStore is the daemon-wide receipt book. Dir is the state
// directory (paths.StateDir()/boards); each board's file lives at
// <dir>/boot-<mac6>.json, written atomically (tmp+rename).
type BootRecordStore struct {
	dir string

	mu       sync.Mutex
	cache    map[string]*bootRecordFile
	families map[string]string
}

func NewBootRecordStore(dir string) *BootRecordStore {
	return &BootRecordStore{dir: dir, cache: map[string]*bootRecordFile{}}
}

func (s *BootRecordStore) path(mac6 string) string {
	return filepath.Join(s.dir, fmt.Sprintf("boot-%s.json", mac6))
}

func (s *BootRecordStore) load(mac6 string) *bootRecordFile {
	if f, ok := s.cache[mac6]; ok {
		return f
	}
	f := &bootRecordFile{Schema: 1}
	if b, err := os.ReadFile(s.path(mac6)); err == nil {
		var g bootRecordFile
		if json.Unmarshal(b, &g) == nil && g.Schema == 1 {
			f = &g
		}
	}
	s.cache[mac6] = f
	return f
}

func (s *BootRecordStore) saveLocked(mac6 string, f *bootRecordFile) error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(f, "", " ")
	if err != nil {
		return err
	}
	tmp := s.path(mac6) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(mac6))
}

// Record is the full pipeline face: classify against the board's stored
// baseline + the daemon's family table, then persist the verdict receipt
// (bounded last-8; banner verdicts adopt the capture's skeleton — drift
// adoption). arbiter is the epoch verdict (confirmed/corruption/baseline).
func (s *BootRecordStore) Record(mac6 string, raw []byte, arbiter string) error {
	v := Classify(raw, s.Baseline(mac6), s.Families())
	return s.recordVerdict(mac6, v, raw, arbiter)
}

// recordVerdict persists a Classify verdict (receipt columns + tier +
// kind + junk) with drift adoption for banner verdicts.
func (s *BootRecordStore) recordVerdict(mac6 string, v Verdict, raw []byte, arbiter string) error {
	r := v.Receipt
	r.At = nowRFC3339()
	r.Kind, r.Tier, r.Arbiter, r.Notes = v.Kind, v.Tier, arbiter, v.Notes
	r.JunkText = v.JunkText
	cur := NormalizeBanner(raw)
	r.SkeletonN = len(cur)
	r.DriftN = len(v.Diff)
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.load(mac6)
	f.Receipts = append(f.Receipts, &r)
	if len(f.Receipts) > 8 {
		f.Receipts = f.Receipts[len(f.Receipts)-8:]
	}
	if v.Kind == "banner" {
		f.Skeleton = cur
	}
	return s.saveLocked(mac6, f)
}

// Receipts returns the board's last-8 (oldest→newest).
func (s *BootRecordStore) Receipts(mac6 string) []*BootReceipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.load(mac6)
	out := make([]*BootReceipt, len(f.Receipts))
	copy(out, f.Receipts)
	return out
}

// Skeleton returns the board's adopted baseline (nil before the first
// banner capture).
func (s *BootRecordStore) Skeleton(mac6 string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.load(mac6).Skeleton...)
}

// Baseline returns the classifier's baseline history form — the single
// adopted skeleton wrapped as the history slice (the frozen match rule
// compares against the newest baseline).
func (s *BootRecordStore) Baseline(mac6 string) [][]string {
	if sk := s.Skeleton(mac6); len(sk) > 0 {
		return [][]string{sk}
	}
	return nil
}

// Families returns the daemon's family table (seeded; grown by capture
// seeding via Classify). One shared table per store — fleet knowledge.
func (s *BootRecordStore) Families() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.families == nil {
		s.families = NewFamilyTable()
	}
	return s.families
}

// nowRFC3339 is a seam (tests could inject; production uses time.Now).
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
