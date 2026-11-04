// resync.go — link-loss reconciliation (owner ruling 2026-11-04): a
// daemon↔board link break does NOT tear anything down — board-side
// scripts keep running (fw behavior) — and when the daemon reconnects
// it reconciles by listing: script.list + diag.pins + wifi.status,
// diffed against the last known listing, reported HONESTLY (survived
// scripts are reported as survived, not killed). The same call at
// daemon startup, against an empty baseline, is the orphan audit:
// listed, never auto-killed (單人紀律 — the operator decides).
package board

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"xuansi/internal/replclient"
)

// PinClaim is one owned pin as diag.pins reports it (fw xs_pin_dump:
// "GPIO%-3d sess=%d owner=%s prim=%s mode=%s watch=%s").
type PinClaim struct {
	GPIO    int
	Session int
	Owner   string // "_eval" for bare eval claims, else the script name
	Prim    string // gpio/uart/i2c/spi/adc/pwm/rmt
	Mode    string
	Watch   string // rising/falling/both/none
}

// ResyncReport is the honest reconciliation result for one board.
type ResyncReport struct {
	Board     string
	Scripts   []string       // script.list verbatim, now
	Pins      []PinClaim     // diag.pins claims, now
	Wifi      map[string]any // wifi.status verbatim
	ColdStart bool           // true when diffed against an empty baseline (daemon start: every script listed is an orphan candidate — listed, not killed)
	Survived  []string       // ran before the break AND still present (fw keeps scripts through link loss)
	Started   []string       // present now, not in the baseline (not started by this daemon — reported as found)
	Gone      []string       // in the baseline, absent now (killed or died during the break)
	PinsNew   []PinClaim     // claimed since the baseline
	PinsGone  []PinClaim     // released since the baseline
}

var (
	// script.list wraps its listing: "scripts (n): a b c" (registry
	// single source xs_script.c; n counts, names space-separated).
	scriptListRe = regexp.MustCompile(`^scripts \((\d+)\):\s*(.*)$`)

	// diag.pins GPIO lines (leading spaces tolerated; chunk splits make
	// line matching per-line, not per-dump).
	pinLineRe = regexp.MustCompile(`GPIO(\d+)\s+sess=(-?\d+)\s+owner=(\S+)\s+prim=(\S+)\s+mode=(\S+)\s+watch=(\S+)`)

	// pins header declares the owned-pin count — the dump terminator.
	pinsHeaderRe = regexp.MustCompile(`pins \((\d+)\):`)
)

// parseScriptList extracts the script names; nil when the listing does
// not carry the canonical header (honest: no guess).
func parseScriptList(out string) []string {
	m := scriptListRe.FindStringSubmatch(strings.TrimSpace(out))
	if m == nil {
		return nil
	}
	rest := strings.TrimSpace(m[2])
	if rest == "" {
		return nil
	}
	return strings.Fields(rest)
}

// parsePinsDump extracts the GPIO claims from a diag.pins stdout dump
// (uart/spi/pwm/adc/rmt summary lines and the events-drop line are not
// pin claims; they are ignored here).
func parsePinsDump(text string) []PinClaim {
	var claims []PinClaim
	for _, m := range pinLineRe.FindAllStringSubmatch(text, -1) {
		gpio, _ := strconv.Atoi(m[1])
		sess, _ := strconv.Atoi(m[2])
		claims = append(claims, PinClaim{
			GPIO: gpio, Session: sess, Owner: m[3], Prim: m[4], Mode: m[5], Watch: m[6],
		})
	}
	return claims
}

// pinsHeaderCount returns the dump's declared owned-pin count, -1 when
// no header has arrived yet (collection continues).
func pinsHeaderCount(text string) int {
	if m := pinsHeaderRe.FindStringSubmatch(text); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	return -1
}

// ScriptList runs script.list and returns the parsed names.
func (s *Supervisor) ScriptList(ctx context.Context) ([]string, error) {
	res, err := s.op(ctx, "script.list", nil)
	if err != nil {
		return nil, err
	}
	out, _ := res["out"].(string)
	return parseScriptList(out), nil
}

// DiagPins collects the diag.pins stdout dump: the collector subscribes
// before the trigger (stdout-first HIL convention), the ack lands
// first, chunks follow; collection ends when the dump's own header
// count is satisfied (protocol-grounded terminator — "pins (n):" counts
// the GPIO claim lines that follow), with the Quiet window as fallback.
// Returns the raw text and the parsed claims.
func (s *Supervisor) DiagPins(ctx context.Context) (string, []PinClaim, error) {
	ctl, err := s.Connect(ctx)
	if err != nil {
		return "", nil, err
	}

	// collector BEFORE the trigger: no chunk can predate the
	// subscription, so nothing is missed
	ch := make(chan string, 16)
	cancel := ctl.OnEvent("stdout", func(e replclient.Event) {
		if d, ok := e.Data["data"].(string); ok {
			select {
			case ch <- d:
			default: // dump larger than the buffer: the terminator ends
				// this long before 16 chunks; overflow only on garbage
			}
		}
	})
	defer cancel()

	// open the board-side gate (idempotent per connection), then trigger
	if _, err := ctl.Op(ctx, "events.subscribe", map[string]any{"source": "stdout"}); err != nil {
		return "", nil, fmt.Errorf("board %s: stdout subscribe: %w", s.ref.MAC6, err)
	}
	if _, err := ctl.Op(ctx, "diag.pins", nil); err != nil {
		return "", nil, fmt.Errorf("board %s: diag.pins: %w", s.ref.MAC6, err)
	}

	var raw []byte
	quiet := time.NewTimer(s.opts.Quiet)
	defer quiet.Stop()
	for {
		// drain whatever already arrived, checking the terminator
		for {
			select {
			case d := <-ch:
				raw = append(raw, d...)
			default:
				goto wait
			}
		}
	wait:
		text := string(raw)
		if n := pinsHeaderCount(text); n >= 0 && len(parsePinsDump(text)) >= n {
			return text, parsePinsDump(text), nil
		}
		select {
		case d := <-ch:
			raw = append(raw, d...)
			quiet.Reset(s.opts.Quiet)
		case <-quiet.C:
			// fallback: report what arrived (honest — a dump that never
			// satisfied its own header is truncated in the trace)
			return text, parsePinsDump(text), nil
		case <-ctx.Done():
			return text, parsePinsDump(text), ctx.Err()
		}
	}
}

// Resync reconciles the board: (re)connect if the link is down, then
// script.list + diag.pins + wifi.status, diffed against the last known
// listing (the baseline becomes this listing). The first call on a
// fresh Supervisor diffs against nothing — the daemon-startup orphan
// audit (ColdStart=true, nothing killed).
func (s *Supervisor) Resync(ctx context.Context) (ResyncReport, error) {
	scripts, err := s.ScriptList(ctx)
	if err != nil {
		return ResyncReport{}, err
	}
	rawPins, pins, err := s.DiagPins(ctx)
	if err != nil {
		return ResyncReport{}, err
	}
	wifi, err := s.op(ctx, "wifi.status", nil)
	if err != nil {
		return ResyncReport{}, err
	}

	s.mu.Lock()
	cold := !s.haveBase
	var survived, started, gone []string
	var pinsNew, pinsGone []PinClaim
	if !cold {
		// diff only against a real baseline — a cold start labels
		// nothing "started": the raw listing is the orphan report
		survived, started, gone = diffStrings(s.base.scripts, scripts)
		_, pinsNew, pinsGone = diffPins(s.base.pins, pins)
	}
	s.base = baseline{scripts: scripts, pins: pins}
	s.haveBase = true
	s.mu.Unlock()

	rep := ResyncReport{
		Board: s.ref.MAC6, Scripts: scripts, Pins: pins, Wifi: wifi,
		ColdStart: cold, Survived: survived, Started: started, Gone: gone,
		PinsNew: pinsNew, PinsGone: pinsGone,
	}
	s.logf("board %s: resync scripts=%v survived=%v started=%v gone=%v pins=%d (cold=%v) wifi_state=%v [diag.pins raw %dB]",
		s.ref.MAC6, scripts, survived, started, gone, len(pins), cold, wifi["state"], len(rawPins))
	return rep, nil
}

// diffStrings returns (both, onlyInB, onlyInA) name-ordered.
func diffStrings(a, b []string) (both, onlyB, onlyA []string) {
	as := map[string]bool{}
	for _, x := range a {
		as[x] = true
	}
	bs := map[string]bool{}
	for _, x := range b {
		bs[x] = true
	}
	for _, x := range a {
		if bs[x] {
			both = append(both, x)
		} else {
			onlyA = append(onlyA, x)
		}
	}
	for _, x := range b {
		if !as[x] {
			onlyB = append(onlyB, x)
		}
	}
	sort.Strings(both)
	sort.Strings(onlyA)
	sort.Strings(onlyB)
	return both, onlyB, onlyA
}

// pinKey identifies a claim across baselines: GPIO+Session. The pin
// slot is owned by one session at a time (fw one-owner rule), so the
// pair is stable while owner/prim/mode/watch are attributes that may
// legitimately differ between observations.
func pinKey(p PinClaim) string {
	return fmt.Sprintf("%d/%d", p.GPIO, p.Session)
}

// diffPins is diffStrings over pin identity (GPIO+session+owner).
func diffPins(a, b []PinClaim) (both, onlyB, onlyA []PinClaim) {
	ak := map[string]PinClaim{}
	for _, p := range a {
		ak[pinKey(p)] = p
	}
	bk := map[string]PinClaim{}
	for _, p := range b {
		bk[pinKey(p)] = p
	}
	for _, p := range a {
		if _, ok := bk[pinKey(p)]; ok {
			both = append(both, p)
		} else {
			onlyA = append(onlyA, p)
		}
	}
	for _, p := range b {
		if _, ok := ak[pinKey(p)]; !ok {
			onlyB = append(onlyB, p)
		}
	}
	return both, onlyB, onlyA
}
