package creds

// provision_test.go — the zero-ritual provisioner's boardless face
// (憑證零儀式波 T2): the provision_state derivation table, the
// GUARD-HARD zero-contact proofs (boards WITH credentials receive zero
// dials — the dial-counter discipline of the ota wave's
// TestPushZeroContactRefusals), the vault-empty block, and the happy
// path's op sequence over a mock board (wifi.setup THEN sec.psk_write,
// materials never in any log line). All credentials/PSKs are dummy.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/replclient"
)

// ---------------------------------------------------------------------------
// mock board + counting source
// ---------------------------------------------------------------------------

// mockBoard scripts the provisioning-relevant board face: what
// wifi.status answers (ssid/state), what state wifi.setup lands in,
// per-op scripted failures, and a post-op hook for timed transitions.
type mockBoard struct {
	mu      sync.Mutex
	ops     []string         // op names in call order
	args    []map[string]any // each call's args, same order
	wifiSS  string           // wifi.status's ssid answer
	wifiST  string           // wifi.status's state answer
	postSet string           // state wifi.setup lands the board in
	fail    map[string]error // one-shot scripted failures
	hellos  int
	onOp    func(op string) // post-canned-handler hook
}

func newMockBoard() *mockBoard {
	return &mockBoard{wifiST: "off", postSet: "connecting", fail: map[string]error{}}
}

func (m *mockBoard) Op(_ context.Context, op string, args map[string]any) (map[string]any, error) {
	m.mu.Lock()
	a := map[string]any{}
	for k, v := range args {
		a[k] = v
	}
	m.ops = append(m.ops, op)
	m.args = append(m.args, a)
	st, ss := m.wifiST, m.wifiSS
	err := m.fail[op]
	if err != nil {
		delete(m.fail, op)
	}
	hook := m.onOp
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	var res map[string]any
	switch op {
	case "wifi.status":
		res = map[string]any{"state": st, "ssid": ss}
		if st == "up" {
			res["ip"] = "10.1.51.191"
			res["port"] = int64(7777)
		}
	case "wifi.setup":
		m.mu.Lock()
		m.wifiSS, _ = args["ssid"].(string)
		m.wifiST = m.postSet
		ps := m.postSet
		m.mu.Unlock()
		res = map[string]any{"state": ps}
	case "sec.psk_write":
		res = map[string]any{"stored": true, "bytes": int64(32)}
	default:
		res = map[string]any{}
	}
	if hook != nil {
		hook(op)
	}
	return res, nil
}

func (m *mockBoard) Hello(_ bool, _ string) (*replclient.HelloInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hellos++
	return &replclient.HelloInfo{Role: "controller", ProtocolVer: 10, FwBuild: "dummy123",
		Net: map[string]any{"state": m.wifiST, "ssid": m.wifiSS}}, nil
}

func (m *mockBoard) called(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, o := range m.ops {
		if o == op {
			n++
		}
	}
	return n
}

func (m *mockBoard) opsOf() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.ops))
	copy(out, m.ops)
	return out
}

func (m *mockBoard) argOf(op string) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, o := range m.ops {
		if o == op {
			return m.args[i]
		}
	}
	return nil
}

// mockSource counts dials per board — the zero-contact proof's counter.
type mockSource struct {
	mu     sync.Mutex
	boards map[string]*mockBoard
	dials  map[string]int
}

func newMockSource(mb *mockBoard) *mockSource {
	return &mockSource{boards: map[string]*mockBoard{"board00": mb}, dials: map[string]int{}}
}

func (s *mockSource) USB(_ context.Context, mac6 string) (Board, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dials[mac6]++
	b, ok := s.boards[mac6]
	if !ok {
		return nil, fmt.Errorf("no mock board %s", mac6)
	}
	return b, nil
}

func (s *mockSource) dialsOf(mac6 string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dials[mac6]
}

// harness wires a Provisioner's Deps around one source/vault pair and
// captures the log + event seams (the no-material vector reads these).
type harness struct {
	src    *mockSource
	vault  Vault
	mu     sync.Mutex
	logs   []string
	events []string
	pskGen int
}

func newHarness(src *mockSource, v Vault) *harness {
	return &harness{src: src, vault: v}
}

func (h *harness) logf(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, fmt.Sprintf(format, args...))
}

func (h *harness) logText() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.logs, "\n")
}

func (h *harness) firedEvents() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.events))
	copy(out, h.events)
	return out
}

func (h *harness) deps() Deps {
	return Deps{
		Source: h.src,
		Vault: func() (Vault, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.vault, nil
		},
		PSK: func() (string, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.pskGen++
			return strings.Repeat("cd", 32), nil // dummy 256-bit hex
		},
		Logf: h.logf,
		Emit: func(mac6, ssid, ip string) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.events = append(h.events, mac6+" "+ssid+" "+ip)
		},
		AssocWait: 2 * time.Second,
		Poll:      5 * time.Millisecond,
	}
}

// settleUp flips the mock to state up d after the afterOp crossing
// (the association's asynchronous landing — the wait provably polls
// instead of succeeding first-probe lucky).
func (m *mockBoard) settleUp(afterOp string, d time.Duration) {
	var once sync.Once
	m.mu.Lock()
	m.onOp = func(op string) {
		if op != afterOp {
			return
		}
		once.Do(func() {
			go func() {
				time.Sleep(d)
				m.mu.Lock()
				m.wifiST = "up"
				m.mu.Unlock()
			}()
		})
	}
	m.mu.Unlock()
}

// ---------------------------------------------------------------------------
// provision_state derivation
// ---------------------------------------------------------------------------

func TestStateOf(t *testing.T) {
	cases := []struct {
		name  string
		sight Sight
		vault bool
		want  State
	}{
		{"wifi live is ok", Sight{Sighted: true, WifiLive: true}, false, StateOK},
		{"sighted with ssid is ok", Sight{Sighted: true, SSID: "some-ssid"}, false, StateOK},
		{"credless + vault wifi = credless", Sight{Sighted: true}, true, StateCredless},
		{"credless + empty vault = vault-empty", Sight{Sighted: true}, false, StateVaultEmpty},
		{"never sighted = no state (honest absence)", Sight{}, false, ""},
		{"never sighted even with vault = no state", Sight{}, true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := StateOf(c.sight, c.vault); got != c.want {
				t.Fatalf("StateOf(%+v, vault=%v) = %q, want %q", c.sight, c.vault, got, c.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GUARD HARD: cred-present boards = zero contact
// ---------------------------------------------------------------------------

func TestConsiderZeroContactCredPresent(t *testing.T) {
	mb := newMockBoard()
	src := newMockSource(mb)
	h := newHarness(src, Vault{WiFiSSID: dummySSID, WiFiPSK: dummyPSK})
	p := NewProvisioner(h.deps())

	// a sighting that shows credentials — the daemon KNOWS the board is
	// provisioned; touching it (a CDC open reboots it!) is forbidden.
	if out := p.Consider(context.Background(), "board00", Sight{Sighted: true, SSID: "already-set"}); out != OutcomeSkip {
		t.Fatalf("outcome = %s, want skip", out)
	}
	if n := src.dialsOf("board00"); n != 0 {
		t.Fatalf("dials = %d, want 0 (a cred-sighted board is zero-contact)", n)
	}
	if ops := mb.opsOf(); len(ops) != 0 {
		t.Fatalf("ops crossed: %v", ops)
	}

	// a live WiFi source (mDNS) — the board is ON the network now; a
	// stale credless sighting cannot license a reboot, still zero contact.
	if out := p.Consider(context.Background(), "board00", Sight{Sighted: true, WifiLive: true}); out != OutcomeSkip {
		t.Fatalf("outcome = %s, want skip", out)
	}
	if n := src.dialsOf("board00"); n != 0 {
		t.Fatalf("dials after wifi-live sighting = %d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// vault empty: blocked, coached, zero contact
// ---------------------------------------------------------------------------

func TestConsiderBlockedVaultEmpty(t *testing.T) {
	mb := newMockBoard()
	src := newMockSource(mb)
	h := newHarness(src, Vault{}) // empty vault
	p := NewProvisioner(h.deps())

	out := p.Consider(context.Background(), "board00", Sight{Sighted: true}) // credless sighting
	if out != OutcomeBlocked {
		t.Fatalf("outcome = %s, want blocked", out)
	}
	if n := src.dialsOf("board00"); n != 0 {
		t.Fatalf("dials = %d, want 0 (nothing to provision with — the board must not be rebooted pointlessly)", n)
	}
	log := h.logText()
	if !strings.Contains(log, "xuansi creds set") {
		t.Fatalf("the blocked log line must coach the owner to the CLI: %q", log)
	}
}

// an unknown board (never crossed) with an empty vault: no knowledge,
// nothing to give — no dial either (the reboot buys nothing).
func TestConsiderUnknownBoardEmptyVaultZeroContact(t *testing.T) {
	mb := newMockBoard()
	src := newMockSource(mb)
	h := newHarness(src, Vault{})
	p := NewProvisioner(h.deps())
	if out := p.Consider(context.Background(), "board00", Sight{}); out != OutcomeSkip {
		t.Fatalf("outcome = %s, want skip", out)
	}
	if n := src.dialsOf("board00"); n != 0 {
		t.Fatalf("dials = %d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// the happy path
// ---------------------------------------------------------------------------

func TestConsiderProvisionsCredlessBoard(t *testing.T) {
	mb := newMockBoard()
	mb.settleUp("wifi.setup", 20*time.Millisecond) // the association lands mid-poll
	src := newMockSource(mb)
	h := newHarness(src, Vault{WiFiSSID: dummySSID, WiFiPSK: dummyPSK})
	p := NewProvisioner(h.deps())

	out := p.Consider(context.Background(), "board00", Sight{Sighted: true}) // credless sighting, full vault
	if out != OutcomeProvisioned {
		t.Fatalf("outcome = %s, want provisioned (log: %s)", out, h.logText())
	}
	ops := mb.opsOf()
	var setup, pskW, status1 int = -1, -1, -1
	for i, op := range ops {
		switch op {
		case "wifi.status":
			if status1 == -1 {
				status1 = i
			}
		case "wifi.setup":
			if setup == -1 {
				setup = i
			}
		case "sec.psk_write":
			if pskW == -1 {
				pskW = i
			}
		}
	}
	if status1 == -1 || setup == -1 || pskW == -1 {
		t.Fatalf("ops missing a provisioning leg: %v", ops)
	}
	if !(status1 < setup && setup < pskW) {
		t.Fatalf("op order wrong: want wifi.status → wifi.setup → sec.psk_write, got %v", ops)
	}
	if mb.called("sec.psk_write") != 1 {
		t.Fatalf("sec.psk_write calls = %d, want 1", mb.called("sec.psk_write"))
	}
	// the materials crossed the wire to the board...
	if a := mb.argOf("wifi.setup"); a["ssid"] != dummySSID || a["pass"] != dummyPSK {
		t.Fatalf("wifi.setup args = %v, want the vault pair", a)
	}
	if a := mb.argOf("sec.psk_write"); a["psk_hex"] != strings.Repeat("cd", 32) {
		t.Fatalf("sec.psk_write psk_hex = %v, want the ensured dummy", a["psk_hex"])
	}
	// ...and NEVER into a log line (the xscreds host-side no-log discipline)
	log := h.logText()
	for _, secret := range []string{dummyPSK, strings.Repeat("cd", 32)} {
		if strings.Contains(log, secret) {
			t.Fatalf("log leaked material: %q", log)
		}
	}
	// the completion event fired exactly once
	if ev := h.firedEvents(); len(ev) != 1 || !strings.Contains(ev[0], "board00") {
		t.Fatalf("provisioned events = %v, want exactly one naming the board", ev)
	}
}

func TestConsiderWaitsForAssociation(t *testing.T) {
	mb := newMockBoard()
	mb.settleUp("wifi.setup", 25*time.Millisecond) // association lands after the polls started
	h := newHarness(newMockSource(mb), Vault{WiFiSSID: dummySSID, WiFiPSK: dummyPSK})
	p := NewProvisioner(h.deps())
	if out := p.Consider(context.Background(), "board00", Sight{Sighted: true}); out != OutcomeProvisioned {
		t.Fatalf("outcome = %s, want provisioned (log: %s)", out, h.logText())
	}
	if n := mb.called("wifi.status"); n < 2 {
		t.Fatalf("wifi.status polls = %d, want ≥2 (the wait provably polled, not first-probe lucky)", n)
	}
}

// a board that reports credentials once actually crossed (stale
// sighting): zero PROVISIONING ops — the guard's in-flight half.
func TestConsiderStopsOnCredfulBoard(t *testing.T) {
	mb := newMockBoard()
	mb.mu.Lock()
	mb.wifiSS = "meanwhile-provisioned"
	mb.mu.Unlock()
	h := newHarness(newMockSource(mb), Vault{WiFiSSID: dummySSID, WiFiPSK: dummyPSK})
	p := NewProvisioner(h.deps())

	// unsighted board: the provisioner may cross once to learn; finding
	// credentials present it must stop with zero provisioning ops.
	out := p.Consider(context.Background(), "board00", Sight{})
	if out != OutcomeCredful {
		t.Fatalf("outcome = %s, want credful", out)
	}
	for _, op := range []string{"wifi.setup", "sec.psk_write"} {
		if n := mb.called(op); n != 0 {
			t.Fatalf("%s crossed %d times on a credful board — zero provisioning contact required", op, n)
		}
	}
	if h.pskGen != 0 {
		t.Fatal("PSK must not be generated when no provisioning happens")
	}
	if ev := h.firedEvents(); len(ev) != 0 {
		t.Fatalf("credful stop emitted events: %v", ev)
	}
}

// association never lands: honest failure, no event.
func TestConsiderAssociationTimeout(t *testing.T) {
	mb := newMockBoard()
	mb.mu.Lock()
	mb.postSet = "fail" // setup "succeeds" but the board parks at fail
	mb.mu.Unlock()
	h := newHarness(newMockSource(mb), Vault{WiFiSSID: dummySSID, WiFiPSK: dummyPSK})
	p := NewProvisioner(h.deps())
	out := p.Consider(context.Background(), "board00", Sight{Sighted: true})
	if out != OutcomeFailed {
		t.Fatalf("outcome = %s, want failed (log: %s)", out, h.logText())
	}
	if ev := h.firedEvents(); len(ev) != 0 {
		t.Fatal("no provisioned event may fire on a failed association")
	}
}

// a failed attempt must not retry the same board against the same
// vault on the next sweep (the reboot-loop guard) — but a CHANGED
// vault re-arms it (the owner fixed the credentials).
func TestConsiderNoRebootLoopAndRearmOnVaultChange(t *testing.T) {
	mb := newMockBoard()
	mb.mu.Lock()
	mb.fail["wifi.setup"] = fmt.Errorf("api: wifi.setup: pass too long")
	mb.mu.Unlock()
	h := newHarness(newMockSource(mb), Vault{WiFiSSID: dummySSID, WiFiPSK: dummyPSK})
	p := NewProvisioner(h.deps())
	if out := p.Consider(context.Background(), "board00", Sight{Sighted: true}); out != OutcomeFailed {
		t.Fatalf("outcome = %s, want failed", out)
	}
	if out := p.Consider(context.Background(), "board00", Sight{Sighted: true}); out != OutcomeSkip {
		t.Fatalf("second Consider outcome = %s, want skip (same vault, same board — no reboot loop)", out)
	}
	if n := h.src.dialsOf("board00"); n != 1 {
		t.Fatalf("dials = %d, want exactly 1 (the failed attempt; no retry)", n)
	}
	// the owner fixes the vault: the next sweep retries and lands
	delete(mb.fail, "wifi.setup")
	mb.settleUp("wifi.setup", 20*time.Millisecond)
	h.mu.Lock()
	h.vault = Vault{WiFiSSID: dummySSID, WiFiPSK: "fixed-dummy-pass"}
	h.mu.Unlock()
	if out := p.Consider(context.Background(), "board00", Sight{Sighted: true}); out != OutcomeProvisioned {
		t.Fatalf("outcome = %s, want provisioned after the vault changed (log: %s)", out, h.logText())
	}
}

// kill-switch: a disabled provisioner never considers anything.
func TestConsiderDisabledNeverTouches(t *testing.T) {
	mb := newMockBoard()
	src := newMockSource(mb)
	h := newHarness(src, Vault{WiFiSSID: dummySSID, WiFiPSK: dummyPSK})
	d := h.deps()
	d.Disabled = true
	p := NewProvisioner(d)
	if out := p.Consider(context.Background(), "board00", Sight{Sighted: true}); out != OutcomeSkip {
		t.Fatalf("outcome = %s, want skip", out)
	}
	if n := src.dialsOf("board00"); n != 0 {
		t.Fatalf("dials = %d, want 0", n)
	}
}
