package server

// provision_test.go — the zero-ritual provisioner's server half
// (憑證零儀式波 T2): the provision_state column on the board face, the
// sweep's guard behavior over the daemon's own supervisor plumbing
// (zero-contact for cred-present boards by DIAL COUNTER, the
// vault-empty block with the coach log line, the happy path's op
// sequence over the CDC fake, the board.provisioned event, the
// kill-switch), and the redaction vector over the daemon's whole log
// buffer. All credentials/PSKs are dummy.

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/creds"
	"xuansi/internal/discover"
	"xuansi/internal/replclient"
	"xuansi/internal/testutil"
)

const (
	provDummySSID = "bench-ap-dummy"
	provDummyPSK  = "dummy-wifi-pass-29-bytes-1234"
)

// provEnv is a server env with the provisioner's knobs injectable:
// the vault dir, the kill-switch, and a CAPTURABLE daemon log (the
// no-material vector reads every line the daemon emitted).
type provEnv struct {
	t    *testing.T
	srv  *Server
	sock string

	fakes    map[string]*fakeCtl
	usbFakes map[string]*fakeCtl

	dmu     sync.Mutex
	dialed  map[string]int
	usbDial map[string]int

	logMu sync.Mutex
	logs  []string
}

func (e *provEnv) logf(format string, args ...any) {
	e.logMu.Lock()
	defer e.logMu.Unlock()
	e.logs = append(e.logs, fmt.Sprintf(format, args...))
}

func (e *provEnv) logText() string {
	e.logMu.Lock()
	defer e.logMu.Unlock()
	return strings.Join(e.logs, "\n")
}

func (e *provEnv) dialedTCP(mac6 string) int {
	e.dmu.Lock()
	defer e.dmu.Unlock()
	return e.dialed[mac6]
}

func (e *provEnv) dialedUSB(mac6 string) int {
	e.dmu.Lock()
	defer e.dmu.Unlock()
	return e.usbDial[mac6]
}

// helloNet scripts a fake's hello reply to carry a net section (the
// sighting's cred source).
func helloNet(f *fakeCtl, ssid, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops["hello"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{
			"protocol_ver": int64(10), "role": "controller", "fw_build": "dummy123",
			"net": map[string]any{"state": state, "ssid": ssid},
		}, nil
	}
}

// armProvisionOps scripts the USB fake's provisioning legs: wifi.status
// answers cred-less until wifi.setup crosses, then associates (state
// up) after settle. sec.psk_write answers the frozen v10 shape.
func armProvisionOps(f *fakeCtl) {
	var mu sync.Mutex
	setupSeen := false
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops["wifi.status"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		mu.Lock()
		defer mu.Unlock()
		if setupSeen {
			return map[string]any{"state": "up", "ssid": provDummySSID, "ip": "10.1.51.191", "port": int64(7777)}, nil
		}
		return map[string]any{"state": "off", "ssid": ""}, nil
	}
	f.ops["wifi.setup"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		mu.Lock()
		setupSeen = true
		mu.Unlock()
		return map[string]any{"state": "connecting"}, nil
	}
	f.ops["sec.psk_write"] = func(args map[string]any) (map[string]any, *replclient.OpError) {
		return map[string]any{"stored": true, "bytes": int64(32)}, nil
	}
}

// newProvEnv builds a serving Server over usb-only boards (the
// zero-ritual's habitat) with the provisioner's Opts injectable. arm
// (optional) scripts the fakes BEFORE Start — the startup audit's own
// provision sweep rides the armed handlers from the first crossing.
func newProvEnv(t *testing.T, vaultDir string, disable bool, arm func(b discover.Board, tcp, usb *fakeCtl), boards ...discover.Board) *provEnv {
	t.Helper()
	e := &provEnv{
		t: t, fakes: map[string]*fakeCtl{}, usbFakes: map[string]*fakeCtl{},
		dialed: map[string]int{}, usbDial: map[string]int{},
	}
	for _, b := range boards {
		e.fakes[b.MAC6] = newFakeCtl()
		e.usbFakes[b.MAC6] = newFakeCtl()
		helloNet(e.fakes[b.MAC6], "", "off")
		helloNet(e.usbFakes[b.MAC6], "", "off")
	}
	if arm != nil {
		for _, b := range boards {
			arm(b, e.fakes[b.MAC6], e.usbFakes[b.MAC6])
		}
	}
	reg := discover.NewRegistry(nil, nil,
		discover.WithUSB(func(ctx context.Context) ([]discover.Board, error) {
			return boards, nil
		}),
		discover.WithMDNS(func(ctx context.Context) ([]discover.Board, error) {
			// only boards that actually carry an mdns source answer mDNS
			// (the usb-only habitat stays mdns-silent)
			var out []discover.Board
			for _, b := range boards {
				if hasSource(b, discover.SourceMDNS) {
					out = append(out, b)
				}
			}
			return out, nil
		}),
	)
	e.sock = filepath.Join(testutil.ShortSockDir(t), "p.sock")
	srv, err := New(Opts{
		SockPath:         e.sock,
		Registry:         reg,
		GraceTTL:         10 * time.Second,
		RefreshInterval:  -1,
		Log:              e.logf,
		BootRecordsDir:   filepath.Join(testutil.ShortSockDir(t), "bootrec"),
		VaultDir:         vaultDir,
		DisableProvision: disable,
		ProvisionWait:    1500 * time.Millisecond, // a Consider goroutine never outlives its env (the 30s production window would shadow later tests under -race load)
		NewSupervisor: func(ref board.BoardRef) *board.Supervisor {
			return board.NewSupervisor(ref, board.SupOpts{
				Log: e.logf,
				DialTCP: func() (board.Controller, error) {
					e.dmu.Lock()
					e.dialed[ref.MAC6]++
					f := e.fakes[ref.MAC6]
					e.dmu.Unlock()
					if f == nil {
						return nil, fmt.Errorf("no fake for %s", ref.MAC6)
					}
					return f, nil
				},
				DialUSB: func() (board.Controller, error) {
					e.dmu.Lock()
					e.usbDial[ref.MAC6]++
					f := e.usbFakes[ref.MAC6]
					e.dmu.Unlock()
					if f == nil {
						return nil, fmt.Errorf("no usb fake for %s", ref.MAC6)
					}
					return f, nil
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e.srv = srv
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	awaitSweep(t, srv.reg, len(boards))
	return e
}

// sweep runs one provision sweep and waits for the evidence: awaitOp
// non-empty polls BOTH fakes for the op (the lane reuses a warm
// controller when one is live); awaitOp "" polls the daemon log for
// awaitLog instead (the blocked/quiet legs' determinism — a fixed
// sleep would flake under load); both "" is the pure zero-contact
// quiet window.
func (e *provEnv) sweep(awaitOp, awaitLog string) {
	e.t.Helper()
	e.srv.provisionSweep(context.Background())
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if awaitOp != "" && (e.usbFakes["d75884"].called(awaitOp) > 0 || e.fakes["d75884"].called(awaitOp) > 0) {
			return
		}
		if awaitLog != "" && strings.Contains(e.logText(), awaitLog) {
			return
		}
		if awaitOp == "" && awaitLog == "" {
			time.Sleep(200 * time.Millisecond) // zero-contact quiet window
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if awaitOp != "" {
		e.t.Fatalf("provision sweep never crossed %s on any fake (log: %s)", awaitOp, e.logText())
	}
	if awaitLog != "" {
		e.t.Fatalf("provision sweep never logged %q (log: %s)", awaitLog, e.logText())
	}
}

func usbBoard(mac6 string) discover.Board {
	return discover.Board{
		MAC6: mac6, Sources: []string{discover.SourceUSB},
		USBPath: "/dev/serial/by-id/fake-" + mac6,
	}
}

// ---------------------------------------------------------------------------
// provision_state on the board face
// ---------------------------------------------------------------------------

func TestProvisionStateInView(t *testing.T) {
	vdir := t.TempDir()
	if err := creds.SaveWiFi(vdir, provDummySSID, provDummyPSK); err != nil {
		t.Fatal(err)
	}
	// cred-less sighting + armed vault → credless
	e := newProvEnv(t, vdir, true, nil, usbBoard("d75884")) // kill-switch on: no sweep side effects
	helloNet(e.usbFakes["d75884"], "", "off")
	e.srv.reg.Enrich("d75884", &replclient.HelloInfo{Net: map[string]any{"state": "off", "ssid": ""}})
	view := e.srv.boardView(e.srv.reg.Snapshot()[0])
	if got := view["provision_state"]; got != "credless" {
		t.Fatalf("provision_state = %v, want credless", got)
	}
	// the same sighting against an EMPTY vault → vault-empty (the daemon
	// wanted to provision but couldn't)
	e2 := newProvEnv(t, t.TempDir(), true, nil, usbBoard("d75884"))
	e2.srv.reg.Enrich("d75884", &replclient.HelloInfo{Net: map[string]any{"state": "off", "ssid": ""}})
	if got := e2.srv.boardView(e2.srv.reg.Snapshot()[0])["provision_state"]; got != "vault-empty" {
		t.Fatalf("provision_state = %v, want vault-empty", got)
	}
	// a cred-showing sighting → ok
	e3 := newProvEnv(t, t.TempDir(), true, nil, usbBoard("d75884"))
	e3.srv.reg.Enrich("d75884", &replclient.HelloInfo{Net: map[string]any{"state": "up", "ssid": "set-already"}})
	if got := e3.srv.boardView(e3.srv.reg.Snapshot()[0])["provision_state"]; got != "ok" {
		t.Fatalf("provision_state = %v, want ok", got)
	}
	// never sighted → honestly absent
	e4 := newProvEnv(t, t.TempDir(), true, nil, usbBoard("d75884"))
	if _, ok := e4.srv.boardView(e4.srv.reg.Snapshot()[0])["provision_state"]; ok {
		t.Fatal("provision_state must be absent before any sighting (the daemon cannot know without a crossing)")
	}
}

// ---------------------------------------------------------------------------
// GUARD HARD: cred-present boards receive zero contact
// ---------------------------------------------------------------------------

func TestProvisionSweepZeroTouchCredPresent(t *testing.T) {
	vdir := t.TempDir()
	if err := creds.SaveWiFi(vdir, provDummySSID, provDummyPSK); err != nil {
		t.Fatal(err)
	}
	// (a) a live mDNS source: the board is ON the WiFi — zero contact
	e := newProvEnv(t, vdir, false, nil, discover.Board{
		MAC6: "d75884", Sources: []string{discover.SourceUSB, discover.SourceMDNS},
		USBPath: "/dev/serial/by-id/fake-d75884", // no Host: mdns-live but not TCP-dialable — the startup audit skips it, isolating the sweep's dials
	})
	e.sweep("", "")
	if n := e.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d, want 0 (a WiFi-live board is zero-contact)", n)
	}
	if n := e.dialedTCP("d75884"); n != 0 {
		t.Fatalf("tcp dials = %d, want 0", n)
	}

	// (b) a cred-showing sighting on a usb-only board: still zero contact.
	// The vault starts EMPTY (the startup sweep blocks with zero
	// contact); the daemon's OWN crossing (watch_events warming the
	// controller) lands a cred-showing sighting; only then is the vault
	// armed live — the sweep must stay zero-contact (no CDC open).
	e2 := newProvEnv(t, t.TempDir(), false, func(_ discover.Board, tcp, _ *fakeCtl) {
		helloNet(tcp, "still-there", "fail") // the warm controller's hello sighting shows creds
	}, usbBoard("d75884"))
	w := dialSock(t, e2.sock)
	if rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"}); asMap(rep["error"]) != nil {
		t.Fatalf("watch: %#v", rep)
	}
	if err := creds.SaveWiFi(vdir, provDummySSID, provDummyPSK); err != nil {
		t.Fatal(err)
	}
	e2.sweep("", "")
	if n := e2.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d, want 0 (a cred-sighted board is zero-contact — a CDC open would reboot it)", n)
	}
}

// ---------------------------------------------------------------------------
// vault empty: blocked, coached, zero contact
// ---------------------------------------------------------------------------

func TestProvisionSweepVaultEmptyBlocked(t *testing.T) {
	e := newProvEnv(t, t.TempDir(), false, nil, usbBoard("d75884")) // empty vault dir
	e.srv.reg.Enrich("d75884", &replclient.HelloInfo{Net: map[string]any{"state": "off", "ssid": ""}})
	e.sweep("", "zero-ritual blocked")
	if n := e.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d, want 0 (nothing to provision with — no pointless reboot)", n)
	}
	log := e.logText()
	if !strings.Contains(log, "xuansi creds set") {
		t.Fatalf("the blocked leg must log the coach line: %q", log)
	}
}

// ---------------------------------------------------------------------------
// the happy path over the daemon's own supervisor plumbing
// ---------------------------------------------------------------------------

func TestProvisionSweepProvisionsCredlessUSBBoard(t *testing.T) {
	vdir := t.TempDir()
	if err := creds.SaveWiFi(vdir, provDummySSID, provDummyPSK); err != nil {
		t.Fatal(err)
	}
	psk, err := creds.EnsurePSK(vdir)
	if err != nil {
		t.Fatal(err)
	}
	e := newProvEnv(t, vdir, false, func(_ discover.Board, _, usb *fakeCtl) { armProvisionOps(usb) }, usbBoard("d75884"))

	// the startup audit's own sweep (armed from the start) provisions
	// the board — the plug-in-and-it-works entry, no test-driven sweep
	// needed; this one waits for it.
	e.sweep("sec.psk_write", "")

	f := e.usbFakes["d75884"]
	// the sequence: wifi.status (cred-less verdict) → wifi.setup (the
	// vault pair) → sec.psk_write (the vault's daemon PSK)
	calls := f.snapshotCalls()
	var status1, setup, pskW = -1, -1, -1
	for i, c := range calls {
		switch c.op {
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
	if status1 == -1 || setup == -1 || pskW == -1 || !(status1 < setup && setup < pskW) {
		t.Fatalf("op order wrong (want wifi.status → wifi.setup → sec.psk_write): %v", callNames(calls))
	}
	if a := findCall(calls, "wifi.setup"); a["ssid"] != provDummySSID || a["pass"] != provDummyPSK {
		t.Fatalf("wifi.setup args = %v, want the vault pair", a)
	}
	if a := findCall(calls, "sec.psk_write"); a["psk_hex"] != psk {
		t.Fatalf("sec.psk_write psk_hex = %v, want the vault's ensured psk", a["psk_hex"])
	}

	// the redaction vector over the daemon's WHOLE log buffer: the
	// sweep + provisioning crossed wifi.setup/sec.psk_write with the
	// vault's material, and none of it may appear in any log line.
	log := e.logText()
	for _, secret := range []string{provDummyPSK, psk} {
		if strings.Contains(log, secret) {
			t.Fatalf("daemon log leaked secret material: %q", log)
		}
	}
}

// the board.provisioned event leg: a watcher is subscribed (which
// pre-warms a controller — the gentle lane reuses it instead of
// opening CDC), the sweep provisions over that warm link, and the
// event reaches the watcher with zero secret fields.
func TestProvisionSweepEmitsProvisionedEvent(t *testing.T) {
	// the vault starts EMPTY (the startup sweep blocks with zero
	// contact), a watcher subscribes (pre-warming a controller the
	// gentle lane must REUSE — no CDC open), then the owner arms the
	// vault LIVE and the sweep provisions: the event reaches the
	// watcher with zero secret fields.
	vdir := t.TempDir()
	e := newProvEnv(t, vdir, false, func(_ discover.Board, tcp, usb *fakeCtl) { armProvisionOps(tcp); armProvisionOps(usb) }, usbBoard("d75884"))
	w := dialSock(t, e.sock)
	if rep, _ := w.call(1, map[string]any{"type": "watch_events", "board": "d75884"}); asMap(rep["error"]) != nil {
		t.Fatalf("watch: %#v", rep)
	}
	beforeUSB := e.dialedUSB("d75884")
	if err := creds.SaveWiFi(vdir, provDummySSID, provDummyPSK); err != nil {
		t.Fatal(err)
	}

	e.sweep("sec.psk_write", "")

	// the warm (TCP-route) controller was reused — no CDC open happened
	if n := e.dialedUSB("d75884"); n != beforeUSB {
		t.Fatalf("usb dials moved %d → %d — a warm controller must be reused, not CDC-opened", beforeUSB, n)
	}
	// the event (the tap may also deliver bookkeeping events first —
	// scan for the provisioned kind, bounded)
	var data map[string]any
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ev := w.recv()
		inner := asMap(asMap(ev["event"])["data"])
		if asStr(inner["kind"]) == "board.provisioned" {
			data = inner
			break
		}
	}
	if data == nil {
		t.Fatal("board.provisioned event never reached the watcher")
	}
	if asStr(data["board"]) != "d75884" || asStr(data["ssid"]) != provDummySSID {
		t.Fatalf("event board/ssid = %v/%v", data["board"], data["ssid"])
	}
	if _, leaked := data["psk"]; leaked {
		t.Fatal("the provisioned event must carry no psk field (zero secret fields)")
	}
	if asStr(data["ip"]) != "10.1.51.191" {
		t.Fatalf("event ip = %v, want the association's address", data["ip"])
	}
	if log := e.logText(); strings.Contains(log, provDummyPSK) {
		t.Fatalf("daemon log leaked the wifi psk: %q", log)
	}
}

// a leased board is the holder's — the sweep must not reboot under them.
func TestProvisionSweepSkipsLeasedBoard(t *testing.T) {
	vdir := t.TempDir()
	if err := creds.SaveWiFi(vdir, provDummySSID, provDummyPSK); err != nil {
		t.Fatal(err)
	}
	// the vault starts EMPTY (the startup sweep blocks with zero
	// contact), the LEASE is taken (the lease itself opens CDC once),
	// and only THEN is the vault armed live — the sweep must refuse to
	// open CDC a second time under the holder.
	e := newProvEnv(t, t.TempDir(), false, nil, usbBoard("d75884"))
	c := dialSock(t, e.sock)
	rep, _ := c.call(1, map[string]any{"type": "lease", "board": "d75884", "holder": "agent-x"})
	if asMap(rep["error"]) != nil {
		t.Fatalf("lease: %#v", rep)
	}
	leaseDials := e.dialedUSB("d75884") // the lease itself opened CDC
	if err := creds.SaveWiFi(vdir, provDummySSID, provDummyPSK); err != nil {
		t.Fatal(err)
	}
	e.sweep("", "")
	if n := e.dialedUSB("d75884"); n != leaseDials {
		t.Fatalf("usb dials = %d after sweep, want unchanged %d (a leased board is the holder's)", n, leaseDials)
	}
}

// the kill-switch: a disabled sweep never touches anything.
func TestProvisionSweepKillSwitch(t *testing.T) {
	vdir := t.TempDir()
	if err := creds.SaveWiFi(vdir, provDummySSID, provDummyPSK); err != nil {
		t.Fatal(err)
	}
	e := newProvEnv(t, vdir, true, nil, usbBoard("d75884")) // DisableProvision
	e.sweep("", "")
	if n := e.dialedUSB("d75884"); n != 0 {
		t.Fatalf("usb dials = %d, want 0 (kill-switch)", n)
	}
	if log := e.logText(); strings.Contains(log, "provision") {
		t.Fatalf("a disabled provisioner must not even log provisioning lines: %q", log)
	}
}

// helpers: the fake's call log, names, one op's args, and a per-op
// call counter (the zero-ritual sweep's await seam).

func (f *fakeCtl) called(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.op == op {
			n++
		}
	}
	return n
}

func (f *fakeCtl) snapshotCalls() []mockCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]mockCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func callNames(cs []mockCall) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.op
	}
	return out
}

func findCall(cs []mockCall, op string) map[string]any {
	for _, c := range cs {
		if c.op == op {
			return c.args
		}
	}
	return nil
}
