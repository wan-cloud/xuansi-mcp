package discover

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"xuansi/internal/replclient"
)

// fakeSources rigs a Registry with canned discovery results.
type fakeSources struct {
	mu   sync.Mutex
	usb  []Board
	mdns []Board
	errU error
	errM error
}

func (f *fakeSources) set(usb, mdns []Board) {
	f.mu.Lock()
	f.usb, f.mdns = usb, mdns
	f.errU, f.errM = nil, nil
	f.mu.Unlock()
}

func (f *fakeSources) failMDNS(e error) { f.mu.Lock(); f.errM = e; f.mu.Unlock() }

func (f *fakeSources) usbFn(ctx context.Context) ([]Board, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usb, f.errU
}

func (f *fakeSources) mdnsFn(ctx context.Context) ([]Board, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mdns, f.errM
}

func newFakeRegistry(aliases map[string]string) (*Registry, *fakeSources) {
	f := &fakeSources{}
	r := NewRegistry(aliases, nil)
	r.usb, r.mdns = f.usbFn, f.mdnsFn
	return r, f
}

// dualA/dualB: two boards' faces for the merge table.
var (
	usbA = Board{MAC6: "d75884", Sources: []string{"usb"},
		USBPath: "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00"}
	usbB = Board{MAC6: "1a2b3c", Sources: []string{"usb"},
		USBPath: "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:1A:2B:3C-if00"}
	mdnsA = Board{MAC6: "d75884", Sources: []string{"mdns"},
		Host: "xuansi-d75884.local", Port: 7777, TXT: map[string]string{"ver": "3", "api": "1"}}
	mdnsB = Board{MAC6: "1a2b3c", Sources: []string{"mdns"},
		Host: "xuansi-1a2b3c.local", Port: 7777, TXT: map[string]string{"ver": "3", "api": "1"}}
)

func TestRefreshDualSourceMerge(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set([]Board{usbA, usbB}, []Board{mdnsA, mdnsB})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	got := r.Snapshot()
	want := []Board{
		{
			MAC6: "1a2b3c", Sources: []string{"usb", "mdns"}, USBPath: usbB.USBPath,
			Host: "xuansi-1a2b3c.local", Port: 7777, TXT: map[string]string{"ver": "3", "api": "1"},
		},
		{
			MAC6: "d75884", Sources: []string{"usb", "mdns"}, USBPath: usbA.USBPath,
			Host: "xuansi-d75884.local", Port: 7777, TXT: map[string]string{"ver": "3", "api": "1"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Snapshot =\n %#v\nwant\n %#v", got, want)
	}
}

func TestRefreshSingleSourceKeep(t *testing.T) {
	r, f := newFakeRegistry(nil)
	// board A on usb only, board B on mdns only — no merge, each keeps its face
	f.set([]Board{usbA}, []Board{mdnsB})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	got := r.Snapshot()
	if len(got) != 2 {
		t.Fatalf("want 2 boards, got %#v", got)
	}
	if !reflect.DeepEqual(got[0].Sources, []string{"mdns"}) || got[0].USBPath != "" {
		t.Errorf("mdns-only board: %#v", got[0])
	}
	if !reflect.DeepEqual(got[1].Sources, []string{"usb"}) || got[1].Host != "" {
		t.Errorf("usb-only board: %#v", got[1])
	}
}

// Source disappear = DOWNGRADE: the mdns face withdraws (wifi.off shape);
// the entry stays with the remaining usb source and its mdns face turns
// last-known (kept, honest stale-marking — no field is zeroed).
func TestRefreshSourceDisappearDowngrade(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set([]Board{usbA}, []Board{mdnsA})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 1: %v", err)
	}
	hello := &replclient.HelloInfo{ProtocolVer: 5, FwBuild: "b310378"}
	r.Enrich("d75884", hello)

	f.set([]Board{usbA}, nil) // wifi off: mdns sweep finds nothing
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	got := r.Snapshot()
	if len(got) != 1 {
		t.Fatalf("want 1 board after downgrade, got %#v", got)
	}
	b := got[0]
	if !reflect.DeepEqual(b.Sources, []string{"usb"}) {
		t.Errorf("downgraded sources = %#v, want [usb]", b.Sources)
	}
	if b.Host != "xuansi-d75884.local" || b.Port != 7777 || b.TXT["ver"] != "3" {
		t.Errorf("last-known mdns face dropped: %#v", b)
	}
	if b.USBPath != usbA.USBPath {
		t.Errorf("usb face lost: %#v", b)
	}
	if b.Hello != hello {
		t.Errorf("enriched hello lost on downgrade: %#v", b.Hello)
	}
}

// All sources gone = the entry STAYS (no hard delete) with empty Sources:
// last-known only, honestly marked as not currently seen by any source.
func TestRefreshAllSourcesGoneStaleKeep(t *testing.T) {
	r, f := newFakeRegistry(map[string]string{"d75884": "bench"})
	f.set([]Board{usbA}, []Board{mdnsA})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 1: %v", err)
	}
	f.set(nil, nil) // board unplugged AND off-wifi
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	got := r.Snapshot()
	if len(got) != 1 {
		t.Fatalf("hard-deleted the board: %#v", got)
	}
	b := got[0]
	if len(b.Sources) != 0 {
		t.Errorf("stale sources = %#v, want empty", b.Sources)
	}
	if b.USBPath != usbA.USBPath || b.Host != mdnsA.Host || b.Alias != "bench" {
		t.Errorf("stale entry lost last-known data: %#v", b)
	}
	// a stale board is still resolvable — it is a known board (T11 owns policy)
	if rb, err := r.Resolve("d75884"); err != nil || rb.MAC6 != "d75884" {
		t.Errorf("Resolve stale board = (%#v, %v)", rb, err)
	}
}

// mDNS sweep FAILURE (multicast-blocked AP shape) is environment
// degradation, not board departure: the mdns face FREEZES at last-known
// instead of being downgraded by a blind sweep, and the failure is
// surfaced via MDNSError. A later clean sweep that finds nothing DOES
// downgrade (that is real knowledge the service is gone).
func TestRefreshMDNSErrorFreezesFace(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set([]Board{usbA}, []Board{mdnsA})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 1: %v", err)
	}

	f.failMDNS(errors.New("no multicast interface available"))
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2 (mdns failure must not fail Refresh): %v", err)
	}
	b := r.Snapshot()[0]
	if !reflect.DeepEqual(b.Sources, []string{"usb", "mdns"}) {
		t.Errorf("degraded refresh downgraded mdns face: %#v", b.Sources)
	}
	if r.MDNSError() == "" {
		t.Error("MDNSError() empty after mdns failure")
	}

	f.set([]Board{usbA}, nil) // mdns healthy again, service genuinely gone
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 3: %v", err)
	}
	b = r.Snapshot()[0]
	if !reflect.DeepEqual(b.Sources, []string{"usb"}) {
		t.Errorf("clean empty sweep must downgrade: %#v", b.Sources)
	}
	if r.MDNSError() != "" {
		t.Errorf("MDNSError() = %q, want cleared", r.MDNSError())
	}
}

// USB failure is a real error: Refresh reports it and merges nothing
// (the registry snapshot is untouched).
func TestRefreshUSBErrorLeavesRegistryUntouched(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set([]Board{usbA}, []Board{mdnsA})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 1: %v", err)
	}
	before := r.Snapshot()

	f.mu.Lock()
	f.errU = errors.New("serial: permission denied")
	f.mu.Unlock()
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("usb failure: want Refresh error")
	}
	if !reflect.DeepEqual(r.Snapshot(), before) {
		t.Error("registry mutated despite usb failure")
	}
}

func TestRefreshAliasFill(t *testing.T) {
	r, f := newFakeRegistry(map[string]string{"d75884": "bench", "1a2b3c": "cam"})
	f.set([]Board{usbA, usbB}, nil)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for _, b := range r.Snapshot() {
		want := map[string]string{"d75884": "bench", "1a2b3c": "cam"}[b.MAC6]
		if b.Alias != want {
			t.Errorf("board %s alias = %q, want %q", b.MAC6, b.Alias, want)
		}
	}
	// a board with no alias entry stays empty
	f.set([]Board{usbA, usbB, {MAC6: "0000aa", Sources: []string{"usb"}, USBPath: "/dev/x"}}, nil)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	for _, b := range r.Snapshot() {
		if b.MAC6 == "0000aa" && b.Alias != "" {
			t.Errorf("unaliased board got alias %q", b.Alias)
		}
	}
}

// Enrichment is the caller's job (lazy): discovery never dials, and the
// recorded Hello survives later refreshes as last-known identity.
func TestEnrichLazyHello(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set([]Board{usbA}, nil)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if b := r.Snapshot()[0]; b.Hello != nil {
		t.Fatalf("Hello must be nil until someone dials, got %#v", b.Hello)
	}
	hello := &replclient.HelloInfo{ProtocolVer: 5, FwBuild: "b310378"}
	r.Enrich("d75884", hello)
	if b := r.Snapshot()[0]; b.Hello != hello {
		t.Errorf("Enrich not recorded: %#v", b.Hello)
	}
	f.set(nil, nil) // board goes fully stale — identity is still known
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	if b := r.Snapshot()[0]; b.Hello != hello {
		t.Errorf("enriched hello lost across refresh: %#v", b.Hello)
	}
	// enrich for an unknown mac6 is a no-op
	r.Enrich("ffffff", hello)
	if n := len(r.Snapshot()); n != 1 {
		t.Errorf("Enrich for unknown mac6 created an entry (%d boards)", n)
	}
}

// TestEnrichEndpointSelfReport: Bug 2② (B15 §4.9③) — a board's OWN
// wifi.setup/wifi.status reply carries its ip/port; EnrichEndpoint
// writes them back onto the registry entry so the daemon dials the
// self-reported address even when discovery never resolved one. Like
// all last-known data the endpoint survives a refresh that did not fill
// it; unknown mac6 and empty payloads are no-ops.
func TestEnrichEndpointSelfReport(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set([]Board{usbA}, nil) // usb-only first appearance: no IP, no port
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if b := r.Snapshot()[0]; b.IP != "" || b.Port != 0 {
		t.Fatalf("precondition: entry must start endpoint-less, got %#v", b)
	}
	r.EnrichEndpoint("d75884", "10.1.51.205", 7777)
	b := r.Snapshot()[0]
	if b.IP != "10.1.51.205" || b.Port != 7777 {
		t.Fatalf("EnrichEndpoint not recorded: ip=%q port=%d", b.IP, b.Port)
	}
	// the self-reported endpoint survives a refresh that fills neither
	// field (last-known carry — the same rule as the browse-resolved IP)
	f.set(nil, []Board{{MAC6: "d75884", Sources: []string{SourceMDNS}, Host: "xuansi-d75884.local"}})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	if b := r.Snapshot()[0]; b.IP != "10.1.51.205" || b.Port != 7777 {
		t.Errorf("self-reported endpoint lost across refresh: ip=%q port=%d", b.IP, b.Port)
	}
	// partial self-reports update only what they carry
	r.EnrichEndpoint("d75884", "10.1.51.206", 0)
	if b := r.Snapshot()[0]; b.IP != "10.1.51.206" || b.Port != 7777 {
		t.Errorf("partial EnrichEndpoint: ip=%q port=%d, want ip moved port kept", b.IP, b.Port)
	}
	// empty payload and unknown mac6 are no-ops
	r.EnrichEndpoint("d75884", "", 0)
	if b := r.Snapshot()[0]; b.IP != "10.1.51.206" {
		t.Errorf("empty EnrichEndpoint mutated the entry: %#v", b)
	}
	r.EnrichEndpoint("ffffff", "1.2.3.4", 7777)
	if n := len(r.Snapshot()); n != 1 {
		t.Errorf("EnrichEndpoint for unknown mac6 created an entry (%d boards)", n)
	}
}

func TestResolveSelectors(t *testing.T) {
	r, f := newFakeRegistry(map[string]string{"d75884": "bench", "1a2b3c": "cam"})
	f.set([]Board{usbA, usbB}, []Board{mdnsA, mdnsB})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	cases := []struct {
		name, sel string
		wantMAC6  string
	}{
		{"mac6 exact", "d75884", "d75884"},
		{"mac6 unique prefix", "d758", "d75884"},
		{"mac6 one char unique", "1", "1a2b3c"},
		{"mac6 case-insensitive", "D75884", "d75884"},
		{"alias exact", "bench", "d75884"},
		{"hostname full", "xuansi-d75884.local", "d75884"},
		{"hostname prefix", "xuansi-d7", "d75884"},
		{"selector padded", "  d75884 ", "d75884"},
	}
	for _, c := range cases {
		b, err := r.Resolve(c.sel)
		if err != nil {
			t.Errorf("%s: Resolve(%q): %v", c.name, c.sel, err)
			continue
		}
		if b.MAC6 != c.wantMAC6 {
			t.Errorf("%s: Resolve(%q) = %s, want %s", c.name, c.sel, b.MAC6, c.wantMAC6)
		}
	}
}

func TestResolveAmbiguousListsCandidates(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set(nil, []Board{
		{MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local"},
		{MAC6: "d7588a", Sources: []string{"mdns"}, Host: "xuansi-d7588a.local"},
	})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// "d7588" prefix-matches both mac6s AND both hostnames — ambiguous
	for _, sel := range []string{"d7588", "xuansi-d7588"} {
		_, err := r.Resolve(sel)
		if err == nil {
			t.Fatalf("Resolve(%q): ambiguity must error, got nil", sel)
		}
		msg := err.Error()
		if !strings.Contains(msg, "d75884") || !strings.Contains(msg, "d7588a") {
			t.Errorf("Resolve(%q) error must list both candidates: %q", sel, msg)
		}
	}
	// alias-vs-mac6 cross-board ambiguity: cam's alias collides with A's mac6 prefix
	r2, f2 := newFakeRegistry(map[string]string{"d7588a": "d758"})
	f2.set([]Board{usbA}, []Board{
		{MAC6: "d7588a", Sources: []string{"mdns"}, Host: "xuansi-d7588a.local"},
	})
	if err := r2.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if _, err := r2.Resolve("d758"); err == nil || !strings.Contains(err.Error(), "d75884") {
		t.Errorf("alias/mac6 collision must error listing both: %v", err)
	}
}

func TestResolveUnmatchedListsCandidates(t *testing.T) {
	r, f := newFakeRegistry(map[string]string{"d75884": "bench"})
	f.set([]Board{usbA}, []Board{mdnsA})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for _, sel := range []string{"zzzz", ""} {
		_, err := r.Resolve(sel)
		if err == nil {
			t.Fatalf("Resolve(%q): unmatched must error", sel)
		}
		msg := err.Error()
		if !strings.Contains(msg, "d75884") || !strings.Contains(msg, "bench") || !strings.Contains(msg, "xuansi-d75884.local") {
			t.Errorf("Resolve(%q) error must list candidate boards (mac6/alias/host): %q", sel, msg)
		}
	}
}

// Refresh/Snapshot/Resolve/Enrich are safe for concurrent use.
func TestConcurrentAccess(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set([]Board{usbA}, []Board{mdnsA})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = r.Refresh(context.Background())
				_ = r.Snapshot()
				_, _ = r.Resolve("d75884")
				r.Enrich("d75884", &replclient.HelloInfo{})
				_ = r.MDNSError()
			}
		}()
	}
	wg.Wait()
	if b, err := r.Resolve("d75884"); err != nil || b.MAC6 != "d75884" {
		t.Errorf("post-concurrency Resolve = (%#v, %v)", b, err)
	}
}

// ---------------------------------------------------------------- dial seam --

// dialSpy captures DialBoard's transport calls (the dial seam; production
// dials replclient, tests assert the addressed endpoint without touching
// devices).
type dialSpy struct {
	tcp []string // host:port strings the TCP face was dialed at
	usb []string // by-id paths the USB face was dialed at
}

// stubDials swaps DialBoard's dial seams for spies; the originals are
// restored on cleanup.
func stubDials(t *testing.T) *dialSpy {
	t.Helper()
	s := &dialSpy{}
	origTCP, origUSB := dialTCP, dialUSB
	dialTCP = func(hostport string, o replclient.DialOpts) (*replclient.Client, error) {
		s.tcp = append(s.tcp, hostport)
		return nil, errors.New("spy: no real dial")
	}
	dialUSB = func(path string, o replclient.DialOpts) (*replclient.Client, error) {
		s.usb = append(s.usb, path)
		return nil, errors.New("spy: no real dial")
	}
	t.Cleanup(func() { dialTCP, dialUSB = origTCP, origUSB })
	return s
}

// TestDialBoardIPFirst: T10 — when the browse filled Board.IP, the TCP
// face dials the ADDRESS, not the hostname (CGO=0 pure resolvers cannot
// resolve .local; the old hostname-only dial SERVFAILs without the
// /etc/hosts pin). USB stays the preferred face; an empty IP falls back
// to the hostname (the pre-T10 behavior); neither is the no-source error.
func TestDialBoardIPFirst(t *testing.T) {
	spy := stubDials(t)
	b := Board{MAC6: "d75884", Sources: []string{"mdns"},
		Host: "xuansi-d75884.local", IP: "10.1.51.205", Port: 7777}
	if _, err := DialBoard(b, replclient.DialOpts{}); err == nil {
		t.Fatal("spy dial returns an error; DialBoard must surface it")
	}
	if len(spy.tcp) != 1 || spy.tcp[0] != "10.1.51.205:7777" {
		t.Errorf("TCP dialed %v, want exactly [10.1.51.205:7777] (IP first, hostname NOT dialed)", spy.tcp)
	}
	if len(spy.usb) != 0 {
		t.Errorf("USB dialed %v, want none for an mdns-only board", spy.usb)
	}
}

func TestDialBoardEmptyIPFallsBackToHost(t *testing.T) {
	spy := stubDials(t)
	b := Board{MAC6: "d75884", Sources: []string{"mdns"},
		Host: "xuansi-d75884.local", Port: 7777} // no A record ever seen
	if _, err := DialBoard(b, replclient.DialOpts{}); err == nil {
		t.Fatal("spy dial returns an error; DialBoard must surface it")
	}
	if len(spy.tcp) != 1 || spy.tcp[0] != "xuansi-d75884.local:7777" {
		t.Errorf("TCP dialed %v, want [xuansi-d75884.local:7777] (hostname fallback)", spy.tcp)
	}
}

func TestDialBoardUSBStillPreferred(t *testing.T) {
	spy := stubDials(t)
	b := Board{MAC6: "d75884", Sources: []string{"usb", "mdns"},
		USBPath: usbA.USBPath, Host: "xuansi-d75884.local", IP: "10.1.51.205", Port: 7777}
	if _, err := DialBoard(b, replclient.DialOpts{}); err == nil {
		t.Fatal("spy dial returns an error; DialBoard must surface it")
	}
	if len(spy.usb) != 1 || spy.usb[0] != usbA.USBPath {
		t.Errorf("USB dialed %v, want [%s] (USB stays the preferred face)", spy.usb, usbA.USBPath)
	}
	if len(spy.tcp) != 0 {
		t.Errorf("TCP dialed %v, want none when USB is live", spy.tcp)
	}
}

func TestDialBoardNoSourceErrorUnchanged(t *testing.T) {
	spy := stubDials(t)
	for _, b := range []Board{
		{MAC6: "d75884", Sources: []string{"mdns"}, Port: 7777},                  // no host, no IP
		{MAC6: "d75884", Sources: []string{"mdns"}, IP: "10.1.51.205"},           // no port
		{MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local"}, // no port
		{MAC6: "d75884", Sources: []string{"usb"}},                               // no USBPath
	} {
		_, err := DialBoard(b, replclient.DialOpts{})
		if err == nil || !strings.Contains(err.Error(), "no live source") {
			t.Errorf("DialBoard(%#v) = %v, want the no-live-source error", b, err)
		}
	}
	if len(spy.tcp)+len(spy.usb) != 0 {
		t.Errorf("no dial may happen, got tcp=%v usb=%v", spy.tcp, spy.usb)
	}
}

// TestRefreshKeepsLastKnownIP: Board.IP is last-known mdns-face data
// like Host/Port/TXT — a later sweep whose packets carried no A record
// keeps the previously seen address, a NEW address wins (the DHCP-drift
// shape the /etc/hosts pin could not survive), and an mdns failure
// freezes the whole face including the IP.
func TestRefreshKeepsLastKnownIP(t *testing.T) {
	r, f := newFakeRegistry(nil)
	withIP := mdnsA
	withIP.IP = "10.1.51.205"
	f.set([]Board{usbA}, []Board{withIP})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 1: %v", err)
	}
	if b := r.Snapshot()[0]; b.IP != "10.1.51.205" {
		t.Fatalf("sweep with A record: IP = %q, want 10.1.51.205", b.IP)
	}

	// sweep sees the service again but no A record this time: keep last-known
	f.set([]Board{usbA}, []Board{mdnsA}) // mdnsA carries no IP
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	if b := r.Snapshot()[0]; b.IP != "10.1.51.205" {
		t.Errorf("sweep without A record erased last-known IP: %#v", b)
	}

	// DHCP drift: a NEW address must win over the stale one
	drift := mdnsA
	drift.IP = "10.1.51.99"
	f.set([]Board{usbA}, []Board{drift})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 3: %v", err)
	}
	if b := r.Snapshot()[0]; b.IP != "10.1.51.99" {
		t.Errorf("drifted address did not win: IP = %q, want 10.1.51.99", b.IP)
	}

	// mdns failure: the face (IP included) freezes at last-known
	f.failMDNS(errors.New("no multicast interface available"))
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 4 (mdns failure must not fail Refresh): %v", err)
	}
	b := r.Snapshot()[0]
	if b.IP != "10.1.51.99" || !b.hasSource(SourceMDNS) {
		t.Errorf("degraded refresh lost the mdns face/IP: %#v", b)
	}
}

// TestResolveErrorListsIP: the candidate display line carries the IP
// next to the hostname (the dialable address is part of the face).
func TestResolveErrorListsIP(t *testing.T) {
	r, f := newFakeRegistry(nil)
	withIP := mdnsA
	withIP.IP = "10.1.51.205"
	f.set(nil, []Board{withIP})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	_, err := r.Resolve("zzzz")
	if err == nil {
		t.Fatal("Resolve(unmatched) must error")
	}
	if !strings.Contains(err.Error(), "10.1.51.205") {
		t.Errorf("Resolve error must list the board IP: %q", err.Error())
	}
}

func TestLoadBoardsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "boards.conf")
	content := "" +
		"# bench boards (name=mac6)\n" +
		"\n" +
		"bench=d75884\n" +
		"  cam = 1A2B3C  \n" + // spaces + uppercase normalized
		"no-separator-line\n" + // malformed: skipped with warning
		"=d75884\n" + // empty name: skipped
		"ghost=abc\n" + // not 6-hex mac6: skipped
		"dup=d75884\n" + // duplicate mac6: last line wins
		"# trailing comment\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _, err := LoadBoardsFile(path)
	if err != nil {
		t.Fatalf("LoadBoardsFile: %v", err)
	}
	want := map[string]string{"d75884": "dup", "1a2b3c": "cam"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LoadBoardsFile = %#v, want %#v", got, want)
	}

	// missing file = no aliases (zero-config), not an error
	got, _, err = LoadBoardsFile(filepath.Join(dir, "absent.conf"))
	if err != nil || len(got) != 0 {
		t.Errorf("missing file: got (%#v, %v), want (empty, nil)", got, err)
	}
}

// Snapshot must be a stable sorted copy: mutating the returned TXT map
// must not corrupt the registry, and repeated calls agree.
func TestSnapshotIsolation(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set([]Board{usbA}, []Board{mdnsA})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	s1 := r.Snapshot()
	s1[0].TXT["ver"] = "mutated"
	// Sources is a slice too: a caller mutating the returned view must
	// not reach the registry's own entry (aliasing the backing array).
	s1[0].Sources[0] = "tampered"
	s1[0].Sources = append(s1[0].Sources, "extra")
	s2 := r.Snapshot()
	if s2[0].TXT["ver"] != "3" {
		t.Error("Snapshot leaks internal TXT map")
	}
	if !reflect.DeepEqual(r.Snapshot(), r.Snapshot()) {
		t.Error("Snapshot not deterministic")
	}

	if got := fmt.Sprint(s2[0].Sources); got != "[usb mdns]" {
		t.Errorf("sources = %s, want [usb mdns] (Snapshot leaks the Sources array)", got)
	}
}

// ------------------------------------------------------------------ pin (⑨) --

// TestPinInjectsLiveEntry: Pin materializes the registry entry
// IMMEDIATELY — no discovery sweep needed (the dogfood shape: a
// cross-subnet board mDNS cannot see gets pinned, then leased straight
// away). The entry carries exactly the pin source and endpoint, and the
// alias fills when boards.conf knows the mac6.
func TestPinInjectsLiveEntry(t *testing.T) {
	r, _ := newFakeRegistry(map[string]string{"d75884": "bench"})
	if err := r.Pin("d75884", "10.1.11.204", 7777); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	got := r.Snapshot()
	if len(got) != 1 {
		t.Fatalf("Snapshot = %#v, want the pinned entry immediately", got)
	}
	b := got[0]
	if b.MAC6 != "d75884" || b.Alias != "bench" {
		t.Errorf("entry = %#v, want mac6 d75884 alias bench", b)
	}
	if !reflect.DeepEqual(b.Sources, []string{SourcePin}) {
		t.Errorf("sources = %v, want [pin]", b.Sources)
	}
	if b.IP != "10.1.11.204" || b.Port != 7777 {
		t.Errorf("endpoint = %s:%d, want 10.1.11.204:7777", b.IP, b.Port)
	}
}

// TestPinSurvivesEmptySweeps: the pin is a LIVE source by declaration —
// a pinned board stays dialable across refreshes even when neither
// discovery face has ever seen it (the cross-subnet bench: mDNS
// unreachable from this host, USB unplugged).
func TestPinSurvivesEmptySweeps(t *testing.T) {
	r, f := newFakeRegistry(nil)
	f.set(nil, nil)
	if err := r.Pin("d75884", "10.1.11.204", 7777); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := r.Refresh(context.Background()); err != nil {
			t.Fatalf("Refresh %d: %v", i, err)
		}
	}
	b, err := r.Resolve("d75884")
	if err != nil {
		t.Fatalf("Resolve after refreshes: %v", err)
	}
	if !reflect.DeepEqual(b.Sources, []string{SourcePin}) || b.IP != "10.1.11.204" || b.Port != 7777 {
		t.Errorf("pinned entry after empty sweeps = %#v, want [pin] at 10.1.11.204:7777", b)
	}
}

// TestPinEndpointWinsOverDiscovery: the pinned endpoint is the user's
// explicit intent for a board discovery gets wrong — it overrides the
// mdns face's address/port in the merged entry while every other face
// (TXT, USB path) still merges. Canonical source order stays
// usb < pin < mdns.
func TestPinEndpointWinsOverDiscovery(t *testing.T) {
	r, f := newFakeRegistry(nil)
	if err := r.Pin("d75884", "10.1.11.204", 7777); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	drifted := mdnsA
	drifted.IP = "10.1.51.205"
	f.set([]Board{usbA}, []Board{drifted})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	b, err := r.Resolve("d75884")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if b.IP != "10.1.11.204" || b.Port != 7777 {
		t.Errorf("endpoint = %s:%d, want the PINNED 10.1.11.204:7777 (pin beats discovery)", b.IP, b.Port)
	}
	if !reflect.DeepEqual(b.Sources, []string{SourceUSB, SourcePin, SourceMDNS}) {
		t.Errorf("sources = %v, want [usb pin mdns] (canonical order)", b.Sources)
	}
	if b.USBPath != usbA.USBPath || b.Host != "xuansi-d75884.local" {
		t.Errorf("other faces lost in the merge: %#v", b)
	}
}

// TestDialBoardPriorityUSBoverPinOverMDNS: the dial order is
// usb > pin > mdns (B15 §4.9) — USB when its face is live, else the TCP
// endpoint with the pin's address winning the slot over mdns's.
func TestDialBoardPriorityUSBoverPinOverMDNS(t *testing.T) {
	// all three live: USB wins
	spy := stubDials(t)
	b := Board{MAC6: "d75884", Sources: []string{"usb", "pin", "mdns"},
		USBPath: usbA.USBPath, Host: "xuansi-d75884.local", IP: "10.1.11.204", Port: 7777}
	if _, err := DialBoard(b, replclient.DialOpts{}); err == nil {
		t.Fatal("spy dial returns an error; DialBoard must surface it")
	}
	if len(spy.usb) != 1 || len(spy.tcp) != 0 {
		t.Errorf("dialed usb=%v tcp=%v, want exactly one USB dial (usb > pin > mdns)", spy.usb, spy.tcp)
	}

	// pin + mdns (the cross-subnet shape): the pinned address is dialed
	spy = stubDials(t)
	b = Board{MAC6: "d75884", Sources: []string{"pin", "mdns"},
		Host: "xuansi-d75884.local", IP: "10.1.11.204", Port: 7777}
	if _, err := DialBoard(b, replclient.DialOpts{}); err == nil {
		t.Fatal("spy dial returns an error; DialBoard must surface it")
	}
	if len(spy.tcp) != 1 || spy.tcp[0] != "10.1.11.204:7777" {
		t.Errorf("TCP dialed %v, want [10.1.11.204:7777] over the pin source", spy.tcp)
	}
}

// TestUnpinDowngradesEntry: unpin removes exactly the pin source — a
// board another face still sees keeps that face, a pin-only board
// degrades to last-known (empty Sources — stale, not deleted; entries
// never hard-delete). Unpinning a never-pinned mac6 reports false.
func TestUnpinDowngradesEntry(t *testing.T) {
	r, f := newFakeRegistry(nil)
	if err := r.Pin("d75884", "10.1.11.204", 7777); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	if err := r.Pin("1a2b3c", "10.1.11.9", 7777); err != nil {
		t.Fatalf("Pin 2: %v", err)
	}
	f.set(nil, []Board{mdnsA}) // d75884 mdns face returns; 1a2b3c stays pin-only
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !r.Unpin("d75884") {
		t.Fatal("Unpin(d75884) = false, want true (it was pinned)")
	}
	if !r.Unpin("1a2b3c") {
		t.Fatal("Unpin(1a2b3c) = false, want true (it was pinned)")
	}
	b, _ := r.Resolve("d75884")
	if !reflect.DeepEqual(b.Sources, []string{SourceMDNS}) {
		t.Errorf("dual-source board after unpin = %v, want [mdns] only", b.Sources)
	}
	b2, _ := r.Resolve("1a2b3c")
	if len(b2.Sources) != 0 {
		t.Errorf("pin-only board after unpin = %v, want empty (stale last-known)", b2.Sources)
	}
	if r.Unpin("1a2b3c") {
		t.Error("Unpin of a never-pinned board = true, want false")
	}
}

// TestPinValidation: Pin refuses a non-mac6 id, an empty host, and a
// port outside 1..65535 — the daemon surfaces these as server-class
// errors, so the registry must be the validator.
func TestPinValidation(t *testing.T) {
	r, _ := newFakeRegistry(nil)
	for _, tc := range []struct {
		mac6, host string
		port       int
		wantIn     string
	}{
		{"d7588", "10.1.11.204", 7777, "mac6"},
		{"d7588z", "10.1.11.204", 7777, "mac6"},
		{"d75884", "", 7777, "host"},
		{"d75884", "10.1.11.204", 0, "port"},
		{"d75884", "10.1.11.204", 65536, "port"},
	} {
		err := r.Pin(tc.mac6, tc.host, tc.port)
		if err == nil || !strings.Contains(err.Error(), tc.wantIn) {
			t.Errorf("Pin(%q,%q,%d) = %v, want an error naming %q", tc.mac6, tc.host, tc.port, err, tc.wantIn)
		}
	}
	if len(r.Snapshot()) != 0 {
		t.Errorf("a refused pin must not materialize an entry: %#v", r.Snapshot())
	}
}

// TestNewRegistrySeedsPins: pins passed at construction (the
// boards.conf `name=mac6@ip:port` seed) behave exactly like runtime
// pins — the first sweep materializes their entries even with empty
// discovery.
func TestNewRegistrySeedsPins(t *testing.T) {
	f := &fakeSources{}
	r := NewRegistry(nil, map[string]Pin{"d75884": {Host: "10.1.11.204", Port: 7777}})
	r.usb, r.mdns = f.usbFn, f.mdnsFn
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	b, err := r.Resolve("d75884")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !reflect.DeepEqual(b.Sources, []string{SourcePin}) || b.IP != "10.1.11.204" || b.Port != 7777 {
		t.Errorf("seeded pin entry = %#v, want [pin] at 10.1.11.204:7777", b)
	}
}

// TestLoadBoardsFileEndpoint: the `name=mac6@ip:port` line (B15 §4.9's
// daemon static seed) parses into alias + pin; plain `name=mac6` lines
// keep working (backward compat); malformed endpoints are skipped with
// a warning like every other malformed line.
func TestLoadBoardsFileEndpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "boards.conf")
	content := "" +
		"bench=d75884@10.1.11.204:7777\n" + // pin seed: alias + endpoint
		"cam=1a2b3c\n" + // plain alias line: unchanged
		"v6=abcdef@[fe80::1]:7777\n" + // bracketed v6 endpoint
		"bad=d7588f@10.1.11.204\n" + // no port: skipped
		"bad2=d7588e@10.1.11.204:0\n" + // port 0: skipped
		"bad3=d7588d@10.1.11.204:99999\n" + // port out of range: skipped
		"bad4=d7588c@\n" // empty endpoint: skipped
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	aliases, pins, err := LoadBoardsFile(path)
	if err != nil {
		t.Fatalf("LoadBoardsFile: %v", err)
	}
	wantAliases := map[string]string{"d75884": "bench", "1a2b3c": "cam", "abcdef": "v6", "d7588f": "bad", "d7588e": "bad2", "d7588d": "bad3", "d7588c": "bad4"}
	if !reflect.DeepEqual(aliases, wantAliases) {
		t.Errorf("aliases = %#v, want %#v (a bad ENDPOINT still names the board)", aliases, wantAliases)
	}
	wantPins := map[string]Pin{
		"d75884": {Host: "10.1.11.204", Port: 7777},
		"abcdef": {Host: "fe80::1", Port: 7777},
	}
	if !reflect.DeepEqual(pins, wantPins) {
		t.Errorf("pins = %#v, want %#v", pins, wantPins)
	}

	// missing file = zero-config, not an error
	aliases, pins, err = LoadBoardsFile(filepath.Join(dir, "absent.conf"))
	if err != nil || len(aliases) != 0 || len(pins) != 0 {
		t.Errorf("missing file: got (%#v, %#v, %v), want (empty, empty, nil)", aliases, pins, err)
	}
}
