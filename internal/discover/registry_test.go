package discover

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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
	// Every registry entry states its model verdict — a conf-less,
	// never-crossed board says so ("unknown"), it never carries a
	// blank that reads as "not resolved yet".
	want := []Board{
		{
			MAC6: "1a2b3c", Sources: []string{"usb", "mdns"}, USBPath: usbB.USBPath, ModelSource: ModelSourceUnknown,
			Host: "xuansi-1a2b3c.local", Port: 7777, TXT: map[string]string{"ver": "3", "api": "1"},
		},
		{
			MAC6: "d75884", Sources: []string{"usb", "mdns"}, USBPath: usbA.USBPath, ModelSource: ModelSourceUnknown,
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
	got, _, _, err := LoadBoardsFile(path)
	if err != nil {
		t.Fatalf("LoadBoardsFile: %v", err)
	}
	want := map[string]string{"d75884": "dup", "1a2b3c": "cam"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LoadBoardsFile = %#v, want %#v", got, want)
	}

	// missing file = no aliases (zero-config), not an error
	got, _, _, err = LoadBoardsFile(filepath.Join(dir, "absent.conf"))
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
	aliases, pins, models, err := LoadBoardsFile(path)
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
	if len(models) != 0 {
		t.Errorf("token-less lines grew models: %#v", models)
	}

	// missing file = zero-config, not an error
	aliases, pins, models, err = LoadBoardsFile(filepath.Join(dir, "absent.conf"))
	if err != nil || len(aliases) != 0 || len(pins) != 0 || len(models) != 0 {
		t.Errorf("missing file: got (%#v, %#v, %#v, %v), want (empty, empty, empty, nil)", aliases, pins, models, err)
	}
}

// ---------------------------------------------------------------------------
// boards.conf key=value tokens: the `model=` override
// ---------------------------------------------------------------------------

// captureLogs swaps the default slog for a buffer for the test's
// duration (restored on cleanup) — the hand-edited-file discipline
// ("skip with a logged warning, never fail the load") is part of the
// contract, so the warnings are asserted, not just tolerated.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &buf
}

// TestLoadBoardsFileModelToken: the key=value token tail. The three
// line shapes (`name=mac6` / `name=mac6@ip:port` / either plus
// space-separated tokens) × legal / illegal / absent `model=` token,
// endpoint coexistence included. The first LEGAL model= token on the
// line is the board's conf model; an illegal one (outside
// ^[a-z0-9][a-z0-9.-]{0,28}$) is skipped with a warning and never
// costs the board its alias or pin — the malformed-line discipline.
func TestLoadBoardsFileModelToken(t *testing.T) {
	logs := captureLogs(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "boards.conf")
	atCeiling := strings.Repeat("m", 29)   // the domain's ceiling…
	overCeiling := strings.Repeat("m", 30) // …and one past it
	content := "" +
		"# the model= token matrix\n" +
		"bench=d75884 model=goouuu-s3cam-v1.3\n" + // plain alias + legal model
		"cam=1a2b3c@10.1.11.204:7777 model=esp32s3-dev\n" + // endpoint + legal model: alias, pin AND model coexist
		"v6=abcdef@[fe80::1]:7777 model=v6.board-1\n" + // bracketed v6 endpoint rides along
		"plain=d75885\n" + // plain alias, no token: alias only
		"pinned=d75886@10.1.11.205:7777\n" + // endpoint, no token: alias + pin only
		"  spaced  =  D75887   model=spaced-1  \n" + // tolerated spacing; mac6 case-normalized, model verbatim
		"spaceat=d75899 @10.1.11.207:7777 model=spaceat-1\n" + // the old parser's tolerated space before the @ survives the token
		"upper=d7588a model=Goouuu-S3\n" + // illegal: uppercase
		"under=d7588b model=goouuu_s3\n" + // illegal: underscore
		"empty=d7588c model=\n" + // illegal: empty value
		"lead=d7588d model=-board\n" + // illegal: leading '-'
		"uni=d7588e model=板1\n" + // illegal: non-ASCII
		"long=d7588f model=" + overCeiling + "\n" + // illegal: 30 chars
		"first=d75890 model=first-1 model=second-2\n" + // the first legal token wins
		"recover=d75891 model=BAD model=good-1\n" + // the illegal token is skipped; the first LEGAL one lands
		"ceil=d75892 model=" + atCeiling + "\n" + // 29 chars: inside the domain
		"mixed=d75893 foo=bar model=kept-1\n" + // an unknown key rides along; the model still lands
		"bare=d75894 model=kept-2 stray\n" + // a bare word rides along too
		"epbad=d75897@10.1.11.204 model=after-ep\n" + // bad endpoint: alias + model stay, no pin
		"epbad2=d75898@10.1.11.206:7777 model=BAD\n" + // endpoint + illegal model: alias + pin stay
		"badmac=zzzzzz model=never-1\n" + // malformed line: nothing lands, tokens and all
		"=d75895 model=never-2\n" + // empty name: malformed line
		"dup=d75896 model=old-1\n" +
		"dup2=d75896 model=new-1\n" + // duplicate mac6: the last line wins, model included
		"dup3=d75896\n" // a later token-less line moves the alias only — like an earlier pin, an earlier model is not cleared
	// CRLF line endings: the shape a Windows hand-edit lands in, new grammar and all.
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(content, "\n", "\r\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	aliases, pins, models, err := LoadBoardsFile(path)
	if err != nil {
		t.Fatalf("LoadBoardsFile: %v", err)
	}
	wantAliases := map[string]string{
		"d75884": "bench", "1a2b3c": "cam", "abcdef": "v6", "d75885": "plain", "d75886": "pinned",
		"d75887": "spaced", "d75899": "spaceat", "d7588a": "upper", "d7588b": "under", "d7588c": "empty", "d7588d": "lead",
		"d7588e": "uni", "d7588f": "long", "d75890": "first", "d75891": "recover", "d75892": "ceil",
		"d75893": "mixed", "d75894": "bare", "d75896": "dup3", "d75897": "epbad", "d75898": "epbad2",
	}
	if !reflect.DeepEqual(aliases, wantAliases) {
		t.Errorf("aliases = %#v\nwant %#v (a skipped token never costs the alias)", aliases, wantAliases)
	}
	wantPins := map[string]Pin{
		"1a2b3c": {Host: "10.1.11.204", Port: 7777},
		"abcdef": {Host: "fe80::1", Port: 7777},
		"d75886": {Host: "10.1.11.205", Port: 7777},
		"d75898": {Host: "10.1.11.206", Port: 7777},
		"d75899": {Host: "10.1.11.207", Port: 7777},
	}
	if !reflect.DeepEqual(pins, wantPins) {
		t.Errorf("pins = %#v, want %#v", pins, wantPins)
	}
	wantModels := map[string]string{
		"d75884": "goouuu-s3cam-v1.3", "1a2b3c": "esp32s3-dev", "abcdef": "v6.board-1",
		"d75887": "spaced-1", "d75890": "first-1", "d75891": "good-1", "d75892": atCeiling,
		"d75893": "kept-1", "d75894": "kept-2", "d75896": "new-1", "d75897": "after-ep", "d75899": "spaceat-1",
	}
	if !reflect.DeepEqual(models, wantModels) {
		t.Errorf("models = %#v\nwant %#v", models, wantModels)
	}
	// every skipped token warned exactly once, by class: the eight
	// illegal values (six illegal-only lines + the BAD token on the
	// recover and epbad2 lines), the one unknown key, the one stray
	// word, the one later legal model token.
	for msg, want := range map[string]int{
		"skipping illegal model= token":      8,
		"skipping token with an unknown key": 1,
		"skipping stray token":               1,
		"ignoring a later model= token":      1,
	} {
		if n := strings.Count(logs.String(), msg); n != want {
			t.Errorf("%q warnings = %d, want %d:\n%s", msg, n, want, logs.String())
		}
	}

	// missing file = zero-config: no models either, no error
	_, _, models, err = LoadBoardsFile(filepath.Join(dir, "absent.conf"))
	if err != nil || len(models) != 0 {
		t.Errorf("missing file: models = %#v, %v, want (empty, nil)", models, err)
	}
}

// legacyLoadBoardsFile is boards.conf's PRE-token parser, frozen
// verbatim from the last commit before the key=value tail landed. It
// is the oracle of the zero-drift claim below: a line that carries no
// key=value token has no business parsing differently than it did then.
func legacyLoadBoardsFile(path string) (map[string]string, map[string]Pin, error) {
	out := map[string]string{}
	pins := map[string]Pin{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, pins, nil
		}
		return nil, nil, fmt.Errorf("discover: read %s: %w", path, err)
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		name = strings.TrimSpace(name)
		value = strings.ToLower(strings.TrimSpace(value))
		mac6, endpoint, hasEndpoint := strings.Cut(value, "@")
		mac6 = strings.TrimSpace(mac6)
		if !ok || name == "" || !isMac6(mac6) {
			slog.Warn("discover: boards.conf: skipping malformed line", "file", path, "line", i+1, "text", line)
			continue
		}
		out[mac6] = name
		if !hasEndpoint {
			continue // the classic alias-only line: nothing more to read
		}
		host, portStr, err := net.SplitHostPort(endpoint)
		if err != nil {
			slog.Warn("discover: boards.conf: skipping malformed pin endpoint (want mac6@ip:port)", "file", path, "line", i+1, "text", line, "err", err)
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port < 1 || port > 65535 || host == "" {
			slog.Warn("discover: boards.conf: skipping malformed pin endpoint (want mac6@ip:port)", "file", path, "line", i+1, "text", line)
			continue
		}
		pins[mac6] = Pin{Host: host, Port: port}
	}
	return out, pins, nil
}

// oldShapeLines renders n token-less boards.conf lines from a fixed
// seed: the documented shapes plus every tolerated-spacing and
// malformed-endpoint quirk the old parser ever had, comments and
// blanks included. By construction not one of them carries a '='
// beyond the name's own separator — every line is old-shape.
func oldShapeLines(n int) []string {
	rng := rand.New(rand.NewSource(20261123))
	pick := func(from ...string) string { return from[rng.Intn(len(from))] }
	spaces := []string{"", " ", "  ", "\t", " \t "}
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		line := pick(spaces...) + pick("bench", "cam", "a-b", "x_y", "", "two words") +
			pick(spaces...) + "=" + pick(spaces...) +
			pick("d75884", "D75884", "1A2B3C", "abcdef", "0F0F0F", "d7588", "d7588z", "12345", "1234567") +
			pick("", " ") + // the tolerated space before the @ (and its junk-host twin, the space after it)
			pick("", "@10.1.11.204:7777", "@[fe80::1]:7777", "@", "@10.1.11.204", "@10.1.11.204:0",
				"@10.1.11.204:99999", "@lab-host.example:7", "@:7777", "@ 10.1.11.5:77") +
			pick("", " ", "  # a note", " stray word", " @10.1.1.1:7", "@", " :", "  #", " # tag:note") +
			pick(spaces...)
		switch rng.Intn(12) {
		case 0:
			line = "#" + line
		case 1:
			line = ""
		}
		lines = append(lines, line)
	}
	return lines
}

// TestLoadBoardsFileOldShapesIdentical: backward compatibility is
// byte-identical, not "close enough". Every token-less line — the
// documented shapes AND the spacing quirks the old parser tolerated
// into junk hosts — must yield exactly the alias and pin maps the
// frozen pre-token parser yields, and grow no model. Curated corpus
// (CRLF, as a Windows hand-edit lands) plus 20k seeded lines.
func TestLoadBoardsFileOldShapesIdentical(t *testing.T) {
	captureLogs(t) // the malformed corpus warns by design; keep it out of the test output
	cases := map[string]struct {
		lines []string
		sep   string
	}{
		"curated (CRLF)": {sep: "\r\n", lines: []string{
			"# bench boards",
			"",
			"bench=d75884",
			"cam = 1A2B3C  ",
			"v6=abcdef@[fe80::1]:7777",
			"spaceat=d7588f @10.1.11.204:7777", // tolerated space before the @: a valid pin
			"bad=d7588e@10.1.11.204",           // no port
			"bad2=d7588d@10.1.11.204:0",        // port 0
			"quirk=d7588c@ 10.1.11.8:77",       // the old parser's tolerance: a pin whose host is " 10.1.11.8"
			"quirk2=d7588b @ 10.1.11.7:77",     // …host " 10.1.11.7"
			"quirk3=d7588a@10.1.11.6:77 stray", // trailing junk after a good endpoint: alias, no pin
			"trailing=d75889 stray",            // trailing junk on an alias line: the whole line is malformed
			"inline=d75888 # not a comment in the old grammar",
			"no-separator-line",
			"=d75884",
			"ghost=abc",
			"dup=d75884",
			"# trailing comment",
		}},
		"seeded (20k lines)": {sep: "\n", lines: oldShapeLines(20000)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "boards.conf")
			if err := os.WriteFile(path, []byte(strings.Join(tc.lines, tc.sep)), 0o644); err != nil {
				t.Fatal(err)
			}
			gotAliases, gotPins, models, err := LoadBoardsFile(path)
			if err != nil {
				t.Fatalf("LoadBoardsFile: %v", err)
			}
			wantAliases, wantPins, lerr := legacyLoadBoardsFile(path)
			if lerr != nil {
				t.Fatalf("legacy parse: %v", lerr)
			}
			if !reflect.DeepEqual(gotAliases, wantAliases) {
				t.Errorf("aliases drifted from the pre-token parser:\n got %#v\nwant %#v", gotAliases, wantAliases)
			}
			if !reflect.DeepEqual(gotPins, wantPins) {
				t.Errorf("pins drifted from the pre-token parser:\n got %#v\nwant %#v", gotPins, wantPins)
			}
			if len(models) != 0 {
				t.Errorf("token-less lines grew models: %#v", models)
			}
			// the oracle must have had something to say — an all-malformed
			// corpus would pass the equality for free
			if len(wantAliases) == 0 {
				t.Fatalf("the corpus parsed to nothing on BOTH parsers — the comparison proves nothing")
			}
			if tc.sep == "\r\n" {
				// …quirks included: the tolerated and the junk-host lines
				// really do pin on the old parser, so their identity is
				// not vacuous
				for mac, want := range map[string]Pin{
					"d7588f": {Host: "10.1.11.204", Port: 7777},
					"d7588c": {Host: " 10.1.11.8", Port: 77},
				} {
					if got, ok := wantPins[mac]; !ok || got != want {
						t.Fatalf("oracle lost its quirk: pins[%s] = %#v, want %#v", mac, got, want)
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// the model chain on the registry: conf → eFuse record (model table) → unknown
// ---------------------------------------------------------------------------

// modelOf resolves one board and answers its model pair.
func modelOf(t *testing.T, r *Registry, mac6 string) (model, source string) {
	t.Helper()
	b, err := r.Resolve(mac6)
	if err != nil {
		t.Fatalf("Resolve(%s): %v", mac6, err)
	}
	return b.Model, b.ModelSource
}

// sighting is one hello sighting carrying the eFuse model record —
// what a board with a burned, locked model block answers.
func sighting(id uint32, date string) *replclient.HelloInfo {
	return &replclient.HelloInfo{FwBuild: "03aff57", HasModel: true, ModelID: id, MfgDate: date}
}

// entryOf resolves one board and answers its whole entry.
func entryOf(t *testing.T, r *Registry, mac6 string) Board {
	t.Helper()
	b, err := r.Resolve(mac6)
	if err != nil {
		t.Fatalf("Resolve(%s): %v", mac6, err)
	}
	return b
}

// checkRecord asserts one entry's raw eFuse record: absent (all zero),
// or present with exactly this id and date.
func checkRecord(t *testing.T, e Board, where string, wantID uint32, wantDate string) {
	t.Helper()
	if wantID == 0 && wantDate == "" {
		if e.HasEfuseModel || e.ModelID != 0 || e.MfgDate != "" {
			t.Errorf("%s: raw record = %v/0x%x/%q, want none", where, e.HasEfuseModel, e.ModelID, e.MfgDate)
		}
		return
	}
	if !e.HasEfuseModel || e.ModelID != wantID || e.MfgDate != wantDate {
		t.Errorf("%s: raw record = %v/0x%x/%q, want present/0x%x/%q", where, e.HasEfuseModel, e.ModelID, e.MfgDate, wantID, wantDate)
	}
}

// TestApplyModelTableHit: a record whose id is in the daemon's model
// table names the model — ModelSource "efuse" — and the raw record
// passes through beside it, verbatim.
func TestApplyModelTableHit(t *testing.T) {
	logs := captureLogs(t)
	r := NewRegistry(nil, nil, WithModelTable(map[uint32]string{0x2a7: "goouuu-s3cam-v1.3"}))
	e := Board{Hello: sighting(0x2a7, "2026-11-20")}
	r.applyModel("d75884", &e)
	if e.Model != "goouuu-s3cam-v1.3" || e.ModelSource != ModelSourceEfuse {
		t.Errorf("verdict = %q/%q, want goouuu-s3cam-v1.3/efuse", e.Model, e.ModelSource)
	}
	checkRecord(t, e, "table hit", 0x2a7, "2026-11-20")
	if strings.Contains(logs.String(), "predates the board") {
		t.Errorf("a table hit coached:\n%s", logs.String())
	}
}

// TestApplyModelTableMiss: a record whose id is NOT in the table is
// still the eFuse leg — ModelSource "efuse" with an EMPTY model — and
// the raw record still passes through (the id is real; only this
// daemon's table lacks it). The coach says so once per board and id:
// not once per sweep, and not again for the same sighting re-applied.
func TestApplyModelTableMiss(t *testing.T) {
	logs := captureLogs(t)
	coach := func() int { return strings.Count(logs.String(), "this daemon's model table predates the board") }
	r := NewRegistry(nil, nil, WithModelTable(map[uint32]string{1: "other-1"}))
	e := Board{Hello: sighting(0x2a7, "2026-11-20")}
	r.applyModel("d75884", &e)
	if e.Model != "" || e.ModelSource != ModelSourceEfuse {
		t.Errorf("verdict = %q/%q, want empty/efuse (the record is real, the table lacks it)", e.Model, e.ModelSource)
	}
	checkRecord(t, e, "table miss", 0x2a7, "2026-11-20")
	if coach() != 1 {
		t.Fatalf("coach lines = %d, want 1:\n%s", coach(), logs.String())
	}
	for _, want := range []string{"unknown model_id", "0x0002a7", "d75884"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the coach lacks %q:\n%s", want, logs.String())
		}
	}
	// the next sweep re-resolves the very same entry: no second coach
	r.applyModel("d75884", &e)
	// …as does a fresh copy of it (every sweep builds a new entry map)
	again := e
	r.applyModel("d75884", &again)
	if e.Model != "" || e.ModelSource != ModelSourceEfuse || coach() != 1 {
		t.Errorf("re-applied miss = %q/%q with %d coach lines, want empty/efuse and still 1", e.Model, e.ModelSource, coach())
	}
	// another id is another coach
	e.Hello = sighting(0x2a8, "2026-11-20")
	r.applyModel("d75884", &e)
	if coach() != 2 {
		t.Errorf("coach lines after a second unknown id = %d, want 2", coach())
	}
	// a record-less sighting clears the record, and coaches nothing
	e.Hello = &replclient.HelloInfo{}
	r.applyModel("d75884", &e)
	if e.Model != "" || e.ModelSource != ModelSourceUnknown || coach() != 2 {
		t.Errorf("record-less sighting = %q/%q with %d coach lines, want empty/unknown and still 2", e.Model, e.ModelSource, coach())
	}
	checkRecord(t, e, "after a record-less sighting", 0, "")
	// the record coming back is a new sighting, and coaches again
	e.Hello = sighting(0x2a8, "2026-11-20")
	r.applyModel("d75884", &e)
	if coach() != 3 {
		t.Errorf("coach lines after the record returned = %d, want 3", coach())
	}
}

// TestConfOverridesEfuse: the conf override outranks the eFuse leg —
// Model is the conf string with ModelSource "conf", whatever the table
// says or lacks — while the raw record still shows beside it; a table
// miss under an override is nothing to coach about.
func TestConfOverridesEfuse(t *testing.T) {
	logs := captureLogs(t)
	hit := NewRegistry(nil, nil,
		WithModels(map[string]string{"d75884": "conf-1"}),
		WithModelTable(map[uint32]string{0x2a7: "table-1"}))
	e := Board{Hello: sighting(0x2a7, "2026-11-20")}
	hit.applyModel("d75884", &e)
	if e.Model != "conf-1" || e.ModelSource != ModelSourceConf {
		t.Errorf("conf board with a table hit = %q/%q, want conf-1/conf (the override always wins)", e.Model, e.ModelSource)
	}
	checkRecord(t, e, "conf board", 0x2a7, "2026-11-20")

	miss := NewRegistry(nil, nil, WithModels(map[string]string{"d75884": "conf-1"})) // the factory-empty table
	e = Board{Hello: sighting(0x2a7, "2026-11-20")}
	miss.applyModel("d75884", &e)
	if e.Model != "conf-1" || e.ModelSource != ModelSourceConf {
		t.Errorf("conf board with a table miss = %q/%q, want conf-1/conf", e.Model, e.ModelSource)
	}
	checkRecord(t, e, "conf board, table miss", 0x2a7, "2026-11-20")
	if strings.Contains(logs.String(), "predates the board") {
		t.Errorf("a conf-overridden board was coached about the table:\n%s", logs.String())
	}

	// another board's override covers only that board
	other := NewRegistry(nil, nil, WithModels(map[string]string{"1a2b3c": "theirs"}))
	e = Board{Hello: sighting(0x2a7, "2026-11-20")}
	other.applyModel("d75884", &e)
	if e.Model != "" || e.ModelSource != ModelSourceEfuse {
		t.Errorf("record board with no override of its own = %q/%q, want empty/efuse", e.Model, e.ModelSource)
	}
}

// TestModelTableFactoryEmpty: the daemon's own table ships empty — no
// real allocation yet — so a registry built without WithModelTable
// misses every id. WithModelTable copies what it is given (a caller
// mutating its map afterwards changes nothing) and keeps only values
// in the model string domain, dropping the rest with a warning that
// names the id: the table is the one other door a model string enters
// the chain through, and `model` stays one string form or is withheld.
func TestModelTableFactoryEmpty(t *testing.T) {
	logs := captureLogs(t)
	if n := len(NewRegistry(nil, nil).modelTable); n != 0 {
		t.Fatalf("the factory model table holds %d entry(ies), want none until the first real allocation", n)
	}
	src := map[uint32]string{1: "ok-1", 2: "Bad_Value", 3: strings.Repeat("x", 30)}
	r := NewRegistry(nil, nil, WithModelTable(src))
	src[1] = "mutated-after"
	src[4] = "late"
	if got := r.modelTable; !reflect.DeepEqual(got, map[uint32]string{1: "ok-1"}) {
		t.Errorf("model table = %#v, want exactly {1: ok-1} (copied, validated)", got)
	}
	if n := strings.Count(logs.String(), "outside the model domain"); n != 2 {
		t.Errorf("out-of-domain warnings = %d, want 2:\n%s", n, logs.String())
	}
	for _, want := range []string{"0x000002", "0x000003"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the warnings lack the id %s:\n%s", want, logs.String())
		}
	}
}

// TestModelSourceChain: Board.Model/ModelSource resolve through the
// chain — the boards.conf override always wins; else the eFuse record
// the hello sighting carried is looked up in the daemon's model table
// (the eFuse leg, backfilled at Enrich); else unknown with NO model.
// The raw record rides beside the verdict whatever it is and survives
// refreshes with it; a sighting without one (older fw) clears it.
func TestModelSourceChain(t *testing.T) {
	logs := captureLogs(t)
	f := &fakeSources{}
	r := NewRegistry(nil, nil,
		WithModels(map[string]string{"d75884": "conf-model"}),
		WithModelTable(map[uint32]string{0x2a7: "table-1", 0x2a8: "table-2"}))
	r.usb, r.mdns = f.usbFn, f.mdnsFn
	f.set([]Board{usbA}, []Board{mdnsB})
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// conf: the override names the model before any sighting; no record yet
	e := entryOf(t, r, "d75884")
	if e.Model != "conf-model" || e.ModelSource != ModelSourceConf {
		t.Errorf("conf board = %q/%q, want conf-model/conf", e.Model, e.ModelSource)
	}
	checkRecord(t, e, "conf board before any sighting", 0, "")
	// unknown: no override, never crossed
	if m, src := modelOf(t, r, "1a2b3c"); m != "" || src != ModelSourceUnknown {
		t.Errorf("uncrossed board = %q/%q, want empty/unknown", m, src)
	}
	checkRecord(t, entryOf(t, r, "1a2b3c"), "uncrossed board", 0, "")
	// efuse: the sighting's record, looked up in the table
	r.Enrich("1a2b3c", sighting(0x2a7, "2026-11-20"))
	if m, src := modelOf(t, r, "1a2b3c"); m != "table-1" || src != ModelSourceEfuse {
		t.Errorf("enriched board = %q/%q, want table-1/efuse", m, src)
	}
	checkRecord(t, entryOf(t, r, "1a2b3c"), "enriched board", 0x2a7, "2026-11-20")
	// conf wins over a record — the raw identity still shows beside it
	r.Enrich("d75884", sighting(0x2a8, "2026-11-21"))
	if m, src := modelOf(t, r, "d75884"); m != "conf-model" || src != ModelSourceConf {
		t.Errorf("conf board after a record sighting = %q/%q, want conf-model/conf (the override always wins)", m, src)
	}
	checkRecord(t, entryOf(t, r, "d75884"), "conf board with a record", 0x2a8, "2026-11-21")
	// a record-less sighting (older fw) is not efuse: it un-claims and
	// clears the raw identity honestly
	r.Enrich("1a2b3c", &replclient.HelloInfo{FwBuild: "b310378"})
	if m, src := modelOf(t, r, "1a2b3c"); m != "" || src != ModelSourceUnknown {
		t.Errorf("board after a record-less sighting = %q/%q, want empty/unknown", m, src)
	}
	checkRecord(t, entryOf(t, r, "1a2b3c"), "after a record-less sighting", 0, "")
	// last-known: verdict and raw record survive a sweep that crosses
	// nothing (the boards go stale, their identity stays)
	r.Enrich("1a2b3c", sighting(0x2a7, "2026-11-20"))
	f.set(nil, nil)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	if m, src := modelOf(t, r, "1a2b3c"); m != "table-1" || src != ModelSourceEfuse {
		t.Errorf("stale board = %q/%q, want table-1/efuse kept", m, src)
	}
	checkRecord(t, entryOf(t, r, "1a2b3c"), "stale board", 0x2a7, "2026-11-20")
	if m, src := modelOf(t, r, "d75884"); m != "conf-model" || src != ModelSourceConf {
		t.Errorf("stale conf board = %q/%q, want conf-model/conf kept", m, src)
	}
	checkRecord(t, entryOf(t, r, "d75884"), "stale conf board", 0x2a8, "2026-11-21")
	// nothing above had a reason to coach: every id was in the table
	if strings.Contains(logs.String(), "predates the board") {
		t.Errorf("an in-table record coached:\n%s", logs.String())
	}

	// the production shape — no conf map, the factory-empty table: a
	// record is still the eFuse leg, with no model string to show
	r2, f2 := newFakeRegistry(nil)
	f2.set([]Board{usbA}, []Board{mdnsB})
	if err := r2.Refresh(context.Background()); err != nil {
		t.Fatalf("factory-table Refresh: %v", err)
	}
	for _, mac6 := range []string{"d75884", "1a2b3c"} {
		b := entryOf(t, r2, mac6)
		if b.Model != "" || b.ModelSource != ModelSourceUnknown {
			t.Errorf("factory-table %s = %q/%q, want empty/unknown", mac6, b.Model, b.ModelSource)
		}
		checkRecord(t, b, "factory-table "+mac6, 0, "")
	}
	r2.Enrich("1a2b3c", sighting(0x2a7, "2026-11-20"))
	if m, src := modelOf(t, r2, "1a2b3c"); m != "" || src != ModelSourceEfuse {
		t.Errorf("factory-table record board = %q/%q, want empty/efuse (a miss, coached)", m, src)
	}
	checkRecord(t, entryOf(t, r2, "1a2b3c"), "factory-table record board", 0x2a7, "2026-11-20")
	if n := strings.Count(logs.String(), "predates the board"); n != 1 {
		t.Errorf("coach lines = %d, want exactly the factory-table miss (1):\n%s", n, logs.String())
	}
	// …and said once: the sweeps that re-resolve the live entry (it is
	// still seen by both faces) are not news, and the miss outlives them
	for i := 0; i < 3; i++ {
		if err := r2.Refresh(context.Background()); err != nil {
			t.Fatalf("factory-table Refresh %d: %v", i+2, err)
		}
	}
	if n := strings.Count(logs.String(), "predates the board"); n != 1 {
		t.Errorf("coach lines after three sweeps = %d, want still 1 (once per board and id, not once per sweep):\n%s", n, logs.String())
	}
	if m, src := modelOf(t, r2, "1a2b3c"); m != "" || src != ModelSourceEfuse {
		t.Errorf("factory-table record board after sweeps = %q/%q, want empty/efuse kept", m, src)
	}
	checkRecord(t, entryOf(t, r2, "1a2b3c"), "factory-table record board after sweeps", 0x2a7, "2026-11-20")
}

// TestPinCarriesConfModel: both pin paths stamp the conf model — a
// runtime Pin materializes the entry with its verdict immediately (no
// sweep), and a seeded pin (the boards.conf `name=mac6@ip:port` line)
// picks it up on the first sweep. A pin without a conf model is
// unknown like any other board.
func TestPinCarriesConfModel(t *testing.T) {
	r := NewRegistry(nil, nil, WithModels(map[string]string{"abcdef": "pin-model"}))
	if err := r.Pin("abcdef", "10.1.11.204", 7777); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	if err := r.Pin("1a2b3c", "10.1.11.9", 7777); err != nil {
		t.Fatalf("Pin 2: %v", err)
	}
	if m, src := modelOf(t, r, "abcdef"); m != "pin-model" || src != ModelSourceConf {
		t.Errorf("runtime-pinned conf board = %q/%q, want pin-model/conf", m, src)
	}
	if m, src := modelOf(t, r, "1a2b3c"); m != "" || src != ModelSourceUnknown {
		t.Errorf("runtime-pinned plain board = %q/%q, want empty/unknown", m, src)
	}

	f := &fakeSources{}
	r2 := NewRegistry(nil, map[string]Pin{"abcdef": {Host: "10.1.11.204", Port: 7777}},
		WithModels(map[string]string{"abcdef": "seed-model"}))
	r2.usb, r2.mdns = f.usbFn, f.mdnsFn
	f.set(nil, nil)
	if err := r2.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if m, src := modelOf(t, r2, "abcdef"); m != "seed-model" || src != ModelSourceConf {
		t.Errorf("seeded-pin conf board = %q/%q, want seed-model/conf", m, src)
	}
}

// TestSplitTokenTail: where the key=value tail begins. Everything
// before the first token is the classic head, verbatim — quirks and
// all; the mac6 slot is never a token, and a line without a token is
// all head.
func TestSplitTokenTail(t *testing.T) {
	for _, tc := range []struct {
		line, head string
		tokens     []string
	}{
		{"bench=d75884", "bench=d75884", nil},
		{"bench=d75884 model=x", "bench=d75884", []string{"model=x"}},
		{"bench = d75884 @10.1.1.1:7 \t model=x  foo=bar", "bench = d75884 @10.1.1.1:7", []string{"model=x", "foo=bar"}},
		{"bench=d75884 @10.1.1.1:7", "bench=d75884 @10.1.1.1:7", nil}, // tolerated space before the @: no token
		{"bench=d75884 # a note", "bench=d75884 # a note", nil},       // an inline note is not a token
		{"bench=d75884 model", "bench=d75884 model", nil},             // a bare word is not a token
		{"bench=d75884 =x", "bench=d75884 =x", nil},                   // neither is a field that starts with '='
		{"x=d75884= model=y", "x=d75884=", []string{"model=y"}},       // the mac6 slot is never a token
		{"x=d75884@10.1.1.1:7= model=y", "x=d75884@10.1.1.1:7=", []string{"model=y"}},
		{"d75884 model=x", "d75884 model=x", nil}, // the name's '=' is the FIRST one: its slot is the token text
		{"=d75884 model=x", "=d75884", []string{"model=x"}},
		{"a=b=c", "a=b=c", nil},
		{"bench=d75884\u00a0model=x", "bench=d75884", []string{"model=x"}}, // whitespace is strings.Fields' notion, NBSP included
	} {
		head, tokens := splitTokenTail(tc.line)
		if head != tc.head || !reflect.DeepEqual(tokens, tc.tokens) {
			t.Errorf("splitTokenTail(%q) = (%q, %q), want (%q, %q)", tc.line, head, tokens, tc.head, tc.tokens)
		}
	}
}

// TestIsModel: the model STRING domain — ^[a-z0-9][a-z0-9.-]{0,28}$,
// 1..29 chars, lowercase alnum first, then lowercase alnum, '.' or '-'.
// It gates both doors a model string enters the chain through (the
// boards.conf model= token and the daemon's model table's values), so
// `model` is one string form wherever it comes from.
func TestIsModel(t *testing.T) {
	for _, s := range []string{"a", "0", "goouuu-s3cam-v1.3", "v6.board-1", "a.b-c.d", strings.Repeat("m", 29)} {
		if !isModel(s) {
			t.Errorf("isModel(%q) = false, want true", s)
		}
	}
	for _, s := range []string{
		"", strings.Repeat("m", 30), "-a", ".a", "A", "aB", "a_b", "a b", "a/b", "板", "a=b", "a\x00",
	} {
		if isModel(s) {
			t.Errorf("isModel(%q) = true, want false", s)
		}
	}
}
