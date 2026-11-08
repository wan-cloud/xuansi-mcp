package discover

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/grandcat/zeroconf"
)

func TestBoardFromEntry(t *testing.T) {
	cases := []struct {
		name  string
		entry *zeroconf.ServiceEntry
		want  Board
		ok    bool
	}{
		{
			name: "live bench service",
			entry: &zeroconf.ServiceEntry{
				ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
				HostName:      "xuansi-d75884.local.",
				Port:          7777,
				Text:          []string{"ver=3", "api=1"},
			},
			want: Board{
				MAC6:    "d75884",
				Sources: []string{"mdns"},
				Host:    "xuansi-d75884.local",
				Port:    7777,
				TXT:     map[string]string{"ver": "3", "api": "1"},
			},
			ok: true,
		},
		{
			name: "trailing dot stripped twice srv-style",
			entry: &zeroconf.ServiceEntry{
				ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-1a2b3c"},
				HostName:      "xuansi-1a2b3c.local.",
				Port:          7777,
				Text:          []string{"ver=3"},
			},
			want: Board{
				MAC6:    "1a2b3c",
				Sources: []string{"mdns"},
				Host:    "xuansi-1a2b3c.local",
				Port:    7777,
				TXT:     map[string]string{"ver": "3"},
			},
			ok: true,
		},
		{
			// SRV missing: fall back to the instance name (§14: instance = hostname 本體)
			name: "instance only",
			entry: &zeroconf.ServiceEntry{
				ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
				Port:          7777,
			},
			want: Board{
				MAC6:    "d75884",
				Sources: []string{"mdns"},
				Port:    7777,
				TXT:     map[string]string{},
			},
			ok: true,
		},
		{
			// TXT pair without "=" is skipped; the rest still parses
			name: "malformed txt pair skipped",
			entry: &zeroconf.ServiceEntry{
				HostName: "xuansi-d75884.local.",
				Port:     7777,
				Text:     []string{"ver=3", "novalue", "api=1"},
			},
			want: Board{
				MAC6:    "d75884",
				Sources: []string{"mdns"},
				Host:    "xuansi-d75884.local",
				Port:    7777,
				TXT:     map[string]string{"ver": "3", "api": "1"},
			},
			ok: true,
		},
		{
			name: "foreign hostname not ours",
			entry: &zeroconf.ServiceEntry{
				HostName: "living-room.local.",
				Port:     7777,
			},
			ok: false,
		},
		{
			name: "hostname without mac6",
			entry: &zeroconf.ServiceEntry{
				HostName: "xuansi-.local.",
			},
			ok: false,
		},
		{
			name: "mac6 too long",
			entry: &zeroconf.ServiceEntry{
				HostName: "xuansi-d758841.local.",
			},
			ok: false,
		},
		{
			name: "mac6 not hex",
			entry: &zeroconf.ServiceEntry{
				HostName: "xuansi-zzzzzz.local.",
			},
			ok: false,
		},
	}
	for _, c := range cases {
		got, ok := boardFromEntry(c.entry)
		if ok != c.ok {
			t.Errorf("%s: boardFromEntry ok = %v, want %v", c.name, ok, c.ok)
			continue
		}
		if !ok {
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: boardFromEntry =\n %#v\nwant\n %#v", c.name, got, c.want)
		}
	}
}

// TestBoardIPFromEntry: the browse entry's free A/AAAA record must ride
// into Board.IP (T10: CGO=0 pure resolvers cannot resolve .local, so
// the dial needs the address the browse already carried and used to
// discard). First IPv4 wins; without v4 the first IPv6; none -> empty
// (the dial falls back to the hostname).
func TestBoardIPFromEntry(t *testing.T) {
	cases := []struct {
		name   string
		entry  *zeroconf.ServiceEntry
		wantIP string
	}{
		{
			name: "ipv4 first entry",
			entry: &zeroconf.ServiceEntry{
				ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
				HostName:      "xuansi-d75884.local.",
				Port:          7777,
				AddrIPv4:      []net.IP{net.ParseIP("10.1.51.205"), net.ParseIP("10.1.51.206")},
			},
			wantIP: "10.1.51.205",
		},
		{
			name: "no v4 falls to first v6",
			entry: &zeroconf.ServiceEntry{
				ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
				HostName:      "xuansi-d75884.local.",
				Port:          7777,
				AddrIPv6:      []net.IP{net.ParseIP("fe80::1"), net.ParseIP("fe80::2")},
			},
			wantIP: "fe80::1",
		},
		{
			name: "v4 preferred over v6",
			entry: &zeroconf.ServiceEntry{
				ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
				HostName:      "xuansi-d75884.local.",
				Port:          7777,
				AddrIPv4:      []net.IP{net.ParseIP("10.1.51.205")},
				AddrIPv6:      []net.IP{net.ParseIP("fe80::1")},
			},
			wantIP: "10.1.51.205",
		},
		{
			name: "no addresses -> empty (hostname fallback)",
			entry: &zeroconf.ServiceEntry{
				ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
				HostName:      "xuansi-d75884.local.",
				Port:          7777,
			},
			wantIP: "",
		},
	}
	for _, c := range cases {
		got, ok := boardFromEntry(c.entry)
		if !ok {
			t.Errorf("%s: boardFromEntry rejected a valid entry", c.name)
			continue
		}
		if got.IP != c.wantIP {
			t.Errorf("%s: Board.IP = %q, want %q", c.name, got.IP, c.wantIP)
		}
	}
}

// A pre-canceled caller context must surface as an error without the
// resolver ever touching the network (pure ctx-driven entry check).
func TestDiscoverMDNSCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := DiscoverMDNS(ctx); err == nil {
		t.Error("canceled ctx: want error, got nil")
	}
}

// The library submits one entry per response packet; a service whose
// SRV+A and TXT land in separate packets must reassemble by window end
// regardless of arrival order, and TXT unions across packets.
func TestMDNSAccumMergesPartialEntries(t *testing.T) {
	partialSRVA := Board{MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local", Port: 7777}
	partialTXT := Board{MAC6: "d75884", Sources: []string{"mdns"}, TXT: map[string]string{"ver": "3"}}
	partialTXT2 := Board{MAC6: "d75884", Sources: []string{"mdns"}, TXT: map[string]string{"api": "1"}}

	for _, order := range [][]Board{
		{partialSRVA, partialTXT, partialTXT2}, // A-record first, TXT packets later
		{partialTXT, partialTXT2, partialSRVA}, // TXT first, SRV+A last
	} {
		a := mdnsAccum{}
		for _, b := range order {
			a.add(b)
		}
		got := a.snapshot()
		if len(got) != 1 {
			t.Fatalf("accum = %#v, want 1 board", got)
		}
		b := got[0]
		if b.Host != "xuansi-d75884.local" || b.Port != 7777 {
			t.Errorf("face incomplete after merge: %#v", b)
		}
		if b.TXT["ver"] != "3" || b.TXT["api"] != "1" {
			t.Errorf("TXT not unioned: %#v", b.TXT)
		}
		if !reflect.DeepEqual(b.Sources, []string{"mdns"}) {
			t.Errorf("sources = %#v", b.Sources)
		}
	}
}

// TestMDNSAccumMergesIPAcrossPackets: the A record can land in a
// separate packet from the SRV (the two-window split-packet bench
// shape) — the accumulator must merge the address into the face like
// any other field, in either arrival order.
func TestMDNSAccumMergesIPAcrossPackets(t *testing.T) {
	srv := Board{MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local", Port: 7777}
	aOnly := Board{MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local", IP: "10.1.51.205"}
	for _, order := range [][]Board{
		{srv, aOnly}, // A-record packet after the SRV packet
		{aOnly, srv}, // A-record packet first
	} {
		a := mdnsAccum{}
		for _, b := range order {
			a.add(b)
		}
		got := a.snapshot()
		if len(got) != 1 {
			t.Fatalf("order %v: accum = %#v, want 1 board", order, got)
		}
		b := got[0]
		if b.IP != "10.1.51.205" || b.Port != 7777 || b.Host != "xuansi-d75884.local" {
			t.Errorf("order %v: face incomplete after merge: %#v", order, b)
		}
	}
}

// fakeBrowser is the mdnsBrowser seam fake: Browse feeds the preset
// entries and closes the channel immediately. browseOnce only relies
// on channel close ending the drain (the real library closes at window
// end), so an instant close exercises the same path without waiting
// out the 1.5s window. browseErr simulates an initial-send launch
// failure.
type fakeBrowser struct {
	entries   []*zeroconf.ServiceEntry
	browseErr error
	browsed   int
}

func (f *fakeBrowser) Browse(_ context.Context, _, _ string, entries chan<- *zeroconf.ServiceEntry) error {
	if f.browseErr != nil {
		return f.browseErr
	}
	for _, e := range f.entries {
		entries <- e
	}
	close(entries)
	f.browsed++
	return nil
}

// TestBrowseIfacesSelection pins the per-interface browse wiring (the
// win multihomed root-cause fix): every usable interface gets its own
// SelectIfaces-pinned resolver and a concurrent Browse in the shared
// window, results from several interfaces merge field-wise through the
// accumulator, a TOTAL launch failure errors while a single interface
// failing only degrades, an empty usable set falls back to the default
// (option-less) resolver, and interfaces are enumerated once per sweep
// — not once per window. Seams under test: browseIfaces (interface
// list) and newResolverFunc (resolver construction).
func TestBrowseIfacesSelection(t *testing.T) {
	origIfaces, origNew := browseIfaces, newResolverFunc
	t.Cleanup(func() { browseIfaces, newResolverFunc = origIfaces, origNew })

	// The win bench shape: two NICs, the board's halves land on
	// different links — only the merge reassembles the face.
	srvHalf := &zeroconf.ServiceEntry{
		ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
		HostName:      "xuansi-d75884.local.",
		Port:          7777,
		AddrIPv4:      []net.IP{net.ParseIP("10.1.51.205")},
	}
	txtHalf := &zeroconf.ServiceEntry{
		ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
		HostName:      "xuansi-d75884.local.",
		Text:          []string{"ver=3", "api=1"},
	}
	ifaces := []net.Interface{
		{Index: 1, Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
		{Index: 2, Name: "wlan0", Flags: net.FlagUp | net.FlagMulticast},
	}

	t.Run("each iface browses pinned and halves merge", func(t *testing.T) {
		var mu sync.Mutex
		calls := 0 // total resolver constructions
		srvFake, txtFake := &fakeBrowser{entries: []*zeroconf.ServiceEntry{srvHalf}}, &fakeBrowser{entries: []*zeroconf.ServiceEntry{txtHalf}}
		newResolverFunc = func(opts ...zeroconf.ClientOption) (mdnsBrowser, error) {
			mu.Lock()
			defer mu.Unlock()
			if len(opts) != 1 {
				t.Errorf("per-iface resolver got %d options, want exactly 1 (SelectIfaces)", len(opts))
			}
			calls++
			if calls == 1 {
				return srvFake, nil
			}
			return txtFake, nil
		}
		accum := mdnsAccum{}
		if err := browseOnce(context.Background(), ifaces, accum); err != nil {
			t.Fatalf("browseOnce: %v", err)
		}
		if calls != len(ifaces) {
			t.Errorf("%d resolvers built for %d ifaces, want one each", calls, len(ifaces))
		}
		if srvFake.browsed != 1 || txtFake.browsed != 1 {
			t.Errorf("Browse calls: srv=%d txt=%d, want 1 and 1", srvFake.browsed, txtFake.browsed)
		}
		got := accum.snapshot()
		if len(got) != 1 {
			t.Fatalf("accum = %#v, want the two halves merged into 1 board", got)
		}
		b := got[0]
		if b.Host != "xuansi-d75884.local" || b.Port != 7777 || b.IP != "10.1.51.205" {
			t.Errorf("face incomplete after cross-iface merge: %#v", b)
		}
		if b.TXT["ver"] != "3" || b.TXT["api"] != "1" {
			t.Errorf("TXT not unioned across ifaces: %#v", b.TXT)
		}
	})

	t.Run("one iface failing degrades not errors", func(t *testing.T) {
		var mu sync.Mutex
		calls := 0
		okFake := &fakeBrowser{entries: []*zeroconf.ServiceEntry{srvHalf, txtHalf}}
		newResolverFunc = func(opts ...zeroconf.ClientOption) (mdnsBrowser, error) {
			mu.Lock()
			calls++
			first := calls == 1
			mu.Unlock()
			if first {
				return nil, errors.New("iface down mid-sweep")
			}
			return okFake, nil
		}
		accum := mdnsAccum{}
		if err := browseOnce(context.Background(), ifaces, accum); err != nil {
			t.Fatalf("single-iface failure must degrade, got error: %v", err)
		}
		if okFake.browsed != 1 {
			t.Errorf("surviving iface Browse calls = %d, want 1", okFake.browsed)
		}
		got := accum.snapshot()
		if len(got) != 1 || got[0].Port != 7777 || got[0].TXT["ver"] != "3" {
			t.Errorf("surviving iface's results lost: %#v", got)
		}
	})

	t.Run("all ifaces failing returns the error", func(t *testing.T) {
		newResolverFunc = func(opts ...zeroconf.ClientOption) (mdnsBrowser, error) {
			return nil, errors.New("no usable multicast interface")
		}
		for _, n := range []int{1, 2} {
			if err := browseOnce(context.Background(), ifaces[:n], mdnsAccum{}); err == nil {
				t.Errorf("%d ifaces all failing: want error, got nil", n)
			} else if !strings.Contains(err.Error(), "no usable multicast interface") {
				t.Errorf("%d ifaces all failing: error = %v", n, err)
			}
		}
	})

	t.Run("no usable iface falls back to default resolver", func(t *testing.T) {
		browseIfaces = func() []net.Interface { return nil }
		var mu sync.Mutex
		var optCounts []int
		newResolverFunc = func(opts ...zeroconf.ClientOption) (mdnsBrowser, error) {
			mu.Lock()
			optCounts = append(optCounts, len(opts))
			mu.Unlock()
			return &fakeBrowser{entries: []*zeroconf.ServiceEntry{srvHalf}}, nil
		}
		boards, err := DiscoverMDNS(context.Background())
		if err != nil {
			t.Fatalf("DiscoverMDNS fallback: %v", err)
		}
		if !reflect.DeepEqual(optCounts, []int{0, 0}) {
			t.Errorf("fallback resolver options per window = %v, want [0 0] (library default, both windows)", optCounts)
		}
		if len(boards) != 1 || boards[0].MAC6 != "d75884" {
			t.Errorf("fallback boards = %#v, want the browsed board", boards)
		}
	})

	t.Run("ifaces enumerated once per sweep", func(t *testing.T) {
		browseIfaces = func() []net.Interface {
			return append(ifaces, net.Interface{Index: 3, Name: "counted", Flags: net.FlagUp | net.FlagMulticast})
		}
		var mu sync.Mutex
		calls := 0
		newResolverFunc = func(opts ...zeroconf.ClientOption) (mdnsBrowser, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			return &fakeBrowser{}, nil
		}
		if _, err := DiscoverMDNS(context.Background()); err != nil {
			t.Fatalf("DiscoverMDNS: %v", err)
		}
		// 3 ifaces x 2 windows, but enumeration must have run ONCE.
		if calls != 6 {
			t.Errorf("resolver constructions = %d, want 6 (3 ifaces x 2 windows from one enumeration)", calls)
		}
	})
}
