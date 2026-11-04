package discover

import (
	"context"
	"reflect"
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
