package discover

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/grandcat/zeroconf"
)

// The frozen protocol's mDNS face (§14): hostname xuansi-<mac6>.local,
// service _xuansi._tcp on port 7777, TXT ver/api (same single source as
// hello's protocol_ver), instance name = the hostname body. The service
// is registered while net is up and withdrawn (mdns_free) on net-down.
const (
	mdnsService = "_xuansi._tcp"
	mdnsDomain  = "local."
	// mdnsTimeout is the TOTAL browse budget per sweep (pinned 3s),
	// split into two sequential windows: on this bench a cache-holding
	// responder consistently answers the FIRST window with a partial
	// record (SRV+A, ttl 120, no TXT) before the owner's complete
	// answer (ttl 4500, TXT ver/api) can land, and the library submits
	// each instance only ONCE per browse — the partial would be final.
	// A second window is where the owner's full record arrives; the
	// accumulator merges both (measured twice, deterministic).
	mdnsTimeout = 3 * time.Second
	mdnsWindow  = mdnsTimeout / 2
	namePrefix  = "xuansi-"
	hostSuffix  = ".local"
)

// DiscoverMDNS browses _xuansi._tcp within the 3s budget (two 1.5s
// windows, see mdnsTimeout) and returns the answering boards merged
// field-wise. The budget expiring on the internal deadline is the
// normal end of a sweep (a clean empty set is real knowledge that no
// service answered — boards off WiFi, or an AP that drops multicast);
// a browse failure (no usable multicast interface, send error) or a
// canceled caller context returns the error instead — Registry.Refresh
// treats those as environment degradation and freezes the mdns face at
// last-known rather than trusting a blind empty sweep. ctx-driven only:
// every goroutine a browse spawns dies with the derived context; the
// entries channel is closed by the library when the window ends.
func DiscoverMDNS(ctx context.Context) ([]Board, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	accum := mdnsAccum{}
	for w := 0; w < 2; w++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := browseOnce(ctx, accum); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return accum.snapshot(), nil
}

// browseOnce runs one browse window into the shared accumulator. The
// window deadline expiring is clean; only a launch failure or caller
// cancellation is an error.
func browseOnce(ctx context.Context, accum mdnsAccum) error {
	resolver, err := zeroconf.NewResolver()
	if err != nil {
		return fmt.Errorf("discover: mdns resolver: %w", err)
	}
	wctx, cancel := context.WithTimeout(ctx, mdnsWindow)
	defer cancel()
	entries := make(chan *zeroconf.ServiceEntry, 16)
	if err := resolver.Browse(wctx, mdnsService, mdnsDomain, entries); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("discover: mdns browse %s: %w", mdnsService, err)
	}
	for e := range entries {
		if b, ok := boardFromEntry(e); ok {
			accum.add(b)
		}
	}
	return nil
}

// mdnsAccum merges browse results as they arrive within one window.
// The same board can surface several times: its own response plus any
// cache-holding responder on the host, and each submitted entry carries
// only ONE response packet's sections — an SRV+A packet and a TXT
// packet of the same service are separate entries (the library keeps
// no cross-packet field state). Merge field-wise: non-empty wins, TXT
// unions — so a face split across packets reassembles by window end.
type mdnsAccum map[string]Board

func (a mdnsAccum) add(b Board) {
	prev, ok := a[b.MAC6]
	if !ok {
		a[b.MAC6] = b
		return
	}
	if b.Host != "" {
		prev.Host = b.Host
	}
	if b.Port != 0 {
		prev.Port = b.Port
	}
	if len(b.TXT) > 0 {
		if prev.TXT == nil {
			prev.TXT = map[string]string{}
		}
		for k, v := range b.TXT {
			prev.TXT[k] = v
		}
	}
	for _, s := range b.Sources {
		prev.addSource(s)
	}
	a[b.MAC6] = prev
}

// snapshot returns the accumulated boards sorted by mac6.
func (a mdnsAccum) snapshot() []Board {
	out := make([]Board, 0, len(a))
	for _, b := range a {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MAC6 < out[j].MAC6 })
	return out
}

// boardFromEntry parses one browse result into a Board. The identity
// comes from the hostname (§14: xuansi-<mac6>.local, the mac6 being the
// efuse STA MAC's last three octets, stable across reboots); an entry
// without an SRV hostname falls back to the instance name (instance =
// hostname body). Anything not shaped xuansi-<6 hex> is not ours and is
// skipped. Host keeps the dotless form ("xuansi-d75884.local") so it
// dials directly; TXT pairs without "=" are dropped.
func boardFromEntry(e *zeroconf.ServiceEntry) (Board, bool) {
	name := e.Instance
	if h := e.HostName; h != "" {
		name = strings.TrimSuffix(strings.TrimSuffix(h, "."), hostSuffix)
	}
	if !strings.HasPrefix(name, namePrefix) {
		return Board{}, false
	}
	mac6 := strings.ToLower(name[len(namePrefix):])
	if !isMac6(mac6) {
		return Board{}, false
	}
	txt := make(map[string]string, len(e.Text))
	for _, t := range e.Text {
		if k, v, ok := strings.Cut(t, "="); ok {
			txt[k] = v
		}
	}
	return Board{
		MAC6:    mac6,
		Sources: []string{SourceMDNS},
		Host:    strings.TrimSuffix(e.HostName, "."),
		Port:    e.Port,
		TXT:     txt,
	}, true
}
