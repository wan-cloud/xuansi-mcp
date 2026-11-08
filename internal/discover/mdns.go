package discover

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
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

// DiscoverMDNS sweeps _xuansi._tcp within the 3s budget (two 1.5s
// windows, see mdnsTimeout) and returns the answering boards merged
// field-wise. Each window runs TWO concurrent lanes over EVERY usable
// interface:
//
//   - the zeroconf browse lane (c464846, the linux/mac primary road):
//     a SelectIfaces-pinned resolver per interface — the multihomed
//     fix; a win real host with 10.1.50.x / 10.1.51.x NICs and the
//     board on the 51 link got zero entries from the default resolver
//     (one 0.0.0.0:5353 socket, default-route sends), while a resolver
//     pinned to the board's link saw it.
//   - the unicast lane (mdnsuni.go): a source-bound PTR query per
//     interface whose reply comes back unicast to the query socket —
//     the road that does not depend on multicast response reception,
//     which is the half the win multihomed bench proved unreliable.
//
// Interfaces are enumerated ONCE per sweep: the 3s budget is far
// shorter than any interface churn (cable pulls, VPN ramps), so
// per-window re-enumeration buys nothing. The budget expiring on the
// internal deadline is the normal end of a sweep (a clean empty set is
// real knowledge that no service answered — boards off WiFi, or an AP
// that drops multicast); a TOTAL sweep failure — every lane blind:
// every interface's browse resolver AND every interface's unicast
// socket failed to launch (or no usable interface and the default
// resolver failed) — or a canceled caller context returns the error
// instead; Registry.Refresh treats those as environment degradation
// and freezes the mdns face at last-known rather than trusting a blind
// empty sweep. ctx-driven only: every goroutine a lane spawns dies
// with the derived context; each resolver's entries channel is closed
// by the library when its window ends.
func DiscoverMDNS(ctx context.Context) ([]Board, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Interface set is fixed for the whole sweep (see doc comment).
	ifaces := browseIfaces()
	accum := mdnsAccum{}
	for w := 0; w < 2; w++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := sweepWindow(ctx, ifaces, accum); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return accum.snapshot(), nil
}

// sweepWindow runs ONE window's two lanes concurrently — the browse
// (multicast) and unicast (source-bound query) lanes each derive the
// same 1.5s window from ctx, so the window budget is shared, not
// doubled, and the total sweep stays pinned at 3s. Lanes accumulate
// into their own accumulators (mdnsAccum is not safe for concurrent
// writes) and merge serially into the sweep accumulator afterwards,
// browse first then unicast — deterministic, and the accumulator's
// field-wise merge dedups a board seen by both lanes. Error rule: a
// lane errors only when EVERY one of its interfaces failed; the sweep
// errors only when BOTH lanes are blind (one blind lane degrades — the
// other lane's data is still an honest sweep).
func sweepWindow(ctx context.Context, ifaces []net.Interface, accum mdnsAccum) error {
	browseAcc, uniAcc := mdnsAccum{}, mdnsAccum{}
	var (
		wg   sync.WaitGroup
		berr error
		uerr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		berr = browseOnce(ctx, ifaces, browseAcc)
	}()
	go func() {
		defer wg.Done()
		uerr = unicastOnce(ctx, ifaces, uniAcc)
	}()
	wg.Wait()
	for _, b := range browseAcc.snapshot() {
		accum.add(b)
	}
	for _, b := range uniAcc.snapshot() {
		accum.add(b)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if berr != nil && uerr != nil {
		return berr
	}
	return nil
}

// mdnsBrowser is the resolver seam: *zeroconf.Resolver satisfies it;
// tests substitute fakes that observe Browse calls.
type mdnsBrowser interface {
	Browse(ctx context.Context, service, domain string, entries chan<- *zeroconf.ServiceEntry) error
}

// newResolverFunc is the constructor seam over zeroconf.NewResolver.
// Per-interface browses pass exactly one option (SelectIfaces pinning
// the resolver to that interface); the no-usable-interface fallback
// passes none (library default).
var newResolverFunc = func(opts ...zeroconf.ClientOption) (mdnsBrowser, error) {
	return zeroconf.NewResolver(opts...)
}

// browseIfaces enumerates the interfaces that each get a dedicated
// browse (seam: tests inject a fake list). Usable = up,
// multicast-capable, not loopback, holding an IPv4 address (ifaceIPv4,
// shared with the unicast lane's source binding): the pinned resolver
// joins the v4 multicast group on its one interface, and a v6-only
// interface would fail resolver construction outright.
// Virtual switches (vEthernet, docker0, VPN tun...) stay in on
// purpose — harmless: each gets its own browse and an empty result is
// honest. Enumeration failure or nothing qualifying returns nil — the
// caller falls back to the default resolver and lets it surface the
// environment error itself.
var browseIfaces = func() []net.Interface {
	all, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []net.Interface
	for _, iface := range all {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if ifaceIPv4(iface) == nil {
			continue
		}
		out = append(out, iface)
	}
	return out
}

// ifaceIPv4 returns the interface's first IPv4 address, nil if none —
// the single source of "which address belongs to this interface" for
// BOTH lanes: browseIfaces' usability filter and the unicast lane's
// query source binding (seam: tests override).
var ifaceIPv4 = func(iface net.Interface) net.IP {
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok {
			if ip4 := ipn.IP.To4(); ip4 != nil {
				return ip4
			}
		}
	}
	return nil
}

// browseOnce runs one browse window into the shared accumulator: one
// concurrent Browse per usable interface (each resolver pinned to its
// interface — the multihomed fix), all sharing this window's deadline;
// without usable interfaces it falls back to the single default
// resolver (pre-fix behavior). Results merge in interface order, so
// accumulation is deterministic. The window deadline expiring is
// clean; the error returns only when EVERY interface browse failed to
// launch (first error text) or the caller canceled — one interface
// failing is that interface's degradation, the rest of the sweep is
// still real data.
func browseOnce(ctx context.Context, ifaces []net.Interface, accum mdnsAccum) error {
	wctx, cancel := context.WithTimeout(ctx, mdnsWindow)
	defer cancel()
	if len(ifaces) == 0 {
		r := browseOne(wctx, nil)
		for _, b := range r.boards {
			accum.add(b)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return r.err
	}
	results := make([]browseResult, len(ifaces))
	var wg sync.WaitGroup
	for i := range ifaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = browseOne(wctx, &ifaces[i])
		}()
	}
	wg.Wait()
	var firstErr error
	failed := 0
	for _, r := range results {
		for _, b := range r.boards {
			accum.add(b)
		}
		if r.err != nil {
			failed++
			if firstErr == nil {
				firstErr = r.err
			}
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if failed == len(ifaces) {
		return firstErr
	}
	return nil
}

// browseResult is one interface's browse outcome: the boards its
// window yielded (possibly split-packet partials) and, when it never
// got off the ground, the launch error.
type browseResult struct {
	boards []Board
	err    error
}

// browseOne browses on a single interface (iface non-nil: the resolver
// is pinned to it via SelectIfaces) or on the library default (iface
// nil). Only construction or the initial query send can fail — the
// window deadline expiring is the normal nil-error end: the library
// closes the entries channel when its context ends, which is where
// the drain loop stops.
func browseOne(wctx context.Context, iface *net.Interface) browseResult {
	var (
		resolver mdnsBrowser
		err      error
	)
	if iface != nil {
		resolver, err = newResolverFunc(zeroconf.SelectIfaces([]net.Interface{*iface}))
	} else {
		resolver, err = newResolverFunc()
	}
	if err != nil {
		return browseResult{err: fmt.Errorf("discover: mdns resolver: %w", err)}
	}
	entries := make(chan *zeroconf.ServiceEntry, 16)
	if err := resolver.Browse(wctx, mdnsService, mdnsDomain, entries); err != nil {
		return browseResult{err: fmt.Errorf("discover: mdns browse %s: %w", mdnsService, err)}
	}
	var boards []Board
	for e := range entries {
		if b, ok := boardFromEntry(e); ok {
			boards = append(boards, b)
		}
	}
	return browseResult{boards: boards}
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
	if b.IP != "" {
		prev.IP = b.IP
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
	// The browse carried the A/AAAA records for free — keep the first
	// v4 (else first v6) as the dial address: CGO=0 pure resolvers
	// cannot resolve .local, so this address is the TCP face's primary
	// route (empty = none seen; the hostname remains the fallback).
	ip := ""
	if len(e.AddrIPv4) > 0 {
		ip = e.AddrIPv4[0].String()
	} else if len(e.AddrIPv6) > 0 {
		ip = e.AddrIPv6[0].String()
	}
	return Board{
		MAC6:    mac6,
		Sources: []string{SourceMDNS},
		Host:    strings.TrimSuffix(e.HostName, "."),
		IP:      ip,
		Port:    e.Port,
		TXT:     txt,
	}, true
}
