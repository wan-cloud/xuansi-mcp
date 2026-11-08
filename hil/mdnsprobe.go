// mdnsprobe.go - B11 win-leg diagnostic step 2: per-interface mDNS
// probe at the zeroconf LIBRARY layer (no discover package on the path
// - the question is which interface the default resolver actually
// queries on each platform). Every FlagUp+FlagMulticast non-loopback
// interface gets its own 3s browse of _xuansi._tcp via
// SelectIfaces([iface]); then ONE default NewResolver() run (the
// library's own interface choice, what discover.DiscoverMDNS rides)
// for contrast. Verdict-free: counts, errors and empty sets are all
// equally valid output; the exit code is always 0.
package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/grandcat/zeroconf"
)

const probeWindow = 3 * time.Second

// browseSummaries runs one browse window on the given resolver and
// returns one "host=/ip=/port=" summary per raw ServiceEntry received
// (no field merging - per-packet partials ARE the diagnostic signal).
func browseSummaries(res *zeroconf.Resolver) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), probeWindow)
	defer cancel()
	entries := make(chan *zeroconf.ServiceEntry, 16)
	if err := res.Browse(ctx, "_xuansi._tcp", "local.", entries); err != nil {
		return nil, err
	}
	var out []string
	for e := range entries {
		ip := ""
		if len(e.AddrIPv4) > 0 {
			ip = e.AddrIPv4[0].String()
		} else if len(e.AddrIPv6) > 0 {
			ip = e.AddrIPv6[0].String()
		}
		out = append(out, fmt.Sprintf("host=%s ip=%s port=%d", e.HostName, ip, e.Port))
	}
	return out, nil
}

func runMDNSProbe() int {
	ifaces, err := net.Interfaces()
	if err != nil {
		fmt.Printf("mdnsprobe: net.Interfaces: %v\n", err)
		return 0
	}
	for i := range ifaces {
		iface := ifaces[i]
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue // down, non-multicast or loopback: not an mDNS leg
		}
		addrs, _ := iface.Addrs()
		names := make([]string, len(addrs))
		for j, a := range addrs {
			names[j] = a.String()
		}
		res, rerr := zeroconf.NewResolver(zeroconf.SelectIfaces([]net.Interface{iface}))
		if rerr != nil {
			fmt.Printf("iface %d %s %v: resolver error: %v\n", iface.Index, iface.Name, names, rerr)
			continue
		}
		sums, berr := browseSummaries(res)
		if berr != nil {
			fmt.Printf("iface %d %s %v: browse error: %v\n", iface.Index, iface.Name, names, berr)
			continue
		}
		fmt.Printf("iface %d %s %v: %d entries (%s)\n",
			iface.Index, iface.Name, names, len(sums), summaries(sums))
	}

	// contrast line: the library default - NewResolver() with no
	// SelectIfaces, zeroconf's own interface pick, in ONE window.
	def, derr := zeroconf.NewResolver()
	if derr != nil {
		fmt.Printf("default (no SelectIfaces): resolver error: %v\n", derr)
		return 0
	}
	sums, berr := browseSummaries(def)
	if berr != nil {
		fmt.Printf("default (no SelectIfaces): browse error: %v\n", berr)
		return 0
	}
	fmt.Printf("default (no SelectIfaces): %d entries (%s)\n", len(sums), summaries(sums))
	return 0
}

// summaries joins entry summaries, or names an empty sweep honestly.
func summaries(sums []string) string {
	if len(sums) == 0 {
		return "none"
	}
	return strings.Join(sums, "; ")
}
