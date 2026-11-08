// mdnsprobe.go - B11 win-leg diagnostic: three verdict-free sections,
// exit always 0.
//
//  1. per-interface: every FlagUp+FlagMulticast non-loopback interface
//     gets its own 3s browse via SelectIfaces([iface]) - which single
//     interface can SEE the board at the zeroconf library layer.
//  2. default: ONE NewResolver() browse, the library's own interface
//     pick (what discover.DiscoverMDNS rides) - the "before" line.
//  3. manual-send experiment (win-leg root cause): another default
//     NewResolver() browse, but WHILE it listens, a hand-rolled
//     minimal DNS PTR question ("_xuansi._tcp.local." IN PTR) is
//     WriteTo'd to 224.0.0.251:5353 from a UDP socket BOUND to each
//     interface's own IPv4 source - the "after" line. Hypothesis under
//     test: zeroconf v1.0.0's queries leave through a single socket
//     bound to no source (default-route pick), so on a multi-homed
//     host whose default-route iface differs from the board's link
//     the library's own query never reaches the board, while
//     reception (group join on all ifaces) is fine. If manual
//     per-source sends wake the board where the default browse alone
//     saw nothing, transmission was the broken half.
//
// Counts, errors and empty sets are all equally valid output.
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

// probeIfaces returns the mDNS-eligible interfaces: FlagUp +
// FlagMulticast, loopback excluded.
func probeIfaces() ([]net.Interface, error) {
	var out []net.Interface
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue // down, non-multicast or loopback: not an mDNS leg
		}
		out = append(out, iface)
	}
	return out, nil
}

// ifaceIPv4 returns the interface's first IPv4 address (nil if none).
func ifaceIPv4(iface net.Interface) net.IP {
	addrs, _ := iface.Addrs()
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok {
			if ip4 := ipn.IP.To4(); ip4 != nil && !ip4.IsLoopback() {
				return ip4
			}
		}
	}
	return nil
}

// entrySummary formats one raw ServiceEntry (host/ip/port) - per-packet
// partials ARE the diagnostic signal, no field merging.
func entrySummary(e *zeroconf.ServiceEntry) string {
	ip := ""
	if len(e.AddrIPv4) > 0 {
		ip = e.AddrIPv4[0].String()
	} else if len(e.AddrIPv6) > 0 {
		ip = e.AddrIPv6[0].String()
	}
	return fmt.Sprintf("host=%s ip=%s port=%d", e.HostName, ip, e.Port)
}

// summaries joins entry summaries, or names an empty sweep honestly.
func summaries(sums []string) string {
	if len(sums) == 0 {
		return "none"
	}
	return strings.Join(sums, "; ")
}

// browseSummaries runs one browse window on the given resolver and
// returns one summary per ServiceEntry received.
func browseSummaries(res *zeroconf.Resolver) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), probeWindow)
	defer cancel()
	entries := make(chan *zeroconf.ServiceEntry, 16)
	if err := res.Browse(ctx, "_xuansi._tcp", "local.", entries); err != nil {
		return nil, err
	}
	var out []string
	for e := range entries {
		out = append(out, entrySummary(e))
	}
	return out, nil
}

// mdnsPTRQuery hand-assembles a minimal DNS frame: header id 0, RD=1,
// QDCOUNT=1, one question "_xuansi._tcp.local." IN PTR (QTYPE=12,
// QCLASS=1). No name compression - a single name in the frame.
func mdnsPTRQuery() []byte {
	var b []byte
	b = append(b, 0, 0)             // ID 0
	b = append(b, 0x01, 0x00)       // flags: RD=1
	b = append(b, 0, 1)             // QDCOUNT 1
	b = append(b, 0, 0, 0, 0, 0, 0) // ANCOUNT/NSCOUNT/ARCOUNT 0
	for _, label := range []string{"_xuansi", "_tcp", "local"} {
		b = append(b, byte(len(label)))
		b = append(b, label...)
	}
	b = append(b, 0)     // QNAME root
	b = append(b, 0, 12) // QTYPE = PTR
	b = append(b, 0, 1)  // QCLASS = IN
	return b
}

func runMDNSProbe() int {
	ifaces, err := probeIfaces()
	if err != nil {
		fmt.Printf("mdnsprobe: net.Interfaces: %v\n", err)
		return 0
	}

	// — section 1: per-interface SelectIfaces browse —
	for i := range ifaces {
		iface := ifaces[i]
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

	// — section 2: default resolver, no manual send (the "before") —
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

	// — section 3: manual-send experiment (the "after") - default
	// browse listening while hand-rolled PTR queries go out from each
	// interface's own bound IPv4 source —
	exp, xerr := zeroconf.NewResolver()
	if xerr != nil {
		fmt.Printf("manual-send default-resolver: resolver error: %v\n", xerr)
		return 0
	}
	wctx, wcancel := context.WithTimeout(context.Background(), probeWindow)
	defer wcancel()
	entries := make(chan *zeroconf.ServiceEntry, 16)
	if err := exp.Browse(wctx, "_xuansi._tcp", "local.", entries); err != nil {
		fmt.Printf("manual-send default-resolver: browse error: %v\n", err)
		return 0
	}
	q := mdnsPTRQuery()
	var conns []*net.UDPConn
	for i := range ifaces {
		iface := ifaces[i]
		ip4 := ifaceIPv4(iface)
		if ip4 == nil {
			fmt.Printf("  manual send %s: no ipv4 addr (skipped)\n", iface.Name)
			continue
		}
		conn, lerr := net.ListenUDP("udp4", &net.UDPAddr{IP: ip4})
		if lerr != nil {
			fmt.Printf("  manual send %s (%s): listen: %v\n", iface.Name, ip4, lerr)
			continue
		}
		n, werr := conn.WriteToUDP(q, &net.UDPAddr{IP: net.IPv4(224, 0, 0, 251), Port: 5353})
		fmt.Printf("  manual send %s (%s): %d bytes to 224.0.0.251:5353 err=%v\n", iface.Name, ip4, n, werr)
		conns = append(conns, conn)
	}
	var xsums []string
	for e := range entries {
		xsums = append(xsums, entrySummary(e))
	}
	// auxiliary: legacy-unicast replies may come back to the manual
	// sockets themselves (query source port != 5353) - queued packets
	// drain with a short deadline once the window has closed.
	for _, conn := range conns {
		buf := make([]byte, 1500)
		total, from := 0, []string{}
		for {
			conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			n, raddr, rerr := conn.ReadFromUDP(buf)
			if rerr != nil {
				break
			}
			total += n
			from = append(from, raddr.String())
		}
		conn.Close()
		if total > 0 {
			fmt.Printf("  manual socket %s: %d bytes back from %s\n", conn.LocalAddr(), total, strings.Join(from, ","))
		}
	}
	fmt.Printf("manual-send default-resolver: %d entries (%s)\n", len(xsums), summaries(xsums))
	return 0
}
