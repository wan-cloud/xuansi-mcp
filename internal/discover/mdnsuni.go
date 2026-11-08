// mDNS unicast query lane: the supplementary discovery road that does
// not depend on receiving multicast responses.
//
// Why it exists (win real-host evidence chain): on the win multihomed
// bench, multicast RESPONSE RECEPTION is the unreliable half — the same
// zeroconf probe over iface21 returned 1 entry then 0 entries on
// consecutive runs — while the unicast query road proved stable
// three-for-three (hil/mdnsprobe.go manual send, cf6c34f): a PTR query
// sent from a socket BOUND to each interface's own IPv4 (ephemeral
// source port) to 224.0.0.251:5353 makes the board reply UNICAST to
// that source port (RFC 6762 §6.7 legacy unicast: source port != 5353),
// which multicast-reception flakiness cannot drop.
//
// Windowing: the lane shares the existing sweep budget — two 1.5s
// windows aligned with the browse windows (one query per interface per
// window, so a sweep transmits twice, matching the proven experiment's
// send pattern). The zeroconf browse lane stays the primary road on
// linux/mac; this lane rides alongside, results merging through the
// same accumulator. The zeroconf dependency is NOT retired here.
package discover

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
)

const (
	// mdnsPort is the mDNS UDP port (RFC 6762).
	mdnsPort = 5353
	// unicastReadBuf: RFC 6762 §17 caps mDNS-over-IPv4 messages at 9000
	// bytes; anything larger is dropped by the parse (bounds-checked).
	unicastReadBuf = 9000
)

var (
	// unicastGroup is where unicast-lane queries go (seam: tests repoint
	// it at a loopback fake board).
	unicastGroup = &net.UDPAddr{IP: net.IPv4(224, 0, 0, 251), Port: mdnsPort}
	// unicastListen binds the per-interface source socket (seam: tests
	// bind loopback instead of the interface's real address).
	unicastListen = func(ip net.IP) (*net.UDPConn, error) {
		return net.ListenUDP("udp4", &net.UDPAddr{IP: ip})
	}
)

// errMalformedFrame marks any DNS wire violation (truncation, forward
// compression pointer, reserved label bits); a frame carrying it is
// dropped, never fatal to the lane.
var errMalformedFrame = errors.New("discover: malformed mDNS frame")

// unicastPTRQuery hand-assembles the 36-byte legacy-unicast PTR query,
// byte-identical to the frame the win manual-send experiment ran stable
// (cf6c34f, three-for-three on the multihomed win host): header id 0,
// RD=1, QDCOUNT 1, one question "_xuansi._tcp.local." IN PTR (QTYPE=12,
// QCLASS=1). No name compression — a single name in the frame. The
// bytes are wire-frozen; TestUnicastPTRQueryByteExact pins them.
func unicastPTRQuery() []byte {
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

// unicastOnce runs one unicast window into the shared accumulator, the
// browse lane's mirror (browseOnce): one concurrent source-bound query
// per usable interface, all sharing the window deadline. Error rules
// are the same all-or-degrade shape — one interface failing to launch
// (bind or send) is that interface's degradation; EVERY interface
// failing returns the first error; an empty interface set returns the
// blind-lane marker (no usable interface) so the sweep-level rule
// (error only when every lane is blind) treats a browse default-
// resolver failure as total. The window deadline expiring is clean.
func unicastOnce(ctx context.Context, ifaces []net.Interface, accum mdnsAccum) error {
	if len(ifaces) == 0 {
		return errors.New("discover: mdns unicast: no usable interface")
	}
	wctx, cancel := context.WithTimeout(ctx, mdnsWindow)
	defer cancel()
	results := make([]browseResult, len(ifaces))
	var wg sync.WaitGroup
	for i := range ifaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = unicastIface(wctx, ifaces[i])
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

// unicastIface probes on one interface: resolves the interface's IPv4
// (the same source browseIfaces pins resolvers to — ifaceIPv4 shared)
// and delegates. An interface that lost its IPv4 mid-sweep soft-skips:
// browseIfaces filtered the set at sweep start, so this is drift, and
// the browse lane's own socket for that interface tells the real story.
func unicastIface(wctx context.Context, iface net.Interface) browseResult {
	ip := ifaceIPv4(iface)
	if ip == nil {
		return browseResult{}
	}
	return unicastOne(wctx, ip)
}

// unicastOne is the per-interface leg: bind a UDP socket to the source
// address (ephemeral port — that is what flips the board into legacy
// unicast reply mode), fire the frozen PTR query at the group, then
// read replies on that same socket until the window ends. Replies are
// parsed per frame; multiple responders and retransmissions all just
// accumulate. The window deadline expiring, or the caller canceling,
// both end the read loop cleanly (a watcher closes the socket the
// moment wctx is done, so the read wakes even with no traffic).
func unicastOne(wctx context.Context, srcIP net.IP) browseResult {
	conn, err := unicastListen(srcIP)
	if err != nil {
		return browseResult{err: fmt.Errorf("discover: mdns unicast listen %s: %w", srcIP, err)}
	}
	defer conn.Close()
	if _, err := conn.WriteToUDP(unicastPTRQuery(), unicastGroup); err != nil {
		return browseResult{err: fmt.Errorf("discover: mdns unicast send %s: %w", srcIP, err)}
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-wctx.Done():
			conn.Close() // wake the read loop at window end; double Close is safe
		case <-done:
		}
	}()
	var boards []Board
	buf := make([]byte, unicastReadBuf)
	for {
		// Guard non-windowed callers; the watcher handles windowed ends.
		conn.SetReadDeadline(time.Now().Add(mdnsWindow))
		n, _, rerr := conn.ReadFromUDP(buf)
		if rerr != nil {
			return browseResult{boards: boards}
		}
		boards = append(boards, boardsFromFrame(buf[:n])...)
	}
}

// dnsRR is one parsed resource record. rdOff keeps the rdata's offset
// within the frame because rdata-embedded names (PTR target, SRV
// target) resolve compression pointers against the WHOLE frame.
type dnsRR struct {
	name  string
	typ   uint16
	rdOff int
	rd    []byte
}

// frameRecords parses a response frame's header, question section and
// every answer/authority/additional record. ok=false for anything
// malformed (truncation, bad names, counts past the frame end) or a
// non-response frame (QR=0: queries carry no discovery data for us).
func frameRecords(frame []byte) ([]dnsRR, bool) {
	if len(frame) < 12 {
		return nil, false
	}
	if frame[2]&0x80 == 0 { // QR unset: a question, not a response
		return nil, false
	}
	qd := int(frame[4])<<8 | int(frame[5])
	an := int(frame[6])<<8 | int(frame[7])
	ns := int(frame[8])<<8 | int(frame[9])
	ar := int(frame[10])<<8 | int(frame[11])
	count := an + ns + ar
	off := 12
	for i := 0; i < qd; i++ {
		_, next, err := dnsName(frame, off)
		if err != nil {
			return nil, false
		}
		off = next + 4 // QTYPE + QCLASS
		if off > len(frame) {
			return nil, false
		}
	}
	rrs := make([]dnsRR, 0, count)
	for i := 0; i < count; i++ {
		name, next, err := dnsName(frame, off)
		if err != nil {
			return nil, false
		}
		off = next
		if off+10 > len(frame) {
			return nil, false
		}
		typ := uint16(frame[off])<<8 | uint16(frame[off+1])
		rdlen := int(frame[off+8])<<8 | int(frame[off+9])
		off += 10
		if off+rdlen > len(frame) {
			return nil, false
		}
		rrs = append(rrs, dnsRR{name: name, typ: typ, rdOff: off, rd: frame[off : off+rdlen]})
		off += rdlen
	}
	return rrs, true
}

// dnsName decodes one possibly-compressed DNS name starting at off
// (RFC 1035 §4.1.4), returning the dotless name and the offset just
// past the name in the ORIGINAL stream (past the first pointer, when
// the name ends compressed). Compression pointers may target any
// earlier byte — the bench board's real frames point into other
// records' rdata (0xC02A → PTR rdata, 0xC04C → SRV rdata; measured
// 2026-11-09) — so the decoder resolves against the whole frame, not
// record boundaries. Backward-only pointers (target strictly before
// the pointer's own bytes) guarantee termination without a hop budget.
func dnsName(frame []byte, off int) (string, int, error) {
	var sb strings.Builder
	next := -1
	pos := off
	for {
		if pos < 0 || pos >= len(frame) {
			return "", 0, errMalformedFrame
		}
		b := frame[pos]
		switch {
		case b == 0x00: // root: name end
			if next < 0 {
				next = pos + 1
			}
			return sb.String(), next, nil
		case b&0xC0 == 0xC0: // compression pointer
			if pos+1 >= len(frame) {
				return "", 0, errMalformedFrame
			}
			if next < 0 {
				next = pos + 2
			}
			target := int(b&0x3F)<<8 | int(frame[pos+1])
			if target >= pos { // forward or self pointer: never valid, would loop
				return "", 0, errMalformedFrame
			}
			pos = target
		case b >= 0x40: // 0x40/0x80 label bits reserved
			return "", 0, errMalformedFrame
		default: // label, 1..63 bytes
			if pos+1+int(b) > len(frame) {
				return "", 0, errMalformedFrame
			}
			if sb.Len() > 0 {
				sb.WriteByte('.')
			}
			sb.Write(frame[pos+1 : pos+1+int(b)])
			pos += 1 + int(b)
		}
	}
}

// boardsFromFrame parses one mDNS response frame into Boards. Records
// are grouped by service-instance name (the PTR rdata target, or the
// SRV/TXT record name); SRV fills host+port, TXT the text pairs, and
// A/AAAA records land on whichever instance's SRV target names them.
// Each group is funneled through boardFromEntry on a synthesized
// ServiceEntry — identity filtering (xuansi-<mac6>), Host dot-stripping,
// first-v4 IP and TXT "=" pairing are the browse lane's exact
// semantics, one code path. Foreign services shape an entry that
// boardFromEntry then rejects; malformed shapes return no boards and
// never panic.
func boardsFromFrame(frame []byte) []Board {
	rrs, ok := frameRecords(frame)
	if !ok {
		return nil
	}
	entries := map[string]*zeroconf.ServiceEntry{}
	hostV4 := map[string][]net.IP{}
	hostV6 := map[string][]net.IP{}
	for _, rr := range rrs {
		key := strings.ToLower(rr.name)
		switch rr.typ {
		case 1: // A
			if len(rr.rd) == 4 {
				hostV4[key] = append(hostV4[key], net.IP(append([]byte(nil), rr.rd...)))
			}
		case 28: // AAAA
			if len(rr.rd) == 16 {
				hostV6[key] = append(hostV6[key], net.IP(append([]byte(nil), rr.rd...)))
			}
		case 12: // PTR: rdata is the service-instance name
			target, _, err := dnsName(frame, rr.rdOff)
			if err != nil {
				continue
			}
			entryFor(entries, target)
		case 33: // SRV: prio, weight, port, target
			if len(rr.rd) < 6 {
				continue
			}
			target, _, err := dnsName(frame, rr.rdOff+6)
			if err != nil {
				continue
			}
			e := entryFor(entries, rr.name)
			e.HostName = target
			e.Port = int(rr.rd[4])<<8 | int(rr.rd[5])
		case 16: // TXT: length-prefixed strings
			e := entryFor(entries, rr.name)
			for p := 0; p < len(rr.rd); {
				l := int(rr.rd[p])
				p++
				if p+l > len(rr.rd) {
					break
				}
				if l > 0 {
					e.Text = append(e.Text, string(rr.rd[p:p+l]))
				}
				p += l
			}
		}
	}
	// Second pass: attach the host's A/AAAA records to every instance
	// whose SRV named that host (records may arrive in any order).
	for _, e := range entries {
		host := strings.ToLower(strings.TrimSuffix(e.HostName, "."))
		if len(hostV4[host]) > 0 {
			e.AddrIPv4 = hostV4[host]
		}
		if len(hostV6[host]) > 0 {
			e.AddrIPv6 = hostV6[host]
		}
	}
	var boards []Board
	for _, e := range entries {
		if b, ok := boardFromEntry(e); ok {
			boards = append(boards, b)
		}
	}
	return boards
}

// entryFor returns the group's entry, creating it on first sight.
// Instance = the instance label of the service name ("xuansi-d75884"
// from "xuansi-d75884._xuansi._tcp.local") — §14: instance = hostname
// body, no dots.
func entryFor(entries map[string]*zeroconf.ServiceEntry, serviceName string) *zeroconf.ServiceEntry {
	key := strings.ToLower(serviceName)
	if e, ok := entries[key]; ok {
		return e
	}
	e := &zeroconf.ServiceEntry{ServiceRecord: zeroconf.ServiceRecord{Instance: instanceLabel(serviceName)}}
	entries[key] = e
	return e
}

// instanceLabel strips the known service+domain suffix, falling back to
// the first label for anything else shaped differently.
func instanceLabel(serviceName string) string {
	s := strings.ToLower(serviceName)
	s = strings.TrimSuffix(s, "."+mdnsService+"."+strings.TrimSuffix(mdnsDomain, "."))
	if i := strings.IndexByte(s, '.'); i >= 0 {
		s = s[:i]
	}
	return s
}
