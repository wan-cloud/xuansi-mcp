package discover

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grandcat/zeroconf"
)

// realBoardFrameHex is the VERBATIM 132-byte legacy-unicast mDNS response
// the bench board (xuansi-d75884, 10.1.51.205) sent back to a
// per-interface source-bound PTR query, captured 2026-11-09 on the linux
// bench (ens18 10.1.50.200) with a standalone unicast probe (bind iface
// IPv4 ephemeral port, hand-rolled 36B PTR query to 224.0.0.251:5353,
// board answers unicast to the query's source port). Frame anatomy,
// decoded by hand and machine (see TestBoardsFromRealFrame):
//
//	header: id 0, flags 0x8400 (QR|AA), QD 0, AN 1, NS 0, AR 3
//	AN  PTR _xuansi._tcp.local → xuansi-d75884._xuansi._tcp.local
//	    (rdata name ends in compression pointer 0xC00C → offset 12)
//	AR  SRV xuansi-d75884._xuansi._tcp.local (name = 0xC02A → offset 42,
//	    INSIDE the PTR record's rdata): port 7777, target xuansi-d75884.local
//	    (target name ends 0xC019 → offset 25 "local")
//	AR  TXT same service name (0xC02A): "api=1", "ver=5"
//	AR  A   xuansi-d75884.local (name = 0xC04C → offset 76, INSIDE the
//	    SRV record's rdata): 10.1.51.205
//
// Every name after the first is compressed, and the pointers target
// rdata regions, not record boundaries — the reason name decoding must
// resolve against the whole frame.
const realBoardFrameHex = "000084000000000100000003" +
	"075f7875616e7369045f746370056c6f63616c00" + // _xuansi._tcp.local.
	"000c0001000011940010" + // PTR, IN, ttl 4500, rdlen 16
	"0d7875616e73692d643735383834c00c" + // "xuansi-d75884" + ptr→12
	"c02a00210001000000780016" + // SRV (name ptr→42), IN, ttl 120, rdlen 22
	"000000001e61" + // prio 0, weight 0, port 7777
	"0d7875616e73692d643735383834c019" + // target "xuansi-d75884" + ptr→25
	"c02a0010000100001194000c" + // TXT (name ptr→42), IN, ttl 4500, rdlen 12
	"056170693d31057665723d35" + // "api=1", "ver=5"
	"c04c00010001000000780004" + // A (name ptr→76), IN, ttl 120, rdlen 4
	"0a0133cd" // 10.1.51.205

// provenQueryHex is the 36-byte PTR question the win real-host
// experiment (cf6c34f manual send) proved stable three-for-three —
// unicastPTRQuery must emit these exact bytes.
const provenQueryHex = "000001000001000000000000" +
	"075f7875616e7369045f746370056c6f63616c00" + // _xuansi._tcp.local.
	"000c0001" // QTYPE PTR, QCLASS IN

func hexBytes(t *testing.T, h string) []byte {
	t.Helper()
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatalf("bad hex fixture: %v", err)
	}
	return b
}

// wireName encodes a dotted name as uncompressed DNS labels (test
// builder — the compression shapes come from the real frame or hand hex).
func wireName(name string) []byte {
	var b []byte
	for _, l := range strings.Split(name, ".") {
		b = append(b, byte(len(l)))
		b = append(b, l...)
	}
	return append(b, 0)
}

func wireRR(name string, typ uint16, rd []byte) []byte {
	b := wireName(name)
	b = append(b, byte(typ>>8), byte(typ&0xFF), 0, 1) // type, class IN
	b = append(b, 0, 0, 0, 120)                       // ttl 120
	b = append(b, byte(len(rd)>>8), byte(len(rd)&0xFF))
	return append(b, rd...)
}

// wireResponse assembles a QR|AA response frame with every RR in the
// answer section (the decoder walks AN+NS+AR uniformly).
func wireResponse(rrs ...[]byte) []byte {
	h := []byte{0, 0, 0x84, 0, 0, 0, 0, byte(len(rrs)), 0, 0, 0, 0}
	for _, rr := range rrs {
		h = append(h, rr...)
	}
	return h
}

func srvRD(port int, target string) []byte {
	return append([]byte{0, 0, 0, 0, byte(port >> 8), byte(port)}, wireName(target)...)
}

func txtRD(pairs ...string) []byte {
	var b []byte
	for _, p := range pairs {
		b = append(b, byte(len(p)))
		b = append(b, p...)
	}
	return b
}

// TestUnicastPTRQueryByteExact: the query frame is wire-frozen — the
// exact 36 bytes the win manual-send experiment ran stable (three-for-
// three on the multihomed win host), byte for byte.
func TestUnicastPTRQueryByteExact(t *testing.T) {
	got := unicastPTRQuery()
	want := hexBytes(t, provenQueryHex)
	if len(got) != 36 {
		t.Fatalf("query len = %d, want 36", len(got))
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("query =\n %x\nwant\n %x", got, want)
	}
}

// TestDNSNameDecoding: label decoding plus RFC 1035 §4.1.4 compression —
// pointers may target any earlier byte (the real board frame points into
// rdata regions), forward pointers are malformed, everything truncated
// or reserved errors instead of panicking.
func TestDNSNameDecoding(t *testing.T) {
	cases := []struct {
		name    string
		frame   []byte
		off     int
		want    string
		next    int
		wantErr bool
	}{
		{name: "plain labels", frame: []byte{2, 'a', 'b', 3, 'c', 'd', 'e', 0, 0xFF}, off: 0, want: "ab.cde", next: 8},
		{name: "root only", frame: []byte{0, 1, 2}, off: 0, want: "", next: 1},
		{name: "pointer back to start", frame: []byte{1, 'x', 0, 0xC0, 0x00}, off: 3, want: "x", next: 5},
		{name: "pointer chain two hops", frame: []byte{3, 'a', 'b', 'c', 0, 0xC0, 0x00, 0xC0, 0x05}, off: 7, want: "abc", next: 9},
		{
			// pointer into a "rdata" region (real board shape: SRV name
			// c02a targets offset 42 inside the PTR rdata)
			name:  "pointer into rdata region",
			frame: append(append([]byte{2, 'h', 'i', 0}, wireName("xuansi-d75884")...), 0xC0, 0x00),
			off:   4 + len(wireName("xuansi-d75884")), want: "hi", next: 4 + len(wireName("xuansi-d75884")) + 2,
		},
		{name: "forward pointer is malformed", frame: []byte{0xC0, 0x02}, off: 0, wantErr: true},
		{name: "self pointer is malformed", frame: []byte{0xC0, 0x00}, off: 0, wantErr: true},
		{name: "label runs past end", frame: []byte{5, 'a'}, off: 0, wantErr: true},
		{name: "pointer runs past end", frame: []byte{0xC0}, off: 0, wantErr: true},
		{name: "reserved high bits", frame: []byte{0x45, 'a', 'b', 'c', 'd', 'e'}, off: 0, wantErr: true},
		{name: "offset past end", frame: []byte{1, 'a'}, off: 9, wantErr: true},
	}
	for _, c := range cases {
		got, next, err := dnsName(c.frame, c.off)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: dnsName = %q, want error", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: dnsName: %v", c.name, err)
			continue
		}
		if got != c.want || next != c.next {
			t.Errorf("%s: dnsName = %q next %d, want %q next %d", c.name, got, next, c.want, c.next)
		}
	}
}

// TestBoardsFromRealFrame: the captured 132-byte board frame parses to
// the complete board face — SRV host+port, TXT pairs, A-record v4 — with
// every name arrived at through compression pointers (two of them into
// rdata regions). This is the fixture the win-defect fix is built around.
func TestBoardsFromRealFrame(t *testing.T) {
	boards := boardsFromFrame(hexBytes(t, realBoardFrameHex))
	want := []Board{{
		MAC6:    "d75884",
		Sources: []string{"mdns"},
		Host:    "xuansi-d75884.local",
		IP:      "10.1.51.205",
		Port:    7777,
		TXT:     map[string]string{"api": "1", "ver": "5"},
	}}
	if !reflect.DeepEqual(boards, want) {
		t.Errorf("boardsFromFrame(real) =\n %#v\nwant\n %#v", boards, want)
	}
}

// TestBoardsFromFrameShapes: partial and foreign frames — identity from
// instance alone (no SRV), foreign services filtered, bad mac6 filtered,
// TXT pairs without "=" dropped, two boards in one frame, and every
// malformed shape returns no boards without panicking.
func TestBoardsFromFrameShapes(t *testing.T) {
	svc := "xuansi-d75884._xuansi._tcp.local"
	cases := []struct {
		name  string
		frame []byte
		want  []Board
	}{
		{
			name:  "ptr only gives instance identity",
			frame: wireResponse(wireRR("_xuansi._tcp.local", 12, wireName(svc))),
			want:  []Board{{MAC6: "d75884", Sources: []string{"mdns"}, TXT: map[string]string{}}},
		},
		{
			name:  "srv without ptr",
			frame: wireResponse(wireRR(svc, 33, srvRD(7777, "xuansi-d75884.local"))),
			want:  []Board{{MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local", Port: 7777, TXT: map[string]string{}}},
		},
		{
			name: "srv txt and a in separate rrs",
			frame: wireResponse(
				wireRR(svc, 33, srvRD(7777, "xuansi-d75884.local")),
				wireRR(svc, 16, txtRD("ver=5")),
				wireRR("xuansi-d75884.local", 1, []byte{10, 1, 51, 205}),
			),
			want: []Board{{
				MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local",
				IP: "10.1.51.205", Port: 7777, TXT: map[string]string{"ver": "5"},
			}},
		},
		{
			name: "txt without equals dropped",
			frame: wireResponse(
				wireRR(svc, 33, srvRD(7777, "xuansi-d75884.local")),
				wireRR(svc, 16, txtRD("novalue", "api=1")),
			),
			want: []Board{{
				MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local",
				Port: 7777, TXT: map[string]string{"api": "1"},
			}},
		},
		{
			name: "two boards in one frame",
			frame: wireResponse(
				wireRR("xuansi-d75884._xuansi._tcp.local", 33, srvRD(7777, "xuansi-d75884.local")),
				wireRR("xuansi-1a2b3c._xuansi._tcp.local", 33, srvRD(7777, "xuansi-1a2b3c.local")),
				wireRR("xuansi-1a2b3c.local", 1, []byte{10, 1, 51, 9}),
			),
			want: []Board{
				{MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local", Port: 7777, TXT: map[string]string{}},
				{MAC6: "1a2b3c", Sources: []string{"mdns"}, Host: "xuansi-1a2b3c.local", IP: "10.1.51.9", Port: 7777, TXT: map[string]string{}},
			},
		},
		{
			name:  "foreign service filtered",
			frame: wireResponse(wireRR("living-room._airplay._tcp.local", 33, srvRD(7000, "living-room.local"))),
			want:  nil,
		},
		{
			name:  "hostname mac6 not hex filtered",
			frame: wireResponse(wireRR("xuansi-zzzzzz._xuansi._tcp.local", 33, srvRD(7777, "xuansi-zzzzzz.local"))),
			want:  nil,
		},
		{
			name:  "query frame ignored",
			frame: append([]byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0}, wireName("_xuansi._tcp.local")...),
			want:  nil,
		},
		{name: "truncated real frame", frame: hexBytes(t, realBoardFrameHex)[:100], want: nil},
		{name: "header only", frame: []byte{0, 0, 0x84, 0, 0, 0, 0, 1, 0, 0, 0, 0}, want: nil},
		{name: "empty", frame: nil, want: nil},
		{name: "short garbage", frame: []byte{1, 2, 3}, want: nil},
	}
	for _, c := range cases {
		got := boardsFromFrame(c.frame)
		if len(got) != len(c.want) {
			t.Errorf("%s: boards = %#v, want %d boards", c.name, got, len(c.want))
			continue
		}
		if len(c.want) > 1 { // multi-board order: match by MAC6 set
			byMac := map[string]Board{}
			for _, b := range got {
				byMac[b.MAC6] = b
			}
			for _, w := range c.want {
				if !reflect.DeepEqual(byMac[w.MAC6], w) {
					t.Errorf("%s: board %s =\n %#v\nwant\n %#v", c.name, w.MAC6, byMac[w.MAC6], w)
				}
			}
			continue
		}
		if len(c.want) == 1 && !reflect.DeepEqual(got[0], c.want[0]) {
			t.Errorf("%s: board =\n %#v\nwant\n %#v", c.name, got[0], c.want[0])
		}
	}
}

// fakeBoard is a loopback UDP socket playing the board: every query it
// receives is captured on queries and answered with frame from the
// query's own source address (legacy-unicast reply shape).
type fakeBoard struct {
	conn    *net.UDPConn
	queries chan []byte
	frame   []byte
}

func startFakeBoard(t *testing.T, reply []byte) *fakeBoard {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("fake board listen: %v", err)
	}
	fb := &fakeBoard{conn: conn, queries: make(chan []byte, 16), frame: reply}
	go fb.serve()
	t.Cleanup(func() { conn.Close() })
	return fb
}

func (fb *fakeBoard) serve() {
	buf := make([]byte, 1500)
	for {
		n, raddr, err := fb.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		select {
		case fb.queries <- append([]byte(nil), buf[:n]...):
		default:
		}
		if fb.frame != nil {
			fb.conn.WriteToUDP(fb.frame, raddr)
		}
	}
}

func (fb *fakeBoard) addr() *net.UDPAddr { return fb.conn.LocalAddr().(*net.UDPAddr) }

// overrideUnicastSeams repoints the unicast lane at loopback for the
// test: queries go to the fake board's address, sockets bind loopback
// regardless of the source IP the lane resolved.
func overrideUnicastSeams(t *testing.T, group *net.UDPAddr) {
	t.Helper()
	origGroup, origListen := unicastGroup, unicastListen
	t.Cleanup(func() { unicastGroup, unicastListen = origGroup, origListen })
	unicastGroup = group
	unicastListen = func(net.IP) (*net.UDPConn, error) {
		return net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	}
}

// TestUnicastOneReply: the single-interface lane, end to end over real
// UDP sockets on loopback — the bound source sends the frozen 36-byte
// query (asserted on the wire at the fake board), the board's real
// captured frame comes back to that same socket, and a complete Board
// falls out. A silent board is a clean empty window, not an error.
func TestUnicastOneReply(t *testing.T) {
	t.Run("reply parsed into board", func(t *testing.T) {
		fb := startFakeBoard(t, hexBytes(t, realBoardFrameHex))
		overrideUnicastSeams(t, fb.addr())
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		r := unicastOne(ctx, net.IPv4(127, 0, 0, 1))
		if r.err != nil {
			t.Fatalf("unicastOne: %v", r.err)
		}
		want := Board{
			MAC6: "d75884", Sources: []string{"mdns"}, Host: "xuansi-d75884.local",
			IP: "10.1.51.205", Port: 7777, TXT: map[string]string{"api": "1", "ver": "5"},
		}
		if len(r.boards) != 1 || !reflect.DeepEqual(r.boards[0], want) {
			t.Errorf("boards = %#v, want the one real-frame board %#v", r.boards, want)
		}
		select {
		case q := <-fb.queries:
			if got := hex.EncodeToString(q); got != provenQueryHex {
				t.Errorf("query on the wire = %s, want frozen %s", got, provenQueryHex)
			}
		case <-time.After(time.Second):
			t.Error("fake board never received a query")
		}
	})

	t.Run("silent board is a clean empty window", func(t *testing.T) {
		fb := startFakeBoard(t, nil)
		overrideUnicastSeams(t, fb.addr())
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		r := unicastOne(ctx, net.IPv4(127, 0, 0, 1))
		if r.err != nil {
			t.Fatalf("unicastOne: %v", r.err)
		}
		if len(r.boards) != 0 {
			t.Errorf("boards = %#v, want none", r.boards)
		}
	})

	t.Run("listen failure is the launch error", func(t *testing.T) {
		origListen := unicastListen
		t.Cleanup(func() { unicastListen = origListen })
		unicastListen = func(net.IP) (*net.UDPConn, error) { return nil, errors.New("bind: no buffer space") }
		r := unicastOne(context.Background(), net.IPv4(127, 0, 0, 1))
		if r.err == nil || !strings.Contains(r.err.Error(), "bind: no buffer space") {
			t.Errorf("err = %v, want launch error", r.err)
		}
	})
}

// TestUnicastOnceAggregation: window aggregation mirrors the browse
// lane — one interface failing degrades, every interface failing
// errors, results from several interfaces merge through the accumulator
// (dedup by mac6), an interface that lost its IPv4 mid-sweep soft-skips,
// and an empty interface set is the blind-lane marker error.
func TestUnicastOnceAggregation(t *testing.T) {
	ifaces := []net.Interface{
		{Index: 91, Name: "fake0", Flags: net.FlagUp | net.FlagMulticast},
		{Index: 92, Name: "fake1", Flags: net.FlagUp | net.FlagMulticast},
	}
	origIfIP := ifaceIPv4
	t.Cleanup(func() { ifaceIPv4 = origIfIP })
	ifaceIPv4 = func(net.Interface) net.IP { return net.IPv4(127, 0, 0, 1) }

	t.Run("two ifaces one board, dedup via accum", func(t *testing.T) {
		fb := startFakeBoard(t, hexBytes(t, realBoardFrameHex))
		overrideUnicastSeams(t, fb.addr())
		accum := mdnsAccum{}
		if err := unicastOnce(context.Background(), ifaces, accum); err != nil {
			t.Fatalf("unicastOnce: %v", err)
		}
		got := accum.snapshot()
		if len(got) != 1 || got[0].MAC6 != "d75884" || got[0].IP != "10.1.51.205" {
			t.Errorf("accum = %#v, want the one deduped real-frame board", got)
		}
		// both interfaces probed (two queries reached the board)
		if len(fb.queries) != 2 {
			t.Errorf("fake board saw %d queries, want 2 (one per interface)", len(fb.queries))
		}
	})

	t.Run("one iface failing degrades not errors", func(t *testing.T) {
		fb := startFakeBoard(t, hexBytes(t, realBoardFrameHex))
		origGroup, origListen := unicastGroup, unicastListen
		t.Cleanup(func() { unicastGroup, unicastListen = origGroup, origListen })
		unicastGroup = fb.addr()
		var mu sync.Mutex
		calls := 0
		unicastListen = func(ip net.IP) (*net.UDPConn, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				return nil, errors.New("iface down mid-sweep")
			}
			return net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		}
		ctx := context.Background() // window-bounded internally; the parent must outlive it
		accum := mdnsAccum{}
		if err := unicastOnce(ctx, ifaces, accum); err != nil {
			t.Fatalf("single-iface failure must degrade, got error: %v", err)
		}
		if got := accum.snapshot(); len(got) != 1 {
			t.Errorf("surviving iface's results lost: %#v", got)
		}
	})

	t.Run("iface without ipv4 soft skips", func(t *testing.T) {
		ifaceIPv4 = func(net.Interface) net.IP { return nil }
		t.Cleanup(func() { ifaceIPv4 = func(net.Interface) net.IP { return net.IPv4(127, 0, 0, 1) } })
		fb := startFakeBoard(t, hexBytes(t, realBoardFrameHex))
		overrideUnicastSeams(t, fb.addr())
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		accum := mdnsAccum{}
		if err := unicastOnce(ctx, ifaces, accum); err != nil {
			t.Fatalf("v4-less ifaces must soft-skip, got error: %v", err)
		}
		if len(accum) != 0 || len(fb.queries) != 0 {
			t.Errorf("soft skip not honored: accum=%d queries=%d", len(accum), len(fb.queries))
		}
	})

	t.Run("all ifaces failing returns the error", func(t *testing.T) {
		origListen := unicastListen
		t.Cleanup(func() { unicastListen = origListen })
		unicastListen = func(net.IP) (*net.UDPConn, error) { return nil, errors.New("no buffer space") }
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		for _, n := range []int{1, 2} {
			if err := unicastOnce(ctx, ifaces[:n], mdnsAccum{}); err == nil {
				t.Errorf("%d ifaces all failing: want error, got nil", n)
			} else if !strings.Contains(err.Error(), "no buffer space") {
				t.Errorf("%d ifaces all failing: error = %v", n, err)
			}
		}
	})

	t.Run("empty iface set is the blind-lane marker", func(t *testing.T) {
		if err := unicastOnce(context.Background(), nil, mdnsAccum{}); err == nil ||
			!strings.Contains(err.Error(), "no usable interface") {
			t.Errorf("err = %v, want no-usable-interface marker", err)
		}
	})
}

// TestDiscoverMDNSBothLanes: the full sweep runs the multicast browse
// and the unicast lane concurrently — browse yields the TXT half, the
// unicast reply carries SRV+A+TXT — and the accumulator reassembles one
// complete face. Both lanes totally blind surfaces the sweep error
// (freeze-at-last-known semantics), one lane blind degrades.
func TestDiscoverMDNSBothLanes(t *testing.T) {
	origIfaces, origNew, origIfIP := browseIfaces, newResolverFunc, ifaceIPv4
	t.Cleanup(func() { browseIfaces, newResolverFunc, ifaceIPv4 = origIfaces, origNew, origIfIP })
	browseIfaces = func() []net.Interface {
		return []net.Interface{{Index: 93, Name: "fake0", Flags: net.FlagUp | net.FlagMulticast}}
	}
	ifaceIPv4 = func(net.Interface) net.IP { return net.IPv4(127, 0, 0, 1) }

	t.Run("lanes merge into one complete face", func(t *testing.T) {
		txtHalf := &zeroconf.ServiceEntry{
			ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
			HostName:      "xuansi-d75884.local.",
			Text:          []string{"ver=3", "api=1"},
		}
		newResolverFunc = func(...zeroconf.ClientOption) (mdnsBrowser, error) {
			return &fakeBrowser{entries: []*zeroconf.ServiceEntry{txtHalf}}, nil
		}
		fb := startFakeBoard(t, hexBytes(t, realBoardFrameHex))
		overrideUnicastSeams(t, fb.addr())
		boards, err := DiscoverMDNS(context.Background())
		if err != nil {
			t.Fatalf("DiscoverMDNS: %v", err)
		}
		if len(boards) != 1 {
			t.Fatalf("boards = %#v, want 1 merged board", boards)
		}
		b := boards[0]
		if b.Host != "xuansi-d75884.local" || b.Port != 7777 || b.IP != "10.1.51.205" {
			t.Errorf("face incomplete after lane merge: %#v", b)
		}
		if b.TXT["api"] != "1" || b.TXT["ver"] != "5" {
			t.Errorf("TXT not unioned across lanes: %#v", b.TXT)
		}
	})

	t.Run("both lanes blind surfaces the error", func(t *testing.T) {
		newResolverFunc = func(...zeroconf.ClientOption) (mdnsBrowser, error) {
			return nil, errors.New("resolver broke")
		}
		origListen := unicastListen
		t.Cleanup(func() { unicastListen = origListen })
		unicastListen = func(net.IP) (*net.UDPConn, error) { return nil, errors.New("bind broke") }
		if _, err := DiscoverMDNS(context.Background()); err == nil {
			t.Error("both lanes totally failing: want sweep error, got nil")
		}
	})
}

// ---------------------------------------------------------------------------
// Bug 2① (B15 §4.11): the A/AAAA followup round for SRV-without-address
// sweeps — the Windows responder shape.
// ---------------------------------------------------------------------------

// TestUnicastHostQueryByteExact: the followup query's wire shape — the
// same frozen header as the proven PTR query (id 0, RD=1, one IN
// question), QNAME the SRV target's labels, QTYPE A (1) or AAAA (28).
func TestUnicastHostQueryByteExact(t *testing.T) {
	got := unicastHostQuery("xuansi-d75884.local", 1)
	want := hexBytes(t, "0000010000010000000000000d7875616e73692d643735383834056c6f63616c0000010001")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("A query =\n %x\nwant\n %x", got, want)
	}
	got6 := unicastHostQuery("xuansi-d75884.local", 28)
	want6 := hexBytes(t, "0000010000010000000000000d7875616e73692d643735383834056c6f63616c00001c0001")
	if !reflect.DeepEqual(got6, want6) {
		t.Errorf("AAAA query =\n %x\nwant\n %x", got6, want6)
	}
}

// fakeAResponder is a loopback mDNS responder for the followup leg: it
// answers a unicast A/AAAA question with one matching record for host
// (staying silent on everything else), so the followup runs over real
// UDP exactly as the daemon runs it.
type fakeAResponder struct {
	conn   *net.UDPConn
	queryQ chan []byte
	v6Only bool // answers only AAAA questions (the v6 test's A-silent shape)
}

func startFakeAResponder(t *testing.T, host string, v6Only bool) *fakeAResponder {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("fake A responder listen: %v", err)
	}
	fr := &fakeAResponder{conn: conn, queryQ: make(chan []byte, 16), v6Only: v6Only}
	go func() {
		buf := make([]byte, 1500)
		for {
			n, raddr, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			q := append([]byte(nil), buf[:n]...)
			select {
			case fr.queryQ <- q:
			default:
			}
			// one question frame: [header 12][QNAME labels… 00][QTYPE 2][QCLASS 2]
			if len(q) < 12+6 {
				continue
			}
			off := 12
			var name []byte
			for off < len(q) && q[off] != 0 {
				l := int(q[off])
				if off+1+l > len(q) {
					name = nil
					break
				}
				if len(name) > 0 {
					name = append(name, '.')
				}
				name = append(name, q[off+1:off+1+l]...)
				off += 1 + l
			}
			if name == nil || off+5 > len(q) {
				continue
			}
			qtype := uint16(q[off+1])<<8 | uint16(q[off+2])
			if fr.v6Only && qtype != 28 {
				continue
			}
			var frame []byte
			switch qtype {
			case 1:
				frame = wireResponse(wireRR(host, 1, []byte{10, 1, 51, 205}))
			case 28:
				ip := net.ParseIP("fe80::1").To16()
				frame = wireResponse(wireRR(host, 28, ip))
			default:
				continue
			}
			conn.WriteToUDP(frame, raddr)
		}
	}()
	t.Cleanup(func() { conn.Close() })
	return fr
}

// TestDiscoverMDNSAddrFollowup: Bug 2① — a browse whose SRV records
// arrive WITHOUT A/AAAA (the Windows responder shape: the sweep knows
// the service exists and its hostname, but the hostname dials as .local
// and SERVFAILs under the CGO=0 pure resolver) triggers ONE bounded
// unicast A followup round whose answer fills Board.IP. A board that
// already carries an address is left alone; a silent responder degrades
// to IP="" — the hostname fallback, never an error.
func TestDiscoverMDNSAddrFollowup(t *testing.T) {
	origIfaces, origNew, origIfIP := browseIfaces, newResolverFunc, ifaceIPv4
	t.Cleanup(func() { browseIfaces, newResolverFunc, ifaceIPv4 = origIfaces, origNew, origIfIP })
	browseIfaces = func() []net.Interface {
		return []net.Interface{{Index: 94, Name: "fake0", Flags: net.FlagUp | net.FlagMulticast}}
	}
	ifaceIPv4 = func(net.Interface) net.IP { return net.IPv4(127, 0, 0, 1) }

	t.Run("SRV without A: followup fills the IP", func(t *testing.T) {
		srvOnly := &zeroconf.ServiceEntry{ // the windows shape: SRV+TXT, no addrs
			ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
			HostName:      "xuansi-d75884.local.",
			Port:          7777,
			Text:          []string{"ver=5", "api=1"},
		}
		newResolverFunc = func(...zeroconf.ClientOption) (mdnsBrowser, error) {
			return &fakeBrowser{entries: []*zeroconf.ServiceEntry{srvOnly}}, nil
		}
		fr := startFakeAResponder(t, "xuansi-d75884.local", false)
		overrideUnicastSeams(t, fr.conn.LocalAddr().(*net.UDPAddr))
		boards, err := DiscoverMDNS(context.Background())
		if err != nil {
			t.Fatalf("DiscoverMDNS: %v", err)
		}
		if len(boards) != 1 {
			t.Fatalf("boards = %#v, want 1", boards)
		}
		if boards[0].IP != "10.1.51.205" {
			t.Errorf("IP = %q, want 10.1.51.205 from the followup round", boards[0].IP)
		}
		if boards[0].Host != "xuansi-d75884.local" || boards[0].Port != 7777 {
			t.Errorf("face mangled by the followup: %#v", boards[0])
		}
		// the unicast PTR lane's questions ride the same socket — drain
		// until the followup's own A question for the host arrives
		deadline := time.After(time.Second)
		for {
			select {
			case q := <-fr.queryQ:
				if len(q) >= 6 && q[len(q)-4] == 0 && q[len(q)-3] == 1 && strings.Contains(string(q), "xuansi-d75884") {
					goto sawA
				}
			case <-deadline:
				t.Error("the followup never sent its A question")
				goto sawA
			}
		}
	sawA:
		if t.Failed() {
			return
		}
	})

	t.Run("browse already carried A: no followup round", func(t *testing.T) {
		complete := &zeroconf.ServiceEntry{
			ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
			HostName:      "xuansi-d75884.local.",
			Port:          7777,
			AddrIPv4:      []net.IP{net.IPv4(10, 1, 51, 205)},
		}
		newResolverFunc = func(...zeroconf.ClientOption) (mdnsBrowser, error) {
			return &fakeBrowser{entries: []*zeroconf.ServiceEntry{complete}}, nil
		}
		fr := startFakeAResponder(t, "xuansi-d75884.local", false)
		overrideUnicastSeams(t, fr.conn.LocalAddr().(*net.UDPAddr))
		boards, err := DiscoverMDNS(context.Background())
		if err != nil {
			t.Fatalf("DiscoverMDNS: %v", err)
		}
		if len(boards) != 1 || boards[0].IP != "10.1.51.205" {
			t.Fatalf("boards = %#v, want the browse-carried address untouched", boards)
		}
		// only PTR questions (the sweep lanes') may have arrived — no
		// A/AAAA followup question for the host
		for {
			select {
			case q := <-fr.queryQ:
				if len(q) >= 6 && (q[len(q)-3] == 1 || q[len(q)-3] == 28) {
					t.Errorf("followup fired anyway: %x", q)
				}
				continue
			default:
			}
			break
		}
	})

	t.Run("silent responder degrades to empty IP", func(t *testing.T) {
		srvOnly := &zeroconf.ServiceEntry{
			ServiceRecord: zeroconf.ServiceRecord{Instance: "xuansi-d75884"},
			HostName:      "xuansi-d75884.local.",
			Port:          7777,
		}
		newResolverFunc = func(...zeroconf.ClientOption) (mdnsBrowser, error) {
			return &fakeBrowser{entries: []*zeroconf.ServiceEntry{srvOnly}}, nil
		}
		fr := startFakeAResponder(t, "other-host.local", false) // answers only the wrong name
		overrideUnicastSeams(t, fr.conn.LocalAddr().(*net.UDPAddr))
		start := time.Now()
		boards, err := DiscoverMDNS(context.Background())
		if err != nil {
			t.Fatalf("DiscoverMDNS: %v", err)
		}
		if len(boards) != 1 || boards[0].IP != "" {
			t.Fatalf("boards = %#v, want honest IP=\"\" (hostname remains the fallback)", boards)
		}
		if elapsed := time.Since(start); elapsed > 6*time.Second {
			t.Errorf("followup took %v — the bounded budget leaked", elapsed)
		}
	})
}

// TestFollowupAddrIPv6: the AAAA round covers a v6-only answer when the
// A round came back empty (the followup walks A then AAAA; the v6 form
// renders through net.IP.String, dialable later via JoinHostPort).
func TestFollowupAddrIPv6(t *testing.T) {
	origIfIP := ifaceIPv4
	t.Cleanup(func() { ifaceIPv4 = origIfIP })
	ifaceIPv4 = func(net.Interface) net.IP { return net.IPv4(127, 0, 0, 1) }
	// the A round stays silent; only the AAAA round answers
	fr := startFakeAResponder(t, "xuansi-d75884.local", true)
	overrideUnicastSeams(t, fr.conn.LocalAddr().(*net.UDPAddr))
	ifaces := []net.Interface{{Index: 95, Name: "fake0", Flags: net.FlagUp | net.FlagMulticast}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got := followupAddr(ctx, ifaces, "xuansi-d75884.local")
	if got != "fe80::1" {
		t.Errorf("followupAddr = %q, want fe80::1 (AAAA round after a silent A round)", got)
	}
}
