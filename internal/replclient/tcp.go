package replclient

import (
	"fmt"
	"net"
	"time"
)

// tcpConnectTimeout bounds establishing the TCP endpoint (the frozen
// host-hub connect timeout).
const tcpConnectTimeout = 6 * time.Second

// DialTCP connects to the board's TCP endpoint (host:port). TCP carries
// the same byte stream as CDC with a different connection lifecycle
// (§13): no DTR/RTS lines, no reboot, no boot prefix, no drain window —
// the first frame written may be hello directly, and a reconnect is a
// new session without rebooting anything. The socket carries host-side
// keepalive pinned to the frozen hub values — 3s idle / 1s interval /
// 3 probes, a dead-link verdict ≈6s after silence starts — because a
// silently vanished board sends nothing at all: a hard reset emits no
// FIN, and the net-down path is known to miss its FIN intermittently
// (§13's mandate that the host bring its own keepalive). Keepalive
// failure surfaces as a read error, which the router turns into the
// client's dead mark (EnableAcks is not a go1.27 field — plain
// non-ack probes are the kernel default). DrainOpts.Drain is
// meaningless here and is ignored (NoDrain semantics, §13).
func DialTCP(hostport string, o DialOpts) (*Client, error) {
	link, err := dialTCPLink(hostport, o)
	if err != nil {
		return nil, err
	}
	c := NewClient(link)
	c.redial = redialRoute{opts: o, dial: func(o DialOpts) (Link, error) {
		return dialTCPLink(hostport, o)
	}}
	return c, nil
}

func dialTCPLink(hostport string, o DialOpts) (Link, error) {
	d := &net.Dialer{
		Timeout: tcpConnectTimeout,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     3 * time.Second,
			Interval: time.Second,
			Count:    3,
		},
	}
	conn, err := d.Dial("tcp", hostport)
	if err != nil {
		return nil, fmt.Errorf("replclient: dial tcp %s: %w", hostport, err)
	}
	o.logf("tcp: connected %s → %s (no reboot, no drain window — §13; keepalive 3s/1s×3 ≈6s verdict)",
		conn.LocalAddr(), conn.RemoteAddr())
	return &tcpLink{conn: conn}, nil
}

// tcpLink is the network endpoint of the Link contract.
type tcpLink struct {
	conn net.Conn
}

func (l *tcpLink) Read(p []byte) (int, error)  { return l.conn.Read(p) }
func (l *tcpLink) Write(p []byte) (int, error) { return l.conn.Write(p) }
func (l *tcpLink) Close() error                { return l.conn.Close() }
func (l *tcpLink) Kind() string                { return "tcp" }
