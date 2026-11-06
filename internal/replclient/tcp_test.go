package replclient

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestDialTCPHelloFirstFrame: over a real loopback TCP listener, the
// first frame DialTCP's client writes can be hello directly — no drain
// window, no boot prefix (§13) — and the transport round-trips ops.
func TestDialTCPHelloFirstFrame(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		serveMockBoardOver(t, conn)
	}()

	c, err := DialTCP(ln.Addr().String(), DialOpts{})
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	if got := c.link.Kind(); got != "tcp" {
		t.Fatalf("link kind = %q, want tcp", got)
	}

	// The very first frame on the wire is the hello request: the mock
	// board answers only framed requests, so a hello reply at all proves
	// byte one was frame-synced (no drain, no junk absorption).
	start := time.Now()
	h, err := c.Hello(false, "tcp-unit")
	if err != nil {
		t.Fatalf("hello: %v", err)
	}
	if h.ProtocolVer != 4 || h.Role != "controller" {
		t.Fatalf("hello = ver %d role %q, want 3/controller", h.ProtocolVer, h.Role)
	}
	if lat := time.Since(start); lat > time.Second {
		t.Fatalf("hello took %v — first frame was not answered directly", lat)
	}
	if _, err := c.Op(context.Background(), "ping", nil); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

// TestDialTCPRefused: a dead endpoint fails the dial with a wrapped
// error and no client.
func TestDialTCPRefused(t *testing.T) {
	// bind then close: the port is very likely to still be unused
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	if _, err := DialTCP(addr, DialOpts{}); err == nil {
		t.Fatal("dial to a closed port succeeded")
	}
}

// TestDialTCPCloseIsClean: Close tears the router down without hanging
// (the keepalive-configured socket unblocks its reader on close).
func TestDialTCPCloseIsClean(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		serveMockBoardOver(t, conn)
	}()

	c, err := DialTCP(ln.Addr().String(), DialOpts{})
	if err != nil {
		t.Fatal(err)
	}
	done := c.rt.done
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("router goroutine leaked past Close")
	}
	if !c.Dead() {
		t.Fatal("closed client must report dead")
	}
}
