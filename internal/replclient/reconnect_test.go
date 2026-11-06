package replclient

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestBackoffSchedule: the wait after the n-th failure doubles from
// 250ms and caps at 4s.
func TestBackoffSchedule(t *testing.T) {
	for _, tc := range []struct {
		failures int
		want     time.Duration
	}{
		{1, 250 * time.Millisecond},
		{2, 500 * time.Millisecond},
		{3, time.Second},
		{4, 2 * time.Second},
		{5, 4 * time.Second},
		{6, 4 * time.Second},
		{100, 4 * time.Second},
	} {
		if got := backoffAfter(tc.failures); got != tc.want {
			t.Errorf("backoffAfter(%d) = %v, want %v", tc.failures, got, tc.want)
		}
	}
}

// reconnectFixture: a dead controller client whose redial route leads
// to a fresh mock board, plus the count of dial attempts.
type reconnectFixture struct {
	c        *Client
	next     *mockBoard
	dials    *int
	dialErrs []error // errors to serve before the first success
}

func newReconnectFixture(t *testing.T) *reconnectFixture {
	t.Helper()
	c, b := newTestClient(t)
	if _, err := c.Hello(false, "hil-name"); err != nil {
		t.Fatalf("hello: %v", err)
	}
	b.goFatal("desync") // §7: nil-id protocol frame → dead
	waitFor(t, c.Dead)

	f := &reconnectFixture{c: c, dials: new(int)}
	f.next = newMockBoard(t)
	c.mu.Lock()
	c.redial = redialRoute{
		opts: DialOpts{Drain: 2 * time.Second},
		dial: func(o DialOpts) (Link, error) {
			*f.dials++
			if len(f.dialErrs) > 0 {
				err := f.dialErrs[0]
				f.dialErrs = f.dialErrs[1:]
				return nil, err
			}
			if o.Drain != 2*time.Second {
				return nil, errors.New("reconnect lost the caller's drain window")
			}
			return f.next.Link, nil
		},
	}
	c.mu.Unlock()
	return f
}

// TestReconnectReHellosWithPreviousRole: Reconnect dials the successor,
// re-hellos with the previous requested identity (client name carried,
// non-observer), and returns a live client. The T4 lesson: the dial
// opts (drain window) must carry over verbatim.
func TestReconnectReHellosWithPreviousRole(t *testing.T) {
	f := newReconnectFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nc, h, err := f.c.Reconnect(ctx)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	if h.Role != "controller" || h.ProtocolVer != 4 {
		t.Fatalf("reconnect hello = role %q ver %d", h.Role, h.ProtocolVer)
	}
	if *f.dials != 1 {
		t.Fatalf("dials = %d, want 1", *f.dials)
	}

	reqs := f.next.got()
	if len(reqs) != 1 || reqs[0].op != "hello" {
		t.Fatalf("successor board saw %#v", reqs)
	}
	if reqs[0].args["client"] != "hil-name" {
		t.Fatalf("re-hello client = %#v, want the previous name carried", reqs[0].args["client"])
	}
	if _, ok := reqs[0].args["observer"]; ok {
		t.Fatal("previous non-observer role must be re-requested without the observer key")
	}
	if _, err := nc.Op(ctx, "ping", nil); err != nil {
		t.Fatalf("successor client unusable: %v", err)
	}
}

// TestReconnectCarriesObserverRole: an observer client re-hellos as
// observer.
func TestReconnectCarriesObserverRole(t *testing.T) {
	c, b := newTestClient(t)
	if _, err := c.Hello(true, "obs"); err != nil {
		t.Fatalf("hello: %v", err)
	}
	b.goFatal("desync")
	waitFor(t, func() bool { return c.Dead() })

	next := newMockBoard(t)
	c.mu.Lock()
	c.redial = redialRoute{dial: func(o DialOpts) (Link, error) { return next.Link, nil }}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	nc, h, err := c.Reconnect(ctx)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	if h.Role != "observer" {
		t.Fatalf("role = %q, want observer re-requested", h.Role)
	}
	reqs := next.got()
	if len(reqs) != 1 || reqs[0].args["observer"] != true {
		t.Fatalf("re-hello args = %#v, want observer=true carried", reqs[0].args)
	}
}

// TestReconnectRetriesWithBackoffUntilCtx: a route that keeps failing
// retries under the backoff schedule and finally reports the context
// error naming the failure count.
func TestReconnectRetriesWithBackoffUntilCtx(t *testing.T) {
	f := newReconnectFixture(t)
	c := f.c
	c.mu.Lock()
	c.redial = redialRoute{dial: func(o DialOpts) (Link, error) {
		*f.dials++
		return nil, errors.New("no carrier")
	}}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := c.Reconnect(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if !strings.Contains(err.Error(), "attempt") {
		t.Fatalf("err = %v, want the failure count named", err)
	}
	elapsed := time.Since(start)
	// t0 fail → wait 250ms → t250 fail → wait 500ms → t750 (ctx fires
	// at 700ms): two dials done, a third never starts.
	if *f.dials != 2 {
		t.Fatalf("dials = %d after %v, want 2", *f.dials, elapsed)
	}
}

// TestReconnectNeedsRoute: a bare NewClient has no redial route.
func TestReconnectNeedsRoute(t *testing.T) {
	c, _ := newTestClient(t)
	if _, _, err := c.Reconnect(context.Background()); err == nil {
		t.Fatal("Reconnect on a route-less client succeeded")
	}
}

// TestSendRawDesyncProbe: injecting the reserved-type frame ([len=1,
// type 0x02], §7) makes the board emit its fatal nil-id protocol reply,
// which kills the client — the HIL desync scenario in miniature.
func TestSendRawDesyncProbe(t *testing.T) {
	c, _ := newTestClient(t)
	if err := c.SendRaw([]byte{0, 0, 0, 1, 0x02}); err != nil {
		t.Fatalf("sendraw: %v", err)
	}
	waitFor(t, c.Dead)
	if reason := c.DeadReason(); !strings.Contains(reason, "protocol") {
		t.Fatalf("dead reason = %q, want the fatal protocol error", reason)
	}
	if err := c.SendRaw([]byte{0}); !errors.Is(err, ErrDead) {
		t.Fatalf("post-dead sendraw err = %v, want ErrDead", err)
	}
}
