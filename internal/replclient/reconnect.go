package replclient

import (
	"context"
	"fmt"
	"time"
)

// redialRoute remembers how a client was built so Reconnect can build
// its successor the same way.
type redialRoute struct {
	dial func(o DialOpts) (Link, error)
	opts DialOpts
}

// Reconnect rebuilds a dead client on a fresh link: USB re-opens the
// port — the board reboots, a new device session (§2) — TCP dials a new
// connection without rebooting anything (§13). Hello is then re-sent
// with the previous requested role and client name, and the dial opts
// (drain window included) carry over verbatim — a reconnect must not
// silently lose open-time settings (the T4 lesson from the host hub:
// its reconnect once fell back to the constructor default and dropped
// the caller's drain window). Between attempts it backs off 250ms
// doubling to a 4s cap; ctx bounds the whole procedure. The board
// arbitrates the granted role (§12): trust the returned HelloInfo, not
// the request.
func (c *Client) Reconnect(ctx context.Context) (*Client, *HelloInfo, error) {
	c.Close() // idempotent: a dead connection still needs its link released
	c.mu.Lock()
	route, observer, name := c.redial, c.helloWant, c.helloName
	c.mu.Unlock()
	if route.dial == nil {
		return nil, nil, fmt.Errorf("replclient: Reconnect: client was not created by DialUSB/DialTCP (no redial route)")
	}

	failures := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, fmt.Errorf("replclient: reconnect abandoned after %d failed attempt(s): %w", failures, err)
		}
		link, err := route.dial(route.opts)
		if err == nil {
			nc := NewClient(link)
			h, herr := nc.Hello(observer, name)
			if herr == nil {
				route.opts.logf("reconnect: hello ok (role %s, %d failed attempt(s) before)", h.Role, failures)
				return nc, h, nil
			}
			nc.Close()
			err = herr
		}
		failures++
		route.opts.logf("reconnect: attempt %d failed: %v", failures, err)
		wait := backoffAfter(failures)
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("replclient: reconnect abandoned after %d failed attempt(s): %w", failures, ctx.Err())
		case <-time.After(wait):
		}
	}
}

// backoffAfter is the wait after the n-th failed attempt: 250ms
// doubling per failure, capped at 4s (1→250ms, 2→500ms, 3→1s, 4→2s,
// 5→4s, then 4s forever).
func backoffAfter(failures int) time.Duration {
	d := 250 * time.Millisecond
	for i := 1; i < failures; i++ {
		d *= 2
		if d >= 4*time.Second {
			return 4 * time.Second
		}
	}
	return d
}
