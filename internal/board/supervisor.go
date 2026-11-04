// Package board supervises the daemon's controller connection to one
// board: the owner's connection ruling (2026-11-04) — the daemon is the
// board's sole controller, keeping a warm TCP connection while work is
// in flight (connect ≠ reboot, host keepalive per §13), with USB as the
// on-demand fallback whose open reboots the board (§2). Agents never
// touch the board directly; they go through the daemon.
package board

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"xuansi/internal/replclient"
)

// ErrControllerSlotHeld: a fresh TCP connection could not take the
// controller role — the board still holds the old slot because it never
// saw the old connection die (a silent bidirectional black-hole kills
// only the host side; the board's TCP is blind — §12 frees the slot only
// on a disconnect it observes, and the board runs no keepalive of its
// own; measured live 2026-11-05, results/2026-11-05-b5-t10-*.log).
// Recovery is the owner's on-demand USB fallback: ConnectUSB reboots
// the board (§2) into a clean arbitration table. The daemon does NOT
// reboot implicitly — the caller decides (a reboot kills board-side
// scripts; the resync after it reports that honestly).
var ErrControllerSlotHeld = errors.New("controller slot held by a connection the board cannot see dead")

// BoardRef identifies one dialable board (from the discover registry).
type BoardRef struct {
	MAC6    string
	Host    string // mDNS face, e.g. "xuansi-d75884.local"
	Port    int    // REPL TCP listener, 7777
	USBPath string // /dev/serial/by-id path (fallback; opening reboots)
}

// TCPAddr renders the TCP endpoint, "" when the board has no mdns face.
func (b BoardRef) TCPAddr() string {
	if b.Host == "" || b.Port == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

// Controller is the connection seam the Supervisor drives. Production
// is a *replclient.Client (its method set satisfies this interface
// verbatim); unit tests script a fake.
type Controller interface {
	Op(ctx context.Context, op string, args map[string]any) (map[string]any, error)
	Hello(observer bool, clientName string) (*replclient.HelloInfo, error)
	Dead() bool
	DeadReason() string
	Close() error
	OnEvent(sourcePrefix string, fn func(replclient.Event)) (cancel func())
}

// SupOpts parameterize a Supervisor; zero values get defaults.
type SupOpts struct {
	// DialTCP/DialUSB are the dial seams (tests inject fakes); defaults
	// dial the BoardRef over replclient (TCP: warm controller route;
	// USB: fallback whose open reboots the board, §2).
	DialTCP func() (Controller, error)
	DialUSB func() (Controller, error)

	ClientName string // hello client name (default "xuansi-daemon")
	Log        func(format string, args ...any)
	Quiet      time.Duration // diag.pins fallback quiet window (default 250ms)
	RetryWait  time.Duration // connect/arbitration retry spacing (default 500ms; 0 legal for tests)
}

// Supervisor owns one board's controller connection and the resync
// baseline. All ops route through op: a live warm connection is reused;
// a dead one triggers a fresh TCP dial (§13: redial == reconnect, no
// reboot) with one op retry so a dead-link op heals.
type Supervisor struct {
	ref  BoardRef
	opts SupOpts

	mu       sync.Mutex
	ctl      Controller
	base     baseline
	haveBase bool
}

// baseline is the last observed board-side listing Resync diffs against.
type baseline struct {
	scripts []string
	pins    []PinClaim
}

// NewSupervisor builds the per-board Supervisor (no connection yet —
// dialing is explicit: Connect warms the TCP route, ConnectUSB the
// on-demand fallback).
func NewSupervisor(ref BoardRef, opts SupOpts) *Supervisor {
	if opts.ClientName == "" {
		opts.ClientName = "xuansi-daemon"
	}
	if opts.Quiet <= 0 {
		opts.Quiet = 250 * time.Millisecond
	}
	if opts.RetryWait <= 0 {
		// 0 stays 0 only when explicitly meaningful? No: default the
		// spacing; tests that want zero set it AFTER construction via
		// the exported opts copy. Keep the struct field honest.
		if opts.RetryWait < 0 {
			opts.RetryWait = 500 * time.Millisecond
		}
	}
	s := &Supervisor{ref: ref, opts: opts}
	addr := ref.TCPAddr()
	if opts.DialTCP == nil {
		if addr == "" {
			opts.DialTCP = func() (Controller, error) {
				return nil, fmt.Errorf("board %s: no TCP endpoint to dial", ref.MAC6)
			}
		} else {
			o := replclient.DialOpts{Log: opts.Log}
			opts.DialTCP = func() (Controller, error) {
				return replclient.DialTCP(addr, o)
			}
		}
	}
	if opts.DialUSB == nil {
		if ref.USBPath == "" {
			opts.DialUSB = func() (Controller, error) {
				return nil, fmt.Errorf("board %s: no USB path to dial", ref.MAC6)
			}
		} else {
			o := replclient.DialOpts{Log: opts.Log}
			opts.DialUSB = func() (Controller, error) {
				return replclient.DialUSB(ref.USBPath, o)
			}
		}
	}
	s.opts = opts
	return s
}

// name resolves the hello client name.
func (s *Supervisor) name() string { return s.opts.ClientName }

// logf logs through the opts seam when present.
func (s *Supervisor) logf(format string, args ...any) {
	if s.opts.Log != nil {
		s.opts.Log(format, args...)
	}
}

// Connect ensures a live warm TCP controller connection: an existing
// live one is reused; otherwise it dials (spaced retries bounded by
// ctx), says hello as the daemon, and — when the board still holds the
// old controller slot (a dead link's FIN may lag) — re-hellos until the
// controller role is granted (§5/§12: re-hello switches identity on the
// same connection). The daemon is the sole controller: never observer.
func (s *Supervisor) Connect(ctx context.Context) (Controller, error) {
	s.mu.Lock()
	ctl := s.ctl
	s.mu.Unlock()
	if ctl != nil && !ctl.Dead() {
		return ctl, nil
	}
	if ctl != nil {
		ctl.Close() // release the corpse (TCP close frees the board slot)
	}

	ctl, err := s.dialLoop(ctx, s.opts.DialTCP)
	if err != nil {
		return nil, err
	}
	// Take the controller role: a lingering old slot forces the first
	// hello to observer; re-hello (same connection, §5) until granted.
	var last *replclient.HelloInfo
	for i := 0; i < 8; i++ {
		if i > 0 {
			if err := sleepCtx(ctx, s.opts.RetryWait); err != nil {
				ctl.Close()
				return nil, err
			}
		}
		info, herr := ctl.Hello(false, s.name())
		if herr != nil {
			ctl.Close()
			return nil, fmt.Errorf("board %s: hello: %w", s.ref.MAC6, herr)
		}
		last = info
		if info.Role == "controller" {
			s.mu.Lock()
			s.ctl = ctl
			s.mu.Unlock()
			s.logf("board %s: controller warm connection up (fw=%s protocol_ver=%d)",
				s.ref.MAC6, info.FwBuild, info.ProtocolVer)
			return ctl, nil
		}
		s.logf("board %s: hello forced observer (old slot lingering) — re-hello attempt %d", s.ref.MAC6, i+1)
	}
	ctl.Close()
	return nil, fmt.Errorf("board %s: %w (last role=%q; recover via the deliberate USB fallback — ConnectUSB)",
		s.ref.MAC6, ErrControllerSlotHeld, last.Role)
}

// dialLoop retries the dial seam until success or ctx ends.
func (s *Supervisor) dialLoop(ctx context.Context, dial func() (Controller, error)) (Controller, error) {
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("board %s: dial abandoned: %w", s.ref.MAC6, err)
		}
		ctl, err := dial()
		if err == nil {
			return ctl, nil
		}
		if i == 0 {
			s.logf("board %s: dial failed: %v", s.ref.MAC6, err)
		}
		if err := sleepCtx(ctx, s.opts.RetryWait); err != nil {
			return nil, fmt.Errorf("board %s: dial abandoned: %w", s.ref.MAC6, err)
		}
	}
}

// ConnectUSB is the on-demand fallback route: opening the CDC port
// REBOOTS the board into a fresh device session (§2) — use only when
// TCP is gone and a reboot is acceptable.
func (s *Supervisor) ConnectUSB(ctx context.Context) (Controller, error) {
	ctl, err := s.dialLoop(ctx, s.opts.DialUSB)
	if err != nil {
		return nil, err
	}
	info, herr := ctl.Hello(false, s.name())
	if herr != nil {
		ctl.Close()
		return nil, fmt.Errorf("board %s: usb hello: %w", s.ref.MAC6, herr)
	}
	s.mu.Lock()
	if s.ctl != nil {
		s.ctl.Close()
	}
	s.ctl = ctl
	s.mu.Unlock()
	s.logf("board %s: USB fallback connection up (board REBOOTED, §2; fw=%s)", s.ref.MAC6, info.FwBuild)
	return ctl, nil
}

// Client returns the live controller, or nil when not connected.
func (s *Supervisor) Client() Controller {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctl != nil && s.ctl.Dead() {
		return nil
	}
	return s.ctl
}

// op runs one op over the warm connection, redialing once when the link
// died under it (dead-link op → fresh TCP dial → retry).
func (s *Supervisor) op(ctx context.Context, op string, args map[string]any) (map[string]any, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		ctl, err := s.Connect(ctx)
		if err != nil {
			return nil, err
		}
		res, err := ctl.Op(ctx, op, args)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if !errors.Is(err, replclient.ErrDead) {
			return nil, err
		}
		s.logf("board %s: op %s hit dead link (%v) — redialing once", s.ref.MAC6, op, err)
		s.mu.Lock()
		if s.ctl == ctl {
			s.ctl.Close()
			s.ctl = nil
		}
		s.mu.Unlock()
	}
	return nil, fmt.Errorf("board %s: %s after redial: %w", s.ref.MAC6, op, lastErr)
}

// TeardownSession runs session.reset{session} on the board: the fw
// releases that session's resident scripts AND its pins to safe state
// (input/high-Z), returning how many scripts stopped.
func (s *Supervisor) TeardownSession(ctx context.Context, session int) (stopped int64, err error) {
	res, err := s.op(ctx, "session.reset", map[string]any{"session": int64(session)})
	if err != nil {
		return 0, err
	}
	n, _ := res["stopped"].(int64)
	s.logf("board %s: session.reset(%d) stopped=%d", s.ref.MAC6, session, n)
	return n, nil
}

// LeaseTeardown implements lease.TeardownFunc over this Supervisor:
// every namespace in sessions (holder session, then the eval namespace
// 0 — lease.teardownNamespaces order) is reset in order; the first
// error aborts (a dead link fails them all — retry via Resync/redial).
func (s *Supervisor) LeaseTeardown(ctx context.Context, boardMAC6, holder string, sessions []int) error {
	for _, sess := range sessions {
		if _, err := s.TeardownSession(ctx, sess); err != nil {
			return fmt.Errorf("board %s: lease teardown (holder %s, namespace %d): %w",
				boardMAC6, holder, sess, err)
		}
	}
	return nil
}

// Close drops the warm connection (TCP close releases the board's
// controller slot, §12).
func (s *Supervisor) Close() {
	s.mu.Lock()
	ctl := s.ctl
	s.ctl = nil
	s.mu.Unlock()
	if ctl != nil {
		ctl.Close()
	}
}

// sleepCtx waits d, aborting early when ctx ends (RetryWait==0 returns
// immediately — the test seam).
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
