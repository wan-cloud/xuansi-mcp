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
	"net"
	"strconv"
	"sync"
	"time"

	"xuansi/internal/replclient"
)

// ErrControllerSlotHeld: a fresh TCP connection could not take the
// controller role within the full arbitration window (ArbitrationWait,
// default 25s). A plain silent dead link does NOT cause this: the board
// runs its own keepalive on every REPL connection (10s idle + 2s×3
// probes — §12 frozen values; half-open slot self-release measured
// 16.5–17.0s live, batch 4 + task 10 fix1), so the supervisor's
// persistent re-hellos ride out that window and take the slot as soon
// as the board releases it. Reaching this error means something beyond
// a silent dead link (fw without board-side keepalive, or a genuinely
// live connection still holding the slot). LAST RESORT only:
// ConnectUSB reboots the board (§2) into a clean arbitration table —
// the reboot KILLS every board-side script, inverting the 斷鏈-resync
// ruling (scripts survive link loss and must be reported, not killed);
// the caller owns that trade-off.
var ErrControllerSlotHeld = errors.New("controller slot not freed within the arbitration window (board keepalive self-release ~17s, §12) — last resort is the deliberate USB fallback, which reboots the board and kills surviving scripts")

// ErrNoDialEndpoint marks a PERMANENT dial refusal: the BoardRef has no
// TCP endpoint (or no USB path) to dial at all — retrying cannot help,
// so dialLoop fails fast instead of burning the caller's whole request
// budget rediscovering that (the USB-only first appearance, Bug 1's
// sibling shape — the endpoint may appear later, and the supervisor
// rebuild on endpoint drift is what picks it up).
var ErrNoDialEndpoint = errors.New("no dial endpoint in the board ref")

// dialAttempts bounds one dialLoop run (B15 §4.1③): three tries — with
// the 6s tcpConnectTimeout inside the real dialer that is an ~18s worst
// case per Connect — instead of riding the caller's full 60s request
// budget in a synchronous spin. A refused endpoint answers instantly,
// so the bound usually costs milliseconds.
const dialAttempts = 3

// BoardRef identifies one dialable board (from the discover registry).
type BoardRef struct {
	MAC6    string
	Host    string // mDNS face, e.g. "xuansi-d75884.local"
	IP      string // browse-resolved A/AAAA, e.g. "10.1.51.205" — dialed first (CGO=0 cannot resolve .local)
	Port    int    // REPL TCP listener, 7777
	USBPath string // /dev/serial/by-id path (fallback; opening reboots)
}

// TCPAddr renders the TCP endpoint, "" when the board has no mdns
// face. The browse-resolved IP is preferred over the hostname: pure-Go
// (CGO_ENABLED=0) builds resolve through the pure resolver, which does
// not speak mDNS — .local SERVFAILs there — so the address the mDNS
// browse already carried is the reliable route. Rendering goes through
// net.JoinHostPort — the same contract DialBoard uses — so a v6
// address dials as "[addr]:port" (a bare "fe80::1:7777" is not a
// dialable endpoint).
func (b BoardRef) TCPAddr() string {
	host := b.IP
	if host == "" {
		host = b.Host
	}
	if host == "" || b.Port == 0 {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(b.Port))
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
	// OnStream installs this connection's 0x02 data-frame sink (nil
	// drops them). Set-only and PER-CONNECTION: a redial's fresh
	// connection starts with no sink — every consumer registers its
	// own on the connection it is about to use (the board's frame hub
	// is asserted around each streaming op, so a redial retry
	// re-asserts it on the new link).
	OnStream(sink replclient.StreamSink)
}

// SupOpts parameterize a Supervisor; zero values get defaults.
type SupOpts struct {
	// DialTCP/DialUSB are the dial seams (tests inject fakes); defaults
	// dial the BoardRef over replclient (TCP: warm controller route;
	// USB: fallback whose open reboots the board, §2).
	DialTCP func() (Controller, error)
	DialUSB func() (Controller, error)

	// BootCapture (B16-T6 D1), when non-nil, rides the USB dial's drain
	// window: the CDC open's reboot makes the window's tail the boot
	// banner — the zero-false-positive capture source (spec §4D). The
	// TCP dial ignores it (no reboot semantics). Fired per open.
	BootCapture func(raw []byte)

	ClientName string // hello client name (default "xuansi-daemon")
	Log        func(format string, args ...any)
	Quiet      time.Duration // diag.pins fallback quiet window (default 250ms)
	RetryWait  time.Duration // arbitration/dial retry spacing (default 500ms; 0 legal with an injected Wait)
	// ArbitrationWait is the total re-hello budget after a dead link:
	// it MUST cover the board's own keepalive slot self-release (10s
	// idle + 2s×3 probes, §12 frozen; measured 16.5–17.0s) plus margin
	// — default 25s. Only after this full window may Connect report
	// ErrControllerSlotHeld (USB reboot = explicit last resort).
	ArbitrationWait time.Duration
	// Wait is the sleep seam for the retry loops; tests inject a
	// virtual clock (advances instantly) so the ≥25s schedule is
	// proven without real waiting. Nil = real context-aware sleep.
	Wait func(d time.Duration) error
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
	ctlUSB   bool // ctl is the USB fallback link (yield-before-close, §12)
	haveBase bool
	base     baseline
}

// usbYieldBound bounds the voluntary §12 yield hello sent before
// closing a live USB controller link. The hello is an immediate op
// (§6; the client's own op bound is 5s) — 2s is generous, and a yield
// that misses it must never delay the close.
const usbYieldBound = 2 * time.Second

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
	if opts.RetryWait <= 0 && opts.Wait == nil {
		opts.RetryWait = 500 * time.Millisecond
	}
	if opts.ArbitrationWait <= 0 {
		// covers the board's own keepalive slot self-release (16.5–17.0s
		// measured, §12) with margin — see ErrControllerSlotHeld
		opts.ArbitrationWait = 25 * time.Second
	}
	s := &Supervisor{ref: ref, opts: opts}
	addr := ref.TCPAddr()
	if opts.DialTCP == nil {
		if addr == "" {
			opts.DialTCP = func() (Controller, error) {
				return nil, fmt.Errorf("board %s: no TCP endpoint to dial: %w", ref.MAC6, ErrNoDialEndpoint)
			}
		} else {
			opts.DialTCP = func() (Controller, error) {
				// DialOpts built at DIAL time, read through the LIVE
				// opts (s.opts — Opts() re-wirings flow into every
				// dial; the BootCapture the server's D1 wrap attaches
				// post-construction rides exactly this seam, per-open
				// by contract).
				return replclient.DialTCP(addr, replclient.DialOpts{Log: s.opts.Log})
			}
		}
	}
	if opts.DialUSB == nil {
		if ref.USBPath == "" {
			opts.DialUSB = func() (Controller, error) {
				return nil, fmt.Errorf("board %s: no USB path to dial: %w", ref.MAC6, ErrNoDialEndpoint)
			}
		} else {
			opts.DialUSB = func() (Controller, error) {
				return replclient.DialUSB(ref.USBPath, replclient.DialOpts{
					Log: s.opts.Log, BootCapture: s.opts.BootCapture})
			}
		}
	}
	s.opts = opts
	return s
}

// Opts returns the Supervisor's live options — the seams (dial funcs,
// wait budget) may be re-wired at runtime, e.g. a USB-dial tripwire in
// the HIL proving the §12-faithful TCP recovery never touches USB.
func (s *Supervisor) Opts() *SupOpts { return &s.opts }

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
// old controller slot — keeps re-helloing for the whole ArbitrationWait
// window (default 25s): a silently dead old link self-releases the slot
// via the BOARD's own keepalive (10s idle + 2s×3 probes, §12; measured
// 16.5–17.0s), and the persistent re-hellos take the slot the moment it
// frees — no reboot, surviving scripts untouched. The daemon is the
// sole controller: never observer. ErrControllerSlotHeld (after the
// FULL window) is the caller's signal that the last-resort USB fallback
// is on the table.
func (s *Supervisor) Connect(ctx context.Context) (Controller, error) {
	s.mu.Lock()
	ctl := s.ctl
	s.mu.Unlock()
	if ctl != nil && !ctl.Dead() {
		return ctl, nil
	}
	if ctl != nil {
		ctl.Close() // release the corpse; with the link merely half-open
		// this FIN/RST also nudges the board's slot release (§12)
	}

	ctl, err := s.dialLoop(ctx, s.opts.DialTCP)
	if err != nil {
		return nil, err
	}
	// Take the controller role: a lingering old slot forces the first
	// hellos to observer; re-hello (same connection, §5) and wait out
	// the board's keepalive self-release window.
	slept := time.Duration(0)
	attempt := 0
	for {
		attempt++
		info, herr := ctl.Hello(false, s.name())
		if herr != nil {
			ctl.Close()
			return nil, fmt.Errorf("board %s: hello: %w", s.ref.MAC6, herr)
		}
		if info.Role == "controller" {
			s.mu.Lock()
			s.ctl = ctl
			s.ctlUSB = false
			s.mu.Unlock()
			if attempt > 1 {
				s.logf("board %s: controller taken on re-hello %d after %v of the %v window (slot self-released — board keepalive, §12)",
					s.ref.MAC6, attempt, slept.Round(time.Millisecond), s.opts.ArbitrationWait)
			}
			s.logf("board %s: controller warm connection up (fw=%s protocol_ver=%d)",
				s.ref.MAC6, info.FwBuild, info.ProtocolVer)
			return ctl, nil
		}
		if slept >= s.opts.ArbitrationWait {
			break
		}
		d := s.opts.RetryWait
		if d <= 0 || d > s.opts.ArbitrationWait-slept {
			d = s.opts.ArbitrationWait - slept
		}
		if err := s.waitCtx(ctx, d); err != nil {
			ctl.Close()
			return nil, err
		}
		slept += d
		if attempt == 1 {
			s.logf("board %s: hello forced observer (old slot not yet released) — re-helloing through the board's keepalive window (self-release ~17s, §12; budget %v)",
				s.ref.MAC6, s.opts.ArbitrationWait)
		}
	}
	ctl.Close()
	return nil, fmt.Errorf("board %s: %w (waited %v over %d re-hello attempts)",
		s.ref.MAC6, ErrControllerSlotHeld, slept.Round(time.Millisecond), attempt)
}

// waitCtx sleeps d through the Wait seam when injected (virtual-clock
// tests), else for real, aborting early when ctx ends.
func (s *Supervisor) waitCtx(ctx context.Context, d time.Duration) error {
	if s.opts.Wait != nil {
		return s.opts.Wait(d)
	}
	return sleepCtx(ctx, d)
}

// dialLoop retries the dial seam a bounded number of times (B15 §4.1③:
// dialAttempts tries ≈ 3 × tcpConnectTimeout worst case, replacing the
// full-request-budget spin). A PERMANENT refusal (ErrNoDialEndpoint)
// fails immediately — no retry can conjure an endpoint.
func (s *Supervisor) dialLoop(ctx context.Context, dial func() (Controller, error)) (Controller, error) {
	var lastErr error
	for i := 0; i < dialAttempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("board %s: dial abandoned: %w", s.ref.MAC6, err)
		}
		ctl, err := dial()
		if err == nil {
			return ctl, nil
		}
		lastErr = err
		if i == 0 {
			s.logf("board %s: dial failed: %v", s.ref.MAC6, err)
		}
		if errors.Is(err, ErrNoDialEndpoint) {
			return nil, fmt.Errorf("board %s: %w", s.ref.MAC6, err)
		}
		if i == dialAttempts-1 {
			break
		}
		if err := s.waitCtx(ctx, s.opts.RetryWait); err != nil {
			return nil, fmt.Errorf("board %s: dial abandoned: %w", s.ref.MAC6, err)
		}
	}
	return nil, fmt.Errorf("board %s: dial failed after %d attempts: %w", s.ref.MAC6, dialAttempts, lastErr)
}

// SameLinkRetry reports whether err is the §4.2 same-link-retry shape:
// the op's REPLY timed out (replclient.ErrOpTimeout) while the
// controller is still live — the frame is likely lost in transit, so
// the idempotent faces resend the SAME op once on the SAME connection
// instead of tearing down a healthy link. A dead-link verdict
// (replclient.ErrDead) never qualifies — that path redials.
func SameLinkRetry(err error, ctl Controller) bool {
	return ctl != nil && !ctl.Dead() && errors.Is(err, replclient.ErrOpTimeout)
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
	old := s.ctl
	oldUSB := s.ctlUSB
	s.ctl = ctl
	s.ctlUSB = true
	s.mu.Unlock()
	if old != nil && old != ctl {
		s.yieldAndClose(old, oldUSB, "USB fallback swap")
	}
	s.logf("board %s: USB fallback connection up (board REBOOTED, §2; fw=%s)", s.ref.MAC6, info.FwBuild)
	return ctl, nil
}

// YieldUSBController voluntarily gives up a live USB controller link:
// the §12 observer yield, then close, leaving the supervisor with
// nothing connected so the next touch dials the default TCP route into
// a free slot. Reports whether a USB controller was dropped. A TCP
// controller is left warm exactly as before — §12 releases its slot on
// close by itself, so there is nothing to yield and no reason to drop
// the warm route. Serves USB-transport lease release.
func (s *Supervisor) YieldUSBController() bool {
	s.mu.Lock()
	ctl := s.ctl
	if ctl == nil || !s.ctlUSB {
		s.mu.Unlock()
		return false
	}
	s.ctl = nil
	s.ctlUSB = false
	s.mu.Unlock()
	s.yieldAndClose(ctl, true, "USB lease release")
	return true
}

// TakeUSBController detaches a LIVE USB controller link WITHOUT
// closing it (B16 F3-①, the §4.4 drift-seam carry-over): an endpoint
// rebuild whose fresh ref keeps the same USBPath can keep serving the
// very link the old seams built — no §12 yield, no corpse close, and
// above all no CDC re-open (one open = one board reboot, §2). Returns
// nil when the warm link is not a live USB controller; the caller
// pairs a non-nil take with AdoptUSBController on the fresh supervisor
// under its own serialization (the daemon's per-board connectMu), so
// the link never leaks between the two halves.
func (s *Supervisor) TakeUSBController() Controller {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctl == nil || s.ctl.Dead() || !s.ctlUSB {
		return nil
	}
	ctl := s.ctl
	s.ctl = nil
	s.ctlUSB = false
	return ctl
}

// AdoptUSBController installs a live USB controller as the warm link
// (the receiving half of the §4.4 drift-seam carry-over — see
// TakeUSBController). A dead or nil controller is ignored (nothing to
// carry); the tappable identity, event handlers, and dial seams all
// live with the caller's per-board state, which is exactly why the
// same controller object can move between supervisors unharmed.
func (s *Supervisor) AdoptUSBController(ctl Controller) {
	if ctl == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctl.Dead() {
		return
	}
	s.ctl = ctl
	s.ctlUSB = true
}

// yieldAndClose closes one controller link — voluntarily yielding the
// §12 controller slot FIRST when the link is a live USB connection: a
// TCP close is visible to the board (FIN/RST → slot release, §12), but
// a closed CDC port is INVISIBLE to the board (no FIN analogue; the
// board's keepalive probes cannot fail against a detached CDC), so
// without the observer re-hello the USB session's slot stays wedged
// past the whole 25s arbitration window and every later TCP lease
// needs the headless-reopen ritual (measured live, fixwave smoke
// S6b–S6d). Best-effort and short-bounded (hello is an immediate op,
// §6): a failed or slow yield is logged honestly and never blocks the
// close — at worst the slot wedges as before, cleared by the next
// board reboot.
func (s *Supervisor) yieldAndClose(ctl Controller, usb bool, why string) {
	if ctl == nil {
		return
	}
	if usb && !ctl.Dead() {
		done := make(chan error, 1)
		go func() {
			_, err := ctl.Hello(true, s.name()+"-yield")
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				s.logf("board %s: USB controller yielded the §12 slot (observer hello) before %s", s.ref.MAC6, why)
			} else {
				s.logf("board %s: USB yield before %s failed (%v) — closing anyway", s.ref.MAC6, why, err)
			}
		case <-time.After(usbYieldBound):
			s.logf("board %s: USB yield before %s did not answer within %v — closing anyway", s.ref.MAC6, why, usbYieldBound)
		}
	}
	ctl.Close()
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

// Transport reports the CURRENT ACTUAL controller route (B15 §5):
// "tcp" or "usb" for the live warm link, "" when nothing is connected —
// never the preferred route, the one in hand (a USB-leased board's
// later ops ride that USB controller up to release, whatever the
// default route would dial). board_status/list_boards surface it.
func (s *Supervisor) Transport() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctl == nil || s.ctl.Dead() {
		return ""
	}
	if s.ctlUSB {
		return "usb"
	}
	return "tcp"
}

// op runs one op over the warm connection. §4.2 (B15): a reply timeout
// on a LIVE link resends the SAME op once on the SAME connection — the
// supervisor's internal ops (script.list / diag.pins / wifi.status /
// session sweeps) are idempotent reads or namespace resets, all safe to
// resend; tearing down a healthy link over one lost frame was the
// timeout→redial→25s-arbitration spiral's root. A dead-link verdict
// (ErrDead) still tears down and redials exactly as before.
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
		if SameLinkRetry(err, ctl) {
			s.logf("board %s: op %s reply timed out with the link alive — resending once on the same link (§4.2)", s.ref.MAC6, op)
			res, err = ctl.Op(ctx, op, args)
			if err == nil {
				return res, nil
			}
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
// controller slot, §12). A live USB controller link yields the §12
// slot first — the board cannot see a CDC close (yieldAndClose).
func (s *Supervisor) Close() {
	s.mu.Lock()
	ctl := s.ctl
	usb := s.ctlUSB
	s.ctl = nil
	s.ctlUSB = false
	s.mu.Unlock()
	if ctl != nil {
		s.yieldAndClose(ctl, usb, "supervisor close")
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
