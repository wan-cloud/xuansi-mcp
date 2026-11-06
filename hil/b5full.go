// b5full.go — task-12 full-suite HIL: the whole daemon, end to end, in ONE
// capture. The REAL daemon binary runs as a subprocess
// (`xuansi serve --sock ... --grace-ttl 15s`) and every batch-5 component
// is exercised through its unix-socket API by this Go driver (house
// style from b5t8/t9/t10):
//
//		S1  serve up          — subprocess, sock bound, initial discovery sweep
//		                         + startup orphan audit (report-only)
//		S2  discovery         — list shows d75884 dual-source usb+mdns, fw
//		                         hello-enriched; TXT ver=5 proven by a
//		                         driver-side registry browse (the daemon's list
//		                         view surfaces fw, not TXT)
//		S3  lease + eval      — holder=h1 lease via the boards.conf alias;
//		                         structured eval (ARGS round-trip, depth-8
//		                         table through the value bridge)
//		S4  fs 16KB           — write/read byte-exact, sha256 both sides,
//		                         daemon-side 16385B cap refusal
//		S5  submit long job   — 3.5s job: ack immediate, API answered while
//		                         the job runs (no stall), job event carries
//		                         the bridged value
//		S6  gpio events       — claim the wired pair via eval, watch_events on
//		                         a SECOND connection, edges driven through the
//		                         daemon to the watcher
//		S7  grace ADOPT       — holder conn dropped mid-job -> third party
//		                         refused (grace-expiring) -> SAME holder
//		                         reconnects within the TTL -> adopt, script
//	                        survived, the in-flight job's event delivered
//		                         to the reconnected watcher (continuity focus)
//		S8  grace EXPIRY      — drop again, wait out the 15s TTL -> script
//		                         gone, pins safe (write-reject probe), next op
//		                         not_held, expiry teardown exactly once
//		S9  link-loss resync  — silent bidirectional iptables hole on the
//		                         daemon<->board 4-tuple -> traffic-free window
//		                         lets the host keepalive verdict (~6s class:
//		                         the EOF-miss dead-link shape) -> rules out ->
//		                         daemon re-takes the controller over TCP (T10
//		                         fix1: rides out the board-side keepalive
//		                         self-release, no reboot) -> script survived
//		                         AND ran through the hole; the in-flight job's
//		                         body proven complete board-side while its
//		                         result frame died with the aborted socket
//		                         (diag.stats/log.read read driver-direct,
//		                         honestly labeled — the daemon API has no diag)
//		S10 release           — polite release tears the board side down
//		S11 mDNS-degraded     — wifi.off from a driver-direct USB controller
//		                         (LAST, dedicated phase: the USB open reboots
//		                         the board): daemon list degrades to usb-only
//		                         (mdns face withdrawn, last-known kept), ops
//		                         through the daemon fail unreachable, the USB
//		                         transport itself proven alive; restore by CDC
//		                         reboot + NVS auto-connect, dual-source back,
//		                         daemon re-heals
//		S12 clean shutdown    — SIGINT -> exit 0 (srv.Done waits for every
//		                         server goroutine — the honest no-leak bar for
//		                         a subprocess), sock removed, shutdown-complete
//
// The 15s grace TTL is a REAL second-scale window waited out honestly in
// S7/S8. sudo/iptables drives the S9 hole; S11 uses the wifi.off variant
// deliberately (dedicated degraded phase, run LAST so the reboot poisons
// nothing before it). One board on the bench: multi-board paths are
// unit-covered (registry_test.go) — noted in the header.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"xuansi/internal/discover"
	"xuansi/internal/replclient"
	"xuansi/internal/server"
)

// ---------------------------------------------------------------- ASCII --

// ascii sanitizes one captured line to plain ASCII (the daemon's slog
// shutdown line carries an em-dash; the capture file stays clean ASCII).
func asciiLine(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 32 || r > 126 {
			return '-'
		}
		return r
	}, s)
}

// ------------------------------------------------------------------ RPC --

// apiReply is one resolved request round.
type apiReply struct {
	result map[string]any
	rerr   *server.RPCError
	err    error
}

// apiConn is one framed-JSON connection to the daemon's unix socket
// (contract: internal/server/proto.go): replies matched by id, event
// pushes fanned to a channel. It is the driver-side stand-in for an agent.
type apiConn struct {
	conn   net.Conn
	events chan server.EventPush

	mu      sync.Mutex
	nextID  int64
	pending map[int64]chan apiReply

	dead     chan struct{}
	closeOne sync.Once
}

func dialAPI(h *harness, sock, tag string) *apiConn {
	conn, err := net.Dial("unix", sock)
	if err != nil {
		h.check(false, fmt.Sprintf("dial daemon sock (%s): %v", tag, err))
		return nil
	}
	c := &apiConn{
		conn:    conn,
		events:  make(chan server.EventPush, 128),
		pending: map[int64]chan apiReply{},
		dead:    make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *apiConn) readLoop() {
	for {
		var m map[string]any
		if err := server.ReadMsg(c.conn, &m); err != nil {
			c.failPending(fmt.Errorf("connection closed: %w", err))
			close(c.dead)
			return
		}
		switch m["type"] {
		case "event":
			var ev server.EventPush
			ev.Board, _ = m["board"].(string)
			if e, ok := m["event"].(map[string]any); ok {
				ev.Event.Source, _ = e["source"].(string)
				if d, ok := e["data"].(map[string]any); ok {
					ev.Event.Data = d
				}
			}
			select {
			case c.events <- ev:
			default: // overflow drops (fire-and-forget, fw §8 style)
			}
		case "reply":
			id, _ := m["id"].(float64)
			c.mu.Lock()
			ch, ok := c.pending[int64(id)]
			delete(c.pending, int64(id))
			c.mu.Unlock()
			if !ok {
				continue
			}
			r := apiReply{}
			if e, ok := m["error"].(map[string]any); ok {
				r.rerr = &server.RPCError{Class: fmt.Sprint(e["class"]), Msg: fmt.Sprint(e["msg"])}
				r.rerr.Holder, _ = e["holder"].(string)
				r.rerr.Since, _ = e["since"].(string)
				r.rerr.GraceExpiry, _ = e["grace_expiry"].(string)
			} else {
				r.result, _ = m["result"].(map[string]any)
			}
			select {
			case ch <- r:
			default:
			}
		}
	}
}

func (c *apiConn) failPending(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pending {
		select {
		case ch <- apiReply{err: err}:
		default:
		}
		delete(c.pending, id)
	}
}

func (c *apiConn) close() {
	c.closeOne.Do(func() { c.conn.Close() })
	<-c.dead // readLoop noticed; pending callers are resolved
}

func (c *apiConn) call(timeout time.Duration, req server.Request) (map[string]any, *server.RPCError, error) {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	ch := make(chan apiReply, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	req.ID = id
	if err := server.WriteMsg(c.conn, &req); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, nil, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.result, r.rerr, r.err
	case <-timer.C:
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, nil, fmt.Errorf("%s: client timeout after %v", req.Type, timeout)
	case <-c.dead:
		select { // the readLoop's failPending may race us to the channel
		case r := <-ch:
			return r.result, r.rerr, r.err
		case <-time.After(100 * time.Millisecond):
			return nil, nil, fmt.Errorf("%s: connection closed", req.Type)
		}
	}
}

// waitEvent waits for one event whose source has the given prefix.
func (c *apiConn) waitEvent(sourcePrefix string, bound time.Duration) (server.EventPush, bool) {
	deadline := time.After(bound)
	for {
		select {
		case e := <-c.events:
			if strings.HasPrefix(e.Event.Source, sourcePrefix) {
				return e, true
			}
			fmt.Printf("  [event] (other) board=%s source=%s data=%v\n", e.Board, e.Event.Source, e.Event.Data)
		case <-deadline:
			return server.EventPush{}, false
		}
	}
}

// --------------------------------------------------------------- daemon --

// daemonProc is the REAL daemon as a subprocess, its stdout+stderr teed
// into the transcript (prefixed [daemon]) and greppable for verdicts.
type daemonProc struct {
	cmd  *exec.Cmd
	sock string

	mu       sync.Mutex
	buf      strings.Builder
	shutdown bool

	exited chan struct{}
	code   int
	waitEr error
}

func startDaemon(h *harness, bin, sock, boardsFile string, ttl time.Duration) *daemonProc {
	cmd := exec.Command(bin, "serve",
		"--sock", sock,
		"--grace-ttl", ttl.String(),
		"--boards-file", boardsFile)
	stdout, err1 := cmd.StdoutPipe()
	stderr, err2 := cmd.StderrPipe()
	if err1 != nil || err2 != nil {
		h.check(false, fmt.Sprintf("daemon pipes: %v %v", err1, err2))
		return nil
	}
	if err := cmd.Start(); err != nil {
		h.check(false, "start daemon subprocess: "+err.Error())
		return nil
	}
	d := &daemonProc{cmd: cmd, sock: sock, exited: make(chan struct{})}
	fmt.Printf("  [daemon] started pid=%d argv=%q\n", cmd.Process.Pid, cmd.Args)
	go d.pump(stdout)
	go d.pump(stderr)
	go func() {
		err := cmd.Wait()
		d.mu.Lock()
		d.waitEr = err
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			d.code = ee.ExitCode()
		}
		d.mu.Unlock()
		close(d.exited)
	}()
	return d
}

func (d *daemonProc) pump(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := asciiLine(sc.Text())
		fmt.Printf("  [daemon] %s\n", line)
		d.mu.Lock()
		d.buf.WriteString(line)
		d.buf.WriteByte('\n')
		if strings.Contains(line, "shutdown complete") {
			d.shutdown = true
		}
		d.mu.Unlock()
	}
}

func (d *daemonProc) output() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.buf.String()
}

func (d *daemonProc) has(s string) bool { return strings.Contains(d.output(), s) }

func (d *daemonProc) count(s string) int { return strings.Count(d.output(), s) }

func (d *daemonProc) shutdownDone() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.shutdown
}

// sigintAndWait delivers SIGINT and waits for a clean exit within bound.
func (d *daemonProc) sigintAndWait(h *harness, bound time.Duration) bool {
	if err := d.cmd.Process.Signal(syscall.SIGINT); err != nil {
		h.check(false, "SIGINT the daemon: "+err.Error())
		return false
	}
	select {
	case <-d.exited:
		return true
	case <-time.After(bound):
		h.check(false, fmt.Sprintf("daemon did not exit within %v of SIGINT (SIGKILLed)", bound))
		_ = d.cmd.Process.Signal(syscall.SIGKILL)
		<-d.exited
		return false
	}
}

// ----------------------------------------------------------------- hole --

// holeState tracks inserted iptables rule pairs so a deferred net can
// always restore the bench, even on an early failure path.
var holeState struct {
	mu    sync.Mutex
	pairs []string // "ip sport" strings currently inserted
}

func holeInsert(h *harness, ip, sport string) bool {
	rules := [][]string{
		{"-I", "OUTPUT", "1", "-p", "tcp", "--dport", "7777", "--sport", sport, "-d", ip, "-j", "DROP"},
		{"-I", "INPUT", "1", "-p", "tcp", "--sport", "7777", "--dport", sport, "-s", ip, "-j", "DROP"},
	}
	for _, r := range rules {
		args := append([]string{"iptables"}, r...)
		if o, err := exec.Command("sudo", append([]string{"-n"}, args...)...).CombinedOutput(); err != nil {
			h.check(false, fmt.Sprintf("insert DROP rule %v: %v (%s)", r[:2], err, strings.TrimSpace(string(o))))
			return false
		}
	}
	holeState.mu.Lock()
	holeState.pairs = append(holeState.pairs, ip+" "+sport)
	holeState.mu.Unlock()
	return true
}

func holeRemove(h *harness, ip, sport string) bool {
	ok := true
	for _, r := range [][]string{
		{"-D", "OUTPUT", "-p", "tcp", "--dport", "7777", "--sport", sport, "-d", ip, "-j", "DROP"},
		{"-D", "INPUT", "-p", "tcp", "--sport", "7777", "--dport", sport, "-s", ip, "-j", "DROP"},
	} {
		args := append([]string{"iptables"}, r...)
		if o, err := exec.Command("sudo", append([]string{"-n"}, args...)...).CombinedOutput(); err != nil {
			fmt.Printf("  [FAIL] remove DROP rule %v: %v (%s)\n", r[1:2], err, strings.TrimSpace(string(o)))
			h.fail++
			h.failures = append(h.failures, "iptables DROP rules removed")
			ok = false
		}
	}
	holeState.mu.Lock()
	kept := holeState.pairs[:0]
	for _, p := range holeState.pairs {
		if p != ip+" "+sport {
			kept = append(kept, p)
		}
	}
	holeState.pairs = kept
	holeState.mu.Unlock()
	return ok
}

// holeCleanupAll is the deferred safety net.
func holeCleanupAll(h *harness) {
	holeState.mu.Lock()
	pairs := append([]string(nil), holeState.pairs...)
	holeState.pairs = nil
	holeState.mu.Unlock()
	for _, p := range pairs {
		f := strings.Fields(p)
		if len(f) == 2 {
			holeRemove(h, f[0], f[1])
		}
	}
}

// ------------------------------------------------------------ utilities --

// pollUntil polls fn until done or the total budget expires; returns the
// last observation note either way.
func pollUntil(interval, total time.Duration, fn func() (bool, string)) (bool, string) {
	var last string
	deadline := time.Now().Add(total)
	for {
		var done bool
		done, last = fn()
		if done {
			return true, last
		}
		if time.Now().After(deadline) {
			return false, last
		}
		time.Sleep(interval)
	}
}

func numEq(v any, want float64) bool {
	f, ok := toFloat(v)
	return ok && f == want
}

// ------------------------------------------------------------------ run --

// runFull drives the whole suite; every scenario prints its checks into
// the single capture. Returns the process exit code.
func runFull(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	const graceTTL = 15 * time.Second
	const boardSel = "d75884"
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")

	// Deferred bench-restore nets: iptables rules out, daemon never left
	// running. Panics and early returns included.
	defer holeCleanupAll(h)

	fmt.Printf("=== b5 task-12 FULL daemon HIL — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count — any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("board: xuansi-d75884 (bench GPIO1<->GPIO2 wired; WiFi creds in NVS)\n")
	fmt.Printf("expected: fw_build=%s protocol_ver=%d (every hello binds in-log; mismatch aborts)\n", expectFwBuild, expectProtocolVer)
	fmt.Printf("single-board bench: multi-board merge/ambiguity paths are unit-covered (internal/discover/registry_test.go)\n")
	fmt.Printf("daemon: %s serve --sock %s --grace-ttl %s --boards-file %s (REAL subprocess, fresh-built at HEAD)\n",
		daemonBin, sock, graceTTL, boardsCfg)
	fmt.Printf("grace-ttl: %s — a real second-scale window, waited out honestly (S7 adopt + S8 expiry)\n", graceTTL)
	fmt.Printf("sudo/iptables: ")
	if sudoAvailable() {
		fmt.Printf("available (S9 silent hole on the daemon<->board 4-tuple)\n")
	} else {
		fmt.Printf("NOT available (S9 falls back to the local-close variant, honestly labeled)\n")
	}
	fmt.Printf("budget note: the daemon API has no budget knob — driver-direct out-of-band budget.set eval_ms=4000\n")
	fmt.Printf("  in S0 (fw cap 0..4000, VM-global, RAM-only: S11's restore reboot resets it; needed for the 3.5s jobs)\n")
	fmt.Printf("capture: results/2026-11-05-b5-full.log — the ONE file, S1..S12 inside\n")

	// ——— S0 prelude: clean board table + evidence binding (driver-direct,
	// labeled) — the headless CDC open reboots the board (slot freed,
	// nothing claimed); wait for the TCP face before the daemon starts so
	// its initial sweep sees both faces ———
	fmt.Println("\n— S0: prelude (driver-direct, labeled): headless reboot + settle + budget —")
	rb, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "headless reboot dial: "+err.Error())
		return finishFull(h, attempt)
	}
	rb.Close()
	h.note("board rebooted headless (no hello — controller slot free, script table clean)")

	up, lastNet := pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		c, err := net.DialTimeout("tcp", tcpEndpoint, 2*time.Second)
		if err != nil {
			return false, fmt.Sprintf("tcp face not up yet (%v)", err)
		}
		c.Close()
		return true, "tcp face up"
	})
	h.check(up, fmt.Sprintf("board TCP face up after reboot (%s)", lastNet))
	if !up {
		return finishFull(h, attempt)
	}

	bc := dialTCPRetry(h, 15*time.Second, "s0-bind")
	if bc == nil {
		return finishFull(h, attempt)
	}
	binfo, err := bc.Hello(false, "b5full-s0")
	if err != nil || !bindHello(h, "s0", binfo) {
		if err != nil {
			h.check(false, "s0 hello: "+err.Error())
		}
		bc.Close()
		return finishFull(h, attempt)
	}
	res, err := bc.Op(ctx, "budget.set", map[string]any{"eval_ms": 4000})
	h.check(err == nil && numEq(res["eval_ms"], 4000),
		fmt.Sprintf("[driver-direct] out-of-band budget.set eval_ms=4000 (reply=%v err=%v — daemon API has no budget knob)", res, err))
	bc.Close()              // TCP close releases the controller slot (§12) — daemon starts clean
	time.Sleep(time.Second) // mDNS registers right after net-up (t9 grace idiom)

	// ——— S1: serve up ———
	fmt.Println("\n— S1: daemon serve up (subprocess) —")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		h.check(false, "mkdir run dir: "+err.Error())
		return finishFull(h, attempt)
	}
	if err := os.WriteFile(boardsCfg, []byte("# b5-full boards.conf: name=mac6\nbench=d75884\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finishFull(h, attempt)
	}
	h.note("boards.conf fixture: %s (alias bench=d75884)", boardsCfg)

	d := startDaemon(h, daemonBin, sock, boardsCfg, graceTTL)
	if d == nil {
		return finishFull(h, attempt)
	}
	defer func() { // never leave the daemon running after the driver exits
		select {
		case <-d.exited:
			return
		default:
		}
		fmt.Println("  [note] driver exit: SIGKILLing the still-running daemon (bench restore)")
		_ = d.cmd.Process.Signal(syscall.SIGKILL)
		<-d.exited
	}()
	ok, last := pollUntil(300*time.Millisecond, 15*time.Second, func() (bool, string) {
		if c, err := net.Dial("unix", sock); err == nil {
			c.Close()
			return true, "sock dialable"
		}
		return false, "sock not bound yet"
	})
	h.check(ok, fmt.Sprintf("daemon sock bound at %s (%s)", sock, last))
	h.check(d.has("daemon serving"), fmt.Sprintf("daemon reports serving (grace-ttl %s)", graceTTL))
	ok, last = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return d.has("initial discovery sweep: 1 board(s)"), "sweep line"
	})
	h.check(ok, fmt.Sprintf("initial discovery sweep found 1 board (%s)", last))

	mon := dialAPI(h, sock, "monitor")
	if mon == nil {
		return finishFull(h, attempt)
	}

	// ——— S2: discovery dual-source ———
	fmt.Println("\n— S2: discovery — list shows d75884 dual-source, fw-enriched; TXT ver=5 —")
	var fwNote string
	ok, fwNote = pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		r, rerr, err := mon.call(10*time.Second, server.Request{Type: "list_boards"})
		if err != nil || rerr != nil {
			return false, fmt.Sprintf("list_boards err=%v rerr=%v", err, rerr)
		}
		bs, _ := r["boards"].([]any)
		for _, b := range bs {
			bm, _ := b.(map[string]any)
			if bm["mac6"] == boardSel {
				if fw, _ := bm["fw"].(string); fw != "" {
					return true, fmt.Sprintf("fw=%s (startup audit hello-enriched)", fw)
				}
				return false, "board present, fw not yet enriched (audit in flight)"
			}
		}
		return false, "board not in list yet"
	})
	lr, _, _ := mon.call(10*time.Second, server.Request{Type: "list_boards"})
	bs, _ := lr["boards"].([]any)
	h.check(ok, fmt.Sprintf("list_boards shows d75884 with fw populated (%s)", fwNote))
	var theBoard map[string]any
	for _, b := range bs {
		if bm, _ := b.(map[string]any); bm["mac6"] == boardSel {
			theBoard = bm
		}
	}
	if theBoard != nil {
		src, _ := theBoard["sources"].([]any)
		srcStrs := make([]string, len(src))
		for i, s := range src {
			srcStrs[i], _ = s.(string)
		}
		h.check(len(srcStrs) == 2 && srcStrs[0] == "usb" && srcStrs[1] == "mdns",
			fmt.Sprintf("dual-source [usb mdns] (got %v)", srcStrs))
		host, _ := theBoard["host"].(string)
		port, _ := theBoard["port"].(float64)
		fw, _ := theBoard["fw"].(string)
		usb, _ := theBoard["usb_path"].(string)
		h.check(host == "xuansi-d75884.local" && port == 7777,
			fmt.Sprintf("mdns face host=%s port=%v", host, port))
		h.check(usb == usbByID, fmt.Sprintf("usb_path is the by-id path (%s)", usb))
		h.check(fw == expectFwBuild, fmt.Sprintf("fw=%s (hello-enriched, binds the expected build)", fw))
		alias, _ := theBoard["alias"].(string)
		h.check(alias == "bench", fmt.Sprintf("alias from boards.conf = %q", alias))
		mdnsErr, _ := lr["mdns_error"].(string)
		h.check(mdnsErr == "", fmt.Sprintf("mdns face healthy (mdns_error=%q)", mdnsErr))
	} else {
		h.check(false, "d75884 entry found in list_boards")
	}
	h.check(d.has("startup audit board d75884"), "startup orphan audit ran (report-only)")
	h.check(d.has("cold=true scripts=[] pins=0"), "audit reported the clean board (cold=true scripts=[] pins=0, nothing killed)")

	// TXT ver=4 — the daemon's list view surfaces fw (hello), not TXT; the
	// TXT binding is proven here by one driver-side registry browse.
	mb, merr := discover.DiscoverMDNS(ctx)
	txtOK := false
	var txtNote string
	for _, b := range mb {
		if b.MAC6 == boardSel {
			txtOK = b.TXT["ver"] == "5" && b.TXT["api"] == "1"
			txtNote = fmt.Sprintf("TXT=%v", b.TXT)
		}
	}
	h.check(merr == nil && txtOK,
		fmt.Sprintf("[driver-side browse] mDNS TXT ver=5 api=1 (%s, err=%v) — same single source as hello protocol_ver (§14)", txtNote, merr))

	// ——— S3: lease + structured eval ———
	fmt.Println("\n— S3: lease (holder=h1, via alias) + structured eval —")
	h1 := dialAPI(h, sock, "h1")
	if h1 == nil {
		return finishFull(h, attempt)
	}
	ler, lrerr, _ := h1.call(20*time.Second, server.Request{Type: "lease", Board: "bench", Holder: "h1"})
	h.check(lrerr == nil && ler != nil && ler["board"] == boardSel && ler["holder"] == "h1",
		fmt.Sprintf("lease via ALIAS %q resolves to board=%v holder=%v session=%v (rerr=%v)", "bench", gAny(ler, "board"), gAny(ler, "holder"), gAny(ler, "session"), lrerr))

	er, rerr, _ := h1.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: "return 6*7"})
	h.check(rerr == nil && er != nil && numEq(er["value"], 42),
		fmt.Sprintf("eval return 6*7 -> value=%v out=%q (rerr=%v)", gAny(er, "value"), gAny(er, "out"), rerr))

	argsRT := map[string]any{"s": "xuansi", "n": float64(7), "b": true, "arr": []any{float64(1), float64(2), float64(3)}}
	er, rerr, _ = h1.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: "return ARGS", Args: argsRT})
	h.check(rerr == nil && reflect.DeepEqual(er["value"], argsRT),
		fmt.Sprintf("eval return ARGS round-trip deep-equal (value=%v, rerr=%v)", gAny(er, "value"), rerr))

	er, rerr, _ = h1.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1",
		Code: `local t=42 for i=1,7 do t={k=t} end return {t=t}`})
	want8 := map[string]any{"t": map[string]any{"k": map[string]any{"k": map[string]any{"k": map[string]any{"k": map[string]any{"k": map[string]any{"k": map[string]any{"k": float64(42)}}}}}}}}
	h.check(rerr == nil && reflect.DeepEqual(er["value"], want8),
		fmt.Sprintf("eval depth-8 table via the value bridge (rerr=%v)", rerr))

	// eval from a THIRD holder while h1's lease is live -> held (ops-level
	// exclusivity; not_held is the no-lease-at-all case, proven in S8)
	nl := dialAPI(h, sock, "stranger")
	if nl != nil {
		_, nrerr, _ := nl.call(10*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "stranger", Code: `return "x"`})
		h.check(nrerr != nil && nrerr.Class == "held" && nrerr.Holder == "h1",
			fmt.Sprintf("eval from a third holder while leased: refused held by h1 (class=%s holder=%s)", classOf(nrerr), holdOf(nrerr)))
		nl.close()
	}

	// ——— S4: fs 16KB ———
	fmt.Println("\n— S4: fs 16KB write/read sha256-exact + daemon-side cap —")
	buf := make([]byte, 16384)
	if _, err := rand.Read(buf); err != nil {
		h.check(false, "rand: "+err.Error())
		return finishFull(h, attempt)
	}
	shaW := sha256.Sum256(buf)
	fpath := "/lua/b5full.tmp"
	_, rerr, _ = h1.call(30*time.Second, server.Request{Type: "fs_write", Board: boardSel, Holder: "h1", Path: fpath, Data: buf})
	h.check(rerr == nil, fmt.Sprintf("fs_write 16384B %s (rerr=%v)", fpath, rerr))
	fr, rerr, _ := h1.call(30*time.Second, server.Request{Type: "fs_read", Board: boardSel, Path: fpath})
	var got []byte
	if fr != nil {
		if b64, _ := fr["data"].(string); b64 != "" {
			got, _ = base64.StdEncoding.DecodeString(b64)
		}
	}
	shaR := sha256.Sum256(got)
	h.check(rerr == nil && len(got) == 16384 && shaW == shaR,
		fmt.Sprintf("fs_read byte-exact (%dB, rerr=%v)", len(got), rerr))
	h.note("sha256 write=%x", shaW)
	h.note("sha256 read =%x", shaR)
	_, rerr, _ = h1.call(10*time.Second, server.Request{Type: "fs_write", Board: boardSel, Holder: "h1", Path: fpath, Data: make([]byte, 16385)})
	h.check(rerr != nil && rerr.Class == "api" && strings.Contains(rerr.Msg, "16384"),
		fmt.Sprintf("fs_write 16385B refused DAEMON-SIDE at the cap (class=%s msg=%.80s)", classOf(rerr), msgOf(rerr)))
	h.note("the 16KB temp file is kept as the fs_read health probe for S9/S11 (overwritten every run)")

	// ——— S5: submit long job (async, no stall, job event with value) ———
	fmt.Println("\n— S5: submit 3.5s job — ack immediate, API live during, event carries value —")
	_, rerr, _ = h1.call(5*time.Second, server.Request{Type: "watch_events", Board: boardSel})
	h.check(rerr == nil, fmt.Sprintf("h1 watch_events (job results fan to watchers; rerr=%v)", rerr))
	t0 := time.Now()
	sr, rerr, _ := h1.call(15*time.Second, server.Request{Type: "submit", Board: boardSel, Holder: "h1",
		Tag: "b5job", Code: `local t=os.clock() while os.clock()-t < 3.5 do end return "b5job-value"`})
	ackLat := time.Since(t0)
	h.check(rerr == nil && sr != nil && sr["tag"] == "b5job" && sr["queued"] == true && ackLat < time.Second,
		fmt.Sprintf("submit ack immediate: {%v queued=%v} in %v (rerr=%v)", gAny(sr, "tag"), gAny(sr, "queued"), ackLat.Round(time.Millisecond), rerr))

	time.Sleep(400 * time.Millisecond) // the job is running on the VM now
	pt := time.Now()
	mr, _, _ := mon.call(10*time.Second, server.Request{Type: "list_boards"})
	monLat := time.Since(pt)
	h.check(mr != nil && monLat < time.Second,
		fmt.Sprintf("list_boards DURING the job answered in %v (daemon loop never stalls)", monLat.Round(time.Millisecond)))
	ft := time.Now()
	fr2, rerr2, _ := mon.call(30*time.Second, server.Request{Type: "fs_read", Board: boardSel, Path: fpath})
	fsLat := time.Since(ft)
	h.check(fr2 != nil && rerr2 == nil,
		fmt.Sprintf("fs_read (board conn-task ro op) during the job answered in %v (rerr=%v)", fsLat.Round(time.Millisecond), rerr2))
	h.note("sync eval during a VM job queues in VM order by design (§6) — the async submit IS the no-stall remedy (protocol diff #7)")

	ev, ok := h1.waitEvent("job", 10*time.Second)
	jobOK := ok && ev.Event.Data["tag"] == "b5job" && ev.Event.Data["ok"] == true && ev.Event.Data["value"] == "b5job-value"
	h.check(jobOK,
		fmt.Sprintf("job event delivered: tag=%v ok=%v value=%v (waited %v)",
			gAny(ev.Event.Data, "tag"), gAny(ev.Event.Data, "ok"), gAny(ev.Event.Data, "value"), time.Since(t0).Round(time.Millisecond)))

	// ——— S6: gpio events through a second (watching) connection ———
	fmt.Println("\n— S6: gpio events — claim via eval, watcher on a second conn, edges through the daemon —")
	er, rerr, _ = h1.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1",
		Code: `bench.gpio.mode(1,"out") bench.gpio.mode(2,"in") return "claimed"`})
	h.check(rerr == nil && er != nil && er["value"] == "claimed",
		fmt.Sprintf("claim GPIO1 out / GPIO2 in via eval (rerr=%v)", rerr))

	watch := dialAPI(h, sock, "gpio-watcher")
	if watch == nil {
		return finishFull(h, attempt)
	}
	_, rerr, _ = watch.call(20*time.Second, server.Request{Type: "watch_events", Board: boardSel, Source: "gpio.2"})
	h.check(rerr == nil, fmt.Sprintf("watcher conn watch_events source=gpio.2 (daemon subscribes board-side; rerr=%v)", rerr))

	er, rerr, _ = h1.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1",
		Code: `bench.gpio.write(1,1) bench.gpio.write(1,0) return "drove"`})
	h.check(rerr == nil && er != nil && er["value"] == "drove",
		fmt.Sprintf("drive GPIO1 edges via eval (rerr=%v)", rerr))

	var edges []string
	edgeDeadline := time.After(3 * time.Second)
edgeLoop:
	for len(edges) < 2 {
		select {
		case e := <-watch.events:
			if e.Event.Source == "gpio.2" {
				edge, _ := e.Event.Data["edge"].(string)
				edges = append(edges, edge)
				h.note("watcher saw gpio.2 edge=%s level=%v", edge, gAny(e.Event.Data, "level"))
			}
		case <-edgeDeadline:
			break edgeLoop
		}
	}
	h.check(len(edges) >= 2 && edges[0] == "rising" && edges[1] == "falling",
		fmt.Sprintf("gpio.2 events pushed through the daemon to the WATCHING conn: %v (want rising then falling)", edges))
	watch.close()

	// ——— S7: grace ADOPT round (long-experiment continuity focus) ———
	fmt.Println("\n— S7: grace ADOPT — holder drop mid-job, SAME holder back within TTL —")
	const counterCode = "_G.b5n = 0; bench.timer_ms(100, function() _G.b5n = _G.b5n + 1 end); " +
		"while true do local f = coroutine.yield(); if f then f() end end"
	_, rerr, _ = h1.call(20*time.Second, server.Request{Type: "script_start", Board: boardSel, Holder: "h1", Name: "b5keep", Code: counterCode})
	h.check(rerr == nil, fmt.Sprintf("script_start b5keep (resident counter, lease session ns; rerr=%v)", rerr))
	time.Sleep(800 * time.Millisecond)
	er, _, _ = h1.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: "return _G.b5n"})
	nDrop := er["value"]

	// in-flight job ACROSS the disconnect (focus item)
	tj := time.Now()
	_, rerr, _ = h1.call(15*time.Second, server.Request{Type: "submit", Board: boardSel, Holder: "h1",
		Tag: "b5adj", Code: `local t=os.clock() while os.clock()-t < 3.5 do end return "b5adj-value"`})
	h.check(rerr == nil, fmt.Sprintf("submit b5adj (in flight across the drop; rerr=%v)", rerr))
	time.Sleep(200 * time.Millisecond)
	h1.close() // agent gone: unix-sock EOF -> OnAgentGone(h1) -> grace
	h.note("h1 conn CLOSED mid-job (b5adj %v in, b5keep counting) — grace window open", time.Since(tj).Round(time.Millisecond))
	time.Sleep(500 * time.Millisecond)

	h2 := dialAPI(h, sock, "h2")
	if h2 == nil {
		return finishFull(h, attempt)
	}
	_, held, _ := h2.call(10*time.Second, server.Request{Type: "lease", Board: boardSel, Holder: "h2"})
	h.check(held != nil && held.Class == "held" && held.Holder == "h1" && held.GraceExpiry != "",
		fmt.Sprintf("third-party lease during grace refused: class=%s holder=%s grace_expiry=%s", classOf(held), held.Holder, held.GraceExpiry))
	h2.close()

	h1r := dialAPI(h, sock, "h1-reconnect")
	if h1r == nil {
		return finishFull(h, attempt)
	}
	// watch FIRST: watch_events needs no lease, and the daemon serves one
	// conn's requests strictly in order — issuing it after the adopt eval
	// let the job's completion event beat the watch to the broadcast
	// (run-1 lesson, honestly recorded; with the watch in place the event
	// queues on this conn's channel and is claimed below).
	_, rerr, _ = h1r.call(5*time.Second, server.Request{Type: "watch_events", Board: boardSel})
	h.check(rerr == nil, fmt.Sprintf("reconnected conn watch_events BEFORE the adopt (rerr=%v)", rerr))
	er, rerr, _ = h1r.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: `return "back"`})
	h.check(rerr == nil && er != nil && er["value"] == "back",
		fmt.Sprintf("SAME holder reconnects within TTL: eval adopted the lease live (rerr=%v)", rerr))
	er, _, _ = h1r.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: "return _G.b5n"})
	h.check(numGt(er["value"], nDrop),
		fmt.Sprintf("b5keep RAN THROUGH grace and the adopt (counter %v -> %v — no reset)", nDrop, er["value"]))
	h.check(d.has("holder h1 adopted board d75884 back live"), "daemon log: holder adopted back live (adopt, not re-acquire)")
	h.check(d.count("reason=expired") == 0, "grace adopt touched nothing (no expiry teardown so far)")

	ev, ok = h1r.waitEvent("job", 8*time.Second)
	h.check(ok && ev.Event.Data["tag"] == "b5adj" && ev.Event.Data["value"] == "b5adj-value",
		fmt.Sprintf("in-flight job's event delivered to the RECONNECTED watcher: tag=%v value=%v (job survived the agent disconnect; the daemon<->board link never dropped, so no orphan)",
			gAny(ev.Event.Data, "tag"), gAny(ev.Event.Data, "value")))
	st, _, _ := mon.call(10*time.Second, server.Request{Type: "board_status", Board: boardSel})
	leaseMap, _ := st["lease"].(map[string]any)
	scripts := scriptsOf(st)
	h.check(leaseMap != nil && leaseMap["holder"] == "h1" && leaseMap["grace_deadline"] == nil,
		fmt.Sprintf("board_status: lease live again holder=%v grace_deadline=%v", gAny(leaseMap, "holder"), gAny(leaseMap, "grace_deadline")))
	h.check(len(scripts) == 1 && scripts[0] == "b5keep", fmt.Sprintf("board_status scripts=%v", scripts))

	// ——— S8: grace EXPIRY round ———
	fmt.Println("\n— S8: grace EXPIRY — drop again, wait out the 15s TTL, teardown exactly once —")
	er, rerr, _ = h1r.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1",
		Code: `bench.gpio.mode(1,"out") return "claimed"`})
	h.check(rerr == nil && er != nil && er["value"] == "claimed",
		fmt.Sprintf("holder eval-claims GPIO1 (pin state for the safe-state proof; rerr=%v)", rerr))
	h1r.close()
	h.note("h1 conn CLOSED again — grace window %s starts", graceTTL)

	time.Sleep(2 * time.Second)
	st, _, _ = mon.call(10*time.Second, server.Request{Type: "board_status", Board: boardSel})
	leaseMap, _ = st["lease"].(map[string]any)
	scripts = scriptsOf(st)
	h.check(leaseMap != nil && leaseMap["grace_deadline"] != nil,
		fmt.Sprintf("during grace: lease kept holder=%v grace_deadline=%v (board state KEPT)", gAny(leaseMap, "holder"), gAny(leaseMap, "grace_deadline")))
	h.check(len(scripts) == 1 && scripts[0] == "b5keep", fmt.Sprintf("during grace: script STILL listed %v", scripts))

	ttl0 := time.Now()
	ok, last = pollUntil(time.Second, graceTTL+25*time.Second, func() (bool, string) {
		st, _, _ := mon.call(10*time.Second, server.Request{Type: "board_status", Board: boardSel})
		lm, _ := st["lease"].(map[string]any)
		if lm == nil {
			return true, "lease gone from board_status"
		}
		return false, fmt.Sprintf("lease still present (holder=%v grace_deadline=%v)", gAny(lm, "holder"), gAny(lm, "grace_deadline"))
	})
	h.check(ok, fmt.Sprintf("grace TTL expired and the lease is gone (waited %v; %s)", time.Since(ttl0).Round(time.Millisecond), last))
	h.check(d.count("reason=expired") == 1,
		fmt.Sprintf("expiry teardown ran EXACTLY ONCE in the whole capture (count=%d)", d.count("reason=expired")))
	scripts = nil
	st, _, _ = mon.call(10*time.Second, server.Request{Type: "board_status", Board: boardSel})
	scripts = scriptsOf(st)
	h.check(len(scripts) == 0, fmt.Sprintf("post-expiry script.list empty (got %v — b5keep torn down)", scripts))

	h1x := dialAPI(h, sock, "h1-post-expiry")
	if h1x != nil {
		_, nrerr, _ := h1x.call(10*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: `return "late"`})
		h.check(nrerr != nil && nrerr.Class == "not_held",
			fmt.Sprintf("post-expiry op as the old holder: not_held (class=%s msg=%.60s)", classOf(nrerr), msgOf(nrerr)))
		h1x.close()
	}

	// pins safe-state: the write-reject probe (diag.pins is not daemon-exposed)
	h2b := dialAPI(h, sock, "h2-safe-state")
	if h2b != nil {
		_, rerr, _ := h2b.call(20*time.Second, server.Request{Type: "lease", Board: boardSel, Holder: "h2b"})
		_, werr, _ := h2b.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h2b", Code: `return bench.gpio.write(1,0)`})
		h.check(werr != nil && werr.Class == "rt" && strings.Contains(werr.Msg, "not configured as output"),
			fmt.Sprintf("post-expiry pins SAFE: GPIO1 write rejected (class=%s msg=%.70s)", classOf(werr), msgOf(werr)))
		h.note("diag.pins is an HIL-only op not exposed by the daemon API — safe-state proven by the write-reject probe (T10 house style)")
		_, rerr, _ = h2b.call(20*time.Second, server.Request{Type: "release", Board: boardSel, Holder: "h2b"})
		h.check(rerr == nil, fmt.Sprintf("h2b release (bench back to free; rerr=%v)", rerr))
		h2b.close()
	}

	// ——— S9: link-loss resync (silent hole) + in-flight job fate ———
	fmt.Println("\n— S9: link-loss resync — silent 4-tuple hole, no-reboot recovery, in-flight job fate —")
	h1n := dialAPI(h, sock, "h1-resync")
	if h1n == nil {
		return finishFull(h, attempt)
	}
	_, rerr, _ = h1n.call(20*time.Second, server.Request{Type: "lease", Board: boardSel, Holder: "h1"})
	h.check(rerr == nil, fmt.Sprintf("fresh lease holder=h1 after the expiry round (rerr=%v)", rerr))
	_, rerr, _ = h1n.call(20*time.Second, server.Request{Type: "script_start", Board: boardSel, Holder: "h1", Name: "b5resync", Code: counterCode})
	h.check(rerr == nil, fmt.Sprintf("script_start b5resync BEFORE the hole (rerr=%v)", rerr))
	_, rerr, _ = h1n.call(5*time.Second, server.Request{Type: "watch_events", Board: boardSel})
	h.check(rerr == nil, fmt.Sprintf("h1n watch_events before the hole (any late job delivery would land here; rerr=%v)", rerr))
	time.Sleep(800 * time.Millisecond)
	er, _, _ = h1n.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: "return _G.b5n"})
	nBefore := er["value"]
	h.note("counter before the hole: %v (script running)", nBefore)

	// the job leaves a side-effect global: whatever happens to its result
	// frame, the BODY running to completion stays observable board-side
	_, rerr, _ = h1n.call(15*time.Second, server.Request{Type: "submit", Board: boardSel, Holder: "h1",
		Tag: "b5orphan", Code: `_G.b5orphan_done = true; local t=os.clock() while os.clock()-t < 3.5 do end return "b5orphan-value"`})
	h.check(rerr == nil, fmt.Sprintf("submit b5orphan — in flight when the link holes (rerr=%v)", rerr))
	time.Sleep(300 * time.Millisecond)

	if !sudoAvailable() {
		h.check(false, "S9 needs sudo iptables for the silent hole (no-sudo local-close fallback NOT run — recorded honestly)")
		h1n.close()
		return finishFull(h, attempt) // the scenario is the heart of AC6; no honest substitute here
	}
	ip := boardIP()
	sport, ok2 := sportOfConn(ip)
	h.check(ip != "" && ok2, fmt.Sprintf("resolved daemon<->board 4-tuple %s:%s <-> %s:7777 (the daemon's warm controller conn — the only ESTAB)", sport, ip, ip))
	if !ok2 {
		h1n.close()
		return finishFull(h, attempt)
	}
	if !holeInsert(h, ip, sport) {
		h1n.close()
		return finishFull(h, attempt)
	}
	h.note("silent hole open (BOTH directions dropped) — board blind, host keepalive must verdict")

	// TRAFFIC-FREE verdict window: any board-touching request the driver
	// sends would put an outbound frame on the daemon's link and reset the
	// kernel keepalive idle timer, stretching the verdict to the board's
	// own ~16s reap (run-1 lesson, honestly recorded). 8s of silence
	// covers the 3s idle + 3x1s probes with margin; only then may we ask
	// board_status (which writes nothing once the controller is dead).
	th := time.Now()
	time.Sleep(8 * time.Second)
	ok, last = pollUntil(5*time.Second, 30*time.Second, func() (bool, string) {
		st, _, _ := mon.call(12*time.Second, server.Request{Type: "board_status", Board: boardSel})
		conn, _ := st["connected"].(bool)
		if !conn {
			return true, "connected=false"
		}
		return false, "connected=true still — sleeping 5s MORE (traffic-free) before re-checking"
	})
	verdict := time.Since(th).Round(time.Millisecond)
	h.check(ok, fmt.Sprintf("daemon's board link declared dead after a traffic-free window (observed at %v; host keepalive 3s idle + 3x1s probes ~= 6s — the EOF-miss dead-link shape, <=6s class; %s)", verdict, last))
	h.note("EOF-miss intermittent -> dead-link <=6s (focus note): the silent hole IS the keepalive-verdict path — traffic-free window %v, verdict fires at ~6s into it (T8 measured 6.1s with local Dead polling)", verdict)

	holeRemove(h, ip, sport)
	h.note("rules out — the daemon redials on the next board op (boardOp path); its supervisor re-hellos through the board-side keepalive self-release (T10 fix1)")

	tr := time.Now()
	var recData []byte
	ok, last = pollUntil(2*time.Second, 120*time.Second, func() (bool, string) {
		fr, rerr, err := mon.call(70*time.Second, server.Request{Type: "fs_read", Board: boardSel, Path: fpath})
		if err != nil {
			return false, fmt.Sprintf("fs_read client err=%v", err)
		}
		if rerr != nil {
			return false, fmt.Sprintf("fs_read rerr class=%s", rerr.Class)
		}
		if b64, _ := fr["data"].(string); b64 != "" {
			recData, _ = base64.StdEncoding.DecodeString(b64)
		}
		return len(recData) == 16384, fmt.Sprintf("fs_read ok (%dB)", len(recData))
	})
	h.check(ok, fmt.Sprintf("board path RECOVERED over TCP after the hole (fs_read probe, %v; %s)", time.Since(tr).Round(time.Millisecond), last))
	h.check(sha256.Sum256(recData) == shaW, "recovered fs_read still byte-exact (same file across the link loss)")
	// the recovery claim: controller re-taken over TCP, NO USB, NO reboot.
	// (Run-2 note: with the traffic-free verdict the supervisor's Connect
	// finds the corpse already dead, so no op "hits a dead link" mid-flight
	// — the redial happens up-front in Connect; both shapes are honest.)
	retook := d.has("controller taken on re-hello") || d.count("controller warm connection up") >= 2
	h.check(retook && !d.has("USB fallback"),
		"daemon re-took the controller over TCP after the hole (re-hello/warm-connection lines present, NO USB fallback dial, NO reboot)")
	if d.has("controller taken on re-hello") {
		h.note("arbitration shape: forced observer -> controller taken on re-hello (the daemon rode out the board-side keepalive self-release — T10 fix1 shape)")
	} else {
		h.note("arbitration shape: slot already self-released at the first redial hello (direct controller) — the board reaped the dead conn during the hole; same no-USB no-reboot recovery")
	}

	st, _, _ = mon.call(10*time.Second, server.Request{Type: "board_status", Board: boardSel})
	scripts = scriptsOf(st)
	connNow, _ := st["connected"].(bool)
	h.check(connNow && len(scripts) == 1 && scripts[0] == "b5resync",
		fmt.Sprintf("post-recovery board_status: connected=%v scripts=%v (b5resync SURVIVED the link loss)", connNow, scripts))
	er, _, _ = h1n.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: "return _G.b5n"})
	h.check(numGt(er["value"], nBefore),
		fmt.Sprintf("script RAN THROUGH the hole and the recovery (counter %v -> %v — no reboot happened)", nBefore, er["value"]))
	h.note("the ResyncReport diff shape is a supervisor-internal (T10 driver-level); through the API the survival evidence is scripts+counter + the daemon's own arbitration log")

	// the in-flight job's fate (focus item, recorded honestly):
	// the body ran to completion on the VM board-side; its result frame
	// was queued to the daemon's conn, which the HOST keepalive abort
	// tore at ~6s — the frame died with the socket (the fw counted it
	// deliverable at queue time, so job_orphans stays 0; the §8 orphan
	// counter fires only when the submitter is gone BEFORE completion —
	// a >16s job is unreachable through the 4s compute-job cap; the
	// fw-layer orphan lifecycle is b4-T7-proven with direct conns).
	er, _, _ = h1n.call(20*time.Second, server.Request{Type: "eval", Board: boardSel, Holder: "h1", Code: "return _G.b5orphan_done"})
	h.check(er != nil && er["value"] == true,
		fmt.Sprintf("in-flight job BODY ran to completion through the link loss (_G.b5orphan_done=%v)", gAny(er, "value")))
	if _, lateOK := h1n.waitEvent("job", 5*time.Second); lateOK {
		h.note("b5orphan result event arrived LATE on recovery (board retransmit won the race) — delivered, not orphaned")
	} else {
		h.note("no b5orphan result event within 5s of recovery — the frame died with the keepalive-aborted socket (expected shape; body ran, result lost)")
	}

	oc := dialTCPRetry(h, 10*time.Second, "orphan-diag")
	if oc != nil {
		oi, oerr := oc.Hello(true, "b5full-orphan-diag")
		if oerr == nil && bindHello(h, "orphan-diag", oi) {
			h.check(oi.Role == "observer",
				fmt.Sprintf("orphan-diag hello role=%s (observer — proof the DAEMON re-took the controller slot)", oi.Role))
			ds, derr := oc.Op(ctx, "diag.stats", nil)
			orphans, _ := ds["job_orphans"].(int64)
			h.check(derr == nil && orphans == 0,
				fmt.Sprintf("[driver-direct] diag.stats job_orphans=%d (err=%v) — the submitter conn was still established board-side at completion, so the fw queued (not orphaned) the frame; its LOSS happened host-side at the keepalive abort (honestly recorded above)", orphans, derr))
			lg, lerr := oc.Op(ctx, "log.read", nil)
			lines := fmt.Sprint(lg["lines"])
			if strings.Contains(lines, "orphaned") {
				h.note("[driver-direct] log.read mentions an orphan line: %v", strings.Contains(lines, "b5orphan"))
			} else {
				h.note("[driver-direct] log.read: no orphan line (consistent with job_orphans=0; the fw-layer orphan lifecycle is proven by b4-T7 with direct conns)")
			}
			_ = lerr
		} else if oerr != nil {
			h.check(false, "orphan-diag hello: "+oerr.Error())
		}
		oc.Close()
	}

	// ——— S10: release ———
	fmt.Println("\n— S10: release — polite release tears the board side down —")
	_, rerr, _ = h1n.call(30*time.Second, server.Request{Type: "release", Board: boardSel, Holder: "h1"})
	h.check(rerr == nil, fmt.Sprintf("release holder=h1 (rerr=%v)", rerr))
	h.check(d.count("reason=released") >= 1, fmt.Sprintf("daemon log: teardown reason=released (count=%d)", d.count("reason=released")))
	st, _, _ = mon.call(10*time.Second, server.Request{Type: "board_status", Board: boardSel})
	scripts = scriptsOf(st)
	leaseMap, _ = st["lease"].(map[string]any)
	h.check(len(scripts) == 0 && leaseMap == nil,
		fmt.Sprintf("post-release: scripts=%v lease=%v (board torn down, free)", scripts, leaseMap))
	h1n.close()

	// ——— S11: mDNS-degraded / USB-only (LAST board-touching phase) ———
	fmt.Println("\n— S11: mDNS-degraded — wifi.off (driver-direct USB, REBOOT labeled), usb-only list, restore —")
	fmt.Println("  [label] this dedicated phase runs LAST: the USB open REBOOTS the board (kills nothing — everything is released)")
	uc, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "degraded-phase USB dial: "+err.Error())
		return finishFull(h, attempt)
	}
	uinfo, err := uc.Hello(false, "b5full-degraded")
	if err != nil || !bindHello(h, "degraded", uinfo) {
		if err != nil {
			h.check(false, "degraded-phase hello: "+err.Error())
		}
		uc.Close()
		return finishFull(h, attempt)
	}
	h.check(settleNetUp(h, uc, "degraded"), "board wifi up before wifi.off")
	wres, werr := uc.Op(ctx, "wifi.off", nil)
	h.check(werr == nil, fmt.Sprintf("[driver-direct] wifi.off from the USB controller (reply=%v err=%v)", wres, werr))

	_, dtcpErr := net.DialTimeout("tcp", tcpEndpoint, 3*time.Second)
	h.check(dtcpErr != nil, fmt.Sprintf("TCP face gone after wifi.off (dial err=%v — expected)", dtcpErr))

	ok, last = pollUntil(500*time.Millisecond, 12*time.Second, func() (bool, string) {
		st, _, _ := mon.call(10*time.Second, server.Request{Type: "board_status", Board: boardSel})
		conn, _ := st["connected"].(bool)
		return !conn, fmt.Sprintf("connected=%v", conn)
	})
	h.check(ok, fmt.Sprintf("daemon's board link dead after net-down (%s — EOF fast path or keepalive, §13 best-effort)", last))

	var usbOnly map[string]any
	ok, last = pollUntil(3*time.Second, 75*time.Second, func() (bool, string) {
		r, _, _ := mon.call(10*time.Second, server.Request{Type: "list_boards"})
		bs, _ := r["boards"].([]any)
		for _, b := range bs {
			if bm, _ := b.(map[string]any); bm["mac6"] == boardSel {
				src, _ := bm["sources"].([]any)
				if len(src) == 1 && src[0] == "usb" {
					usbOnly = bm
					return true, "usb-only"
				}
				return false, fmt.Sprintf("sources=%v (waiting for the next refresh sweep)", src)
			}
		}
		return false, "board entry gone?"
	})
	h.check(ok, fmt.Sprintf("list degraded to usb-only after wifi.off (%s — mdns service withdrawn at net-down, §14)", last))
	if usbOnly != nil {
		h.check(usbOnly["host"] == "xuansi-d75884.local", fmt.Sprintf("last-known mdns face kept host=%v (honest stale-marking, no hard delete)", usbOnly["host"]))
		h.check(usbOnly["usb_path"] == usbByID, fmt.Sprintf("usb source still live (%v)", usbOnly["usb_path"]))
	}
	st, _, _ = mon.call(10*time.Second, server.Request{Type: "board_status", Board: boardSel})
	_, hasLease := st["lease"].(map[string]any)
	h.check(st != nil && !hasLease, "board_status still resolves + answers over the usb-only entry (no dial, lease null)")

	ut := time.Now()
	_, urerr, uerr := mon.call(75*time.Second, server.Request{Type: "fs_read", Board: boardSel, Path: fpath})
	h.check(uerr == nil && urerr != nil && urerr.Class == "unreachable",
		fmt.Sprintf("board op through the daemon with TCP down: class=%s after %v (the daemon never dials USB — owner ruling: USB fallback is the caller's explicit last resort; recorded honestly)",
			classOf(urerr), time.Since(ut).Round(time.Millisecond)))

	uv, _, uverr := uc.Eval(ctx, `return "usb-ok"`, nil)
	h.check(uverr == nil && uv == "usb-ok",
		fmt.Sprintf("[driver-direct] USB transport itself ALIVE with wifi down (eval value=%v err=%v)", uv, uverr))

	h.note("restore: close + reopen CDC (board reboots, killing nothing) -> NVS auto-connect -> mDNS back")
	uc.Close()
	rc, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "restore USB dial: "+err.Error())
		return finishFull(h, attempt)
	}
	rinfo, err := rc.Hello(false, "b5full-restore")
	if err != nil || !bindHello(h, "restore", rinfo) {
		if err != nil {
			h.check(false, "restore hello: "+err.Error())
		}
		rc.Close()
		return finishFull(h, attempt)
	}
	upRestore := settleNetUp(h, rc, "restore")
	if !upRestore {
		// bounded-retry parked in fail (§14): explicit wifi.setup from the
		// USB controller (t9 pattern; creds git-ignored, pass never logged)
		ssid, pass, okc := loadCreds(defaultCreds)
		if !okc {
			h.check(false, "STA parked in fail and creds unavailable at "+defaultCreds)
			rc.Close()
			return finishFull(h, attempt)
		}
		h.note("bounded retry parked in fail — explicit wifi.setup restore (creds %s, pass never logged)", defaultCreds)
		_, serr2 := rc.Op(ctx, "wifi.setup", map[string]any{"ssid": ssid, "pass": pass})
		h.check(serr2 == nil, fmt.Sprintf("wifi.setup restore (err=%v)", serr2))
		upRestore = settleNetUp(h, rc, "restore after wifi.setup")
	}
	h.check(upRestore, "wifi auto-recovered after the restore reboot (NVS creds; explicit setup only if parked)")
	// the USB controller must YIELD before the port closes: CDC has no
	// host-disconnect signal (§12) — the board would hold the controller
	// slot until its keepalive/reboot and the daemon's TCP hellos would
	// wait out the whole arbitration window (run-1 lesson: slot_held).
	iy, yerr := rc.Hello(true, "b5full-restore")
	yieldOK := yerr == nil && iy != nil && iy.Role == "observer"
	h.check(yieldOK, fmt.Sprintf("restore USB controller yields the slot (re-hello observer, §12; role=%s err=%v)", helloRole(iy), yerr))
	rc.Close()
	h.note("the restore reboot also reset the out-of-band eval_ms budget to fw defaults (RAM-only state)")
	time.Sleep(time.Second) // mDNS registers right after net-up

	var dual map[string]any
	ok, last = pollUntil(3*time.Second, 75*time.Second, func() (bool, string) {
		r, _, _ := mon.call(10*time.Second, server.Request{Type: "list_boards"})
		bs, _ := r["boards"].([]any)
		for _, b := range bs {
			if bm, _ := b.(map[string]any); bm["mac6"] == boardSel {
				src, _ := bm["sources"].([]any)
				if len(src) == 2 && src[0] == "usb" && src[1] == "mdns" {
					dual = bm
					return true, "dual-source"
				}
				return false, fmt.Sprintf("sources=%v", src)
			}
		}
		return false, "entry gone?"
	})
	h.check(ok && dual != nil && dual["fw"] == expectFwBuild,
		fmt.Sprintf("dual-source restored after wifi recovery (%s; fw=%v)", last, gAny(dual, "fw")))

	rt2 := time.Now()
	var heal []byte
	healOK, lastH := pollUntil(2*time.Second, 75*time.Second, func() (bool, string) {
		fr, rerr, err := mon.call(70*time.Second, server.Request{Type: "fs_read", Board: boardSel, Path: fpath})
		if err != nil {
			return false, fmt.Sprintf("fs_read err=%v", err)
		}
		if rerr != nil {
			return false, fmt.Sprintf("class=%s", rerr.Class)
		}
		if b64, _ := fr["data"].(string); b64 != "" {
			heal, _ = base64.StdEncoding.DecodeString(b64)
		}
		return len(heal) == 16384, fmt.Sprintf("%dB", len(heal))
	})
	h.check(healOK && sha256.Sum256(heal) == shaW,
		fmt.Sprintf("daemon RE-HEALED the board path after wifi restore (fs_read byte-exact, %v; %s)", time.Since(rt2).Round(time.Millisecond), lastH))

	// ——— S12: clean shutdown ———
	fmt.Println("\n— S12: clean shutdown — SIGINT, exit 0, sock removed —")
	fmt.Println("  [note] order: the dedicated degraded phase (S11) ran before shutdown so its reboot poisoned nothing (plan instruction)")
	mon.close()
	ok = d.sigintAndWait(h, 20*time.Second)
	if !ok {
		return finishFull(h, attempt)
	}
	h.check(d.code == 0 && d.waitEr == nil, fmt.Sprintf("daemon exited clean (code=%d waitErr=%v)", d.code, d.waitEr))
	h.check(d.shutdownDone(), "daemon's own final log line: 'shutdown complete'")
	_, serr := os.Stat(sock)
	h.check(os.IsNotExist(serr), fmt.Sprintf("sock file removed at exit (stat err=%v)", serr))
	h.note("no-goroutine-leak bar for a SUBPROCESS: exit 0 requires srv.Done() — the daemon joins every server goroutine before printing")
	h.note("'shutdown complete' and exiting 0 IS the in-process runtime.NumGoroutine reconciliation made observable")

	return finishFull(h, attempt)
}

// finishFull prints the closing verdict.
func finishFull(h *harness, attempt int) int {
	code := h.finish("b5-full")
	if h.fail == 0 {
		fmt.Printf("=== ALL PASS — task-12 single-capture evidence complete (attempt %d, S1..S12) ===\n", attempt)
	}
	return code
}

// ------------------------------------------------------- small accessors --

func gAny(m map[string]any, k string) any {
	if m == nil {
		return "<nil>"
	}
	return m[k]
}

func classOf(e *server.RPCError) string {
	if e == nil {
		return "<nil>"
	}
	return e.Class
}

// holdOf reads the holder field of a held-class error.
func holdOf(e *server.RPCError) string {
	if e == nil {
		return "<nil>"
	}
	return e.Holder
}

// helloRole renders a hello's role nil-safely.
func helloRole(i *replclient.HelloInfo) string {
	if i == nil {
		return "<nil>"
	}
	return i.Role
}

func msgOf(e *server.RPCError) string {
	if e == nil {
		return "<nil>"
	}
	return e.Msg
}

// scriptsOf extracts the scripts list from a board_status result.
func scriptsOf(st map[string]any) []string {
	if st == nil {
		return nil
	}
	raw, _ := st["scripts"].([]any)
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if str, ok := s.(string); ok {
			out = append(out, str)
		}
	}
	return out
}
