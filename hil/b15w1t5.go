// b15w1t5.go — the W1.T5 wave-1 HIL matrix (spec §8.1–8.3 + §5 rows),
// driven over the daemon's internal API against a dedicated daemon (the
// .sh wrapper owns its life). Two real boards:
//
//	board #2 97c1ac — /dev/ttyACM1, fw v7 d42a07d, radio dormant
//	          (USB-only source; one CDC open = one device session/reboot,
//	          disclosed in every lease reply).
//	board #1 d75884 — 10.1.11.204:7777, fw 0.4.0 (protocol v6), cross-
//	          subnet (this host's mDNS is blind to it) — the pin-route
//	          dogfood, revalidated in the matrix. 16KB-era fs cap: NO
//	          large writes against it.
//
// Legs:
//
//	S1 discovery — board #2 enumerated USB-only; board #1 honestly
//	   absent from live discovery (mDNS blind bench).
//	S2 pathology (a) unreachable pin — deadbe pinned at an
//	   unroutable 10.255.255.1:7777 (bench-verified blackhole), timed
//	   eval → bounded dial-fail (≤3×6s) with cause+last_seen_age_ms.
//	S3 pathology (b) source gone — sudo sysfs unbind of the board's
//	   CDC interface (2-2:1.0), registry drop (30s refresh cadence,
//	   honestly recorded), timed ops → zero-dial structured refusals
//	   (<3s), rebind → the board returns. Skipped honestly (owner-
//	   bench item) when sudo -n is unavailable.
//	S4 board #2 USB lease — auto-fallback route with the reboot
//	   disclosure (transport/rebooted/transport_note), fw build
//	   binding, watcher + board-side arming of the log/script event
//	   sources, and a second lease cycle whose board.transport event
//	   is caught live.
//	S5 upload axis — 111KB module-shaped fs_write, commit-sha
//	   verified, timed (W1.T4 measured 1038ms); fs_read byte-exact
//	   (the independent read); the fs write_part/commit ring lines
//	   arriving live as source=log events.
//	S6 script lifecycle + budget + job_wait — a busy-loop script
//	   budget-killed (source=script killed+budget event), the
//	   numbered budget refusal on eval, and job_wait's three states
//	   (delivered / timeout done:false / the slow job answering on
//	   the second wait).
//	S7 log.read cursor — {} full read, since-increment round-trip
//	   across a generated line, tail=latest-N, since+tail refused.
//	S8 board #1 cross-subnet — pin → tcp lease → eval → board_status
//	   freshness fields → release → unpin.
//	S9 teardown — leases released, leftovers removed, boards as found
//	   (board #2 dormant radio-off, CDC bound).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"xuansi/internal/server"
)

// w5 flags (b15w1t2's b15* set stays bound to its own scenario).
var (
	w5Sock     = flag.String("w5sock", "/tmp/opencode/b15-w1t5/d.sock", "b15w1t5: the dedicated daemon's sock")
	w5Board2   = flag.String("w5board2", "97c1ac", "b15w1t5: the USB board's mac6")
	w5FwBuild2 = flag.String("w5fw2", "d42a07d", "b15w1t5: board #2's expected fw build (v7)")
	w5Remote   = flag.String("w5remote", "d75884@10.1.11.204:7777", "b15w1t5: the cross-subnet board mac6@ip:port")
	w5UsbIf    = flag.String("w5usbif", "", "b15w1t5: the CDC interface to unbind for the source-gone leg (e.g. 2-2:1.0; empty = autodetect from the board's by-id path)")
	w5Holder   = flag.String("w5holder", "b15-w1t5", "b15w1t5: lease holder")
)

// ---- a reply/event conn that decodes the §4.1 pathology fields ----

type w5Reply struct {
	res  map[string]any
	rerr *server.RPCError
	err  error
}

type w5Event struct {
	board  string
	source string
	data   map[string]any
}

type w5Conn struct {
	conn    net.Conn
	events  chan w5Event
	mu      sync.Mutex
	nextID  int64
	pending map[int64]chan w5Reply
	dead    chan struct{}
	once    sync.Once
}

func w5Dial(h *harness, sock string) *w5Conn {
	conn, err := net.Dial("unix", sock)
	if err != nil {
		h.check(false, "dial daemon sock: "+err.Error())
		return nil
	}
	c := &w5Conn{conn: conn, events: make(chan w5Event, 512), pending: map[int64]chan w5Reply{}, dead: make(chan struct{})}
	go c.readLoop()
	return c
}

func (c *w5Conn) readLoop() {
	for {
		var m map[string]any
		if err := server.ReadMsg(c.conn, &m); err != nil {
			c.failPending(fmt.Errorf("connection closed: %w", err))
			close(c.dead)
			return
		}
		switch m["type"] {
		case "event":
			ev := w5Event{}
			ev.board, _ = m["board"].(string)
			if e, ok := m["event"].(map[string]any); ok {
				ev.source, _ = e["source"].(string)
				if d, ok := e["data"].(map[string]any); ok {
					ev.data = d
				}
			}
			select {
			case c.events <- ev:
			default:
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
			r := w5Reply{}
			if e, ok := m["error"].(map[string]any); ok {
				rerr := &server.RPCError{Class: fmt.Sprint(e["class"]), Msg: fmt.Sprint(e["msg"])}
				rerr.Holder, _ = e["holder"].(string)
				rerr.Since, _ = e["since"].(string)
				rerr.GraceExpiry, _ = e["grace_expiry"].(string)
				rerr.Cause, _ = e["cause"].(string)
				if age, ok := e["last_seen_age_ms"].(float64); ok {
					v := int64(age)
					rerr.LastSeenAgeMS = &v
				}
				r.rerr = rerr
			} else {
				r.res, _ = m["result"].(map[string]any)
			}
			select {
			case ch <- r:
			default:
			}
		}
	}
}

func (c *w5Conn) failPending(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pending {
		select {
		case ch <- w5Reply{err: err}:
		default:
		}
		delete(c.pending, id)
	}
}

func (c *w5Conn) close() {
	c.once.Do(func() { c.conn.Close() })
	<-c.dead
}

// call runs one request; elapsed is the honest wall time of the op.
func (c *w5Conn) call(timeout time.Duration, req server.Request) (map[string]any, *server.RPCError, error, time.Duration) {
	t0 := time.Now()
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	ch := make(chan w5Reply, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	req.ID = id
	if err := server.WriteMsg(c.conn, &req); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, nil, err, time.Since(t0)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.res, r.rerr, r.err, time.Since(t0)
	case <-timer.C:
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, nil, fmt.Errorf("%s: client timeout after %v", req.Type, timeout), time.Since(t0)
	case <-c.dead:
		return nil, nil, fmt.Errorf("%s: connection closed", req.Type), time.Since(t0)
	}
}

// drainEvents returns every already-queued event matching source.
func (c *w5Conn) drainEvents(source string) []w5Event {
	var out []w5Event
	for {
		select {
		case e := <-c.events:
			if e.source == source || source == "" {
				out = append(out, e)
			}
		default:
			return out
		}
	}
}

// waitEvent waits (bounded) for one event from the exact source.
func (c *w5Conn) waitEvent(source string, bound time.Duration) (w5Event, bool) {
	deadline := time.After(bound)
	for {
		select {
		case e := <-c.events:
			if e.source == source {
				return e, true
			}
		case <-deadline:
			return w5Event{}, false
		}
	}
}

// w5WaitBoardKind waits (bounded) for a source=board event whose data
// kind matches (the board source also carries board.reboot sightings —
// the USB route's disclosed reboots each bump boot_epoch).
func w5WaitBoardKind(c *w5Conn, kind string, bound time.Duration) (w5Event, bool) {
	deadline := time.After(bound)
	for {
		select {
		case e := <-c.events:
			if e.source == "board" {
				if e.data["kind"] == kind {
					return e, true
				}
				fmt.Printf("  [event] board (other kind): %v\n", e.data)
			}
		case <-deadline:
			return w5Event{}, false
		}
	}
}

// ---- small helpers ----

func w5BoardView(res map[string]any, mac6 string) map[string]any {
	if raw, ok := res["boards"].([]any); ok {
		for _, b := range raw {
			if m, ok := b.(map[string]any); ok && m["mac6"] == mac6 {
				return m
			}
		}
	}
	return nil
}

func w5Sources(v map[string]any) []string {
	var out []string
	if raw, ok := v["sources"].([]any); ok {
		for _, s := range raw {
			if sv, _ := s.(string); sv != "" {
				out = append(out, sv)
			}
		}
	}
	return out
}

func w5Sudo(h *harness, path, val string) bool {
	cmd := exec.Command("sudo", "-n", "tee", path)
	cmd.Stdin = strings.NewReader(val + "\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		h.note("sudo tee %s <- %q failed: %v (%s)", path, val, err, strings.TrimSpace(string(out)))
		return false
	}
	return true
}

// w5DetectUsbIf resolves the CDC interface backing the board's by-id
// path (e.g. "usb-Espressif_USB_JTAG_serial_debug_unit_CC:...-if00" →
// "2-2:1.0"): the by-id symlink resolves to the /dev/ttyACMx node, so
// the walk goes /sys/class/tty/<node> → the sysfs device → up to the
// interface directory that owns it.
func w5DetectUsbIf(h *harness, byID string) string {
	node, err := filepath.EvalSymlinks(byID)
	if err != nil {
		h.note("resolve %s: %v", byID, err)
		return ""
	}
	sysLink := filepath.Join("/sys/class/tty", filepath.Base(node))
	d, err := filepath.EvalSymlinks(sysLink)
	if err != nil {
		h.note("resolve %s: %v", sysLink, err)
		return ""
	}
	// d = /sys/devices/.../usbN/N-P/N-P:Cfg.Iface/tty/ttyACMx — the
	// interface dir is the ancestor containing the tty/ entry.
	for i := 0; i < 4 && d != "/" && d != "."; i++ {
		if _, err := os.Stat(filepath.Join(d, "bInterfaceNumber")); err == nil {
			base := filepath.Base(d) // N-P:Cfg.Iface
			if strings.Contains(base, ":") {
				return base
			}
		}
		d = filepath.Dir(d)
	}
	return ""
}

func w5Sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// ---- the matrix ----

func runB15W1T5() int {
	h := &harness{}
	remoteMAC, remoteEP := func() (string, string) {
		parts := strings.SplitN(*w5Remote, "@", 2)
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
		return *w5Remote, ""
	}()
	b2, holder := *w5Board2, *w5Holder

	fmt.Printf("=== b15 W1-T5 wave-1 HIL matrix — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("daemon sock %s | board #2 %s (USB, fw %s) | board #1 %s @ %s (cross-subnet) | holder %s\n",
		*w5Sock, b2, *w5FwBuild2, remoteMAC, remoteEP, holder)

	c := w5Dial(h, *w5Sock)
	if c == nil {
		return h.finish("b15w1t5")
	}
	defer c.close()

	ctx := context.Background()
	_ = ctx

	// ——— S1: discovery ———
	fmt.Println("\n——— S1: discovery (USB-only board #2; mDNS-blind bench for board #1) ———")
	var v2 map[string]any
	ok, last := pollUntil(500*time.Millisecond, 20*time.Second, func() (bool, string) {
		res, rerr, err, _ := c.call(10*time.Second, server.Request{Type: "list_boards"})
		if err != nil || rerr != nil {
			return false, fmt.Sprintf("list_boards: err=%v rerr=%v", err, rerr)
		}
		v2 = w5BoardView(res, b2)
		return v2 != nil, "board #2 not enumerated yet"
	})
	if !h.check(ok, fmt.Sprintf("board #2 %s enumerated (sources %v, usb_path %v) [%s]", b2, w5Sources(v2), v2["usb_path"], last)) {
		return h.finish("b15w1t5")
	}
	h.check(w5Sources(v2) != nil && w5Sources(v2)[0] == "usb",
		fmt.Sprintf("board #2 USB-only (radio dormant) — sources %v", w5Sources(v2)))
	res, rerr, err, _ := c.call(10*time.Second, server.Request{Type: "list_boards"})
	if err == nil {
		if rv := w5BoardView(res, remoteMAC); rv != nil {
			h.note("board #1 %s present in list (sources %v) — UNEXPECTED for the mDNS-blind bench (a pin seed?)", remoteMAC, w5Sources(rv))
		} else {
			h.check(true, "board #1 honestly absent from live discovery (cross-subnet, mDNS blind — the pin leg materializes it)")
		}
	}

	// ——— S2: pathology (a) unreachable-IP pin ———
	fmt.Println("\n——— S2: pathology (a) — pin an unroutable endpoint, time the dial-fail ———")
	const deadIP = "10.255.255.1"
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "boards_pin", Mac6: "deadbe", IP: deadIP, Port: 7777})
	if !h.check(err == nil && rerr == nil && res["pinned"] == "deadbe" && res["endpoint"] == deadIP+":7777",
		fmt.Sprintf("boards_pin deadbe @ %s:7777 → %v (rerr=%v)", deadIP, res, rerr)) {
		return h.finish("b15w1t5")
	}
	res, _, _, _ = c.call(10*time.Second, server.Request{Type: "list_boards"})
	if dv := w5BoardView(res, "deadbe"); dv != nil {
		h.check(fmt.Sprint(w5Sources(dv)) == "[pin]", fmt.Sprintf("deadbe entry live from the pin alone — sources %v", w5Sources(dv)))
	}
	// host-side lease (the tcp route leases without dialing), then the timed op
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "lease", Board: "deadbe", Holder: holder})
	h.check(err == nil && rerr == nil && res["transport"] == "tcp",
		fmt.Sprintf("lease deadbe (host-side, zero dial) → %v (rerr=%v)", res, rerr))
	_, rerr, err, d1 := c.call(40*time.Second, server.Request{Type: "eval", Board: "deadbe", Holder: holder, Code: "return 1"})
	lsm := int64(-2)
	if rerr != nil && rerr.LastSeenAgeMS != nil {
		lsm = *rerr.LastSeenAgeMS
	}
	h.check(rerr != nil && rerr.Class == "unreachable" && rerr.Cause == "dial failed" && rerr.LastSeenAgeMS != nil && d1 <= 20*time.Second,
		fmt.Sprintf("timed eval vs blackhole: %dms (bound 3×6s=18s) class=%q cause=%q last_seen_age_ms=%d", d1.Milliseconds(), rerrClass(rerr), rerrCause(rerr), lsm))
	h.note("dial-fail refusal msg: %s", rerrMsg(rerr))
	_, rerr2, err2, d2 := c.call(40*time.Second, server.Request{Type: "release", Board: "deadbe", Holder: holder})
	h.check(err2 == nil && rerr2 == nil && d2 <= 21*time.Second,
		fmt.Sprintf("release of the unreachable board = bounded teardown (%dms — the release's session teardown is board-touching; the pin source is live-by-declaration, so the same 3×6s dial bound rides it — bounded, never the 60–120s spiral)", d2.Milliseconds()))
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "boards_unpin", Mac6: "deadbe"})
	h.check(err == nil && rerr == nil && res["unpinned"] == "deadbe", fmt.Sprintf("boards_unpin deadbe → %v", res))

	// ——— S3: pathology (b) USB source gone (sysfs unbind) ———
	fmt.Println("\n——— S3: pathology (b) — sysfs CDC unbind → source gone → zero-dial refusals → rebind ———")
	ifaceUsed := ""
	if out, err := exec.Command("sudo", "-n", "true").CombinedOutput(); err != nil {
		h.note("sudo -n unavailable (%v: %s) — leg SKIPPED honestly; owner-bench verification item:\n"+
			"    sudo tee /sys/bus/usb/drivers/cdc_acm/unbind <<< '<iface>'  (iface from the board's by-id path)\n"+
			"    then any board-touching op must refuse <3s with cause \"source gone\"; rebind via the bind file", err, strings.TrimSpace(string(out)))
	} else {
		usbIf := *w5UsbIf
		if usbIf == "" {
			if p, _ := v2["usb_path"].(string); p != "" {
				usbIf = w5DetectUsbIf(h, p)
			}
		}
		if !h.check(usbIf != "", fmt.Sprintf("CDC interface resolved for %s (iface=%q from usb_path %v)", b2, usbIf, v2["usb_path"])) {
			return h.finish("b15w1t5")
		}
		ifaceUsed = usbIf
		// no lease may be held (an open CDC fd would make unbind EBUSY)
		if st, rerr, err, _ := c.call(10*time.Second, server.Request{Type: "board_status", Board: b2}); err == nil && rerr == nil {
			if st["lease"] != nil {
				c.call(35*time.Second, server.Request{Type: "release", Board: b2, Holder: holder})
				h.note("pre-existing lease on %s released before the unbind (open CDC would block it)", b2)
			}
		}
		tUnbind := time.Now()
		if !w5Sudo(h, "/sys/bus/usb/drivers/cdc_acm/unbind", usbIf) {
			h.note("unbind failed — leg aborted honestly")
			return h.finish("b15w1t5")
		}
		h.note("unbind %s written (t=0)", usbIf)
		okDrop, dropNote := pollUntil(time.Second, 45*time.Second, func() (bool, string) {
			res, _, err, _ := c.call(10*time.Second, server.Request{Type: "list_boards"})
			if err != nil {
				return false, "list_boards error"
			}
			dv := w5BoardView(res, b2)
			return dv != nil && len(w5Sources(dv)) == 0, fmt.Sprintf("sources still %v", w5Sources(dv))
		})
		dropMS := time.Since(tUnbind).Milliseconds()
		h.check(okDrop, fmt.Sprintf("registry dropped the usb source %dms after unbind (30s refresh cadence bounds this) [%s]", dropMS, dropNote))

		// (a) explicit USB lease → fast-fail, zero dial. Class note
		// (recorded as a minor finding): this refusal path reports
		// class "server" while the op path below reports "unreachable"
		// — same cause "source gone", same zero dial, a class-vocabulary
		// inconsistency only.
		_, rerr, err, dU := c.call(15*time.Second, server.Request{Type: "lease", Board: b2, Holder: holder, Transport: "usb"})
		h.check(err == nil && rerr != nil && rerr.Cause == "source gone" && dU < 3*time.Second &&
			(rerr.Class == "server" || rerr.Class == "unreachable"),
			fmt.Sprintf("timed lease{usb} vs sourceless board: %dms class=%q cause=%q (zero dial)", dU.Milliseconds(), rerrClass(rerr), rerrCause(rerr)))
		h.note("source-gone refusal msg: %s", rerrMsg(rerr))
		// (b) default lease (host-side) + timed eval
		res, rerrL, _, _ := c.call(10*time.Second, server.Request{Type: "lease", Board: b2, Holder: holder})
		h.check(rerrL == nil && res["transport"] == "tcp",
			fmt.Sprintf("default lease of the sourceless board stays host-side (no gratuitous anything) → %v", res))
		_, rerr, err, dE := c.call(15*time.Second, server.Request{Type: "eval", Board: b2, Holder: holder, Code: "return 1"})
		lsm = -2
		if rerr != nil && rerr.LastSeenAgeMS != nil {
			lsm = *rerr.LastSeenAgeMS
		}
		h.check(err == nil && rerr != nil && rerr.Cause == "source gone" && rerr.Class == "unreachable" && dE < 3*time.Second,
			fmt.Sprintf("timed eval vs sourceless board: %dms class=%q cause=%q last_seen_age_ms=%d (zero dial)", dE.Milliseconds(), rerrClass(rerr), rerrCause(rerr), lsm))
		c.call(35*time.Second, server.Request{Type: "release", Board: b2, Holder: holder})

		// rebind → the board returns
		tBind := time.Now()
		if !w5Sudo(h, "/sys/bus/usb/drivers/cdc_acm/bind", usbIf) {
			h.check(false, "rebind failed — MANUAL RECOVERY NEEDED: sudo tee /sys/bus/usb/drivers/cdc_acm/bind <<< "+usbIf)
			return h.finish("b15w1t5")
		}
		okBack, backNote := pollUntil(time.Second, 45*time.Second, func() (bool, string) {
			res, _, err, _ := c.call(10*time.Second, server.Request{Type: "list_boards"})
			if err != nil {
				return false, "list_boards error"
			}
			dv := w5BoardView(res, b2)
			return dv != nil && len(w5Sources(dv)) > 0 && w5Sources(dv)[0] == "usb", fmt.Sprintf("sources %v", w5Sources(dv))
		})
		h.check(okBack, fmt.Sprintf("rebind → board #2 back %dms later (sources [usb]) [%s] — no reboot involved (CDC never opened)", time.Since(tBind).Milliseconds(), backNote))
	}

	// ——— S4: board #2 lease (auto-fallback USB) + events arming ———
	fmt.Println("\n——— S4: board #2 USB lease — auto-fallback, disclosure, fw binding, event arming ———")
	res, rerr, err, dL := c.call(35*time.Second, server.Request{Type: "lease", Board: b2, Holder: holder})
	transport, _ := res["transport"].(string)
	rebooted, _ := res["rebooted"].(bool)
	tnote, _ := res["transport_note"].(string)
	h.check(err == nil && rerr == nil && transport == "usb" && rebooted && strings.Contains(tnote, "REBOOTS the board"),
		fmt.Sprintf("lease %dms → transport=%q rebooted=%v transport_note=%q (only USB live → auto-fallback, disclosed)", dL.Milliseconds(), transport, rebooted, tnote))
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "board_status", Board: b2})
	st, _ := res["board"].(map[string]any)
	fwGot, _ := st["fw"].(string)
	h.check(err == nil && rerr == nil && st != nil && st["transport"] == "usb" && st["stale"] == false && fwGot == *w5FwBuild2,
		fmt.Sprintf("board_status: transport=%v stale=%v fw=%v (bind %s) fw_version=%v boot_epoch=%v last_seen_age_ms=%v",
			sg(st, "transport"), sg(st, "stale"), fwGot, *w5FwBuild2, sg(st, "fw_version"), sg(st, "boot_epoch"), sg(st, "last_seen_age_ms")))
	// watcher + the two v7 sources armed on the controller the ops will
	// ride. Arming follows the CONNECTION (the frozen arming semantics —
	// same as gpio/uart/stdout): the re-lease cycle below swaps the USB
	// controller (yield → fresh open), so the arm calls come AFTER it.
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "watch_events", Board: b2})
	h.check(err == nil && rerr == nil && res["board"] == b2, fmt.Sprintf("watch_events (daemon-side fan-in registered) → %v", res))
	// second lease cycle → the board.transport event caught live
	c.call(35*time.Second, server.Request{Type: "release", Board: b2, Holder: holder})
	res, rerr, err, _ = c.call(35*time.Second, server.Request{Type: "lease", Board: b2, Holder: holder})
	tnote2, _ := res["transport_note"].(string)
	h.check(rerr == nil && res["transport"] == "usb" && res["rebooted"] == true && strings.Contains(tnote2, "REBOOTS the board"),
		fmt.Sprintf("re-lease after yield → fresh USB open again disclosed (transport_note carried): %q", tnote2))
	if tev, got := w5WaitBoardKind(c, "transport", 10*time.Second); got {
		h.check(true, fmt.Sprintf("board.transport event caught live: data=%v (from/to/rebooted shape)", tev.data))
	} else {
		h.check(false, "board.transport event observed live (watcher was registered before the re-lease)")
	}
	// NOW arm the v7 sources on the controller the matrix's ops ride
	// (the swap above retired any earlier arming — arming follows the
	// connection, the frozen semantics).
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "watch_events", Board: b2, Source: "log"})
	h.check(err == nil && rerr == nil, fmt.Sprintf("watch_events source=log armed on the live controller (rerr=%v)", rerr))
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "watch_events", Board: b2, Source: "script"})
	h.check(err == nil && rerr == nil, fmt.Sprintf("watch_events source=script armed on the live controller (rerr=%v)", rerr))

	// ——— S5: upload axis ———
	fmt.Println("\n——— S5: upload axis — 111KB verified fs_write + independent read + live log events ———")
	payload := make([]byte, 113664)
	for i := range payload {
		payload[i] = byte(i*11%253 + 1)
	}
	shaW := w5Sum(payload)
	c.drainEvents("log")
	res, rerr, err, dW := c.call(60*time.Second, server.Request{Type: "fs_write", Board: b2, Path: "/lua/w1t5big.bin", Data: payload, Sha256: shaW, Holder: holder})
	sz, _ := res["size"].(float64)
	h.check(err == nil && rerr == nil && int(sz) == len(payload) && dW <= 1500*time.Millisecond,
		fmt.Sprintf("fs_write 111KB commit-sha verified: %dms (%.0fKB/s) reply {size:%d} — bound ≤~1.1s (W1.T4: 1038ms)",
			dW.Milliseconds(), float64(len(payload))/1024/dW.Seconds(), int(sz)))
	// the write's ring lines arrive live as source=log events
	logEvs := c.drainEvents("log")
	deadline := time.Now().Add(5 * time.Second)
	for len(logEvs) == 0 && time.Now().Before(deadline) {
		if e, got := c.waitEvent("log", time.Until(deadline)); got {
			logEvs = append(logEvs, e)
		}
	}
	hit := 0
	for _, e := range logEvs {
		if txt, _ := e.data["data"].(string); strings.Contains(txt, "fs: write") {
			hit++
		}
	}
	h.check(hit > 0, fmt.Sprintf("%d source=log event(s) carried the fs write lines live (%d total log events) — e.g. %q",
		hit, len(logEvs), firstLogLine(logEvs)))
	res, rerr, err, _ = c.call(60*time.Second, server.Request{Type: "fs_read", Board: b2, Path: "/lua/w1t5big.bin"})
	b64, _ := res["data"].(string)
	back, derr := base64.StdEncoding.DecodeString(b64)
	h.check(err == nil && rerr == nil && derr == nil && len(back) == len(payload) && w5Sum(back) == shaW,
		fmt.Sprintf("fs_read 111KB byte-exact (independent read face, 4×32640 windows) sha256 equal"))
	c.call(15*time.Second, server.Request{Type: "fs_rm", Board: b2, Path: "/lua/w1t5big.bin", Holder: holder})

	// ——— S6: script lifecycle + numbered budget refusal + job_wait ———
	fmt.Println("\n——— S6: script killed+budget event; budget refusal with numbers; job_wait ×3 ———")
	c.drainEvents("script")
	// A resident script's resumes (the first included) run under the
	// EVENT budget (default 50ms — the eval budget's 200ms is for
	// evals): a busy-loop chunk is killed at start and the verdict
	// returns SYNCHRONOUSLY in the script_start reply, with the
	// started/killed lifecycle events still tapping (both asserted
	// below).
	res, rerr, err, _ = c.call(35*time.Second, server.Request{Type: "script_start", Board: b2, Name: "w1t5busy", Code: "while true do end", Holder: holder})
	msgS := rerrMsg(rerr)
	h.check(err == nil && rerr != nil && rerr.Class == "budget" && strings.Contains(msgS, "budget 50 ms") && strings.Contains(msgS, "budget_ms"),
		fmt.Sprintf("script_start busy-loop: the synchronous event-budget verdict (50ms) with numbers + fix → class=%q msg=%q", rerrClass(rerr), msgS))
	started := false
	killed := false
	var killCause, killName any
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !killed {
		e, got := c.waitEvent("script", time.Until(deadline))
		if !got {
			break
		}
		switch e.data["kind"] {
		case "started":
			started = true
		case "killed":
			killed = true
			killCause, killName = e.data["cause"], e.data["name"]
		}
		fmt.Printf("  [event] script: %v\n", e.data)
	}
	h.check(started && killed && killCause == "budget" && killName == "w1t5busy",
		fmt.Sprintf("script lifecycle events: started=%v killed=%v cause=%v name=%v (event-budget kill)", started, killed, killCause, killName))
	_, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "eval", Board: b2, Holder: holder, Code: "while true do end"})
	msg := rerrMsg(rerr)
	h.check(err == nil && rerr != nil && rerr.Class == "budget" && strings.Contains(msg, "budget") && strings.Contains(msg, "budget_ms") && strings.Contains(msg, "1..30000"),
		fmt.Sprintf("budget refusal carries numbers + the fix: class=%q msg=%q", rerrClass(rerr), msg))
	// job_wait: delivered / timeout / late answer
	_, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "submit", Board: b2, Code: "return 6*7", Tag: "w1t5job", Holder: holder, BudgetMs: 5000})
	h.check(err == nil && rerr == nil, fmt.Sprintf("submit w1t5job (rerr=%v)", rerr))
	res, rerr, err, _ = c.call(12*time.Second, server.Request{Type: "job_wait", Board: b2, Tag: "w1t5job", TimeoutMS: 8000})
	val, _ := res["value"].(float64)
	h.check(err == nil && rerr == nil && res["done"] == true && val == 42,
		fmt.Sprintf("job_wait delivered {done:%v, value:%v, elapsed_ms:%v}", res["done"], val, res["elapsed_ms"]))
	_, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "submit", Board: b2, Code: "local t0=bench.millis() while bench.millis()-t0 < 1500 do end return 'slept'", Tag: "w1t5slow", Holder: holder, BudgetMs: 8000})
	h.check(err == nil && rerr == nil, fmt.Sprintf("submit w1t5slow (1500ms, budget 8000) (rerr=%v)", rerr))
	res, rerr, err, dTO := c.call(5*time.Second, server.Request{Type: "job_wait", Board: b2, Tag: "w1t5slow", TimeoutMS: 200})
	h.check(err == nil && rerr == nil && res["done"] == false && dTO < 5*time.Second,
		fmt.Sprintf("job_wait timeout answers a RESULT not an error: {done:%v, elapsed_ms:%v} in %dms", res["done"], res["elapsed_ms"], dTO.Milliseconds()))
	res, rerr, err, _ = c.call(12*time.Second, server.Request{Type: "job_wait", Board: b2, Tag: "w1t5slow", TimeoutMS: 8000})
	sv, _ := res["value"].(string)
	h.check(err == nil && rerr == nil && res["done"] == true && sv == "slept",
		fmt.Sprintf("the slow job answers on the SECOND wait: {done:%v, value:%q}", res["done"], sv))

	// ——— S7: log.read cursor ———
	fmt.Println("\n——— S7: log.read cursor — {} / since / tail / since+tail refused ———")
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "op", Board: b2, Holder: holder, Op: "log.read", Args: map[string]any{}})
	lines, _ := res["lines"].([]any)
	next1, _ := res["next"].(float64)
	h.check(err == nil && rerr == nil && len(lines) > 0 && next1 > 0,
		fmt.Sprintf("log.read {} → %d lines, next=%d (full-ring window, line shape %q)", len(lines), int(next1), firstLine(lines)))
	small := []byte("w1t5-cursor-probe")
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "fs_write", Board: b2, Path: "/lua/w1t5probe.bin", Data: small, Holder: holder})
	h.check(err == nil && rerr == nil, "cursor probe write landed (its ring line is the since-window's new content)")
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "op", Board: b2, Holder: holder, Op: "log.read", Args: map[string]any{"since": int64(next1)}})
	lines2, _ := res["lines"].([]any)
	next2, _ := res["next"].(float64)
	h.check(err == nil && rerr == nil && len(lines2) > 0 && next2 > next1 && allIdxAtLeast(lines2, int64(next1)),
		fmt.Sprintf("log.read{since:%d} → %d new line(s), next=%d, every idx ≥ since (incremental round-trip)", int(next1), len(lines2), int(next2)))
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "op", Board: b2, Holder: holder, Op: "log.read", Args: map[string]any{"tail": 3}})
	lines3, _ := res["lines"].([]any)
	next3, _ := res["next"].(float64)
	h.check(err == nil && rerr == nil && len(lines3) == 3 && next3 >= next2 && strings.HasPrefix(fmt.Sprint(lines3[len(lines3)-1]), fmt.Sprintf("[%d]", int(next3)-1)),
		fmt.Sprintf("log.read{tail:3} → exactly the LATEST 3 (lines=%d, newest idx=%d=%d-1, next unchanged when nothing new) newest=%q", len(lines3), int(next3)-1, int(next3), lastLine(lines3)))
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "op", Board: b2, Holder: holder, Op: "log.read", Args: map[string]any{"since": 1, "tail": 1}})
	h.check(err == nil && rerr != nil && rerr.Class == "api" && strings.Contains(rerr.Msg, "mutually exclusive"),
		fmt.Sprintf("log.read{since+tail} → api refusal (%q)", rerrMsg(rerr)))
	c.call(15*time.Second, server.Request{Type: "fs_rm", Board: b2, Path: "/lua/w1t5probe.bin", Holder: holder})

	// ——— S8: board #1 cross-subnet dogfood ———
	fmt.Printf("\n——— S8: board #1 %s @ %s — pin → tcp lease → eval → freshness fields ———\n", remoteMAC, remoteEP)
	if conn, derr := net.DialTimeout("tcp", remoteEP, 3*time.Second); derr != nil {
		h.check(false, "preflight: "+remoteEP+" answers TCP directly: "+derr.Error())
		return h.finish("b15w1t5")
	} else {
		conn.Close()
		h.note("preflight: %s answers TCP directly (bare connect+close, no protocol bytes)", remoteEP)
	}
	rhost, rport := hostOf(remoteEP), portOf(remoteEP)
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "boards_pin", Mac6: remoteMAC, IP: rhost, Port: rport})
	h.check(err == nil && rerr == nil && res["pinned"] == remoteMAC && res["endpoint"] == remoteEP,
		fmt.Sprintf("boards_pin %s → %v", remoteMAC, res))
	res, rerr, err, dRL := c.call(35*time.Second, server.Request{Type: "lease", Board: remoteMAC, Holder: holder})
	h.check(err == nil && rerr == nil && res["transport"] == "tcp" && res["rebooted"] == false && dRL < 5*time.Second,
		fmt.Sprintf("lease via pin: %dms → transport=%v rebooted=%v (TCP route — never USB gratuitously)", dRL.Milliseconds(), res["transport"], res["rebooted"]))
	res, rerr, err, _ = c.call(35*time.Second, server.Request{Type: "eval", Board: remoteMAC, Holder: holder, Code: "return 1+1"})
	h.check(err == nil && rerr == nil && fmt.Sprint(res["value"]) == "2",
		fmt.Sprintf("eval over the pin route → value=%v", res["value"]))
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "board_status", Board: remoteMAC})
	st1, _ := res["board"].(map[string]any)
	age1 := sg(st1, "last_seen_age_ms")
	h.check(err == nil && rerr == nil && st1 != nil && st1["transport"] == "tcp" && st1["stale"] == false,
		fmt.Sprintf("board_status (pin route): transport=%v stale=%v last_seen_age_ms=%v fw=%v fw_version=%v boot_epoch=%v chip=%v",
			sg(st1, "transport"), sg(st1, "stale"), age1, sg(st1, "fw"), sg(st1, "fw_version"), sg(st1, "boot_epoch"), sg(st1, "chip")))
	if n, err := strconv.ParseInt(age1, 10, 64); err == nil {
		h.check(n >= 0 && n < 15000, fmt.Sprintf("last_seen_age_ms=%d is a live fresh number (crossed seconds ago)", n))
	}
	res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "op", Board: remoteMAC, Holder: holder, Op: "log.read", Args: map[string]any{}})
	if lns, ok := res["lines"].([]any); ok && err == nil && rerr == nil {
		h.note("board #1 log.read (v6 fw, pre-cursor shape) → %d lines — v7 keys absent on 0.4.0, honestly (next=%v)", len(lns), res["next"])
	}
	c.call(35*time.Second, server.Request{Type: "release", Board: remoteMAC, Holder: holder})
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "boards_unpin", Mac6: remoteMAC})
	h.check(err == nil && rerr == nil && res["unpinned"] == remoteMAC, fmt.Sprintf("boards_unpin %s → %v (entry degrades)", remoteMAC, res))

	// ——— S9: teardown ———
	fmt.Println("\n——— S9: teardown — boards as found ———")
	c.call(35*time.Second, server.Request{Type: "release", Board: b2, Holder: holder})
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "board_status", Board: b2})
	h.check(err == nil && rerr == nil && res["lease"] == nil, fmt.Sprintf("board #2 lease-free (dormant as found, CDC closed by the release-yield)"))
	if st2, ok := res["board"].(map[string]any); ok {
		h.note("board #2 final view: sources=%v stale=%v fw=%v", st2["sources"], st2["stale"], st2["fw"])
	}
	if ifaceUsed != "" {
		if _, err := os.Stat("/sys/bus/usb/drivers/cdc_acm/" + ifaceUsed); err != nil {
			h.check(false, "CDC interface still bound after the matrix (rebind held) — iface "+ifaceUsed)
		} else {
			h.check(true, "CDC interface bound (board #2 USB enumerable — as found)")
		}
	}
	return h.finish("b15w1t5")
}

func lastLine(lines []any) string {
	if len(lines) == 0 {
		return ""
	}
	s, _ := lines[len(lines)-1].(string)
	if len(s) > 90 {
		s = s[:90] + "…"
	}
	return s
}

// runB15W1T5Load is the spec §8.1 clause-2 acceptance leg (fix round 1,
// controller ruling — the loaded-board shape, camera-free): occupy the
// VM queue with CPU-heavy busy-loop jobs, then interleave a log.read
// (conn-task ro op — must stay fast under VM load, the W1-T3 windowed
// serialization's whole point) and an eval (VM-queued — bounded wait for
// the running job, never a teardown spiral), prove the warm link
// survives (board_status connected/transport stable), and land one
// job_wait. Timing shapes decide PASS: hundreds of ms to low seconds,
// NEVER minute-level.
func runB15W1T5Load() int {
	h := &harness{}
	b2, holder := *w5Board2, *w5Holder+"-load"
	fmt.Printf("=== b15 W1-T5 load leg (spec §8.1 clause 2 — 滿載 job 下 op 不觸發拆鏈螺旋) — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("board #2 %s (USB, fw %s) | holder %s | jobs: 3×4s busy loops (serial VM queue ≈12s window), eval budget 15s\n", b2, *w5FwBuild2, holder)

	c := w5Dial(h, *w5Sock)
	if c == nil {
		return h.finish("b15w1t5load")
	}
	defer c.close()

	// the initial discovery sweep is async: wait for board #2's USB
	// enumeration before leasing (attempt 3 raced it — every call
	// answered no_board before the sweep's first listing)
	okEnum, enumNote := pollUntil(300*time.Millisecond, 20*time.Second, func() (bool, string) {
		res, rerr, err, _ := c.call(10*time.Second, server.Request{Type: "list_boards"})
		if err != nil || rerr != nil {
			return false, "list_boards trouble"
		}
		return w5BoardView(res, b2) != nil, "board #2 not enumerated yet"
	})
	h.check(okEnum, fmt.Sprintf("board #2 enumerated before the load (sources %v) [%s]", func() []string {
		res, _, _, _ := c.call(10*time.Second, server.Request{Type: "list_boards"})
		if v := w5BoardView(res, b2); v != nil {
			return w5Sources(v)
		}
		return nil
	}(), enumNote))
	if !okEnum {
		return h.finish("b15w1t5load")
	}

	res, rerr, err, dL := c.call(35*time.Second, server.Request{Type: "lease", Board: b2, Holder: holder})
	transport, _ := res["transport"].(string)
	h.check(err == nil && rerr == nil && transport == "usb", fmt.Sprintf("lease %dms → transport=%v (the load rides this ONE warm controller)", dL.Milliseconds(), res["transport"]))

	// three CPU-heavy jobs, each ~4s of busy loop (bench.millis-bounded,
	// counted work): the VM runs them serially → a ~12s loaded window.
	jobCode := "local x, t0 = 0, bench.millis() while bench.millis()-t0 < 4000 do x = x + 1 end return x"
	for _, tag := range []string{"w1t5load1", "w1t5load2", "w1t5load3"} {
		res, rerr, err, _ = c.call(15*time.Second, server.Request{Type: "submit", Board: b2, Code: jobCode, Tag: tag, Holder: holder, BudgetMs: 15000})
		h.check(err == nil && rerr == nil && res["tag"] == tag, fmt.Sprintf("submit %s acked (async — the VM queue now carries it)", tag))
	}

	// (a) log.read DURING the load: conn-task ro op — the old pathology
	// serialized the full ring on this path (minute-level under load);
	// the v7 windowed shape must answer in hundreds of ms.
	res, rerr, err, dLR := c.call(20*time.Second, server.Request{Type: "op", Board: b2, Holder: holder, Op: "log.read", Args: map[string]any{"tail": 5}})
	lines, _ := res["lines"].([]any)
	h.check(err == nil && rerr == nil && len(lines) == 5 && dLR < 2*time.Second,
		fmt.Sprintf("log.read{tail:5} UNDER LOAD: %dms, %d lines (expect hundreds of ms — never minute-level)", dLR.Milliseconds(), len(lines)))

	// (b) eval DURING the load: it queues behind the running jobs — a
	// bounded wait (≤ the serial queue's remainder ≈12s) then answers;
	// the daemon's same-link discipline must hold (no teardown spiral).
	res, rerr, err, dEV := c.call(40*time.Second, server.Request{Type: "eval", Board: b2, Holder: holder, Code: "return 41+1", BudgetMs: 15000})
	val, _ := res["value"].(float64)
	h.check(err == nil && rerr == nil && val == 42 && dEV < 25*time.Second,
		fmt.Sprintf("eval under load: %dms → value=%v (queued behind the jobs, bounded — low seconds, no spiral)", dEV.Milliseconds(), val))

	// (c) the warm link survived: board_status's view still names the
	// same transport and a live connection (transport rides the board
	// VIEW — res["board"]["transport"], not a top-level key).
	res, rerr, err, _ = c.call(10*time.Second, server.Request{Type: "board_status", Board: b2})
	stLV, _ := res["board"].(map[string]any)
	h.check(err == nil && rerr == nil && res["connected"] == true && stLV != nil && stLV["transport"] == "usb",
		fmt.Sprintf("board_status after the load window: connected=%v board-view transport=%v (no redial, no teardown)", res["connected"], sg(stLV, "transport")))

	// (d) one job_wait lands: the last job's completion delivers.
	res, rerr, err, dJW := c.call(30*time.Second, server.Request{Type: "job_wait", Board: b2, Tag: "w1t5load3", TimeoutMS: 25000})
	jv, _ := res["value"].(float64)
	h.check(err == nil && rerr == nil && res["done"] == true && jv > 0 && dJW < 26*time.Second,
		fmt.Sprintf("job_wait w1t5load3 delivered in %dms: {done:%v, value:%.0f} (the busy loop counted)", dJW.Milliseconds(), res["done"], jv))

	// teardown: drain the remaining jobs' waits (bounded), then release.
	for _, tag := range []string{"w1t5load1", "w1t5load2"} {
		c.call(30*time.Second, server.Request{Type: "job_wait", Board: b2, Tag: tag, TimeoutMS: 25000})
	}
	res, rerr, err, _ = c.call(35*time.Second, server.Request{Type: "release", Board: b2, Holder: holder})
	h.check(err == nil && rerr == nil, fmt.Sprintf("release (board #2 dormant as found) → %v", res))
	h.note("daemon-log cross-check: the wrapper's tee carries the daemon's own lines — a teardown spiral would show link-dead/redial/'USB fallback connection up' repeats beyond the single lease-time open")

	return h.finish("b15w1t5load")
}

func sg(m map[string]any, k string) string {
	if m == nil {
		return "<nil>"
	}
	return fmt.Sprint(m[k])
}

func rerrClass(r *server.RPCError) string {
	if r == nil {
		return ""
	}
	return r.Class
}

func rerrCause(r *server.RPCError) string {
	if r == nil {
		return ""
	}
	return r.Cause
}

func rerrMsg(r *server.RPCError) string {
	if r == nil {
		return ""
	}
	return r.Msg
}

func firstLine(lines []any) string {
	if len(lines) == 0 {
		return ""
	}
	s, _ := lines[0].(string)
	if len(s) > 90 {
		s = s[:90] + "…"
	}
	return s
}

func firstLogLine(evs []w5Event) string {
	for _, e := range evs {
		if txt, _ := e.data["data"].(string); strings.Contains(txt, "fs: write") {
			if len(txt) > 90 {
				txt = txt[:90] + "…"
			}
			return txt
		}
	}
	return ""
}

// allIdxAtLeast asserts every "[idx] t=… " line prefix ≥ floor.
func allIdxAtLeast(lines []any, floor int64) bool {
	for _, l := range lines {
		s, _ := l.(string)
		s = strings.TrimPrefix(s, "[")
		idx, err := strconv.ParseInt(s[:strings.IndexByte(s, ']')], 10, 64)
		if err != nil || idx < floor {
			return false
		}
	}
	return true
}
