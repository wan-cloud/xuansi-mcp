// Command hil is the task-8 hardware-in-the-loop runner: it drives the
// live bench board (Goouuu ESP32-S3-CAM, mac6 d75884, fw 9dce098,
// protocol_ver 5 — the B10-T1 canonical rebind) through both transports and prints a check-by-check
// transcript for capture into results/.
//
//	scenario usb — drain/reboot semantics (§2): eval string + structured
//	  bridge round-trips, fs 16KB sha256, submit + job event with a
//	  zero-stall ping proof, gpio events off the wired pair, §7 desync
//	  (reserved 0x02 type byte) → Dead → Reconnect recovery.
//	scenario tcp — no-drain first-frame hello (§13), USB/TCP coexistence
//	  arbitration (§12: reboot steals, forced observer, voluntary yield,
//	  instant release on TCP close), keepalive dead-link measured via
//	  iptables DROP on the 4-tuple (preferred) and the wifi.off net-down
//	  path (EOF fast path or degraded keepalive — recorded honestly).
//
// Every hello prints its protocol_ver and fw_build in-log (evidence
// binding); a mismatch against the expected build aborts the scenario.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"time"

	"xuansi/internal/replclient"
)

const (
	expectProtocolVer = int64(5)  // B10-T1: fs chunked-transport overlay era
	expectFwBuild     = "dcaa9ae" // B12-P2 fullclean canonical (protocol_ver 6); the B5-era suites (usb/tcp/full) additionally bind protocol_ver 5 below and run red-by-design on v6 — era evidence, kept

	// HOUSE RULE (B10-T1 fix rounds 1-2): a fw canonical bump rebinds EVERY
	// *FwBuild constant in hil/ IN THE SAME COMMIT — expectFwBuild/b6FwBuild/
	// b7FwBuild/b8FwBuild/b8t2FwBuild/b9FwBuild. These bind at RUNTIME against
	// hello's fw_build; a stale one false-FAILs the next HIL run of that suite
	// against the freshly flashed board (found twice: ver constants in round 1,
	// fw_build constants in round 2 — same failure class both times).

	usbByID     = "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00"
	tcpEndpoint = "xuansi-d75884.local:7777"
)

func main() {
	scenario := flag.String("scenario", "", "usb | tcp | discover | lease | full")
	deadlink := flag.String("deadlink", "auto", "tcp keepalive path: auto | iptables | wifioff | all")
	creds := flag.String("creds", "", "wifi credentials file for the b5t9 explicit-restore path (default: repo-relative incoming/wifi-wan-places.txt)")
	attempt := flag.Int("attempt", 1, "full: honest attempt number recorded in the capture header")
	daemonBin := flag.String("daemon", "bin/xuansi-linux-amd64", "full: daemon binary the orchestration drives as a subprocess")
	runDir := flag.String("run", "/tmp/opencode/b5-full", "full: scratch dir for the daemon sock + boards.conf")
	// b10t7 (closed loop): aim-round number, JPEG evidence dir, geometry
	// reference "x,y,w,h" + tolerance px for the loop's pairing check
	round := flag.Int("round", 1, "b10t7aim: aim round number (recorded in the JPEG names + verdict)")
	outDir := flag.String("outdir", "", "b10t7: dir the aim/loop JPEG evidence copies land in")
	refGeom := flag.String("ref", "", "b10t7: reference screen bbox x,y,w,h from a converged aim round (empty = honest red: pairing must fail)")
	tolPx := flag.Int("tol", 20, "b10t7: geometry pairing tolerance in pixels")
	flag.Parse()
	switch *scenario {
	case "usb":
		os.Exit(runUSB())
	case "tcp":
		os.Exit(runTCP(*deadlink))
	case "discover":
		os.Exit(runDiscover(*creds))
	case "lease":
		os.Exit(runLease())
	case "full":
		os.Exit(runFull(*attempt, *daemonBin, *runDir))
	case "b6mcp":
		os.Exit(runB6MCP(*attempt, *daemonBin, *runDir))
	case "b7mcp":
		os.Exit(runB7MCP(*attempt, *daemonBin, *runDir))
	case "b8t1":
		os.Exit(runB8T1(*attempt, *daemonBin, *runDir))
	case "b8t2":
		os.Exit(runB8T2(*attempt, *daemonBin, *runDir))
	case "b9camera":
		os.Exit(runB9Camera(*attempt, *daemonBin, *runDir))
	case "b10t5":
		os.Exit(runB10T5(*attempt, *daemonBin, *runDir))
	case "b10t8b":
		os.Exit(runB10T8B(*attempt, *daemonBin, *runDir))
	case "b10t7aim":
		os.Exit(runB10T7Aim(*round, *daemonBin, *runDir, *outDir))
	case "b10t7":
		os.Exit(runB10T7Loop(*attempt, *daemonBin, *runDir, *outDir, *refGeom, *tolPx))
	case "spawnidle":
		os.Exit(runSpawnIdle(*attempt, *daemonBin, *runDir))
	case "b11toolchain":
		os.Exit(runB11Toolchain(*attempt, *daemonBin, *runDir))
	case "mdnsprobe":
		os.Exit(runMDNSProbe())
	case "b15w1t2":
		os.Exit(runB15W1T2(*b15Sock, *b15Board, *b15Endpoint, *b15Holder))
	default:
		fmt.Fprintln(os.Stderr, "usage: hil -scenario usb|tcp|discover|lease|full|b6mcp|b7mcp|b8t1|b8t2|b9camera|b10t5|b10t7aim|b10t7|b10t8b|spawnidle|b11toolchain|mdnsprobe|b15w1t2 [-deadlink auto|iptables|wifioff|all] [-creds PATH] [-attempt N] [-daemon BIN] [-run DIR] [-round N] [-outdir DIR] [-ref x,y,w,h] [-tol PX] [-b15sock SOCK] [-b15board MAC6] [-b15endpoint IP:PORT] [-b15holder H]")
		os.Exit(2)
	}
}

// harness counts checks; a failed check is collected for the footer.
type harness struct {
	pass     int
	fail     int
	failures []string
}

func (h *harness) check(cond bool, label string) bool {
	if cond {
		h.pass++
		fmt.Printf("  [PASS] %s\n", label)
	} else {
		h.fail++
		h.failures = append(h.failures, label)
		fmt.Printf("  [FAIL] %s\n", label)
	}
	return cond
}

func (h *harness) note(format string, args ...any) {
	fmt.Printf("  [note] "+format+"\n", args...)
}

func (h *harness) finish(scenario string) int {
	fmt.Printf("=== %s: %d pass, %d fail ===\n", scenario, h.pass, h.fail)
	for _, f := range h.failures {
		fmt.Printf("  FAILED: %s\n", f)
	}
	if h.fail > 0 {
		return 1
	}
	return 0
}

func dialLog(format string, args ...any) {
	fmt.Printf("[dial] "+format+"\n", args...)
}

// logHello prints the in-log evidence line: protocol_ver + fw_build
// binding plus the arbitrated role and boot reason.
func logHello(tag string, h *replclient.HelloInfo) {
	fmt.Printf("  hello[%s]: protocol_ver=%d fw_build=%s role=%s reset=%s fw=%q idf=%s chip=%s lua=%s fs=%v net=%v\n",
		tag, h.ProtocolVer, h.FwBuild, h.Role, h.Reset, h.Fw, h.Idf, h.Chip, h.Lua, h.Fs, h.Net)
}

// bindHello asserts the evidence binding — the run must be against the
// flashed 9dce098 speaking protocol_ver 5 (the B10-T1 canonical build);
// anything else means the board is not the build this task was
// validated for: STOP.
func bindHello(h *harness, tag string, info *replclient.HelloInfo) bool {
	logHello(tag, info)
	if !h.check(info.ProtocolVer == expectProtocolVer && info.FwBuild == expectFwBuild,
		fmt.Sprintf("hello[%s] binds protocol_ver=%d fw_build=%s (want %d/%s)",
			tag, info.ProtocolVer, info.FwBuild, expectProtocolVer, expectFwBuild)) {
		fmt.Printf("  STOP: board is not the expected build — aborting scenario\n")
		return false
	}
	return true
}

// waitDeadFrom polls Dead until true; returns elapsed-since-t0 and ok.
func waitDeadFrom(c *replclient.Client, t0 time.Time, timeout time.Duration) (time.Duration, bool) {
	deadline := t0.Add(timeout)
	for !c.Dead() {
		if time.Now().After(deadline) {
			return time.Since(t0), false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return time.Since(t0), true
}

func opTimeoutError(err error) string { return fmt.Sprintf("%v", err) }

// ---------------------------------------------------------------- usb ---

func runUSB() int {
	h := &harness{}
	ctx := context.Background()
	fmt.Printf("=== b5-t8 USB HIL — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("device: %s (expect fw_build=%s protocol_ver=%d)\n", usbByID, expectFwBuild, expectProtocolVer)

	// — dial: open pulls DTR/RTS → board reboots (§2); drain absorbs the
	//   ~2.6KB ROM console prefix so the frame stream starts pure —
	c, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "DialUSB: "+opTimeoutError(err))
		return h.finish("usb")
	}
	defer c.Close()

	// — hello: fresh device session, controller by default —
	info, err := c.Hello(false, "b5t8-usb")
	if err != nil {
		h.check(false, "hello: "+opTimeoutError(err))
		return h.finish("usb")
	}
	if !bindHello(h, "usb", info) {
		return h.finish("usb")
	}
	h.check(info.Role == "controller", fmt.Sprintf("fresh CDC session role=%s (want controller)", info.Role))

	// — eval: plain string value through the v3 bridge —
	out, v, err := c.Eval(ctx, "return 6*7", nil, 0)
	h.check(err == nil && v == int64(42), fmt.Sprintf("eval return 6*7 → value=%v out=%q err=%v", v, out, err))

	// — eval: structured round-trip (T1 bridge): ARGS map in, ARGS back —
	args := map[string]any{"s": "xuansi", "n": int64(7), "b": true,
		"arr": []any{int64(1), int64(2), int64(3)}}
	_, v, err = c.Eval(ctx, "return ARGS", args, 0)
	h.check(err == nil && reflect.DeepEqual(v, args),
		fmt.Sprintf("eval return ARGS round-trip byte-shaped equal (err=%v)", err))

	// — eval: nested depth-8 table through the value bridge (v3: value
	//   depth ≤8 per-value, root counting 1 — built Lua-side so the
	//   args key stays shallow; a depth-8 ARGS would exceed the args
	//   decode depth budget — different limit, same number). A returned
	//   table's out is its type name ("table", format_results), so only
	//   the bridged value is asserted. —
	out, v, err = c.Eval(ctx, `local t=42 for i=1,7 do t={k=t} end return {t=t}`, nil, 0)
	want := map[string]any{"t": map[string]any{"k": map[string]any{"k": map[string]any{"k": map[string]any{"k": map[string]any{"k": map[string]any{"k": map[string]any{"k": int64(42)}}}}}}}}
	ok8 := err == nil && reflect.DeepEqual(v, want)
	h.check(ok8, fmt.Sprintf("eval depth-8 table via value bridge (err=%v)", err))

	// — settle: wait for the boot auto-connect to finish (net up) before
	//   heavy traffic — during association the conn task's RX chunks can
	//   gap past the fw's 2s truncated-frame timer (§7 type 3), which
	//   the first HIL round hit at the 16KB write — recorded honestly —
	netUp := false
	for i := 0; i < 30 && !netUp; i++ {
		if st, serr := c.Op(ctx, "wifi.status", nil); serr == nil {
			if s, _ := st["state"].(string); s == "up" {
				netUp = true
				h.note("net up before heavy traffic: ip=%v ssid=%v", st["ip"], st["ssid"])
				break
			} else if i == 0 {
				h.note("wifi settling: state=%v (boot auto-connect in progress)", st["state"])
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	h.check(netUp, "board wifi settled to up before the 16KB transfer")

	// — fs 16KB: random bytes, single fs.write, byte-exact read-back,
	//   sha256 both sides, cap refused client-side, rm cleanup —
	buf := make([]byte, 16384)
	if _, err := rand.Read(buf); err != nil {
		h.check(false, "rand: "+opTimeoutError(err))
		return h.finish("usb")
	}
	shaW := sha256.Sum256(buf)
	err = c.FsWrite(ctx, "/lua/t8buf.tmp", buf)
	h.check(err == nil, fmt.Sprintf("fs.write 16384B /lua/t8buf.tmp (err=%v)", err))
	got, err := c.FsRead(ctx, "/lua/t8buf.tmp")
	shaR := sha256.Sum256(got)
	h.check(err == nil && bytes.Equal(got, buf),
		fmt.Sprintf("fs.read byte-exact (%d bytes, err=%v)", len(got), err))
	h.note("sha256 write=%x", shaW)
	h.note("sha256 read =%x", shaR)
	h.check(shaW == shaR, "sha256(write) == sha256(read)")
	err = c.FsWrite(ctx, "/lua/t8big.tmp", make([]byte, 16385))
	h.check(err != nil && strings.Contains(err.Error(), "16384"),
		fmt.Sprintf("fs.write 16385B refused client-side at the 16384B cap (err=%v)", err))
	_, err = c.Op(ctx, "fs.rm", map[string]any{"path": "/lua/t8buf.tmp"})
	h.check(err == nil, fmt.Sprintf("fs.rm /lua/t8buf.tmp cleanup (err=%v)", err))
	_, err = c.FsRead(ctx, "/lua/t8buf.tmp")
	var oe *replclient.OpError
	h.check(errors.As(err, &oe) && oe.Class == "api",
		fmt.Sprintf("fs.read after rm → board api error (err=%v)", err))

	// — submit 4s job: ack immediate, ping during the job proves the
	//   conn task never stalls (§6), result arrives as a job event with
	//   its bridged value —
	_, err = c.Op(ctx, "budget.set", map[string]any{"eval_ms": 4000})
	h.check(err == nil, fmt.Sprintf("budget.set eval_ms=4000 for the 4s job (err=%v)", err))
	jobs := make(chan replclient.Event, 1)
	cancelJobSub := c.OnEvent("job", func(e replclient.Event) { jobs <- e })
	defer cancelJobSub()
	t0 := time.Now()
	err = c.Submit(ctx, `local t=os.clock() while os.clock()-t < 4 do end return "job4s-value"`, "t8job", 0)
	ackLat := time.Since(t0)
	h.check(err == nil && ackLat < time.Second,
		fmt.Sprintf("submit ack in %v (err=%v) — immediate, job queued", ackLat, err))
	time.Sleep(300 * time.Millisecond) // let the job get running
	pt := time.Now()
	_, err = c.Op(ctx, "ping", nil)
	pingLat := time.Since(pt)
	h.check(err == nil && pingLat < time.Second,
		fmt.Sprintf("ping during 4s job answered in %v (err=%v) — zero-stall", pingLat, err))
	var job replclient.Event
	select {
	case job = <-jobs:
	case <-time.After(8 * time.Second):
		h.check(false, "job event delivered within 8s")
	}
	if job.Data != nil {
		jobLat := time.Since(t0)
		h.check(job.Data["tag"] == "t8job" && job.Data["ok"] == true && job.Data["value"] == "job4s-value",
			fmt.Sprintf("job event data ok/tag/value (latency %v): %#v", jobLat, job.Data))
	}
	_, err = c.Op(ctx, "budget.set", map[string]any{"eval_ms": 200})
	h.note("budget restored to eval_ms=200 (err=%v)", err)

	// — gpio events: claim the wired pair GPIO1(out)↔GPIO2(in), subscribe,
	//   drive edges, observe rising+falling on gpio.2 —
	_, v, err = c.Eval(ctx, `bench.gpio.mode(1,"out") bench.gpio.mode(2,"in") return "claimed"`, nil, 0)
	h.check(err == nil && v == "claimed", fmt.Sprintf("claim GPIO1 out / GPIO2 in (err=%v)", err))
	_, err = c.Op(ctx, "events.subscribe", map[string]any{"source": "gpio.2"})
	h.check(err == nil, fmt.Sprintf("events.subscribe gpio.2 (err=%v)", err))
	evs := make(chan replclient.Event, 8)
	cancelGpioSub := c.OnEvent("gpio.", func(e replclient.Event) { evs <- e })
	defer cancelGpioSub()
	_, v, err = c.Eval(ctx, `bench.gpio.write(1,1) bench.gpio.write(1,0) return "drove"`, nil, 0)
	h.check(err == nil && v == "drove", fmt.Sprintf("drive GPIO1 edges (err=%v)", err))
	var edges []string
collect:
	for len(edges) < 2 {
		select {
		case e := <-evs:
			if e.Source == "gpio.2" {
				edge, _ := e.Data["edge"].(string)
				edges = append(edges, edge)
				h.note("event gpio.2 edge=%s level=%v", edge, e.Data["level"])
			}
		case <-time.After(3 * time.Second):
			break collect
		}
	}
	h.check(len(edges) >= 2 && edges[0] == "rising" && edges[1] == "falling",
		fmt.Sprintf("gpio.2 events observed %v (want rising then falling)", edges))
	_, err = c.Op(ctx, "events.unsubscribe", map[string]any{"source": "gpio.2"})
	h.note("events.unsubscribe gpio.2 (err=%v)", err)

	// — desync: inject a reserved-type 0x02 frame (§7) — the board
	//   answers one protocol error frame with id=nil then goes
	//   CS_DEAD-silent; the client must mark Dead —
	err = c.SendRaw([]byte{0, 0, 0, 1, 0x02})
	h.check(err == nil, fmt.Sprintf("sendraw type=0x02 frame (err=%v)", err))
	dl, ok := waitDeadFrom(c, time.Now(), 3*time.Second)
	h.check(ok, fmt.Sprintf("Dead() within 3s of the desync frame (took %v)", dl))
	h.note("dead reason: %s", c.DeadReason())
	_, err = c.Op(ctx, "ping", nil)
	h.check(errors.Is(err, replclient.ErrDead),
		fmt.Sprintf("post-desync op fast-fails ErrDead (err=%v)", err))

	// — recovery: Reconnect — CDC reopens the port (board reboots, fresh
	//   session), drains, re-hellos with the previous identity —
	rctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	t0 = time.Now()
	c2, h2, err := c.Reconnect(rctx)
	rl := time.Since(t0)
	if err != nil {
		h.check(false, fmt.Sprintf("Reconnect (err=%v)", err))
		return h.finish("usb")
	}
	defer c2.Close()
	h.note("reconnect took %v (close→reopen→reboot→drain→hello)", rl)
	if !bindHello(h, "reconnect", h2) {
		return h.finish("usb")
	}
	h.check(h2.Role == "controller", fmt.Sprintf("post-reconnect role=%s (fresh session re-took controller)", h2.Role))
	_, v, err = c2.Eval(ctx, `return "recovered"`, nil, 0)
	h.check(err == nil && v == "recovered", fmt.Sprintf("reconnected client evaluates (value=%v err=%v)", v, err))

	return h.finish("usb")
}

// ---------------------------------------------------------------- tcp ---

func runTCP(deadlinkMode string) int {
	h := &harness{}
	ctx := context.Background()
	fmt.Printf("=== b5-t8 TCP HIL — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("endpoint: %s (expect fw_build=%s protocol_ver=%d), deadlink=%s\n",
		tcpEndpoint, expectFwBuild, expectProtocolVer, deadlinkMode)

	// — fresh-boot precondition: the previous test run's CDC zombie
	//   holds the controller slot (§12: CDC has no host-disconnect
	//   signal and holds until reboot) — reboot the board headless
	//   (open+pulse+drain, no hello: nothing is claimed) so the first
	//   TCP hello arbitrates on a clean table —
	rb, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "headless reboot dial: "+opTimeoutError(err))
		return h.finish("tcp")
	}
	rb.Close()
	h.note("board rebooted headless (no hello sent — slot left free)")

	// — TCP hello with no drain window: connect ≠ reboot; the first
	//   frame on the wire is hello itself (§13) —
	tc := dialTCPRetry(h, 15*time.Second, "initial")
	if tc == nil {
		return h.finish("tcp")
	}
	t0 := time.Now()
	info, err := tc.Hello(false, "b5t8-tcp")
	helloLat := time.Since(t0)
	if err != nil {
		h.check(false, "tcp hello: "+opTimeoutError(err))
		return h.finish("tcp")
	}
	if !bindHello(h, "tcp", info) {
		return h.finish("tcp")
	}
	h.check(info.Role == "controller", fmt.Sprintf("initial TCP hello role=%s (single connection → controller)", info.Role))
	h.check(helloLat < time.Second,
		fmt.Sprintf("no-drain hello answered in %v — first frame was hello (§13)", helloLat))
	_, err = tc.Op(ctx, "ping", nil)
	h.check(err == nil, fmt.Sprintf("ping over TCP (err=%v)", err))

	// — USB open steals arbitration by rebooting the board: the CDC open
	//   pulls DTR/RTS (§2), the chip hard-resets — no FIN on the old TCP
	//   session, so silence + host keepalive is the only verdict path —
	pt := time.Now()
	_, err = tc.Op(ctx, "ping", nil) // fresh traffic: keepalive idle timer restarts here
	if err != nil {
		h.note("pre-steal ping err=%v (continuing — keepalive timer baselined anyway)", err)
	}
	uc, uerr := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	uinfo, uerr2 := uc.Hello(false, "b5t8-usb-steal")
	if uerr != nil || uerr2 != nil {
		h.check(false, fmt.Sprintf("USB open during TCP session (dial=%v hello=%v)", uerr, uerr2))
		return h.finish("tcp")
	}
	if !bindHello(h, "usb-steal", uinfo) {
		return h.finish("tcp")
	}
	h.check(uinfo.Role == "controller", "USB hello after reboot = controller (arbitration table is RAM-only)")
	deadLat, ok := waitDeadFrom(tc, pt, 15*time.Second)
	h.check(ok, fmt.Sprintf("old TCP conn declared dead %v after its last traffic (board reboot — keepalive path)", deadLat))
	h.note("tcp dead reason: %s", tc.DeadReason())
	tc.Close()

	// — coexistence: CDC holds the controller slot (§12 — CDC has no
	//   host-disconnect signal and holds until reboot/yield): a new TCP
	//   hello is forced observer —
	tc2 := dialTCPRetry(h, 15*time.Second, "coexist")
	if tc2 == nil {
		return h.finish("tcp")
	}
	defer tc2.Close()
	i2, err := tc2.Hello(false, "b5t8-tcp2")
	if err != nil {
		h.check(false, "coexist hello: "+opTimeoutError(err))
		return h.finish("tcp")
	}
	logHello("tcp-forced-observer", i2)
	h.check(i2.Role == "observer", fmt.Sprintf("later hello while CDC holds the slot → forced observer (role=%s)", i2.Role))

	// — USB voluntary yield: controller re-hello as observer releases
	//   the slot (§12) —
	iy, err := uc.Hello(true, "b5t8-usb-steal")
	if err != nil {
		h.check(false, "yield hello: "+opTimeoutError(err))
		return h.finish("tcp")
	}
	logHello("usb-yield", iy)
	h.check(iy.Role == "observer", "USB controller re-hello as observer = voluntary yield")

	// — TCP takes the freed slot on the same connection (re-hello
	//   switches identity, §5/§12) —
	i3, err := tc2.Hello(false, "b5t8-tcp2")
	if err != nil {
		h.check(false, "takeover hello: "+opTimeoutError(err))
		return h.finish("tcp")
	}
	logHello("tcp-takeover", i3)
	h.check(i3.Role == "controller", "TCP re-hello non-observer took the freed slot")

	// — TCP disconnect releases the slot (§12): close tc2 → the USB
	//   observer re-hellos into controller. The board processes the FIN
	//   asynchronously, so the re-hello polls briefly rather than
	//   racing it —
	tc2.Close()
	var iu *replclient.HelloInfo
	for i := 0; i < 8; i++ {
		iu, err = uc.Hello(false, "b5t8-usb-steal")
		if err == nil && iu.Role == "controller" {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err != nil {
		h.check(false, "post-TCP-close USB hello: "+opTimeoutError(err))
		return h.finish("tcp")
	}
	logHello("usb-retake", iu)
	h.check(iu.Role == "controller", "TCP close released the slot; USB re-hello took controller")

	// — keepalive dead-link measurements —
	wantIPTables := deadlinkMode == "iptables" || deadlinkMode == "all" ||
		(deadlinkMode == "auto" && sudoAvailable())
	wantWifiOff := deadlinkMode == "wifioff" || deadlinkMode == "all"
	if !wantIPTables && !wantWifiOff {
		wantWifiOff = true // auto without sudo falls back to the wifi.off path
	}
	if wantIPTables {
		iptablesDeadlink(h, ctx)
	}
	if wantWifiOff {
		wifiOffDeadlink(h, ctx, uc)
	}

	return h.finish("tcp")
}

// dialTCPRetry dials the endpoint until it answers or the wait expires
// (the listener exists only while net is up, §12 — right after a reboot
// the board is still associating).
func dialTCPRetry(h *harness, wait time.Duration, tag string) *replclient.Client {
	deadline := time.Now().Add(wait)
	for {
		c, err := replclient.DialTCP(tcpEndpoint, replclient.DialOpts{Log: dialLog})
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			h.check(false, fmt.Sprintf("dial tcp (%s): %v", tag, err))
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// sudoAvailable reports whether non-interactive sudo works (the
// iptables deadlink path needs it).
func sudoAvailable() bool {
	return exec.Command("sudo", "-n", "true").Run() == nil
}

// boardIP resolves the endpoint hostname; empty on failure.
func boardIP() string {
	addr, err := net.ResolveTCPAddr("tcp", tcpEndpoint)
	if err != nil {
		return ""
	}
	return addr.IP.String()
}

// iptablesDeadlink: black-hole the host↔board 4-tuple in BOTH
// directions (a silent dead link — the cable-pull shape) and measure
// Dead latency. OUTPUT-only is NOT enough for the host verdict: the
// board keeps retransmitting un-ACKed data inbound, and every inbound
// segment refreshes the kernel's keepalive idle timer, so host probes
// never fire (measured: verdict only at ~16s via the board's retrans
// abort; ss showed keepalive probes-out frozen at 0 — see the task-8
// report). With both directions dropped, host keepalive (3s idle /
// 1s interval / 3 probes) must verdict ≈6s. The sport pins the rule to
// exactly this connection; rules are removed either way.
func iptablesDeadlink(h *harness, ctx context.Context) {
	fmt.Println("  — keepalive dead-link, path A: iptables black-hole on the 4-tuple —")
	ip := boardIP()
	if ip == "" {
		h.check(false, "resolve board ip for iptables path")
		return
	}
	tc := dialTCPRetry(h, 15*time.Second, "iptables")
	if tc == nil {
		return
	}
	defer tc.Close()
	info, err := tc.Hello(false, "b5t8-ka")
	if err != nil || !bindHello(h, "ka-iptables", info) {
		return
	}
	h.note("tcp role during keepalive test: %s (arbitration irrelevant here)", info.Role)
	_, err = tc.Op(ctx, "ping", nil) // fresh traffic: idle timer restarts here
	if err != nil {
		h.check(false, "ping before DROP: "+opTimeoutError(err))
		return
	}
	sport, ok := sportOfConn(ip)
	if !ok {
		h.check(false, "locate the conn's local port via ss")
		return
	}
	h.note("4-tuple host:%s → %s:7777 (black-holed both directions)", sport, ip)

	outRule := []string{"-I", "OUTPUT", "1", "-p", "tcp", "--dport", "7777", "--sport", sport, "-d", ip, "-j", "DROP"}
	inRule := []string{"-I", "INPUT", "1", "-p", "tcp", "--sport", "7777", "--dport", sport, "-s", ip, "-j", "DROP"}
	for _, r := range [][]string{outRule, inRule} {
		args := append([]string{"iptables"}, r...)
		if out, err := exec.Command("sudo", append([]string{"-n"}, args...)...).CombinedOutput(); err != nil {
			h.check(false, fmt.Sprintf("%v: %v (%s)", r, err, strings.TrimSpace(string(out))))
			return
		}
	}
	fmt.Printf("  [note] rules inserted: OUTPUT+INPUT DROP on the 4-tuple (deadlink window open)\n")
	defer func() {
		for _, chain := range []string{"OUTPUT", "INPUT"} {
			if out, err := exec.Command("sudo", "-n", "iptables", "-L", chain, "-v", "-n").CombinedOutput(); err == nil {
				for _, line := range strings.Split(string(out), "\n") {
					if strings.Contains(line, "7777") {
						fmt.Printf("  [note] %s accounting: %s\n", chain, strings.TrimSpace(line))
					}
				}
			}
		}
		for _, r := range [][]string{
			{"-D", "OUTPUT", "-p", "tcp", "--dport", "7777", "--sport", sport, "-d", ip, "-j", "DROP"},
			{"-D", "INPUT", "-p", "tcp", "--sport", "7777", "--dport", sport, "-s", ip, "-j", "DROP"},
		} {
			args := append([]string{"iptables"}, r...)
			if out, err := exec.Command("sudo", append([]string{"-n"}, args...)...).CombinedOutput(); err != nil {
				fmt.Printf("  [FAIL] rule removal %v: %v (%s)\n", r[1], err, strings.TrimSpace(string(out)))
				h.fail++
				h.failures = append(h.failures, "iptables rules removed")
			}
		}
		fmt.Println("  [note] rules removed — deadlink window closed")
	}()

	t0 := time.Now()
	lat, ok := waitDeadFrom(tc, t0, 15*time.Second)
	h.check(ok, fmt.Sprintf("Dead() verdict arrived (took %v)", lat))
	h.note("dead reason: %s", tc.DeadReason())
	h.check(lat <= 7*time.Second,
		fmt.Sprintf("keepalive dead-link %v ≤ ~6s class (assert ≤7s; idle 3s + 3×1s probes)", lat))
}

// sportOfConn finds the local port of our established conn to the
// board's REPL port via ss (during the deadlink window this host holds
// exactly one such conn by construction).
func sportOfConn(ip string) (string, bool) {
	out, err := exec.Command("ss", "-tno", "dport = :7777").CombinedOutput()
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, "ESTAB") || !strings.Contains(line, ip+":7777") {
			continue
		}
		f := strings.Fields(line)
		for i, tok := range f {
			if strings.HasPrefix(tok, ip+":7777") && i > 0 {
				if lp := f[i-1]; strings.Contains(lp, ":") {
					return strings.SplitN(lp, ":", 2)[1], true
				}
			}
		}
	}
	return "", false
}

// wifiOffDeadlink: net-down from a second (USB) connection — the board
// closes TCP before esp_wifi_stop(); typical EOF ≈0.10–0.15s, but the
// FIN is known to be missed intermittently (degrades to keepalive
// ≈6s). Whichever bucket is recorded honestly. Recovery: reboot via the
// CDC open (NVS creds auto-connect, §14).
func wifiOffDeadlink(h *harness, ctx context.Context, uc *replclient.Client) {
	fmt.Println("  — keepalive dead-link, path B: wifi.off net-down from a second USB connection —")
	if uc == nil || uc.Dead() {
		h.check(false, "need a live USB controller to issue wifi.off")
		return
	}
	tc := dialTCPRetry(h, 15*time.Second, "wifioff")
	if tc == nil {
		return
	}
	info, err := tc.Hello(false, "b5t8-ka2")
	if err != nil || !bindHello(h, "ka-wifioff", info) {
		tc.Close()
		return
	}
	_, err = tc.Op(ctx, "ping", nil) // fresh traffic
	if err != nil {
		h.check(false, "ping before wifi.off: "+opTimeoutError(err))
		return
	}

	t0 := time.Now()
	res, err := uc.Op(ctx, "wifi.off", nil)
	h.check(err == nil, fmt.Sprintf("wifi.off from USB controller (err=%v)", err))
	h.note("wifi.off reply: %v", res)
	lat, ok := waitDeadFrom(tc, t0, 15*time.Second)
	if h.check(ok, fmt.Sprintf("TCP conn declared dead %v after wifi.off", lat)) {
		if lat < time.Second {
			h.note("fast path: EOF delivered (board closed before esp_wifi_stop)")
		} else {
			h.note("FIN missed — degraded to host keepalive verdict (known intermittent, §13)")
		}
	}
	h.note("dead reason: %s", tc.DeadReason())
	tc.Close()

	// — recovery: closing and reopening CDC reboots the board; NVS
	//   creds auto-connect (~2.7s); poll wifi.status back to up —
	uc.Close()
	rc, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "re-dial USB for wifi recovery: "+opTimeoutError(err))
		return
	}
	defer rc.Close()
	rinfo, err := rc.Hello(false, "b5t8-usb-recover")
	if err != nil {
		h.check(false, "recovery hello: "+opTimeoutError(err))
		return
	}
	if !bindHello(h, "usb-recover", rinfo) {
		return
	}
	up := false
	var lastState any
	for i := 0; i < 20 && !up; i++ {
		st, serr := rc.Op(ctx, "wifi.status", nil)
		if serr == nil {
			lastState = st["state"]
			if s, _ := st["state"].(string); s == "up" {
				up = true
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	h.check(up, fmt.Sprintf("wifi auto-recovered after reboot (state=%v)", lastState))
}
