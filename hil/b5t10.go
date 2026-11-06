// b5t10.go — task-10 hardware-in-the-loop runner: the lease + grace-TTL
// teardown + link-loss resync semantics against the live bench board,
// including the eval-pins ruling probes (design question resolved with
// evidence, not source-reading).
//
//	scenario lease —
//	  R0 ruling probes: eval-claimed pins are session-scoped in the
//	     SHARED namespace 0 (owner=_eval): claim GPIO1/2 via eval →
//	     diag.pins shows sess=0; session.reset(77) (other namespace)
//	     does NOT release them; session.reset(0) does, proven by
//	     write-rejection (safe state = the pin is no longer an output).
//	  R1 grace round (TTL=10s): agent eval-claims GPIO1 + starts a
//	     resident script under ITS session id → agent gone → grace:
//	     script.list still shows it, diag.pins still claimed → third
//	     party refused ErrHeld(grace-expiring) → TTL expiry: daemon
//	     runs session.reset(holder)+session.reset(0), script.list
//	     empty, pins safe — teardown exactly once.
//	  R2 adopt round: fresh acquire (new session id) → gone → third
//	     party refused → Adopt WITHIN the TTL (board state kept — the
//	     script's counter kept counting through grace) → polite
//	     Release tears down the same way.
//	  R3 link-loss resync (board-visible shape): script running →
//	     one-way iptables hole (board→host dropped) + our FIN reaches
//	     the board → board observes the disconnect, frees the §12 slot,
//	     scripts keep running → Resync reports the survivor honestly.
//	     (wifi.off is the wrong dead-link tool here: it needs a USB
//	     controller whose open REBOOTS the board and kills the scripts.)
//	  R4 link-loss SILENT shape (§12-faithful recovery): bidirectional
//	     black-hole → host keepalive verdict ~6s while the board sees
//	     nothing — but the board runs its OWN keepalive (10s/2s×3, §12;
//	     self-release measured 16.5–17.0s), so the supervisor's
//	     persistent re-hellos (ArbitrationWait=25s) wait out the window
//	     and re-take controller over TCP — NO reboot, the script
//	     survives the whole hole+recovery (counter proves it).
//	  R5 USB fallback route sanity: the deliberate last resort
//	     (ConnectUSB reboots, §2) proven post-cleanup when it kills
//	     nothing; the full wedge→reboot→resync chain is in the pre-fix
//	     run's R4 (results/2026-11-05-b5-t10-teardown.log).
//
// Every hello binds protocol_ver/fw_build in-log (evidence binding;
// a mismatch aborts the scenario.
package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"xuansi/internal/board"
	"xuansi/internal/lease"
	"xuansi/internal/replclient"
)

// leaseLogTee prints lease trace lines and counts expiry teardowns (the
// exactly-once assertion reads the counter).
type leaseLogTee struct {
	expiredTeardowns int
	releasedTeardown int
}

func (t *leaseLogTee) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	fmt.Printf("  [lease] %s\n", line)
	if strings.Contains(line, "teardown reason=expired") {
		t.expiredTeardowns++
	}
	if strings.Contains(line, "teardown reason=released") {
		t.releasedTeardown++
	}
}

// resident counter script: global counter so eval can read progress
// (_G shared across the single VM — batch-1 `eval return n` idiom).
const t10Script = "_G.t10n = 0; bench.timer_ms(100, function() _G.t10n = _G.t10n + 1 end); " +
	"while true do local f = coroutine.yield(); if f then f() end end"

func runLease() int {
	h := &harness{}
	ctx := context.Background()
	tee := &leaseLogTee{}
	fmt.Printf("=== b5-t10 lease HIL — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("device: %s / %s (expect fw_build=%s protocol_ver=%d), grace TTL=10s (HIL short)\n",
		usbByID, tcpEndpoint, expectFwBuild, expectProtocolVer)

	// ——— Phase 0: clean table — headless reboot frees the controller
	// slot and clears any previous run's residue (§2: CDC open reboots;
	// no hello sent — nothing claimed) ———
	rb, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "headless reboot dial: "+err.Error())
		return h.finish("lease")
	}
	rb.Close()
	h.note("board rebooted headless (no hello — slot free, table clean)")

	// ——— bind conn: hello evidence binding on the real TCP face ———
	bc := dialTCPRetry(h, 30*time.Second, "bind")
	if bc == nil {
		return h.finish("lease")
	}
	binfo, err := bc.Hello(false, "b5t10-bind")
	if err != nil || !bindHello(h, "bind", binfo) {
		if err != nil {
			h.check(false, "bind hello: "+err.Error())
		}
		bc.Close()
		return h.finish("lease")
	}
	bc.Close() // TCP close releases the slot (§12)

	// ——— the daemon stand-in: Supervisor over the warm TCP route ———
	sup := board.NewSupervisor(board.BoardRef{
		MAC6: "d75884", Host: "xuansi-d75884.local", Port: 7777, USBPath: usbByID,
	}, board.SupOpts{Log: func(format string, args ...any) {
		fmt.Printf("  [board] "+format+"\n", args...)
	}})
	defer sup.Close()
	if _, err := sup.Connect(ctx); err != nil {
		h.check(false, "supervisor connect: "+err.Error())
		return h.finish("lease")
	}

	// ——— R0: the eval-pins ruling probes ———
	fmt.Println("  — R0: eval pins vs session namespaces (design question) —")
	_, v, err := supEval(ctx, sup, `bench.gpio.mode(1,"out") bench.gpio.mode(2,"in") return "claimed"`)
	h.check(err == nil && v == "claimed", fmt.Sprintf("eval claim GPIO1 out / GPIO2 in (err=%v)", err))
	raw, pins, err := sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins]\n%s", indentDump(raw))
	p0 := err == nil && len(pins) == 2 && pins[0].GPIO == 1 && pins[0].Session == 0 && pins[0].Owner == "_eval" &&
		pins[1].GPIO == 2 && pins[1].Session == 0 && pins[1].Owner == "_eval"
	h.check(p0, fmt.Sprintf("eval claims land in session 0 owner _eval (parsed=%+v err=%v)", pins, err))

	// a script under namespace 77: survives reset of OTHER namespaces
	_, err = sup.Client().Op(ctx, "script.start", map[string]any{
		"name": "t10probe", "code": t10Script, "session": int64(77)})
	h.check(err == nil, fmt.Sprintf("script.start t10probe session=77 (err=%v)", err))
	names, err := sup.ScriptList(ctx)
	h.check(err == nil && len(names) == 1 && names[0] == "t10probe", fmt.Sprintf("script.list=[%v] (err=%v)", names, err))
	n77, err := sup.TeardownSession(ctx, 77)
	h.check(err == nil && n77 == 1, fmt.Sprintf("session.reset(77) stopped=%d (err=%v)", n77, err))
	raw, pins, err = sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins after reset(77)]\n%s", indentDump(raw))
	h.check(err == nil && len(pins) == 2 && pins[0].Session == 0,
		fmt.Sprintf("eval pins SURVIVE session.reset(77) — not session-77-scoped (parsed=%+v)", pins))
	names, _ = sup.ScriptList(ctx)
	h.check(len(names) == 0, fmt.Sprintf("namespace-77 script gone (list=%v)", names))

	// …but ARE session-scoped: reset of namespace 0 releases them
	n0, err := sup.TeardownSession(ctx, 0)
	h.check(err == nil, fmt.Sprintf("session.reset(0) (stopped=%d err=%v)", n0, err))
	raw, pins, err = sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins after reset(0)]\n%s", indentDump(raw))
	h.check(err == nil && len(pins) == 0, fmt.Sprintf("eval pins RELEASED by session.reset(0) — session-0-scoped (parsed=%+v)", pins))
	_, _, werr := supEval(ctx, sup, `return bench.gpio.write(1,0)`)
	var woe *replclient.OpError
	h.check(errors.As(werr, &woe),
		fmt.Sprintf("safe-state proof: GPIO1 write rejected after sweep (err=%v — not an output anymore)", werr))
	h.note("RULING: eval pins are session-scoped in namespace 0; daemon teardown = reset(holder) + reset(0) sweep")

	// ——— the lease manager over the supervisor (short HIL TTL) ———
	mgr := lease.NewManager(lease.Policy{
		GraceTTL: 10 * time.Second,
		Clock:    lease.RealClock{},
		OnExpiry: sup.LeaseTeardown,
		Log:      tee.logf,
	})
	defer mgr.Close()

	// ——— R1: grace → TTL expiry teardown ———
	fmt.Println("  — R1: agent gone → grace → TTL expiry → daemon teardown —")
	if err := mgr.Acquire("d75884", "agent-a"); err != nil {
		h.check(false, "R1 acquire: "+err.Error())
		return h.finish("lease")
	}
	s1, _ := mgr.Session("d75884")
	_, v, err = supEval(ctx, sup, `bench.gpio.mode(1,"out") return "claimed"`)
	h.check(err == nil && v == "claimed", fmt.Sprintf("agent eval-claims GPIO1 (err=%v)", err))
	_, err = sup.Client().Op(ctx, "script.start", map[string]any{
		"name": "t10keep", "code": t10Script, "session": int64(s1)})
	h.check(err == nil, fmt.Sprintf("agent script.start t10keep session=%d (err=%v)", s1, err))
	_, _, _ = supEval(ctx, sup, `return _G.t10n`) // counter readable (script alive)
	raw, pins, _ = sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins before drop]\n%s", indentDump(raw))
	h.check(len(pins) == 1 && pins[0].Session == 0 && pins[0].Owner == "_eval",
		fmt.Sprintf("pre-drop capture: GPIO1 claimed by _eval sess=0 (parsed=%+v)", pins))

	mgr.OnAgentGone("agent-a")
	snap := mgr.Snapshot()
	h.check(len(snap) == 1 && !snap[0].GraceDeadline.IsZero(), fmt.Sprintf("grace entered (snapshot=%+v)", snap))
	names, err = sup.ScriptList(ctx)
	h.check(err == nil && len(names) == 1 && names[0] == "t10keep", fmt.Sprintf("grace: script STILL listed [%v]", names))
	raw, pins, _ = sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins during grace]\n%s", indentDump(raw))
	h.check(len(pins) == 1 && pins[0].Owner == "_eval", fmt.Sprintf("grace: pin STILL claimed (parsed=%+v)", pins))

	// third party during grace: refused, grace-expiring marker
	err = mgr.Acquire("d75884", "agent-b")
	var eh *lease.ErrHeld
	h.check(errors.As(err, &eh) && eh.GraceExpiring() && eh.Holder == "agent-a",
		fmt.Sprintf("third-party acquire during grace → ErrHeld grace-expiring (%v)", err))

	h.note("waiting out the 10s grace TTL…")
	waitForLease(h, mgr, tee, 25*time.Second, 1)
	names, err = sup.ScriptList(ctx)
	h.check(err == nil && len(names) == 0, fmt.Sprintf("post-expiry script.list empty (got %v err=%v)", names, err))
	raw, pins, _ = sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins after expiry]\n%s", indentDump(raw))
	h.check(len(pins) == 0, fmt.Sprintf("post-expiry pins safe state (parsed=%+v)", pins))
	_, _, werr = supEval(ctx, sup, `return bench.gpio.write(1,0)`)
	h.check(errors.As(werr, &woe), fmt.Sprintf("post-expiry GPIO1 write rejected (err=%v)", werr))
	h.check(tee.expiredTeardowns == 1, fmt.Sprintf("expiry teardown ran EXACTLY ONCE (count=%d)", tee.expiredTeardowns))
	h.check(len(mgr.Snapshot()) == 0, "lease released host-side after expiry")

	// ——— R2: adopt within the TTL (接手不重置) ———
	fmt.Println("  — R2: reconnect within TTL → adopt, board state kept —")
	if err := mgr.Acquire("d75884", "agent-a"); err != nil {
		h.check(false, "R2 acquire: "+err.Error())
		return h.finish("lease")
	}
	s2, _ := mgr.Session("d75884")
	h.check(s2 != s1, fmt.Sprintf("fresh session namespace after re-acquire (%d ≠ %d)", s2, s1))
	_, err = sup.Client().Op(ctx, "script.start", map[string]any{
		"name": "t10adopt", "code": t10Script, "session": int64(s2)})
	h.check(err == nil, fmt.Sprintf("script.start t10adopt session=%d (err=%v)", s2, err))
	_, v, err = supEval(ctx, sup, `bench.gpio.mode(2,"in") return "claimed"`)
	h.check(err == nil && v == "claimed", fmt.Sprintf("agent eval-claims GPIO2 (err=%v)", err))
	time.Sleep(700 * time.Millisecond) // let the counter tick
	_, nDrop, _ := supEval(ctx, sup, `return _G.t10n`)

	mgr.OnAgentGone("agent-a")
	err = mgr.Acquire("d75884", "agent-b")
	h.check(errors.As(err, &eh) && eh.GraceExpiring(), fmt.Sprintf("grace third-party refused again (%v)", err))

	time.Sleep(6 * time.Second) // 6s < 10s: inside the window
	h.check(mgr.Adopt("agent-a"), "Adopt(agent-a) within TTL")
	names, _ = sup.ScriptList(ctx)
	h.check(len(names) == 1 && names[0] == "t10adopt", fmt.Sprintf("adopted: t10adopt STILL RUNNING (list=%v)", names))
	raw, pins, _ = sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins after adopt]\n%s", indentDump(raw))
	gpio2 := false
	for _, p := range pins {
		if p.GPIO == 2 && p.Owner == "_eval" {
			gpio2 = true
		}
	}
	h.check(gpio2, fmt.Sprintf("adopted: GPIO2 still claimed (parsed=%+v)", pins))
	h.note("GPIO1 mode=none residue seen above = the write-rejection PROBE auto-claiming (fw pin_begin claims before the mode gate) — namespace-0 residue the release sweep cleans below")
	_, nAfter, _ := supEval(ctx, sup, `return _G.t10n`)
	h.check(numGt(nAfter, nDrop),
		fmt.Sprintf("script RAN THROUGH grace (counter %v → %v — adopt kept it, no reset)", nDrop, nAfter))
	snap = mgr.Snapshot()
	h.check(len(snap) == 1 && snap[0].GraceDeadline.IsZero(), fmt.Sprintf("lease live again (snapshot=%+v)", snap))
	h.check(tee.expiredTeardowns == 1 && tee.releasedTeardown == 0,
		fmt.Sprintf("adopt touched nothing (expired=%d released=%d)", tee.expiredTeardowns, tee.releasedTeardown))

	// polite release: same teardown semantics
	h.check(mgr.Release("d75884", "agent-a") == nil, "polite Release")
	waitForRelease(tee, 10*time.Second)
	names, _ = sup.ScriptList(ctx)
	h.check(len(names) == 0, fmt.Sprintf("post-release script.list empty (got %v)", names))
	raw, pins, _ = sup.DiagPins(ctx)
	h.check(len(pins) == 0, fmt.Sprintf("post-release pins safe (parsed=%+v)", pins))
	h.check(tee.releasedTeardown == 1, fmt.Sprintf("release teardown ran once (count=%d)", tee.releasedTeardown))

	// ——— R3: link loss → scripts survive → reconnect → honest resync ———
	fmt.Println("  — R3: daemon↔board link loss → resync (survived reported honestly) —")
	if err := mgr.Acquire("d75884", "agent-a"); err != nil {
		h.check(false, "R3 acquire: "+err.Error())
		return h.finish("lease")
	}
	s3, _ := mgr.Session("d75884")
	_, err = sup.Client().Op(ctx, "script.start", map[string]any{
		"name": "t10resync", "code": t10Script, "session": int64(s3)})
	h.check(err == nil, fmt.Sprintf("script.start t10resync session=%d (err=%v)", s3, err))
	_, v, err = supEval(ctx, sup, `bench.gpio.mode(1,"out") return "claimed"`)
	h.check(err == nil && v == "claimed", fmt.Sprintf("eval claim GPIO1 (err=%v)", err))

	rep1, err := sup.Resync(ctx) // baseline
	h.check(err == nil && len(rep1.Scripts) == 1 && rep1.Scripts[0] == "t10resync",
		fmt.Sprintf("baseline resync scripts=%v (err=%v)", rep1.Scripts, err))

	// ——— the link-loss dead-link shapes ———
	// (a) board-visible disconnect: one-way INPUT hole + our FIN (outbound
	//     alive) — the board observes the disconnect, frees the §12 slot,
	//     keeps its scripts running: the survive-and-resync shape.
	// (b) silent bidirectional hole (R4): the host keepalive verdicts at
	//     ~6s while the board sees nothing — BUT the board runs its OWN
	//     keepalive (10s idle + 2s×3 probes, §12 frozen; self-release
	//     measured 16.5–17.0s), so the slot is NOT permanently wedged:
	//     the supervisor's persistent re-hellos (ArbitrationWait=25s)
	//     ride out the window and take the slot over TCP — no reboot,
	//     surviving scripts untouched. The USB fallback (reboot) is the
	//     explicitly-labeled LAST RESORT only after the full window.
	deadlink := "oneway-fin"
	if !sudoAvailable() {
		deadlink = "local-close"
		h.note("no sudo — local close (dead-link DETECTION is T8-proven; here we prove reconnect+resync semantics)")
	}
	if deadlink == "oneway-fin" {
		if !oneWayHoleAndClose(h, sup) {
			deadlink = "local-close(fallback)"
			sup.Client().Close()
		}
	} else {
		sup.Client().Close()
	}
	waitForDeadSup(h, sup, 15*time.Second)
	h.note("dead-link path used: %s", deadlink)

	rep2, err := sup.Resync(ctx) // reconnect + honest diff
	h.check(err == nil, fmt.Sprintf("resync after link loss (err=%v)", err))
	fmt.Printf("  [resync] scripts=%v survived=%v started=%v gone=%v pins=%d wifi_state=%v\n",
		rep2.Scripts, rep2.Survived, rep2.Started, rep2.Gone, len(rep2.Pins), rep2.Wifi["state"])
	h.check(err == nil && !rep2.ColdStart, "resync diffed against the pre-break baseline")
	h.check(len(rep2.Survived) == 1 && rep2.Survived[0] == "t10resync",
		fmt.Sprintf("t10resync SURVIVED the link loss and is reported (survived=%v)", rep2.Survived))
	h.check(len(rep2.Gone) == 0 && len(rep2.Started) == 0,
		fmt.Sprintf("no phantom diff (started=%v gone=%v)", rep2.Started, rep2.Gone))
	h.check(len(rep2.Pins) == 1 && rep2.Pins[0].GPIO == 1 && rep2.Pins[0].Session == 0,
		fmt.Sprintf("GPIO1 claim survived too (pins=%+v)", rep2.Pins))
	_, nFinal, _ := supEval(ctx, sup, `return _G.t10n`)
	h.note("script kept running through the dead-link window (counter now %v)", nFinal)

	// ——— R4: silent bidirectional hole → wait out the board's keepalive
	// self-release → re-take controller over TCP (§12-faithful recovery,
	// NO reboot — scripts must survive) ———
	fmt.Println("  — R4: silent black-hole → board keepalive self-release → TCP re-take (no reboot) —")
	if sudoAvailable() {
		// read the counter while the link is still alive — the
		// post-recovery read below proves the script straddled everything
		_, nBeforeRecovery, _ := supEval(ctx, sup, `return _G.t10n`)
		h.note("counter before the silent hole: %v (script running)", nBeforeRecovery)
		if silentHoleUntilVerdict(h, sup) {
			// The reconnect now PERSISTS: re-hellos across the board's
			// self-release window (budget 25s ≥ 17s lower bound). The
			// t10resync script keeps running throughout — the counter
			// below proves it straddled the whole dead-link + recovery.
			origUSB := sup.Opts().DialUSB
			usbDials := 0
			sup.Opts().DialUSB = func() (board.Controller, error) { // tripwire: the §12-faithful path must not touch USB
				usbDials++
				return nil, fmt.Errorf("tripwire: unexpected USB fallback in the §12-faithful recovery")
			}
			rctx, cancel := context.WithTimeout(ctx, 40*time.Second)
			_, cerr := sup.Connect(rctx)
			cancel()
			sup.Opts().DialUSB = origUSB // restore for the R5 fallback sanity
			h.check(cerr == nil && usbDials == 0,
				fmt.Sprintf("controller re-taken over TCP after the self-release window (err=%v, usbDials=%d)", cerr, usbDials))

			rep3, rerr := sup.Resync(ctx)
			h.check(rerr == nil, fmt.Sprintf("resync after silent-hole recovery (err=%v)", rerr))
			fmt.Printf("  [resync] scripts=%v survived=%v started=%v gone=%v pins=%d wifi_state=%v\n",
				rep3.Scripts, rep3.Survived, rep3.Started, rep3.Gone, len(rep3.Pins), rep3.Wifi["state"])
			h.check(len(rep3.Survived) == 1 && rep3.Survived[0] == "t10resync" && len(rep3.Gone) == 0,
				fmt.Sprintf("script SURVIVED the silent hole AND the no-reboot recovery (survived=%v gone=%v)", rep3.Survived, rep3.Gone))
			_, nAfterRecovery, _ := supEval(ctx, sup, `return _G.t10n`)
			h.check(numGt(nAfterRecovery, nBeforeRecovery),
				fmt.Sprintf("counter %v → %v — the script ran through the entire silent-hole + recovery window", nBeforeRecovery, nAfterRecovery))
			h.note("full USB-fallback chain (wedge error → deliberate reboot → honest nothing-survived resync → TCP migration) is proven by the pre-fix run's R4: results/2026-11-05-b5-t10-teardown.log")
		}
	} else {
		h.note("no sudo — R4 silent-hole self-release path skipped (unit tests pin the ≥25s schedule; the window is board-measured 16.5–17.0s, batch 4)")
	}

	// final cleanup: release tears everything down
	h.check(mgr.Release("d75884", "agent-a") == nil, "final release")
	waitForRelease(tee, 10*time.Second)
	names, _ = sup.ScriptList(ctx)
	h.check(len(names) == 0, fmt.Sprintf("final script.list empty (got %v)", names))
	raw, pins, _ = sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins final]\n%s", indentDump(raw))
	h.check(len(pins) == 0, fmt.Sprintf("final pins safe state (parsed=%+v)", pins))
	mgr.Close()

	// ——— R5: USB fallback route sanity ———
	// The LAST-RESORT route stays proven on the current code: nothing is
	// running anymore (the release above tore everything down), so the
	// deliberate reboot kills nothing. The full fallback chain (wedge
	// error → reboot → honest nothing-survived resync → TCP migration)
	// lives in the pre-fix run's R4: results/2026-11-05-b5-t10-teardown.log.
	fmt.Println("  — R5: USB fallback route sanity (post-cleanup; the reboot kills nothing now —")
	uctx, cancelU := context.WithTimeout(ctx, 45*time.Second)
	_, uerr := sup.ConnectUSB(uctx)
	cancelU()
	h.check(uerr == nil, fmt.Sprintf("ConnectUSB fallback route works (board REBOOTED, §2; err=%v)", uerr))
	names, _ = sup.ScriptList(ctx)
	h.check(len(names) == 0, fmt.Sprintf("post-reboot script.list empty (got %v)", names))
	rawF, pinsF, _ := sup.DiagPins(ctx)
	fmt.Printf("  [diag.pins post-usb-fallback]\n%s", indentDump(rawF))
	h.check(len(pinsF) == 0, fmt.Sprintf("post-reboot pins safe (parsed=%+v)", pinsF))

	return h.finish("lease")
}

// supEval runs one eval over the supervisor's warm controller
// connection (the daemon-stand-in path agents will use).
func supEval(ctx context.Context, sup *board.Supervisor, code string) (out string, value any, err error) {
	c := sup.Client()
	if c == nil {
		return "", nil, fmt.Errorf("supervisor has no live controller")
	}
	res, err := c.Op(ctx, "eval", map[string]any{"code": code})
	if err != nil {
		return "", nil, err
	}
	out, _ = res["out"].(string)
	return out, res["value"], nil
}

// waitForLease waits until the expiry teardown trace fired want times.
func waitForLease(h *harness, mgr *lease.Manager, tee *leaseLogTee, bound time.Duration, want int) {
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if tee.expiredTeardowns >= want && len(mgr.Snapshot()) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.check(false, fmt.Sprintf("lease expiry did not settle within %v (expired=%d)", bound, tee.expiredTeardowns))
}

// waitForRelease waits for the release teardown trace.
func waitForRelease(tee *leaseLogTee, bound time.Duration) bool {
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if tee.releasedTeardown >= 1 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// waitForDeadSup waits until the supervisor holds no live controller.
func waitForDeadSup(h *harness, sup *board.Supervisor, bound time.Duration) {
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if sup.Client() == nil {
			h.note("supervisor's board link declared dead (keepalive verdict)")
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.check(false, fmt.Sprintf("board link not declared dead within %v", bound))
}

// oneWayHoleAndClose injects the board-visible dead link: drop only
// board→host (INPUT), then close the client — our FIN travels the still
// open outbound path, the board observes the disconnect (§12 frees the
// controller slot) while its scripts keep running. The surviving
// half-open window is momentary; the rule comes out immediately after.
func oneWayHoleAndClose(h *harness, sup *board.Supervisor) bool {
	ip := boardIP()
	if ip == "" {
		h.check(false, "resolve board ip for the one-way hole")
		return false
	}
	sport, ok := sportOfConn(ip)
	if !ok {
		h.check(false, "locate supervisor conn local port via ss")
		return false
	}
	h.note("one-way hole: dropping %s:7777 → host:%s (board→host), then closing — FIN reaches the board, scripts keep running", ip, sport)
	inRule := []string{"-I", "INPUT", "1", "-p", "tcp", "--sport", "7777", "--dport", sport, "-s", ip, "-j", "DROP"}
	args := append([]string{"iptables"}, inRule...)
	if out, err := exec.Command("sudo", append([]string{"-n"}, args...)...).CombinedOutput(); err != nil {
		h.check(false, fmt.Sprintf("insert INPUT DROP: %v (%s)", err, strings.TrimSpace(string(out))))
		return false
	}
	remove := func() {
		if out, err := exec.Command("sudo", "-n", "iptables", "-D", "INPUT", "-p", "tcp",
			"--sport", "7777", "--dport", sport, "-s", ip, "-j", "DROP").CombinedOutput(); err != nil {
			fmt.Printf("  [FAIL] rule removal: %v (%s)\n", err, strings.TrimSpace(string(out)))
			h.fail++
			h.failures = append(h.failures, "iptables rules removed")
		}
	}
	time.Sleep(300 * time.Millisecond) // let any in-flight board traffic die in the hole first
	c := sup.Client()
	if c == nil {
		remove()
		return false
	}
	c.Close() // FIN → board: real disconnect, board-side
	remove()
	return true
}

// silentHoleUntilVerdict injects the SILENT dead link: both directions
// dropped, held until the host keepalive verdict (~6s) — the board sees
// nothing, its controller slot wedges (the R4 recovery case). Rules are
// removed either way; reports whether the verdict arrived.
func silentHoleUntilVerdict(h *harness, sup *board.Supervisor) bool {
	ip := boardIP()
	if ip == "" {
		h.check(false, "resolve board ip for the silent hole")
		return false
	}
	sport, ok := sportOfConn(ip)
	if !ok {
		h.check(false, "locate supervisor conn local port via ss")
		return false
	}
	h.note("silent hole: dropping BOTH directions of host:%s ↔ %s:7777, holding until the host keepalive verdict", sport, ip)
	outRule := []string{"-I", "OUTPUT", "1", "-p", "tcp", "--dport", "7777", "--sport", sport, "-d", ip, "-j", "DROP"}
	inRule := []string{"-I", "INPUT", "1", "-p", "tcp", "--sport", "7777", "--dport", sport, "-s", ip, "-j", "DROP"}
	for _, r := range [][]string{outRule, inRule} {
		args := append([]string{"iptables"}, r...)
		if out, err := exec.Command("sudo", append([]string{"-n"}, args...)...).CombinedOutput(); err != nil {
			h.check(false, fmt.Sprintf("%v: %v (%s)", r, err, strings.TrimSpace(string(out))))
			return false
		}
	}
	verdict := false
	t0 := time.Now()
	deadline := t0.Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if sup.Client() == nil {
			verdict = true
			break
		}
		time.Sleep(50 * time.Millisecond)
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
	if verdict {
		h.note("keepalive verdict after %v — host side dead, board BLIND (slot will wedge)", time.Since(t0).Round(time.Millisecond))
	} else {
		h.check(false, "no keepalive verdict within 15s of the silent hole")
	}
	return verdict
}

// indentDump prefixes each dump line for the transcript.
func indentDump(raw string) string {
	lines := strings.Split(strings.TrimRight(raw, "\n"), "\n")
	for i, l := range lines {
		lines[i] = "  | " + l
	}
	return strings.Join(lines, "\n") + "\n"
}

// numGt compares two eval-bridged counters (int64 or float).
func numGt(a, b any) bool {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	return aok && bok && af > bf
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	}
	return 0, false
}
