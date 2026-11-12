// b16t3drift.go — the B16 F3-① board leg (task T3 step 3, choice (a)):
// the run3 drift trigger replayed on the REAL board #1 over USB, with
// the radio untouched (T2 owns it — no flash, no wifi state change;
// wifi.status is a read). Against a RUNNING daemon (dedicated sock the
// .sh wrapper owns):
//
//	D0 lease transport=usb — the §4.4 fallback open (one CDC open =
//	   one reboot, the §2 normal; the radio state rides through).
//	D1 board_status: connected=true, transport=usb.
//	D2 the run3 TRIGGER: op wifi.status — its reply self-reports the
//	   board's port, EnrichEndpoint materializes Port 0→7777, and the
//	   NEXT supervisor() acquisition drifts the ref — the exact shape
//	   that killed the lease in run3 ("stale link closed", then
//	   "no TCP endpoint to dial" until release→re-lease).
//	D3 the fixed behavior: an eval on the SAME lease (no release)
//	   succeeds — the drift rebuild CARRIED the live USB controller
//	   across the seam.
//	D4 board_status: transport=usb, last_seen_age_ms fresh (the
//	   carried link crossed — continuity, spec AC3).
//	D5 keepalive (F3-②): idle 8s with NO ops, then board_status —
//	   last_seen_age_ms must sit inside the keepalive window (<6s),
//	   not at the idle wall clock.
//	D6 release.
package main

import (
	"flag"
	"fmt"
	"time"

	"xuansi/internal/server"
)

func runB16T3Drift(sock, board, holder string) int {
	h := &harness{}
	fmt.Printf("=== b16 T3 drift carry-over board leg — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("board %s (radio untouched — T2's patient), daemon sock %s\n", board, sock)

	c := dialAPI(h, sock, "b16t3")
	if c == nil {
		return h.finish("b16t3drift")
	}
	defer c.close()

	// ——— D0: the §4.4 usb lease (one open = one reboot) ———
	res, rerr, err := c.call(30*time.Second, server.Request{Type: "lease", Board: board, Holder: holder, Transport: "usb"})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D0 usb lease: err=%v rerr=%v", err, rerr))
		return h.finish("b16t3drift")
	}
	h.check(res["transport"] == "usb" && res["rebooted"] == true,
		fmt.Sprintf("D0 usb lease = {transport:%v, rebooted:%v}, want {usb, true}", res["transport"], res["rebooted"]))

	// ——— D1: connected over usb ———
	res, rerr, err = c.call(10*time.Second, server.Request{Type: "board_status", Board: board})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D1 board_status: err=%v rerr=%v", err, rerr))
		return h.finish("b16t3drift")
	}
	h.check(res["connected"] == true, fmt.Sprintf("D1 connected = %v, want true", res["connected"]))

	// ——— D2: the run3 trigger — the wifi op reply self-reports the
	// port; EnrichEndpoint materializes it; the next acquisition
	// drifts (read-only: the radio state is not touched) ———
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "op", Board: board, Holder: holder, Op: "wifi.status"})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D2 wifi.status: err=%v rerr=%v", err, rerr))
		return h.finish("b16t3drift")
	}
	wifi, _ := res["result"].(map[string]any)
	h.note(fmt.Sprintf("D2 wifi.status reply = %v (the self-report that drifts the ref; radio state untouched)", wifi))

	// ——— D3: the run3 death sentence, now survived WITHOUT release ———
	res, rerr, err = c.call(15*time.Second, server.Request{Type: "eval", Board: board, Holder: holder, Code: "return 7*6"})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D3 eval after the drift trigger: err=%v rerr=%v — run3 needed release→re-lease here", err, rerr))
		return h.finish("b16t3drift")
	}
	out, _ := res["out"].(string)
	h.check(out == "42", fmt.Sprintf("D3 eval after the drift trigger = %q (same lease, no release — the carried link), want 42", out))

	// ——— D4: continuity — the crossing right after the drift is fresh ———
	res, rerr, err = c.call(10*time.Second, server.Request{Type: "board_status", Board: board})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D4 board_status: err=%v rerr=%v", err, rerr))
		return h.finish("b16t3drift")
	}
	bv, _ := res["board"].(map[string]any)
	age, _ := bv["last_seen_age_ms"].(float64)
	h.check(res["connected"] == true && bv["transport"] == "usb",
		fmt.Sprintf("D4 connected/transport = %v/%v, want true/usb", res["connected"], bv["transport"]))
	h.check(age < 2000, fmt.Sprintf("D4 last_seen_age_ms = %.0f after the drift crossing, want <2000 (continuity)", age))

	// ——— D5: the keepalive keeps last_seen a reachability fact ———
	fmt.Println("D5 idling 8s (no ops) — the keepalive window the run3 seat slept through…")
	time.Sleep(8 * time.Second)
	res, rerr, err = c.call(10*time.Second, server.Request{Type: "board_status", Board: board})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("D5 board_status: err=%v rerr=%v", err, rerr))
		return h.finish("b16t3drift")
	}
	bv, _ = res["board"].(map[string]any)
	age, _ = bv["last_seen_age_ms"].(float64)
	h.check(age < 6000, fmt.Sprintf("D5 last_seen_age_ms = %.0f after 8s idle, want <6000 (keepalive-serviced, not the idle wall clock)", age))

	// ——— D6: release ———
	_, rerr, err = c.call(10*time.Second, server.Request{Type: "release", Board: board, Holder: holder})
	h.check(err == nil && rerr == nil, fmt.Sprintf("D6 release: err=%v rerr=%v", err, rerr))
	return h.finish("b16t3drift")
}

// flags for the dispatcher (`go run ./hil -scenario b16t3drift`).
var (
	t3Sock   = flag.String("t3sock", "/tmp/opencode/b16-t3-drift/d.sock", "b16t3drift: the running daemon's sock")
	t3Board  = flag.String("t3board", "d75884", "b16t3drift: the board's mac6 (board #1, radio untouched)")
	t3Holder = flag.String("t3holder", "b16-t3drift", "b16t3drift: lease holder")
)

func runB16T3DriftCmd() int { return runB16T3Drift(*t3Sock, *t3Board, *t3Holder) }
