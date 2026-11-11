// b15w1t2.go — the ⑨/B15 §4.9 cross-subnet dogfood driver (W1-T2
// step 4): board #1 (d75884, 10.1.11.204:7777) is a REAL board this
// host's mDNS cannot see (different subnet) but TCP reaches directly.
// Against a RUNNING daemon (dedicated sock — the .sh wrapper owns its
// life): pin the endpoint through the API, then lease/eval/log.read/
// watch_events all ride the pin, and unpin degrades the entry. The CLI
// legs (boards pin / list / lease / eval) run in the .sh wrapper —
// this driver is the internal-API half plus the ops the CLI has no
// subcommand for (board op log.read, watch_events).
package main

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"xuansi/internal/server"
)

func runB15W1T2(sock, board, endpoint, holder string) int {
	h := &harness{}
	fmt.Printf("=== b15 W1-T2 cross-subnet pin dogfood — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("board %s at %s (mDNS unreachable from this host by bench design), daemon sock %s\n", board, endpoint, sock)

	// ——— preflight: the endpoint answers TCP directly (no protocol
	// bytes sent — a bare connect+close, §13) ———
	conn, err := net.DialTimeout("tcp", endpoint, 3*time.Second)
	if err != nil {
		h.check(false, "preflight TCP dial "+endpoint+": "+err.Error())
		return h.finish("b15w1t2")
	}
	conn.Close()
	h.note(fmt.Sprintf("preflight: %s answers TCP directly", endpoint))

	c := dialAPI(h, sock, "b15w1t2")
	if c == nil {
		return h.finish("b15w1t2")
	}
	defer c.close()

	// ——— pin: the registry's third source, immediate effect ———
	res, rerr, err := c.call(10*time.Second, server.Request{Type: "boards_pin", Mac6: board, IP: hostOf(endpoint), Port: portOf(endpoint)})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("boards_pin: err=%v rerr=%v", err, rerr))
		return h.finish("b15w1t2")
	}
	h.check(res["pinned"] == board && res["endpoint"] == endpoint,
		fmt.Sprintf("boards_pin reply = {pinned:%v, endpoint:%v}, want {%s, %s}", res["pinned"], res["endpoint"], board, endpoint))

	// ——— list: the pin-only entry is live immediately ———
	res, rerr, err = c.call(10*time.Second, server.Request{Type: "list_boards"})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("list_boards: err=%v rerr=%v", err, rerr))
		return h.finish("b15w1t2")
	}
	var view map[string]any
	for _, b := range res["boards"].([]any) {
		if m, ok := b.(map[string]any); ok && m["mac6"] == board {
			view = m
		}
	}
	pinOnly := view != nil && len(view["sources"].([]any)) == 1 && view["sources"].([]any)[0] == "pin"
	h.check(pinOnly, fmt.Sprintf("list_boards entry = %v, want sources [pin] only (mDNS blind bench)", view))
	if view != nil {
		fmt.Printf("  [list_boards] mac6=%v sources=%v ip=%v port=%v stale=%v\n",
			view["mac6"], view["sources"], view["ip"], view["port"], view["stale"])
	}

	// ——— default lease: the pin is a TCP-capable source → tcp ———
	res, rerr, err = c.call(35*time.Second, server.Request{Type: "lease", Board: board, Holder: holder})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("lease: err=%v rerr=%v", err, rerr))
		return h.finish("b15w1t2")
	}
	h.check(res["transport"] == "tcp" && res["rebooted"] == false,
		fmt.Sprintf("lease reply = %v, want transport tcp / rebooted false (the pin route; never USB)", res))
	fmt.Printf("  [lease] holder=%v session=%v transport=%v rebooted=%v\n", res["holder"], res["session"], res["transport"], res["rebooted"])

	// ——— eval rides the pinned endpoint ———
	res, rerr, err = c.call(35*time.Second, server.Request{Type: "eval", Board: board, Holder: holder, Code: "return 1+1"})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("eval: err=%v rerr=%v", err, rerr))
		return h.finish("b15w1t2")
	}
	h.check(fmt.Sprint(res["value"]) == "2", fmt.Sprintf("eval return 1+1 → value=%v, want 2", res["value"]))

	// ——— board op log.read: the allowlisted op face over the pin ———
	res, rerr, err = c.call(35*time.Second, server.Request{Type: "op", Board: board, Holder: holder, Op: "log.read", Args: map[string]any{}})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("op log.read: err=%v rerr=%v", err, rerr))
		return h.finish("b15w1t2")
	}
	lines := len(res["lines"].([]any))
	h.check(lines > 0, fmt.Sprintf("op log.read → %d lines (want >0), reply keys %v", lines, keysOf(res)))

	// ——— watch_events: the subscription rides the same link ———
	res, rerr, err = c.call(35*time.Second, server.Request{Type: "watch_events", Board: board})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("watch_events: err=%v rerr=%v", err, rerr))
		return h.finish("b15w1t2")
	}
	h.check(res["board"] == board, fmt.Sprintf("watch_events ack = %v", res))
	// whatever arrives in a 2s window (a fresh daemon's first sighting
	// is a baseline, not an event — emptiness is legal; the ack is the
	// subscription proof)
	select {
	case ev := <-c.events:
		h.note(fmt.Sprintf("event while watched: source=%v data=%v", ev.Event.Source, ev.Event.Data))
	case <-time.After(2 * time.Second):
		h.note("no event within 2s (legal: a fresh daemon baselines the first sighting)")
	}

	// ——— board_status: the freshness face over the pin route ———
	res, rerr, err = c.call(10*time.Second, server.Request{Type: "board_status", Board: board})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("board_status: err=%v rerr=%v", err, rerr))
		return h.finish("b15w1t2")
	}
	st := res["board"].(map[string]any)
	h.check(st["transport"] == "tcp" && st["stale"] == false,
		fmt.Sprintf("board_status view = %v, want transport tcp / not stale", st))
	fmt.Printf("  [board_status] transport=%v last_seen_age_ms=%v stale=%v fw=%v boot_epoch=%v\n",
		st["transport"], st["last_seen_age_ms"], st["stale"], st["fw"], st["boot_epoch"])

	// ——— release + unpin: the entry degrades honestly ———
	if _, rerr, err = c.call(35*time.Second, server.Request{Type: "release", Board: board, Holder: holder}); err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("release: err=%v rerr=%v", err, rerr))
	}
	res, rerr, err = c.call(10*time.Second, server.Request{Type: "boards_unpin", Mac6: board})
	if err != nil || rerr != nil {
		h.check(false, fmt.Sprintf("boards_unpin: err=%v rerr=%v", err, rerr))
		return h.finish("b15w1t2")
	}
	h.check(res["unpinned"] == board, fmt.Sprintf("boards_unpin reply = %v", res))
	res, _, _ = c.call(10*time.Second, server.Request{Type: "list_boards"})
	for _, b := range res["boards"].([]any) {
		if m, ok := b.(map[string]any); ok && m["mac6"] == board {
			view = m
		}
	}
	postSrcs, _ := view["sources"].([]any) // nil (absent) reads as empty — the degraded entry
	// The frozen staleness rule (W1-T1): stale = no live source AND no
	// crossing within staleAfter (30s). We crossed this board seconds
	// ago, so honest is sources-gone + stale:false NOW — the entry
	// decays to stale only after the window passes.
	h.check(len(postSrcs) == 0 && view["stale"] == false,
		fmt.Sprintf("post-unpin entry = sources %v stale %v, want degraded-but-fresh (sources gone; stale lands after the 30s window)", view["sources"], view["stale"]))
	h.note("cross-subnet dogfood complete: pin → lease/eval/log.read/events/board_status all rode the pin")

	return h.finish("b15w1t2")
}

// hostOf/portOf split an ip:port endpoint (the driver's flag input).
func hostOf(endpoint string) string {
	h, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return endpoint
	}
	return h
}

func portOf(endpoint string) int {
	_, p, err := net.SplitHostPort(endpoint)
	if err != nil {
		return 0
	}
	n := 0
	for _, c := range p {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func keysOf(m map[string]any) string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return strings.Join(ks, ",")
}

// b15 flags registration hook (main's flag set lives in b5t8.go — the
// scenario reads them there via the shared vars below).
var (
	b15Sock     = flag.String("b15sock", "/tmp/opencode/b15-w1t2/d.sock", "b15w1t2: the running daemon's sock")
	b15Board    = flag.String("b15board", "d75884", "b15w1t2: the cross-subnet board's mac6")
	b15Endpoint = flag.String("b15endpoint", "10.1.11.204:7777", "b15w1t2: the pinned TCP endpoint")
	b15Holder   = flag.String("b15holder", "b15-w1t2", "b15w1t2: lease holder")
)
