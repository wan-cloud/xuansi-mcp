// b5t9.go — task-9 hardware-in-the-loop runner: live discovery of the
// bench board via both faces, merged by mac6, with the honest
// stale-marking lifecycle exercised for real.
//
//	scenario discover —
//	  A: board on WiFi+USB → Refresh ⇒ exactly ONE entry (mac6=d75884,
//	     sources usb+mdns, TXT ver=3), Resolve by mac6/alias/hostname,
//	     unmatched selector errors listing candidates; lazy enrich
//	     (observer hello, never steals the controller slot, §12).
//	  B: wifi.off from the USB controller (T8 pattern) → Refresh ⇒ the
//	     mdns source downgrades away, the usb face and all last-known
//	     data stay (no hard delete).
//	  C: restore via CDC reboot (NVS auto-connect; explicit wifi.setup
//	     with the git-ignored creds file only if the STA parked in
//	     fail, §14) → Refresh ⇒ dual-source back.
//
// Bench honesty note: this bench has ONE physical board, so multi-board
// paths (dual boards merged, ambiguity errors between two live boards)
// are covered by the unit table in internal/discover/registry_test.go —
// the results header records this limitation.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"xuansi/internal/discover"
	"xuansi/internal/replclient"
)

// defaultCreds derives the git-ignored wifi credentials file the way
// the fw repo's xscreds.py does (repo-relative, never logged).
var defaultCreds = filepath.Join("..", "..", "incoming", "wifi-wan-places.txt")

var (
	credSSID = regexp.MustCompile(`^SSID\s*[:：]\s*(.+)$`)
	credPass = regexp.MustCompile(`^(?:密碼|密码|Password|PASS|pass)\s*[:：]\s*(.+)$`)
)

// loadCreds reads the credentials file (SSID is public; the password
// exists only in memory — never printed, never logged).
func loadCreds(path string) (ssid, pass string, ok bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", false
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if m := credSSID.FindStringSubmatch(line); m != nil {
			ssid = strings.TrimSpace(m[1])
		}
		if m := credPass.FindStringSubmatch(line); m != nil {
			pass = strings.TrimSpace(m[1])
		}
	}
	return ssid, pass, ssid != "" && pass != ""
}

// boardLine renders one board for the transcript.
func boardLine(b discover.Board) string {
	src := strings.Join(b.Sources, "+")
	if src == "" {
		src = "stale(none)"
	}
	s := fmt.Sprintf("mac6=%s alias=%q sources=[%s] usb=%s host=%s port=%d txt=%v",
		b.MAC6, b.Alias, src, b.USBPath, b.Host, b.Port, b.TXT)
	if b.Hello != nil {
		s += fmt.Sprintf(" hello{protocol_ver=%d fw_build=%s}", b.Hello.ProtocolVer, b.Hello.FwBuild)
	} else {
		s += " hello=nil"
	}
	return s
}

// settleNetUp polls wifi.status until state=up (T8's settle idiom: the
// association takes ~3s after boot; asserting before it settles races
// the mDNS registration this task discovers through).
func settleNetUp(h *harness, c *replclient.Client, tag string) bool {
	for i := 0; i < 30; i++ {
		if st, err := c.Op(context.Background(), "wifi.status", nil); err == nil {
			if s, _ := st["state"].(string); s == "up" {
				h.note("%s: net up (ip=%v ssid=%v)", tag, st["ip"], st["ssid"])
				return true
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func runDiscover(credsPath string) int {
	h := &harness{}
	ctx := context.Background()
	fmt.Printf("=== b5-t9 discovery HIL — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("device: %s (expect fw_build=%s protocol_ver=%d)\n", usbByID, expectFwBuild, expectProtocolVer)
	fmt.Printf("bench note: ONE physical board — multi-board merge/ambiguity paths are unit-covered (internal/discover/registry_test.go)\n")

	// ——— Phase A: board on WiFi + USB ———
	// Settle first (T8 idiom): the CDC open reboots the board; wifi
	// associates ~3s; mDNS registers on net-up (§14).
	uc, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "settle dial: "+err.Error())
		return h.finish("discover")
	}
	info, err := uc.Hello(false, "b5t9-settle")
	if err != nil || !bindHello(h, "settle", info) {
		if err != nil {
			h.check(false, "settle hello: "+err.Error())
		}
		return h.finish("discover")
	}
	h.check(settleNetUp(h, uc, "phase A"), "board wifi settled to up before discovery")
	uc.Close()

	// boards.conf fixture: a malformed line rides along as live evidence
	// of the lenient parse (skip + warn, file may be hand-edited).
	if err := os.MkdirAll("results", 0o755); err != nil {
		h.check(false, "mkdir results: "+err.Error())
		return h.finish("discover")
	}
	conf := filepath.Join("results", "2026-11-05-b5-t9-boards.conf")
	confText := "# b5-t9 boards.conf fixture: name=mac6, '#' comments\n" +
		"bench=d75884\n" +
		"this-line-has-no-separator   <- malformed: skipped with a warning\n"
	if err := os.WriteFile(conf, []byte(confText), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return h.finish("discover")
	}
	fmt.Printf("  boards.conf fixture: %s\n", conf)
	aliases, err := discover.LoadBoardsFile(conf)
	h.check(err == nil && len(aliases) == 1 && aliases["d75884"] == "bench",
		fmt.Sprintf("LoadBoardsFile: aliases=%v (malformed line skipped+warned above, err=%v)", aliases, err))

	reg := discover.NewRegistry(aliases)
	t0 := time.Now()
	err = reg.Refresh(ctx)
	h.check(err == nil && reg.MDNSError() == "", fmt.Sprintf("Refresh dual-source (err=%v, mdnsErr=%q)", err, reg.MDNSError()))
	h.note("refresh took %v (usb sweep + 3s mdns browse window)", time.Since(t0).Round(time.Millisecond))

	snap := reg.Snapshot()
	for _, b := range snap {
		fmt.Printf("  board: %s\n", boardLine(b))
	}
	h.check(len(snap) == 1, fmt.Sprintf("exactly one board discovered (got %d)", len(snap)))
	if len(snap) != 1 {
		return h.finish("discover")
	}
	b := snap[0]
	h.check(b.MAC6 == "d75884", fmt.Sprintf("mac6=%s (want d75884 — by-id MAC tail = mDNS hostname tail)", b.MAC6))
	h.check(strings.Join(b.Sources, "+") == "usb+mdns", fmt.Sprintf("sources=%v (one entry, both faces)", b.Sources))
	h.check(b.USBPath == usbByID, fmt.Sprintf("USBPath is the by-id path (%s)", b.USBPath))
	h.check(b.Host == "xuansi-d75884.local" && b.Port == 7777, fmt.Sprintf("mdns face host=%s port=%d", b.Host, b.Port))
	h.check(b.TXT["ver"] == "3" && b.TXT["api"] == "1", fmt.Sprintf("TXT ver=%s api=%s (§14, same source as hello protocol_ver)", b.TXT["ver"], b.TXT["api"]))
	h.check(b.Alias == "bench", fmt.Sprintf("alias from boards.conf = %q", b.Alias))
	h.check(b.Hello == nil, "Hello nil before any dial (lazy enrichment)")

	// — Resolve: every rule, plus the never-guess error —
	if rb, err := reg.Resolve("d75884"); err != nil || rb.MAC6 != "d75884" {
		h.check(false, fmt.Sprintf("Resolve(\"d75884\"): (%v, %v)", rb, err))
	} else {
		h.check(true, "Resolve(\"d75884\") — mac6 exact")
	}
	if _, err := reg.Resolve("d7"); err != nil {
		h.check(false, fmt.Sprintf("Resolve(\"d7\") unique prefix: %v", err))
	} else {
		h.check(true, "Resolve(\"d7\") — mac6 unique prefix")
	}
	if _, err := reg.Resolve("bench"); err != nil {
		h.check(false, fmt.Sprintf("Resolve(\"bench\") alias: %v", err))
	} else {
		h.check(true, "Resolve(\"bench\") — boards.conf alias")
	}
	if _, err := reg.Resolve("xuansi-d7"); err != nil {
		h.check(false, fmt.Sprintf("Resolve(\"xuansi-d7\") hostname prefix: %v", err))
	} else {
		h.check(true, "Resolve(\"xuansi-d7\") — hostname prefix")
	}
	_, rerr := reg.Resolve("zzzz")
	h.check(rerr != nil && strings.Contains(rerr.Error(), "d75884") && strings.Contains(rerr.Error(), "bench"),
		fmt.Sprintf("Resolve(\"zzzz\") errors listing candidates (never guess): %v", rerr))
	h.note("ambiguity errors need ≥2 boards — single-board bench; unit-covered (TestResolveAmbiguousListsCandidates)")

	// ——— lazy enrich: dial via the registry, observer hello, record ———
	eb, err := reg.Resolve("d75884")
	if err != nil {
		h.check(false, "enrich resolve: "+err.Error())
		return h.finish("discover")
	}
	ec, err := discover.DialBoard(eb, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "DialBoard: "+err.Error())
		return h.finish("discover")
	}
	hi, err := ec.Hello(true, "b5t9-enrich") // observer: enrichment must never steal the controller slot (§12)
	if err != nil {
		h.check(false, "enrich hello: "+err.Error())
		ec.Close()
		return h.finish("discover")
	}
	if !bindHello(h, "enrich", hi) {
		ec.Close()
		return h.finish("discover")
	}
	h.check(hi.Role == "observer", fmt.Sprintf("enrich hello role=%s (observer requested and granted)", hi.Role))
	reg.Enrich("d75884", hi)
	ec.Close()
	b2 := reg.Snapshot()[0]
	h.check(b2.Hello != nil && b2.Hello.FwBuild == expectFwBuild && b2.Hello.ProtocolVer == expectProtocolVer,
		fmt.Sprintf("Enrich recorded: %s", boardLine(b2)))

	// ——— Phase B: wifi.off from a USB controller (T8 pattern) ———
	bc, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "phase B dial: "+err.Error())
		return h.finish("discover")
	}
	binfo, err := bc.Hello(false, "b5t9-wifioff")
	if err != nil || !bindHello(h, "wifioff", binfo) {
		if err != nil {
			h.check(false, "phase B hello: "+err.Error())
		}
		bc.Close()
		return h.finish("discover")
	}
	h.check(settleNetUp(h, bc, "phase B"), "wifi up before wifi.off (fresh reboot association)")

	res, err := bc.Op(ctx, "wifi.off", nil)
	h.check(err == nil, fmt.Sprintf("wifi.off from USB controller (err=%v)", err))
	h.note("wifi.off reply: %v (NVS creds kept — §14)", res)

	err = reg.Refresh(ctx)
	h.check(err == nil && reg.MDNSError() == "", fmt.Sprintf("Refresh after wifi.off (err=%v, mdnsErr=%q)", err, reg.MDNSError()))
	snapB := reg.Snapshot()
	for _, sb := range snapB {
		fmt.Printf("  board: %s\n", boardLine(sb))
	}
	h.check(len(snapB) == 1, fmt.Sprintf("still exactly one entry (got %d)", len(snapB)))
	if len(snapB) == 1 {
		b3 := snapB[0]
		h.check(strings.Join(b3.Sources, "+") == "usb",
			fmt.Sprintf("mdns source downgraded away (sources=%v) — service withdrawn at net-down (§14)", b3.Sources))
		h.check(b3.USBPath == usbByID, "usb face still live")
		h.check(b3.Host == "xuansi-d75884.local" && b3.Port == 7777 && b3.TXT["ver"] == "3",
			fmt.Sprintf("last-known mdns face kept (host=%s port=%d txt=%v) — honest stale-marking, no hard delete", b3.Host, b3.Port, b3.TXT))
		h.check(b3.Hello != nil && b3.Hello.FwBuild == expectFwBuild, "enriched identity survived the downgrade")
	}

	// ——— Phase C: restore ———
	bc.Close() // next CDC open reboots the board → NVS auto-connect (§14)
	rc, err := replclient.DialUSB(usbByID, replclient.DialOpts{Log: dialLog})
	if err != nil {
		h.check(false, "restore dial: "+err.Error())
		return h.finish("discover")
	}
	defer rc.Close()
	rinfo, err := rc.Hello(false, "b5t9-restore")
	if err != nil || !bindHello(h, "restore", rinfo) {
		if err != nil {
			h.check(false, "restore hello: "+err.Error())
		}
		return h.finish("discover")
	}
	up := settleNetUp(h, rc, "phase C")
	if !up {
		// bounded retry parked in fail (§14): explicit wifi.setup restore.
		// Creds from the git-ignored file; the password is never printed
		// or logged, and wifi.setup's reply contains no pass (§14 table).
		path := credsPath
		if path == "" {
			path = defaultCreds
		}
		ssid, pass, ok := loadCreds(path)
		if !ok {
			h.check(false, "STA parked in fail and creds unavailable at "+path)
			return h.finish("discover")
		}
		h.note("bounded retry parked in fail — explicit wifi.setup restore (creds file %s, pass never logged)", path)
		res, err := rc.Op(ctx, "wifi.setup", map[string]any{"ssid": ssid, "pass": pass})
		h.check(err == nil, fmt.Sprintf("wifi.setup (reply carries no pass): state=%v ssid=%v err=%v", res["state"], res["ssid"], err))
		up = settleNetUp(h, rc, "phase C after wifi.setup")
	}
	h.check(up, "wifi restored (auto-reconnect after reboot; explicit setup only if parked)")
	time.Sleep(time.Second) // grace: mDNS registers right after net-up

	err = reg.Refresh(ctx)
	h.check(err == nil && reg.MDNSError() == "", fmt.Sprintf("final Refresh (err=%v, mdnsErr=%q)", err, reg.MDNSError()))
	snapC := reg.Snapshot()
	for _, sc := range snapC {
		fmt.Printf("  board: %s\n", boardLine(sc))
	}
	h.check(len(snapC) == 1 && strings.Join(snapC[0].Sources, "+") == "usb+mdns",
		fmt.Sprintf("dual-source restored after wifi recovery (sources=%v)", snapC[0].Sources))

	return h.finish("discover")
}
