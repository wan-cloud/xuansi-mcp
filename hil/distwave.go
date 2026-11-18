package main

// distwave.go — the distribution-unification wave's HIL (Task 9,
// plan 2026-11-18): four legs against the REAL distribution faces and
// the REAL boards.
//
//	S0  prelude: clean bench, board #2 (97c1ac, the LA platform board —
//	    the sacrificial one; board #1 stays product-state zero-touch)
//	    reachable, sandboxed HEAD serve (--fw-poll armed) + mcp child +
//	    initialize + tools/list == 57 with the fw pair (the seat pin)
//	S1  H1 drivers v2 over the REAL release CDN: driver_fetch
//	    (ledwatch@3.0.0, the DEFAULT entry — releases/latest/download)
//	    lands both artifacts sha-verified; module_doc answers from the
//	    release's meta_url (source=github, ops table non-empty)
//	S2  H2 the fw chain against the rc release (pinned source_url +
//	    version 0.7.1-rc.1 — the ④b prerelease machinery):
//	    fw_versions shows 97c1ac behind + the protocol-11 compat
//	    WARNING (design: warn, never block) → fw_fetch downloads
//	    sha-pinned (second call skips) → ota_push pushes/reboots/verifies
//	    → fw_versions re-check: up_to_date TRUE; board.reboot event
//	    carries reason "ota". NOTE (honest): the D9 poller's DEFAULT
//	    entry 404s on the prerelease-only repo (by design — prereleases
//	    never take over latest), so the positive fw_update annotation
//	    is unit-pinned, not seat-observable until the canonical 0.7.1.
//	    Board #2 ENDS ON the rc image (a true-tree 0.7.1-train build,
//	    fw_build ec60abf): reverting needs the canonical build, which
//	    is the wave-close's own step — ledgered deviation.
//	S3  H4 toolchain live leg: toolchain_status against the NEW default
//	    entry (xuansi-toolchain latest/download) — generation v3,
//	    pieces present (the cache's sha-pinned state rides).
//
// Evidence discipline: ASCII only, verdicts in-file, honest attempts.

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	dwBoard    = "97c1ac"
	dwTCPEndp  = "xuansi-97c1ac.local:7777"
	dwHolder   = "distwave-hil"
	dwRoot     = "https://github.com/wan-cloud/xuansi-fw-images/releases"
	dwVersion  = "0.7.1-rc.1"
	dwFwBuild  = "ec60abf" // the rc tag's commit (the canonical anchor)
	dwOtaSHA   = "8ef0ba2301659f63db79be6b68f614c9cfe20e6e376acf3aac40bddff5220408"
	dwProtocol = 11        // the mid-train tree's face — the compat warning's trigger
)

// startDWServe launches the daemon with --fw-poll armed (startB6Daemon
// has no extra-args seam; this is its --fw-poll twin).
func startDWServe(h *harness, bin, sock, boardsFile, xdg string, fwPoll string) *b6Proc {
	cmd := exec.Command(bin, "serve", "--sock", sock, "--grace-ttl", b6GraceTTL.String(),
		"--boards-file", boardsFile, "--fw-poll", fwPoll)
	cmd.Env = append(os.Environ(), xdg)
	return startPumped(h, cmd, "daemon", xdg)
}

func runDistWave(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== dist-wave HIL: drivers v2 CDN + fw rc chain + toolchain new entry - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("serving daemon: %s (freshly built at HEAD by the orchestrating script)\n", daemonBin)
	fmt.Printf("board: %s over TCP %s (the LA platform board - board #1 product-state stays zero-touch)\n", dwBoard, dwTCPEndp)
	fmt.Printf("fw target: %s@%s pinned %s/%s (the 4b prerelease machinery)\n", dwVersion, dwFwBuild, dwRoot, dwVersion)

	var serve, mcp *b6Proc
	defer func() {
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int {
		code := h.finish("distwave")
		if h.fail == 0 {
			fmt.Printf("=== ALL PASS - distribution faces live: drivers v2 CDN, the fw rc chain end-to-end, the toolchain new entry (attempt %d) ===\n", attempt)
		}
		return code
	}

	// ---------- S0: prelude ----------
	fmt.Println("\n- S0: prelude - clean bench, board reachable, HEAD serve (--fw-poll) + mcp + seat pin -")
	staleServe(h)
	if conn, err := net.DialTimeout("tcp", dwTCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v", dwTCPEndp, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, "board TCP face reachable at "+dwTCPEndp+" (dial only)")
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# dist-wave boards.conf: no aliases - mac6 selector\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}

	serve = startDWServe(h, daemonBin, sock, boardsCfg, "XDG_DATA_HOME="+xdg, "1s")
	if serve == nil {
		return finish()
	}
	ok, last := pollUntil(300*time.Millisecond, 15*time.Second, func() (bool, string) {
		if c, err := net.Dial("unix", sock); err == nil {
			c.Close()
			return true, "sock dialable"
		}
		return false, "sock not bound yet"
	})
	h.check(ok, "HEAD daemon sock bound ("+last+")")
	ok, _ = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return serve.has("startup audit board " + dwBoard), "audit line"
	})
	h.check(ok, "startup audit tapped the board (first sighting)")

	mcp, mcpR, mcpW := startB6MCP(h, daemonBin, sock, "XDG_DATA_HOME="+xdg)
	if mcp == nil {
		return finish()
	}
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "distwave-hil", Version: "1.0"},
		&mcpsdk.ClientOptions{
			ResourceUpdatedHandler: func(_ context.Context, _ *mcpsdk.ResourceUpdatedNotificationRequest) {},
			ToolListChangedHandler: func(context.Context, *mcpsdk.ToolListChangedRequest) {},
		})
	b := &b6Client{h: h}
	cs, cerr := cl.Connect(ctx, &mcpsdk.IOTransport{Reader: mcpR, Writer: mcpW}, nil)
	if cerr != nil {
		h.check(false, "MCP client initialize: "+cerr.Error())
		return finish()
	}
	b.cs = cs
	si := cs.InitializeResult()
	h.check(si != nil && si.ServerInfo.Name == "xuansi",
		fmt.Sprintf("initialize handshake ok: server=%s version=%s", initField(si, "name"), initField(si, "ver")))

	lr, lerr := cs.ListTools(ctx, &mcpsdk.ListToolsParams{})
	names := map[string]bool{}
	if lerr == nil {
		for _, t := range lr.Tools {
			names[t.Name] = true
		}
	}
	h.check(lerr == nil && len(lr.Tools) == 57,
		fmt.Sprintf("tools/list serves %d static tools, want 57 (the fw pair on top of 55)", len(lr.Tools)))
	h.check(names["xuansi_fw_versions"] && names["xuansi_fw_fetch"] && names["xuansi_ota_push"],
		"the seat carries the fw pair + ota_push (the chain)")

	// ---------- S1: H1 drivers v2 over the real CDN ----------
	fmt.Println("\n- S1: H1 - driver_fetch over the REAL release index (default entry) + module_doc via meta_url -")
	m, text, isErr, _ := b.call(ctx, 60*time.Second, "xuansi_driver_fetch",
		map[string]any{"name": "ledwatch", "version": "3.0.0"})
	files, _ := m["files"].([]any)
	h.check(!isErr && len(files) == 2,
		fmt.Sprintf("W10 driver_fetch ledwatch@3.0.0 over releases/latest/download: %d files (text=%.80s)", len(files), text))
	if !isErr && m["kind"] == "production" {
		h.check(true, "W11 kind crosses verbatim: production")
	} else {
		h.check(false, fmt.Sprintf("W11 kind = %v", m["kind"]))
	}
	// the github leg needs a module NOT in the workspace (W12's
	// attempt-1 lesson: workspace-first is by design — ledwatch was
	// just fetched)
	m2, text2, isErr2, _ := b.call(ctx, 60*time.Second, "xuansi_module_doc",
		map[string]any{"name": "jpge", "version": "2.0.0"})
	ops2, _ := m2["ops"].([]any)
	h.check(!isErr2 && m2["source"] == "github" && len(ops2) > 0,
		fmt.Sprintf("W12 module_doc answers from the release's meta_url: source=%v ops=%d (text=%.60s)", m2["source"], len(ops2), text2))

	// ---------- S2: H2 the fw rc chain ----------
	fmt.Println("\n- S2: H2 - fw_versions (behind + compat warning) -> fw_fetch (sha-pinned, skip) -> ota_push -> re-verify -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_fw_versions",
		map[string]any{"source_url": dwRoot})
	latest, _ := m["latest"].(map[string]any)
	warn, _ := m["warnings"].([]any)
	behind := false
	if boards, _ := m["boards"].([]any); boards != nil {
		for _, be := range boards {
			if bm, _ := be.(map[string]any); bm["board"] == dwBoard && bm["up_to_date"] == false {
				behind = true
			}
		}
	}
	h.check(!isErr && latest != nil && latest["fw_version"] == dwVersion && latest["fw_build"] == dwFwBuild+strings.Repeat("x", 0),
		fmt.Sprintf("W20 fw_versions latest pins the rc identity: %v@%v (text=%.60s)", latestV(latest), latestB(latest), text))
	h.check(behind, fmt.Sprintf("W21 board %s reads BEHIND (up_to_date=false, build-equality rule)", dwBoard))
	h.check(len(warn) >= 1 && strings.Contains(fmt.Sprint(warn), "protocol"),
		fmt.Sprintf("W22 the protocol-%d compat WARNING fires (warn-not-block): %v", dwProtocol, warn))

	m, text, isErr, _ = b.call(ctx, 120*time.Second, "xuansi_fw_fetch",
		map[string]any{"version": dwVersion, "source_url": dwRoot})
	path, _ := m["path"].(string)
	h.check(!isErr && m["downloaded"] == true && strings.HasPrefix(filepath.Base(path), "xuansi_fw-0.7.1-"+dwFwBuild[:7]),
		fmt.Sprintf("W23 fw_fetch downloads the rc sha-pinned: %s (text=%.60s)", path, text))
	m, _, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_fw_fetch",
		map[string]any{"version": dwVersion, "source_url": dwRoot})
	h.check(!isErr && m["downloaded"] == false && m["path"] == path,
		"W24 second fw_fetch skips (the self-identifying cache name)")

	// the push: real transport + activate + return + identity triple
	m, text, isErr, _ = b.call(ctx, 180*time.Second, "xuansi_ota_push",
		map[string]any{"boards": []any{dwBoard}, "path": path, "force": true})
	rows, _ := m["rows"].([]any)
	outcome, detail := "", ""
	if len(rows) == 1 {
		if rm, _ := rows[0].(map[string]any); rm != nil {
			outcome, _ = rm["outcome"].(string)
			detail, _ = rm["detail"].(string)
		}
	}
	h.check(!isErr && outcome == "pushed",
		fmt.Sprintf("W25 ota_push outcome=%s detail=%.90s (text=%.60s)", outcome, detail, text))

	// re-verify: up_to_date TRUE + the ota reboot event
	ok, _ = pollUntil(1*time.Second, 30*time.Second, func() (bool, string) {
		m, _, isErr, _ := b.call(ctx, 60*time.Second, "xuansi_fw_versions", map[string]any{"source_url": dwRoot})
		if isErr {
			return false, "fw_versions error"
		}
		if boards, _ := m["boards"].([]any); boards != nil {
			for _, be := range boards {
				if bm, _ := be.(map[string]any); bm["board"] == dwBoard {
					return bm["up_to_date"] == true, fmt.Sprintf("up_to_date=%v", bm["up_to_date"])
				}
			}
		}
		return false, "board row absent"
	})
	h.check(ok, "W26 post-push fw_versions: the board reads up_to_date TRUE (fw_build equality)")
	m, _, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_events_recent",
		map[string]any{"board": dwBoard, "source_prefix": "board"})
	evText := fmt.Sprint(m["events"])
	h.check(!isErr && strings.Contains(evText, "board.reboot") && strings.Contains(evText, "ota"),
		"W27 the board.reboot event carries reason ota (the agent-visible push aftermath)")

	// ---------- S3: H4 toolchain live leg ----------
	fmt.Println("\n- S3: H4 - toolchain_status against the NEW default entry (xuansi-toolchain) -")
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_toolchain_status", map[string]any{})
	h.check(!isErr && m["generation"] == "toolchain-v3" && fmt.Sprint(m["index_source"]) != "",
		fmt.Sprintf("W30 toolchain_status answers over the new entry: generation=%v (text=%.60s)", m["generation"], text))

	fmt.Println("\n- cleanup - children torn down by defer; board #2 ENDS ON the rc image (ledgered: the canonical 0.7.1 wave-close overwrites it) -")
	return finish()
}

func latestV(latest map[string]any) any {
	if latest == nil {
		return "?"
	}
	return latest["fw_version"]
}

func latestB(latest map[string]any) any {
	if latest == nil {
		return "?"
	}
	return latest["fw_build"]
}
