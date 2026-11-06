// b9-camera.go - batch 9 task 5 HIL: the camera face END TO END on the
// real bench board - MCP xuansi_camera_shot through the WHOLE pipeline
// (the v2-wave1 acceptance face: one tool call, a host-landed openable
// JPEG), landed-file verification (SOI/EOI + Go image/jpeg decode
// dimensions + sha256/bytes against the reply), the refusal matrix
// (no-lease, bad framesize/frames, the fw observer vector), the warm
// path, and the bench left as found (camera off, its 14 pins released).
//
// Three real process layers (b6/b8 house style): go-sdk MCP client over
// os.Pipe <-> `xuansi mcp` <-> `xuansi serve` <-> the real board over
// TCP (xuansi-d75884.local:7777; the CDC port is NEVER opened by the
// daemon - that reboots the board; the re-flash and the fw observer
// vector are the only deliberate USB/extra-connection touches, both
// labeled). XDG-sandboxed so the real user workspace is untouched; the
// landed JPEGs STAY in the sandbox run dir after the capture (the
// controller visually verifies color byte-order from them).
//
// Steps:
//
//	S0  prelude - no stale processes, board TCP reachable, XDG sandbox,
//	    serve up + sweep, mcp up + initialize, xuansi_camera_shot
//	    served (21 static tools), list_boards binds fw c366c0a, the
//	    board starts clean (no lease)
//	S1  no-lease refusal - xuansi_camera_shot BEFORE any lease must
//	    refuse not_held (camera.capture is not ro; Review Focus #5's
//	    MCP leg)
//	S2  first shot, VGA default - lease -> board_op camera.status
//	    {inited:false} -> ONE xuansi_camera_shot (timed: the sensor
//	    power-up + AE settle first-shot latency) -> 1 file, w=640
//	    h=480 sensor GC2145 gaps=0 -> SOI/EOI + decode 640x480 +
//	    sha256 == file bytes -> camera.status now {inited:true}
//	S3  warm burst - frames=2 -> two distinct files, both verify,
//	    wall time noted against S2's first-shot latency
//	S4  QVGA variant - framesize QVGA -> decode 320x240 (the size
//	    switch restart latency noted)
//	S5  input refusals - framesize SVGA -> api error naming QVGA and
//	    VGA; frames=5 -> api error naming 1..4; neither touched the
//	    board (no capture in the daemon log)
//	S6  warm repeat - back to VGA default (the second size switch),
//	    verify again - the repeat-shot warm path
//	S7  fw observer vector (Review Focus #5's board leg, deliberately
//	    OUTSIDE the MCP pipeline - the b7 out-of-pipeline precedent):
//	    a direct replclient TCP connection hellos observer and issues
//	    camera.capture -> the fw's controller-gate refusal (class
//	    protocol), the daemon's warm controller untouched
//	S8  camera off + pins - board_op camera.off -> camera.status
//	    {inited:false} -> diag.pins (stdout capture cap-1): none of
//	    the camera's 14 pins held
//	S9  as-found + teardown - release -> no lease -> mcp EOF exit 0,
//	    serve SIGINT exit 0, sock removed, sandbox accounting with the
//	    FULL landed-JPEG inventory (path + bytes + sha256 + dims) left
//	    in place for the visual color-order verification
//
// Single capture, numbered PASS/FAIL per check, ASCII only (the
// batch-8/9 evidence discipline).
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"image/jpeg"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"xuansi/internal/replclient"
)

const (
	b9Board   = "d75884"  // the bench board (GC2145 camera on the 14-pin gate)
	b9FwBuild = "c366c0a" // B9-T4 canonical re-flash: fw repo main HEAD, protocol_ver 4
	b9Holder  = "b9-t5"   // custom lease holder for this run
	b9TCPEndp = "xuansi-d75884.local:7777"
)

// b9CameraPins is the camera's 14-pin occupancy (the board wiring
// table): the as-found check wants every one of them absent from a
// diag.pins dump after camera.off.
var b9CameraPins = []string{
	"GPIO4", "GPIO5", "GPIO6", "GPIO7", "GPIO8", "GPIO9", "GPIO10",
	"GPIO11", "GPIO12", "GPIO13", "GPIO15", "GPIO16", "GPIO17", "GPIO18",
}

// b9VerifyJPEG checks one landed file against its reply record and the
// wanted geometry: SOI (FFD8) head / EOI (FFD9) tail, a real Go
// image/jpeg decode to wantW x wantH, byte count > 0 and == the
// reply's bytes, and sha256(body) == the reply's sha256. Returns
// whether everything held.
func b9VerifyJPEG(h *harness, fm map[string]any, wantW, wantH int) bool {
	path := b6Str(fm, "path")
	body, rerr := os.ReadFile(path)
	if rerr != nil {
		h.check(false, fmt.Sprintf("landed file readable from THIS HIL process (%s: %v)", path, rerr))
		return false
	}
	ok := true
	ok = h.check(len(body) > 2 && body[0] == 0xFF && body[1] == 0xD8,
		fmt.Sprintf("%s SOI marker FFD8 in place (head % X)", filepath.Base(path), body[:2])) && ok
	ok = h.check(body[len(body)-2] == 0xFF && body[len(body)-1] == 0xD9,
		fmt.Sprintf("%s EOI marker FFD9 in place (tail % X)", filepath.Base(path), body[len(body)-2:])) && ok
	cfg, derr := jpeg.DecodeConfig(bytes.NewReader(body))
	ok = h.check(derr == nil && cfg.Width == wantW && cfg.Height == wantH,
		fmt.Sprintf("%s decodes via Go image/jpeg to %dx%d, want %dx%d (err=%v)",
			filepath.Base(path), cfg.Width, cfg.Height, wantW, wantH, derr)) && ok
	wantSHA := fmt.Sprintf("%x", sha256.Sum256(body))
	gotSHA := b6Str(fm, "sha256")
	wantBytes, _ := b6Num(fm, "bytes")
	shortSHA := gotSHA
	if len(shortSHA) > 12 {
		shortSHA = shortSHA[:12]
	}
	ok = h.check(len(body) > 0 && int64(len(body)) == int64(wantBytes) && wantSHA == gotSHA,
		fmt.Sprintf("%s sha256/bytes reconcile with the reply (%dB sha %s..)", filepath.Base(path), len(body), shortSHA)) && ok
	return ok
}

func runB9Camera(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== b9 task-5 HIL: the camera face - MCP xuansi_camera_shot end to end - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("board: %s over TCP %s (daemon never opens CDC - that reboots); fw expect %s (protocol_ver 4)\n", b9Board, b9TCPEndp, b9FwBuild)
	fmt.Printf("discipline: evidence ASCII only; the landed JPEGs STAY under the sandbox run dir for visual verification\n")

	var serve, mcp *b6Proc
	defer func() { // die-safe: never leave processes or a lease behind
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int {
		code := h.finish("b9-t5")
		if h.fail == 0 {
			fmt.Printf("=== ALL PASS - one MCP call lands openable JPEGs on the host: first-shot + warm + QVGA + burst verified, refusals honest, board as found (attempt %d, S0..S9) ===\n", attempt)
		}
		return code
	}

	// ---------- S0: prelude ----------
	fmt.Println("\n- S0: prelude - clean bench, board reachable, sandbox, serve+mcp up, tool served -")
	if out, err := exec.Command("pgrep", "-af", "xuansi").CombinedOutput(); err == nil {
		stale := 0
		for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Fields(ln)
			if len(f) >= 2 && strings.Contains(filepath.Base(f[0]), "xuansi") && (f[1] == "serve" || f[1] == "mcp") {
				stale++
				fmt.Printf("  [note] stale xuansi process: %s\n", asciiLine(ln))
			}
		}
		h.check(stale == 0, fmt.Sprintf("no stale xuansi serve/mcp processes on the bench (found %d)", stale))
	} else {
		h.note("pgrep: no xuansi processes found (clean bench)")
	}
	if conn, err := net.DialTimeout("tcp", b9TCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v (USB fallback OUT of scope - opening it reboots the board)", b9TCPEndp, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only - no reboot, no USB open)", b9TCPEndp))
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b9-t5 boards.conf: no aliases - the mac6 selector is used directly\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}
	h.note("XDG sandbox: XDG_DATA_HOME=%s (the JPEGs land inside; the real user workspace untouched)", xdg)

	serve = startB6Daemon(h, daemonBin, sock, boardsCfg, "XDG_DATA_HOME="+xdg)
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
	h.check(ok, fmt.Sprintf("daemon sock bound at %s (%s)", sock, last))
	h.check(serve.has("daemon serving"), fmt.Sprintf("daemon reports serving (grace-ttl %s)", b6GraceTTL))
	ok, last = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return serve.has("initial discovery sweep: 1 board(s)"), "sweep line"
	})
	h.check(ok, fmt.Sprintf("initial discovery sweep found 1 board (%s)", last))

	mcp, mcpR, mcpW := startB6MCP(h, daemonBin, sock, "XDG_DATA_HOME="+xdg)
	if mcp == nil {
		return finish()
	}
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b9-hil", Version: "1.0"},
		&mcpsdk.ClientOptions{
			ResourceUpdatedHandler: func(_ context.Context, _ *mcpsdk.ResourceUpdatedNotificationRequest) {},
			ToolListChangedHandler: func(context.Context, *mcpsdk.ToolListChangedRequest) {},
		})
	b := &b6Client{h: h}
	cs, cerr := cl.Connect(ctx, &mcpsdk.IOTransport{Reader: mcpR, Writer: mcpW}, nil)
	if cerr != nil {
		h.check(false, "MCP client initialize over the child's stdio pipes: "+cerr.Error())
		return finish()
	}
	b.cs = cs
	si := cs.InitializeResult()
	h.check(si != nil && si.ServerInfo.Name == "xuansi",
		fmt.Sprintf("initialize handshake ok: server=%s version=%s (protocol %s)", initField(si, "name"), initField(si, "ver"), initField(si, "proto")))

	names, terr := b.toolNames(ctx)
	servedCam := false
	for _, n := range names {
		if n == "xuansi_camera_shot" {
			servedCam = true
		}
	}
	h.check(terr == nil && servedCam,
		fmt.Sprintf("static face serves xuansi_camera_shot (served=%v of %d tools)", servedCam, len(names)))

	ok, last = pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		m, _, isErr, ok := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
		if !ok || isErr {
			return false, "list_boards trouble"
		}
		for _, e := range m["boards"].([]any) {
			if bm, yes := e.(map[string]any); yes && bm["mac6"] == b9Board {
				if fw, _ := bm["fw"].(string); fw != "" {
					return true, "fw populated (hello-enriched)"
				}
				return false, "board listed, fw not yet enriched"
			}
		}
		return false, "board not in list yet"
	})
	h.check(ok, fmt.Sprintf("list_boards shows %s with fw populated (%s)", b9Board, last))
	m, text, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
	fw := ""
	for _, e := range m["boards"].([]any) {
		if bm, _ := e.(map[string]any); bm["mac6"] == b9Board {
			fw, _ = bm["fw"].(string)
		}
	}
	h.check(!isErr && fw == b9FwBuild, fmt.Sprintf("fw BUILD BINDING: %q == expected %q (B9-T4 canonical; text=%.60s)", fw, b9FwBuild, text))

	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b9Board})
	_, hasLease := m["lease"]
	h.check(!isErr && !hasLease, fmt.Sprintf("precondition: board starts with NO lease (lease key %v; text=%.50s)", hasLease, text))

	// ---------- S1: no-lease refusal ----------
	fmt.Println("\n- S1: xuansi_camera_shot WITHOUT the lease -> refused (camera.capture is not ro) -")
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_camera_shot", map[string]any{"board": b9Board})
	h.check(isErr && strings.HasPrefix(text, "not_held:"),
		fmt.Sprintf("no-lease camera_shot refused not_held (text=%.80s)", text))

	// ---------- S2: first shot, VGA default ----------
	fmt.Println("\n- S2: lease -> camera.status {inited:false} -> FIRST xuansi_camera_shot (VGA default, timed) -> verify -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b9Board, "holder": b9Holder})
	leaseSess, _ := b6Num(m, "session")
	h.check(!isErr && b6Str(m, "board") == b9Board && b6Str(m, "holder") == b9Holder && leaseSess > 0,
		fmt.Sprintf("lease holder=%s session=%.0f (default TCP route)", b9Holder, leaseSess))
	h.check(b6Str(m, "transport") == "" && b6Str(m, "warning") == "",
		fmt.Sprintf("lease took the DEFAULT TCP route: transport=%q warning=%q (no reboot)", b6Str(m, "transport"), b6Str(m, "warning")))

	shotN := 0
	shot := func(label string, args map[string]any, wantW, wantH, wantFiles int) bool {
		shotN++
		t0 := time.Now()
		m, text, isErr, _ := b.call(ctx, 40*time.Second, "xuansi_camera_shot", args)
		dur := time.Since(t0).Round(time.Millisecond)
		if isErr {
			h.check(false, fmt.Sprintf("%s: xuansi_camera_shot failed (text=%.120s)", label, text))
			return false
		}
		files, _ := m["files"].([]any)
		all := h.check(len(files) == wantFiles,
			fmt.Sprintf("%s: %d file(s) in the reply, want %d (gaps=%v sensor=%v)", label, len(files), wantFiles, gAny(m, "gaps"), gAny(m, "sensor"))) && h.check(b6Str(m, "sensor") == "GC2145", fmt.Sprintf("%s: sensor == GC2145 (%v)", label, gAny(m, "sensor")))
		for _, fe := range files {
			fm, _ := fe.(map[string]any)
			all = b9VerifyJPEG(h, fm, wantW, wantH) && all
		}
		h.note("%s wall time: %v (shot #%d)", label, dur, shotN)
		return all
	}

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b9Board, "op": "camera.status"})
	h.check(!isErr && !bolAny(m, "inited"),
		fmt.Sprintf("board_op camera.status BEFORE any shot: {inited:%v} - the lazy engine is cold (text=%.60s)", bolAny(m, "inited"), text))

	shot("S2 first shot (VGA default)", map[string]any{"board": b9Board}, 640, 480, 1)

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b9Board, "op": "camera.status"})
	h.check(!isErr && bolAny(m, "inited"),
		fmt.Sprintf("board_op camera.status AFTER the shot: {inited:%v sensor:%v} - the lazy engine warmed (text=%.60s)", bolAny(m, "inited"), gAny(m, "sensor"), text))

	// ---------- S3: warm burst ----------
	fmt.Println("\n- S3: frames=2 warm burst -> two distinct files, both verify -")
	shot("S3 warm burst (frames=2)", map[string]any{"board": b9Board, "frames": float64(2)}, 640, 480, 2)
	h.note("burst latency per frame vs the S2 first shot above is the warm-path evidence (encode ~0.65s/frame vs the ~3s cold start)")

	// ---------- S4: QVGA variant ----------
	fmt.Println("\n- S4: framesize QVGA -> decode 320x240 (the size switch restart rides the latency) -")
	shot("S4 QVGA", map[string]any{"board": b9Board, "framesize": "QVGA"}, 320, 240, 1)

	// ---------- S5: input refusals ----------
	fmt.Println("\n- S5: bad framesize / frames -> api refusals BEFORE any board contact -")
	landingsBefore := serve.count("camera_shot " + b9Board + ":")
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_camera_shot",
		map[string]any{"board": b9Board, "framesize": "SVGA"})
	h.check(isErr && strings.HasPrefix(text, "api:") && strings.Contains(text, "QVGA") && strings.Contains(text, "VGA"),
		fmt.Sprintf("framesize SVGA refused api naming QVGA and VGA (text=%.100s)", text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_camera_shot",
		map[string]any{"board": b9Board, "frames": float64(5)})
	h.check(isErr && strings.HasPrefix(text, "api:") && strings.Contains(text, "1..4"),
		fmt.Sprintf("frames=5 refused api naming the 1..4 range (text=%.100s)", text))
	h.check(serve.count("camera_shot "+b9Board+":") == landingsBefore,
		fmt.Sprintf("neither refusal produced a capture (daemon landing lines before=%d after=%d)",
			landingsBefore, serve.count("camera_shot "+b9Board+":")))

	// ---------- S6: warm repeat (back to VGA) ----------
	fmt.Println("\n- S6: warm repeat - VGA default again (second size switch) -> verify -")
	shot("S6 warm repeat (VGA default)", map[string]any{"board": b9Board}, 640, 480, 1)

	// ---------- S7: the fw observer vector ----------
	fmt.Println("\n- S7: fw observer vector - a DIRECT observer connection's camera.capture is refused (out-of-pipeline, labeled) -")
	oc, oerr := replclient.DialTCP(b9TCPEndp, replclient.DialOpts{})
	if oerr != nil {
		h.check(false, fmt.Sprintf("observer dial %s: %v", b9TCPEndp, oerr))
	} else {
		oinfo, herr := oc.Hello(true, "b9-t5-observer")
		if herr != nil {
			h.check(false, fmt.Sprintf("observer hello: %v", herr))
		} else {
			h.check(oinfo.Role == "observer",
				fmt.Sprintf("direct TCP hello as observer granted role=%s (fw=%s ver=%d - the daemon's controller untouched)", oinfo.Role, oinfo.FwBuild, oinfo.ProtocolVer))
			_, oerr := oc.Op(ctx, "camera.capture", map[string]any{})
			var oe *replclient.OpError
			if errors.As(oerr, &oe) {
				h.check(oe.Class == "protocol",
					fmt.Sprintf("observer camera.capture REFUSED by the fw gate: class=%s msg=%.80s (controller-only op; the MCP leg of this matrix was S1's not_held)", oe.Class, oe.Msg))
			} else {
				h.check(false, fmt.Sprintf("observer camera.capture err = %v, want the fw's protocol-class refusal", oerr))
			}
		}
		oc.Close()
	}

	// ---------- S8: camera off + pins ----------
	fmt.Println("\n- S8: board_op camera.off -> status cold -> diag.pins: the 14 camera pins all free -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b9Board, "op": "camera.off"})
	h.check(!isErr, fmt.Sprintf("board_op camera.off through the allowlist (text=%.60s)", text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b9Board, "op": "camera.status"})
	h.check(!isErr && !bolAny(m, "inited"),
		fmt.Sprintf("camera.status after off: {inited:%v} - the engine is cold again (text=%.60s)", bolAny(m, "inited"), text))

	capCh := b.callAsync("xuansi_capture", map[string]any{"board": b9Board, "source": "stdout", "seconds": float64(4)})
	time.Sleep(700 * time.Millisecond) // let the subscription settle
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b9Board, "op": "diag.pins"})
	h.check(!isErr, fmt.Sprintf("board_op diag.pins (dump lands on stdout into the capture; text=%.40s)", text))
	cres := <-capCh
	h.check(cres.ok && !cres.isErr,
		fmt.Sprintf("stdout capture returned {path=%v bytes=%v reason=%v}", gAny(cres.m, "path"), gAny(cres.m, "bytes"), b6Str(cres.m, "reason")))
	capData, rdErr := os.ReadFile(b6Str(cres.m, "path"))
	h.check(rdErr == nil, fmt.Sprintf("pins dump capture readable (%d bytes)", len(capData)))
	var busy []string
	cont := asciiLine(string(capData))
	for _, pin := range b9CameraPins {
		if strings.Contains(cont, pin) {
			busy = append(busy, pin)
		}
	}
	h.check(len(busy) == 0,
		fmt.Sprintf("diag.pins after camera.off: none of the 14 camera pins held (busy=%v)", busy))

	// ---------- S9: as-found + teardown ----------
	fmt.Println("\n- S9: release -> no lease -> mcp EOF -> serve SIGINT -> sandbox accounting (JPEGs LEFT IN PLACE) -")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release", map[string]any{"board": b9Board, "holder": b9Holder})
	h.check(!isErr && b6Str(m, "board") == b9Board && b6Str(m, "holder") == b9Holder,
		fmt.Sprintf("release the lease (text=%.60s)", text))
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b9Board})
	_, hasLease2 := m["lease"]
	h.check(!isErr && !hasLease2, fmt.Sprintf("post-release board_status shows NO lease (lease key %v)", hasLease2))

	if err := cs.Close(); err != nil {
		h.note("client session close: %v (proceeding to EOF wait)", err)
	}
	select {
	case <-mcp.exited:
		code, werr := mcp.exitStats()
		h.check(code == 0 && werr == nil,
			fmt.Sprintf("xuansi mcp exited clean on stdin EOF (code=%d waitErr=%v)", code, werr))
	case <-time.After(8 * time.Second):
		h.check(false, "xuansi mcp did not exit within 8s of the client close (EOF on stdin)")
		mcp.kill("mcp EOF timeout")
	}
	if !serve.sigintAndWait(h, 20*time.Second) {
		return finish()
	}
	code, werr := serve.exitStats()
	h.check(code == 0 && werr == nil, fmt.Sprintf("daemon exited clean (code=%d waitErr=%v)", code, werr))
	h.check(serve.shutdownDone(), "daemon's own final log line: 'shutdown complete' (no goroutine-leak complaint)")
	_, serr := os.Stat(sock)
	h.check(os.IsNotExist(serr), fmt.Sprintf("sock file removed at exit (stat err=%v)", serr))

	// the landed-JPEG inventory - LEFT IN PLACE for the visual color-order
	// verification (the controller reads them from the run dir)
	capsDir := filepath.Join(xdg, "xuansi", "captures")
	entries, _ := os.ReadDir(capsDir)
	jpegs := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".jpg") {
			continue
		}
		jpegs++
		p := filepath.Join(capsDir, e.Name())
		body, rerr := os.ReadFile(p)
		if rerr != nil {
			h.note("  JPEG %s: unreadable (%v)", e.Name(), rerr)
			continue
		}
		cfg, derr := jpeg.DecodeConfig(bytes.NewReader(body))
		dims := fmt.Sprintf("%dx%d", cfg.Width, cfg.Height)
		if derr != nil {
			dims = "decode-error"
		}
		h.note("  LANDED JPEG (left in place): %s  bytes=%d  sha256=%x  dims=%s", p, len(body), sha256.Sum256(body), dims)
	}
	h.check(jpegs == 5, fmt.Sprintf("sandbox holds exactly the five landed JPEGs of S2/S3(x2)/S4/S6 (found %d)", jpegs))
	h.note("visual verification handoff: the controller checks color byte-order (R/B) on these files; the fw's jpgSetRgb565BE() knob is NOT touched here")
	return finish()
}

// bolAny pulls a bool from a decoded JSON map.
func bolAny(m map[string]any, k string) bool {
	v, _ := m[k].(bool)
	return v
}
