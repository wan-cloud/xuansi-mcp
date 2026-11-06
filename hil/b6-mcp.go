// b6-mcp.go — task-13 full MCP HIL: the entire batch-6 MCP face on the
// REAL bench board, end to end, through a REAL MCP client — the go-sdk
// client (mcp.NewClient) riding mcp.IOTransport over the stdio pipes of
// an `xuansi mcp` SUBPROCESS, which itself rides the real `xuansi
// serve` daemon over its unix socket. Three honest process layers:
//
//	driver (this file, the MCP client)
//	  ⇅ os.Pipe stdio [MCP JSON-RPC frames]
//	xuansi mcp --sock … (the face under test: 17 static + caps-driven
//	  dynamic tools, event resources, capture, workspace — XDG sandboxed)
//	  ⇅ unix socket [framed JSON]
//	xuansi serve (the real daemon; the real board over TCP)
//
// One connected story, S0..S14 (task-13 brief sequence):
//
//	S0  prelude    — board TCP face reachable (dial only — no USB, no
//	                 reboot), run dir + XDG sandbox prepared, no stale
//	                 xuansi processes on the bench
//	S1  serve up   — real subprocess, sock bound, initial discovery
//	                 sweep found the board
//	S2  mcp up     — real subprocess; SDK client initialize handshake
//	                 over the pipes; serverInfo binds the face
//	S3  discovery  — xuansi_list_boards: d75884 dual-source [usb mdns],
//	                 fw=775ecaa (the build binding), host/port/mdns face
//	S4  events sub — subscribe xuansi://boards/d75884/events (exists
//	                 once the face's refresh listed the board); the
//	                 snapshot reads back as the board's ring
//	S5  lease      — xuansi_lease holder=b6-hil, default TCP route (no
//	                 transport field → no usb, no warning); budget
//	                 prelude through the op face (budget.set eval_ms
//	                 4000 for the 4s-class job below — the daemon API
//	                 now HAS the knob: no driver-direct out-of-band)
//	S6  dynamic    — tools/list: ≥30 d75884_-prefixed tools (caps-
//	                 driven; they appear only after the lease — the ops
//	                 face is lease-gated, so the unleased refresh reads
//	                 not_held and stays static-only by design);
//	                 d75884_gpio_mode(1,"out")+(2,"in") on the WIRED
//	                 pair (pin map v1: GPIO1↔GPIO2, both outside the 18
//	                 banned pins), gpio_write(1,1) → gpio_read(1)=1, a
//	                 direct xuansi_run_lua cross-check
//	                 (bench.gpio.read(1)), then write 0 → read 0
//	S7  args bridge— xuansi_run_lua deep-8 nested table via args→ARGS→
//	                 value: structure deep-equal, `value` key present
//	S8  submit+job — xuansi_submit ~4s job (tag) → ack {tag,queued};
//	                 the job-done event lands through the event face
//	                 (resource-updated notification + ring), tag+value
//	                 asserted via xuansi_events_recent
//	S9  gpio edges — pins claimed in S6; a concurrent xuansi_capture
//	                 (source gpio.2 — the MCP face's board-side arming
//	                 path for per-pin sources, §8 claim precondition met)
//	                 makes the wired edges flow; drive write 1→0 via the
//	                 dynamic tool; the rising+falling pair lands in the
//	                 capture FILE (jsonl) AND the ring (events_recent)
//	                 AND fires a resource-updated notification
//	S10 events     — xuansi_events_recent {board, limit} sanity:
//	                 non-empty, newest-last ordered
//	S11 capture    — xuansi_capture stdout 4s while an eval prints
//	                 distinctive lines; the file lands under the
//	                 daemon's captures root (inside the XDG sandbox),
//	                 readable from THIS process, containing the lines
//	S12 workspace  — ws_put a tiny driver (--@api 1 module) →
//	                 driver_push → run_lua require("bench.drv.b6tiny")
//	                 .ok == true; ws_list active version; files under
//	                 the sandboxed workspace root
//	S13 ops        — board_op budget.set 3000 → budget.get round-trip
//	                 (the translated frozen no-keys getter), caps
//	                 (api_version + the 30-op primitives table), heap +
//	                 wifi snapshot lines for the capture (read-only;
//	                 wifi.setup/off stay OUT of HIL scope — no real
//	                 credentials anywhere; allowlist is unit-covered)
//	S14 teardown   — release → board_status shows no lease; the MCP
//	                 client closes → `xuansi mcp` sees stdin EOF, exits
//	                 0; serve SIGINT → exit 0 + "shutdown complete"
//	                 (the subprocess no-goroutine-leak bar), sock gone
//
// Single capture, numbered PASS/FAIL per check (house style from
// b5full.go); failing runs keep their own transcripts via b6-full.sh.
package main

import (
	"bufio"
	"context"
	"encoding/json"
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

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	b6Board    = "d75884"  // the bench board (pin map v1: GPIO1<->GPIO2 wired)
	b6FwBuild  = "9dce098" // B10-T1 canonical re-flash (ver 5, the chunked-transport era)
	b6Holder   = "b6-hil"  // custom lease holder for this run
	b6TCPEndp  = "xuansi-d75884.local:7777"
	b6Resource = "xuansi://boards/" + b6Board + "/events"
	b6GraceTTL = 30 * time.Second
	b6Driver   = "b6tiny" // the S12 workspace driver name
)

// ------------------------------------------------------------------ procs --

// b6Proc is one real subprocess with its output teed into the
// transcript under a prefix and greppable for verdicts.
type b6Proc struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser // the mcp child's stdin write end (nil for serve)
	prefix string

	mu       sync.Mutex
	buf      strings.Builder
	shutdown bool // saw the serve "shutdown complete" line

	exited chan struct{}
	code   int
	waitEr error
}

// startB6Daemon launches the REAL daemon (serve) with its stdout+stderr
// pumped into the transcript. extraEnv rides the environment (the XDG
// sandbox).
func startB6Daemon(h *harness, bin, sock, boardsFile string, extraEnv string) *b6Proc {
	cmd := exec.Command(bin, "serve", "--sock", sock, "--grace-ttl", b6GraceTTL.String(), "--boards-file", boardsFile)
	cmd.Env = append(os.Environ(), extraEnv)
	return startPumped(h, cmd, "daemon", extraEnv)
}

// startB6MCP launches the REAL `xuansi mcp` face with MCP frames on
// dedicated os.Pipes (stdin from the client, stdout to the client) and
// its stderr pumped into the transcript. Returns the proc plus the two
// pipe ends the SDK transport rides.
func startB6MCP(h *harness, bin, sock, extraEnv string) (*b6Proc, io.ReadCloser, io.WriteCloser) {
	inR, inW, err1 := os.Pipe()   // child stdin <- client writes
	outR, outW, err2 := os.Pipe() // child stdout -> client reads
	if err1 != nil || err2 != nil {
		h.check(false, fmt.Sprintf("mcp pipes: %v %v", err1, err2))
		return nil, nil, nil
	}
	cmd := exec.Command(bin, "mcp", "--sock", sock)
	cmd.Env = append(os.Environ(), extraEnv)
	cmd.Stdin, cmd.Stdout = inR, outW
	p := startPumped(h, cmd, "mcp", extraEnv)
	if p == nil {
		inR.Close()
		inW.Close()
		outR.Close()
		outW.Close()
		return nil, nil, nil
	}
	p.stdin = inW
	return p, outR, inW
}

// startPumped starts cmd and pumps ONLY its stderr (for mcp, stdout is
// the MCP channel; for serve both faces are logs — stdout joins the
// pump there via StdoutPipe).
func startPumped(h *harness, cmd *exec.Cmd, prefix, extraEnv string) *b6Proc {
	var pumps []io.Reader
	if cmd.Stdout == nil { // serve: stdout is log output
		if r, err := cmd.StdoutPipe(); err == nil {
			pumps = append(pumps, r)
		}
	}
	if r, err := cmd.StderrPipe(); err == nil {
		pumps = append(pumps, r)
	}
	if err := cmd.Start(); err != nil {
		h.check(false, fmt.Sprintf("start %s subprocess: %v", prefix, err))
		return nil
	}
	p := &b6Proc{cmd: cmd, prefix: prefix, exited: make(chan struct{})}
	fmt.Printf("  [%s] started pid=%d argv=%q env+=%q\n", prefix, cmd.Process.Pid, cmd.Args, extraEnv)
	for _, r := range pumps {
		go p.pump(r)
	}
	go func() {
		err := cmd.Wait()
		// the child's own pipe ends died with it; close the PARENT-held
		// copies of the child's ends so the peer side (the SDK
		// transport's reader) sees EOF instead of staying propped open
		if f, ok := cmd.Stdin.(*os.File); ok {
			_ = f.Close()
		}
		if f, ok := cmd.Stdout.(*os.File); ok {
			_ = f.Close()
		}
		p.mu.Lock()
		p.waitEr = err
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			p.code = ee.ExitCode()
		}
		p.mu.Unlock()
		close(p.exited)
	}()
	return p
}

func (p *b6Proc) pump(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := asciiLine(sc.Text())
		fmt.Printf("  [%s] %s\n", p.prefix, line)
		p.mu.Lock()
		p.buf.WriteString(line)
		p.buf.WriteByte('\n')
		if strings.Contains(line, "shutdown complete") {
			p.shutdown = true
		}
		p.mu.Unlock()
	}
}

func (p *b6Proc) has(s string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Contains(p.buf.String(), s)
}

func (p *b6Proc) count(s string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Count(p.buf.String(), s)
}

func (p *b6Proc) shutdownDone() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.shutdown
}

func (p *b6Proc) exitStats() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.code, p.waitEr
}

// kill is the deferred bench-restore: never leave a subprocess running.
func (p *b6Proc) kill(label string) {
	if p == nil {
		return
	}
	select {
	case <-p.exited:
		return
	default:
	}
	fmt.Printf("  [note] driver exit: SIGKILLing the still-running %s (%s)\n", p.prefix, label)
	_ = p.cmd.Process.Signal(syscall.SIGKILL)
	<-p.exited
}

// sigintAndWait delivers SIGINT and waits for a clean exit within bound.
func (p *b6Proc) sigintAndWait(h *harness, bound time.Duration) bool {
	if err := p.cmd.Process.Signal(syscall.SIGINT); err != nil {
		h.check(false, "SIGINT the daemon: "+err.Error())
		return false
	}
	select {
	case <-p.exited:
		return true
	case <-time.After(bound):
		h.check(false, fmt.Sprintf("daemon did not exit within %v of SIGINT (SIGKILLed)", bound))
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
		<-p.exited
		return false
	}
}

// ---------------------------------------------------------------- client --

// b6Client is the REAL MCP client side: one go-sdk ClientSession over
// the mcp child's stdio pipes, with the notification handler the event
// face needs (resource-updated re-read hints).
type b6Client struct {
	h       *harness
	cs      *mcpsdk.ClientSession
	updates chan string // resource URIs the server said changed
}

// call runs one tool call and returns (structured, text, isToolErr,
// rpcOK). Tool errors are contract, not transport trouble.
func (b *b6Client) call(ctx context.Context, timeout time.Duration, name string, args map[string]any) (m map[string]any, text string, isErr, ok bool) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := b.cs.CallTool(cctx, &mcpsdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		b.h.note("[rpc] %s: %v", name, err)
		return nil, fmt.Sprint(err), true, false
	}
	for _, c := range res.Content {
		if tc, ok := c.(*mcpsdk.TextContent); ok {
			text = tc.Text
			break
		}
	}
	m, _ = res.StructuredContent.(map[string]any)
	return m, text, res.IsError, true
}

// callAsync is call on its own goroutine (the blocking capture tool).
func (b *b6Client) callAsync(name string, args map[string]any) chan asyncRes {
	out := make(chan asyncRes, 1)
	go func() {
		m, text, isErr, ok := b.call(context.Background(), 5*time.Minute, name, args)
		out <- asyncRes{m: m, text: text, isErr: isErr, ok: ok}
	}()
	return out
}

type asyncRes struct {
	m     map[string]any
	text  string
	isErr bool
	ok    bool
}

// toolNames lists every tool name the server serves (cursor-followed).
func (b *b6Client) toolNames(ctx context.Context) ([]string, error) {
	var names []string
	for t, err := range b.cs.Tools(ctx, &mcpsdk.ListToolsParams{}) {
		if err != nil {
			return names, err
		}
		names = append(names, t.Name)
	}
	return names, nil
}

// drainUpdates counts pending resource-updated notifications for uri.
func (b *b6Client) drainUpdates(uri string) int {
	n := 0
	for {
		select {
		case u := <-b.updates:
			if u == uri {
				n++
			}
		default:
			return n
		}
	}
}

// ------------------------------------------------------------------ utils --

// b6Num reads a JSON number field.
func b6Num(m map[string]any, k string) (float64, bool) {
	if m == nil {
		return 0, false
	}
	f, ok := m[k].(float64)
	return f, ok
}

// b6Str reads a string field.
func b6Str(m map[string]any, k string) string {
	if m == nil {
		return ""
	}
	s, _ := m[k].(string)
	return s
}

// b6Nest builds the depth-n nested {k=…} table with 42 at the bottom
// (b5full's depth-8 shape: n=7 below an outer key = 8 containers).
func b6Nest(n int) any {
	if n == 0 {
		return float64(42)
	}
	return map[string]any{"k": b6Nest(n - 1)}
}

// b6CapEdges scans a gpio capture file's JSONL lines for the gpio.<pin>
// edge sequence.
func b6CapEdges(data []byte, source string) []string {
	var edges []string
	for _, ln := range strings.Split(string(data), "\n") {
		if ln == "" {
			continue
		}
		var e struct {
			Source string `json:"source"`
			Data   struct {
				Edge string `json:"edge"`
			} `json:"data"`
		}
		if json.Unmarshal([]byte(ln), &e) != nil {
			continue
		}
		if e.Source == source {
			edges = append(edges, e.Data.Edge)
		}
	}
	return edges
}

// ------------------------------------------------------------------- run --

func runB6MCP(attempt int, daemonBin, runDir string) int {
	h := &harness{}
	ctx := context.Background()

	fmt.Printf("=== b6 task-13 full MCP HIL — %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count — any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("board: %s (bench GPIO1<->GPIO2 wired; TCP %s; fw expect %s; WiFi creds in NVS — never touched)\n",
		b6Board, b6TCPEndp, b6FwBuild)
	fmt.Printf("client: go-sdk mcp.NewClient over mcp.IOTransport (the mcp child's stdio pipes) — a REAL MCP client, zero test hooks\n")
	fmt.Printf("wifi discipline: board_op coverage = budget.get/set + caps + READ-ONLY wifi.status snapshot lines;\n")
	fmt.Printf("  wifi.setup/off stay unit-covered (allowlist test) and out of real-credential HIL scope — no credentials in any capture/log\n")

	var serve, mcp *b6Proc
	defer func() { // bench restore on every path
		mcp.kill("mcp child")
		serve.kill("daemon")
	}()

	finish := func() int { return finishB6(h, attempt) }

	// ——— S0: prelude ————————————————————————————————————————
	fmt.Println("\n— S0: prelude — board reachable, sandbox prepared, bench clean —")
	if out, err := exec.Command("pgrep", "-af", "xuansi").CombinedOutput(); err == nil {
		stale := 0
		for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			f := strings.Fields(ln)
			// a real stale face = an xuansi BINARY running serve/mcp as
			// its argv[1] (pgrep -f also matches the codegraph server,
			// whose cmdline merely contains the project PATH "xuansi-…")
			if len(f) >= 2 && strings.Contains(filepath.Base(f[0]), "xuansi") && (f[1] == "serve" || f[1] == "mcp") {
				stale++
				fmt.Printf("  [note] stale xuansi process: %s\n", asciiLine(ln))
			}
		}
		h.check(stale == 0, fmt.Sprintf("no stale xuansi serve/mcp processes on the bench (found %d)", stale))
	} else {
		h.note("pgrep: no xuansi processes found (clean bench)")
	}
	if _, err := os.Stat("/dev/ttyACM0"); err == nil {
		h.note("/dev/ttyACM0 present (USB discovery enumerates it; the daemon never opens it — no reboot)")
	} else {
		h.note("NOTE: /dev/ttyACM0 absent — the usb source may drop to mdns-only (honest note)")
	}

	if conn, err := net.DialTimeout("tcp", b6TCPEndp, 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s: %v (USB fallback OUT of scope — opening it reboots the board)", b6TCPEndp, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s (dial only — no reboot, no USB open)", b6TCPEndp))
	}

	xdg := filepath.Join(runDir, "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		h.check(false, "mkdir run/XDG dir: "+err.Error())
		return finish()
	}
	sock := filepath.Join(runDir, "daemon.sock")
	boardsCfg := filepath.Join(runDir, "boards.conf")
	if err := os.WriteFile(boardsCfg, []byte("# b6-full boards.conf: NO aliases — the dynamic tools take the mac6 prefix (d75884_*)\n"), 0o644); err != nil {
		h.check(false, "write boards.conf: "+err.Error())
		return finish()
	}
	h.note("XDG sandbox: XDG_DATA_HOME=%s (serve captures + mcp workspace land inside; the real user workspace untouched)", xdg)
	h.note("boards.conf: comment-only (no alias) -> dynamic tool prefix = %q", b6Board+"_")

	// ——— S1: serve up ———————————————————————————————————————
	fmt.Println("\n— S1: daemon serve up (REAL subprocess) —")
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

	// ——— S2: mcp up + client initialize ——————————————————————
	fmt.Println("\n— S2: xuansi mcp up (REAL subprocess) + SDK client initialize over the pipes —")
	var mcpR io.ReadCloser
	var mcpW io.WriteCloser
	mcp, mcpR, mcpW = startB6MCP(h, daemonBin, sock, "XDG_DATA_HOME="+xdg)
	if mcp == nil {
		return finish()
	}
	updates := make(chan string, 64)
	cl := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "b6-hil", Version: "1.0"}, &mcpsdk.ClientOptions{
		ResourceUpdatedHandler: func(_ context.Context, r *mcpsdk.ResourceUpdatedNotificationRequest) {
			select {
			case updates <- r.Params.URI:
			default:
			}
		},
		ToolListChangedHandler: func(context.Context, *mcpsdk.ToolListChangedRequest) {},
	})
	b := &b6Client{h: h, cs: nil, updates: updates}
	cs, cerr := cl.Connect(ctx, &mcpsdk.IOTransport{Reader: mcpR, Writer: mcpW}, nil)
	if cerr != nil {
		h.check(false, "MCP client initialize over the child's stdio pipes: "+cerr.Error())
		return finish()
	}
	b.cs = cs
	si := cs.InitializeResult()
	h.check(si != nil && si.ServerInfo.Name == "xuansi",
		fmt.Sprintf("initialize handshake ok: server=%s version=%s (protocol %s)", initField(si, "name"), initField(si, "ver"), initField(si, "proto")))

	// ——— S3: discovery through the tool ——————————————————————
	fmt.Println("\n— S3: xuansi_list_boards — d75884 dual-source, fw build binding —")
	var theBoard map[string]any
	ok, last = pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		m, _, isErr, ok := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
		if !ok || isErr {
			return false, fmt.Sprintf("list_boards trouble (isErr=%v)", isErr)
		}
		for _, e := range m["boards"].([]any) {
			if bm, yes := e.(map[string]any); yes && bm["mac6"] == b6Board {
				if fw, _ := bm["fw"].(string); fw != "" {
					theBoard = bm
					return true, "fw populated (hello-enriched)"
				}
				return false, "board listed, fw not yet enriched"
			}
		}
		return false, "board not in list yet"
	})
	if !ok || theBoard == nil {
		h.check(false, fmt.Sprintf("list_boards shows %s with fw populated (%s)", b6Board, last))
		return finish()
	}
	h.check(true, fmt.Sprintf("list_boards shows %s with fw populated (%s)", b6Board, last))
	srcs := []string{}
	if raw, yes := theBoard["sources"].([]any); yes {
		for _, s := range raw {
			if sv, _ := s.(string); sv != "" {
				srcs = append(srcs, sv)
			}
		}
	}
	h.check(len(srcs) == 2 && srcs[0] == "usb" && srcs[1] == "mdns",
		fmt.Sprintf("dual-source merged [usb mdns] (got %v)", srcs))
	fw, _ := theBoard["fw"].(string)
	h.check(fw == b6FwBuild,
		fmt.Sprintf("fw BUILD BINDING: %q == expected %q (flashed fw == fw HEAD bfa8e04 main/, T4 empty-diff)", fw, b6FwBuild))
	host, _ := theBoard["host"].(string)
	port, _ := theBoard["port"].(float64)
	usbPath, _ := theBoard["usb_path"].(string)
	h.check(host == "xuansi-d75884.local" && port == 7777,
		fmt.Sprintf("mdns face host=%s port=%v", host, port))
	h.check(usbPath != "" && strings.HasPrefix(usbPath, "/dev/"),
		fmt.Sprintf("usb_path enumerated without opening (%s)", usbPath))
	m, _, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_list_boards", map[string]any{})
	h.check(!isErr && b6Str(m, "mdns_error") == "",
		fmt.Sprintf("mdns face healthy (mdns_error=%q)", b6Str(m, "mdns_error")))

	// ——— S4: events resource subscribe + snapshot ————————————
	fmt.Println("\n— S4: subscribe the board's event resource; snapshot reads back —")
	subOK := false
	for i := 0; i < 10 && !subOK; i++ {
		if err := cs.Subscribe(ctx, &mcpsdk.SubscribeParams{URI: b6Resource}); err == nil {
			subOK = true
		} else {
			time.Sleep(time.Second) // the face's refresh adds the resource on its first board list
		}
	}
	h.check(subOK, fmt.Sprintf("resources/subscribe %s ok (resource exists — the face's refresh listed the board)", b6Resource))
	if !subOK {
		return finish()
	}
	rr, rerr := cs.ReadResource(ctx, &mcpsdk.ReadResourceParams{URI: b6Resource})
	snapOK := false
	var snapNote string
	if rerr == nil && len(rr.Contents) > 0 {
		var snap map[string]any
		if jerr := json.Unmarshal([]byte(rr.Contents[0].Text), &snap); jerr == nil && snap["board"] == b6Board {
			snapOK = true
			snapNote = fmt.Sprintf("board=%s ring_cap=%v", snap["board"], snap["ring_cap"])
		} else if jerr != nil {
			snapNote = "snapshot json: " + jerr.Error()
		} else {
			snapNote = fmt.Sprintf("board=%v", snap["board"])
		}
	} else if rerr != nil {
		snapNote = rerr.Error()
	}
	h.check(snapOK, fmt.Sprintf("resources/read snapshot parses as %s's ring (%s)", b6Board, snapNote))

	// ——— S5: lease (custom holder, TCP route) + budget prelude —
	fmt.Println("\n— S5: xuansi_lease holder=b6-hil (default TCP route) + op-face budget prelude —")
	m, text, isErr, _ := b.call(ctx, 30*time.Second, "xuansi_lease", map[string]any{"board": b6Board, "holder": b6Holder})
	h.check(!isErr && b6Str(m, "board") == b6Board && b6Str(m, "holder") == b6Holder,
		fmt.Sprintf("lease holder=%s board=%s session=%v (text=%q)", b6Holder, b6Str(m, "board"), gAny(m, "session"), text))
	sess, _ := b6Num(m, "session")
	h.check(sess > 0 && b6Str(m, "transport") == "" && b6Str(m, "warning") == "",
		fmt.Sprintf("lease took the DEFAULT TCP route: session=%v transport=%q warning=%q (no usb field, no reboot)", sess, b6Str(m, "transport"), b6Str(m, "warning")))

	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b6Board, "op": "budget.set", "args": map[string]any{"eval_ms": 4000}})
	evMS, _ := b6Num(m, "eval_ms")
	h.check(!isErr && evMS == 4000,
		fmt.Sprintf("[prelude] board_op budget.set eval_ms=4000 for the 4s-class job (reply eval_ms=%v event_ms=%v; text=%q)", evMS, gAny(m, "event_ms"), text))

	// ——— S6: dynamic tools ———————————————————————————————————
	fmt.Println("\n— S6: dynamic tools — ≥30 d75884_* after the lease; gpio on the wired pair —")
	ok, last = pollUntil(time.Second, 25*time.Second, func() (bool, string) {
		names, err := b.toolNames(ctx)
		if err != nil {
			return false, "tools/list: " + err.Error()
		}
		for _, n := range names {
			if n == b6Board+"_gpio_mode" {
				return true, fmt.Sprintf("%d tools total", len(names))
			}
		}
		return false, "dynamic set not expanded yet (caps is lease-gated — first refresh after the lease)"
	})
	h.check(ok, fmt.Sprintf("dynamic tools expanded after the lease (%s)", last))
	names, nerr := b.toolNames(ctx)
	if nerr != nil {
		h.check(false, "tools/list: "+nerr.Error())
		return finish()
	}
	dyn := 0
	for _, n := range names {
		if strings.HasPrefix(n, b6Board+"_") {
			dyn++
		}
	}
	h.check(dyn >= 30, fmt.Sprintf("tools/list carries >=30 %s_-prefixed tools (count=%d, total tools=%d)", b6Board, dyn, len(names)))
	have := map[string]bool{}
	for _, n := range names {
		have[n] = true
	}
	h.check(have[b6Board+"_gpio_mode"] && have[b6Board+"_gpio_write"] && have[b6Board+"_gpio_read"],
		fmt.Sprintf("exact dynamic names present: %s (gpio_mode/write/read)", b6Board))
	h.note("static face: 17 tools; dynamic: %d; sample: %s_gpio_mode/_gpio_write/_gpio_read/_uart1_setup/_i2c0_xfer?=%v",
		dyn, b6Board, have[b6Board+"_uart1_setup"])

	m, text, isErr, _ = b.call(ctx, 30*time.Second, b6Board+"_gpio_mode", map[string]any{"pin": float64(1), "mode": "out"})
	h.check(!isErr, fmt.Sprintf("d75884_gpio_mode(1,\"out\") — claims the wired output pin (text=%q)", text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, b6Board+"_gpio_mode", map[string]any{"pin": float64(2), "mode": "in"})
	h.check(!isErr, fmt.Sprintf("d75884_gpio_mode(2,\"in\") — claims the wired input pin (text=%q)", text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, b6Board+"_gpio_write", map[string]any{"pin": float64(1), "level": float64(1)})
	h.check(!isErr, fmt.Sprintf("d75884_gpio_write(1,1) — drives the wired output high (text=%q)", text))
	// fw semantic (xs_pin.c): out-mode pins are configured GPIO_MODE_OUTPUT
	// with the input buffer disabled, so their level is NOT readable back —
	// the pin map's own loop is the readback: the WIRED INPUT pin 2 mirrors
	// the driven level (bench pin map v1: GPIO1 <-> GPIO2 direct wire).
	m, text, isErr, _ = b.call(ctx, 30*time.Second, b6Board+"_gpio_read", map[string]any{"pin": float64(2)})
	v1, _ := m["value"].(float64)
	h.check(!isErr && m["value"] != nil && v1 == 1,
		fmt.Sprintf("d75884_gpio_read(2) -> value=%v (the wired input mirrors the driven high; text=%q)", gAny(m, "value"), text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b6Board, "code": "return bench.gpio.read(2)"})
	xv, _ := m["value"].(float64)
	h.check(!isErr && xv == 1,
		fmt.Sprintf("[cross-check] xuansi_run_lua bench.gpio.read(2) -> value=%v (same pin, bridge path; text=%q)", gAny(m, "value"), text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, b6Board+"_gpio_write", map[string]any{"pin": float64(1), "level": float64(0)})
	h.check(!isErr, fmt.Sprintf("d75884_gpio_write(1,0) — park the pair low before the edge phase (text=%q)", text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, b6Board+"_gpio_read", map[string]any{"pin": float64(2)})
	v0, _ := m["value"].(float64)
	h.check(!isErr && v0 == 0, fmt.Sprintf("d75884_gpio_read(2) -> value=%v (wired input follows the parked low)", gAny(m, "value")))

	// ——— S7: deep args bridge + deep-8 return —————————————————
	fmt.Println("\n— S7: deep nested tables — args->ARGS->value (depth-6) + return value (depth-8) —")
	// The ARGS side: the frozen codec's frame budget (XS_CODEC_MAX_DEPTH
	// 8, 訊框深度＝1) leaves SIX nested containers for an args value —
	// pinned on the board by this HIL (a 7-container args table refuses
	// with "protocol: args: malformed at byte N"; 6 round-trips
	// clean). Depth-6 here is that exact boundary shape.
	want6 := map[string]any{"t": b6Nest(5)}
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b6Board, "code": "return ARGS", "args": want6})
	_, hasValue := m["value"]
	h.check(!isErr && hasValue && reflect.DeepEqual(m["value"], want6),
		fmt.Sprintf("depth-6 args table (the codec boundary) round-trip deep-equal, `value` key present (isErr=%v hasValue=%v; text=%.120s)", isErr, hasValue, text))
	// The RETURN side rides the separate value bridge (xs_bridge:
	// 根容器＝1, cap 8) — the full b5-proven depth-8 shape.
	want8 := map[string]any{"t": b6Nest(7)}
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b6Board, "code": `local t=42 for i=1,7 do t={k=t} end return {t=t}`})
	h.check(!isErr && reflect.DeepEqual(m["value"], want8),
		fmt.Sprintf("depth-8 return table through the value bridge deep-equal (isErr=%v; text=%.120s)", isErr, text))

	// ——— S8: submit ~4s job -> job event through the event face —
	fmt.Println("\n— S8: xuansi_submit 4s-class job -> job-done event through resource+ring —")
	jobTag := "b6job4s"
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_submit",
		map[string]any{"board": b6Board, "tag": jobTag,
			"code": `local t=os.clock() while os.clock()-t < 3.8 do end return "b6job4s-value"`})
	h.check(!isErr && b6Str(m, "tag") == jobTag && m["queued"] == true,
		fmt.Sprintf("submit ack {tag=%v queued=%v} (text=%q)", b6Str(m, "tag"), gAny(m, "queued"), text))
	b.drainUpdates(b6Resource) // drop pre-job hints; the job's own hint must land fresh

	var jobEv map[string]any
	ok, last = pollUntil(500*time.Millisecond, 15*time.Second, func() (bool, string) {
		m, _, isErr, _ := b.call(ctx, 15*time.Second, "xuansi_events_recent",
			map[string]any{"board": b6Board, "source_prefix": "job", "limit": 10})
		if isErr {
			return false, "events_recent: " + b6Str(m, "board")
		}
		for _, e := range evsList(m) {
			em, _ := e.(map[string]any)
			d, _ := em["data"].(map[string]any)
			if b6Str(d, "tag") == jobTag && d["ok"] == true {
				jobEv = d
				return true, fmt.Sprintf("tag=%v ok=%v value=%v", d["tag"], d["ok"], gAny(d, "value"))
			}
		}
		return false, "job still running (3.8s busy loop)"
	})
	h.check(ok && b6Str(jobEv, "value") == "b6job4s-value",
		fmt.Sprintf("job-done event in the ring: tag=%v ok=%v value=%v (%s; submitted through THIS process, so the direct-delivered job event landed in its own ring)",
			b6Str(jobEv, "tag"), gAny(jobEv, "ok"), gAny(jobEv, "value"), last))
	time.Sleep(300 * time.Millisecond) // the coalesced hint follows the ring append
	n := b.drainUpdates(b6Resource)
	h.check(n >= 1, fmt.Sprintf("resource-updated notification arrived for %s (%d hint(s) drained)", b6Resource, n))

	// ——— S9: gpio edges through the event face (capture-armed) —
	fmt.Println("\n— S9: gpio edge events — capture-armed gpio.2, wired edges, ring+file+notification —")
	capCh := b.callAsync("xuansi_capture", map[string]any{"board": b6Board, "source": "gpio.2", "seconds": float64(4)})
	ok, last = pollUntil(300*time.Millisecond, 10*time.Second, func() (bool, string) {
		return serve.has("capture cap-1 started (" + b6Board + " gpio.2"), "daemon log: capture started"
	})
	h.check(ok, fmt.Sprintf("gpio.2 capture armed board-side through the MCP capture face (%s) — the per-pin source arming path", last))
	b.drainUpdates(b6Resource)
	m, text, isErr, _ = b.call(ctx, 30*time.Second, b6Board+"_gpio_write", map[string]any{"pin": float64(1), "level": float64(1)})
	h.check(!isErr, fmt.Sprintf("edge 1: d75884_gpio_write(1,1) -> rising on the wired gpio.2 (text=%q)", text))
	time.Sleep(200 * time.Millisecond)
	m, text, isErr, _ = b.call(ctx, 30*time.Second, b6Board+"_gpio_write", map[string]any{"pin": float64(1), "level": float64(0)})
	h.check(!isErr, fmt.Sprintf("edge 2: d75884_gpio_write(1,0) -> falling on the wired gpio.2 (text=%q)", text))

	cres := <-capCh
	h.check(cres.ok && !cres.isErr,
		fmt.Sprintf("xuansi_capture gpio.2 returned {path=%v bytes=%v reason=%v}", gAny(cres.m, "path"), gAny(cres.m, "bytes"), gAny(cres.m, "reason")))
	capPath := b6Str(cres.m, "path")
	capBytes, _ := b6Num(cres.m, "bytes")
	capsRoot := filepath.Join(xdg, "xuansi", "captures")
	h.check(capPath != "" && strings.HasPrefix(capPath, capsRoot),
		fmt.Sprintf("capture file inside the XDG sandbox (%s under %s)", capPath, capsRoot))
	h.check(b6Str(cres.m, "reason") == "limit" && capBytes > 0,
		fmt.Sprintf("capture ended reason=%q bytes=%v (the 4s bound passed)", b6Str(cres.m, "reason"), capBytes))
	var capData []byte
	if capPath != "" {
		capData, _ = os.ReadFile(capPath)
	}
	edges := b6CapEdges(capData, "gpio.2")
	h.check(len(edges) >= 2 && edges[0] == "rising" && edges[1] == "falling",
		fmt.Sprintf("capture FILE holds the gpio.2 edges in order: %v (jsonl)", edges))
	h.check(serve.has("capture cap-1 done (limit)"), "daemon log: capture cap-1 done (limit)")

	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_events_recent",
		map[string]any{"board": b6Board, "source_prefix": "gpio", "limit": 20})
	var ringEdges []string
	if !isErr {
		if evs, yes := m["events"].([]any); yes {
			for _, e := range evs {
				em, _ := e.(map[string]any)
				if b6Str(em, "source") == "gpio.2" {
					if d, _ := em["data"].(map[string]any); d != nil {
						ringEdges = append(ringEdges, b6Str(d, "edge"))
					}
				}
			}
		}
	}
	h.check(len(ringEdges) >= 2 && ringEdges[0] == "rising" && ringEdges[1] == "falling",
		fmt.Sprintf("xuansi_events_recent ring holds the same edges: %v (event face delivery)", ringEdges))
	time.Sleep(300 * time.Millisecond)
	n = b.drainUpdates(b6Resource)
	h.check(n >= 1, fmt.Sprintf("resource-updated notification fired for the gpio edges (%d hint(s))", n))

	// ——— S10: events_recent sanity ——————————————————————————
	fmt.Println("\n— S10: xuansi_events_recent {board, limit} sanity — non-empty, ordered —")
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_events_recent",
		map[string]any{"board": b6Board, "limit": 50})
	evs, _ := m["events"].([]any)
	h.check(!isErr && len(evs) > 0,
		fmt.Sprintf("events_recent {board, limit:50} non-empty (%d events; text=%.80s)", len(evs), text))
	ordered := true
	var prev time.Time
	for i, e := range evs {
		em, _ := e.(map[string]any)
		ts, perr := time.Parse(time.RFC3339Nano, b6Str(em, "ts"))
		if perr != nil || (i > 0 && ts.Before(prev)) {
			ordered = false
			break
		}
		prev = ts
	}
	h.check(ordered && len(evs) > 0, fmt.Sprintf("ring view newest-last ordered (ts monotonic across %d events)", len(evs)))
	h.note("ring sample: %v", ringSummary(evs))

	// ——— S11: stdout capture while an eval prints ————————————
	fmt.Println("\n— S11: xuansi_capture stdout 4s while an eval prints lines —")
	capCh = b.callAsync("xuansi_capture", map[string]any{"board": b6Board, "source": "stdout", "seconds": float64(4)})
	ok, last = pollUntil(300*time.Millisecond, 10*time.Second, func() (bool, string) {
		return serve.has("capture cap-2 started (" + b6Board + " stdout"), "daemon log: capture started"
	})
	h.check(ok, fmt.Sprintf("stdout capture armed board-side (%s)", last))
	time.Sleep(500 * time.Millisecond) // let the subscription settle before printing
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b6Board, "code": `print("b6cap-alpha") print("b6cap-beta") print("b6cap-gamma") return "printed"`})
	h.check(!isErr && b6Str(m, "value") == "printed",
		fmt.Sprintf("eval printed 3 distinctive lines while the capture ran (value=%v out=%q)", gAny(m, "value"), b6Str(m, "out")))

	cres = <-capCh
	h.check(cres.ok && !cres.isErr && b6Str(cres.m, "reason") == "limit",
		fmt.Sprintf("stdout capture returned {path=%v bytes=%v reason=%v}", gAny(cres.m, "path"), gAny(cres.m, "bytes"), gAny(cres.m, "reason")))
	soPath := b6Str(cres.m, "path")
	soBytes, _ := b6Num(cres.m, "bytes")
	h.check(soPath != "" && strings.HasPrefix(soPath, capsRoot),
		fmt.Sprintf("stdout capture file inside the XDG sandbox (%s)", soPath))
	var soData []byte
	var rdErr error
	if soPath != "" {
		soData, rdErr = os.ReadFile(soPath)
	}
	cont := string(soData)
	h.check(rdErr == nil && strings.Contains(cont, "b6cap-alpha") && strings.Contains(cont, "b6cap-beta") && strings.Contains(cont, "b6cap-gamma"),
		fmt.Sprintf("file READABLE FROM THIS HIL PROCESS and contains all 3 printed lines (bytes=%v, file bytes=%d, rdErr=%v)", soBytes, len(soData), rdErr))

	// ——— S12: workspace put -> push -> require ———————————————
	fmt.Println("\n— S12: workspace — ws_put tiny driver -> driver_push -> require on the board —")
	drvContent := "--@api 1\nlocal M = {}\nM.ok = true\nreturn M\n"
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_ws_put",
		map[string]any{"name": b6Driver, "version": "1.0.0", "content": drvContent})
	sz, _ := b6Num(m, "size")
	h.check(!isErr && b6Str(m, "sha256") != "" && int(sz) == len(drvContent),
		fmt.Sprintf("ws_put %s@1.0.0 stored (sha256=%.16s… size=%v; text=%.80s)", b6Driver, b6Str(m, "sha256"), gAny(m, "size"), text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_ws_list", map[string]any{})
	stored, active0 := false, ""
	if drs, yes := m["drivers"].([]any); yes {
		for _, d := range drs {
			dm, _ := d.(map[string]any)
			if b6Str(dm, "name") == b6Driver {
				active0 = b6Str(dm, "active")
				if vs, ok2 := dm["versions"].([]any); ok2 {
					for _, v := range vs {
						if vm, _ := v.(map[string]any); b6Str(vm, "version") == "1.0.0" {
							stored = true
						}
					}
				}
			}
		}
	}
	h.check(!isErr && stored && active0 == "",
		fmt.Sprintf("ws_list sees version 1.0.0 stored, active ABSENT before any push (stored=%v active=%q)", stored, active0))
	m, text, isErr, _ = b.call(ctx, 60*time.Second, "xuansi_driver_push",
		map[string]any{"board": b6Board, "name": b6Driver, "version": "1.0.0"})
	pb, _ := b6Num(m, "bytes")
	h.check(!isErr && b6Str(m, "board") == b6Board && b6Str(m, "path") == "/drv/"+b6Driver+".lua" && int(pb) == len(drvContent),
		fmt.Sprintf("driver_push landed /drv/%s.lua on the board, read-back verified (bytes=%v sha=%.16s…; text=%.80s)", b6Driver, gAny(m, "bytes"), b6Str(m, "sha256"), text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_run_lua",
		map[string]any{"board": b6Board, "code": "local d = require(\"bench.drv." + b6Driver + "\")\nreturn d.ok"})
	h.check(!isErr && m["value"] == true,
		fmt.Sprintf("require(\"bench.drv.%s\").ok == true through the --@api gate (value=%v; text=%.80s)", b6Driver, gAny(m, "value"), text))
	m, text, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_ws_list", map[string]any{})
	active := ""
	if drs, yes := m["drivers"].([]any); yes {
		for _, d := range drs {
			if dm, _ := d.(map[string]any); b6Str(dm, "name") == b6Driver {
				active = b6Str(dm, "active")
			}
		}
	}
	h.check(!isErr && active == "1.0.0",
		fmt.Sprintf("manifest ACTIVE version right after the verified push (active=%q)", active))
	wsRoot := filepath.Join(xdg, "xuansi", "workspace")
	drvFile := filepath.Join(wsRoot, "drivers", b6Driver+"@1.0.0.lua")
	disk, derr := os.ReadFile(drvFile)
	man, manErr := os.ReadFile(filepath.Join(wsRoot, "manifest.json"))
	manParses := false
	if manErr == nil {
		var j map[string]any
		manParses = json.Unmarshal(man, &j) == nil
	}
	h.check(derr == nil && string(disk) == drvContent && manParses,
		fmt.Sprintf("workspace roots under the XDG sandbox: drivers/%s@1.0.0.lua byte-exact on disk, manifest.json parses (root=%s)", b6Driver, wsRoot))

	// ——— S13: ops face ——————————————————————————————————————
	fmt.Println("\n— S13: board_op — budget.set/get round-trip, caps, heap+wifi snapshot lines —")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b6Board, "op": "budget.set", "args": map[string]any{"eval_ms": 3000}})
	evSet, _ := b6Num(m, "eval_ms")
	h.check(!isErr && evSet == 3000,
		fmt.Sprintf("board_op budget.set {eval_ms:3000} -> applied eval_ms=%v event_ms=%v (text=%q)", evSet, gAny(m, "event_ms"), text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b6Board, "op": "budget.get"})
	evGet, _ := b6Num(m, "eval_ms")
	h.check(!isErr && evGet == 3000,
		fmt.Sprintf("board_op budget.get round-trip -> eval_ms=%v (the frozen no-keys getter, translated daemon-side; text=%q)", evGet, text))
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b6Board, "op": "caps"})
	ver, _ := b6Num(m, "api_version")
	opsCount := 0
	if prims, yes := m["primitives"].(map[string]any); yes {
		for _, pv := range prims {
			if pm, _ := pv.(map[string]any); pm != nil {
				if ops, ok2 := pm["ops"].([]any); ok2 {
					opsCount += len(ops)
				}
			}
		}
	}
	h.check(!isErr && ver == 1 && opsCount >= 30,
		fmt.Sprintf("board_op caps: api_version=%v primitives advertise %d ops (the 30-signature table's source; text=%.60s)", ver, opsCount, text))
	m, _, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b6Board, "op": "diag.stats"})
	h.note("heap snapshot (diag.stats carries the heap block; diag.heapinfo replies an empty ack by fw design): %s", oneLineJSON(m, isErr))
	m, _, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_board_op",
		map[string]any{"board": b6Board, "op": "wifi.status"})
	state, rssi := b6Str(m, "state"), gAny(m, "rssi")
	h.check(!isErr && state == "up",
		fmt.Sprintf("wifi snapshot (READ-ONLY op, no credentials anywhere): state=%s ssid=%s ip=%v rssi=%v", state, b6Str(m, "ssid"), gAny(m, "ip"), rssi))

	// ——— S14: teardown ———————————————————————————————————————
	fmt.Println("\n— S14: release, mcp EOF exit 0, serve SIGINT exit 0 —")
	m, text, isErr, _ = b.call(ctx, 30*time.Second, "xuansi_release", map[string]any{"board": b6Board})
	h.check(!isErr && b6Str(m, "board") == b6Board && b6Str(m, "holder") == b6Holder,
		fmt.Sprintf("release holder=%s (text=%q)", b6Holder, text))
	m, _, isErr, _ = b.call(ctx, 15*time.Second, "xuansi_board_status", map[string]any{"board": b6Board})
	_, hasLease := m["lease"]
	h.check(!isErr && !hasLease, fmt.Sprintf("post-release board_status shows NO lease (lease key %v, connected=%v)", hasLease, gAny(m, "connected")))
	h.check(serve.count("reason=released") >= 1, fmt.Sprintf("daemon log: lease teardown reason=released (count=%d)", serve.count("reason=released")))

	if err := cs.Close(); err != nil {
		h.note("client session close: %v (proceeding to EOF wait)", err)
	}
	select {
	case <-mcp.exited:
		code, werr := mcp.exitStats()
		h.check(code == 0 && werr == nil,
			fmt.Sprintf("xuansi mcp exited clean on stdin EOF (code=%d waitErr=%v)", code, werr))
		h.check(!mcp.has("xuansi mcp:"), "mcp stderr carries no error line (clean EOF path)")
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
	h.note("no-goroutine-leak bar for a SUBPROCESS: exit 0 requires srv.Done() — every server goroutine joined before 'shutdown complete'")
	h.note("XDG sandbox intact: captures under %s, workspace under %s — the real user store untouched", capsRoot, wsRoot)

	return finish()
}

func finishB6(h *harness, attempt int) int {
	code := h.finish("b6-mcp")
	if h.fail == 0 {
		fmt.Printf("=== ALL PASS — task-13 single-capture evidence complete (attempt %d, S0..S14) ===\n", attempt)
	}
	return code
}

// ------------------------------------------------------------- small utils --

// initField renders one initialize-result field for the transcript.
func initField(si *mcpsdk.InitializeResult, what string) string {
	if si == nil {
		return "?"
	}
	switch what {
	case "ver":
		return si.ServerInfo.Version
	case "proto":
		return string(si.ProtocolVersion)
	default:
		return si.ServerInfo.Name
	}
}

// evsList safely reads the events array field.
func evsList(m map[string]any) []any {
	a, _ := m["events"].([]any)
	return a
}

// oneLineJSON renders an op reply as one compact ASCII line.
func oneLineJSON(m map[string]any, isErr bool) string {
	if isErr {
		return "<error>"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprint(m)
	}
	s := asciiLine(string(b))
	if len(s) > 160 {
		s = s[:160] + "..."
	}
	return s
}

// ringSummary renders up to 6 ring entries compactly.
func ringSummary(evs []any) string {
	var parts []string
	for i, e := range evs {
		if i >= 6 {
			parts = append(parts, "...")
			break
		}
		em, _ := e.(map[string]any)
		parts = append(parts, fmt.Sprintf("%s@%v", b6Str(em, "source"), b6Str(em, "ts")))
	}
	return strings.Join(parts, ", ")
}
