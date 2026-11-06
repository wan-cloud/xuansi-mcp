// spawnidle.go - task-7 full spawn/idle HIL: the batch's client-side
// auto-spawn (task 3), the spawn-race exit-0 loser (task 2) and the
// idle self-exit (task 4) proven with REAL processes on the REAL bench
// board in ONE continuous single-capture run - b6/b7 house style.
//
// Three honest process layers, no mocks, no test seams:
//
//	driver (this file)
//	  | spawns + observes REAL `xuansi` CLI subprocesses (list, lease,
//	  | eval, status, release) under an XDG SANDBOX env, plus REAL
//	  | `xuansi serve` daemons: the CLI's auto-spawned DETACHED ones
//	  | (found by PID, evidenced through the sandbox StateDir
//	  | serve.log) and deliberately-started ones (the S4 idle-exit
//	  | staging serve, the S6 double-start loser).
//	xuansi serve (auto-spawned by the client, detached, setsid)
//	  | TCP
//	the real bench board d75884 (TCP face only - USB is never opened
//	  and never re-dialed; the by-id path is only noted as present)
//
// One connected story, S0..S7 (task-7 brief):
//
//	S0  cold spawn - the sandbox has NO daemon; `xuansi list` (the
//	    DEFAULT sock resolution through XDG_RUNTIME_DIR - no --sock
//	    anywhere) auto-spawns a detached serve: the serve process
//	    exists, serve.log in the sandbox StateDir carries the serving
//	    line naming our sock, and list's stdout stays protocol-clean
//	    (the table and nothing else - the spawn notice rides stderr);
//	    a second list dials through WITHOUT spawning again and the
//	    board row (d75884, usb+mdns, fw populated) appears once the
//	    sweep lands.
//	S1  resident holder - lease holder=spawn-h1 (session N) +
//	    script_start spawnkeep (a 200ms counter in the lease session,
//	    b5full's resident form) + counter read n1 + budget prelude
//	    (eval_ms=15000 - the resident eval below busy-loops ~10s,
//	    past the default 200ms budget; restored at S5); then a REAL
//	    resident eval client process (the 10s busy loop claims GPIO1
//	    for ns-0 owner _eval) whose front-door eval adopts the
//	    just-grace lease back live (the adopt line in serve.log).
//	S2  kill the client process (SIGKILL, mid-eval) - the unix conn
//	    dies with it, serve.log shows agent-gone holder=spawn-h1
//	    (grace window open, board state KEPT), `xuansi status` prints
//	    grace-expiring, and the killed client never saw its value.
//	S3  adopt within the window - a fresh holder=spawn-h1 connection
//	    adopts the SAME lease (serve.log adopt line, board_status
//	    live again with no grace deadline, spawnkeep still listed);
//	    the counter read n2 > n1 proves the SAME live Lua VM - no
//	    board reset, no USB re-dial (serve.log carries no USB
//	    transport line at any point).
//	S4  idle self-exit - the auto-spawned daemon is SIGINT'd (its
//	    shutdown message names the surviving-scripts semantics), then
//	    a deliberately-started serve with --idle-exit 15s --grace-ttl
//	    15s (staging documented: auto-spawn passes only --sock, so
//	    the idle window is staged by starting this serve ourselves;
//	    its output is teed into the same serve.log) serves one list
//	    client; after the last client leaves, the REAL 15s window
//	    fires: process gone, socket removed, serve.log tail carries
//	    the idle-exit line, exit code 0.
//	S5  survivors' audit - `xuansi list` cold-starts spawn #2 whose
//	    startup orphan audit lists spawnkeep (and the ns-0 GPIO1
//	    claim) - the board-side script RAN THROUGH both daemon
//	    deaths; then the survivors are cleaned (script stopped,
//	    session 0 reset, GPIO1 write-reject probe, budget restored,
//	    release) so the bench is left in its default state.
//	S6  concurrent spawn race - kill the daemon; TWO list clients
//	    start the same instant; both spawn a serve, the bind-as-lock
//	    race leaves exactly ONE serve alive, the loser exits 0
//	    (serve.log "already listening ... leaving"), no double-bind
//	    crash ("address already in use" absent), both clients
//	    succeed; a deliberate double-start against the winner then
//	    proves the exit-0 loser path at process level (an observable
//	    exit code + the leaving line), and the winner's own audit
//	    shows the board clean (scripts=[]).
//	S7  second-cycle smoke - against the race's winner daemon: list
//	    (no respawn, still one serve), then lease + eval 6*7 +
//	    release through fresh CLI processes - the second spawn cycle
//	    works immediately.
//
// Evidence discipline (b6/b7): single capture; per-check PASS/FAIL;
// failing runs keep their own transcripts via spawnidle-full.sh;
// honest attempt counting. The 15s idle window at S4 is waited out
// for real (b5's grace-TTL precedent). ASCII-only transcript.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"xuansi/internal/discover"
	"xuansi/internal/server"
)

const (
	spBoard   = "d75884" // the bench board (GPIO1<->GPIO2 wired pair, TCP face)
	spTCPEndp = "xuansi-d75884.local:7777"
	spHolder  = "spawn-h1"  // the S1-S3 lease identity (kill/adopt subject)
	spFinal   = "spawn-fin" // the S5 cleanup lease identity
	spC2      = "spawn-c2"  // the S7 second-cycle lease identity
	spScript  = "spawnkeep" // the board-side survivor script name
	// spIdleWin is the S4 idle-exit/grace window (staged on the
	// deliberately-started serve; waited out for real).
	spIdleWin = 15 * time.Second
	// spResidentSecs is how long the S1 resident eval busy-loops
	// (killed ~1.2s in; the residual keeps the VM's eval slot busy and
	// queues S3's read behind it - VM-order evals, b5full S5 note).
	// The eval budget's hard ceiling is 4000ms (conn-task TWDT), so
	// 3.5s under a 4000ms budget is the longest honest resident hold.
	spResidentSecs = 3.5
)

// spCounterCode is b5full's resident counter form: a 200ms timer
// incrementing a VM-global, yielding forever. Started once at S1; its
// value across S2/S3 proves the same live VM (no reset), and its
// survival across the S4 daemon deaths is what S5's audit lists.
const spCounterCode = "_G.spawncnt = 0; " +
	"bench.timer_ms(200, function() _G.spawncnt = _G.spawncnt + 1 end); " +
	"while true do local f = coroutine.yield(); if f then f() end end"

// ---------------------------------------------------------------- sandbox --

// spSandbox is the XDG-isolated world every child runs in: the client's
// DEFAULT sock resolution ($XDG_RUNTIME_DIR/xuansi/daemon.sock), the
// spawned serves' serve.log ($XDG_STATE_HOME/xuansi/serve.log), the
// daemon's captures/workspace ($XDG_DATA_HOME/xuansi) and the
// boards.conf lookup ($XDG_CONFIG_HOME/xuansi - absent, zero-config).
// The real user state is never touched.
type spSandbox struct {
	bin        string // absolute daemon binary path
	run        string // scratch run dir
	runtimeDir string
	stateDir   string
	dataDir    string
	configDir  string
	sock       string
	serveLog   string
	env        []string

	mu    sync.Mutex
	procs []*spProc // every child this driver started (deferred restore)
}

func spNewSandbox(h *harness, bin, run string) *spSandbox {
	abs, err := filepath.Abs(bin)
	if err != nil {
		h.check(false, "resolve daemon binary path: "+err.Error())
		return nil
	}
	s := &spSandbox{
		bin:        abs,
		run:        run,
		runtimeDir: filepath.Join(run, "runtime"),
		stateDir:   filepath.Join(run, "state"),
		dataDir:    filepath.Join(run, "data"),
		configDir:  filepath.Join(run, "config"),
	}
	s.sock = filepath.Join(s.runtimeDir, "xuansi", "daemon.sock")
	s.serveLog = filepath.Join(s.stateDir, "xuansi", "serve.log")
	for _, d := range []string{s.runtimeDir, s.stateDir, s.dataDir, s.configDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			h.check(false, "mkdir sandbox dir "+d+": "+err.Error())
			return nil
		}
	}
	// later duplicates win in exec env: the sandbox XDG vars override
	// whatever the session had set
	s.env = append(os.Environ(),
		"XDG_RUNTIME_DIR="+s.runtimeDir,
		"XDG_STATE_HOME="+s.stateDir,
		"XDG_DATA_HOME="+s.dataDir,
		"XDG_CONFIG_HOME="+s.configDir,
	)
	return s
}

// restore is the deferred bench-restore on every exit path: kill every
// child this driver started, then any sandbox serve still alive (a
// failed run must not leave a detached daemon behind). The pattern is
// sock-specific - the user's own real daemon, if any, never matches.
func (s *spSandbox) restore() {
	s.mu.Lock()
	procs := append([]*spProc(nil), s.procs...)
	s.mu.Unlock()
	for _, p := range procs {
		p.kill()
	}
	for _, pid := range spServePIDs(s) {
		fmt.Printf("  [note] restore: SIGKILLing leftover sandbox serve pid=%d (the wrapper wipes the run dir; the board was left clean by the S5 teardown whenever the run got that far)\n", pid)
		spSignal(pid, syscall.SIGKILL)
	}
}

// ------------------------------------------------------------------ procs --

// spProc is one REAL child process (a CLI client or a deliberately
// started serve) with stdout and stderr pumped SEPARATELY into the
// transcript (prefixes "out"/"err") and captured for parsing; tee, when
// non-nil, additionally receives every line (the S4 staging serve
// appends to the sandbox serve.log through it).
type spProc struct {
	cmd    *exec.Cmd
	prefix string
	pumps  sync.WaitGroup // the two output pumps; waited after exit so buffers are complete

	mu   sync.Mutex
	out  strings.Builder
	errB strings.Builder

	exited chan struct{}
	code   int
	waitEr error
}

func (s *spSandbox) start(h *harness, prefix string, tee io.Writer, args ...string) *spProc {
	cmd := exec.Command(s.bin, args...)
	cmd.Env = s.env
	outR, err1 := cmd.StdoutPipe()
	errR, err2 := cmd.StderrPipe()
	if err1 != nil || err2 != nil {
		h.check(false, fmt.Sprintf("%s pipes: %v %v", prefix, err1, err2))
		return nil
	}
	if err := cmd.Start(); err != nil {
		h.check(false, fmt.Sprintf("start %s subprocess: %v", prefix, err))
		return nil
	}
	p := &spProc{cmd: cmd, prefix: prefix, exited: make(chan struct{})}
	fmt.Printf("  [%s] started pid=%d argv=%q\n", prefix, cmd.Process.Pid, cmd.Args)
	p.pumps.Add(2) // the two output pumps: waited after exit so the buffers are COMPLETE before anyone parses them
	go p.pump(outR, "out", tee)
	go p.pump(errR, "err", tee)
	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		p.waitEr = err
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			p.code = ee.ExitCode()
		}
		p.mu.Unlock()
		close(p.exited)
	}()
	s.mu.Lock()
	s.procs = append(s.procs, p)
	s.mu.Unlock()
	return p
}

func (p *spProc) pump(r io.Reader, which string, tee io.Writer) {
	defer p.pumps.Done()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := asciiLine(sc.Text())
		fmt.Printf("  [%s %s] %s\n", p.prefix, which, line)
		if tee != nil {
			fmt.Fprintf(tee, "%s\n", line)
		}
		p.mu.Lock()
		if which == "out" {
			p.out.WriteString(line)
			p.out.WriteByte('\n')
		} else {
			p.errB.WriteString(line)
			p.errB.WriteByte('\n')
		}
		p.mu.Unlock()
	}
}

// settled waits for the process to exit AND both output pumps to drain
// (cmd.Wait closes the pipes, so the pumps EOF promptly after) — reading
// the buffers before this races half-drained output.
func (p *spProc) settled() {
	<-p.exited
	p.pumps.Wait()
}

func (p *spProc) wait(bound time.Duration) bool {
	select {
	case <-p.exited:
		p.pumps.Wait()
		return true
	case <-time.After(bound):
		return false
	}
}

func (p *spProc) exitStats() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.code, p.waitEr
}

func (p *spProc) kill() {
	select {
	case <-p.exited:
		p.pumps.Wait()
		return
	default:
	}
	fmt.Printf("  [note] driver: SIGKILLing the still-running %s (pid=%d)\n", p.prefix, p.cmd.Process.Pid)
	_ = p.cmd.Process.Signal(syscall.SIGKILL)
	p.settled()
}

func (p *spProc) stdoutStr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

func (p *spProc) stderrStr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.errB.String()
}

// cli runs one CLI client to completion and returns (code, stdout,
// stderr). The 90s bound covers the auto-spawn retry window plus a
// cold board op with headroom.
func (s *spSandbox) cli(h *harness, label string, args ...string) (int, string, string) {
	p := s.start(h, label, nil, args...)
	if p == nil {
		return -1, "", ""
	}
	if !p.wait(90 * time.Second) {
		p.kill()
		return -1, p.stdoutStr(), p.stderrStr()
	}
	c, _ := p.exitStats()
	return c, p.stdoutStr(), p.stderrStr()
}

// --------------------------------------------------------- serve watching --

// spSignal delivers sig to a detached pid (the auto-spawned daemons are
// not our children). os.Process.Signal keeps the hil package
// cross-compilable for the windows matrix targets; it only ever runs on
// the Linux bench.
func spSignal(pid int, sig syscall.Signal) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Signal(sig)
	}
}

// spServePIDs finds every `xuansi serve` bound to OUR sandbox sock:
// pgrep -af over the exact argv prefix "<bin> serve --sock <sock>",
// then a HasPrefix filter so only true argv matches count (b6/b7's
// pgrep-false-positive lesson). Auto-spawned serves land here because
// the spawner re-execs the client's own binary with exactly these
// leading args.
func spServePIDs(s *spSandbox) []int {
	pat := s.bin + " serve --sock " + s.sock
	out, err := exec.Command("pgrep", "-af", pat).Output()
	if err != nil {
		return nil // pgrep exit 1 = no matches
	}
	var pids []int
	for _, ln := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if ln == "" {
			continue
		}
		f := strings.SplitN(ln, " ", 2)
		pid, aerr := strconv.Atoi(f[0])
		if aerr != nil || len(f) < 2 || !strings.HasPrefix(f[1], pat) {
			continue
		}
		pids = append(pids, pid)
	}
	return pids
}

// waitServes polls until exactly want sandbox serves are alive.
func (s *spSandbox) waitServes(h *harness, want int, bound time.Duration) (bool, string) {
	return pollUntil(300*time.Millisecond, bound, func() (bool, string) {
		n := len(spServePIDs(s))
		return n == want, fmt.Sprintf("%d serve(s) alive (want %d)", n, want)
	})
}

// sigintServes SIGINTs every sandbox serve (the auto-spawned daemons
// are detached, not our children - the signal goes straight to the
// pid) and asserts they left AND removed the socket.
func (s *spSandbox) sigintServes(h *harness, label string) bool {
	pids := spServePIDs(s)
	if len(pids) == 0 {
		h.check(false, label+": no sandbox serve process found to SIGINT")
		return false
	}
	for _, pid := range pids {
		spSignal(pid, syscall.SIGINT)
	}
	ok, last := s.waitServes(h, 0, 20*time.Second)
	h.check(ok, fmt.Sprintf("%s: SIGINT -> detached daemon gone within 20s (%s)", label, last))
	_, serr := os.Stat(s.sock)
	sockGone := os.IsNotExist(serr)
	h.check(sockGone, fmt.Sprintf("%s: sock file removed at exit (stat err=%v)", label, serr))
	return ok && sockGone
}

// ------------------------------------------------------------- serve.log --

// The spawned daemons append their stdout+stderr to the sandbox
// StateDir serve.log (spawn.go's redirect); these helpers read it with
// byte-offset marks so each scenario phase greps only its own slice.
func (s *spSandbox) logSize() int64 {
	fi, err := os.Stat(s.serveLog)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func (s *spSandbox) logSince(mark int64) string {
	b, err := os.ReadFile(s.serveLog)
	if err != nil {
		return ""
	}
	if mark < 0 || mark > int64(len(b)) {
		return ""
	}
	return string(b[mark:])
}

func (s *spSandbox) logHasSince(mark int64, marker string) bool {
	return strings.Contains(s.logSince(mark), marker)
}

func (s *spSandbox) logCountAll(marker string) int {
	b, err := os.ReadFile(s.serveLog)
	if err != nil {
		return 0
	}
	return strings.Count(string(b), marker)
}

func (s *spSandbox) logTail(n int) string {
	b, err := os.ReadFile(s.serveLog)
	if err != nil {
		return "(no serve.log: " + err.Error() + ")"
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n    ")
}

// --------------------------------------------------------------- parsing --

// spSplitRow splits one tabwriter-padded list line into its 5 LOGICAL
// columns, nil for any other shape. The HOST column carries the DIALED
// endpoint (final-fixwave item-6 ripple): "10.1.51.205:7777
// (xuansi-d75884.local)" — six whitespace fields with the hostname in
// parens — beside the space-free forms "<addr>:<port>" and "-" (five
// fields). Tabwriter pads with spaces, so whitespace is the only
// column grammar; the strict closed shape IS the pollution check.
func spSplitRow(ln string) []string {
	f := strings.Fields(ln)
	switch len(f) {
	case 5:
		return f // header, "-" or bare endpoint in the HOST column
	case 6:
		if strings.HasPrefix(f[4], "(") && strings.HasSuffix(f[4], ")") {
			return []string{f[0], f[1], f[2], f[3] + " " + f[4], f[5]}
		}
	}
	return nil
}

// spCleanListTable asserts a `xuansi list` stdout is PROTOCOL-CLEAN:
// the tabwriter header first, every line a well-formed 5-column row
// (spSplitRow — the HOST column may carry the paren-hostname form),
// and zero log noise (slog lines or spawn notices must never leak into
// stdout - spawn.go's whole redirect discipline).
func spCleanListTable(h *harness, out, label string) bool {
	lines := []string{}
	if strings.TrimSpace(out) != "" {
		lines = strings.Split(strings.TrimRight(out, "\n"), "\n")
	}
	headOK := len(lines) >= 1
	if headOK {
		f := spSplitRow(lines[0])
		headOK = len(f) == 5 && f[0] == "ALIAS" && f[1] == "MAC6" && f[2] == "SOURCES" && f[3] == "HOST" && f[4] == "FW"
	}
	noise := 0
	colsOK := true
	for _, ln := range lines {
		if spSplitRow(ln) == nil {
			colsOK = false
		}
		if strings.Contains(ln, "level=") || strings.Contains(ln, "msg=") ||
			strings.Contains(ln, "spawned") || strings.Contains(ln, "xuansi serve") {
			noise++
		}
	}
	pass := headOK && colsOK && noise == 0
	h.check(pass, fmt.Sprintf("%s: stdout protocol-clean (header=%v cols=%v, %d noise line(s) over %d line(s))",
		label, headOK, colsOK, noise, len(lines)))
	return pass
}

// spExpectEndpoint is the canonical HOST-column value this driver pins
// for the bench board: the mDNS-resolved IP with the port, hostname in
// parens - exactly what the daemon's dialEndpoint emits now that the
// display shows the DIALED address (T10 / final-fixwave item-6).
func spExpectEndpoint(boardIP string) string {
	host, _, err := net.SplitHostPort(spTCPEndp)
	if err != nil {
		return boardIP + ":7777"
	}
	return boardIP + ":7777 (" + host + ")"
}

// spListRow extracts one board's row fields from a list stdout, nil if
// the board is not listed yet. Returns the 5 logical columns
// (spSplitRow), so index 3 is the full dialEndpoint and index 4 the fw
// build whatever shape the HOST column took.
func spListRow(out, mac6 string) []string {
	for _, ln := range strings.Split(out, "\n") {
		if f := spSplitRow(ln); f != nil && f[1] == mac6 && f[0] != "ALIAS" {
			return f
		}
	}
	return nil
}

// ------------------------------------------------------------------- run --

func runSpawnIdle(attempt int, daemonBin, runDir string) int {
	h := &harness{}

	fmt.Printf("=== task-7 spawn/idle full HIL - %s ===\n", time.Now().Format(time.RFC3339))
	fmt.Printf("attempt: %d (honest count - any failure is fixed and the WHOLE capture rerun)\n", attempt)
	fmt.Printf("board: %s (TCP %s; GPIO1<->GPIO2 wired pair; USB by-id present, never opened)\n", spBoard, spTCPEndp)
	fmt.Printf("story: S0 cold auto-spawn -> S1 resident holder -> S2 kill client -> S3 adopt in window -> S4 idle self-exit (15s real) ->\n")
	fmt.Printf("  S5 survivors' audit + cleanup -> S6 concurrent spawn race (one winner, loser exit 0) -> S7 second-cycle smoke\n")
	fmt.Printf("discipline: default sock resolution through the sandbox XDG env (no --sock flag anywhere); serve.log = the sandbox StateDir copy\n")

	s := spNewSandbox(h, daemonBin, runDir)
	if s == nil {
		return finishSpawnIdle(h, attempt)
	}
	defer s.restore()

	finish := func() int { return finishSpawnIdle(h, attempt) }

	// ——— S0: prelude + cold auto-spawn ————————————————————————————
	fmt.Println("\n- S0: cold sandbox, `xuansi list` auto-spawns a detached serve, stdout stays protocol-clean -")
	if _, err := os.Stat(usbByID); err == nil {
		h.note("USB by-id present (%s) - enumerated by discovery, never opened (no reboot, no USB route)", usbByID)
	} else {
		h.note("NOTE: USB by-id absent (%v) - discovery may run mdns-only this run (honest note)", err)
	}
	// T10 regression premise: the /etc/hosts board pin is GONE, so this
	// driver must not resolve the hostname either (the old preflight
	// dialed xuansi-d75884.local and silently leaned on the pin). The
	// preflight now browses mDNS (the same face the daemon uses), takes
	// the board's browse-resolved A-record address, and dials the IP
	// directly: .local resolution is nowhere on the critical path.
	if hosts, herr := os.ReadFile("/etc/hosts"); herr == nil && strings.Contains(string(hosts), "xuansi-") {
		h.check(false, "bench shape: /etc/hosts still carries a xuansi pin - remove it (the T10 regression premise) and rerun")
		return finish()
	}
	h.check(true, "bench shape: /etc/hosts carries NO xuansi pin (T10 regression premise holds)")
	mb, merr := discover.DiscoverMDNS(context.Background())
	var boardIP string
	for _, b := range mb {
		if b.MAC6 == spBoard {
			boardIP = b.IP
		}
	}
	h.check(boardIP != "", fmt.Sprintf("mDNS browse found %s with a browse-resolved IP (boards=%d err=%v) - the address every dial below rides", spBoard, len(mb), merr))
	if boardIP == "" {
		return finish()
	}
	if conn, err := net.DialTimeout("tcp", net.JoinHostPort(boardIP, "7777"), 3*time.Second); err != nil {
		h.check(false, fmt.Sprintf("board TCP face reachable at %s:7777 (mDNS-resolved IP dial): %v", boardIP, err))
		return finish()
	} else {
		conn.Close()
		h.check(true, fmt.Sprintf("board TCP face reachable at %s:7777 (mDNS-resolved IP dial, no hostname resolution; mDNS face %s)", boardIP, spTCPEndp))
	}
	if n := len(spServePIDs(s)); n != 0 {
		h.check(false, fmt.Sprintf("bench clean: no sandbox serve before the run (found %d - kill it / rerun the wrapper)", n))
		return finish()
	}
	h.note("sandbox: XDG_RUNTIME_DIR=%s (sock %s)", s.runtimeDir, s.sock)
	h.note("          XDG_STATE_HOME=%s (serve.log %s)", s.stateDir, s.serveLog)
	h.note("          XDG_DATA_HOME=%s, XDG_CONFIG_HOME=%s (zero-config boards.conf)", s.dataDir, s.configDir)

	mark0 := s.logSize() // 0 on a fresh run dir; kept for honesty on reruns
	code, out1, err1 := s.cli(h, "list-1", "list")
	h.check(code == 0, fmt.Sprintf("S0 first `xuansi list` (cold, no daemon) exit 0 (code=%d)", code))
	h.check(strings.Contains(err1, "spawned a detached xuansi serve"),
		"S0 client stderr carries the spawn notice (the auto-spawn path fired; it rides stderr, never stdout)")
	spCleanListTable(h, out1, "S0 list #1")
	_, serr := os.Stat(s.sock)
	h.check(serr == nil, fmt.Sprintf("S0 daemon sock bound at the DEFAULT path %s (stat err=%v)", s.sock, serr))
	ok, last := s.waitServes(h, 1, 15*time.Second)
	h.check(ok, fmt.Sprintf("S0 auto-spawned serve process exists, exactly one (%s)", last))
	ok, last = pollUntil(300*time.Millisecond, 15*time.Second, func() (bool, string) {
		return s.logHasSince(mark0, "daemon serving "+s.sock), "serve.log: serving line"
	})
	h.check(ok, fmt.Sprintf("S0 serve.log (sandbox StateDir) carries the serving line naming our sock (%s)", last))
	ok, last = pollUntil(500*time.Millisecond, 45*time.Second, func() (bool, string) {
		return s.logHasSince(mark0, "initial discovery sweep: 1 board(s)"), "serve.log: sweep line"
	})
	h.check(ok, fmt.Sprintf("S0 initial discovery sweep found the board (%s)", last))

	// second list dials THROUGH - no second spawn, no spawn notice
	code, out2, err2 := s.cli(h, "list-2", "list")
	h.check(code == 0 && !strings.Contains(err2, "spawned"),
		fmt.Sprintf("S0 second list dials through without spawning again (code=%d, spawn notice absent=%v)", code, !strings.Contains(err2, "spawned")))
	spCleanListTable(h, out2, "S0 list #2")
	ok, last = s.waitServes(h, 1, 5*time.Second)
	h.check(ok, fmt.Sprintf("S0 still exactly one serve after the second client (%s)", last))

	// the board row appears once the sweep + hello enrichment land
	var row []string
	ok, last = pollUntil(time.Second, 45*time.Second, func() (bool, string) {
		c, o, _ := s.cli(h, "list-poll", "list")
		row = spListRow(o, spBoard)
		if c == 0 && row != nil && row[4] != "-" {
			spCleanListTable(h, o, "S0 list poll")
			return true, fmt.Sprintf("row %v", row)
		}
		return false, fmt.Sprintf("row=%v (code=%d)", row != nil, c)
	})
	h.check(ok, fmt.Sprintf("S0 list shows %s with fw populated (%s)", spBoard, last))
	if row != nil {
		h.check(row[2] == "usb+mdns", fmt.Sprintf("S0 dual-source merged usb+mdns (sources=%s)", row[2]))
		h.check(row[3] == spExpectEndpoint(boardIP),
			fmt.Sprintf("S0 HOST column pins the dialed endpoint %q (got %q) - IP first, hostname in parens (item-6 dialEndpoint)", spExpectEndpoint(boardIP), row[3]))
		h.note("fw build in the row: %s (binding recorded in-log)", row[4])
	}
	h.check(s.logCountAll("daemon serving "+s.sock) == 1,
		fmt.Sprintf("S0 exactly ONE serving banner so far (count=%d - nobody double-started)", s.logCountAll("daemon serving "+s.sock)))

	// ——— S1: lease + resident script + resident eval client ————————
	fmt.Println("\n- S1: lease holder=spawn-h1, script_start spawnkeep, resident eval client holds ns-0 -")
	a1 := dialAPI(h, s.sock, "spawn-h1")
	if a1 == nil {
		return finish()
	}
	ler, lrerr, lerr1 := a1.call(30*time.Second, server.Request{Type: "lease", Board: spBoard, Holder: spHolder})
	var sessN int64
	if ler != nil {
		if f, ok := ler["session"].(float64); ok {
			sessN = int64(f)
		}
	}
	h.check(lerr1 == nil && lrerr == nil && ler != nil && ler["holder"] == spHolder && sessN > 0,
		fmt.Sprintf("lease holder=%s session=%d (raw apiConn; rerr=%v err=%v)", spHolder, sessN, lrerr, lerr1))
	sr, srerr, serr1 := a1.call(30*time.Second, server.Request{Type: "script_start", Board: spBoard, Holder: spHolder,
		Name: spScript, Code: spCounterCode})
	h.check(serr1 == nil && srerr == nil, fmt.Sprintf("script_start %s acked (200ms counter, lease session ns; reply %s)", spScript, oneLineJSON(sr, srerr != nil || serr1 != nil)))
	time.Sleep(800 * time.Millisecond) // let the timer tick
	er, eerr, eerr1 := a1.call(30*time.Second, server.Request{Type: "eval", Board: spBoard, Holder: spHolder, Code: "return _G.spawncnt"})
	n1 := gAny(er, "value")
	h.check(eerr1 == nil && eerr == nil && numGt(n1, 0), fmt.Sprintf("counter reads n1=%v through the lease (script resident and counting; rerr=%v err=%v)", n1, eerr, eerr1))
	// budget prelude (b5full/b7 convention): the resident eval below
	// busy-loops ~3.5s, past the default 200ms eval budget - raise it
	// to the fw's 4000ms ceiling for the S1 hold; the as-found values
	// (eval_ms=200 event_ms=50, b7 S10's restore) go back at S5
	br, brerr, brerr2 := a1.call(30*time.Second, server.Request{Type: "op", Board: spBoard, Holder: spHolder,
		Op: "budget.set", Args: map[string]any{"eval_ms": 4000, "event_ms": 500}})
	evMS, _ := br["eval_ms"].(float64)
	h.check(brerr == nil && brerr2 == nil && evMS == 4000,
		fmt.Sprintf("[prelude] budget.set eval_ms=4000 event_ms=500 for the resident hold (reply eval_ms=%v event_ms=%v; rerr=%v err=%v)", evMS, gAny(br, "event_ms"), brerr, brerr2))
	mark1 := s.logSize()
	a1.close() // the leasing conn drops: grace (board state kept) - the CLI reality of one-shot clients
	h.note("apiConn #1 closed - lease enters grace (one-shot client semantics); the resident eval client walks back in next")

	// the RESIDENT client: a real `xuansi eval` PROCESS holding a 10s
	// busy-loop eval that claims GPIO1 for ns-0 (owner _eval) - it
	// re-adopts the lease on its way in (front door) and its process
	// death is S2's subject
	residentCode := fmt.Sprintf(`bench.gpio.mode(1,"out") local t=os.clock() while os.clock()-t < %.1f do end return "resident-done"`, spResidentSecs)
	resident := s.start(h, "resident", nil, "eval", "--holder", spHolder, spBoard, residentCode)
	if resident == nil {
		return finish()
	}
	tRes := time.Now()
	time.Sleep(1200 * time.Millisecond) // connect + adopt + the busy loop is underway (killed ~1.2s into the 3.5s hold)
	ok, last = pollUntil(300*time.Millisecond, 10*time.Second, func() (bool, string) {
		return s.logHasSince(mark1, "holder "+spHolder+" adopted board "+spBoard+" back live"), "serve.log: adopt line"
	})
	h.check(ok, fmt.Sprintf("S1 resident client's eval ADOPTED the grace lease on its way in (%s)", last))

	// ——— S2: kill the client process ——————————————————————————————
	fmt.Println("\n- S2: SIGKILL the resident client mid-eval - conn dies, grace window opens -")
	mark2 := s.logSize()
	resident.kill()
	h.note("resident client SIGKILLed %.1fs after launch (mid-eval; the daemon notices the dead conn when its in-flight request returns, so the agent-gone line can trail the kill by the eval's residual time)",
		time.Since(tRes).Seconds())
	h.check(!strings.Contains(resident.stderrStr(), "budget: cpu budget exceeded"),
		"S2 the resident eval was still INSIDE the board's eval budget when killed (no `budget: cpu budget exceeded` on its stderr - a real mid-eval kill)")
	ok, last = pollUntil(300*time.Millisecond, 25*time.Second, func() (bool, string) {
		return s.logHasSince(mark2, "agent-gone holder="+spHolder), "serve.log: agent-gone line"
	})
	h.check(ok, fmt.Sprintf("S2 daemon saw the client die: agent-gone holder=%s logged, grace window open (%s)", spHolder, last))
	h.check(!strings.Contains(resident.stdoutStr(), "resident-done"),
		"S2 the killed client never received its eval value (killed mid-eval, stdout carries no result)")
	scode, sout, _ := s.cli(h, "status-grace", "status", spBoard)
	h.check(scode == 0 && strings.Contains(sout, "grace-expiring at") && strings.Contains(sout, spHolder),
		fmt.Sprintf("S2 `xuansi status` during grace prints holder=%s grace-expiring (code=%d)", spHolder, scode))
	h.check(strings.Contains(sout, spScript), fmt.Sprintf("S2 status still lists the resident script during grace (board state KEPT; scripts line contains %s)", spScript))

	// ——— S3: adopt the same holder within the window ———————————————
	fmt.Println("\n- S3: fresh holder=spawn-h1 connection ADOPTS the same lease in-window -")
	mark3 := s.logSize()
	a2 := dialAPI(h, s.sock, "h1-reconnect")
	if a2 == nil {
		return finish()
	}
	// the read eval queues behind the killed resident's residual loop
	// (VM-order evals): the ADOPT itself is host-side and immediate,
	// the reply lands when the board drains the slot
	er, eerr, eerr1 = a2.call(45*time.Second, server.Request{Type: "eval", Board: spBoard, Holder: spHolder, Code: "return _G.spawncnt"})
	n2 := gAny(er, "value")
	h.check(eerr1 == nil && eerr == nil && numGt(n2, n1),
		fmt.Sprintf("S3 same holder reconnected IN-WINDOW: eval adopted the lease and the counter kept counting n1=%v -> n2=%v (SAME live Lua VM - no reset; rerr=%v err=%v)", n1, n2, eerr, eerr1))
	h.check(s.logHasSince(mark3, "holder "+spHolder+" adopted board "+spBoard+" back live"),
		"S3 serve.log carries the adopt line (adopt, not re-acquire)")
	st, sterr, _ := a2.call(15*time.Second, server.Request{Type: "board_status", Board: spBoard})
	leaseMap, _ := st["lease"].(map[string]any)
	scripts := scriptsOf(st)
	liveAgain := sterr == nil && leaseMap != nil && leaseMap["holder"] == spHolder && leaseMap["grace_deadline"] == nil
	h.check(liveAgain, fmt.Sprintf("S3 board_status while the holder conn is open: lease LIVE again (holder=%v grace_deadline=%v)", gAny(leaseMap, "holder"), gAny(leaseMap, "grace_deadline")))
	h.check(len(scripts) == 1 && scripts[0] == spScript, fmt.Sprintf("S3 board_status scripts=%v (the resident script listed through the adopt)", scripts))
	h.check(!strings.Contains(s.logSince(mark0), "leased over USB"),
		"S3 NO USB re-dial anywhere so far (serve.log carries no USB-transport lease line - TCP warm controller throughout)")
	a2.close()
	h.note("apiConn #2 closed (the lease re-enters grace - one-shot client reality; the S4 staging supersedes this daemon anyway)")

	// ——— S4: idle self-exit (15s, staged) ——————————————————————————
	fmt.Println("\n- S4: idle self-exit - SIGINT the auto-spawned daemon, stage --idle-exit 15s, wait the REAL window -")
	mark4 := s.logSize()
	if !s.sigintServes(h, "S4 auto-spawned daemon") {
		return finish()
	}
	h.check(s.logHasSince(mark4, "board-side scripts keep running"),
		"S4 the daemon's shutdown message names the surviving-scripts semantics (what S5 proves)")
	h.check(s.logHasSince(mark4, "shutdown complete"), "S4 SIGINT shutdown completed cleanly (final log line)")

	// staging (documented): auto-spawn passes only `serve --sock`, so
	// the idle window is staged by starting the serve ourselves with
	// --idle-exit 15s --grace-ttl 15s; its output is teed into the SAME
	// sandbox serve.log so the tail assertion reads the real file
	tee, terr := os.OpenFile(s.serveLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if terr != nil {
		h.check(false, "S4 open serve.log tee: "+terr.Error())
		return finish()
	}
	d2 := s.start(h, "idle-serve", tee, "serve", "--sock", s.sock,
		"--idle-exit", spIdleWin.String(), "--grace-ttl", spIdleWin.String())
	if d2 == nil {
		tee.Close()
		return finish()
	}
	ok, last = pollUntil(300*time.Millisecond, 15*time.Second, func() (bool, string) {
		if c, derr := net.Dial("unix", s.sock); derr == nil {
			c.Close()
			return true, "sock dialable"
		}
		return false, "sock not bound yet"
	})
	h.check(ok, fmt.Sprintf("S4 staged serve (idle-exit %s) bound the sock (%s)", spIdleWin, last))
	h.check(s.logHasSince(mark4, "daemon serving "+s.sock), "S4 staged serve's serving line in serve.log (via the tee)")
	code, out4, _ := s.cli(h, "list-idle", "list")
	h.check(code == 0, fmt.Sprintf("S4 one client against the staged serve: `xuansi list` exit 0 (code=%d)", code))
	spCleanListTable(h, out4, "S0-style protocol check (list against staged serve)")

	// the client left -> the REAL 15s window, waited out for real
	tIdle := time.Now()
	exited := d2.wait(spIdleWin + 30*time.Second)
	ic, iwerr := d2.exitStats()
	h.check(exited && ic == 0 && iwerr == nil,
		fmt.Sprintf("S4 idle self-exit: staged serve exited on its own after the window (waited %v, code=%d waitErr=%v)",
			time.Since(tIdle).Round(time.Millisecond), ic, iwerr))
	tee.Close()
	ok, last = s.waitServes(h, 0, 5*time.Second)
	h.check(ok, fmt.Sprintf("S4 no serve process left (%s)", last))
	_, serr = os.Stat(s.sock)
	h.check(os.IsNotExist(serr), fmt.Sprintf("S4 socket file removed by the idle shutdown (stat err=%v)", serr))
	h.check(s.logHasSince(mark4, "idle exit"),
		"S4 serve.log tail carries the idle-exit line (the window fired with no clients)")
	h.check(s.logHasSince(mark4, "shutdown complete"), "S4 idle shutdown walked the graceful path (shutdown complete)")
	h.note("S4 serve.log tail (last 6 lines):\n    %s", asciiLine(s.logTail(6)))

	// ——— S5: survivors' audit + cleanup ————————————————————————————
	fmt.Println("\n- S5: spawn #2's startup audit lists the surviving script; then the bench is cleaned -")
	mark5 := s.logSize()
	code, _, err5 := s.cli(h, "list-3", "list")
	h.check(code == 0 && strings.Contains(err5, "spawned a detached xuansi serve"),
		fmt.Sprintf("S5 cold again: `xuansi list` auto-spawned serve #2 (code=%d, spawn notice present)", code))
	ok, last = s.waitServes(h, 1, 15*time.Second)
	h.check(ok, fmt.Sprintf("S5 exactly one serve alive (%s)", last))
	ok, last = pollUntil(500*time.Millisecond, 30*time.Second, func() (bool, string) {
		if s.logHasSince(mark5, "startup audit board "+spBoard+":") && s.logHasSince(mark5, "scripts=["+spScript+"]") {
			return true, "audit line with the survivor listed"
		}
		return false, "waiting for the startup orphan audit"
	})
	h.check(ok, fmt.Sprintf("S5 spawn #2's startup orphan audit LISTS %s - the board-side script ran through BOTH daemon deaths (%s)", spScript, last))
	h.check(s.logHasSince(mark5, "pins=1"),
		"S5 the audit also counts pins=1 - the resident eval's ns-0 GPIO1 claim survived too (owner _eval, session 0)")
	scode, sout, _ = s.cli(h, "status-audit", "status", spBoard)
	h.check(scode == 0 && strings.Contains(sout, spScript) && strings.Contains(sout, "connected: yes"),
		fmt.Sprintf("S5 live `xuansi status` against spawn #2 lists %s, connected (code=%d)", spScript, scode))
	h.note("S5 status view:\n%s", strings.ReplaceAll(strings.TrimSpace(sout), "\n", "\n    "))

	// cleanup: stop the survivor, sweep ns-0, prove the pin freed, release
	a4 := dialAPI(h, s.sock, "spawn-fin")
	if a4 == nil {
		return finish()
	}
	ler, lrerr, lerr4 := a4.call(30*time.Second, server.Request{Type: "lease", Board: spBoard, Holder: spFinal})
	h.check(lerr4 == nil && lrerr == nil && ler != nil && ler["holder"] == spFinal,
		fmt.Sprintf("S5 cleanup lease holder=%s (rerr=%v err=%v)", spFinal, lrerr, lerr4))
	stopped, stopErr, stopErr2 := a4.call(30*time.Second, server.Request{Type: "script_stop", Board: spBoard, Holder: spFinal, Name: spScript})
	if stopErr == nil && stopErr2 == nil {
		h.note("S5 script_stop %s by name through a fresh lease (reply %v)", spScript, oneLineJSON(stopped, false))
	} else {
		h.note("S5 script_stop refused (rerr=%v err=%v) - falling back to session_reset %d (the script's own session)", stopErr, stopErr2, sessN)
		rr, rerr2, rerr2b := a4.call(30*time.Second, server.Request{Type: "session_reset", Board: spBoard, Holder: spFinal, Session: int(sessN)})
		h.check(rerr2 == nil && rerr2b == nil, fmt.Sprintf("S5 session_reset %d tore the survivor down (reply %v; rerr=%v err=%v)", sessN, oneLineJSON(rr, rerr2 != nil || rerr2b != nil), rerr2, rerr2b))
	}
	st, sterr2, _ := a4.call(15*time.Second, server.Request{Type: "board_status", Board: spBoard})
	scripts = scriptsOf(st)
	h.check(sterr2 == nil && len(scripts) == 0, fmt.Sprintf("S5 survivor script gone from board_status (scripts=%v, status err=%v)", scripts, sterr2))
	rr, rrerr, rrerr2 := a4.call(30*time.Second, server.Request{Type: "session_reset", Board: spBoard, Holder: spFinal, Session: 0})
	h.check(rrerr == nil && rrerr2 == nil, fmt.Sprintf("S5 session_reset 0 swept the ns-0 _eval claims (GPIO1 freed; reply %v; rerr=%v err=%v)", oneLineJSON(rr, rrerr != nil || rrerr2 != nil), rrerr, rrerr2))
	_, werr, werr2 := a4.call(30*time.Second, server.Request{Type: "eval", Board: spBoard, Holder: spFinal, Code: `return bench.gpio.write(1,0)`})
	wrOK := werr != nil && werr.Class == "rt" && strings.Contains(werr.Msg, "not configured as output")
	wrWhy := "no error (write accepted?!)"
	if werr != nil {
		wrWhy = fmt.Sprintf("class=%s msg=%.60s", werr.Class, werr.Msg)
	} else if werr2 != nil {
		wrWhy = "call err=" + werr2.Error()
	}
	h.check(wrOK, fmt.Sprintf("S5 GPIO1 freed by the sweep: write-reject probe (%s - b5full S8's safe-state shape)", wrWhy))
	// budget restore (as-found values, b7 S10's): the S1 prelude's
	// 4000/500 goes back to 200/50 BEFORE the release (the op face is
	// lease-gated)
	brr, brrerr, brrerr2 := a4.call(30*time.Second, server.Request{Type: "op", Board: spBoard, Holder: spFinal,
		Op: "budget.set", Args: map[string]any{"eval_ms": 200, "event_ms": 50}})
	rMS, _ := brr["eval_ms"].(float64)
	h.check(brrerr == nil && brrerr2 == nil && rMS == 200,
		fmt.Sprintf("S5 budget restored eval_ms=200 event_ms=50 (board as found; reply eval_ms=%v event_ms=%v)", rMS, gAny(brr, "event_ms")))
	_, relerr, relerr2 := a4.call(30*time.Second, server.Request{Type: "release", Board: spBoard, Holder: spFinal})
	h.check(relerr == nil && relerr2 == nil, fmt.Sprintf("S5 cleanup release holder=%s (bench back to free; rerr=%v err=%v)", spFinal, relerr, relerr2))
	a4.close()
	scode, sout, _ = s.cli(h, "status-clean", "status", spBoard)
	h.check(scode == 0 && strings.Contains(sout, "(free)") && !strings.Contains(sout, spScript),
		fmt.Sprintf("S5 post-cleanup status: lease (free), %s gone (code=%d; note: status prints no scripts line at all for a zero-script board - the daemon's nil list marshals as JSON null)", spScript, scode))

	// ——— S6: concurrent spawn race ————————————————————————————————
	fmt.Println("\n- S6: kill the daemon, TWO clients the same instant - one winner, loser exits 0 -")
	mark6 := s.logSize()
	if !s.sigintServes(h, "S6 daemon #2") {
		return finish()
	}
	ra := s.start(h, "race-a", nil, "list")
	rb := s.start(h, "race-b", nil, "list")
	if ra == nil || rb == nil {
		return finish()
	}
	ka := ra.wait(90 * time.Second)
	kb := rb.wait(90 * time.Second)
	ca, _ := ra.exitStats()
	cb, _ := rb.exitStats()
	h.check(ka && kb && ca == 0 && cb == 0,
		fmt.Sprintf("S6 BOTH concurrent clients succeeded (a: waited=%v code=%d, b: waited=%v code=%d)", ka, ca, kb, cb))
	spawns := 0
	for _, p := range []*spProc{ra, rb} {
		if strings.Contains(p.stderrStr(), "spawned a detached xuansi serve") {
			spawns++
		}
	}
	h.note("S6 %d of the 2 racing clients took the spawn path (both spawn = a real bind race; 1 = the winner bound before the loser probed)", spawns)
	ok, last = s.waitServes(h, 1, 20*time.Second)
	h.check(ok, fmt.Sprintf("S6 exactly ONE serve survived the race (%s)", last))
	h.check(s.logHasSince(mark6, "daemon serving "+s.sock), "S6 the winner's serving line is in serve.log")
	if spawns == 2 {
		h.check(s.logHasSince(mark6, "already listening"),
			"S6 the racing loser serve logged `already listening ... leaving` in serve.log (the exit-0 path; its own exit code is unreapable - detached, reparented - so the deliberate double-start below proves the code)")
	}
	h.check(!s.logHasSince(mark6, "address already in use"),
		"S6 no double-bind crash shape in serve.log (the bind-as-lock window held)")

	// deterministic loser: a deliberate double-start against the live
	// winner - THIS one's exit code is observable
	loser := s.start(h, "double-start", nil, "serve", "--sock", s.sock)
	if loser == nil {
		return finish()
	}
	lw := loser.wait(15 * time.Second)
	lc, lwerr := loser.exitStats()
	h.check(lw && lc == 0 && lwerr == nil,
		fmt.Sprintf("S6 deliberate double-start against the winner: exit code 0 (waited=%v code=%d err=%v)", lw, lc, lwerr))
	h.check(strings.Contains(loser.stderrStr(), "already listening"),
		"S6 the loser's own stderr carries the `already listening ... leaving` line (task 2's ErrAlreadyListening exit-0 path)")
	ok, last = s.waitServes(h, 1, 5*time.Second)
	h.check(ok, fmt.Sprintf("S6 still exactly one serve after the double-start (%s)", last))
	ok, last = pollUntil(500*time.Millisecond, 30*time.Second, func() (bool, string) {
		return s.logHasSince(mark6, "scripts=[]"), "audit: clean board"
	})
	h.check(ok, fmt.Sprintf("S6 the winner's own startup audit shows the board CLEAN after the S5 teardown (scripts=[] in serve.log; %s)", last))
	h.note("S6 audit line context: %s", asciiLine(lastAuditLine(s.logSince(mark6))))

	// ——— S7: second-cycle smoke ———————————————————————————————————
	fmt.Println("\n- S7: second cycle against the race's winner - list, lease, eval, release -")
	code, out7, err7 := s.cli(h, "list-4", "list")
	h.check(code == 0 && !strings.Contains(err7, "spawned"),
		fmt.Sprintf("S7 list against the live winner: exit 0, no respawn (code=%d)", code))
	spCleanListTable(h, out7, "S7 list")
	row7 := spListRow(out7, spBoard)
	h.check(row7 != nil, fmt.Sprintf("S7 the board is listed by the winner daemon (row=%v)", row7))
	if row7 != nil {
		h.check(row7[3] == spExpectEndpoint(boardIP),
			fmt.Sprintf("S7 HOST column pins the dialed endpoint %q (got %q) - IP first, hostname in parens (item-6 dialEndpoint)", spExpectEndpoint(boardIP), row7[3]))
	}
	ok, _ = s.waitServes(h, 1, 5*time.Second)
	h.check(ok, "S7 still exactly one serve (no respawn needed)")
	code, lout, _ := s.cli(h, "lease-c2", "lease", "--holder", spC2, spBoard)
	h.check(code == 0 && strings.Contains(lout, "leased "+spBoard+" holder="+spC2),
		fmt.Sprintf("S7 lease --holder %s through a fresh CLI process (code=%d out=%.60s)", spC2, code, strings.TrimSpace(lout)))
	code, eout, _ := s.cli(h, "eval-c2", "eval", "--holder", spC2, spBoard, "return 6*7")
	h.check(code == 0 && strings.Contains(eout, "value: 42"),
		fmt.Sprintf("S7 eval 6*7 -> `value: 42` (code=%d out=%.40s)", code, strings.TrimSpace(eout)))
	code, rout, _ := s.cli(h, "release-c2", "release", "--holder", spC2, spBoard)
	h.check(code == 0 && strings.Contains(rout, "released "+spBoard),
		fmt.Sprintf("S7 release leaves the bench free (code=%d out=%.60s)", code, strings.TrimSpace(rout)))

	// ——— teardown: the final daemon + accounting ————————————————————
	fmt.Println("\n- teardown: SIGINT the winner, final accounting -")
	mark8 := s.logSize()
	if !s.sigintServes(h, "S7 winner daemon") {
		return finish()
	}
	h.check(s.logHasSince(mark8, "shutdown complete"), "final daemon shutdown complete (graceful, socket removed)")
	ok, last = s.waitServes(h, 0, 5*time.Second)
	h.check(ok, fmt.Sprintf("no serve processes left anywhere (%s)", last))
	h.note("serve.log total %dB, %d serving banner(s), %d shutdown(s)", s.logSize(), s.logCountAll("daemon serving "+s.sock), s.logCountAll("shutdown complete"))
	h.note("final serve.log tail (last 6 lines):\n    %s", asciiLine(s.logTail(6)))
	h.note("board left in its default state: no lease (released at S5/S7), no scripts (stopped at S5), pins swept (session_reset 0 + write-reject proof), budget restored to as-found 200/50 at S5")
	h.note("sandbox accounting: everything under %s (runtime/state/data/config) - the real user XDG state was never touched", s.run)

	return finish()
}

// lastAuditLine pulls the startup-audit line out of a serve.log slice
// (context note for the transcript).
func lastAuditLine(since string) string {
	for _, ln := range strings.Split(since, "\n") {
		if strings.Contains(ln, "startup audit board "+spBoard+":") {
			return ln
		}
	}
	return "(no audit line yet)"
}

func finishSpawnIdle(h *harness, attempt int) int {
	code := h.finish("spawnidle")
	if h.fail == 0 {
		fmt.Printf("=== ALL PASS - task-7 spawn/idle story: cold auto-spawn, kill+adopt in window, idle self-exit, concurrent race, second cycle (attempt %d, S0..S7) ===\n", attempt)
	}
	return code
}
