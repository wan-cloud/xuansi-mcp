package main

// spawn.go — the client-side auto-spawn: every daemon-client entry
// (dialDaemon for the CLI commands, runMCP for the agent face) first
// guarantees a daemon is listening on the socket — dial through; not
// listening → spawn a DETACHED `xuansi serve` against the same socket
// and retry the dial until it answers. The serve bind itself is the
// single-instance lock (no flock): concurrent spawners race, the
// loser's serve sees server.ErrAlreadyListening and exits 0 (task 2),
// and every client just keeps dialing until the winner answers —
// ensureDaemon never looks at the spawned serve's exit.
//
// The spawned serve's stdout AND stderr append to
// paths.StateDir()/serve.log — NEVER the client's stdio: the MCP face
// rides stdout, so one stray serve banner in the parent's stream
// would corrupt the protocol.

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"xuansi/internal/paths"
)

const (
	// serveLogName is the spawned daemon's log file under StateDir().
	serveLogName = "serve.log"
	// serveLogTailLines is how many log lines the retry-deadline
	// error quotes.
	serveLogTailLines = 5
)

// Test seams (package vars; this package's tests never run parallel):
// serveSpawn builds the spawn command, spawnDial is the liveness
// probe, spawnRetryEvery/spawnRetryFor bound the post-spawn retry
// loop (defaults: 100ms cadence, 10s window).
var (
	serveSpawn      func(sock, logPath string) *exec.Cmd = buildServeCmd
	spawnDial       func(path string) (net.Conn, error)  = dialUnix
	spawnRetryEvery                                      = 100 * time.Millisecond
	spawnRetryFor                                        = 10 * time.Second
)

// ensureBeforeDial is the wiring seam: dialDaemon and runMCP call
// through it, so existing CLI/MCP tests no-op it instead of really
// spawning a serve on their dial-failure paths (preflight ruling).
var ensureBeforeDial func(sockPath string) error = ensureDaemon

// dialUnix is the real liveness probe behind the spawnDial seam.
func dialUnix(path string) (net.Conn, error) { return net.Dial("unix", path) }

// buildServeCmd is the default serveSpawn: re-exec this binary as
// `serve --sock <sock>`, detached (per-OS detach), with stdout AND
// stderr appended to the same serve.log handle. The stdio redirect is
// unconditional — a spawned serve must never inherit the client's
// stdout, where MCP traffic may be flowing.
func buildServeCmd(sock, logPath string) *exec.Cmd {
	exe, err := os.Executable()
	if err != nil {
		exe = "" // Start() fails with "exec: no command"; ensureDaemon wraps it
	}
	cmd := exec.Command(exe, "serve", "--sock", sock)
	out := openServeLog(logPath)
	cmd.Stdout = out
	cmd.Stderr = out
	detach(cmd)
	return cmd
}

// openServeLog opens logPath O_APPEND|O_CREATE (making its directory
// first). If that fails it degrades to os.DevNull — still never the
// parent's stdio — and says so on stderr: a daemon without a log is
// better than no daemon, and an inherited stdout is never acceptable.
func openServeLog(logPath string) *os.File {
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		slog.Warn("spawn: cannot create the serve log directory; spawned output goes to devnull", "dir", dir, "err", err)
		return devNullWritable()
	}
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		slog.Warn("spawn: cannot open the serve log; spawned output goes to devnull", "path", logPath, "err", err)
		return devNullWritable()
	}
	return f
}

// devNullWritable opens os.DevNull for writing; it panics on failure
// because there is no honest fallback left — inheriting the client's
// stdio is exactly the corruption this path exists to prevent.
func devNullWritable() *os.File {
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		panic(fmt.Sprintf("xuansi: cannot open %s: %v", os.DevNull, err))
	}
	return f
}

// ensureDaemon guarantees a daemon is listening on sockPath: a dial
// that connects means one is live; otherwise it spawns a detached
// `xuansi serve` against the same socket (bind-as-lock — see the file
// comment) and retries the dial every spawnRetryEvery until
// spawnRetryFor runs out. The deadline error quotes the tail of
// serve.log so a serve that died on startup says why.
func ensureDaemon(sockPath string) error {
	if conn, err := spawnDial(sockPath); err == nil {
		conn.Close()
		return nil
	}

	logPath := filepath.Join(paths.StateDir(), serveLogName)
	cmd := serveSpawn(sockPath, logPath)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cannot spawn xuansi serve for %s: %w", sockPath, err)
	}
	slog.Info("spawned a detached xuansi serve; waiting for it to listen", "sock", sockPath, "pid", cmd.Process.Pid, "log", logPath)
	// The parent's copies of the log fd close now; the child keeps its own.
	if f, ok := cmd.Stdout.(*os.File); ok {
		f.Close()
	}
	if f, ok := cmd.Stderr.(*os.File); ok && cmd.Stdout != cmd.Stderr {
		f.Close()
	}

	for deadline := time.Now().Add(spawnRetryFor); ; {
		time.Sleep(spawnRetryEvery)
		if conn, err := spawnDial(sockPath); err == nil {
			conn.Close()
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("daemon not reachable at %s: the spawned xuansi serve never listened within %s — last %d lines of %s:\n%s",
				sockPath, spawnRetryFor, serveLogTailLines, logPath, serveLogTail(logPath, serveLogTailLines))
		}
	}
}

// serveLogTail returns the last n lines of the spawn log, or a note
// saying it is unreadable (the error message must stay honest when
// the log never materialized).
func serveLogTail(logPath string, n int) string {
	b, err := os.ReadFile(logPath)
	if err != nil {
		return fmt.Sprintf("(cannot read %s: %v)", logPath, err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
