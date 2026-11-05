package main

// spawn_test.go — the client auto-spawn path (spawn.go) against REAL
// unix sockets and REAL helper processes: the liveness probe is a real
// net.Dial, the stand-in daemon really listens and holds conns, and
// the failure-tail helper is a real spawned process that writes the
// marker cross-process before dying. The package seams (serveSpawn,
// spawnDial, spawnRetryEvery/For) are swapped per test and restored at
// cleanup; no test here spawns a real `xuansi serve` (the preflight
// ruling — existing CLI tests no-op the ensureBeforeDial wiring seam
// instead).

import (
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"xuansi/internal/paths"
)

// spawnFailHelperEnv carries the log path to the failure-tail helper
// subprocess (TestXuansiSpawnFailHelperProcess below).
const spawnFailHelperEnv = "XUANSI_SPAWN_FAIL_HELPER_LOG"

// spawnFailMarker is the line the failure-tail helper writes into the
// serve log before exiting non-zero; the retry-deadline error must
// quote it.
const spawnFailMarker = "xuansi-spawn-fail-marker: cannot bind, dying"

// noOpCmd is a startable, instantly-exiting stand-in command: the
// seam's real job (listening) is played by the goroutine beside it —
// ensureDaemon never reads the spawned process's exit (bind-as-lock).
func noOpCmd() *exec.Cmd {
	return exec.Command(os.Args[0], "-test.run=^$")
}

// shrinkSpawnRetries points the retry loop at test scale.
func shrinkSpawnRetries(t *testing.T, every, window time.Duration) {
	t.Helper()
	origEvery, origFor := spawnRetryEvery, spawnRetryFor
	spawnRetryEvery, spawnRetryFor = every, window
	t.Cleanup(func() { spawnRetryEvery, spawnRetryFor = origEvery, origFor })
}

// overrideSpawn swaps the serveSpawn seam for the test's duration.
func overrideSpawn(t *testing.T, f func(sock, logPath string) *exec.Cmd) {
	t.Helper()
	orig := serveSpawn
	serveSpawn = f
	t.Cleanup(func() { serveSpawn = orig })
}

// overrideDial wraps the spawnDial seam: the dial stays REAL (a true
// unix connect), the wrapper only records that one got through.
func overrideDial(t *testing.T, record *bool) {
	t.Helper()
	orig := spawnDial
	spawnDial = func(path string) (net.Conn, error) {
		c, err := net.Dial("unix", path)
		if err == nil && record != nil {
			*record = true
		}
		return c, err
	}
	t.Cleanup(func() { spawnDial = orig })
}

// quietSlog keeps ensureDaemon's spawn notices out of the test log.
func quietSlog(t *testing.T) {
	t.Helper()
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })
}

// listenAndHold is the stand-in daemon: after delay it listens on the
// unix sock and holds every accepted conn open (a probe dial must find
// a live peer); the bound channel closes once the sock is really
// listening (or failed to), and the listener closes at cleanup.
func listenAndHold(t *testing.T, sock string, delay time.Duration) <-chan struct{} {
	t.Helper()
	bound := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		time.Sleep(delay)
		ln, err := net.Listen("unix", sock)
		if err != nil {
			close(bound)
			return
		}
		defer ln.Close()
		go func() { <-done; ln.Close() }()
		close(bound)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 256)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return bound
}

// TestEnsureDaemonSpawnsAndDials: a dead socket spawns (via the
// serveSpawn seam) a helper that listens 150ms in; ensureDaemon
// retries the REAL dial until it connects, returns nil, and handed
// the seam the same sock plus the serve.log path under StateDir().
func TestEnsureDaemonSpawnsAndDials(t *testing.T) {
	quietSlog(t)
	sock := filepath.Join(t.TempDir(), "spawn.sock")
	shrinkSpawnRetries(t, 10*time.Millisecond, 5*time.Second)
	bound := listenAndHold(t, sock, 150*time.Millisecond)

	var gotSock, gotLog string
	overrideSpawn(t, func(sock, logPath string) *exec.Cmd {
		gotSock, gotLog = sock, logPath
		return noOpCmd()
	})
	var dialed bool
	overrideDial(t, &dialed)

	if err := ensureDaemon(sock); err != nil {
		t.Fatalf("ensureDaemon: %v", err)
	}
	<-bound // the stand-in daemon really bound the sock
	if !dialed {
		t.Fatal("spawnDial never got a connection through")
	}
	if gotSock != sock {
		t.Fatalf("serveSpawn received sock %q, want %q", gotSock, sock)
	}
	if want := filepath.Join(paths.StateDir(), serveLogName); gotLog != want {
		t.Fatalf("serveSpawn received log path %q, want %q", gotLog, want)
	}
}

// TestEnsureDaemonNoSpawnWhenAlive: a live daemon passes straight
// through — the dial succeeds and serveSpawn must never fire.
func TestEnsureDaemonNoSpawnWhenAlive(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "alive.sock")
	bound := listenAndHold(t, sock, 0)
	<-bound // the stand-in daemon is really listening before we probe
	spawned := false
	overrideSpawn(t, func(sock, logPath string) *exec.Cmd {
		spawned = true
		return noOpCmd()
	})
	// spawnDial stays the real net.Dial.

	if err := ensureDaemon(sock); err != nil {
		t.Fatalf("ensureDaemon: %v", err)
	}
	if spawned {
		t.Fatal("serveSpawn fired although the daemon was already alive")
	}
}

// TestEnsureDaemonFailureTail: the spawned helper dies non-zero
// without ever listening; after the retry window the error names the
// sock and quotes the marker line the helper wrote into the serve
// log. XDG_STATE_HOME redirects StateDir() (linux resolution) into the
// test's temp dir so the marker never touches the real serve.log.
func TestEnsureDaemonFailureTail(t *testing.T) {
	quietSlog(t)
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	sock := filepath.Join(t.TempDir(), "dead.sock")
	shrinkSpawnRetries(t, 5*time.Millisecond, time.Second)

	overrideSpawn(t, func(sock, logPath string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=TestXuansiSpawnFailHelperProcess")
		cmd.Env = append(os.Environ(), spawnFailHelperEnv+"="+logPath)
		return cmd
	})
	// spawnDial stays the real net.Dial — against a socket nothing
	// ever binds.

	err := ensureDaemon(sock)
	if err == nil {
		t.Fatal("ensureDaemon succeeded; want the retry-deadline failure")
	}
	wantLog := filepath.Join(state, "xuansi", serveLogName)
	for _, want := range []string{sock, wantLog, spawnFailMarker} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err.Error(), want)
		}
	}
}

// TestXuansiSpawnFailHelperProcess is not a test: it is the body of
// the fake spawned serve — writes the marker into the serve log, then
// exits non-zero WITHOUT ever listening (the exact shape of a serve
// that fails before binding).
func TestXuansiSpawnFailHelperProcess(t *testing.T) {
	logPath := os.Getenv(spawnFailHelperEnv)
	if logPath == "" {
		t.Skip("helper body: runs only as a spawned subprocess")
	}
	// a real spawned serve finds its log dir made by the spawner's
	// buildServeCmd; this helper has no spawner side, so it makes its
	// own (idempotent) before writing
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		os.Exit(4)
	}
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(4)
	}
	if _, err := f.WriteString(spawnFailMarker + "\n"); err != nil {
		os.Exit(4)
	}
	f.Close()
	os.Exit(3)
}

// TestSpawnRedirectsStdio: the command the default serveSpawn builds
// sends stdout AND stderr to the SAME serve.log handle (opened
// O_APPEND), never the client's stdio — proven behaviorally: another
// writer appends "B" first, then this still-at-offset-0 handle's "A"
// must land AFTER it, which only O_APPEND guarantees.
func TestSpawnRedirectsStdio(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "state", "xuansi", serveLogName) // nested: MkdirAll is part of the deal
	sock := filepath.Join(dir, "stdio.sock")

	cmd := serveSpawn(sock, logPath)
	t.Cleanup(func() {
		if f, ok := cmd.Stdout.(*os.File); ok {
			f.Close()
		}
		if f, ok := cmd.Stderr.(*os.File); ok && cmd.Stdout != cmd.Stderr {
			f.Close()
		}
	})

	if len(cmd.Args) < 4 || cmd.Args[1] != "serve" || cmd.Args[2] != "--sock" || cmd.Args[3] != sock {
		t.Fatalf("cmd.Args = %v, want [exe serve --sock %s]", cmd.Args, sock)
	}
	out, ok := cmd.Stdout.(*os.File)
	if !ok {
		t.Fatalf("cmd.Stdout is %T, want *os.File (never nil: the spawn must not inherit stdio)", cmd.Stdout)
	}
	if out.Name() != logPath {
		t.Fatalf("stdout goes to %q, want %q", out.Name(), logPath)
	}
	errf, ok := cmd.Stderr.(*os.File)
	if !ok {
		t.Fatalf("cmd.Stderr is %T, want *os.File", cmd.Stderr)
	}
	if errf != out {
		t.Fatal("stdout and stderr are different handles; want the same serve.log file")
	}

	// O_APPEND, behaviorally (see the comment above).
	other, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open second writer: %v", err)
	}
	if _, err := other.WriteString("B\n"); err != nil {
		t.Fatalf("second writer: %v", err)
	}
	other.Close()
	if _, err := out.WriteString("A\n"); err != nil {
		t.Fatalf("cmd's handle write: %v", err)
	}
	got, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if string(got) != "B\nA\n" {
		t.Fatalf("log = %q, want %q — the handle is not O_APPEND", got, "B\nA\n")
	}
}
