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
)

// spawnFailHelperEnv carries the log path to the failure-tail helper
// subprocess (TestXuansiSpawnFailHelperProcess below).
const spawnFailHelperEnv = "XUANSI_SPAWN_FAIL_HELPER_LOG"

// spawnFailMarker is the line the failure-tail helper writes into the
// serve log before exiting non-zero; the retry-deadline error must
// quote it.
const spawnFailMarker = "xuansi-spawn-fail-marker: cannot bind, dying"

// testExe resolves this test binary's path for the re-exec helpers
// below. os.Args[0] is a bare RELATIVE name when the binary is invoked
// bare, and Windows refuses to run an executable resolved relative to
// the current directory ("cannot run executable found relative to
// current directory" — the T9 finding); production spawn.go already
// uses os.Executable(), the test seam must match it. os.Args[0] is the
// documented fallback when Executable fails.
func testExe() string {
	if exe, err := os.Executable(); err == nil {
		return exe
	}
	return os.Args[0]
}

// noOpCmd is a startable, instantly-exiting stand-in command: the
// seam's real job (listening) is played by the goroutine beside it —
// ensureDaemon never reads the spawned process's exit (bind-as-lock).
func noOpCmd() *exec.Cmd {
	return exec.Command(testExe(), "-test.run=^$")
}

// shrinkSpawnRetries points the retry loop at test scale.
func shrinkSpawnRetries(t *testing.T, every, window time.Duration) {
	t.Helper()
	origEvery, origFor := spawnRetryEvery, spawnRetryFor
	spawnRetryEvery, spawnRetryFor = every, window
	t.Cleanup(func() { spawnRetryEvery, spawnRetryFor = origEvery, origFor })
}

// sandboxStateDir points the spawnStateDir seam at a fresh sandbox
// dir and returns it: the spawn tests must never read or write the
// REAL user state dir on any OS, and env-key redirection cannot do
// the job — XDG_STATE_HOME only covers the linux resolution while
// Windows' rides LOCALAPPDATA, which is how the FailureTail marker
// landed in the real AppData serve.log (the T9 Windows finding).
func sandboxStateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	orig := spawnStateDir
	spawnStateDir = func() string { return dir }
	t.Cleanup(func() { spawnStateDir = orig })
	return dir
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
// the seam the same sock plus the serve.log path under the injected
// sandbox state dir.
func TestEnsureDaemonSpawnsAndDials(t *testing.T) {
	quietSlog(t)
	state := sandboxStateDir(t)
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
	if want := filepath.Join(state, serveLogName); gotLog != want {
		t.Fatalf("serveSpawn received log path %q, want %q", gotLog, want)
	}
}

// TestEnsureDaemonNoSpawnWhenAlive: a live daemon passes straight
// through — the dial succeeds and serveSpawn must never fire.
func TestEnsureDaemonNoSpawnWhenAlive(t *testing.T) {
	sandboxStateDir(t) // even a mistaken spawn must stay off the real state dir
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
// log. The serve.log dir comes from the injected sandbox state dir —
// never the real user state dir, and via the seam rather than env
// keys: XDG_STATE_HOME only redirects the linux resolution, so on
// Windows the marker used to land in the real AppData serve.log
// (the T9 finding).
func TestEnsureDaemonFailureTail(t *testing.T) {
	quietSlog(t)
	state := sandboxStateDir(t)
	sock := filepath.Join(t.TempDir(), "dead.sock")
	shrinkSpawnRetries(t, 5*time.Millisecond, time.Second)

	overrideSpawn(t, func(sock, logPath string) *exec.Cmd {
		cmd := exec.Command(testExe(), "-test.run=TestXuansiSpawnFailHelperProcess")
		cmd.Env = append(os.Environ(), spawnFailHelperEnv+"="+logPath)
		return cmd
	})
	// spawnDial stays the real net.Dial — against a socket nothing
	// ever binds.

	err := ensureDaemon(sock)
	if err == nil {
		t.Fatal("ensureDaemon succeeded; want the retry-deadline failure")
	}
	wantLog := filepath.Join(state, serveLogName)
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

// TestStartServeRetryFlow pins startServe's two-attempt shape with
// OS-neutral fakes (the real trigger — a kill-on-close job denying
// CREATE_BREAKAWAY_FROM_JOB — is windows-only; its classifier has its
// own windows-tagged test): a Start error the hook flags as survivable
// triggers exactly ONE rebuild at attempt 1 and succeeds there; a
// declined or absent hook surfaces the original error with no rebuild.
// No real serve ever spawns — attempt 0 is a nonexistent binary, the
// rebuild is a noOpCmd.
func TestStartServeRetryFlow(t *testing.T) {
	quietSlog(t)
	state := sandboxStateDir(t)
	sock := filepath.Join(t.TempDir(), "flow.sock")
	logPath := filepath.Join(state, serveLogName)
	failing := func(sock, logPath string) *exec.Cmd {
		return exec.Command(filepath.Join(t.TempDir(), "no-such-xuansi-binary"))
	}

	t.Run("hook flags error: one plainer rebuild wins", func(t *testing.T) {
		overrideSpawn(t, failing)
		origHook := spawnRetryOnStartErr
		spawnRetryOnStartErr = func(error) bool { return true }
		t.Cleanup(func() { spawnRetryOnStartErr = origHook })

		var attempts []int
		cmd, err := startServe(sock, logPath, func(sock, logPath string, attempt int) *exec.Cmd {
			attempts = append(attempts, attempt)
			return noOpCmd()
		})
		if err != nil {
			t.Fatalf("startServe: %v", err)
		}
		if len(attempts) != 1 || attempts[0] != 1 {
			t.Fatalf("rebuild attempts = %v, want exactly [1]", attempts)
		}
		if cmd == nil || cmd.Process == nil {
			t.Fatal("startServe returned no started command")
		}
	})

	t.Run("hook declines: original error, no rebuild", func(t *testing.T) {
		overrideSpawn(t, failing)
		origHook := spawnRetryOnStartErr
		spawnRetryOnStartErr = func(error) bool { return false }
		t.Cleanup(func() { spawnRetryOnStartErr = origHook })

		called := false
		_, err := startServe(sock, logPath, func(sock, logPath string, attempt int) *exec.Cmd {
			called = true
			return noOpCmd()
		})
		if err == nil {
			t.Fatal("startServe succeeded; want the original Start error")
		}
		if called {
			t.Fatal("rebuild ran although the hook declined")
		}
	})

	t.Run("no hook: never retries", func(t *testing.T) {
		overrideSpawn(t, failing)
		origHook := spawnRetryOnStartErr
		spawnRetryOnStartErr = nil
		t.Cleanup(func() { spawnRetryOnStartErr = origHook })

		called := false
		_, err := startServe(sock, logPath, func(sock, logPath string, attempt int) *exec.Cmd {
			called = true
			return noOpCmd()
		})
		if err == nil {
			t.Fatal("startServe succeeded; want the original Start error")
		}
		if called {
			t.Fatal("rebuild ran although no hook is wired (non-windows shape)")
		}
	})
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
