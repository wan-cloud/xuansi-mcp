// Package paths resolves the daemon's per-OS filesystem landing spots:
// the API socket, the state directory, and the data directory. The
// public wrappers (SockPath/StateDir/DataDir) read the live
// environment; the *For pure functions take goos + getenv (+ home) so
// the Windows and macOS resolutions are testable on any builder, and
// the Linux branch is BYTE-IDENTICAL to the inline logic it absorbed
// (server.DefaultSockPath / capturesRoot / mcp.wsRoot).
package paths

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// SockPath is the daemon's API socket (a file, not a directory).
func SockPath() string { return sockPathFor(runtime.GOOS, osEnv()) }

// StateDir is the daemon's state directory; callers join their own
// file names onto it.
func StateDir() string { return stateDirFor(runtime.GOOS, osEnv(), homeDir()) }

// DataDir is the daemon's data directory (captures, workspace);
// callers join their own file names onto it.
func DataDir() string { return dataDirFor(runtime.GOOS, osEnv(), homeDir()) }

// osEnv is the live env lookup the wrappers hand the pure functions.
// On Windows %LOCALAPPDATA% is sourced through os.UserCacheDir — the
// sanctioned accessor returns <LocalAppData>\Cache there, so its
// parent is the value — with the raw env var as fallback when even
// that fails; elsewhere it is plain os.Getenv.
func osEnv() func(string) string {
	if runtime.GOOS != "windows" {
		return os.Getenv
	}
	lad := ""
	if d, err := os.UserCacheDir(); err == nil {
		lad = filepath.Dir(d)
	} else {
		lad = os.Getenv("LOCALAPPDATA")
	}
	return func(key string) string {
		if key == "LOCALAPPDATA" {
			return lad
		}
		return os.Getenv(key)
	}
}

// homeDir is the wrapper's home source ("" when unavailable); the pure
// functions never touch os.UserHomeDir themselves.
func homeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home
}

// sockPathFor is the pure socket resolution, keyed on goos: Linux
// $XDG_RUNTIME_DIR/xuansi/daemon.sock with the /tmp/xuansi-daemon.sock
// fallback (byte-identical to the pre-paths DefaultSockPath); Windows
// %LOCALAPPDATA%\xuansi\daemon.sock; macOS $TMPDIR/xuansi/daemon.sock.
func sockPathFor(goos string, getenv func(string) string) string {
	switch goos {
	case "windows":
		if d := getenv("LOCALAPPDATA"); d != "" {
			return winJoin(d, "xuansi", "daemon.sock")
		}
		return winJoin("xuansi", "daemon.sock")
	case "darwin":
		if t := getenv("TMPDIR"); t != "" {
			return filepath.Join(t, "xuansi", "daemon.sock")
		}
		return filepath.Join("/tmp", "xuansi", "daemon.sock")
	default: // linux — and anything unrecognized keeps today's behavior
		if x := getenv("XDG_RUNTIME_DIR"); x != "" {
			return filepath.Join(x, "xuansi", "daemon.sock")
		}
		return "/tmp/xuansi-daemon.sock"
	}
}

// stateDirFor is the pure state-directory resolution, keyed on goos
// (home injected; "" = unavailable): Linux $XDG_STATE_HOME/xuansi or
// ~/.local/state/xuansi with the relative fallback when home is
// unknown; Windows %LOCALAPPDATA%\xuansi\state; macOS
// ~/Library/Logs/xuansi.
func stateDirFor(goos string, getenv func(string) string, home string) string {
	switch goos {
	case "windows":
		if d := getenv("LOCALAPPDATA"); d != "" {
			return winJoin(d, "xuansi", "state")
		}
		return winJoin("xuansi", "state")
	case "darwin":
		return filepath.Join(home, "Library", "Logs", "xuansi")
	default: // linux — XDG state convention, relative fallback when home is unknown
		if s := getenv("XDG_STATE_HOME"); s != "" {
			return filepath.Join(s, "xuansi")
		}
		if home == "" {
			return filepath.Join(".local", "state", "xuansi")
		}
		return filepath.Join(home, ".local", "state", "xuansi")
	}
}

// dataDirFor is the pure data-directory resolution, keyed on goos
// (home injected; "" = unavailable): Linux $XDG_DATA_HOME/xuansi or
// ~/.local/share/xuansi with the relative fallback when home is
// unknown (byte-identical to the pre-paths capturesRoot/wsRoot
// prefix); Windows %LOCALAPPDATA%\xuansi; macOS
// ~/Library/Application Support/xuansi.
func dataDirFor(goos string, getenv func(string) string, home string) string {
	switch goos {
	case "windows":
		if d := getenv("LOCALAPPDATA"); d != "" {
			return winJoin(d, "xuansi")
		}
		return "xuansi"
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "xuansi")
	default: // linux — XDG data convention, relative fallback when home is unknown
		if d := getenv("XDG_DATA_HOME"); d != "" {
			return filepath.Join(d, "xuansi")
		}
		if home == "" {
			return filepath.Join(".local", "share", "xuansi")
		}
		return filepath.Join(home, ".local", "share", "xuansi")
	}
}

// winJoin joins with backslash separators regardless of the host OS —
// the pure goos-keyed functions must yield the Windows spelling on a
// Linux builder too — tolerating a trailing separator on the base.
func winJoin(base string, rest ...string) string {
	p := strings.TrimSuffix(base, `\`)
	for _, r := range rest {
		p += `\` + r
	}
	return p
}
