//go:build windows

package paths

// osenv_windows_test.go — the live osEnv derivation, which no pure
// *For test can reach (they take an injected getenv; osEnv IS the
// live one). This was exactly the parked Task-1 review gap: osEnv had
// no coverage anywhere, and its Windows branch silently sourced
// LOCALAPPDATA one level high (filepath.Dir of os.UserCacheDir, which
// on Windows already IS %LocalAppData% — no \Cache suffix), landing
// sock/state/data under AppData\xuansi instead of
// AppData\Local\xuansi on the real machine (the T9 live finding).
// Runs on any Windows host; the controller cross-compiles and runs it
// on the T9 machine.

import (
	"os"
	"strings"
	"testing"
)

// TestOsEnvWindowsLocalAppData pins the contract against machine
// truth: the derived LOCALAPPDATA equals os.UserCacheDir() verbatim
// (the accessor's Windows definition is Getenv("LocalAppData")) and,
// when the env var is really set, equals it too — no Dir() stripping
// the "Local" component.
func TestOsEnvWindowsLocalAppData(t *testing.T) {
	env := osEnv()

	ucd, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("os.UserCacheDir unavailable (%v); the fallback branch covers this host", err)
	}
	if got := env("LOCALAPPDATA"); got != ucd {
		t.Fatalf("osEnv()(LOCALAPPDATA) = %q, want os.UserCacheDir() = %q verbatim — no filepath.Dir", got, ucd)
	}
	if real := os.Getenv("LOCALAPPDATA"); real != "" {
		if got := env("LOCALAPPDATA"); got != real {
			t.Fatalf("osEnv()(LOCALAPPDATA) = %q, want the real env var %q", got, real)
		}
		// the machine truth keeps the Local component — one lowercase
		// check, tolerant of case, that would have caught the bug:
		// AppData\xuansi (Dir-stripped) has no \Local\ anywhere.
		if !strings.Contains(strings.ToLower(real), `\local`) {
			t.Logf("note: LOCALAPPDATA=%q has no \\Local component on this host — unusual but respected", real)
		}
	} else {
		t.Log("LOCALAPPDATA not set; UserCacheDir equality above still pins the derivation")
	}

	// non-LOCALAPPDATA keys pass through to the live env untouched.
	t.Setenv("XUANSI_TEST_OSENV_PASSTHROUGH", "1")
	if got := env("XUANSI_TEST_OSENV_PASSTHROUGH"); got != "1" {
		t.Fatalf("osEnv passthrough for other keys = %q, want 1", got)
	}

	// End-to-end through the public wrappers — the live shape that
	// broke on the machine: every landing sits under
	// %LOCALAPPDATA%\xuansi, not one level high. (winJoin tolerates a
	// trailing separator on the base; the expectations match.)
	if real := os.Getenv("LOCALAPPDATA"); real != "" {
		base := strings.TrimSuffix(real, `\`)
		if got, want := SockPath(), base+`\xuansi\daemon.sock`; got != want {
			t.Errorf("SockPath() = %q, want %q", got, want)
		}
		if got, want := StateDir(), base+`\xuansi\state`; got != want {
			t.Errorf("StateDir() = %q, want %q", got, want)
		}
		if got, want := DataDir(), base+`\xuansi`; got != want {
			t.Errorf("DataDir() = %q, want %q", got, want)
		}
	}
}
