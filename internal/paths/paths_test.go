package paths

// paths_test.go — the pure per-OS resolution functions against an
// injected env/home (no build tags, no live environment), so every
// landing is pinnable from this Linux builder. The Linux rows are
// BYTE-IDENTICAL pins of the pre-paths inline logic
// (server.DefaultSockPath / capturesRoot / mcp.wsRoot); the Windows
// and macOS rows pin the new landings (%LOCALAPPDATA%, ~/Library).

import "testing"

// fakeEnv adapts a plain map into the getenv seam the pure functions
// take; a missing key is an unset variable.
func fakeEnv(kv map[string]string) func(string) string {
	return func(key string) string { return kv[key] }
}

func TestSockPathFor(t *testing.T) {
	cases := []struct {
		name string
		goos string
		env  map[string]string
		want string
	}{
		{"linux XDG_RUNTIME_DIR set", "linux",
			map[string]string{"XDG_RUNTIME_DIR": "/r"}, "/r/xuansi/daemon.sock"},
		{"linux XDG_RUNTIME_DIR unset falls back to /tmp (byte-identical)", "linux",
			map[string]string{}, "/tmp/xuansi-daemon.sock"},
		{"windows LOCALAPPDATA set", "windows",
			map[string]string{"LOCALAPPDATA": `C:\L`}, `C:\L\xuansi\daemon.sock`},
		{"darwin TMPDIR set", "darwin",
			map[string]string{"TMPDIR": "/t/"}, "/t/xuansi/daemon.sock"},
	}
	for _, c := range cases {
		if got := sockPathFor(c.goos, fakeEnv(c.env)); got != c.want {
			t.Errorf("%s: sockPathFor(%q) = %q, want %q", c.name, c.goos, got, c.want)
		}
	}
}

func TestStateDirFor(t *testing.T) {
	cases := []struct {
		name string
		goos string
		env  map[string]string
		home string
		want string
	}{
		{"linux XDG_STATE_HOME set", "linux",
			map[string]string{"XDG_STATE_HOME": "/s"}, "/h", "/s/xuansi"},
		{"linux unset, home known", "linux",
			map[string]string{}, "/h", "/h/.local/state/xuansi"},
		{"linux unset, home unavailable keeps the relative fallback", "linux",
			map[string]string{}, "", ".local/state/xuansi"},
		{"windows LOCALAPPDATA set", "windows",
			map[string]string{"LOCALAPPDATA": `C:\L`}, "", `C:\L\xuansi\state`},
		{"darwin home known", "darwin",
			map[string]string{}, "/h", "/h/Library/Logs/xuansi"},
	}
	for _, c := range cases {
		if got := stateDirFor(c.goos, fakeEnv(c.env), c.home); got != c.want {
			t.Errorf("%s: stateDirFor(%q, home=%q) = %q, want %q", c.name, c.goos, c.home, got, c.want)
		}
	}
}

func TestDataDirFor(t *testing.T) {
	cases := []struct {
		name string
		goos string
		env  map[string]string
		home string
		want string
	}{
		{"linux XDG_DATA_HOME set (byte-identical)", "linux",
			map[string]string{"XDG_DATA_HOME": "/d"}, "/h", "/d/xuansi"},
		{"linux unset, home known (byte-identical)", "linux",
			map[string]string{}, "/h", "/h/.local/share/xuansi"},
		{"linux unset, home unavailable keeps the relative fallback", "linux",
			map[string]string{}, "", ".local/share/xuansi"},
		{"windows LOCALAPPDATA set", "windows",
			map[string]string{"LOCALAPPDATA": `C:\L`}, "", `C:\L\xuansi`},
		{"darwin home known", "darwin",
			map[string]string{}, "/h", "/h/Library/Application Support/xuansi"},
	}
	for _, c := range cases {
		if got := dataDirFor(c.goos, fakeEnv(c.env), c.home); got != c.want {
			t.Errorf("%s: dataDirFor(%q, home=%q) = %q, want %q", c.name, c.goos, c.home, got, c.want)
		}
	}
}
