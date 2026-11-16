package main

// creds_test.go — the owner CLI's boardless face (憑證零儀式波 T2):
// `xuansi creds set|import|status` against a temp vault dir, with the
// redaction vector (the dummy PSK never crosses stdout/stderr) and the
// xscreds-format import. All material is dummy.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"xuansi/internal/creds"
)

// runCredsArgs is the argv shape every test uses: the subcommand first,
// --dir right after it (Go's flag parsing stops at the first positional,
// so the vault pin must precede import's file argument).
func runCredsArgs(t *testing.T, dir string, args ...string) (string, string, int) {
	t.Helper()
	var out, errOut strings.Builder
	argv := append([]string{"creds", args[0], "--dir", dir}, args[1:]...)
	code := run(nil, &out, &errOut, argv)
	return out.String(), errOut.String(), code
}

func writeDummyCredsFile(t *testing.T, dir, text string) string {
	t.Helper()
	p := filepath.Join(dir, "dummy-creds.txt")
	if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCredsImportParsesAndStores(t *testing.T) {
	dir := t.TempDir()
	f := writeDummyCredsFile(t, t.TempDir(), "SSID: bench-ap-dummy\n密碼: dummy-wifi-pass-29-bytes-1234\n")
	out, errOut, code := runCredsArgs(t, dir, "import", f)
	if code != exitOK {
		t.Fatalf("exit = %d, out=%q err=%q", code, out, errOut)
	}
	v, err := creds.Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if v.WiFiSSID != "bench-ap-dummy" || v.WiFiPSK != "dummy-wifi-pass-29-bytes-1234" {
		t.Fatalf("vault = %q/%q, want the imported pair", v.WiFiSSID, v.WiFiPSK)
	}
	if !strings.Contains(out, "bench-ap-dummy") {
		t.Fatalf("import must echo the ssid: %q", out)
	}
	if !strings.Contains(out, "**** (29B)") {
		t.Fatalf("import must render the masked psk: %q", out)
	}
	if strings.Contains(out+errOut, "dummy-wifi-pass-29-bytes-1234") {
		t.Fatalf("import output leaked the psk material: %q %q", out, errOut)
	}
}

func TestCredsImportBadFile(t *testing.T) {
	dir := t.TempDir()
	f := writeDummyCredsFile(t, t.TempDir(), "nothing usable here\n")
	_, errOut, code := runCredsArgs(t, dir, "import", f)
	if code == exitOK {
		t.Fatal("an unparseable file must fail")
	}
	if !strings.Contains(errOut, "SSID") {
		t.Fatalf("the error must name what the file lacked: %q", errOut)
	}
	if _, errOut2, code2 := runCredsArgs(t, dir, "import", "/nonexistent/creds.txt"); code2 == exitOK {
		_ = errOut2
		t.Fatal("a missing file must fail")
	}
}

func TestCredsStatusNeverPrintsMaterial(t *testing.T) {
	dir := t.TempDir()
	if err := creds.SaveWiFi(dir, "bench-ap-dummy", "dummy-wifi-pass-29-bytes-1234"); err != nil {
		t.Fatal(err)
	}
	psk, err := creds.EnsurePSK(dir)
	if err != nil {
		t.Fatal(err)
	}
	out, errOut, code := runCredsArgs(t, dir, "status")
	if code != exitOK {
		t.Fatalf("exit = %d (out=%q err=%q)", code, out, errOut)
	}
	if !strings.Contains(out, "bench-ap-dummy") || !strings.Contains(out, "**** (29B)") {
		t.Fatalf("status must name the ssid and the masked psk: %q", out)
	}
	for _, secret := range []string{"dummy-wifi-pass-29-bytes-1234", psk} {
		if strings.Contains(out+errOut, secret) {
			t.Fatalf("status leaked material: %q %q", out, errOut)
		}
	}
}

func TestCredsSetPiped(t *testing.T) {
	dir := t.TempDir()
	// piped (non-tty) stdin: the reads fall back to plain lines — the
	// echo concern is moot off a terminal, and this is the test seam.
	var out, errOut strings.Builder
	code := run(strings.NewReader("bench-ap-dummy\ndummy-wifi-pass-29-bytes-1234\n"), &out, &errOut,
		[]string{"creds", "set", "--dir", dir})
	if code != exitOK {
		t.Fatalf("exit = %d (out=%q err=%q)", code, out.String(), errOut.String())
	}
	v, err := creds.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if v.WiFiSSID != "bench-ap-dummy" || v.WiFiPSK != "dummy-wifi-pass-29-bytes-1234" {
		t.Fatalf("vault = %q/%q, want the entered pair", v.WiFiSSID, v.WiFiPSK)
	}
	if strings.Contains(out.String()+errOut.String(), "dummy-wifi-pass-29-bytes-1234") {
		t.Fatalf("set output leaked the psk: out=%q err=%q", out.String(), errOut.String())
	}
}

func TestCredsUsageAndUnknown(t *testing.T) {
	var out, errOut strings.Builder
	if code := run(nil, &out, &errOut, []string{"creds"}); code != exitUsage {
		t.Fatalf("bare creds exit = %d, want usage", code)
	}
	if !strings.Contains(errOut.String(), "set") || !strings.Contains(errOut.String(), "import") || !strings.Contains(errOut.String(), "status") {
		t.Fatalf("usage must name the three subcommands: %q", errOut.String())
	}
	if code := run(nil, &out, &errOut, []string{"creds", "frobnicate"}); code != exitUsage {
		t.Fatalf("unknown subcommand exit = %d, want usage", code)
	}
}
