package creds

// cli_test.go — the owner-CLI helpers' boardless face: the xscreds host
// file format parser (fw tools/xscreds.py's exact line grammar), the
// secret mask, and the status renderer's NEVER-PRINTS-MATERIAL vector
// (every string that crosses stdout is asserted free of the dummy
// secrets — the redaction 鐵則's CLI face).

import (
	"strings"
	"testing"
)

func TestParseHostCreds(t *testing.T) {
	cases := []struct {
		name   string
		text   string
		ssid   string
		psk    string
		failed bool
	}{
		{"ascii colon", "SSID: bench-ap-dummy\nPassword: dummy-pass\n", "bench-ap-dummy", "dummy-pass", false},
		{"full-width colon", "SSID：全角dummy\n密碼：密碼dummy\n", "全角dummy", "密碼dummy", false},
		{"simplified keys", "SSID : spaced\n密码: pw\n", "spaced", "pw", false},
		{"pass variants", "SSID: s\nPASS: p1\npass: p2\n", "s", "p2", false}, // last wins, python twin
		{"noise lines skipped", "# comment\n\nSSID: s\n密碼: p\nextra line\n", "s", "p", false},
		{"missing psk", "SSID: only-ssid\n", "", "", true},
		{"missing ssid", "Password: only-pass\n", "", "", true},
		{"empty file", "", "", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ssid, psk, err := ParseHostCreds(c.text)
			if c.failed {
				if err == nil {
					t.Fatalf("ParseHostCreds(%q) = %q/%q, want an error", c.text, ssid, psk)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseHostCreds(%q): %v", c.text, err)
			}
			if ssid != c.ssid || psk != c.psk {
				t.Fatalf("ParseHostCreds(%q) = %q/%q, want %q/%q", c.text, ssid, psk, c.ssid, c.psk)
			}
		})
	}
}

func TestMask(t *testing.T) {
	if m := Mask(dummyPSK); m != "**** (29B)" {
		t.Fatalf("Mask = %q, want %q", m, "**** (29B)")
	}
	if m := Mask(""); m != "" {
		t.Fatalf("Mask(\"\") = %q, want empty (absence renders as absence)", m)
	}
}

// TestStatusTextZeroMaterial is the CLI face's redaction vector: the
// status output of a FULL vault names the ssid and the presence/size
// of every secret, but never the material itself.
func TestStatusTextZeroMaterial(t *testing.T) {
	dir := t.TempDir()
	if err := SaveWiFi(dir, dummySSID, dummyPSK); err != nil {
		t.Fatal(err)
	}
	psk, err := EnsurePSK(dir)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := StatusText(dir, v)
	if !strings.Contains(out, dummySSID) {
		t.Fatalf("status must show the ssid (not secret): %q", out)
	}
	if !strings.Contains(out, "**** (29B)") {
		t.Fatalf("status must carry the masked wifi psk: %q", out)
	}
	for _, secret := range []string{dummyPSK, psk} {
		if secret == "" {
			continue
		}
		if strings.Contains(out, secret) {
			t.Fatalf("status output leaked secret material %q: %q", secret, out)
		}
		// a long secret's tail must not survive a partial render either
		if len(secret) > 8 && strings.Contains(out, secret[len(secret)-8:]) {
			t.Fatalf("status output leaked the secret's tail: %q", out)
		}
	}
}

func TestStatusTextAbsentLegs(t *testing.T) {
	out := StatusText(t.TempDir(), Vault{})
	if strings.Contains(out, "****") {
		t.Fatalf("an empty vault must not render a masked-secret line: %q", out)
	}
	if !strings.Contains(out, "absent") {
		t.Fatalf("an empty vault must say absent honestly: %q", out)
	}
}
