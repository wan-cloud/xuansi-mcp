package creds

// vault_test.go — the credential vault's boardless face (憑證零儀式波
// T2): Load/Save round-trip, the landing-spot permissions (0700/0600 —
// the vault is the host's secret holdings), and EnsurePSK's idempotent
// 256-bit generation (存在即不重生成 — the PSK is daemon-owned for its
// whole lifecycle; a regeneration would orphan every already-provisioned
// board). ALL material here is dummy.

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	dummySSID = "bench-ap-dummy"
	dummyPSK  = "dummy-wifi-pass-29-bytes-1234" // 29B dummy, never real
)

func TestVaultSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := SaveWiFi(dir, dummySSID, dummyPSK); err != nil {
		t.Fatalf("SaveWiFi: %v", err)
	}
	v, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if v.WiFiSSID != dummySSID || v.WiFiPSK != dummyPSK {
		t.Fatalf("round-trip = %q/%q, want the saved pair", v.WiFiSSID, v.WiFiPSK)
	}
	if !v.WiFiPresent() {
		t.Fatal("WiFiPresent = false after a full save")
	}
}

func TestVaultEmptyDirLoadsZero(t *testing.T) {
	v, err := Load(t.TempDir())
	if err != nil {
		t.Fatalf("Load on an empty dir must be a zero Vault, not an error: %v", err)
	}
	if v.WiFiPresent() || v.PSKPresent() || v.Full() {
		t.Fatalf("empty dir loaded non-zero vault: %+v", v)
	}
}

func TestVaultPermissions(t *testing.T) {
	dir := t.TempDir()
	if err := SaveWiFi(dir, dummySSID, dummyPSK); err != nil {
		t.Fatalf("SaveWiFi: %v", err)
	}
	if _, err := EnsurePSK(dir); err != nil {
		t.Fatalf("EnsurePSK: %v", err)
	}
	di, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := di.Mode().Perm(); perm != 0o700 {
		t.Fatalf("vault dir mode = %o, want 0700", perm)
	}
	for _, name := range []string{"creds.json", "psk.hex"} {
		fi, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if perm := fi.Mode().Perm(); perm != 0o600 {
			t.Fatalf("%s mode = %o, want 0600", name, perm)
		}
	}
}

func TestEnsurePSKIdempotent(t *testing.T) {
	dir := t.TempDir()
	a, err := EnsurePSK(dir)
	if err != nil {
		t.Fatalf("first EnsurePSK: %v", err)
	}
	b, err := EnsurePSK(dir)
	if err != nil {
		t.Fatalf("second EnsurePSK: %v", err)
	}
	if a != b {
		t.Fatal("EnsurePSK regenerated: the PSK must be generated once and stay stable (daemon-owned lifecycle)")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "psk.hex"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(raw)) != a {
		t.Fatal("psk.hex on disk drifted from the answered value")
	}
	// a THIRD call must not rewrite the file either (mtime-stable is
	// untestable portably; byte-stable is the honest proxy and the
	// file was just proven equal — now prove a fresh load agrees).
	v, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if v.PSKHex != a {
		t.Fatalf("Load saw %q, want the generated %q", v.PSKHex, a)
	}
}

func TestEnsurePSKShape(t *testing.T) {
	a, err := EnsurePSK(t.TempDir())
	if err != nil {
		t.Fatalf("EnsurePSK: %v", err)
	}
	if len(a) != 64 {
		t.Fatalf("psk hex len = %d, want 64 (256-bit)", len(a))
	}
	if _, err := hex.DecodeString(a); err != nil {
		t.Fatalf("psk hex not decodable: %v", err)
	}
}

func TestEnsurePSKKeepsExistingFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "psk.hex"), []byte(strings.Repeat("ab", 32)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := EnsurePSK(dir)
	if err != nil {
		t.Fatalf("EnsurePSK: %v", err)
	}
	if a != strings.Repeat("ab", 32) {
		t.Fatalf("EnsurePSK overwrote an existing psk.hex (%q) — it must keep what is there", a)
	}
}

func TestLoadMalformedCredsJSON(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "creds.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("malformed creds.json must be an error, not a silent zero vault")
	}
}

func TestLoadRejectsOddPSKHex(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "psk.hex"), []byte("zz"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("a psk.hex that is not 64 hex chars must be an error (a corrupted PSK must not be silently re-derived or used)")
	}
}
