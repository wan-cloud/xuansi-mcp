// Package creds is the daemon's credential vault and the zero-ritual
// provisioner (憑證零儀式波 T2). The vault holds the owner-provided
// WiFi credentials (entered once through the owner CLI — `xuansi creds
// set|import` — never through any agent-visible channel) and the
// DAEMON-OWNED 256-bit device PSK (generated with crypto/rand on first
// need, never exposed: not on the MCP face, not in replies, not in
// logs — the redaction 鐵則; the firmware's sec.psk_write op is
// write-only, so even the board never reads it back).
//
// Landing spot: paths.StateDir() (Linux ~/.local/state/xuansi/ — the
// spec §5 ruling: NOT DataDir, whose captures FIFO could sweep it),
// directory 0700, files 0600:
//
//	creds.json  {"wifi_ssid": ..., "wifi_psk": ...}   owner-provided once
//	psk.hex     64 hex chars                          daemon-generated
//
// The provisioner (provision.go) consumes both: a CDC-sighted board
// with no WiFi credentials gets the vault's WiFi pair + the PSK pushed
// automatically — the plug-in-and-it-works flow; boards the daemon
// KNOWS to carry credentials receive zero contact (a CDC open reboots
// the board, and a provisioned board must never be touched).
package creds

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// pskBytes is the PSK length the frozen firmware face pins
// (sec.psk_write refuses anything but 64 hex chars = 32B).
const pskBytes = 32

// pskHexRe pins the psk.hex file's shape: exactly 64 hex chars.
var pskHexRe = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// Vault is the daemon's credential holdings at one landing spot. The
// zero value is an honest empty vault (nothing provided, nothing
// generated); the strings may be SECRET MATERIAL and must never cross
// a log line, a reply, or the agent channel (Mask/StatusText render
// the non-secret views).
type Vault struct {
	WiFiSSID string // owner-provided; SSID is not classified secret (the frozen discipline)
	WiFiPSK  string // owner-provided — SECRET
	PSKHex   string // daemon-generated — SECRET; empty until first need
}

// WiFiPresent reports the owner's WiFi pair is complete.
func (v Vault) WiFiPresent() bool { return v.WiFiSSID != "" && v.WiFiPSK != "" }

// PSKPresent reports a generated device PSK exists.
func (v Vault) PSKPresent() bool { return v.PSKHex != "" }

// Full reports the provisioner's complete kit (WiFi + PSK).
func (v Vault) Full() bool { return v.WiFiPresent() && v.PSKPresent() }

// vaultJSON is creds.json's on-disk shape.
type vaultJSON struct {
	WiFiSSID string `json:"wifi_ssid"`
	WiFiPSK  string `json:"wifi_psk"`
}

// Load reads the vault at dir. A missing file is an honest zero field
// (an absent vault is the pre-setup state, not an error); a present
// but malformed file IS an error — silently re-deriving or ignoring
// secret material would hide corruption the owner must see.
func Load(dir string) (Vault, error) {
	var v Vault
	b, err := os.ReadFile(filepath.Join(dir, "creds.json"))
	switch {
	case err == nil:
		var j vaultJSON
		if jerr := json.Unmarshal(b, &j); jerr != nil {
			return v, fmt.Errorf("creds: %s: malformed creds.json: %w", dir, jerr)
		}
		v.WiFiSSID, v.WiFiPSK = j.WiFiSSID, j.WiFiPSK
	case errors.Is(err, os.ErrNotExist):
		// absent = zero (pre-setup)
	default:
		return v, fmt.Errorf("creds: read %s: %w", filepath.Join(dir, "creds.json"), err)
	}
	pb, err := os.ReadFile(filepath.Join(dir, "psk.hex"))
	switch {
	case err == nil:
		s := strings.TrimSpace(string(pb))
		if !pskHexRe.MatchString(s) {
			return v, fmt.Errorf("creds: %s: psk.hex is not 64 hex chars — refusing to use or regenerate corrupted material (delete the file to re-arm generation)", filepath.Join(dir, "psk.hex"))
		}
		v.PSKHex = strings.ToLower(s)
	case errors.Is(err, os.ErrNotExist):
		// absent = not generated yet (first need generates)
	default:
		return v, fmt.Errorf("creds: read %s: %w", filepath.Join(dir, "psk.hex"), err)
	}
	return v, nil
}

// SaveWiFi writes the owner's WiFi pair to dir/creds.json (atomic
// tmp+rename, 0600, dir 0700). It never touches psk.hex — the PSK's
// lifecycle is the daemon's alone (owner ruling 2026-11-16).
func SaveWiFi(dir, ssid, psk string) error {
	if err := ensureDir(dir); err != nil {
		return err
	}
	b, err := json.MarshalIndent(vaultJSON{WiFiSSID: ssid, WiFiPSK: psk}, "", "  ")
	if err != nil {
		return err
	}
	return writeFile0600(filepath.Join(dir, "creds.json"), append(b, '\n'))
}

// EnsurePSK answers the daemon-owned PSK, generating it on first need:
// 32 bytes from crypto/rand, hex-encoded, persisted to dir/psk.hex.
// IDEMPOTENT BY OWNERSHIP — an existing valid psk.hex is returned
// verbatim and never regenerated (regenerating would orphan every
// already-provisioned board; the generation is once per host, for the
// vault's whole life).
func EnsurePSK(dir string) (string, error) {
	if v, err := Load(dir); err != nil {
		return "", err
	} else if v.PSKPresent() {
		return v.PSKHex, nil
	}
	raw := make([]byte, pskBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("creds: psk generation: %w", err)
	}
	s := hex.EncodeToString(raw)
	if err := ensureDir(dir); err != nil {
		return "", err
	}
	// Exclusive create: two racing first-needs must not each write a
	// different PSK — the loser's material would orphan its board. A
	// lost race re-reads the winner's file (idempotent by then).
	p := filepath.Join(dir, "psk.hex")
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return EnsurePSK(dir) // a concurrent first-need won; adopt its material
		}
		return "", fmt.Errorf("creds: create %s: %w", p, err)
	}
	if _, err := f.Write([]byte(s + "\n")); err != nil {
		f.Close()
		return "", fmt.Errorf("creds: write %s: %w", p, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("creds: close %s: %w", p, err)
	}
	return s, nil
}

// ensureDir creates dir (and parents) with the vault's 0700 — and
// enforces 0700 on a pre-existing dir too (the vault may share the
// daemon's state dir, whose earlier creation ran under a different
// umask; the enforcement is idempotent).
func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creds: mkdir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("creds: chmod %s: %w", dir, err)
	}
	return nil
}

// writeFile0600 lands contents atomically (tmp + rename) with 0600.
func writeFile0600(path string, contents []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, contents, 0o600); err != nil {
		return fmt.Errorf("creds: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("creds: rename %s: %w", path, err)
	}
	return nil
}
