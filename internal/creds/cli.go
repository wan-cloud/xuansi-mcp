package creds

// cli.go — the owner-CLI's pure helpers: the xscreds host-file parser
// (the frozen line grammar of fw tools/xscreds.py — the file the
// incoming/ flow already uses, so `xuansi creds import` adopts it
// verbatim), the secret mask, and the status renderer. These render
// the ONLY views of the vault that ever reach a terminal; none of them
// emits secret material (the vector test pins that).

import (
	"fmt"
	"regexp"
	"strings"
)

// The xscreds line grammar (xscreds.py, byte-for-byte semantics):
// an SSID line and a password line, each "key : value" or "key ：value"
// (full-width colon tolerated), later lines winning. Missing file legs
// are the caller's error (the CLI names what the file lacked).
var (
	ssidRe = regexp.MustCompile(`^SSID\s*[:：]\s*(.+)$`)
	passRe = regexp.MustCompile(`^(?:密碼|密码|Password|PASS|pass)\s*[:：]\s*(.+)$`)
)

// ParseHostCreds parses one xscreds-format credential file's text into
// (ssid, psk). SSID is not classified secret (the frozen discipline);
// the PSK is — it lives in memory and the vault, never a log.
func ParseHostCreds(text string) (ssid, psk string, err error) {
	for _, raw := range strings.Split(text, "\n") {
		s := strings.TrimSpace(raw)
		if m := ssidRe.FindStringSubmatch(s); m != nil {
			ssid = strings.TrimSpace(m[1])
			continue
		}
		if m := passRe.FindStringSubmatch(s); m != nil {
			psk = strings.TrimSpace(m[1])
		}
	}
	if ssid == "" && psk == "" {
		return "", "", fmt.Errorf("no SSID and no password line found (want lines like `SSID: name` and `Password: secret`)")
	}
	if ssid == "" {
		return "", "", fmt.Errorf("no SSID line found (want a line like `SSID: name`)")
	}
	if psk == "" {
		return "", "", fmt.Errorf("no password line found (want a line like `Password: secret`, `密碼: …`, `PASS: …`)")
	}
	return ssid, psk, nil
}

// Mask renders a secret's presence without its material:
// "**** (29B)". Empty renders empty — absence shows as absence.
func Mask(secret string) string {
	if secret == "" {
		return ""
	}
	return fmt.Sprintf("**** (%dB)", len(secret))
}

// StatusText renders `xuansi creds status`'s whole output: the vault's
// landing spot, the WiFi pair's presence (ssid shown — not secret; the
// psk masked), and the device PSK's presence. NEVER material; the
// zero-material vector test pins this against a full vault.
func StatusText(dir string, v Vault) string {
	var b strings.Builder
	fmt.Fprintf(&b, "vault: %s\n", dir)
	if v.WiFiSSID != "" {
		fmt.Fprintf(&b, "wifi ssid: %s\n", v.WiFiSSID)
	} else {
		b.WriteString("wifi ssid: absent\n")
	}
	if m := Mask(v.WiFiPSK); m != "" {
		fmt.Fprintf(&b, "wifi psk: %s (never shown)\n", m)
	} else {
		b.WriteString("wifi psk: absent\n")
	}
	if v.PSKPresent() {
		fmt.Fprintf(&b, "device psk: present, %dB (daemon-generated, never shown)\n", len(v.PSKHex)/2)
	} else {
		b.WriteString("device psk: absent (the daemon generates it on first provisioning; the owner provides nothing)\n")
	}
	if !v.WiFiPresent() {
		b.WriteString("state: vault empty — run `xuansi creds set` (or `xuansi creds import <file>`) on this host to arm the zero-ritual provisioner\n")
	} else {
		b.WriteString("state: ready — credless boards sighted on USB are provisioned automatically\n")
	}
	return b.String()
}
