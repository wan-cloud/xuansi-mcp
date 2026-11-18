package ota

// fwdist_test.go — the fw distribution manifest's parse contract (dist
// wave D4): field validation is the compat gate's data source, so a
// malformed manifest refuses before anything downloads.

import (
	"strings"
	"testing"
)

func TestParseFwManifestValid(t *testing.T) {
	m, err := ParseFwManifest([]byte(`{
		"fw_version": "0.7.1", "fw_build": "abc1234def56789abc1234def56789abc1234def",
		"protocol_ver": 10,
		"ota_sha": "346e996200000000000000000000000000000000000000000000000000000000",
		"bin_sha256": "ebe70c1997d9706872f2f32f09dcfc871ac1a45a192efd7b0ff9f8bb32b06f46",
		"bin_bytes": 1514024, "min_daemon": "0.6.0"
	}`))
	if err != nil {
		t.Fatalf("valid manifest refused: %v", err)
	}
	if m.FwVersion != "0.7.1" || m.ProtocolVer != 10 || m.BinBytes != 1514024 || m.MinDaemon != "0.6.0" {
		t.Fatalf("fields = %+v", m)
	}
}

func TestParseFwManifestInvalid(t *testing.T) {
	bad := []struct {
		name string
		body string
		want string
	}{
		{"empty version", `"fw_version": ""`, "fw_version"},
		{"short build", `"fw_build": "abc"`, "fw_build"},
		{"bad ota_sha", `"ota_sha": "xyz"`, "ota_sha"},
		{"bad bin sha", `"bin_sha256": "0"`, "bin_sha256"},
		{"zero bytes", `"bin_bytes": 0`, "bin_bytes"},
		{"over cap", `"bin_bytes": 99999999`, "bin"},
		{"zero protocol", `"protocol_ver": 0`, "protocol_ver"},
		{"bad min_daemon", `"min_daemon": "x.y"`, "min_daemon"},
		{"letters version", `"fw_version": "x.y.z"`, "fw_version"},
		{"v-prefixed version", `"fw_version": "v0.7.1"`, "fw_version"},
		{"empty middle part", `"fw_version": "0..1"`, "fw_version"},
		{"trailing space", `"fw_version": "0.7.1 "`, "fw_version"},
		{"v-prefixed min_daemon", `"min_daemon": "v1.2.3"`, "min_daemon"},
	}
	for _, b := range bad {
		body := `{"fw_version":"0.7.1","fw_build":"abc1234def56789abc1234def56789abc1234def","protocol_ver":10,` +
			`"ota_sha":"346e996200000000000000000000000000000000000000000000000000000000",` +
			`"bin_sha256":"ebe70c1997d9706872f2f32f09dcfc871ac1a45a192efd7b0ff9f8bb32b06f46",` +
			`"bin_bytes":1514024,"min_daemon":"0.6.0"}`
		// splice the bad field over its good twin
		good := map[string]string{
			"empty version": `"fw_version":"0.7.1"`, "short build": `"fw_build":"abc1234def56789abc1234def56789abc1234def"`,
			"bad ota_sha": `"ota_sha":"346e996200000000000000000000000000000000000000000000000000000000"`,
			"bad bin sha": `"bin_sha256":"ebe70c1997d9706872f2f32f09dcfc871ac1a45a192efd7b0ff9f8bb32b06f46"`,
			"zero bytes":  `"bin_bytes":1514024`, "over cap": `"bin_bytes":1514024`,
			"zero protocol": `"protocol_ver":10`, "bad min_daemon": `"min_daemon":"0.6.0"`,
			"letters version": `"fw_version":"0.7.1"`, "v-prefixed version": `"fw_version":"0.7.1"`,
			"empty middle part": `"fw_version":"0.7.1"`, "trailing space": `"fw_version":"0.7.1"`,
			"v-prefixed min_daemon": `"min_daemon":"0.6.0"`,
		}[b.name]
		if b.name == "over cap" {
			body = strings.Replace(body, `"bin_bytes":1514024`, `"bin_bytes":99999999`, 1)
		} else {
			body = strings.Replace(body, good, b.body, 1)
		}
		if _, err := ParseFwManifest([]byte(body)); err == nil || !strings.Contains(err.Error(), b.want) {
			t.Errorf("%s: want refusal naming %q, got %v", b.name, b.want, err)
		}
	}
}
