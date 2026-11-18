// Package ota — fwdist.go: the fw DISTRIBUTION manifest (dist wave
// D4/D5). The images repo (wan-cloud/xuansi-fw-images) publishes one
// manifest.json per fw tag; this file is the daemon-side parse face
// and the protocol ceiling the compat warning reads.
package ota

import (
	"encoding/json"
	"fmt"
	"strings"
)

// SupportedProtocolVer is the highest fw protocol face THIS daemon
// build speaks. Bump ritual: the same wave that re-binds the daemon
// to a new fw face (the HIL re-bind discipline) moves the constant —
// a manifest above it only warns (fetch != flash), never blocks.
const SupportedProtocolVer = 10

// fwBinMax bounds one fw image download — the ota slot size (3 MiB)
// plus packaging slack; anything larger can neither stage nor flash.
const fwBinMax = 4 << 20

// FwManifest is the images repo's per-tag manifest.json. Identity
// fields mirror the fw's own app_desc (fw_build = the SOURCE commit —
// the canonical-anchor ruling 2026-11-16; ota_sha = the elf sha256).
type FwManifest struct {
	FwVersion   string `json:"fw_version"`
	FwBuild     string `json:"fw_build"`
	ProtocolVer int    `json:"protocol_ver"`
	OtaSha      string `json:"ota_sha"`
	BinSha256   string `json:"bin_sha256"`
	BinBytes    int64  `json:"bin_bytes"`
	MinDaemon   string `json:"min_daemon"`
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// semverOK is the loose x.y.z shape check (numbers only) — the compat
// comparison itself is server-side.
func semverOK(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return false
	}
	for _, p := range parts {
		if p == "" || len(p) > 9 {
			for _, c := range p {
				if c < '0' || c > '9' {
					return false
				}
			}
		}
	}
	return true
}

// ParseFwManifest decodes and validates one manifest body: every field
// is load-bearing for the compat gate or the sha-pinned download, so
// a malformed manifest refuses by name before anything downloads.
func ParseFwManifest(b []byte) (*FwManifest, error) {
	var m FwManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("fw manifest: %v", err)
	}
	switch {
	case m.FwVersion == "" || !semverOK(m.FwVersion):
		return nil, fmt.Errorf("fw manifest: fw_version %q must be x.y.z", m.FwVersion)
	case len(m.FwBuild) < 7:
		return nil, fmt.Errorf("fw manifest: fw_build %q too short (the source commit id)", m.FwBuild)
	case m.ProtocolVer <= 0:
		return nil, fmt.Errorf("fw manifest: protocol_ver %d invalid", m.ProtocolVer)
	case !isHex64(m.OtaSha):
		return nil, fmt.Errorf("fw manifest: ota_sha must be 64 hex chars")
	case !isHex64(m.BinSha256):
		return nil, fmt.Errorf("fw manifest: bin_sha256 must be 64 hex chars")
	case m.BinBytes <= 0 || m.BinBytes > fwBinMax:
		return nil, fmt.Errorf("fw manifest: bin_bytes %d outside 1..%d (the ota slot bound)", m.BinBytes, fwBinMax)
	case m.MinDaemon == "" || !semverOK(m.MinDaemon):
		return nil, fmt.Errorf("fw manifest: min_daemon %q must be x.y.z", m.MinDaemon)
	}
	return &m, nil
}

// CmpSemver orders two x.y.z strings (-1/0/+1); non-semver inputs
// compare equal (the caller treats unknown as "cannot say").
func CmpSemver(a, b string) int {
	ap, bp := semverParts(a), semverParts(b)
	for i := 0; i < 3; i++ {
		if ap[i] != bp[i] {
			if ap[i] < bp[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func semverParts(s string) [3]int {
	var out [3]int
	for i, p := range strings.SplitN(s, ".", 3) {
		if i > 2 {
			break
		}
		n := 0
		for _, c := range p {
			if c < '0' || c > '9' {
				return out
			}
			n = n*10 + int(c-'0')
		}
		out[i] = n
	}
	return out
}
