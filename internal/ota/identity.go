// Package ota is the host-side half of the OTA ops wave (T6, protocol
// v10): the image-identity parser (spec §4.3 — the SAME embedded bytes
// the fw reads its running header with) and the fleet push orchestration
// (spec §5 — preflight, transport over the v8 uplink lane's ota cargo,
// activate, reboot-return wait, identity triple match, per-board rows).
package ota

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// The esp_app_desc_t's frozen coordinates inside an ESP app image: the
// 256-byte descriptor sits at image offset 0x20; within it, magic_word
// at +0x00 (0xABCD5432), version[32] at +0x10 (NUL-padded project
// VERSION), app_elf_sha256[32] raw at +0x90 (image coordinates 0x30 and
// 0xB0 — the offsets the T5 HIL tool's host parser and the fw's own
// esp_app_get_description read, one identity mechanism, three uses).
const (
	appDescOff    = 0x20
	appDescLen    = 256
	appDescMagic  = 0xABCD5432
	descVerOff    = 0x10
	descVerLen    = 32
	descElfShaOff = 0x90
)

// AppDesc is the pushed image's own identity (spec §4.3): the build's
// version string and its elf sha256 (hex64). ElfShaHex is the identity
// face every OTA receipt speaks — hello's ota_sha, ota.status's
// running_sha, activate's {sha} — NOT the transport digest (the
// full-file sha256 the begin op pins; two identity faces, honestly
// distinct: a rebuild of the same version string gets a new elf sha).
type AppDesc struct {
	Version   string
	ElfShaHex string
}

// ParseAppDesc reads the image identity at the fixed offset. Refusals:
// an image too short to carry the descriptor, or a wrong magic word —
// both mean "not an ESP app image", the zero-contact bad-bin refusal
// (the fw's own esp_ota_write would reject the first frame's 0xE9 much
// later, after the claim machinery had already spun).
func ParseAppDesc(bin []byte) (AppDesc, error) {
	if len(bin) < appDescOff+appDescLen {
		return AppDesc{}, fmt.Errorf("ota: %d-byte image is too short for the 256B app_desc at offset 0x20", len(bin))
	}
	d := bin[appDescOff : appDescOff+appDescLen]
	if m := binary.LittleEndian.Uint32(d[0:4]); m != appDescMagic {
		return AppDesc{}, fmt.Errorf("ota: app_desc magic %#x at +0x20, want 0xABCD5432 — not an ESP app image", m)
	}
	ver := cstr(d[descVerOff : descVerOff+descVerLen])
	sha := hex.EncodeToString(d[descElfShaOff : descElfShaOff+32])
	if ver == "" {
		return AppDesc{}, fmt.Errorf("ota: app_desc.version is empty — a canonical build always carries the project VERSION")
	}
	return AppDesc{Version: ver, ElfShaHex: sha}, nil
}

// cstr reads a NUL-padded fixed-size string field, stopping at the
// first NUL (a field with no NUL at all reads to its bound — a
// full-width version string is legal).
func cstr(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}
