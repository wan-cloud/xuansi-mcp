package ota

// identity_test.go — the image-identity parser's boardless vectors
// (OTA ops wave T6, spec §4.3): the esp_app_desc_t at fixed offset 0x20
// of an ESP app image — magic 0xABCD5432, version[32] at desc+0x10,
// app_elf_sha256[32] raw at desc+0x90 (the same offsets the fw reads
// its own running header with and the T5 HIL tool's host parser pinned:
// ver@0x30, elf-sha@0xB0 in image coordinates).

import (
	"encoding/binary"
	"strings"
	"testing"
)

// mkImage builds a minimal plausible image: the 0xE9 image magic, a
// 256-byte app_desc at 0x20 with the given version and elf sha bytes.
func mkImage(ver string, elfSha []byte) []byte {
	bin := make([]byte, 0x20+256)
	bin[0] = 0xE9 // the ESP image magic (esp_ota_write's own guard)
	d := bin[0x20:]
	binary.LittleEndian.PutUint32(d[0:4], 0xABCD5432)
	copy(d[0x10:0x30], ver) // version[32], NUL-padded
	copy(d[0x90:0xB0], elfSha)
	return bin
}

func TestParseAppDescGood(t *testing.T) {
	sha := []byte{0x34, 0x6e, 0x99, 0x62} // first bytes of the canonical 346e9962… build
	sha = append(sha, make([]byte, 28)...)
	bin := mkImage("0.6.0", sha)
	d, err := ParseAppDesc(bin)
	if err != nil {
		t.Fatalf("ParseAppDesc: %v", err)
	}
	if d.Version != "0.6.0" {
		t.Fatalf("version = %q, want 0.6.0", d.Version)
	}
	want := "346e996200000000000000000000000000000000000000000000000000000000"
	if d.ElfShaHex != want {
		t.Fatalf("elf sha = %q, want %q", d.ElfShaHex, want)
	}
}

func TestParseAppDescVerNulTerminated(t *testing.T) {
	// a version string filling all 32 bytes with no NUL must not bleed
	// into the fields that follow — the parser stops at the field bound
	long := strings.Repeat("7", 32)
	bin := mkImage(long, make([]byte, 32))
	d, err := ParseAppDesc(bin)
	if err != nil {
		t.Fatalf("ParseAppDesc: %v", err)
	}
	if d.Version != long {
		t.Fatalf("version = %q, want the full 32-byte field", d.Version)
	}
}

func TestParseAppDescShortImage(t *testing.T) {
	full := mkImage("0.6.0", make([]byte, 32))
	for _, cut := range []int{0, 1, 0x20, 0x20 + 255} {
		if _, err := ParseAppDesc(full[:cut]); err == nil {
			t.Fatalf("a %d-byte image must be refused (the 256B desc at 0x20 does not fit)", cut)
		}
	}
}

func TestParseAppDescBadMagic(t *testing.T) {
	bin := mkImage("0.6.0", make([]byte, 32))
	binary.LittleEndian.PutUint32(bin[0x20:0x24], 0xDEADBEEF)
	_, err := ParseAppDesc(bin)
	if err == nil {
		t.Fatal("a wrong app_desc magic must be refused")
	}
	if !strings.Contains(err.Error(), "magic") {
		t.Fatalf("err = %v, want the magic named", err)
	}
}

func TestParseAppDescNoTruncateSideEffect(t *testing.T) {
	// the parser must read, never rewrite: a padded 33rd byte past the
	// version field stays whatever the image carried
	bin := mkImage("1.2.3", make([]byte, 32))
	bin[0x20+0x30] = 'X' // first byte of project_name
	if _, err := ParseAppDesc(bin); err != nil {
		t.Fatalf("ParseAppDesc: %v", err)
	}
	if bin[0x20+0x30] != 'X' {
		t.Fatal("the image bytes must be untouched")
	}
}
