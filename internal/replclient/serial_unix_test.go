//go:build !windows

package replclient

import (
	"os"
	"path/filepath"
	"testing"
)

// TestResolveUSBDevice (unix): by-id basename and full path both
// resolve through the by-id dir (symlink canonicalized to the real tty
// node). Unix-only fixture (first CI matrix run 42158718976): udev
// by-id names carry MAC colons — invalid in NTFS filenames, so the
// windows run died with "filename syntax is incorrect" at fixture
// build — and by-id symlink trees are unix device-manager semantics;
// the portable legs (direct path, unknown name) live in serial_test.go
// and run everywhere.
//
// The expected path is canonicalized with EvalSymlinks the same way
// the product canonicalizes: on darwin t.TempDir() lives under
// /var/folders, a symlink the kernel resolves to /private/var/folders
// — comparing the literal fixture path failed the first macos matrix
// run.
func TestResolveUSBDevice(t *testing.T) {
	dir := t.TempDir()
	old := byIDDir
	byIDDir = filepath.Join(dir, "serial", "by-id")
	t.Cleanup(func() { byIDDir = old })

	// fixture tree: by-id/usb-…-if00 → ../../ttyACM0 (relative symlink,
	// shaped like the real udev layout)
	byIDPath := filepath.Join(byIDDir, "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00")
	if err := os.MkdirAll(byIDDir, 0o755); err != nil {
		t.Fatal(err)
	}
	ttyPath := filepath.Join(dir, "ttyACM0")
	if err := os.WriteFile(ttyPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../ttyACM0", byIDPath); err != nil {
		t.Fatal(err)
	}
	wantTty := canonical(t, ttyPath)

	base := filepath.Base(byIDPath)
	for _, in := range []string{byIDPath, base} {
		path, mac6, err := resolveUSBDevice(in)
		if err != nil {
			t.Fatalf("resolveUSBDevice(%q): %v", in, err)
		}
		if path != wantTty {
			t.Errorf("resolveUSBDevice(%q) path = %q, want canonical %q", in, path, wantTty)
		}
		if mac6 != "d75884" {
			t.Errorf("resolveUSBDevice(%q) mac6 = %q, want d75884", in, mac6)
		}
	}
}
