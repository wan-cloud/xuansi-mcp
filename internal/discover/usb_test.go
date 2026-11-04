package discover

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// setUSBSeams points enumeration at a fixture tree and a fake live-port
// list, restoring both on cleanup.
func setUSBSeams(t *testing.T, byID string, live []string) {
	t.Helper()
	oldDir, oldPorts := byIDDir, serialPorts
	byIDDir, serialPorts = byID, func() ([]string, error) { return live, nil }
	t.Cleanup(func() { byIDDir, serialPorts = oldDir, oldPorts })
}

// makeByIDTree builds a /dev-shaped fixture: real tty nodes plus a
// by-id dir of symlinks covering every enumeration branch (live
// Espressif boards, a non-Espressif serial device, a stale symlink
// whose node vanished, a phantom by-id name resolving to a node the
// serial enquirer no longer lists, and a garbage name).
func makeByIDTree(t *testing.T) (root, byID string) {
	t.Helper()
	root = t.TempDir()
	byID = filepath.Join(root, "by-id")
	if err := os.MkdirAll(byID, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"ttyACM0", "ttyACM1", "ttyUSB0", "ttyACM9"} {
		if err := os.WriteFile(filepath.Join(root, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	links := map[string]string{
		// live Espressif boards (the bench pair: d75884 + a second unit)
		"usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00": "../ttyACM0",
		"usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:1A:2B:3C-if00": "../ttyACM1",
		// non-Espressif serial device with a MAC-like group: ignored by the filter
		"usb-FTDI_FT232R_USB_Serial_A903O5D3-if00-port0": "../ttyUSB0",
		// stale symlink: the target node is gone entirely
		"usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:DE:AD:BE:EF-if00": "../ttyGONE",
		// phantom: symlink resolves, but the serial enquirer does not list the node
		"usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:0A:0B:0C:0D-if00": "../ttyACM9",
		// garbage name (no vendor shape): ignored
		"garbage-name": "../ttyACM0",
	}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(byID, name)); err != nil {
			t.Fatal(err)
		}
	}
	return root, byID
}

func TestMac6FromByID(t *testing.T) {
	cases := []struct {
		name   string
		byID   string
		want6  string
		wantOK bool
	}{
		// the live bench board's actual by-id name
		{"live bench", "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00", "d75884", true},
		// other interface number of the same unit — MAC group is what matters
		{"other if", "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if02", "d75884", true},
		// MAC embedded mid-name (suffix after the group)
		{"mac embedded", "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:1A:2B:3C-if00-port0", "1a2b3c", true},
		// lowercase MAC letters tolerated
		{"lowercase hex", "usb-Espressif_USB_JTAG_serial_debug_unit_3c:0f:02:d7:58:84-if00", "d75884", true},
		// parsing is vendor-agnostic: a MAC group in a non-Espressif name still
		// parses — vendor filtering happens before this, in DiscoverUSB
		{"non espressif but mac6", "usb-FTDI_FT232R_3C:0F:02:AA:BB:CC-if00-port0", "aabbcc", true},
		// garbage: no six-octet group anywhere
		{"no mac group", "usb-Espressif_USB_JTAG_serial_debug_unit-if00", "", false},
		{"plain tty", "ttyACM0", "", false},
		{"empty", "", "", false},
		// five octets is not a MAC
		{"five octets", "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58-if00", "", false},
	}
	for _, c := range cases {
		got6, gotOK := mac6FromByID(c.byID)
		if got6 != c.want6 || gotOK != c.wantOK {
			t.Errorf("%s: mac6FromByID(%q) = (%q,%v), want (%q,%v)", c.name, c.byID, got6, gotOK, c.want6, c.wantOK)
		}
	}
}

func TestDiscoverUSB(t *testing.T) {
	root, byID := makeByIDTree(t)
	setUSBSeams(t, byID, []string{
		filepath.Join(root, "ttyACM0"),
		filepath.Join(root, "ttyACM1"),
		filepath.Join(root, "ttyUSB0"),
	})
	boards, err := DiscoverUSB(context.Background())
	if err != nil {
		t.Fatalf("DiscoverUSB: %v", err)
	}
	want := []Board{
		{MAC6: "1a2b3c", Sources: []string{"usb"}, USBPath: filepath.Join(byID, "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:1A:2B:3C-if00")},
		{MAC6: "d75884", Sources: []string{"usb"}, USBPath: filepath.Join(byID, "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00")},
	}
	if !reflect.DeepEqual(boards, want) {
		t.Errorf("DiscoverUSB =\n %#v\nwant\n %#v", boards, want)
	}
}

func TestDiscoverUSBNoByIDDir(t *testing.T) {
	setUSBSeams(t, filepath.Join(t.TempDir(), "does-not-exist"), nil)
	boards, err := DiscoverUSB(context.Background())
	if err != nil || len(boards) != 0 {
		t.Errorf("missing by-id dir: got (%#v, %v), want (empty, nil)", boards, err)
	}
}

func TestDiscoverUSBCanceled(t *testing.T) {
	setUSBSeams(t, "/dev/serial/by-id", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := DiscoverUSB(ctx); err == nil {
		t.Error("canceled ctx: want error, got nil")
	}
}
