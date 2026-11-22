package replclient

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMac6FromByID: table-driven by-id string → mac6 extraction. The
// identity is the last three octets of the colon-separated MAC segment,
// lower-cased; non-MAC by-id names and direct tty paths yield no mac6.
func TestMac6FromByID(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
		ok   bool
	}{
		{
			// the actual bench device (Goouuu ESP32-S3-CAM)
			in:   "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00",
			want: "d75884", ok: true,
		},
		{
			in:   "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00",
			want: "d75884", ok: true,
		},
		{
			// lowercase MAC letters must normalize up
			in:   "usb-Espressif_USB_JTAG_serial_debug_unit_3c:0f:02:d7:58:84-if00",
			want: "d75884", ok: true,
		},
		{
			// another Espressif MAC: mac6 is the last three octets
			in:   "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:AA:BB:CC-if00",
			want: "aabbcc", ok: true,
		},
		{
			// a non-MAC by-id name (FTDI-style): no colon group
			in: "usb-FTDI_FT232R_USB_UART_A50285BI-if00-port0", want: "", ok: false,
		},
		{
			// five octets is not a MAC
			in: "usb-X_3C:0F:02:D7:58-if00", want: "", ok: false,
		},
		{
			// non-hex octets are not a MAC
			in: "usb-X_3C:0F:02:D7:58:ZZ-if00", want: "", ok: false,
		},
		{
			// direct tty path carries no identity
			in: "/dev/ttyACM0", want: "", ok: false,
		},
	} {
		got, ok := mac6FromByID(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("mac6FromByID(%q) = (%q, %v), want (%q, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// TestResolveUSBDevice: the by-id symlink leg lives in
// serial_unix_test.go (//go:build !windows — udev by-id names carry
// MAC colons, invalid in NTFS filenames, and by-id symlink trees are
// unix device-manager semantics; first CI matrix run 42158718976).
// The portable legs (direct path, unknown name) are here and run on
// every platform: a direct existing path is returned as-is and an
// unknown name errors naming both tried candidates.
func TestResolveUSBDeviceDirectAndUnknown(t *testing.T) {
	ttyPath := filepath.Join(t.TempDir(), "ttyACM0")
	if err := os.WriteFile(ttyPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	wantTty := canonical(t, ttyPath)
	path, _, err := resolveUSBDevice(wantTty)
	if err != nil || path != wantTty {
		t.Fatalf("direct path: (%q, %v), want (%q, nil)", path, err, wantTty)
	}
	_, _, err = resolveUSBDevice("usb-No_Such_Device-if00")
	if err == nil {
		t.Fatal("unknown device resolved without error")
	}
	// The fallback candidate is printed the way the product builds it —
	// filepath.Join(byIDDir, base) — which on windows Cleans the
	// separators ("\\dev\\serial\\by-id\\…"); asserting the joined form
	// (not a literal unix spelling) keeps the check portable and pinned
	// to the construction under test (first CI matrix round 3 finding).
	for _, want := range []string{"usb-No_Such_Device-if00", filepath.Join(byIDDir, "usb-No_Such_Device-if00")} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention candidate %q", err, want)
		}
	}
}

// canonical resolves symlinks in p the way the product does — the
// expected-value twin of resolveUSBDevice's EvalSymlinks (darwin's
// /var→/private/var temp roots are the case that matters; on windows
// and plain linux dirs it is the identity).
func canonical(t *testing.T, p string) string {
	t.Helper()
	real, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatal(err)
	}
	return real
}

// TestResolveUSBDeviceCOMNameNoByIDFallback (⑤b): a Windows COM port
// name is a DIRECT device name — the udev by-id fallback must not be
// bolted onto it. The 2026-11-22 Windows incident rendered
// `usb device "COM4" not found (tried COM4, \dev\serial\by-id\COM4)`
// nine times while the board re-enumerated: a POSIX by-id prefix glued
// to a COM name is noise on every host that has COM ports. The
// not-found error names the COM name alone; by-id-style names keep the
// two-candidate fallback untouched.
func TestResolveUSBDeviceCOMNameNoByIDFallback(t *testing.T) {
	_, _, err := resolveUSBDevice("COM4")
	if err == nil {
		t.Fatal("COM4 resolved without error on a host with no such port")
	}
	if strings.Contains(err.Error(), "by-id") {
		t.Errorf("error %q bolts the by-id prefix onto a COM name", err)
	}
	if !strings.Contains(err.Error(), `"COM4" not found (tried COM4)`) {
		t.Errorf("error %q, want the bare COM candidate list", err)
	}
	// lower-case com names are COM ports too (windows names are
	// case-insensitive)
	if _, _, err := resolveUSBDevice("com4"); err == nil || strings.Contains(err.Error(), "by-id") {
		t.Errorf("com4: %v, want the same COM-shape refusal", err)
	}
	// by-id-style names keep the two-candidate fallback
	_, _, err = resolveUSBDevice("usb-No_Such_Device-if00")
	if err == nil || !strings.Contains(err.Error(), filepath.Join(byIDDir, "usb-No_Such_Device-if00")) {
		t.Errorf("by-id fallback changed shape: %v", err)
	}
}

// TestDialOptsDrainWindow: zero Drain → the 4.5s CDC default; explicit
// Drain honored; NoDrain suppresses.
func TestDialOptsDrainWindow(t *testing.T) {
	for _, tc := range []struct {
		opts DialOpts
		want time.Duration
		ok   bool
	}{
		{DialOpts{}, usbDrainDefault, true},
		{DialOpts{Drain: time.Second}, time.Second, true},
		{DialOpts{NoDrain: true}, 0, false},
		{DialOpts{Drain: time.Second, NoDrain: true}, 0, false},
	} {
		got, ok := tc.opts.drainWindow()
		if got != tc.want || ok != tc.ok {
			t.Errorf("(%v).drainWindow() = (%v, %v), want (%v, %v)", tc.opts, got, ok, tc.want, tc.ok)
		}
	}
	// The default must outlast the fw's startup: uptime-zero ≈ pulse
	// +0.8s and the REPL frame layer comes up at uptime ≈3.11s, so the
	// first hello lands at uptime ≈(window+1−3.11)s — the window needs
	// ≥4.2s for margin (see usbDrainDefault's calibration comment).
	if usbDrainDefault < 4200*time.Millisecond {
		t.Errorf("usbDrainDefault = %v, want ≥4.2s (past fw repl-up)", usbDrainDefault)
	}
}
