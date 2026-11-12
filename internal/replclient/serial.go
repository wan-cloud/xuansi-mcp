package replclient

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.bug.st/serial"
)

// usbDrainDefault is the CDC open window, measured from the RTS-pulse
// release that reboots the board (§2: one connection = one device
// session). Calibration against the live board (b310378, ring-log
// t= values aligned to host clock): chip uptime-zero sits ~0.8s after
// the pulse release, the fw's app_main sleeps 3s before bringing the
// REPL frame layer up at uptime ≈3.11s, and the boot console prefix
// (~2.6KB ROM + IDF lines, 3781B measured) ends ~1.1s in. The window
// must therefore end comfortably AFTER repl-up: 4.5s puts the first
// hello at uptime ≈3.7s — past the measured startup race where a frame
// buffered before repl-up is mis-dispatched (phantom observer role,
// conn left un-hello'd; see the task-8 report). The frozen page's
// "drain ≥3.5s" minimum is respected with margin.
const usbDrainDefault = 4500 * time.Millisecond

// byIDDir is where udev exposes stable USB device names; a var so tests
// can point resolution at a fixture tree.
var byIDDir = "/dev/serial/by-id"

// bootCaptureKeep is how much of the drain window the BootCapture hook
// retains (the TAIL of the window — the freshest boot banner): ~4KB per
// the D1 ruling (a full banner measures ~2.6KB ROM prefix + IDF boot
// lines; 4KB covers it with margin).
const bootCaptureKeep = 4096

// DialOpts parameterize the dialers. The zero value is the CDC default:
// a 4.5s drain window (usbDrainDefault — past the fw's repl-up, see its
// calibration comment). NoDrain suppresses the window entirely — the TCP
// dialer never drains (§13: no reboot, no boot prefix, the first frame
// may be hello) and behaves as if NoDrain were set. Log, when non-nil,
// receives human-readable transport lines for HIL transcripts.
//
// BootCapture (B16-T6 D1), when non-nil, receives the LAST ~4KB of the
// CDC drain window on every open — refreshed per open (one connection =
// one device session = one banner). The drain window's contents after a
// CDC open are NECESSARILY the boot banner of the reboot the open caused
// (the zero-false-positive capture source, spec §4D); TCP dials never
// fire it (no reboot semantics). The callback runs on the dialing
// goroutine before the client is returned; it must be quick and
// panic-free.
type DialOpts struct {
	Drain       time.Duration
	NoDrain     bool
	Log         func(format string, args ...any)
	BootCapture func(raw []byte)
}

func (o DialOpts) logf(format string, args ...any) {
	if o.Log != nil {
		o.Log(format, args...)
	}
}

// drainWindow resolves the effective CDC absorb window: (0, false) when
// draining is suppressed.
func (o DialOpts) drainWindow() (time.Duration, bool) {
	if o.NoDrain {
		return 0, false
	}
	if o.Drain <= 0 {
		return usbDrainDefault, true
	}
	return o.Drain, true
}

// DialUSB connects to a board over USB CDC. byIdOrPath is a
// /dev/serial/by-id name (full path or bare basename) or a direct
// device path; by-id names re-resolve on every dial, surviving
// re-enumeration across the reboot each open causes. Opening pulses
// the board into a fresh device session (§2 — the reboot IS the
// connection semantics, not a side effect to suppress), and the drain
// window that follows absorbs the boot ROM prefix so the frame stream
// starts pure. The returned client has not said hello: role and
// identity are the caller's policy (Hello, §5/§12). A client dialed
// this way carries a redial route for Reconnect.
func DialUSB(byIdOrPath string, o DialOpts) (*Client, error) {
	link, err := dialUSBLink(byIdOrPath, o)
	if err != nil {
		return nil, err
	}
	c := NewClient(link)
	c.redial = redialRoute{opts: o, dial: func(o DialOpts) (Link, error) {
		return dialUSBLink(byIdOrPath, o)
	}}
	return c, nil
}

// dialUSBLink opens the CDC device and reboots the board into a fresh
// session — the reboot IS the open semantics (§2: one connection = one
// device session; §12: the arbitration table is RAM-only), replaying
// the host hub's proven line sequence byte-for-byte in spirit:
// suppress DTR/RTS right after open (so the open transition cannot
// accidentally land in download mode), then pulse RTS with DTR low
// (EN pulled, GPIO0 high → run app). A plain open with this serial
// library leaves the lines where the kernel put them and does NOT
// reboot the S3 (probed on the bench: asserting DTR+RTS together is
// also a no-op — the USB-JTAG reset logic is transition-shaped), so
// the pulse is explicit, not assumed.
func dialUSBLink(byIdOrPath string, o DialOpts) (Link, error) {
	path, mac6, err := resolveUSBDevice(byIdOrPath)
	if err != nil {
		return nil, err
	}
	if mac6 != "" {
		o.logf("usb: %s → %s (mac6 %s)", byIdOrPath, path, mac6)
	} else {
		o.logf("usb: %s → %s", byIdOrPath, path)
	}
	port, err := serial.Open(path, &serial.Mode{BaudRate: 115200})
	if err != nil {
		return nil, fmt.Errorf("replclient: open %s: %w", path, err)
	}
	if err := rebootOnOpen(port); err != nil {
		port.Close()
		return nil, fmt.Errorf("replclient: reboot pulse %s: %w", path, err)
	}
	if window, drain := o.drainWindow(); drain {
		n, derr := drainPort(port, window, o.BootCapture)
		if derr != nil {
			port.Close()
			return nil, fmt.Errorf("replclient: drain %s: %w", path, derr)
		}
		o.logf("usb: RTS pulse rebooted the board; %.1fs drain absorbed %d bytes (ROM prefix ≈2.6KB + IDF boot lines, §2)",
			window.Seconds(), n)
	} else {
		o.logf("usb: NoDrain — caller owns pre-frame bytes on %s", path)
	}
	return &serialLink{port: port}, nil
}

// rebootOnOpen is the deterministic open-time reset: lines low (no
// accidental download entry), a short settle, input flushed, then RTS
// high→low with DTR low — EN pulses, the chip resets into the app.
func rebootOnOpen(port serial.Port) error {
	if err := port.SetDTR(false); err != nil {
		return err
	}
	if err := port.SetRTS(false); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)
	if err := port.ResetInputBuffer(); err != nil {
		return err
	}
	if err := port.SetRTS(true); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return port.SetRTS(false)
}

// drainPort reads and discards everything arriving in the window. The
// rebooted board's ROM console prefix is non-frame bytes, and this
// client's frame layer is deliberately strict (no byte-sliding junk
// absorption — the Python hub's tolerance is not carried over, §7), so
// the prefix must never reach the router. The window also has to outlast
// the fw's own startup (see usbDrainDefault): a first frame written
// before the REPL is up is buffered by the USB driver and mis-dispatched
// at repl-up. Reads poll in 100ms slices so the window ends promptly; a
// timed-out Read returns (0, nil) and the deadline check exits.
//
// B16-T6 D1: capture, when non-nil, receives the TAIL of the window
// (last bootCaptureKeep bytes) — "read and retained" replaces "read and
// discarded" for the banner's diagnostic value (receipt columns + death
// classifier baseline; every open refreshes it — one connection = one
// banner).
func drainPort(port serial.Port, window time.Duration, capture func([]byte)) (int, error) {
	if err := port.SetReadTimeout(100 * time.Millisecond); err != nil {
		return 0, err
	}
	defer port.SetReadTimeout(serial.NoTimeout)
	deadline := time.Now().Add(window)
	total := 0
	buf := make([]byte, 4096)
	var tail []byte
	for time.Until(deadline) > 0 {
		n, err := port.Read(buf)
		total += n
		if capture != nil && n > 0 {
			tail = append(tail, buf[:n]...)
			if len(tail) > bootCaptureKeep {
				tail = tail[len(tail)-bootCaptureKeep:]
			}
		}
		if err != nil {
			if capture != nil && len(tail) > 0 {
				capture(tail)
			}
			return total, err
		}
	}
	if capture != nil && len(tail) > 0 {
		capture(tail)
	}
	return total, nil
}

// serialLink is the CDC endpoint of the Link contract. Reads block
// until bytes arrive; Close unblocks a pending read (the transport
// signals it), so the router's read loop always ends with the client.
type serialLink struct {
	port serial.Port
}

func (l *serialLink) Read(p []byte) (int, error)  { return l.port.Read(p) }
func (l *serialLink) Write(p []byte) (int, error) { return l.port.Write(p) }
func (l *serialLink) Close() error                { return l.port.Close() }
func (l *serialLink) Kind() string                { return "usb" }

// mac6FromByID pulls the board's mac6 identity out of a by-id device
// name: the last three octets of the trailing MAC group, lower-cased —
// "usb-Espressif_USB_JTAG_serial_debug_unit_3C:0F:02:D7:58:84-if00" →
// "d75884" (the first octet is glued to the product string by an
// underscore, so the group is matched as a whole, not tokenized).
// Purely for logs and correlation; dialing never depends on it
// ("", false when no MAC group is present).
func mac6FromByID(s string) (string, bool) {
	m := macInByID.FindString(s)
	if m == "" {
		return "", false
	}
	parts := strings.Split(m, ":")
	return strings.ToLower(strings.Join(parts[3:], "")), true
}

// macInByID matches one colon-separated six-octet MAC group anywhere in
// a device name (greedy scan, last match used).
var macInByID = regexp.MustCompile(`(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}`)

// resolveUSBDevice maps a by-id name (full path or bare basename) or a
// direct device path to an openable tty path, canonicalizing a by-id
// symlink down to the real /dev/ttyACM* node so logs name the device
// the kernel opened.
func resolveUSBDevice(byIdOrPath string) (path string, mac6 string, err error) {
	mac6, _ = mac6FromByID(byIdOrPath)
	candidates := []string{byIdOrPath}
	if !strings.HasPrefix(byIdOrPath, byIDDir+string(filepath.Separator)) {
		candidates = append(candidates, filepath.Join(byIDDir, filepath.Base(byIdOrPath)))
	}
	for _, p := range candidates {
		if _, statErr := os.Stat(p); statErr == nil {
			if real, evalErr := filepath.EvalSymlinks(p); evalErr == nil {
				p = real
			}
			return p, mac6, nil
		}
	}
	return "", mac6, fmt.Errorf("replclient: usb device %q not found (tried %s)",
		byIdOrPath, strings.Join(candidates, ", "))
}
