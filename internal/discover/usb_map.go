package discover

import (
	"strings"

	"go.bug.st/serial/enumerator"
)

// espressifVID is the USB vendor ID of the Espressif USB-JTAG-Serial
// debug unit the boards expose (VID:PID 303a:1001) — the enumerator
// face of the espressifPrefix filter the linux by-id scan uses.
const espressifVID = "303A"

// espressifPID is the matching product id, spelling the registry key
// the Windows fallback walks (VID_303A&PID_1001).
const espressifPID = "1001"

// usbSerialFallback is the per-host seam that resolves LIVE enumerated
// port names to USB serials when the enumerator itself cannot; nil on
// hosts that don't need it (darwin, linux — linux doesn't ride
// mapEnumerated at all). Windows wires it to the registry walk in
// usb_windows_registry.go: go.bug.st/serial v1.8.0's parseDeviceID
// regex (`\\(\w+)$`) cannot capture Espressif's colon-separated MAC
// iSerial — `\w` excludes ':' — on the device AND on the parent
// fallback walk, so USB serials come back empty and every board was
// skipped wholesale on Windows (the T9 live finding; no newer
// upstream exists).
var usbSerialFallback func() map[string]string

// fallbackSerials calls the usbSerialFallback seam when wired. An
// absent or unreadable fallback is nil — discovery then degrades to
// the enumerator-only behavior rather than failing.
func fallbackSerials() map[string]string {
	if usbSerialFallback == nil {
		return nil
	}
	return usbSerialFallback()
}

// mapEnumerated filters an enumerated port list down to boards: USB
// ports reporting the Espressif VID (case-insensitive — hosts disagree
// on VID casing) become Boards keyed by mac6, the last six hex chars of
// the USB serial number after stripping ':'/'-' separators, lower-cased
// — the same identity the linux by-id path derives from the device
// name, so both faces of USB discovery agree on a board's key (§14).
// serialByPort is the live-port→serial fallback join (windows
// registry): a port whose SerialNumber came back empty from the
// enumerator looks its name up there, and only there — the join runs
// on LIVE ports, so a stale registry instance whose PortName names no
// enumerated port can never surface, and the enumerator's own serial,
// when present, stays authoritative. A serial whose tail is not six
// hex chars (too short, or non-hex) has no MAC to key on and is
// skipped; a unit exposing several interfaces (same serial under
// several port names) counts once, the first port winning. USBPath is
// the port name itself (COMn on windows, /dev/tty.usbmodem* on
// darwin): the stablest handle those hosts give.
func mapEnumerated(ports []*enumerator.PortDetails, serialByPort map[string]string) []Board {
	var boards []Board
	seen := map[string]bool{}
	for _, p := range ports {
		if p == nil || !p.IsUSB || !strings.EqualFold(p.VID, espressifVID) {
			continue // not one of ours
		}
		serial := p.SerialNumber
		if serial == "" {
			serial = serialByPort[p.Name] // live-port join; "" when absent
		}
		mac6, ok := mac6FromSerial(serial)
		if !ok {
			continue // no MAC tail: not keyable by identity
		}
		if seen[mac6] {
			continue // same unit, another interface
		}
		seen[mac6] = true
		boards = append(boards, Board{
			MAC6:    mac6,
			Sources: []string{SourceUSB},
			USBPath: p.Name,
		})
	}
	return boards
}

// mac6FromSerial pulls the board's mac6 identity out of a USB serial
// number: strip ':'/'-' separators, take the last six chars, lower-case
// — "3C:0F:02:D7:58:84" → "d75884", "3C-0F-02-D7-58-84" likewise. A
// tail that is not six hex chars (short serials, garbage) is not a MAC
// and yields ("", false). Byte-wise tailing is safe: a multi-byte rune
// inside the tail always leaves non-hex bytes and is rejected.
func mac6FromSerial(serial string) (string, bool) {
	stripped := strings.Map(func(r rune) rune {
		if r == ':' || r == '-' {
			return -1
		}
		return r
	}, serial)
	if len(stripped) < 6 {
		return "", false
	}
	tail := strings.ToLower(stripped[len(stripped)-6:])
	if !isMac6(tail) {
		return "", false
	}
	return tail, true
}
