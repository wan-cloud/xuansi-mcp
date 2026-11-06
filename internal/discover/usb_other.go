//go:build windows || darwin

package discover

import (
	"context"
	"fmt"

	"go.bug.st/serial/enumerator"
)

// DiscoverUSB enumerates boards on USB where /dev/serial/by-id does not
// exist: the serial enumerator's detailed port list carries each USB
// port's VID and serial number, and mapEnumerated applies the same
// Espressif VID filter and mac6-from-serial identity the linux by-id
// scan derives from device names — both faces of USB discovery key a
// board identically, so the registry merges them without special cases.
// Windows feeds mapEnumerated the registry serial fallback: the
// enumerator cannot parse Espressif's colon-MAC iSerial (upstream \w
// regex), so serials resolve from the PnP enum tree there (see
// usb_windows_registry.go). USBPath is the enumerated port name (COMn
// / /dev/tty.usbmodem*): the stablest handle these hosts expose. A
// caller-canceled context errors, matching the linux face.
func DiscoverUSB(ctx context.Context) ([]Board, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ports, err := enumerator.GetDetailedPortsList()
	if err != nil {
		return nil, fmt.Errorf("discover: enumerate: %w", err)
	}
	return mapEnumerated(ports, fallbackSerials()), nil
}
