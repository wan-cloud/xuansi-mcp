package discover

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"go.bug.st/serial"
)

// espressifPrefix is the /dev/serial/by-id name prefix of the Espressif
// USB-JTAG-Serial debug unit the boards expose (VID:PID 303a:1001): the
// stable udev name embeds the unit's MAC. Serial devices not matching
// this prefix are not xuansi boards and are ignored.
const espressifPrefix = "usb-Espressif_USB_JTAG_serial_debug_unit_"

// Seams for tests: where udev exposes stable names, and how the live
// serial port set is enumerated.
var (
	byIDDir     = "/dev/serial/by-id"
	serialPorts = serial.GetPortsList
)

// DiscoverUSB enumerates boards on USB: it scans /dev/serial/by-id for
// Espressif USB-JTAG-Serial units (stable names carrying the board MAC)
// and cross-checks each against the live port list from the serial
// enquirer, so a stale by-id symlink whose node vanished mid-scan
// cannot produce a phantom board. Each board keys on its mac6 (the last
// three MAC octets, lower-case — same identity the fw's mDNS hostname
// uses, §14); a unit exposing several interfaces still counts once.
// USBPath is the by-id path (preferred over the /dev/ttyACM* node: it
// survives re-enumeration across the reboot every CDC open causes, §2).
// A missing by-id dir (no serial subsystem) is an empty result, not an
// error; a caller-canceled context errors.
func DiscoverUSB(ctx context.Context) ([]Board, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	names, err := os.ReadDir(byIDDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("discover: read %s: %w", byIDDir, err)
	}
	live, err := serialPorts()
	if err != nil {
		return nil, fmt.Errorf("discover: serial ports: %w", err)
	}
	liveSet := make(map[string]bool, len(live))
	for _, p := range live {
		liveSet[p] = true
	}
	sort.Slice(names, func(i, j int) bool { return names[i].Name() < names[j].Name() })

	var boards []Board
	seen := map[string]bool{}
	for _, de := range names {
		select {
		case <-ctx.Done():
			return boards, ctx.Err()
		default:
		}
		name := de.Name()
		if !strings.HasPrefix(name, espressifPrefix) {
			continue // someone else's serial device
		}
		mac6, ok := mac6FromByID(name)
		if !ok {
			continue // no MAC group: not keyable by identity
		}
		if seen[mac6] {
			continue // same unit, another interface
		}
		byIDPath := filepath.Join(byIDDir, name)
		node, err := filepath.EvalSymlinks(byIDPath)
		if err != nil {
			continue // stale symlink: the device is gone
		}
		if !liveSet[node] {
			continue // phantom name: node no longer enumerated
		}
		seen[mac6] = true
		boards = append(boards, Board{
			MAC6:    mac6,
			Sources: []string{SourceUSB},
			USBPath: byIDPath,
		})
	}
	return boards, nil
}

// mac6FromByID pulls the board's mac6 identity out of a by-id device
// name: the last three octets of the trailing six-octet MAC group,
// lower-cased — "…unit_3C:0F:02:D7:58:84-if00" → "d75884". Mirrors
// replclient's dial-side parse (the MAC group is glued to the product
// string by underscores, so it is matched as a whole, not tokenized;
// the last group wins when several appear). ("", false) when no MAC
// group is present.
func mac6FromByID(s string) (string, bool) {
	groups := macInByID.FindAllString(s, -1)
	if len(groups) == 0 {
		return "", false
	}
	last := groups[len(groups)-1]
	parts := strings.Split(last, ":")
	return strings.ToLower(strings.Join(parts[3:], "")), true
}

// macInByID matches one colon-separated six-octet MAC group anywhere in
// a device name.
var macInByID = regexp.MustCompile(`(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}`)
