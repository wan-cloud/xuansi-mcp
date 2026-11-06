package discover

import (
	"reflect"
	"testing"

	"go.bug.st/serial/enumerator"
)

// TestMapEnumerated pins the enumerator→Board mapping rule: USB ports
// with the Espressif VID (case-insensitive) become boards keyed by the
// mac6 pulled from the USB serial number's tail (same identity the
// linux by-id path derives, so both faces agree on a board's key);
// everything else — other vendors, non-USB ports, serials without a
// 6-hex tail — is skipped, and a unit exposing several interfaces
// (same serial, several COM names) counts once, first port winning.
func TestMapEnumerated(t *testing.T) {
	cases := []struct {
		name  string
		ports []*enumerator.PortDetails
		want  []Board
	}{
		{
			// the bench board as windows/darwin see it: VID:PID 303a:1001,
			// MAC as serial number, COM port name
			name: "espressif vid hit",
			ports: []*enumerator.PortDetails{
				{Name: "COM3", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "3C:0F:02:D7:58:84"},
			},
			want: []Board{
				{MAC6: "d75884", Sources: []string{SourceUSB}, USBPath: "COM3"},
			},
		},
		{
			// VID case varies by OS reporting; EqualFold matches
			name: "lowercase vid variant",
			ports: []*enumerator.PortDetails{
				{Name: "COM3", IsUSB: true, VID: "303a", PID: "1001", SerialNumber: "3C:0F:02:D7:58:84"},
			},
			want: []Board{
				{MAC6: "d75884", Sources: []string{SourceUSB}, USBPath: "COM3"},
			},
		},
		{
			// other vendors' USB serial devices are not xuansi boards
			name: "other vid ignored",
			ports: []*enumerator.PortDetails{
				{Name: "COM5", IsUSB: true, VID: "0403", PID: "6001", SerialNumber: "A903O5D3"},
			},
			want: nil,
		},
		{
			// a VID string alone proves nothing without the USB flag
			name: "non usb ignored",
			ports: []*enumerator.PortDetails{
				{Name: "COM3", IsUSB: false, VID: "303A", PID: "1001", SerialNumber: "3C:0F:02:D7:58:84"},
			},
			want: nil,
		},
		{
			// brief's explicit row: full MAC serial → last three octets
			name: "mac6 from colon serial",
			ports: []*enumerator.PortDetails{
				{Name: "COM4", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "AA:BB:CC:DD:EE:FF"},
			},
			want: []Board{
				{MAC6: "ddeeff", Sources: []string{SourceUSB}, USBPath: "COM4"},
			},
		},
		{
			// serial with no separators: same tail-6 rule (S3 units
			// report the bare MAC this way on some hosts)
			name: "bare mac serial",
			ports: []*enumerator.PortDetails{
				{Name: "COM3", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "3C0F02D75884"},
			},
			want: []Board{
				{MAC6: "d75884", Sources: []string{SourceUSB}, USBPath: "COM3"},
			},
		},
		{
			// fewer than 6 hex chars after stripping: no mac6 to key on
			name: "serial too short ignored",
			ports: []*enumerator.PortDetails{
				{Name: "COM3", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "D7:58"},
			},
			want: nil,
		},
		{
			// empty serial: same bucket — not keyable
			name: "empty serial ignored",
			ports: []*enumerator.PortDetails{
				{Name: "COM3", IsUSB: true, VID: "303A", PID: "1001"},
			},
			want: nil,
		},
		{
			// tail chars outside hex (ZZ) cannot be a MAC octet pair
			name: "non hex tail skipped",
			ports: []*enumerator.PortDetails{
				{Name: "COM3", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "AA:BB:CC:DD:EE:ZZ"},
			},
			want: nil,
		},
		{
			// one unit, two CDC interfaces → two COM names, one serial:
			// dedup by mac6, first port in the list wins
			name: "same serial two interfaces dedup",
			ports: []*enumerator.PortDetails{
				{Name: "COM3", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "3C:0F:02:D7:58:84"},
				{Name: "COM4", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "3C:0F:02:D7:58:84"},
			},
			want: []Board{
				{MAC6: "d75884", Sources: []string{SourceUSB}, USBPath: "COM3"},
			},
		},
		{
			// mixed bench: noise before, between and after real boards;
			// output order follows the input order
			name: "mixed ports",
			ports: []*enumerator.PortDetails{
				{Name: "COM5", IsUSB: true, VID: "0403", PID: "6001", SerialNumber: "A903O5D3"},
				{Name: "COM3", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "3c:0f:02:d7:58:84"},
				{Name: "COM6", IsUSB: false, VID: "303A", PID: "1001", SerialNumber: "3C:0F:02:1A:2B:3C"},
				{Name: "COM4", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "3C:0F:02:1A:2B:3C"},
			},
			want: []Board{
				{MAC6: "d75884", Sources: []string{SourceUSB}, USBPath: "COM3"},
				{MAC6: "1a2b3c", Sources: []string{SourceUSB}, USBPath: "COM4"},
			},
		},
		{
			name:  "no ports",
			ports: nil,
			want:  nil,
		},
	}
	for _, c := range cases {
		got := mapEnumerated(c.ports, nil)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: mapEnumerated =\n %#v\nwant\n %#v", c.name, got, c.want)
		}
	}
}

// TestMapEnumeratedSerialFallback pins the registry-fallback join fed
// to mapEnumerated on Windows (the pure half of the T9 fix; the
// registry walk itself is the thin windows-tagged reader feeding this
// map): an EMPTY enumerator serial is backfilled from the fallback map
// by live port name, the enumerator's own serial stays authoritative,
// and the join runs only on live ports — a stale registry instance of
// an unplugged board whose PortName names no enumerated port never
// surfaces.
func TestMapEnumeratedSerialFallback(t *testing.T) {
	cases := []struct {
		name         string
		ports        []*enumerator.PortDetails
		serialByPort map[string]string
		want         []Board
	}{
		{
			// the T9 machine's exact live shape: COM8 enumerates with
			// the right VID but an EMPTY serial (upstream \w regex),
			// and the registry walk resolved COM8 → the MAC instance
			name: "t9 live: empty serial backfilled by com name",
			ports: []*enumerator.PortDetails{
				{Name: "COM8", IsUSB: true, VID: "303A", PID: "1001"},
			},
			serialByPort: map[string]string{"COM8": "3C:0F:02:D7:58:84"},
			want: []Board{
				{MAC6: "d75884", Sources: []string{SourceUSB}, USBPath: "COM8"},
			},
		},
		{
			// stale registry instance (the unplugged second board
			// ACA70482D44C observed on the machine): its PortName maps
			// to no LIVE enumerated port, so it is ignored — the join
			// keys on live ports, never on registry entries
			name: "stale instance with no live port ignored",
			ports: []*enumerator.PortDetails{
				{Name: "COM8", IsUSB: true, VID: "303A", PID: "1001"},
			},
			serialByPort: map[string]string{"COM9": "AC:A7:04:82:D4:4C", "COM8": "3C:0F:02:D7:58:84"},
			want: []Board{
				{MAC6: "d75884", Sources: []string{SourceUSB}, USBPath: "COM8"},
			},
		},
		{
			// the enumerator's serial, when present, wins: the
			// fallback is only for the empty case
			name: "enumerator serial authoritative",
			ports: []*enumerator.PortDetails{
				{Name: "COM8", IsUSB: true, VID: "303A", PID: "1001", SerialNumber: "3C:0F:02:1A:2B:3C"},
			},
			serialByPort: map[string]string{"COM8": "3C:0F:02:D7:58:84"},
			want: []Board{
				{MAC6: "1a2b3c", Sources: []string{SourceUSB}, USBPath: "COM8"},
			},
		},
		{
			// empty serial with no fallback entry: unkeyable, skipped
			name: "fallback miss skipped",
			ports: []*enumerator.PortDetails{
				{Name: "COM8", IsUSB: true, VID: "303A", PID: "1001"},
			},
			serialByPort: map[string]string{"COM7": "3C:0F:02:D7:58:84"},
			want:         nil,
		},
		{
			// the fallback is consulted only for Espressif USB ports:
			// a non-USB port named in the map gains nothing
			name: "non usb port never backfilled",
			ports: []*enumerator.PortDetails{
				{Name: "COM8", IsUSB: false, VID: "303A", PID: "1001"},
			},
			serialByPort: map[string]string{"COM8": "3C:0F:02:D7:58:84"},
			want:         nil,
		},
		{
			// same gate for another vendor's USB port
			name: "other vid never backfilled",
			ports: []*enumerator.PortDetails{
				{Name: "COM8", IsUSB: true, VID: "0403", PID: "6001"},
			},
			serialByPort: map[string]string{"COM8": "3C:0F:02:D7:58:84"},
			want:         nil,
		},
		{
			// a registry instance id that is not a MAC-tailed serial
			// (opaque instance) still fails the mac6 gate downstream
			// — its stripped tail is not six hex chars
			name: "garbage fallback serial skipped",
			ports: []*enumerator.PortDetails{
				{Name: "COM8", IsUSB: true, VID: "303A", PID: "1001"},
			},
			serialByPort: map[string]string{"COM8": "6&2f1ez4d"},
			want:         nil,
		},
		{
			// dedup rides the backfilled identity too: two ports, one
			// registry-resolved unit, first port wins
			name: "same backfilled serial dedup",
			ports: []*enumerator.PortDetails{
				{Name: "COM8", IsUSB: true, VID: "303A", PID: "1001"},
				{Name: "COM9", IsUSB: true, VID: "303A", PID: "1001"},
			},
			serialByPort: map[string]string{"COM8": "3C:0F:02:D7:58:84", "COM9": "3C:0F:02:D7:58:84"},
			want: []Board{
				{MAC6: "d75884", Sources: []string{SourceUSB}, USBPath: "COM8"},
			},
		},
	}
	for _, c := range cases {
		got := mapEnumerated(c.ports, c.serialByPort)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: mapEnumerated =\n %#v\nwant\n %#v", c.name, got, c.want)
		}
	}
}
