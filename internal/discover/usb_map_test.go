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
		got := mapEnumerated(c.ports)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: mapEnumerated =\n %#v\nwant\n %#v", c.name, got, c.want)
		}
	}
}
