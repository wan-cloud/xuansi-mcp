//go:build windows

package discover

// usb_windows_registry.go — the Windows face of the usbSerialFallback
// seam: resolves live COM port names to USB serials by walking the
// PnP enumeration tree in the registry. Exists because go.bug.st/
// serial v1.8.0's parseDeviceID regex (`\\(\w+)$`) cannot capture
// Espressif's colon-separated MAC iSerial (`\w` excludes ':'), on the
// device id AND on the parent-fallback walk — so SerialNumber comes
// back empty and USB discovery dies at the mac6 filter (the T9 live
// finding). No subprocess calls: the daemon must not shell out in its
// discovery loop, so this reads the registry through
// golang.org/x/sys/windows/registry (promoted direct per the spec
// pre-authorization).
//
// Registry layout walked (facts established on the T9 machine):
//
//	HKLM\SYSTEM\CurrentControlSet\Enum\USB\VID_303A&PID_1001\<inst>
//	  → device instances; <inst> IS the iSerial (the colon MAC) for
//	    these units — live board "3C:0F:02:D7:58:84", stale unplugged
//	    "AC:A7:04:82:D4:4C" both sit here.
//	HKLM\SYSTEM\CurrentControlSet\Enum\USB\VID_303A&PID_1001&MI_00\<iface>\Device Parameters
//	  → REG_SZ PortName ("COM8"): the CDC interface children of the
//	    composite device, siblings of the VID_303A&PID_1001 key.
//
// Linking interface child → parent instance is done defensively, in
// the two spellings Windows uses, both verified against the parent
// instance set before any serial is attached:
//
//  1. instance-embedded: the child instance is the parent instance
//     plus a "&<loc>&<if>" location suffix (serial-keyed devices),
//     so stripping the suffix names the parent;
//  2. ParentIdPrefix: the child instance starts with a prefix the
//     PARENT key carries as its ParentIdPrefix value (opaque-keyed
//     devices).
//
// A single-interface device (no MI children) keeps PortName directly
// under its own Device Parameters — also read. The walk is
// best-effort per key: an unreadable branch contributes nothing, and
// the whole function yields whatever it could resolve; mapEnumerated
// joins the result onto LIVE enumerator ports only, so stale
// instances of unplugged boards can never surface as boards.

import (
	"strings"

	"golang.org/x/sys/windows/registry"
)

// usbEnumRoot is the PnP USB enumeration root the walk starts at.
const usbEnumRoot = `SYSTEM\CurrentControlSet\Enum\USB`

func init() { usbSerialFallback = registrySerialByPort }

// registrySerialByPort walks the registry's USB enum tree for the
// Espressif VID:PID and maps each interface's PortName (COMn) to the
// parent device instance id — the MAC serial. Nil when the tree is
// unreadable (behaves like an absent fallback).
func registrySerialByPort() map[string]string {
	devPath := usbEnumRoot + `\VID_` + espressifVID + `&PID_` + espressifPID
	devKey, err := registry.OpenKey(registry.LOCAL_MACHINE, devPath, registry.READ)
	if err != nil {
		return nil
	}
	defer devKey.Close()
	instances, err := devKey.ReadSubKeyNames(-1)
	if err != nil {
		return nil
	}

	serialByPort := map[string]string{}
	instanceSet := make(map[string]bool, len(instances))
	// byPrefix: ParentIdPrefix value → instance id (link strategy 2).
	byPrefix := map[string]string{}
	for _, inst := range instances {
		instanceSet[inst] = true
		if prefix, err := getStringValue(devKey, inst, "ParentIdPrefix"); err == nil && prefix != "" {
			byPrefix[prefix] = inst
		}
		// single-interface layout: PortName right under the device key
		if com, ok := devicePortName(devPath + `\` + inst); ok {
			serialByPort[com] = inst
		}
	}

	// composite layout: the MI interface siblings
	usbKey, err := registry.OpenKey(registry.LOCAL_MACHINE, usbEnumRoot, registry.READ)
	if err != nil {
		return serialByPort
	}
	defer usbKey.Close()
	siblings, err := usbKey.ReadSubKeyNames(-1)
	if err != nil {
		return serialByPort
	}
	miPrefix := `VID_` + espressifVID + `&PID_` + espressifPID + `&MI_`
	for _, sib := range siblings {
		if !strings.HasPrefix(sib, miPrefix) {
			continue // not one of this device's interfaces
		}
		miPath := usbEnumRoot + `\` + sib
		miKey, err := registry.OpenKey(registry.LOCAL_MACHINE, miPath, registry.READ)
		if err != nil {
			continue
		}
		ifaces, err := miKey.ReadSubKeyNames(-1)
		miKey.Close()
		if err != nil {
			continue
		}
		for _, ifc := range ifaces {
			com, ok := devicePortName(miPath + `\` + ifc)
			if !ok {
				continue // not a port-bearing interface
			}
			if parent := parentInstanceOf(ifc, instanceSet, byPrefix); parent != "" {
				serialByPort[com] = parent
			}
		}
	}
	return serialByPort
}

// parentInstanceOf links an interface instance id back to its parent
// device instance: the location-stripped candidate when that names a
// real instance (serial-keyed devices carry the parent verbatim as a
// prefix), else the instance whose ParentIdPrefix the id starts with.
// "" when no parent in the set matches.
func parentInstanceOf(ifc string, instanceSet map[string]bool, byPrefix map[string]string) string {
	if parent := stripLocationSuffix(ifc); parent != ifc && instanceSet[parent] {
		return parent
	}
	for prefix, inst := range byPrefix {
		if ifc == prefix || strings.HasPrefix(ifc, prefix+`&`) {
			return inst
		}
	}
	return ""
}

// stripLocationSuffix drops the trailing "&<loc>&<interface>" pair
// from an interface instance id ("3C:0F:02:D7:58:84&0&0000" →
// "3C:0F:02:D7:58:84"; opaque "6&2f1e2b4d&0&0000" → "6&2f1e2b4d"),
// leaving the parent-instance candidate. Few than three "&"-separated
// components is no location suffix at all; the MAC itself never
// contains '&'.
func stripLocationSuffix(ifc string) string {
	parts := strings.Split(ifc, `&`)
	if len(parts) < 3 {
		return ifc
	}
	return strings.Join(parts[:len(parts)-2], `&`)
}

// devicePortName reads the PortName string value under
// <devPath>\Device Parameters ("COM8"); ok=false when the key or value
// is absent (a non-port-bearing devnode).
func devicePortName(devPath string) (string, bool) {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, devPath+`\Device Parameters`, registry.READ)
	if err != nil {
		return "", false
	}
	defer k.Close()
	port, _, err := k.GetStringValue("PortName")
	if err != nil || port == "" {
		return "", false
	}
	return port, true
}

// getStringValue reads a REG_SZ value from a subkey of k.
func getStringValue(k registry.Key, sub, name string) (string, error) {
	sk, err := registry.OpenKey(k, sub, registry.READ)
	if err != nil {
		return "", err
	}
	defer sk.Close()
	v, _, err := sk.GetStringValue(name)
	return v, err
}
