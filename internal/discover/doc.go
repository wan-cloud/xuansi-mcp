// Package discover finds boards and keeps them in one registry keyed by
// the board's 6-byte MAC (mac6): USB enumeration (by-id scan on linux,
// serial-enumerator USB descriptors on windows/darwin) and mDNS
// discovery of _xuansi._tcp — a per-interface multicast browse plus a
// source-bound unicast query lane for hosts where multicast response
// reception is unreliable — are merged by mac6, with alias support and
// unique-prefix selector resolution (mac6 / alias / hostname).
package discover
