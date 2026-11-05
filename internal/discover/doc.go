// Package discover finds boards and keeps them in one registry keyed by
// the board's 6-byte MAC (mac6): USB enumeration (by-id scan on linux,
// serial-enumerator USB descriptors on windows/darwin) and mDNS browse
// of _xuansi._tcp are merged by mac6, with alias support and
// unique-prefix selector resolution (mac6 / alias / hostname).
package discover
