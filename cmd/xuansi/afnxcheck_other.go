//go:build !windows

package main

// afnxcheck_other.go — the non-Windows side of the AF_UNIX-support
// classification (afnxcheck_windows.go): no other supported platform
// refuses the address family, so the answer is always false, and the
// linux build gains no x/sys import for a path it can never take.

// isAFNXUnsupported is the windows-only WSAEAFNOSUPPORT (10047)
// classification; false everywhere else.
func isAFNXUnsupported(err error) bool { return false }
