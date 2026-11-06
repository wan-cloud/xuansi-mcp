//go:build !windows

package server

// sockclaim_unix.go — the bind-temp + hard-link atomic socket claim is
// supported here (linux, darwin): a unix socket IS its inode, so a
// hard link to a bound socket's file is another live name for the same
// listener — the claim protocol in bindSock builds on that (verified:
// linux in this repo's tests, darwin on the T9 machine).
const linkClaimSupported = true
