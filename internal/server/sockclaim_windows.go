//go:build windows

package server

// sockclaim_windows.go — NO socket hard-link claim on Windows, by
// decision rather than runtime probing: NTFS may well hard-link the
// socket FILE, but Windows AF_UNIX resolves names through the file,
// not the inode — a "successful" link could strand the listener behind
// a name connects do not reach. bindSock keeps the direct bind there,
// whose conflict classification (isAddrInUse, the WSAEADDRINUSE form)
// already walks the spawn race the wave-1 way.
const linkClaimSupported = false
