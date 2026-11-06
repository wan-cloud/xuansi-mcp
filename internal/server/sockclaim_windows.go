//go:build windows

package server

// sockclaim_windows.go — NO socket hard-link claim on Windows, by
// decision rather than runtime probing: NTFS may well hard-link the
// socket FILE, but Windows AF_UNIX resolves names through the file,
// not the inode — a "successful" link could strand the listener behind
// a name connects do not reach. bindSock keeps the direct bind there,
// whose conflict classification (isAddrInUse, the WSAEADDRINUSE form)
// already walks the spawn race the wave-1 way.
//
// Residual, accepted (final-review minor): the direct bind keeps the
// remove-stale TOCTOU the claim protocol closed on darwin — a racing
// prober may dial the winner's bound-but-not-yet-listening socket,
// get refused, misjudge it stale, and fail to Remove a live-bound
// socket; its own bind then fails and the loser exits 1.
// Self-healing: the winner is unaffected, and the loser's spawner
// client just keeps dialing until the winner's listen answers
// (spawn.go's retry loop). The darwin finding that motivated the
// claim protocol does not apply here — Windows AF_UNIX links are not
// inode-based, so the two-winners orphaning the link protocol
// prevents cannot even be expressed on this platform.
const linkClaimSupported = false
