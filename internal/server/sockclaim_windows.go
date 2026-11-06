//go:build windows

package server

import (
	"fmt"
	"net"
	"os"
	"time"
)

// sockclaim_windows.go — NO socket hard-link claim on Windows, by
// decision rather than runtime probing: NTFS may well hard-link the
// socket FILE, but Windows AF_UNIX resolves names through the file,
// not the inode — a "successful" link could strand the listener behind
// a name connects do not reach. What replaces it is a TRANSITION LOCK:
// the whole stale-check → remove → bind sequence runs under an
// O_CREATE|O_EXCL lockfile beside the sock (<sock>.lck), so the
// remove-stale TOCTOU the old comment called "residual, accepted" —
// a racing prober dialing the winner's bound-but-not-yet-listening
// socket, misjudging it stale, removing it and rebinding (the CI
// hammer's round-7 shape: two winners plus a bare "remove ... cannot
// find" loser) — is closed: exactly one racer transitions at a time,
// everyone else either observes the sock go ALIVE (the graceful
// ErrAlreadyListening loser path) or waits for the lock.
//
// Protocol (claim → transition → release, bounded stale recovery):
//
//	claim    O_CREATE|O_EXCL on <sock>.lck — atomic on NTFS, so
//	         exactly one racer becomes the holder.
//	hold     the stat→probe→remove stale→bindDirect sequence; the
//	         holder releases (deletes) the lock after bind.
//	wait     a non-holder polls: sock alive → graceful loser; lock
//	         gone → retry the claim. Past transitionGrace with the
//	         sock still dead AND the lock still present, the holder
//	         crashed mid-transition: remove the stale sock and the
//	         stale lock, retry the claim (bounded — repeated stale
//	         recoveries are an adversary, not a race).
//	after    liveness stays the sock probe (the lock serializes the
//	         TRANSITION only — post-bind, the socket itself answers).
//
// The darwin two-winners orphaning the unix link protocol prevents
// cannot be expressed here (Windows AF_UNIX links are not
// inode-based), and this protocol prevents its own version of it.
const linkClaimSupported = false

// sockLockSuffix names the transition lockfile beside the sock.
const sockLockSuffix = ".lck"

// transitionGrace bounds how long a non-holder waits before declaring
// a held lock stale (holder crashed): the whole transition under the
// lock is single-digit milliseconds, so 500ms is two orders of margin.
const transitionGrace = 500 * time.Millisecond

// lockPoll is the non-holder's poll cadence while waiting out a
// holder's transition.
const lockPoll = 2 * time.Millisecond

// bindSockPlatform (windows): the transition-lock claim protocol above.
func bindSockPlatform(path string) (net.Listener, error) {
	lock := path + sockLockSuffix
	const attempts = 4
	for attempt := 0; attempt < attempts; attempt++ {
		lf, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			// holder: run the whole transition, then release
			ln, berr := bindLocked(path)
			lf.Close()
			os.Remove(lock) // release; best-effort — a crashed release self-heals via stale recovery
			return ln, berr
		}
		if !os.IsExist(err) {
			return nil, fmt.Errorf("server: claim transition lock %s: %w", lock, err)
		}
		// someone holds the transition: wait it out — alive is the
		// graceful loser path, a released lock means retry the claim
		deadline := time.Now().Add(transitionGrace)
		for time.Now().Before(deadline) {
			if sockAlive(path) {
				return nil, fmt.Errorf("%w: %s", ErrAlreadyListening, path)
			}
			if _, lerr := os.Stat(lock); os.IsNotExist(lerr) {
				break // released: retry the claim
			}
			time.Sleep(lockPoll)
		}
		// grace expired — only a CRASHED holder still shows a lock with
		// a dead sock; re-verify both, then recover the pair
		if sockAlive(path) {
			return nil, fmt.Errorf("%w: %s", ErrAlreadyListening, path)
		}
		if _, lerr := os.Stat(lock); lerr == nil {
			os.Remove(path) // stale sock (absent is fine); then the stale lock
			if rerr := os.Remove(lock); rerr != nil && !os.IsNotExist(rerr) {
				return nil, fmt.Errorf("server: remove stale transition lock %s: %w", lock, rerr)
			}
			continue
		}
		// lock vanished during recovery checks: retry the claim
	}
	return nil, fmt.Errorf("server: claim %s: transition lock contention did not settle after %d attempts", path, attempts)
}

// bindLocked is the holder-only transition: the stale-check prelude
// (identical to the unix one) followed by the direct bind. Only the
// lock holder calls it — concurrent transitions are impossible.
func bindLocked(path string) (net.Listener, error) {
	if fi, err := os.Stat(path); err == nil {
		if fi.Mode()&os.ModeSocket != 0 {
			if c, derr := net.Dial("unix", path); derr == nil {
				c.Close()
				return nil, fmt.Errorf("%w: %s", ErrAlreadyListening, path)
			}
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("server: remove stale sock %s: %w", path, err)
			}
		} else {
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("server: clear non-socket path %s: %w", path, err)
			}
		}
	}
	return bindDirect(path)
}

// sockAlive probes the sock path with a dial — the liveness oracle for
// both the wait loop (a transitioning winner going live) and the
// stale-recovery decision.
func sockAlive(path string) bool {
	c, err := net.Dial("unix", path)
	if err == nil {
		c.Close()
		return true
	}
	return false
}
