// Package lease grants exclusive, per-board leases to agent sessions.
// On agent disconnect the lease enters a grace period (grace-TTL): the
// board-side state is kept, the owner may take over again within the
// TTL, and on expiry the daemon resets the session and releases the
// board into a safe state. Daemon-to-board link breaks are reconciled
// by resync, not teardown.
package lease
