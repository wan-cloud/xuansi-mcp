package main

// b7_drivers_sha_test.go — B8 item 1's regression proof, boardless:
// the generated driver constants in b7-drivers.go hash to exactly the
// controller-ruled sha256s/lengths b7DriverSHAAsserts pins (it is the
// same check the b7 HIL runs at S0 startup). A regeneration that
// altered a single byte — or a regenerated header that stopped matching
// the committed file — fails HERE, at `go test ./...`, instead of at
// the bench.

import "testing"

func TestB7DriverSHAAsserts(t *testing.T) {
	h := &harness{}
	if !b7DriverSHAAsserts(h) {
		t.Fatalf("driver sha asserts failed:\n%s", h.failures)
	}
	if h.fail != 0 || len(h.failures) != 0 {
		t.Fatalf("sha asserts reported %d failure(s): %v", h.fail, h.failures)
	}
}
