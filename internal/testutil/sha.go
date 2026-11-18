package testutil

// sha.go — the hermetic test-suite sha helper.

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// Sha256Hex hashes b and returns the 64-char hex digest.
func Sha256Hex(t testing.TB, b []byte) string {
	t.Helper()
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
