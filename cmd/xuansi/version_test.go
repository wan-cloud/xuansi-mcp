package main

import "testing"

// TestVersionInjected: run with
//
//	go test ./cmd/xuansi/ -ldflags "-X xuansi/cmd/xuansi.Version=v9.9.9-test" -run TestVersionInjected
//
// and the linker-set string must arrive in Version byte-for-byte. That
// is the seam `make build` bakes `git describe --tags --always --dirty`
// into (Makefile VERSION) and the npm packages' version rides on — a
// broken injection here means shipped binaries would say "dev".
//
// Note the symbol path: a go-test binary links the package under test
// under its import path, not "main", so the test harness must say
// xuansi/cmd/xuansi.Version — while real binaries (go build via
// make build) are genuine main packages and take the plain
// -X main.Version=... the Makefile passes (verified separately by the
// strings-on-bin/xuansi-linux-amd64 gate).
//
// Without injection the var keeps its "dev" default, so plain `go test`
// skips: the assertion proves nothing unless the linker path was
// exercised.

func TestVersionInjected(t *testing.T) {
	if Version == "dev" {
		t.Skip(`Version=="dev" (no injection); rerun with -ldflags "-X main.Version=v9.9.9-test"`)
	}
	if Version != "v9.9.9-test" {
		t.Fatalf("Version = %q, want %q (injection mangled)", Version, "v9.9.9-test")
	}
}
