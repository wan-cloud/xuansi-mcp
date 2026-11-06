GO ?= go

# Version stamped into every `make build` binary via -X main.Version
# (B8.5 T1): single source = git describe; override with `make build
# VERSION=vX.Y.Z`. `make matrix` deliberately does NOT inject — it stays
# a pure vet/build gate. The leading "v" is stripped right below so
# tag vX.Y.Z = npm X.Y.Z = binary X.Y.Z — one version shape across the
# whole release (controller ruling 2026-11-06; mcp initialize reports
# serverInfo.version, which must be bare semver like the npm side).
VERSION ?= $(shell git describe --tags --always --dirty)
VERSION := $(VERSION:v%=%)

# Cross-compile matrix: linux/windows × amd64/arm64, pure Go
# (CGO_ENABLED=0 — the project has no cgo). darwin is deliberately
# absent: go.bug.st/serial/enumerator's darwin backend is cgo-only
# (task-5 finding), so darwin binaries build on-machine (task 9),
# never cross-compiled from Linux.
MATRIX := linux/amd64 linux/arm64 windows/amd64 windows/arm64

.PHONY: vet test build matrix clean

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

# Cross-compile all four matrix targets; windows artifacts carry .exe.
# Each binary carries the VERSION stamp (single source: git describe).
build:
	mkdir -p bin
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/xuansi-linux-amd64 ./cmd/xuansi
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/xuansi-linux-arm64 ./cmd/xuansi
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/xuansi-windows-amd64.exe ./cmd/xuansi
	GOOS=windows GOARCH=arm64 CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-X main.Version=$(VERSION)" -o bin/xuansi-windows-arm64.exe ./cmd/xuansi

# Gate the whole matrix: vet + compile-everything per combo. go build
# ./... spans multiple main packages (cmd/xuansi + hil), so it compiles
# all of them and discards the binaries — nothing lands in bin/.
matrix:
	@set -e; for t in $(MATRIX); do \
		os=$${t%/*}; arch=$${t#*/}; \
		echo "== matrix GOOS=$$os GOARCH=$$arch (CGO_ENABLED=0): vet + build =="; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 $(GO) vet ./...; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 $(GO) build ./...; \
	done

clean:
	rm -rf bin
