GO ?= go

.PHONY: vet test build clean

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

# Cross-compile both supported architectures.
build:
	mkdir -p bin
	GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o bin/xuansi-linux-amd64 ./cmd/xuansi
	GOOS=linux GOARCH=arm64 $(GO) build -trimpath -o bin/xuansi-linux-arm64 ./cmd/xuansi

clean:
	rm -rf bin
