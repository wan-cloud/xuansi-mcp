# xuansi-daemon

Host-side daemon core of the xuansi hardware probe (懸絲 — "reading the pulse
through the thread"): an agent behind this daemon reads and drives ESP32
boards over the wire REPL protocol. The daemon discovers boards (USB by-id +
mDNS, merged by the board's 6-byte MAC), holds an exclusive per-board lease
with grace-TTL teardown when an agent drops, and serves the internal API to
agents over a unix socket; the `xuansi` CLI drives all of it. Layering
bottom-up: `internal/replclient` → `internal/discover` → `internal/lease`
(pure lease/teardown policy) + `internal/board` (per-board controller
connection + resync) → `internal/server`, fronted by `cmd/xuansi`.

## Build

Requires Go ≥ 1.27.

    make vet     # go vet ./...
    make test    # go test ./...
    make build   # bin/xuansi-linux-amd64 + bin/xuansi-linux-arm64 (GOOS/GOARCH cross-compile)
    make clean   # remove bin/

## Dependency policy

Stdlib only, plus exactly four approved external dependencies (introduced
by their owning tasks, not before):

- `vmihailenco/msgpack/v5` — wire frame codec
- `go.bug.st/serial` — USB serial transport
- `grandcat/zeroconf` — mDNS discovery
- `modelcontextprotocol/go-sdk` — the MCP face (`xuansi mcp`)

Nothing else enters go.mod without changing this policy. The whitelist
governs DIRECT dependencies: the go-sdk pulls its own indirect
requirements (golang.org/x/… and friends), which is fine — direct
additions are what need a ruling. The CLI uses stdlib `flag` subcommand
dispatch (no cobra). Logging is stdlib `log/slog`, text handler on
stderr, `--verbose` for debug level.

## Layout

    cmd/xuansi/          CLI entry: stdlib-flag subcommand dispatch
                         (serve / list / status / lease / release / eval /
                         repl / mcp)
    internal/replclient/ frame codec + Link abstraction + router + client
                         (repl.py protocol knowledge, Go port)
    internal/discover/   USB by-id + mDNS browse → mac6-joined board registry
    internal/lease/      exclusive per-board lease + grace-TTL teardown
                         (clock-injected policy: adopt-within-TTL, expiry
                         session.reset exactly-once, eval-namespace sweep)
    internal/board/      per-board supervisor: warm TCP controller route,
                         USB fallback (reboot, on demand), link-loss resync
    internal/server/     unix socket internal API (length-prefixed JSON:
                         request / reply / event)
    hil/                 live-board HIL runners (b5t8.go: -scenario usb|tcp;
                         b5t9.go: -scenario discover; b5t10.go: -scenario lease)
                         + capture scripts → results/
    results/             committed evidence (ASCII filenames)

## Docs (mgmt repo)

- Plan: ../../docs/superpowers/plans/2026-11-04-v1批次5-fw前置與daemon核心.md
- Spec: ../../docs/資料/懸絲設計文件.md (§6 script lifecycle/teardown, §8 daemon & MCP face)
- Protocol: ../../docs/資料/v1-repl-協議.md (§6 per-op silence bounds = client timeout table)
- Rulings: ../../docs/資料/v1執行裁決與遺留.md
