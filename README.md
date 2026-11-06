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
    make matrix  # every cross combo (below): CGO_ENABLED=0 vet + build ./..., no binaries written
    make build   # bin/xuansi-<os>-<arch> for all 4 cross targets (windows ones end .exe)
    make clean   # remove bin/

Cross matrix: linux/windows × amd64/arm64, built pure-Go (`CGO_ENABLED=0`
— the project has no cgo), so all four targets cross-compile from any
host. **darwin is not cross-compiled**: `go.bug.st/serial/enumerator`'s
darwin backend is cgo-only (T5 finding), so a darwin binary cannot be
produced from Linux; build it on the mac itself (`go build ./cmd/xuansi`,
on-machine, task 9).

## Paths (internal/paths)

Per-OS landing spots, resolved by `internal/paths` (the implemented
source of truth — this table mirrors it exactly):

| What | Linux | Windows | macOS |
|---|---|---|---|
| API socket | `$XDG_RUNTIME_DIR/xuansi/daemon.sock`, else `/tmp/xuansi-daemon.sock` | `%LOCALAPPDATA%\xuansi\daemon.sock` | `$TMPDIR/xuansi/daemon.sock`, else `/tmp/xuansi/daemon.sock` |
| State dir (holds `serve.log`) | `$XDG_STATE_HOME/xuansi`, else `~/.local/state/xuansi` | `%LOCALAPPDATA%\xuansi\state` | `~/Library/Logs/xuansi` |
| Data dir (captures + workspace) | `$XDG_DATA_HOME/xuansi`, else `~/.local/share/xuansi` | `%LOCALAPPDATA%\xuansi` | `~/Library/Application Support/xuansi` |

Callers join their own file names onto the state/data dirs. When
`%LOCALAPPDATA%` is not resolvable the Windows spellings fall back to
relative `xuansi\…` paths; when `$HOME` is unknown the Linux fallbacks
go relative too (see `internal/paths/paths.go`).

## Dependency policy

Stdlib only, plus exactly five approved external dependencies (introduced
by their owning tasks, not before):

- `vmihailenco/msgpack/v5` — wire frame codec
- `go.bug.st/serial` — USB serial transport; `go.bug.st/serial/enumerator`
  (USB port enumeration) is a subpackage of this same module, not a new
  dependency
- `grandcat/zeroconf` — mDNS discovery
- `modelcontextprotocol/go-sdk` — the MCP face (`xuansi mcp`)
- `golang.org/x/sys` — `windows/registry`, the USB serial fallback's
  PnP enum-tree walk (upstream's `\w` serial regex cannot parse
  Espressif's colon-MAC iSerial); promoted from indirect to direct per
  the spec's pre-authorization — Go-team-maintained quasi-stdlib

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
    internal/paths/      per-OS socket/state/data resolution (see Paths
                         table above; Windows %LOCALAPPDATA%, macOS Library)
    hil/                 live-board HIL runners (b5t8.go: -scenario usb|tcp;
                         b5t9.go: -scenario discover; b5t10.go: -scenario lease)
                         + capture scripts → results/
    results/             committed evidence (ASCII filenames)

## Documentation

Protocol behavior, design decisions, and the frozen REPL contract are
documented in comments and tests throughout this repo. Design docs and
planning records are maintained separately (not public).

## Firmware

This is a hardware workbench project: the ESP32 firmware is closed-source
for now and ships with the devices — it is not part of this repository.
What lives here is the host side: the daemon, CLI, and MCP face that an
agent drives over the wire. Board drivers are Lua scripts pushed to the
device at runtime, so the driver surface stays an open extension point
independent of the firmware image.

## License

Apache-2.0 — see [LICENSE](LICENSE).
