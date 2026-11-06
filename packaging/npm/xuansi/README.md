# xuansi

xuansi (懸絲) — hardware probe daemon for agent-controlled ESP32 board
testing and control, with an MCP (Model Context Protocol) stdio face.
This npm package is the **launcher**: it resolves the prebuilt binary for
your platform and execs it with stdio passed through untouched, so MCP
JSON-RPC frames flow byte-exact between your agent and the daemon.

The binary itself arrives via `optionalDependencies` — npm installs
exactly one `@xuansi/<platform>` subpackage matching your OS/arch
(`os` + `cpu` fields do the selection automatically).

## Install

    npm install xuansi

## Usage

    xuansi list          # boards known to the daemon (USB + mDNS)
    xuansi serve         # run the daemon (discovery, leasing, unix-socket API)
    xuansi-mcp           # MCP face over stdio (same as `xuansi mcp`)

Zero-install via npx:

    npx -y xuansi list
    npx -y xuansi-mcp        # alias package — same as `xuansi mcp`

## Environment

- `XUANSI_BIN` — absolute path to a real xuansi binary; skips
  platform-package resolution entirely. Useful for testing and for
  running a locally-built binary.

## Platform packages

| platform      | package                 |
|---------------|-------------------------|
| linux-x64     | `@xuansi/linux-x64`     |
| linux-arm64   | `@xuansi/linux-arm64`   |
| win32-x64     | `@xuansi/win32-x64`     |
| win32-arm64   | `@xuansi/win32-arm64`   |
| darwin-x64    | `@xuansi/darwin-x64`    |
| darwin-arm64  | `@xuansi/darwin-arm64`  |

## MCP wiring (stdio)

Point your MCP client at the `xuansi-mcp` command, e.g. for OpenCode:

    { "mcp": { "xuansi": { "type": "local", "command": ["npx", "-y", "xuansi-mcp"] } } }
