# xuansi-mcp

Zero-install MCP alias for the `xuansi` package (懸絲 — hardware probe
daemon with an MCP stdio face for agent-controlled ESP32 testing):

    npx -y xuansi-mcp

npx resolves **package** names, so that invocation ships as this package.
Its single dependency is `xuansi` (pinned per release); the `xuansi-mcp`
bin delegates to that package's launcher and runs exactly `xuansi mcp` —
same binary resolution (`@xuansi/<platform>` optionalDependencies, or
`XUANSI_BIN`), same byte-exact stdio passthrough, same exit/signal
forwarding.

Point your MCP client at it, e.g. for OpenCode:

    { "mcp": { "xuansi": { "type": "local", "command": ["npx", "-y", "xuansi-mcp"] } } }
