# xuansi

此包名為**佔位保留**（防搶注／防釣魚）：`xuansi` 這個名字僅作守位之用，實際功能全部在主包 **`xuansi-mcp`** 之中，本包不提供任何命令。請安裝並使用 `xuansi-mcp`——全局安裝：`npm install -g xuansi-mcp`（之後即可用 `xuansi list`、`xuansi serve`、`xuansi-mcp` 等命令）；零安裝直接運行：`npx -y xuansi-mcp list`，或把 MCP 客戶端指向 `npx -y xuansi-mcp`。本佔位包唯一的依賴就是 `xuansi-mcp`，安裝它會一併帶上主包，但沒有自己的 bin。

This package name is a **defensive placeholder** (squat-guard): the `xuansi` name is reserved only to protect it — all real functionality lives in the main package **`xuansi-mcp`**, and this package provides no commands. Please install and use `xuansi-mcp`: globally via `npm install -g xuansi-mcp` (then use `xuansi list`, `xuansi serve`, `xuansi-mcp`, …), or zero-install with `npx -y xuansi-mcp list`, or point your MCP client at `npx -y xuansi-mcp`. This placeholder's only dependency is `xuansi-mcp`, so installing it pulls in the main package too, but it ships no bin of its own.
