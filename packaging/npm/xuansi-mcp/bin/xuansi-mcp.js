#!/usr/bin/env node
// xuansi-mcp — standalone alias package (controller ruling 2026-11-06):
// npx resolves PACKAGE names, so the design's `npx -y xuansi-mcp`
// zero-install invocation needs a package published under exactly that
// name. This bin delegates to the installed `xuansi` package's launcher
// and runs exactly `xuansi mcp [args...]` — binary resolution, stdio
// passthrough and exit/signal forwarding all come from the launcher
// (single implementation, zero extra hop).
'use strict';

const launcher = require('xuansi/bin/xuansi.js');

if (require.main === module) {
  launcher.run(['mcp'].concat(process.argv.slice(2)));
}
