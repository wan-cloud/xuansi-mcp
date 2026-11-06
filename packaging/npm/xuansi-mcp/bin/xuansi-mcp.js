#!/usr/bin/env node
// xuansi-mcp — in-package dual bin (family restructure 2026-11-06): the
// MAIN package is xuansi-mcp, so this bin calls the sibling launcher
// programmatically — `xuansi-mcp [args...]` runs exactly
// `xuansi mcp [args...]` via require("./xuansi.js").run(). Same binary
// resolution, stdio-inherit spawn and exit/signal forwarding — single
// implementation, zero dependency hop (the old alias package's
// `require("xuansi/bin/xuansi.js")` cross-package hop is gone).
'use strict';

const launcher = require('./xuansi.js');

if (require.main === module) {
  launcher.run(['mcp'].concat(process.argv.slice(2)));
}
