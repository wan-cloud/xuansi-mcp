#!/usr/bin/env node
// xuansi-mcp — thin alias over the launcher: `xuansi-mcp [args...]` runs
// exactly `xuansi mcp [args...]`. Reuses the same resolution, stdio-inherit
// spawn and exit/signal forwarding (single implementation, zero extra hop).
'use strict';

const launcher = require('./xuansi.js');

if (require.main === module) {
  launcher.run(['mcp'].concat(process.argv.slice(2)));
}
