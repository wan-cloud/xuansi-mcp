#!/usr/bin/env node
// xuansi launcher — pure Node stdlib, ZERO npm dependencies.
//
// Resolves the platform binary and execs it with stdio inherited, so MCP
// frames pass through byte-exact in both directions (launcher adds no
// prefix, no buffering, no trailing bytes). Errors go to stderr only.
// The child's exit code — or signal — is forwarded to the parent.
//
// Resolution order:
//   1. $XUANSI_BIN — absolute path to a real binary; skips platform-package
//      resolution entirely. Test/dev seam; also lets users point at a
//      locally-built binary.
//   2. @xuansi/<platform> optionalDependency — npm auto-installs only the
//      subpackage matching the host (os+cpu fields in each subpackage).
//
// B8.5 Task 3 (family restructure 2026-11-06: this launcher lives in the
// MAIN package xuansi-mcp); platform map is the single source consumed by
// packaging/scripts/assemble-npm.sh (drift trips that script).

'use strict';

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

// Key: process.platform + '-' + process.arch  ->  npm subpackage name.
// Exactly six entries (B8.5 review focus #2: a missing/mistyped key
// strands that platform's users without a binary).
const platformPackage = {
  'linux-x64': '@xuansi/linux-x64',
  'linux-arm64': '@xuansi/linux-arm64',
  'win32-x64': '@xuansi/win32-x64',
  'win32-arm64': '@xuansi/win32-arm64',
  'darwin-x64': '@xuansi/darwin-x64',
  'darwin-arm64': '@xuansi/darwin-arm64',
};

const INSTALL_HINT =
  'install the matching @xuansi/<platform> package (npm install xuansi-mcp), ' +
  'or set XUANSI_BIN=/path/to/xuansi-binary';

class BinaryResolutionError extends Error {}

// resolveBinaryPath(opts) -> absolute binary path; throws BinaryResolutionError
// (message carries the install hint) when nothing resolves.
// opts: { platform, arch, binOverride, fromDir } — defaults pull from the
// live process; the seams exist so the smoke test can drive all six keys.
function resolveBinaryPath(opts) {
  opts = opts || {};
  const platform = opts.platform || process.platform;
  const arch = opts.arch || process.arch;
  const binOverride = 'binOverride' in opts ? opts.binOverride : process.env.XUANSI_BIN;
  const fromDir = opts.fromDir || __dirname;

  if (binOverride) {
    if (!fs.existsSync(binOverride)) {
      throw new BinaryResolutionError(
        `XUANSI_BIN points at a missing file: ${binOverride}`);
    }
    return binOverride;
  }

  const key = platform + '-' + arch;
  const pkg = platformPackage[key];
  if (!pkg) {
    throw new BinaryResolutionError(
      `no prebuilt xuansi binary for ${key} (${INSTALL_HINT})`);
  }
  let pkgJson;
  try {
    pkgJson = require.resolve(pkg + '/package.json', { paths: [fromDir] });
  } catch (_) {
    throw new BinaryResolutionError(
      `platform package ${pkg} is not installed (${INSTALL_HINT})`);
  }
  const bin = path.join(path.dirname(pkgJson), 'bin',
    platform === 'win32' ? 'xuansi.exe' : 'xuansi');
  if (!fs.existsSync(bin)) {
    throw new BinaryResolutionError(
      `platform package ${pkg} is present but its binary is missing (${bin})`);
  }
  return bin;
}

// run(argv): resolve, spawn with inherited stdio, forward signals + exit code.
function run(argv) {
  let bin;
  try {
    bin = resolveBinaryPath();
  } catch (err) {
    process.stderr.write(`xuansi: ${err.message}\n`);
    process.exit(1);
  }

  const child = spawn(bin, argv, { stdio: 'inherit' });

  // Forward termination signals (posix) so Ctrl-C and supervisors reach
  // the daemon through the launcher hop.
  let pendingSignal = null;
  ['SIGINT', 'SIGTERM'].forEach((sig) => {
    process.on(sig, () => {
      pendingSignal = sig;
      child.kill(sig);
    });
  });

  child.on('error', (err) => {
    process.stderr.write(`xuansi: failed to run ${bin}: ${err.message}\n`);
    process.exit(1);
  });

  child.on('close', (code, signal) => {
    if (code !== null) process.exit(code);
    // Child died by signal: die the same way (restore the default
    // disposition, then re-raise) so shells see the launcher's fate.
    const sig = signal || pendingSignal;
    if (sig && process.platform !== 'win32') {
      process.removeAllListeners(sig);
      process.kill(process.pid, sig);
    }
    process.exit(1);
  });
}

// Test seam: the map and resolver are importable; the CLI runs only when
// executed directly (the sibling bin/xuansi-mcp.js reuses run() with
// "mcp" prepended).
module.exports = { platformPackage, resolveBinaryPath, INSTALL_HINT, run };

if (require.main === module) {
  run(process.argv.slice(2));
}
