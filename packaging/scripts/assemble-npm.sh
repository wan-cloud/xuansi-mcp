#!/usr/bin/env bash
# assemble-npm.sh <version> — assemble the npm publish family from bin/
# build outputs into packaging/dist/xuansi-<version>/.
#
# Layout produced:
#   packaging/dist/xuansi-<version>/
#     xuansi/                 main package: version stamped into package.json,
#                             optionalDependencies PINNED to <version>
#                             (in-repo they are "*"), bin/ + README.md
#     <platform-key>/         one dir per platform subpackage (standalone
#                             publishable): package.json (os+cpu+files) and
#                             bin/xuansi (bin/xuansi.exe on win32 keys)
#     xuansi-mcp/             alias package (controller ruling 2026-11-06):
#                             version stamped, dependency on xuansi PINNED
#                             (in-repo "*"), bin/xuansi-mcp.js + README.md
#
# Publish order: the six @xuansi/<platform> subpackages FIRST, then the
# main `xuansi` package, then the alias `xuansi-mcp` LAST — every package
# must have its pinned dependencies already on the registry at the moment
# it becomes installable (xuansi pins the subpackages; xuansi-mcp pins
# xuansi).
#
# Binary source convention (bin/ at repo root, `make build` outputs):
#     linux-x64    <- bin/xuansi-linux-amd64
#     linux-arm64  <- bin/xuansi-linux-arm64
#     win32-x64    <- bin/xuansi-windows-amd64.exe
#     win32-arm64  <- bin/xuansi-windows-arm64.exe
#
# darwin/ slot convention (B8.5 T3): darwin is cgo-only (builds on macOS,
# never cross-compiled from Linux). CI's macos job builds the two darwin
# binaries and drops them at:
#     darwin-x64   <- bin/xuansi-darwin-amd64
#     darwin-arm64 <- bin/xuansi-darwin-arm64
# BEFORE the publish job runs this script. When those files are absent
# (local Linux assembly), the two darwin subpackages are SKIPPED with a
# printed note — never an error. linux/win32 artifacts missing IS an
# error: run `make build` first.
#
# The six platform keys are read from the launcher's platformPackage map
# (packaging/npm/xuansi/bin/xuansi.js) — single source; a map key without
# a bin/ artifact mapping below trips this script (and the smoke test
# checks the map against the same six keys in the other direction).
#
# Version MUST be bare semver (npm rejects a leading "v"; strip it from
# the git tag: ${GITHUB_REF_NAME#v}).
set -euo pipefail

usage() { echo "usage: assemble-npm.sh <version>   # e.g. assemble-npm.sh 0.1.0" >&2; exit 2; }

[ $# -eq 1 ] || usage
VERSION=$1
case $VERSION in
  v*) echo "assemble-npm.sh: version must be bare semver, got '$VERSION' (strip the leading v)" >&2; exit 2 ;;
esac

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=$ROOT/bin
MAIN_SRC=$ROOT/packaging/npm/xuansi
LAUNCHER=$MAIN_SRC/bin/xuansi.js
DIST=$ROOT/packaging/dist/xuansi-$VERSION

[ -f "$LAUNCHER" ] || { echo "assemble-npm.sh: launcher missing: $LAUNCHER" >&2; exit 1; }

# key -> bin/ artifact (single source of platforms = launcher map; the
# case's * arm is the drift tripwire).
artifact_for() {
  case $1 in
    linux-x64)    echo "xuansi-linux-amd64" ;;
    linux-arm64)  echo "xuansi-linux-arm64" ;;
    win32-x64)    echo "xuansi-windows-amd64.exe" ;;
    win32-arm64)  echo "xuansi-windows-arm64.exe" ;;
    darwin-x64)   echo "xuansi-darwin-amd64" ;;
    darwin-arm64) echo "xuansi-darwin-arm64" ;;
    *) echo "assemble-npm.sh: launcher map key '$1' has no bin/ artifact mapping — update this script" >&2; return 1 ;;
  esac
}

echo "== assemble-npm $VERSION -> ${DIST#"$ROOT"/}"

rm -rf "$DIST"
mkdir -p "$DIST/xuansi/bin"

# ---- main package: launcher bins + README + stamped/pinned package.json
cp "$MAIN_SRC/bin/xuansi.js" "$MAIN_SRC/bin/xuansi-mcp.js" "$DIST/xuansi/bin/"
chmod 755 "$DIST/xuansi/bin/xuansi.js" "$DIST/xuansi/bin/xuansi-mcp.js"
cp "$MAIN_SRC/README.md" "$DIST/xuansi/README.md"
node - "$MAIN_SRC/package.json" "$DIST/xuansi/package.json" "$VERSION" <<'EOF'
const fs = require('fs');
const [src, dest, version] = process.argv.slice(2);
const pkg = JSON.parse(fs.readFileSync(src, 'utf8'));
pkg.version = version;
// Pin optionalDependencies to this exact version — in-repo "*" lets local
// dev install any; at publish time the family must move in lockstep.
for (const name of Object.keys(pkg.optionalDependencies || {})) {
  pkg.optionalDependencies[name] = version;
}
fs.writeFileSync(dest, JSON.stringify(pkg, null, 2) + '\n');
EOF
echo "   main package: xuansi@$VERSION (optionalDependencies pinned)"

# ---- alias package xuansi-mcp: npx resolves package names, so the
# design's `npx -y xuansi-mcp` invocation needs a package published
# under exactly that name (controller ruling 2026-11-06). Published
# LAST — it pins a dependency on `xuansi` (see publish order above).
ALIAS_SRC=$ROOT/packaging/npm/xuansi-mcp
mkdir -p "$DIST/xuansi-mcp/bin"
cp "$ALIAS_SRC/bin/xuansi-mcp.js" "$DIST/xuansi-mcp/bin/"
chmod 755 "$DIST/xuansi-mcp/bin/xuansi-mcp.js"
cp "$ALIAS_SRC/README.md" "$DIST/xuansi-mcp/README.md"
node - "$ALIAS_SRC/package.json" "$DIST/xuansi-mcp/package.json" "$VERSION" <<'EOF'
const fs = require('fs');
const [src, dest, version] = process.argv.slice(2);
const pkg = JSON.parse(fs.readFileSync(src, 'utf8'));
pkg.version = version;
// Pin the xuansi dependency — in-repo "*" tracks local dev; at publish
// time the family must move in lockstep.
for (const name of Object.keys(pkg.dependencies || {})) {
  pkg.dependencies[name] = version;
}
fs.writeFileSync(dest, JSON.stringify(pkg, null, 2) + '\n');
EOF
echo "   alias package: xuansi-mcp@$VERSION (dependency xuansi pinned)"

# ---- six platform subpackages (keys from the launcher map)
SKIPPED_DARWIN=0
while IFS= read -r key; do
  os=${key%-*}; cpu=${key#*-}
  artifact=$(artifact_for "$key")
  src_bin=$BIN/$artifact
  if [ ! -f "$src_bin" ]; then
    if [ "$os" = darwin ]; then
      echo "   NOTE skipped darwin subpackage @xuansi/$key: no $artifact"
      echo "        (expected locally; CI's macos job provides it — see header)"
      SKIPPED_DARWIN=$((SKIPPED_DARWIN + 1))
      continue
    fi
    echo "assemble-npm.sh: missing $src_bin — run 'make build' first" >&2
    exit 1
  fi
  binname=xuansi; [ "$os" = win32 ] && binname=xuansi.exe
  mkdir -p "$DIST/$key/bin"
  cp "$src_bin" "$DIST/$key/bin/$binname"
  chmod 755 "$DIST/$key/bin/$binname"
  node - "$DIST/$key/package.json" "@xuansi/$key" "$VERSION" "$os" "$cpu" <<'EOF'
const fs = require('fs');
const [dest, name, version, os, cpu] = process.argv.slice(2);
fs.writeFileSync(dest, JSON.stringify({
  name,
  version,
  description: `xuansi daemon binary for ${os}-${cpu} (installed automatically by the xuansi package)`,
  // repository.url must EXACTLY match the GitHub repo — hard requirement
  // of npm trusted publishers (OIDC): the registry matches it against
  // the per-package trusted publisher config. Same value as the two
  // hand-maintained templates (packaging/npm/*/package.json).
  repository: { type: 'git', url: 'git+https://github.com/wan-cloud/xuansi-mcp.git' },
  os: [os],
  cpu: [cpu],
  files: ['bin'],
}, null, 2) + '\n');
EOF
  echo "   subpackage: @xuansi/$key@$VERSION  <- bin/$artifact"
done < <(node -e 'process.stdout.write(Object.keys(require(process.argv[1]).platformPackage).sort().join("\n") + "\n")' "$LAUNCHER")

# ---- sanity: everything assembled is a map key; nothing silently lost
# (assembled subpackages + darwin skips must equal the map's key count —
# guards against loop/pipe bugs dropping a platform)
node - "$LAUNCHER" "$DIST" <<'EOF'
const fs = require('fs');
const { platformPackage } = require(process.argv[2]);
const dist = process.argv[3];
const keys = new Set(Object.keys(platformPackage));
// 'xuansi' (main) and 'xuansi-mcp' (alias) are the two non-subpackage
// dist entries; everything else must be a launcher-map key.
const made = fs.readdirSync(dist).filter((e) => e !== 'xuansi' && e !== 'xuansi-mcp');
for (const entry of made) {
  if (!keys.has(entry)) {
    console.error(`assemble-npm.sh: unexpected dist entry '${entry}' (not in launcher map)`);
    process.exit(1);
  }
}
const expected = keys.size - [...keys].filter((k) => k.startsWith('darwin-')
  && !made.includes(k)).length;
if (made.length !== expected) {
  console.error(`assemble-npm.sh: assembled ${made.length} subpackages, expected ${expected} (map=${keys.size}, darwin-skipped=${keys.size - made.length})`);
  console.error(`  present: ${made.sort().join(', ')}`);
  process.exit(1);
}
EOF

echo "== done: $DIST"
[ $SKIPPED_DARWIN -eq 0 ] || echo "   ($SKIPPED_DARWIN darwin subpackage(s) skipped — CI macos job fills the slot)"
