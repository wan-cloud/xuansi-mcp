#!/usr/bin/env bash
# launcher-test.sh — smoke test for the xuansi-mcp npm main package
# launcher (B8.5 T3; family restructure 2026-11-06: main package IS
# xuansi-mcp, dual bin, alias hop removed).
#
# Posix (Linux/macOS) + node on PATH; hermetic (mktemp scratch, no npm
# install — the launcher is stdlib-only). Exit 0 iff every check passes.
#
# Checks (plan 2026-11-06-b85-npm-publish Task 3 Steps 1-3 + Task 5):
#   1. platform map unit: exactly six keys, values @xuansi/<key>
#   2. map dry-resolution: each key resolves against a stub node_modules
#      (proves require.resolve + bin/xuansi(.exe) naming for all six)
#   3. unsupported platform: resolution error carries the install hint
#   4. stdio purity: XUANSI_BIN fake binary echoing stdin frame sequence
#      -> launcher stdout is byte-exact (no prefix/buffering/trailing NL),
#      stderr untouched
#   5. exit-code forwarding: fake exit 7 -> launcher 7 (and 0 -> 0)
#   6. xuansi-mcp arg prepending: forwarded argv starts with "mcp"
#   7. missing platform package + no XUANSI_BIN: install hint on stderr,
#      empty stdout, non-zero exit
#   8. SIGTERM forwarding: TERM to launcher reaches the child
#   9. main package contract: name xuansi-mcp, DUAL bin map
#      (xuansi + xuansi-mcp), six @xuansi/* optionalDependencies
#  10. dual-bin stdio passthrough: bin/xuansi-mcp.js (in-package sibling
#      require of the launcher — no dependency hop) -> stdout byte-exact
#      through the programmatic run() call, stderr clean
#  11. dual-bin exit forwarding: fake exit 7 -> xuansi-mcp bin exit 7
#  12. placeholder package contract: name "xuansi", version 0.0.1, NO
#      bin, files README.md only, single dependency xuansi-mcp
#      (squat-guard — must never grow a bin)
set -u

TESTDIR=$(cd "$(dirname "$0")" && pwd)
LAUNCHER="$TESTDIR/../bin/xuansi.js"
MCP_LAUNCHER="$TESTDIR/../bin/xuansi-mcp.js"
PLACEHOLDER_SRC=$(cd "$TESTDIR/../../xuansi-placeholder" && pwd)
SCRATCH=$(mktemp -d /tmp/xuansi-launcher-test.XXXXXX)
trap 'rm -rf "$SCRATCH"' EXIT

PASS=0
FAIL=0
ok()  { PASS=$((PASS + 1)); echo "ok   - $1"; }
bad() { FAIL=$((FAIL + 1)); echo "FAIL - $1"; }

# ---------------------------------------------------------------- fakes
cat >"$SCRATCH/fake-cat.sh" <<'EOF'
#!/bin/sh
# fake xuansi binary: byte-exact stdin -> stdout echo
cat
EOF
cat >"$SCRATCH/fake-exit.sh" <<'EOF'
#!/bin/sh
exit 7
EOF
cat >"$SCRATCH/fake-exit0.sh" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$SCRATCH/fake-argv.sh" <<'EOF'
#!/bin/sh
printf '%s\n' "$@"
EOF
cat >"$SCRATCH/fake-term.sh" <<'EOF'
#!/bin/sh
trap 'printf "TERM-FORWARDED\n"; exit 9' TERM
while :; do sleep 1 & wait $!; done
EOF
chmod +x "$SCRATCH"/*.sh

# --------------------------------------------- 1+2: map keys + dry resolve
node - "$LAUNCHER" "$SCRATCH" <<'EOF'
const { platformPackage, resolveBinaryPath } = require(process.argv[2]);
const fs = require('fs');
const path = require('path');
const scratch = process.argv[3];

const expect = ['darwin-arm64', 'darwin-x64', 'linux-arm64', 'linux-x64',
                'win32-arm64', 'win32-x64'];
const got = Object.keys(platformPackage).sort();
if (got.join(',') !== expect.join(',')) {
  console.error(`map keys mismatch\n  got:    ${got.join(',')}\n  expect: ${expect.join(',')}`);
  process.exit(1);
}

let failures = 0;
for (const key of got) {
  if (platformPackage[key] !== '@xuansi/' + key) {
    console.error(`map value wrong for ${key}: ${platformPackage[key]}`);
    failures++;
    continue;
  }
  // Stub the subpackage under scratch/node_modules so require.resolve
  // (handed fromDir=scratch) can find it — stands in for the real
  // optionalDependency install.
  const dir = path.join(scratch, 'node_modules', '@xuansi', key);
  fs.mkdirSync(path.join(dir, 'bin'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'package.json'),
    JSON.stringify({ name: '@xuansi/' + key, version: '0.0.0' }));
  const [platform, arch] = key.split('-');
  const binName = platform === 'win32' ? 'xuansi.exe' : 'xuansi';
  fs.writeFileSync(path.join(dir, 'bin', binName), '#!/bin/sh\n');

  const want = path.join(dir, 'bin', binName);
  try {
    const bin = resolveBinaryPath({ platform, arch, binOverride: null, fromDir: scratch });
    if (bin !== want) {
      console.error(`resolve ${key}: got ${bin}, want ${want}`);
      failures++;
    }
  } catch (err) {
    console.error(`resolve ${key} threw: ${err.message}`);
    failures++;
  }
}
process.exit(failures);
EOF
[ $? -eq 0 ] && ok "platform map: six exact keys + six stub dry-resolutions" \
               || bad "platform map keys/values or dry-resolution (see above)"

# ------------------------------------- 3: unsupported platform -> hint
node - "$LAUNCHER" <<'EOF'
const { resolveBinaryPath } = require(process.argv[2]);
try {
  resolveBinaryPath({ platform: 'sunos', arch: 'x64', binOverride: null, fromDir: '/' });
  console.error('unsupported platform did not throw');
  process.exit(1);
} catch (err) {
  const msg = err.message;
  if (!/XUANSI_BIN/.test(msg) || !/install/i.test(msg)) {
    console.error(`error message lacks install hint: ${msg}`);
    process.exit(1);
  }
}
EOF
[ $? -eq 0 ] && ok "unsupported platform: error carries install hint" \
               || bad "unsupported platform error/hint (see above)"

# ------------------------- 4: stdio purity — byte-exact stdin/stdout echo
{
  printf 'Content-Length: 58\r\n\r\n'
  printf '{"jsonrpc":"2.0","id":1,"method":"initialize"}\n'
  printf 'reply-frame MiXeDcAsE payload bytes:\xc3\xa9\xc3\xa8 tail\n'
  printf 'final-frame-no-trailing-newline'
} >"$SCRATCH/frames.in"

XUANSI_BIN="$SCRATCH/fake-cat.sh" node "$LAUNCHER" \
  <"$SCRATCH/frames.in" >"$SCRATCH/frames.out" 2>"$SCRATCH/frames.err"
rc=$?
if [ $rc -eq 0 ] && cmp -s "$SCRATCH/frames.in" "$SCRATCH/frames.out" \
   && [ ! -s "$SCRATCH/frames.err" ]; then
  ok "stdio inherit: stdout byte-exact (frame seq), stderr clean"
else
  bad "stdio purity (rc=$rc; run: cmp $(cmp -s "$SCRATCH/frames.in" "$SCRATCH/frames.out" && echo eq || echo differs); stderr=$(wc -c <"$SCRATCH/frames.err")B)"
fi

# ------------------------------------------- 5: exit-code forwarding
XUANSI_BIN="$SCRATCH/fake-exit.sh" node "$LAUNCHER" >/dev/null 2>&1
[ $? -eq 7 ] && ok "exit-code forwarding: fake exit 7 -> launcher 7" \
             || bad "exit-code forwarding (expected 7)"
XUANSI_BIN="$SCRATCH/fake-exit0.sh" node "$LAUNCHER" >/dev/null 2>&1
[ $? -eq 0 ] && ok "exit-code forwarding: fake exit 0 -> launcher 0" \
             || bad "exit-code forwarding (expected 0)"

# ------------------------------------------ 6: xuansi-mcp prepends "mcp"
# fake-argv prints one argv element per line; expect "mcp" first.
out=$(XUANSI_BIN="$SCRATCH/fake-argv.sh" node "$MCP_LAUNCHER" --transport stdio extra 2>/dev/null)
want=$(printf '%s\n' mcp --transport stdio extra)
if [ "$out" = "$want" ] && [ "$(printf '%s\n' "$out" | head -1)" = "mcp" ]; then
  ok "xuansi-mcp: argv forwarded with 'mcp' prepended"
else
  bad "xuansi-mcp prepend (got: $(printf '%s|' $(printf '%s\n' "$out")) )"
fi

# ------------------- 7: no matching package, no XUANSI_BIN -> hint+exit
( cd "$SCRATCH" && env -u XUANSI_BIN node "$LAUNCHER" list ) \
  >"$SCRATCH/miss.out" 2>"$SCRATCH/miss.err"
rc=$?
if [ $rc -ne 0 ] && grep -q 'XUANSI_BIN' "$SCRATCH/miss.err" \
   && grep -qi 'install' "$SCRATCH/miss.err" && [ ! -s "$SCRATCH/miss.out" ]; then
  ok "missing package: install hint on stderr, empty stdout, exit $rc"
else
  bad "missing-package path (rc=$rc)"
fi

# -------------------------------------------- 8: SIGTERM forwarding
XUANSI_BIN="$SCRATCH/fake-term.sh" node "$LAUNCHER" serve \
  >"$SCRATCH/term.out" 2>"$SCRATCH/term.err" &
tpid=$!
sleep 1
kill -TERM "$tpid" 2>/dev/null
wait "$tpid"; trc=$?
if [ $trc -eq 9 ] && grep -q 'TERM-FORWARDED' "$SCRATCH/term.out"; then
  ok "SIGTERM forwarding: child saw TERM, exit 9 propagated"
else
  bad "SIGTERM forwarding (rc=$trc, out=$(cat "$SCRATCH/term.out"))"
fi

# ------------------ 9: main package contract (family restructure)
# The main package IS xuansi-mcp: dual bin (xuansi + xuansi-mcp), six
# @xuansi/* optionalDependencies ("*" in-repo; assemble pins them).
node - "$TESTDIR/../package.json" <<'EOF'
const pkg = JSON.parse(require('fs').readFileSync(process.argv[2], 'utf8'));
const errs = [];
if (pkg.name !== 'xuansi-mcp') errs.push(`name: ${pkg.name}`);
if (!pkg.bin || pkg.bin['xuansi'] !== 'bin/xuansi.js'
            || pkg.bin['xuansi-mcp'] !== 'bin/xuansi-mcp.js')
  errs.push(`bin map: ${JSON.stringify(pkg.bin)}`);
const opt = Object.keys(pkg.optionalDependencies || {}).sort();
const want = ['@xuansi/darwin-arm64', '@xuansi/darwin-x64',
              '@xuansi/linux-arm64', '@xuansi/linux-x64',
              '@xuansi/win32-arm64', '@xuansi/win32-x64'];
if (opt.join(',') !== want.join(','))
  errs.push(`optionalDependencies: ${opt.join(',')}`);
process.exit(errs.length ? (console.error(errs.join('; ')), 1) : 0);
EOF
[ $? -eq 0 ] && ok "main package contract: name xuansi-mcp, dual bin, six @xuansi/* optionalDeps" \
               || bad "main package contract (see above)"

# -------- 10-11: dual-bin — bin/xuansi-mcp.js drives the sibling
# launcher programmatically (require("./xuansi.js").run(["mcp",...]));
# no dependency hop anymore. Prepend itself is check 6 (same bin); here:
# stdio byte-exactness and exit forwarding through the programmatic path.

XUANSI_BIN="$SCRATCH/fake-cat.sh" node "$MCP_LAUNCHER" \
  <"$SCRATCH/frames.in" >"$SCRATCH/dual-frames.out" 2>"$SCRATCH/dual-frames.err"
rc=$?
if [ $rc -eq 0 ] && cmp -s "$SCRATCH/frames.in" "$SCRATCH/dual-frames.out" \
   && [ ! -s "$SCRATCH/dual-frames.err" ]; then
  ok "dual-bin stdio passthrough: xuansi-mcp bin -> run() -> byte-exact stdout, stderr clean"
else
  bad "dual-bin stdio passthrough (rc=$rc; stderr=$(wc -c <"$SCRATCH/dual-frames.err")B)"
fi

XUANSI_BIN="$SCRATCH/fake-exit.sh" node "$MCP_LAUNCHER" >/dev/null 2>&1
[ $? -eq 7 ] && ok "dual-bin exit forwarding: fake exit 7 -> xuansi-mcp bin 7" \
              || bad "dual-bin exit forwarding (expected 7)"

# ---------------- 12: placeholder package xuansi (squat-guard)
# Owner ruling 2026-11-06: bare `xuansi` on npm is a defensive
# placeholder pointing at xuansi-mcp — it must NEVER grow a bin.
node - "$PLACEHOLDER_SRC/package.json" "$PLACEHOLDER_SRC/README.md" <<'EOF'
const fs = require('fs');
const [pkgPath, readmePath] = process.argv.slice(2);
const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
const errs = [];
if (pkg.name !== 'xuansi') errs.push(`name: ${pkg.name}`);
if (pkg.version !== '0.0.1') errs.push(`version: ${pkg.version} (must stay 0.0.1)`);
if (pkg.bin) errs.push(`must have NO bin: ${JSON.stringify(pkg.bin)}`);
const deps = Object.keys(pkg.dependencies || {});
if (deps.length !== 1 || deps[0] !== 'xuansi-mcp')
  errs.push(`deps: ${JSON.stringify(pkg.dependencies)}`);
if (JSON.stringify(pkg.files || []) !== '["README.md"]')
  errs.push(`files: ${JSON.stringify(pkg.files)}`);
const readme = fs.readFileSync(readmePath, 'utf8');
if (!/xuansi-mcp/.test(readme)) errs.push('README does not point at xuansi-mcp');
process.exit(errs.length ? (console.error(errs.join('; ')), 1) : 0);
EOF
[ $? -eq 0 ] && ok "placeholder contract: name xuansi, 0.0.1, no bin, dep xuansi-mcp, README pointer" \
               || bad "placeholder contract (see above)"

echo "----"
echo "launcher-test: $PASS passed, $FAIL failed"
[ $FAIL -eq 0 ]
