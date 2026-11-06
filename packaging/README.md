# packaging/ — npm publish family

How the xuansi npm package family is assembled and published.

**Family restructure 2026-11-06 (owner ruling: 7 live + 1 squat-guard)**:
the MAIN package is `xuansi-mcp` (dual bin `xuansi` + `xuansi-mcp`); the
bare `xuansi` name on npm is a defensive placeholder that provides no
commands and only points users at `xuansi-mcp`.

## Layout

    packaging/npm/xuansi-mcp/        MAIN package source (version 0.0.0
                                     placeholder in-repo; assemble stamps
                                     the real version)
      bin/xuansi.js                  launcher — pure Node stdlib, zero npm
                                     deps; platform map (single source)
                                     lives here; exports run()
      bin/xuansi-mcp.js              dual bin: prepends "mcp" to argv via
                                     the SIBLING launcher (in-package
                                     require — no dependency hop)
      test/launcher-test.sh          bash smoke test (map, stdio purity,
                                     exit/signal forwarding, install-hint
                                     error path, dual bin, placeholder)
    packaging/npm/xuansi-placeholder/  squat-guard `xuansi` (version
                                     0.0.1, NO bin, files: README.md,
                                     dep: xuansi-mcp) — one-time MANUAL
                                     publish at bootstrap; NEVER assembled
                                     or released by CI
    packaging/scripts/assemble-npm.sh   builds the publish tree (below)
    packaging/dist/xuansi-<ver>/        ASSEMBLY OUTPUT — never committed
      xuansi-mcp/                    main package (version stamped, dual
                                     bin, optionalDeps pinned)
      linux-x64/ ... darwin-arm64/   six platform subpackages, each
                                     standalone-publishable

## The 7 live packages (+1 placeholder)

| # | package                | role                                                       |
|---|------------------------|------------------------------------------------------------|
| 1 | `@xuansi/linux-x64`    | platform binary subpackage (os+cpu selected)               |
| 2 | `@xuansi/linux-arm64`  | platform binary subpackage                                 |
| 3 | `@xuansi/win32-x64`    | platform binary subpackage                                 |
| 4 | `@xuansi/win32-arm64`  | platform binary subpackage                                 |
| 5 | `@xuansi/darwin-x64`   | platform binary subpackage                                 |
| 6 | `@xuansi/darwin-arm64` | platform binary subpackage                                 |
| 7 | `xuansi-mcp`           | MAIN launcher package; bins `xuansi` + `xuansi-mcp`        |
| — | `xuansi` (placeholder) | squat-guard, NO bin, dep `xuansi-mcp`; manual one-time     |
|   |                        | publish at bootstrap; version stays 0.0.1; not in CI       |

## Rules

- **Version is parameterized**: `assemble-npm.sh <version>` stamps it into
  the main package and all six subpackages, and **pins** the main
  package's `optionalDependencies` to that exact version (in-repo they
  are `*`). The placeholder template is untouched by assemble (version
  stays `0.0.1` forever) and never lands in `dist/` — the script guards
  against exactly that leak. npm versions are bare semver — strip the
  tag's leading `v`.
- **Single source for platforms**: the `platformPackage` map in
  `packaging/npm/xuansi-mcp/bin/xuansi.js` is canonical.
  `assemble-npm.sh` reads its keys; a map key without a `bin/` artifact
  mapping trips the script (and vice versa via the smoke test's six-key
  check).
- **darwin/ slot convention**: `make build` (Linux) produces only the
  four linux/win32 binaries. darwin binaries are cgo (build on macOS
  only). CI's macos job builds them and drops them at
  `bin/xuansi-darwin-amd64` and `bin/xuansi-darwin-arm64`; the publish
  job runs `assemble-npm.sh` **after** those files are in place.
  Locally they are absent → the two darwin subpackages are skipped with
  a printed note (never an error).
- **Publish order** (CI): the six `@xuansi/<platform>` subpackages
  first, then the main `xuansi-mcp` last — **7 publishes**; the main
  package's pinned optionalDependencies must be resolvable on the
  registry at the moment it becomes installable.
- `packaging/dist/` is gitignored; `results/` holds acceptance
  transcripts (ASCII filenames).

## Bootstrap flow (one-time, manual)

1. Create the 7 trusted-publisher entries below on npmjs.com (≥2 days
   before the first release tag — validation window).
2. `npm publish` the placeholder `packaging/npm/xuansi-placeholder/`
   ONCE, by hand (owner login; no CI, no trusted publisher — it never
   changes again, version pinned at 0.0.1).
3. Push the first `v*` tag → release.yml builds, assembles and
   publishes the 7 live packages.

## Registry access — npm Trusted Publishers (OIDC)

Publishing authenticates via OIDC trusted publishing — there is **no
`NPM_TOKEN` secret** (removed, owner-directed). The `publish` job in
`.github/workflows/release.yml` carries `permissions: id-token: write`
(job-level; workflow-level stays `contents: read`); Node 24 ships
npm ≥ 11.5.1, which auto-detects the Actions OIDC identity — no
`NODE_AUTH_TOKEN` env on any publish step.

One-time setup on npmjs.com — a trusted publisher entry per package,
**7 entries** (`xuansi-mcp` and the six `@xuansi/<platform>`
subpackages). Every entry gets the same fields:

| Field        | Value               |
|--------------|---------------------|
| Repository   | `wan-cloud/xuansi-mcp` |
| Workflow     | `release.yml`       |
| Environment  | *(none — leave empty; the job declares no environment)* |
| Command      | `npm publish` (allowed) |

Notes:

- The placeholder `xuansi` deliberately has **no** trusted-publisher
  entry: it is never published by CI, only the one manual bootstrap
  publish above.
- **`repository.url` is load-bearing**: every published package.json
  carries `"repository": {"type":"git","url":"git+https://github.com/wan-cloud/xuansi-mcp.git"}`
  — it must EXACTLY match the GitHub repo the trusted publisher names.
  The hand-maintained template (`packaging/npm/xuansi-mcp/`,
  `packaging/npm/xuansi-placeholder/`) and the subpackage generator in
  `packaging/scripts/assemble-npm.sh` (single source for the six
  generated package.jsons) all stamp the same value.
- **First-publish validation window**: a trusted publisher created for a
  package name that does not exist on the registry yet enters a ~2-day
  validation window before it can be used — create all 7 entries at
  least two days before pushing the first release tag, or the first
  publish 403s.
- **Private repo ⇒ no provenance**: npm provenance attestations require
  a public repository. If `wan-cloud/xuansi-mcp` is (or goes) private,
  OIDC publishes still work but no provenance statement is generated.
