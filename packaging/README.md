# packaging/ — npm publish family

How the xuansi npm package family is assembled and published.

## Layout

    packaging/npm/xuansi/        main package source (version 0.0.0 placeholder
                                 in-repo; assemble stamps the real version)
      bin/xuansi.js              launcher — pure Node stdlib, zero npm deps;
                                 platform map (single source) lives here
      bin/xuansi-mcp.js          alias: prepends "mcp" to argv, reuses the launcher
      test/launcher-test.sh      bash smoke test (map, stdio purity, exit/signal
                                 forwarding, install-hint error path, alias pkg)
    packaging/npm/xuansi-mcp/    alias package source: `npx -y xuansi-mcp`
                                 (npx resolves package names) — depends on
                                 xuansi ("*" in-repo; assemble pins it)
    packaging/scripts/assemble-npm.sh   builds the publish tree (below)
    packaging/dist/xuansi-<ver>/        ASSEMBLY OUTPUT — never committed
      xuansi/                    main package (version stamped, optionalDeps pinned)
      linux-x64/ ... darwin-arm64/      six platform subpackages, each standalone-publishable
      xuansi-mcp/                alias package (version stamped, xuansi dep pinned)

## Rules

- **Version is parameterized**: `assemble-npm.sh <version>` stamps it into
  the main package, the alias package and all six subpackages, and **pins**
  the main package's `optionalDependencies` and the alias package's
  `xuansi` dependency to that exact version (in-repo they are `*`).
  npm versions are bare semver — strip the tag's leading `v`.
- **Single source for platforms**: the `platformPackage` map in
  `bin/xuansi.js` is canonical. `assemble-npm.sh` reads its keys; a map
  key without a `bin/` artifact mapping trips the script (and vice versa
  via the smoke test's six-key check).
- **darwin/ slot convention**: `make build` (Linux) produces only the
  four linux/win32 binaries. darwin binaries are cgo (build on macOS
  only). CI's macos job builds them and drops them at
  `bin/xuansi-darwin-amd64` and `bin/xuansi-darwin-arm64`; the publish
  job runs `assemble-npm.sh` **after** those files are in place.
  Locally they are absent → the two darwin subpackages are skipped with
  a printed note (never an error).
- **Publish order** (CI): the six `@xuansi/<platform>` subpackages
  first, then the main `xuansi` package, then the alias `xuansi-mcp`
  last — every package's pinned dependencies must be resolvable on the
  registry at the moment it becomes installable.
- `packaging/dist/` is gitignored; `results/` holds acceptance
  transcripts (ASCII filenames).
