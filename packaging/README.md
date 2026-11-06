# packaging/ — npm publish family

How the xuansi npm package family is assembled and published.

## Layout

    packaging/npm/xuansi/        main package source (version 0.0.0 placeholder
                                 in-repo; assemble stamps the real version)
      bin/xuansi.js              launcher — pure Node stdlib, zero npm deps;
                                 platform map (single source) lives here
      bin/xuansi-mcp.js          alias: prepends "mcp" to argv, reuses the launcher
      test/launcher-test.sh      bash smoke test (map, stdio purity, exit/signal
                                 forwarding, install-hint error path)
    packaging/scripts/assemble-npm.sh   builds the publish tree (below)
    packaging/dist/xuansi-<ver>/        ASSEMBLY OUTPUT — never committed
      xuansi/                    main package (version stamped, optionalDeps pinned)
      linux-x64/ ... darwin-arm64/      six platform subpackages, each standalone-publishable

## Rules

- **Version is parameterized**: `assemble-npm.sh <version>` stamps it into
  the main package and all six subpackages, and **pins** the main
  package's `optionalDependencies` to that exact version (in-repo they
  are `*`). npm versions are bare semver — strip the tag's leading `v`.
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
  first, the main `xuansi` package last — the main package's pinned
  optionalDependencies must be resolvable at install time.
- `packaging/dist/` is gitignored; `results/` holds acceptance
  transcripts (ASCII filenames).
