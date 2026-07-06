# Publishing plan (clients + server crates)

How to publish the five clients and the server-side crates (ganglion, keratin) to
their registries: names, what each package still needs, and the per-package
process. License is MIT across the board (the AI-training reservation in
`AI_POLICY.md` and the `[workspace.metadata.sovereignty]` Cargo metadata is a
policy document, not a license change, so registries see standard MIT).

## Name availability (checked 2026-07-06)

| Package | Registry | Name | Status |
| --- | --- | --- | --- |
| TypeScript client | npm | `@fibril/client` (scoped) | FREE. Unscoped `fibril` is a squatted placeholder, so stay scoped. |
| Python client | PyPI | `fibril` | FREE |
| C# client | NuGet | `Fibril` (and/or `Fibril.Client`) | both FREE |
| Rust client | crates.io | `fibril-client` | FREE |
| Go client | Go modules | `github.com/Axmouth/fibril/clients/go` | n/a (module path = repo path) |
| Coordination | crates.io | `ganglion` (+ `ganglion-core/-openraft/-coordination/-storage`) | all FREE. GRAB SOON. |
| Storage | crates.io | `keratin-log`, `stroma-core`, `stroma-common` | all FREE |
| Broker | crates.io | `fibril-broker` (or `fibril-server`) | FREE. Bare `fibril` is taken. |

Taken names, squatted vs used:

- **npm `fibril`** — SQUATTED. A v0.0.1 "placeholder for a top secret open source
  project" by an unrelated author. Disputable via npm's parked-package policy, but
  not worth it: `@fibril/client` (scoped) is free and is already the package name.
- **crates.io `fibril`** — USED. A real (dormant) "communicating fibers" library,
  ~11k downloads, last released 2023. Cannot be reclaimed. Publish the broker as
  `fibril-broker`; the whole workspace already uses free `fibril-*` names.
- **crates.io `keratin`** — USED. A real "embedded modular database", ~21k
  downloads, updated 2024. Cannot be reclaimed. There is no `keratin` crate to
  publish anyway: the storage workspace's crates are `keratin-log` and `stroma-*`,
  all free.

**Secure the free names soon (first come first served):** crates.io `ganglion`
(+ its four member crates), `fibril-client`, `fibril-protocol`, `fibril-util`,
`fibril-config`, `fibril-broker`, `fibril-storage`, `keratin-log`, `stroma-core`,
`stroma-common`; PyPI `fibril`; NuGet `Fibril`; and the `@fibril` npm org/scope.
Reserving a crates.io name means publishing a real 0.x (there is no reserve-only),
so a minimal first release doubles as the reservation.

## What each package still needs

- **TypeScript (`@fibril/client`)** — `package.json` has name/version/exports/files
  but is missing `repository`, `license`, `author`, `keywords`, and
  `publishConfig: { "access": "public" }` (scoped packages default to restricted).
  Claim the `@fibril` npm org for the scope.
- **Python (`fibril`)** — `pyproject.toml` is close: name, version, MIT license,
  authors, classifiers. Add `[project.urls]` (Homepage/Repository/Documentation)
  and confirm the `readme` field points at a package README for the PyPI page.
- **C# (`Fibril`)** — no packaging metadata at all. Add to `Fibril.csproj` (or a
  `Directory.Build.props`): `PackageId`, `Version`, `Authors`, `Company`,
  `Description`, `PackageLicenseExpression=MIT`, `RepositoryUrl`, `PackageTags`,
  `PackageReadmeFile`, and `IsPackable=true`. The library already targets `net8.0`
  (broad reach).
- **Rust client (`fibril-client`)** — `Cargo.toml` has only `name` +
  `version.workspace`. crates.io requires at least `description` and `license`; add
  `description`, `license = "MIT"`, `repository`, `readme`, `keywords`,
  `categories`. See the coupling caveat below.
- **Go** — nothing to package; needs a version tag (see process).

## Publish process per package

### TypeScript -> npm
1. `cd clients/typescript && npm run build` (produces `dist/`; `files` already
   limits the tarball to `dist`).
2. `npm publish --access public` (scoped packages need `--access public` the first
   time), or via GitHub Actions with npm Trusted Publishing (OIDC, no long-lived
   token). Enable 2FA on the account/org.
3. Verify: `npm view @fibril/client`.

### Python -> PyPI
1. `cd clients/python && uv build` (or `python -m build`) -> `dist/*.whl` + sdist.
2. `uv publish` (or `twine upload dist/*`). Prefer PyPI Trusted Publishing (OIDC
   from GitHub Actions) so no API token is stored.
3. Verify the project page renders the README and metadata.

### C# -> NuGet
1. Add the packaging metadata above, then `dotnet pack -c Release
   clients/csharp/Fibril/Fibril.csproj` -> `bin/Release/Fibril.<version>.nupkg`.
2. `dotnet nuget push Fibril.<version>.nupkg -s https://api.nuget.org/v3/index.json
   -k <API_KEY>`, or NuGet Trusted Publishing via GitHub Actions.
3. Verify on nuget.org.

### Rust client -> crates.io
1. Fill in the Cargo metadata (description/license/repository/readme).
2. crates.io forbids `path`/`git` dependencies: every dependency must be a
   published crate. Publish the dependency crates bottom-up first (see coupling
   caveat), then `cargo publish -p fibril-client`.
3. `cargo publish --dry-run -p fibril-client` first to catch missing metadata and
   unpublished deps.

### Go -> Go modules (no registry upload)
Go resolves modules from the repo, so there is no publish step, only a tag. The Go
client is a submodule (`clients/go`), so the tag must be path-prefixed:
`git tag clients/go/v0.5.0 && git push origin clients/go/v0.5.0`. Then
`go get github.com/Axmouth/fibril/clients/go@v0.5.0` works and it appears on
pkg.go.dev. Requires the repo to be public.

### ganglion + keratin -> crates.io
Both are multi-crate workspaces, so publish members bottom-up (a crate's deps must
already be on crates.io):
- ganglion: `ganglion-core` and `ganglion-storage` -> `ganglion-openraft` ->
  `ganglion-coordination` -> `ganglion` (adjust to the real dep order).
- keratin/stroma: `stroma-common` -> `stroma-core` -> `keratin-log`.
Each needs `description` + `license = "MIT"` in its `Cargo.toml`. Grab `ganglion`
soon while it is free.

## Cross-cutting

- **Coupling caveat (Rust client).** Unlike the TS/Python/Go/C# clients, which are
  standalone, `fibril-client`'s runtime `[dependencies]` include `fibril-broker`
  and `fibril-storage` (server-side crates); `fibril` and `fibril-config` are only
  `[dev-dependencies]` (test-only, stripped on publish). So publishing the Rust
  client to crates.io drags in most of the server workspace. Two options: publish
  the whole `fibril-*` workspace together, or (cleaner, longer term) decouple the
  client so it depends only on `fibril-protocol` (+ a small shared-types crate),
  not `fibril-broker`/`fibril-storage`.
- **Versioning.** Everything currently reads 0.4.0; the workspace version is 0.4.0.
  First registry releases should be the current cycle's version (0.5.0). Keep the
  client versions and the broker/workspace version aligned per
  `scripts/release.sh` / `release-all.sh`.
- **License / AI policy.** Publish as MIT (the SPDX registries expect). Link
  `AI_POLICY.md` from each package README so the text-and-data-mining reservation
  travels with the package; there is no standard registry metadata field for it.
- **Trusted publishing (OIDC).** npm, PyPI, NuGet, and crates.io all support
  publishing from GitHub Actions via OIDC now. Prefer it over stored tokens for
  every package.
