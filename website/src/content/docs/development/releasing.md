---
title: Versioning and releasing
description: How the Fibril repo group is versioned, how a release is cut, and how versioned artifacts are produced.
---

This is a development note: the versioning model and the release process.

## Version groups

Fibril spans more than one repository, and each repository carries its own
version line:

- **Fibril** (this repo: broker, clients, CLI, admin) - one version shared across
  all its crates via `[workspace.package]`, so they always move in lockstep.
- **Stroma** - the durable queue/stream substrate. It tends to move closely with
  Fibril (most changes touch both), so its version usually tracks Fibril's even
  though it is its own line. It currently lives inside the Keratin repo and will
  split into its own repo.
- **Keratin** - the append-only log under Stroma. Its own version and cadence.
- **Ganglion** - the coordination/raft layer. Its own version and cadence.

SemVer applies to each. Pre-1.0, a minor version may still change the API and
wire protocol. 1.0 is the commitment to stability (see the roadmap's 1.0 gates).
Versions advance one minor at a time, no vanity jumps.

## Cutting a release

The Fibril repo has `scripts/release.sh`. Keratin and Ganglion do not have one
yet. From the Fibril repo:

```sh
scripts/release.sh --check     # verify it is release-ready (changelog + build)
scripts/release.sh 0.5.0       # bump everywhere, snapshot docs, commit, tag v0.5.0
```

`release.sh` does, in order:

1. **Changelog gate.** It refuses unless `CHANGELOG.md` has a dated
   `## [<version>]` section - the release must be described before it is cut.
2. **Bump.** It sets the single repo version everywhere (the workspace version
   and the TypeScript/Python client manifests and their default client version).
3. **Docs snapshot.** The current docs live unversioned at the site root. The
   release registers the new minor with the `starlight-versions` plugin and runs
   a site build, which freezes the whole documentation set under `docs/<minor>`
   (for example `docs/0.2`) and rewrites its intra-doc links to that versioned
   path. An adopter on a release reads the docs as they shipped, with a version
   picker to switch, while the root keeps moving.
4. **Build gate.** `cargo check --workspace` must pass at the new version.
5. **Tag.** It commits the bump and creates an annotated `v<version>` tag whose
   body is the changelog section. It does not push - pushing the tag is the
   deliberate publish step.

Pushing the tag triggers `.github/workflows/release.yaml`, which builds the
server image and pushes it tagged `:<version>`, `:<minor>`, and `:latest` to
GHCR, then cuts a GitHub release from the changelog section. (CI already pushes
`:main` and `:sha-<short>` on every merge. The release adds the version tags.)

## Coordinated releases (the overlord)

While the repos evolve together, `scripts/release-all.sh <version>` cuts a synced
release across the whole group: it runs each repo's own `release.sh` in
dependency order (Keratin/Stroma, then Ganglion, then Fibril). A repo without a
release script yet, which is Keratin and Ganglion today, is reported and skipped. This overlord is a convenience for
the lockstep phase. As the repos diverge they release independently and it is
only used for the occasional coordinated cut.

## The cadence (gate, build, gate, release)

The release rhythm mirrors the roadmap's gate structure: open a version, land the
work for it (each item closing a slice of a gate), then a release gate - update
the changelog, run `release.sh --check`, refresh the docs snapshot - before
tagging. The same shape repeats for each minor on the way to 1.0.

## Publishing packages

No package is on a registry yet. Everything is MIT. The AI policy is a separate
document, not a license change, so link `AI_POLICY.md` from each package README.
Prefer trusted publishing from GitHub Actions (OIDC) over stored tokens, which
npm, PyPI, NuGet and crates.io all support.

Names, as checked on 2026-10-29:

| Package | Registry | Name | State |
| --- | --- | --- | --- |
| Coordination | crates.io | `ganglion`, `ganglion-core`, `ganglion-openraft`, `ganglion-coordination`, `ganglion-storage` | Claimed, 0.0.1 placeholders |
| Storage | crates.io | `keratin-log`, `stroma` | Claimed, 0.0.1 placeholders. Their repository field points at a `stroma` repo that does not exist, so fix it on the first real release. |
| Storage | crates.io | `stroma-core`, `stroma-common` | Free |
| Broker and Rust client | crates.io | `fibril-client`, `fibril-wire`, `fibril-util`, `fibril-broker` and the other `fibril-*` workspace names | Free |
| TypeScript client | npm | `@fibril/client` | Free. The `@fibril` org could not be checked from the registry API, so confirm by creating it. |
| Python client | PyPI | `fibril` | Free |
| C# client | NuGet | `Fibril`, `Fibril.Client` | Free |
| Go client | Go modules | `github.com/Axmouth/fibril/clients/go` | Module path is the repo path, and the repo is public |

Taken by unrelated projects: crates.io `fibril` (a fibers library) and `keratin`
(an embedded database), and npm `fibril` (a parked placeholder). None of them is
needed, since the workspaces already use `fibril-*`, `keratin-log` and scoped npm
names.

crates.io has no name reservation, so a minimal first release is the reservation.

What each package still needs:

- **TypeScript:** `repository`, `license`, `author`, `keywords` and
  `publishConfig: { "access": "public" }` in `package.json`, since scoped
  packages default to restricted.
- **Python:** `[project.urls]` and a `readme` pointing at the package README.
- **C#:** packaging metadata in `Fibril.csproj` or a `Directory.Build.props`
  (`PackageId`, `Version`, `Authors`, `Description`,
  `PackageLicenseExpression=MIT`, `RepositoryUrl`, `PackageTags`,
  `PackageReadmeFile`).
- **Rust crates:** the client chain and the Keratin crates carry their metadata.
  In-repo dependencies name a version beside their path or git source, through
  `[workspace.dependencies]` in Fibril, which `scripts/release.sh` bumps.
  crates.io resolves every dependency from the registry, so publish bottom-up:
  1. `stroma-common` and `keratin-log` (Keratin repo)
  2. `stroma-core`
  3. `fibril-common` and `fibril-util`
  4. `fibril-wire`
  5. `fibril-client`

  `cargo publish --dry-run -p <crate>` passes for the crates in steps 1 and 3.
  The later steps can only run their dry-run once the earlier ones are on
  crates.io. `fibril-util` keeps log setup and certificate parsing behind its
  `server` feature, so the client does not pull them in.
- **Ganglion and the broker crates:** blocked. `ganglion-openraft` depends on a
  patched copy of openraft 0.9.25 (see `vendor/openraft/PATCHES.md` in Ganglion),
  and no released openraft 0.9 carries the fix. Publishing needs either an
  upstream release with it or the patched copy published under its own name.
  The broker crates depend on Ganglion, so they wait too. Ganglion also has no
  LICENSE file yet.
- **Go and C#:** a README each.

Per registry:

- **npm:** `npm run build` in `clients/typescript`, then
  `npm publish --access public`.
- **PyPI:** `uv build` then `uv publish` in `clients/python`.
- **NuGet:** `dotnet pack -c Release clients/csharp/Fibril/Fibril.csproj`, then
  `dotnet nuget push` the `.nupkg`.
- **Go:** no upload. The client is a submodule, so tag with its path, for example
  `git tag clients/go/v0.5.0`, and push the tag. The repo must be public for
  pkg.go.dev.

First registry releases use the current cycle's version, with client and
workspace versions aligned through `scripts/release.sh`.
