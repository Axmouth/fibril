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
wire protocol; 1.0 is the commitment to stability (see the roadmap's 1.0 gates).
Versions advance one minor at a time, no vanity jumps.

## Cutting a release

Each repo has its own `scripts/release.sh`. From a repo:

```sh
scripts/release.sh --check     # verify it is release-ready (changelog + build)
scripts/release.sh 0.3.0       # bump everywhere, snapshot docs, commit, tag v0.3.0
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
`:main` and `:sha-<short>` on every merge; the release adds the version tags.)

## Coordinated releases (the overlord)

While the repos evolve together, `scripts/release-all.sh <version>` cuts a synced
release across the whole group: it runs each repo's own `release.sh` in
dependency order (Keratin/Stroma, then Ganglion, then Fibril). A repo without a
release script yet is reported and skipped. This overlord is a convenience for
the lockstep phase; as the repos diverge they release independently and it is
only used for the occasional coordinated cut.

## The cadence (gate, build, gate, release)

The release rhythm mirrors the roadmap's gate structure: open a version, land the
work for it (each item closing a slice of a gate), then a release gate - update
the changelog, run `release.sh --check`, refresh the docs snapshot - before
tagging. The same shape repeats for each minor on the way to 1.0.
