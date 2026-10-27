---
title: Configuration sources
description: Design for choosing whether the cluster, a seeding file or a managed file owns runtime settings, users and access policy.
---

This is a proposed design. Nothing here is implemented yet. It builds on the
[configuration policy](/development/config-policy/) and
[configuration design](/development/config-design/), and replaces the
`[runtime_locks]` mechanism described there. The access policy it mentions is
planned work with its own design.

## Problem

Fibril keeps cluster-wide configuration in the cluster. Runtime settings and
users replicate through coordination, every node applies the same values, and
the admin dashboard edits them live. Operators never keep files in sync across
nodes just to keep the cluster consistent.

Some deployments want the opposite. Configuration management, GitOps or
reviewed pull requests own the configuration, and changes made in a dashboard
are unwanted. Today the only tool for that is `[runtime_locks]`, which covers
one settings section and is evaluated per node. A node with a lock keeps its own
file values and rejects the replicated ones, so two nodes can run with different
settings and behavior changes silently on failover.

The goal is one explicit choice of who owns each cluster-wide document, with
the same guarantee in every mode: all nodes run the same configuration, and any
disagreement is visible.

## Current foundation

- Runtime settings are one document. In cluster mode the cluster attribute
  `fibril/runtime_settings` holds it with a `cluster_version`, and each node
  copies it into its local global store. Admin updates are compare-and-swap on
  that version.
- Startup config can seed runtime settings (`[runtime_seed.*]`) and users
  (`auth.seed_users`). Seeds apply only when no document exists yet. After
  that the persisted document owns the values.
- `[runtime_locks]` pins a section to the node's startup values on every boot,
  and the admin API rejects edits to it with HTTP 423 `setting_locked`.
- Users are a separate document from settings, so password hashes never travel
  with the settings JSON.
- Nodes publish advisory heartbeat labels to coordination, and helpers
  aggregate them across live nodes (topology adoption, cohort membership).
  Configuration reports can use the same channel.

## Scope

Configuration sources apply to cluster-wide documents:

- runtime settings
- users
- the access policy, once it exists

Startup fields stay node-local and outside this design: addresses, the data
directory, storage tuning such as `storage.keratin.*`, TLS material and the
cluster secret. Nodes may legitimately differ in those, for example different
drives or advertise addresses.

## Sources

Each document has a source.

| Source | First boot | Later file changes | Dashboard and API edits |
| --- | --- | --- | --- |
| `cluster` | Built-in defaults. File values are ignored | Ignored | Allowed |
| `seed` (default) | File values seed the document | Ignored, with a visible notice | Allowed |
| `file` | File values become the document | Adopted once every live node agrees | Rejected, read-only view |

`seed` is the current behavior and stays the default, so existing setups keep
working. `cluster` suits deployments that manage everything from the
dashboard and want file values to have no effect at all. `file` suits managed
deployments where the file is the only place changes are made.

## Configuration

One source applies to everything, and each document can override it. A
document has one source as a whole, so a file never owns half of the runtime
settings.

```toml
[config]
# cluster | seed | file
source = "seed"

[runtime]
# The runtime settings document is owned by the file.
source = "file"

[runtime.delivery]
inflight_ttl_ms = 30000

[runtime.idle_queue_cleanup]
enabled = true
evict_after_ms = 600000

[auth]
source = "file"

[[auth.users]]
name = "ops"
password_hash = "$argon2id$v=19$m=19456,t=2,p=1$..."
```

- `[runtime.*]` replaces `[runtime_seed.*]`, since the section is no longer
  only a seed. `runtime_seed` stays accepted as an alias for one release with a
  startup warning.
- `[runtime_locks]` is removed. Its one lockable section is covered by
  `source = "file"` on the runtime settings document. A config that still
  sets it fails to start with an error pointing here, since silently changing
  who owns the other sections would be worse.
- `auth.seed_users` keeps working in `seed` mode. `[[auth.users]]` is the
  general form.
- In `file` mode users carry `password_hash` in PHC format. Plaintext passwords
  are rejected there, because they would sit in managed files and every node
  would hash them with a different salt, so their fingerprints would never
  match. A new `fibrilctl user hash` command prints a hash to paste in.
- Environment variables and CLI flags still overlay the file. The fingerprint
  covers the merged result, so a node with an extra environment override shows
  up as a mismatch.

## Fingerprints

Each node computes a fingerprint per document: BLAKE3 over a canonical encoding of the merged values (sorted keys,
defaults filled in, comments and formatting dropped). Reordering a TOML file or
adding a comment does not change the fingerprint. Changing a value or the
source does.

Nodes report their fingerprints and sources in a heartbeat label. Reports are
advisory, like the other labels, and a missing or malformed report counts as
unknown, never as agreement.

## Seed mode

The document records where its values came from: the source, the fingerprint
of the file that seeded it and when.

On boot and on every later report, a node compares its own fingerprint with the
recorded one. When they differ, the node logs once and the settings page shows:

> Config file changed since it seeded the cluster. Using the cluster
> configuration.

The notice names the nodes whose files differ. It clears when the files match
again or when an operator reseeds. Reseeding is an explicit, version-checked
action (`fibrilctl config reseed` or a dashboard button) that writes the
current file values into the document. It is refused while the live nodes
disagree on the file, so the result never depends on which node handles the
request.

## File mode

### Adoption

The cluster document always holds one adopted version. A new file version is
adopted when every live member reports the same fingerprint for it. The
controller then writes that content as the new document, with
`origin = "file"`, the fingerprint and the adoption time. Nodes apply it through
the existing settings sync.

Until then the new version is pending. The cluster keeps running on the
adopted version, the settings page shows "pending, 2 of 3 nodes", and a rolling
change completes when the last node restarts with the new file. Every node runs
the adopted version the whole time, including nodes whose file already holds
the next one. No node ever runs its own file values.

A pending version that stays partial for longer than a timeout becomes an
attention item naming the nodes still on other fingerprints. The proposed
timeout is 10 minutes, as a runtime setting.

On first boot with no document, the first node to boot writes its file values,
as seeding does today. Nodes that join later compare against that.

### Mismatch is a blocker for change

A mismatch always blocks adopting new file content. It never stops traffic by
default, because a rolling change is a mismatch by design.

For deployments that want a hard stop, `config.strict = true` makes a booting
node refuse to start when its fingerprint differs from the adopted version and
no other live node reports the same new fingerprint. That catches a node
restored from an old image, while still letting the first node of a rolling
change start. The node logs both fingerprints and the differing sections.

### Edits and visibility

The admin API rejects edits to file-owned documents with HTTP 423
`setting_locked`, as locks do today, and the error names the source. The
dashboard shows the values read-only with the adopted fingerprint, adoption
time and per-node agreement.

### Changing the source

The source is part of the report. Switching a document from `file` to `seed`
or `cluster` is a file change like any other: it takes effect once every live
node reports the new source. Until then the document stays file-owned. Mixed
sources across nodes are a mismatch.

## Standalone brokers

A standalone broker has no other nodes to agree with. `file` applies the file
on every boot, `seed` seeds once and reports later changes, and `cluster` uses
the persisted values. The same notices appear on the settings page.

## Admin and CLI surface

- `GET /admin/api/config/sources` returns, per document: the
  source, the adopted fingerprint and time, each node's reported source and
  fingerprint, and the state (`in_sync`, `pending`, `changed_since_seed`,
  `mismatch`, `unknown`).
- `fibrilctl config status` prints the same as a table.
- `fibrilctl config reseed --expected-version <n>` reseeds in `seed` mode.
- The settings page shows a source badge on each document and the notices above.

## Attention items

- A file changed since seeding (seed mode).
- A pending file version past its timeout (file mode).
- Nodes reporting different sources for the same document.
- A node whose report is missing or malformed for longer than the timeout.

These feed the dashboard now and the planned MCP diagnostics later.

## Access policy

The planned access policy is one more document with the same three sources and
the same file schema in TOML, the dashboard and the API. A policy copied from
the dashboard into a file, or back, needs no conversion.

## Implementation order

1. Canonical encoding and fingerprints for the runtime settings and user documents,
   with unit tests for formatting-independent fingerprints.
2. Origin metadata on the cluster settings and user documents, and the seed
   mode notice. This alone covers the most common surprise.
3. Heartbeat reports and the sources status API and CLI.
4. File mode adoption by the controller, the pending state and its timeout.
5. `[runtime.*]`, `[auth] source` and `[[auth.users]]`, with the
   `runtime_seed` alias and the `runtime_locks` startup error.
6. Strict start, dashboard badges and read-only views.

## Test plan

- Fingerprints ignore key order, comments and formatting, and change with any
  value or source change.
- Seed mode: a changed file on one node raises the notice naming that node and
  leaves the document unchanged. Reseed is refused while nodes disagree.
- File mode in a three-node cluster: a rolling change stays pending through two
  restarts and is adopted on the third. Every node reports the adopted version
  throughout.
- A node killed during a pending change, then restarted with the old file,
  leaves the change pending and raises the timeout item.
- Strict start refuses a node with a stale file and admits the first node of a
  rolling change.
- Admin edits to a file-owned document fail with 423, and edits to a seeded
  document succeed.
- An environment override on one node shows as a mismatch.
- Standalone brokers apply each source as described.

## Open questions

- Whether nodes that are members but down should count for adoption after a
  grace period, so a permanently lost node does not hold a change pending
  until it is removed from the cluster.
- The default for `config.strict`. Off keeps rolling changes forgiving. On is
  safer for managed deployments that never restore old images.
- Whether reseeding should also be available in `cluster` mode as a one-off
  import from a file.
