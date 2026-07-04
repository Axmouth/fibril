---
title: Setting up a cluster
description: Bring up a secured multi-broker Fibril cluster, with or without manual steps.
slug: 0.3/deployment/cluster
---

A Fibril cluster is a set of brokers running in `ganglion` mode that form one
coordination group, replicate ownership and metadata, and route clients to the
current owner of each partition. This guide covers the security shape an
operator has to put in place for it to work: the cluster secret, TLS across
nodes, and user accounts.

Clustering is experimental and not yet production-ready high availability.

## What every node needs

* **The same cluster secret.** Nodes authenticate to each other with a shared
  secret, never with a user account. Ganglion mode refuses to start without one.
* **The coordination peers.** Each node knows the raft address of every member,
  including itself, and exactly one node bootstraps the group on first boot.
* **TLS, if connections leave the host.** Passwords and replicated data travel
  the network. Enable the `tls` section (see [configuration](/0.3/configuration/)).
* **Users.** The built-in `fibril`/`fibril` pair works from loopback only, so
  create at least one real user for clients that connect across the network.

## The cluster secret

Generate one and give every node the same value:

```sh
fibrilctl secret generate --show
```

That writes `<data_dir>/cluster.secret` (mode `0600`) and prints the value.
Distribute it however your platform distributes secrets:

* copy the file to each node's data dir, or
* set `FIBRIL_CLUSTER_SECRET` to the value, or
* point `coordination.secret_path` at a mounted secret file.

Resolution order is `FIBRIL_CLUSTER_SECRET`, then `coordination.secret_path`,
then the `<data_dir>/cluster.secret` file. User accounts never authenticate
node-to-node connections, so rotating a user cannot wedge replication.

## Entry-level: first-boot setup

For a hands-on bring-up, boot the first node with `setup.mode = true` and no
data yet. It serves a setup page on `127.0.0.1:<admin port>` instead of
starting the broker. Choose the TLS path, optionally create an admin user, and
choose **Generate one (first node)** for the cluster secret. The broker starts
once you apply. Copy the printed secret to the other nodes, boot each of them in
setup mode too, and choose **Paste the secret from an existing node**.

## Unattended: config and environment only

For automation, autoscaling, or immutable infrastructure, provide everything up
front so no node ever waits on a person. A compose service for one broker:

```yaml
environment:
  FIBRIL_COORDINATION_MODE: ganglion
  FIBRIL_CLUSTER_SECRET: ${FIBRIL_CLUSTER_SECRET}   # same value on every node
  FIBRIL_TLS_ENABLED: "true"
  FIBRIL_TLS_AUTO_SELF_SIGNED: "true"
  FIBRIL_AUTH_USERNAME: ops
  FIBRIL_AUTH_PASSWORD: ${FIBRIL_OPS_PASSWORD}
```

With TLS and the secret provided explicitly, setup mode (if set) marks itself
complete and boots straight through, so the same image works interactively and
unattended. `compose.cluster.example.yaml` in the repository is a runnable
three-broker example (it uses a clearly labelled demo-only secret, which a real
deployment replaces).

## Verify

From any node:

```sh
fibrilctl admin topology
```

It shows the members, per-partition ownership, and consensus state. A healthy
cluster lists every node and a leader. Clients can connect to any broker and are
redirected to the owner of each partition.

## Adding and removing users

Users are cluster data. Create one on any node and it replicates to all:

```sh
fibrilctl user add ops --user-password "$FIBRIL_OPS_PASSWORD"
fibrilctl user list
fibrilctl user remove ops
```

The same operations are on the dashboard settings page.
