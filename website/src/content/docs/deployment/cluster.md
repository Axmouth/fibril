---
title: Setting up a cluster
description: Bring up a secured multi-broker Fibril cluster, with or without manual steps.
---

A Fibril cluster is a set of brokers running in `ganglion` mode that form one
coordination group, replicate ownership and metadata, and route clients to the
current owner of each partition. This guide covers the security shape an
operator has to put in place for it to work: the cluster secret, TLS across
nodes, and user accounts.

Clustering is experimental and not yet production-ready high availability.

## What every node needs

- **The same cluster secret.** Nodes authenticate to each other with a shared
  secret, never with a user account. Ganglion mode refuses to start without one.
- **The coordination peers.** Each node knows the raft address of every member,
  including itself, and exactly one node bootstraps the group on first boot.
- **TLS, if connections leave the host.** Passwords and replicated data travel
  the network. Enable the `tls` section (see [configuration](/configuration/)).
  With TLS enabled, replication and coordination traffic between brokers is
  encrypted too, which requires every node's certificate to chain to a CA all
  peers trust - see [TLS across nodes](#tls-across-nodes).
- **Users.** The built-in `fibril`/`fibril` pair works from loopback only, so
  create at least one real user for clients that connect across the network.

## The cluster secret

Generate one and give every node the same value:

```sh
fibrilctl secret generate --show
```

That writes `<data_dir>/cluster.secret` (mode `0600`) and prints the value.
Distribute it however your platform distributes secrets:

- copy the file to each node's data dir, or
- set `FIBRIL_CLUSTER_SECRET` to the value, or
- point `coordination.secret_path` at a mounted secret file.

Resolution order is `FIBRIL_CLUSTER_SECRET`, then `coordination.secret_path`,
then the `<data_dir>/cluster.secret` file. User accounts never authenticate
node-to-node connections, so rotating a user cannot wedge replication.

## TLS across nodes

When `tls.enabled = true`, follower-to-owner replication and the coordination
raft channel also run over TLS (`tls.inter_broker`, which follows `enabled`
unless set explicitly). Peers verify each other's certificate against a CA the
same way clients do, so every node's certificate must chain to a CA all nodes
trust. Two lanes:

**Bring your own CA.** Issue each node a certificate from your CA, covering
the node's broker advertise host and its coordination peer host in the
subject alternative names. Point `tls.peer_ca_path` at the CA on every node
(unset, it falls back to the generated `<data_dir>/tls/ca.pem` when present,
then OS roots).

**Shared generated CA.** Generate material on the first node, then copy only
the CA pair to each other node's TLS dir before its first boot:

```sh
# On the first node (or ahead of time with fibrilctl cert generate):
#   <data_dir>/tls/ca.pem  ca.key  server.pem  server.key
# On every other node, before first boot:
scp node1:<data_dir>/tls/ca.{pem,key} <data_dir>/tls/
```

A node that finds only `ca.pem` + `ca.key` mints its own server certificate
from that CA on boot, so the whole deployment shares one CA and one client
pin while each node keeps its own key. Independently generated material on
each node does NOT work for inter-broker TLS: every node would have its own
CA and reject its peers. If a mesh or tunnel already encrypts inter-broker
traffic, set `tls.inter_broker = false` instead.

Transport identity and cluster membership stay separate on purpose: TLS
proves which host a connection reaches, the cluster secret authenticates the
node inside the session. Both apply.

With `tls.client_auth = require`, the listeners only complete handshakes with
holders of a deployment-CA certificate, which closes the network surface to
everything unidentified - including the coordination and replication ports
(brokers present their own certificate when dialing peers). Workload clients
then authenticate by certificate identity instead of password: issue one with
`fibrilctl cert issue <identity>` and create the matching user. `request`
mode does the same verification while still admitting certless
password-authing clients, which is the migration lane.

## Rotating certificates

A node's server certificate rotates without a restart as long as the CA stays
the same. Replace `server.pem` and `server.key` on disk (for generated
material: delete the pair and reboot-free mint by running
`fibrilctl cert generate` again, or issue from your CA), then:

```sh
fibrilctl admin reload-tls
```

The broker validates the new pair fully before swapping, so invalid material
is rejected with the old certificate still serving. New handshakes (clients,
replication, dashboard) present the new certificate, established connections
keep the one they negotiated. Rotating the CA itself requires a rolling
restart with the new CA distributed first, and re-pinning any
fingerprint-pinned clients.

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

With TLS enabled across several nodes, provision the shared CA pair into each
node's `<data_dir>/tls` (see [TLS across nodes](#tls-across-nodes)) so the
generated leaves chain to one CA - or set `FIBRIL_TLS_INTER_BROKER: "false"`
when something else encrypts the inter-broker path.

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
