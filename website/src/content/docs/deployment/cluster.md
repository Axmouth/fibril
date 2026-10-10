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

Storage power policy can affect both owner and follower persistence latency.
For Linux NVMe hosts, see the
[idle-latency tuning procedure](/configuration/#nvme-idle-latency-on-linux)
and test each host's storage before choosing a deployment policy.

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

That writes `<data_dir>/node/cluster.secret` (mode `0600`) and prints the value.
On a fresh data dir it first sets up the node layout using the configured
`coordination.node_id`, so run it with the same config the node boots with.
Distribute it however your platform distributes secrets:

- copy the file to `<data_dir>/node/cluster.secret` on each node, or
- set `FIBRIL_CLUSTER_SECRET` to the value, or
- point `coordination.secret_path` at a mounted secret file.

Startup refuses a data dir that holds files but no node layout, so a file
copied into an empty directory makes it look foreign. Prepare the data dir
first, then copy:

1. Run `fibrilctl node init` on the node, with the same config (or
   `--data-dir`) and `coordination.node_id` it will start with. It creates
   the node layout and prints where each file goes. Running it again is safe.
2. Copy the shared files into `<data_dir>/node/`: `cluster.secret`, and for a
   shared generated CA `tls/ca.pem` and `tls/ca.key` (create `tls/` first).
   Keep the secret and keys readable only by the server user (`chmod 600`).
3. Start the server.

Resolution order is `FIBRIL_CLUSTER_SECRET`, then `coordination.secret_path`,
then the `<data_dir>/node/cluster.secret` file. User accounts never authenticate
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
(unset, it falls back to the generated `<data_dir>/node/tls/ca.pem` when present,
then OS roots).

**Shared generated CA.** Generate material on the first node, then copy only
the CA pair to each other node's TLS dir before its first boot:

```sh
# On the first node (or ahead of time with fibrilctl cert generate):
#   <data_dir>/node/tls/ca.pem  ca.key  server.pem  server.key
# On every other node, after fibrilctl node init and before first start:
scp node1:<data_dir>/node/tls/ca.{pem,key} <data_dir>/node/tls/
```

As with the cluster secret, run `fibrilctl node init` on the receiving node
before the copy (see [the cluster secret](#the-cluster-secret)).

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
node's `<data_dir>/node/tls` (see [TLS across nodes](#tls-across-nodes)) so the
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

## Restarts and stuck partitions

When every node restarts at once, the newly elected controller keeps existing
owners in place until every registered node heartbeats again, or
until `coordination.ganglion.controller_startup_grace_ms` (default 30 seconds)
expires, so a node that boots late keeps its partitions. A node that is gone for
good delays owner failover by up to that grace after each leader change. Set
the grace to `0` to turn this off.

A partition whose only full copy is on a missing owner (owner-only durable
streams, and legacy single-copy queues) waits for that owner instead of moving.
The topology output lists such holds, and pending recoveries that automatic
recovery will not finish, under `consensus.controller.attention`. Two guarded
commands resolve them:

```sh
fibrilctl admin recovery withdraw <topic> <partition> [--group <group>] [--stream]
fibrilctl admin recovery accept-owner-loss <topic> <partition> [--group <group>] [--stream] --confirm-data-loss
```

Accepting owner loss gives up every write that only the old owner stored. See
[failure modes](/reliability/failure-modes/#a-recovery-or-held-owner-needs-an-operator)
for when each command is refused and what it changes.

## Adding and removing users

Users are cluster data. Create one on any node and it replicates to all:

```sh
fibrilctl user add ops --user-password "$FIBRIL_OPS_PASSWORD"
fibrilctl user list
fibrilctl user remove ops
```

The same operations are on the dashboard settings page.
