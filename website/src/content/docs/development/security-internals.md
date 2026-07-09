---
title: TLS and authentication internals
description: How transport security and broker authentication are implemented, and the design decisions behind them.
---

This page is the design record for the 0.3 security work: TLS in transit and
broker authentication. It covers how the pieces fit together and why the shape
was chosen, for a reader changing this code rather than operating it. For
operator instructions, see [configuration](/configuration/) and the
[cluster setup guide](/deployment/cluster/).

## TLS transport

TLS is rustls at the network boundary. `handle_connection` is generic over the
stream type, so a TLS-wrapped stream and a plain TCP stream flow through the
same protocol path with no per-site conditional, which also keeps the
deterministic-simulation build compiling unchanged. The material and rustls
setup live in a dedicated `fibril-tls` crate, so the CLI can generate
certificates without depending on the server crate.

Fibril never ships a certificate. Shipped default certificates are the
default-password vulnerability class: every deployment would share one private
key. Operators either supply their own PEM files or opt into per-deployment
generation (the Elasticsearch 8 pattern), which creates a CA and a server
certificate under the data dir and prints the CA fingerprint so clients can pin
it. Generated mode serves the leaf plus the CA so a client pinning the CA
fingerprint has the CA in the presented chain to validate against.

A fingerprint pin is not a presence check. The client accepts the broker only
when the pinned certificate is the presented leaf, or is a CA that actually
signed the presented leaf - path validation with the pinned certificate as the
sole trust anchor. Accepting the connection merely because the pinned
certificate appears somewhere in the chain would be bypassable: the CA
certificate is public, so a man-in-the-middle could staple it beside a leaf
whose key it holds and be trusted. Path validation is what lets a CA pin survive
leaf rotation without trusting a rogue leaf. Hostname is not checked under a pin,
because the pin, not a name, is the trust root.

### Naming a transport mismatch

The frame header carries no magic number. A TLS ClientHello read by the
plaintext frame codec parses its leading bytes as an enormous payload length
and the connection hangs, and a plaintext frame handed to a TLS acceptor fails
as an opaque handshake error. So the accept path sniffs the first bytes of every
connection before choosing a transport: a TLS ClientHello starts `0x16 0x03`,
and a plaintext first frame starts with two zero length bytes. A plaintext
client on a TLS listener receives a plaintext error frame with code `426`
echoing the HELLO request id, so the client surfaces it through its pending
request rather than a bare disconnect. The reverse direction has no in-band
channel, so the client infers a probable plaintext broker from a handshake that
ends early.

The clients keep three failure classes apart because their fixes differ: a
transport mismatch (fix config on one side), a certificate-trust failure (fix
the CA path or the pin), and other handshake or configuration errors. Collapsing
them into one error would make the guidance useless.

## Authentication

Users are cluster-shared data, not node configuration. The user document (name
to argon2 hash) lives in the durable global store under its own key, kept
separate from the runtime-settings document so a settings write can never
clobber users and hashes never appear on the settings surface. It seeds from
config only when the store is empty; after that the store owns the users.

The `AuthHandler` trait decides with connection context and returns a denial
that carries the message the client sees, so a rejection guides the fix instead
of failing with a bare `401`. The decision order is: the node principal, then
stored users from any peer, then the built-in `fibril`/`fibril` pair from
loopback only. Loopback-only default credentials are the RabbitMQ guest model:
local development works out of the box while remote access requires a real user.
A real user named `fibril` shadows the built-in pair entirely. Verification of
an unknown user still runs a dummy argon2 verification so response timing does
not reveal whether a username exists.

### Breaking the trust circularity

Live user edits replicate over the cluster, but replication needs
authentication, and a joining node has no user data yet. Every system that
replicates users resolves this the same way: node-to-node trust is
operator-provisioned and completely separate from the user database. Fibril uses
a cluster shared secret (the Erlang-cookie / MongoDB-keyFile precedent).
Replication and coordination connections authenticate as a node principal
(`@node`, a namespace real usernames cannot claim) with the secret, never with a
user account. Ganglion mode refuses to start without one. This deliberately does
not follow the Kafka inter-broker-user pattern, where a superuser in the regular
user database is used by brokers, because that entangles node trust with user
lifecycle: rotating that user could wedge the cluster.

### Replicating user changes

The cluster path mirrors the runtime-settings document exactly: a versioned
`fibril/auth_users` cluster attribute, compare-and-set updates, and a
watch-driven sync task that adopts newer versions into each node's local store.
The first node to write seeds the document from its local (config-seeded) view.

## First-boot setup

Setup mode exists to make both easy without forcing either. When armed and no
completed marker exists, the server serves only a localhost setup page and the
broker listener stays down, so no traffic can precede the operator's choice. The
choice (TLS material, an optional admin user, an optional cluster secret)
persists as a config overlay written to the data dir, which boot layers below
explicit file and environment config, so explicit configuration always wins. A
fully configured deployment marks setup complete and boots straight through,
which is the unattended lane. The setup listener binds loopback because the
supply path uploads a private key and no credentials exist yet to protect a
wider bind.
