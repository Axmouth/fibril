# TUI example rework plan (task #32)

Queued plan for the rework. Scope for the first pass: **Core + partitions**.
Interactive keys are designed here but implemented in a later stretch pass.

## Goal

A live, illustrative terminal visualizer of **real Fibril wire traffic** against
a **live broker**: it shows the actual frames (handshake, auth, subscribe,
publish, confirm, deliver, ack, ping/pong, error) flowing between clients and the
broker, with **partition lanes**. Runtime-configurable. Doubles as a "is the
cluster alive and moving packets" eyeball check and a companion to the tryout
script.

## Key decision: stay frame-level (do NOT use the high-level client)

The whole value is showing the actual protocol frames. The high-level
`fibril-client` hides the handshake/auth/ping-pong/confirm internals, so it would
erase exactly what makes this interesting. Keep driving the protocol at the
**frame level** via the `fibril-protocol` codec (`ProtoCodec` / `try_encode` /
`try_decode`), as today - but refactor the hand-rolled per-connection driver into
one clean, instrumented driver that emits a `VisualEvent` for **every frame in
and out**. We keep full wire visibility; we just stop duplicating ad-hoc
handshake code.

## Core

1. **Runtime config** (clap): `--addr` (default 127.0.0.1:9876), `--clients N`,
   `--partitions P`, `--topic`, `--group`, `--rate` (msgs/s), `--payload-size`,
   `--confirm`, `--auth user:pass`, `--key-mode keyed|round-robin`. Small,
   illustrative defaults (not a stress test). Some of these also adjustable at
   runtime via keys (see Interactive).
2. **One node per client** with in + out ports (fix the current pub-is-also-sub
   duplication where one connection renders as two nodes). **Responsive layout**
   via `ratatui` constraints - adapt node count/positions to terminal size and
   degrade gracefully when small, instead of the hard "too small" cutoff and
   hardcoded x/y.
3. **Frame-accurate visualization** in both directions: every sent/received frame
   emits a `VisualEvent`; balls colored by op - setup (hello/auth/subscribe) in
   muted colors, publish blue, confirm returning teal, deliver green, ack
   returning, ping/pong dim, error red. Show the **return paths** (confirm, ack),
   not just one-way.
4. **Real metrics HUD** (revive the dead latency tracking + compute_stats):
   published/s, confirmed/s, delivered/s, acked/s, in-flight (unconfirmed /
   unacked), and p50/p99 publish->deliver latency, plus per-op frame counts.
5. **Sane payloads/rates** (configurable; small defaults) so it reads as
   illustrative.

## Partitions (the Fibril-specific upgrade)

6. Render the broker as **P partition lanes**. Keyed publishes route by FNV-1a
   hash to a lane, so per-key routing is visible (the ball enters the broker at
   its partition). Show per-partition offset / depth.
7. Config: `--partitions P` and `--key-mode keyed|round-robin`.

## Live server + tryout integration

8. Works against a **live broker** (configurable `--addr`); **no embedded
   broker**.
9. Add a launch path from the link-based tryout script (`scripts/cluster-tryout.sh`):
   a `--viz` flag (or companion command) that runs the TUI against the
   just-started cluster.
10. Run the viz in the **attached** path (like `--hold`) so quitting the TUI
    fires the existing `trap cleanup EXIT`, which already `kill`s every server it
    started. Verify the `--keep` early-return is not taken in viz mode, so
    shutdown always destroys the servers it launched.

**North star:** a single command (the tryout script, eventually
`curl fibril.sh/tryout.sh | sh`) that spins up a cluster AND drops you straight
into the live interactive TUI - watch real traffic and partitions, press keys to
kill a consumer or repartition, then Ctrl-C tears the whole thing down. That is
the onboarding demo to aim the interactive stretch (and the tryout `--viz` hook)
at.

## Tryout demo seeding (pairs with --viz, and good on its own)

An empty cluster is a dull demo. The tryout should come up already alive:

- **Seed queues with partitions on startup** (a couple of topics, multi-partition,
  maybe one grouped) via `fibrilctl` / the admin API once the cluster is up, so
  the topology page and the TUI immediately show partitions spread across nodes -
  not an empty board.
- **Optional light demo load**: a small publisher + consumer firing a steady,
  modest stream (lighter than `--steady-bench`; reuse `steady_c` or a tiny
  dedicated generator) so there is visible traffic flowing without configuring
  anything. Gate it behind a flag (e.g. `--demo-load`) and make it part of the
  `--viz` path.
- Keep it bounded and illustrative (small rates/payloads), and ensure it is torn
  down by the same `trap cleanup EXIT` as the servers.

This is partly a `scripts/cluster-tryout.sh` change (seeding + a flag), not just
the TUI crate, but it is what makes the one-command demo land.

## Phase 2: interactive keys

The stretch that turns the viz from a passive board into a guided tour of
Fibril's distinctive behavior. Driven by a lock-free `Control` (atomics) shared
between the UI (mutates on key press) and the drivers (read live each cycle).

Done (work live on the standalone `--viz` broker):

- [x] `Tab` - move focus between clients. `q` / `Esc` - quit.
- [x] `space` - pause/resume the focused client's publishing (its lane goes quiet,
      then resumes).
- [x] `[` / `]` - decrease / increase publish rate. `c` - toggle confirm mode
      (the cyan confirm return path appears). `g` - switch keyed vs round-robin
      routing (the lane spread changes).

Done (manual-ack pass): consumers now subscribe with manual ack, so each delivery
shows an ack return path and there is unacked in-flight to redeliver. Each client
is run by a supervisor that reconnects when it is brought back.

- [x] `k` / `r` - kill / restart the focused client. On `k` the session drops its
      connections, the broker reclaims the unacked in-flight, and on `r` the client
      reconnects, re-subscribes, and receives the redeliveries (at-least-once).
- [x] `n` - nack (requeue) the focused client's next delivery, which the broker
      redelivers.

Done (cohort pass): `--consumer-group <name>` makes every client an exclusive
cohort member - each subscribes to every partition (on its owner) with the cohort
id and a reused server-minted member id, exactly as the high-level client's
`.exclusive()` does, and the broker's per-partition gate delivers each partition
to one member. Killing a member (`k`) now moves its partitions to a surviving
peer (not just redelivery-to-self on reconnect). The cohort name shows in the HUD.

Queued:

- `+` / `-` - add / remove a partition (live repartition). Needs ganglion (live
  repartition is coordination-only). Now that the visualizer routes across a
  cluster, this is feasible against `--viz --ganglion` via the admin repartition
  API, with the lanes appearing/draining live.

## Cluster owner-routing - DONE

The visualizer now fetches topology (declare then poll until every partition has
an owner), connects to every broker, and routes each publish and subscribe to the
partition's owner connection (read halves merged with `select_all`, sinks indexed
by broker so pings are answered on the right connection). Lanes are colored by
owning broker. Degrades cleanly to a single broker when ownership is unknown
(standalone). Launch against a real cluster with
`scripts/cluster-tryout.sh --viz --ganglion --nodes 3`.

These are designed now but implemented after Phase 1 (Core + partitions). They
pair with the one-command demo north star: fire it up, then press keys to make
failover / repartition / redelivery happen on screen.

## Out of scope (this pass)

- High-level `fibril-client` (would hide traffic).
- Embedded in-process broker (we want a live server).

## Checklist

Phase 1 (Core + partitions) - DONE:

- [x] CLI config (clap) + small defaults.
- [x] Refactor per-connection frame driver -> one instrumented driver emitting a
      `VisualEvent` per frame (single select loop per client, answers pings inline).
- [x] One node per client + responsive layout.
- [x] Lifecycle return paths (confirm, deliver, pong) + colors. Manual-ack return
      path is a Phase 2 addition (Phase 1 uses auto-ack).
- [x] Metrics HUD + p50/p99 publish->deliver latency (from the deliver frame).
- [x] Partition lanes + keyed routing (FNV-1a, matching the broker).
- [x] `--addr` live-server config (plus declares the topic with `--partitions`).
- [ ] tryout `--viz` launch hook; confirm trap-cleanup destroys launched servers.

Phase 2 (queued): interactive keys (see above).
