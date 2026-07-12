# Scenario files

`scripts/scenario.sh` runs a scripted sequence of operator actions against
script-managed brokers on non-default ports, so demos and dashboard
verification never touch a real deployment. Full verb reference lives in the
script header; this is the operational summary.

## Running

```sh
# Single broker (ports 18081 admin / 19876 broker):
scripts/scenario.sh scripts/scenarios/activity-tour.scenario

# Three-node coordinated cluster (admin 18081..18083, broker 19876..19878):
scripts/scenario.sh scripts/scenarios/membership-churn.scenario --nodes 3 --ganglion

# Drive an already-running broker instead of managing nodes:
scripts/scenario.sh my.scenario --admin http://127.0.0.1:8081
```

Ctrl-C tears everything down. Node data lives in temp dirs and is removed on
exit.

## Verbs

One step per line, `#` comments allowed:

| verb | effect |
| --- | --- |
| `say <text>` | narrate progress to the terminal |
| `wait <seconds>` | let the dashboard breathe |
| `declare-queue <topic> [partitions]` | declare a work queue |
| `declare-stream <topic> [partitions] [durability]` | declare a stream |
| `publish-burst <topic> <count> [size]` | real client load via `e2e_c` (runs at full speed, so a burst is seconds long) |
| `consume <topic> <seconds>` | background reader draining the topic (caps at 1M messages) |
| `stream-load <topic> <rate> <seconds> [readers] [partitions]` | rate-limited stream writers plus durable-cursor readers via `bench_stream`; cursor names are stable so a later run resumes the same cursors |
| `test-publish <topic> [text...]` | one marked message via the admin API |
| `delete-queue <topic>` | delete a queue |
| `drain <grace_ms>` | announce a drain |
| `node-kill <n>` / `node-start <n>` | cluster membership churn (`--nodes` mode) |

## Included scenarios

- `activity-tour.scenario` - a tour of the Activity feed and attention panel
  on one broker: declares, a no-consumer backlog raising attention, the
  resolve, test publishes, a delete, a drain.
- `membership-churn.scenario` - nodes leave and rejoin while the Cluster page
  and Activity feed react (X-eyed ghosts, node_left / node_joined). Run with
  `--nodes 3 --ganglion`.
- `load-signals.scenario` - the cluster diagram's load ladder: idle shimmer,
  a flat-out burst (strained face), the cooling tail, a delivery-side drain.
  Run with `--nodes 3 --ganglion`.
- `stream-tour.scenario` - the Streams page: a multi-partition durable
  stream declared through the admin API, durable readers at tail, cursors
  falling behind during a readerless append, then the same cursors
  resuming and catching up. Run with `--nodes 3 --ganglion`.

## Extra load recipes

The Connections page diagram wants live publisher and subscriber
connections. Steady traffic that does not saturate the box (confirmed
publishing is RTT-bound):

```sh
# Against a scenario-managed cluster (broker port 19876):
target/release/e2e_c --addr 127.0.0.1:19876 -m 2000000 -c 1 --writer --confirmed --topic orders
target/release/e2e_c --addr 127.0.0.1:19876 -m 100000000 -c 2 --reader --firehose --topic orders --idle-timeout-ms 300000
```

The smart client follows partition ownership, so in a cluster the
connections land on the owner broker's page - check its admin, not
necessarily node 1's.

CAUTION: scenario node data lives under /tmp. A publish burst is written to
disk on the owner AND its followers, so size bursts for the tmpfs you have
(3M x 256B is fine, 50M is not).
