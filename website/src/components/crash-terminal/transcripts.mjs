// Terminal transcripts for the crash test section. Commands, output and log
// lines come from recorded runs of fibril-server and fibrilctl built from the
// repo (single node, then a local three-node ganglion cluster). Log lines are
// trimmed: timestamps, thread ids and file paths are cut, module paths keep
// their last segment, and low-value fields are dropped. Message wording and
// the kept field names are exact. Lines marked proposed are not printed by
// Fibril today. scripts/record-crash-demo.mjs reruns both scenarios and checks
// every line here still matches.

const payload = '{"to":"ana@example.com"}';
const L = (step, pane, kind, text, extra = {}) => ({ step, pane, kind, text, ...extra });

const single = {
  id: "single",
  label: "Crash and restart",
  panes: [{ id: "shell", title: "~/fibril-demo", states: [["running"], ["running"], ["broker down", "down"], ["running"], ["running"]] }],
  steps: ["Start the broker", "Publish one message and wait for the confirm", "Kill the broker with SIGKILL", "Start it again on the same data dir", "Consume the message"],
  final: "Recorded run: published and confirmed, killed with SIGKILL, restarted, consumed.",
  foot: "Recorded from fibril-server and fibrilctl built from source, on a scratch data dir with the default config. Log lines are trimmed: no timestamps, thread ids or file paths, module paths cut to their last segment, low-value fields dropped.",
  lines: [
    L(0, "shell", "cmd", "fibril-server &"),
    L(1, "shell", "cmd", `fibrilctl queue publish email.send --message '${payload}'`),
    L(1, "shell", "out", "published email.send at offset 0"),
    L(2, "shell", "cmd", "kill -9 %1"),
    L(2, "shell", "kill", "[1]+  Killed          fibril-server"),
    L(3, "shell", "cmd", "fibril-server &"),
    L(4, "shell", "cmd", "fibrilctl queue consume email.send"),
    L(4, "shell", "log", 'INFO fibril_broker::broker: queue recovered from disk topic="email.send" partition=0 next_offset=1'),
    L(4, "shell", "out", payload),
  ],
};

const down = ["killed", "down"];
const failover = {
  id: "failover",
  label: "Failover",
  panes: [
    { id: "node-1", title: "node-1 / raft 1 / :9876", states: [["owner"], ["owner"], down, down, down, down, down] },
    { id: "node-2", title: "node-2 / raft 2 / :9877", states: [["follower"], ["follower"], ["owner gone", "wait"], ["raft leader", "wait"], ["recovery source", "wait"], ["owner, epoch 2"], ["owner, epoch 2"]] },
    { id: "node-3", title: "node-3 / raft 3 / :9878", states: [["follower"], ["follower"], ["owner gone", "wait"], ["owner gone", "wait"], ["sealed", "wait"], ["follower"], ["follower"]] },
    { id: "client", title: "client" },
  ],
  steps: [
    "Three nodes start, node-1 owns email.send",
    "The publish is confirmed once 2 of 3 copies are on disk",
    "The owner is killed with kill -9",
    "node-2 wins the raft election",
    "node-1 misses the 9 s heartbeat timeout, node-2 seals the survivors and picks itself",
    "node-2 activates as owner under epoch 2",
    "The client reads the message from the new owner",
  ],
  final: "Recorded run with default timeouts: owner killed, node-2 activated about 10 seconds later, message consumed. With eager failover on, the same run took about 6 seconds.",
  foot: "Recorded with scripts/record-crash-demo.mjs from a local three-node cluster: FIBRIL_COORDINATION_MODE=ganglion, a shared FIBRIL_CLUSTER_SECRET, FIBRIL_COORDINATION_PEERS for raft ids 1 to 3 and FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY=replica_durable:2. The queue was declared first with fibrilctl queue declare. Broker ports as in compose.cluster.example.yaml. Default timeouts: brokers heartbeat every 3 s and drop out of placement after 9 s (coordination.ganglion.liveness_ttl_ms). With eager failover on (runtime_seed.replication.eager_failover), the same scenario activated node-2 about 6 s after the kill. Most of that was electing a new raft leader, since node-1 also led raft. Times are seconds after the kill. Log lines are trimmed: no timestamps or file paths, module paths cut to their last segment, low-value fields dropped.",
  lines: [
    L(0, "node-1", "cmd", "fibril-server"),
    L(0, "node-2", "cmd", "fibril-server"),
    L(0, "node-3", "cmd", "fibril-server"),
    L(0, "node-1", "log", "INFO fibril_broker::broker: applying queue assignment transition intent=BecomeOwner next_epoch=Some(1)"),
    L(1, "client", "cmd", `fibrilctl queue publish email.send --message '${payload}'`),
    L(1, "client", "out", "published email.send at offset 1"),
    L(2, "node-1", "kill", "Killed"),
    L(2, "node-2", "log", 'WARN fibril_broker::replication: follower replication worker tick failed: Unknown("owner peer connect failed: Connection refused (os error 111)")', { t: "+0.1s" }),
    L(3, "node-2", "log", "INFO openraft::engine::handler::server_state_handler: become leader id=2", { t: "+4.6s" }),
    L(4, "node-2", "log", "WARN fibril_coordination_ganglion: broker heartbeat expired, node is excluded from placement node=node-1 since_last_heartbeat_ms=10056 ttl_ms=9000", { t: "+9.4s" }),
    L(4, "node-3", "log", 'INFO fibril_coordination_ganglion::recovery_witnesses: recovery seal accepted from old replica, history compatibility is checked once the required seals arrive replica="node-2"', { t: "+9.5s" }),
    L(4, "node-2", "log", 'INFO fibril_coordination_ganglion::recovery_selection: verified queue recovery source selected; installation and activation still required source="node-2"', { t: "+9.5s" }),
    L(5, "node-2", "log", 'INFO fibril::recovery_driver: queue recovery activated topic="email.send" epoch=2 replicas=2', { t: "+9.7s" }),
    L(5, "node-3", "log", "INFO fibril_broker::broker: applying queue assignment transition intent=BecomeFollower next_epoch=Some(2)", { t: "+9.7s" }),
    L(6, "client", "cmd", "fibrilctl --broker 127.0.0.1:9877 queue consume email.send"),
    L(6, "client", "out", payload),
  ],
};

export const terminalTabs = [single, failover];
