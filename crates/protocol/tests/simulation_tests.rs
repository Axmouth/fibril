//! Deterministic cluster simulation tests (task #97).
//!
//! These run the real broker, replication, and protocol code inside a turmoil
//! `Sim`, where time is simulated and the network can be partitioned or dropped
//! deterministically. They only compile under the `simulation` feature, which
//! flips `fibril_util::net` (and therefore the broker connection handler, the
//! follower replication dial, and the `Conn` alias) from tokio's TCP to
//! turmoil's simulated TCP. Run them with:
//!
//!   cargo test -p fibril-protocol --features simulation --test simulation_tests
//!
//! turmoil gives each simulated host its own current-thread tokio runtime plus a
//! LocalSet, so a broker built INSIDE a host closure spawns its background tasks
//! onto that host's runtime and its timers run on the simulated clock. The flip
//! side is that a broker can only be driven from within its own host - there is
//! no shared runtime across hosts - so cross-host orchestration here goes through
//! the simulated network or through plain shared memory (atomics), never by
//! calling another host's broker.
#![cfg(feature = "simulation")]

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use fibril_broker::broker::{
    Broker, BrokerConfig, FollowerReplicationWorkerConfig, FollowerReplicationWorkerStatus,
    QueueOwnership,
};
use fibril_broker::coordination::{
    Coordination, CoordinationSnapshot, DeterministicPartitionPlacement,
    DeterministicStreamPlacement, NodeInfo, PartitionAssignment, QueueIdentity,
    ReplicationDurabilityPolicy, StaticCoordination, StreamIdentity,
};
use fibril_broker::queue_engine::StromaEngine;
use fibril_coordination_ganglion::GanglionCoordination;
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_protocol::v1::handler::{ConnectionSettings, run_server};
use fibril_protocol::v1::replication::CoordinationProtocolOwnerPeerResolver;
use fibril_storage::Partition;
use fibril_util::{StaticAuthHandler, unix_millis};
use stroma_core::{KeratinConfig, SnapshotConfig, StromaKeratinConfig, TempDir};
use uuid::Uuid;

/// Build a fresh on-disk StromaEngine in a unique temp directory. The directory
/// is held by the returned `TempDir` guard for the lifetime of the broker.
async fn open_engine(tag: &str) -> (StromaEngine, TempDir) {
    let dir = TempDir {
        root: std::env::current_dir()
            .unwrap()
            .join("test_data")
            .join(format!("sim-{tag}-{}", Uuid::now_v7())),
    };
    std::fs::create_dir_all(&dir.root).unwrap();
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    (engine, dir)
}

fn test_broker_config() -> BrokerConfig {
    BrokerConfig {
        inflight_ttl_ms: 2_000,
        expiry_poll_min_ms: 50,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 50,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    }
}

/// Smoke test: the broker actually runs inside a turmoil host - construction,
/// background tasks, keratin disk I/O, and a publish whose confirm resolves on
/// the simulated clock. No network is involved, which isolates the question of
/// whether the broker cooperates with turmoil's per-host current-thread runtime
/// and simulated time before any cluster scenario builds on it.
#[test]
fn broker_runs_inside_turmoil_host() {
    let mut sim = turmoil::Builder::new().build();

    sim.client("node", async {
        let topic = "sim.smoke";
        let (engine, _dir) = open_engine("smoke").await;
        let broker = Broker::new(engine, test_broker_config(), None);

        let (publisher, _confirms) = broker
            .get_publisher(topic, Partition::new(0), &None)
            .await
            .unwrap();
        for payload in [b"first".as_slice(), b"second".as_slice()] {
            let reply = publisher
                .publish(
                    payload.to_vec(),
                    unix_millis(),
                    unix_millis(),
                    None,
                    Default::default(),
                    None,
                )
                .await
                .unwrap();
            reply.await.unwrap().unwrap();
        }

        let checkpoint = broker
            .export_owner_state_checkpoint(topic, Partition::new(0), None)
            .await
            .unwrap();
        assert_eq!(
            checkpoint.message_next_offset, 2,
            "both publishes are durable on the simulated host"
        );

        broker.shutdown().await;
        Ok(())
    });

    sim.run().expect("simulation runs to completion");
}

const OWNER_PORT: u16 = 9100;

fn owner_addr() -> SocketAddr {
    SocketAddr::new(turmoil::lookup("a-owner"), OWNER_PORT)
}

fn bind_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), OWNER_PORT)
}

fn node(id: &str, addr: SocketAddr) -> NodeInfo {
    NodeInfo {
        node_id: id.to_string(),
        broker_addr: addr.to_string(),
        admin_addr: None,
    }
}

/// The coordination view held by the follower host: both nodes registered (the
/// owner at its real simulated listener address, so the resolver dials it) and
/// the partition assigned to the owner with the follower replicating it.
fn follower_snapshot(topic: &str, epoch: u64, generation: u64) -> CoordinationSnapshot {
    let queue = QueueIdentity::new(topic, Partition::new(0), None);
    let mut nodes = HashMap::new();
    nodes.insert("a-owner".to_string(), node("a-owner", owner_addr()));
    nodes.insert(
        "b-follower".to_string(),
        node("b-follower", SocketAddr::new(turmoil::lookup("b-follower"), OWNER_PORT)),
    );
    let assignment = PartitionAssignment::new(
        queue.clone(),
        "a-owner",
        vec!["b-follower".to_string()],
        epoch,
    );
    CoordinationSnapshot {
        nodes,
        assignments: HashMap::from([(queue, assignment)]),
        stream_assignments: HashMap::new(),
        generation,
    }
}

/// Run an owner broker on its own simulated host: publish `payloads`, fence its
/// logs at epoch 1 (as its own watcher's BecomeOwner would in production), then
/// serve the replication protocol forever. The host stays up until the sim
/// crashes it.
fn spawn_owner_host(sim: &mut turmoil::Sim<'_>, topic: &'static str, payloads: &'static [&[u8]]) {
    sim.host("a-owner", move || async move {
        let (engine, _dir) = open_engine("owner").await;
        let broker = Broker::new(engine, test_broker_config(), None);

        let (publisher, _confirms) = broker
            .get_publisher(topic, Partition::new(0), &None)
            .await
            .unwrap();
        for payload in payloads {
            let reply = publisher
                .publish(
                    payload.to_vec(),
                    unix_millis(),
                    unix_millis(),
                    None,
                    Default::default(),
                    None,
                )
                .await
                .unwrap();
            reply.await.unwrap().unwrap();
        }
        broker
            .advance_replication_epoch(topic, Partition::new(0), None, 1)
            .await
            .unwrap();

        run_server(
            bind_addr(),
            broker,
            TcpStats::new(10),
            ConnectionStats::new(),
            None::<StaticAuthHandler>,
            ConnectionSettings::new(Some(60)),
            None,
            None,
            None,
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        Ok(())
    });
}

/// A follower broker on host `b-follower` that, driven only by its supervised
/// assignment watcher and a static view pointing at the owner, replicates to
/// caught-up over the simulated network within `deadline`. Shared by the clean
/// and the lossy-link catch-up scenarios.
async fn run_catch_up_follower(
    topic: &'static str,
    tag: &str,
    deadline: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, _dir) = open_engine(tag).await;
    let broker = Broker::new(engine, test_broker_config(), None);

    let coordination = Arc::new(StaticCoordination::new(
        "b-follower",
        follower_snapshot(topic, 1, 1),
    ));
    let resolver = Arc::new(CoordinationProtocolOwnerPeerResolver::new(
        coordination.clone(),
    ));
    broker.spawn_assignment_watcher_with_follower_replication(
        coordination.clone(),
        resolver.clone(),
        FollowerReplicationWorkerConfig {
            caught_up_poll_ms: 60_000,
            ..Default::default()
        },
    );

    tokio::time::timeout(deadline, async {
        loop {
            let state = broker
                .follower_replication_worker_snapshot(topic, Partition::new(0), None)
                .await;
            if state
                .as_ref()
                .is_some_and(|s| s.status == FollowerReplicationWorkerStatus::CaughtUp)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("follower catches up");

    resolver.close_all().await;
    broker.shutdown().await;
    Ok(())
}

/// A follower broker, driven only by its supervised assignment watcher, catches
/// up to the owner over the simulated network, all on the simulated clock.
#[test]
fn follower_catches_up_over_simulated_network() {
    let topic = "sim.catchup";
    let payloads: &[&[u8]] = &[b"first", b"second"];

    let mut sim = turmoil::Builder::new().build();
    spawn_owner_host(&mut sim, topic, payloads);
    sim.client("b-follower", async move {
        run_catch_up_follower(topic, "follower", Duration::from_secs(30)).await
    });

    sim.run().expect("simulation runs to completion");
}

/// The same catch-up, but the link between owner and follower drops, repairs, and
/// delays messages throughout. The replication worker reconnects through the
/// disruption and still reaches caught-up - the flapping-follower path the
/// single-node tests cannot exercise. A fixed RNG seed keeps the fault schedule
/// deterministic across runs.
#[test]
fn follower_catches_up_over_lossy_link() {
    let topic = "sim.lossy.catchup";
    let payloads: &[&[u8]] = &[b"first", b"second", b"third"];

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(300))
        .min_message_latency(Duration::from_millis(10))
        .max_message_latency(Duration::from_millis(150))
        .fail_rate(0.03)
        .repair_rate(0.9)
        .rng_seed(1)
        .build();
    spawn_owner_host(&mut sim, topic, payloads);
    sim.client("b-follower", async move {
        run_catch_up_follower(topic, "lossy-follower", Duration::from_secs(180)).await
    });

    sim.run().expect("simulation runs to completion");
}

/// The headline scenario: a caught-up follower takes over when the owner is cut
/// off by a network partition, with no data loss and a fenced epoch bump.
///
/// Static coordination scripts the placement (no ganglion on the seam yet), so
/// the follower drives its own view: it catches up, the main thread partitions
/// the owner away, and the follower then promotes itself under a bumped epoch and
/// serves a fresh publish. The promoted log continues from exactly the replicated
/// tails (no loss), and promotion only happens under the higher epoch (the
/// fencing mechanism that prevents a stale owner from being accepted). The
/// stronger split-brain assertion - a returning old owner being refused - needs
/// shared coordination (ganglion raft on the seam) and is the next scenario.
#[test]
fn owner_partition_fails_over_to_caught_up_follower() {
    let topic = "sim.failover";
    let payloads: &[&[u8]] = &[b"failover-first", b"failover-second"];

    let mut sim = turmoil::Builder::new().build();
    spawn_owner_host(&mut sim, topic, payloads);

    let caught_up = Arc::new(AtomicBool::new(false));
    let owner_cut_off = Arc::new(AtomicBool::new(false));

    let follower_caught_up = caught_up.clone();
    let follower_cut_off = owner_cut_off.clone();
    sim.client("b-follower", async move {
        let (engine, _dir) = open_engine("failover").await;
        let broker = Broker::new(engine, test_broker_config(), None);

        let coordination = Arc::new(StaticCoordination::new(
            "b-follower",
            follower_snapshot(topic, 1, 1),
        ));
        let resolver = Arc::new(CoordinationProtocolOwnerPeerResolver::new(
            coordination.clone(),
        ));
        broker.spawn_assignment_watcher_with_follower_replication(
            coordination.clone(),
            resolver.clone(),
            FollowerReplicationWorkerConfig {
                caught_up_poll_ms: 60_000,
                ..Default::default()
            },
        );

        // Phase 1: replicate to caught-up, then signal the orchestrator.
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let state = broker
                    .follower_replication_worker_snapshot(topic, Partition::new(0), None)
                    .await;
                if state
                    .as_ref()
                    .is_some_and(|s| s.status == FollowerReplicationWorkerStatus::CaughtUp)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("follower catches up before the partition");
        let owner_checkpoint = broker
            .follower_replication_worker_snapshot(topic, Partition::new(0), None)
            .await
            .expect("worker snapshot after catch-up");
        let replicated_message_next = owner_checkpoint.message_next_offset;
        assert_eq!(
            replicated_message_next, 2,
            "the follower replicated exactly the owner's two messages"
        );
        caught_up.store(true, Ordering::SeqCst);

        // Phase 2: wait for the orchestrator to partition the owner away, then
        // promote under a fenced epoch by advancing this node's coordination.
        tokio::time::timeout(Duration::from_secs(30), async {
            while !follower_cut_off.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("owner is partitioned away");

        let queue = QueueIdentity::new(topic, Partition::new(0), None);
        let mut promoted = follower_snapshot(topic, 2, 2);
        promoted.assignments.insert(
            queue.clone(),
            PartitionAssignment::new(queue.clone(), "b-follower", vec![], 2),
        );
        coordination.update_snapshot(promoted);

        // Phase 3: the watcher promotes at local tails and the broker serves as
        // owner, proven by a successful new publish.
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok((publisher, _confirms)) =
                    broker.get_publisher(topic, Partition::new(0), &None).await
                {
                    if let Ok(reply) = publisher
                        .publish(
                            b"post-failover".to_vec(),
                            unix_millis(),
                            unix_millis(),
                            None,
                            Default::default(),
                            None,
                        )
                        .await
                    {
                        if reply.await.unwrap().is_ok() {
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("promoted follower accepts owner traffic after failover");

        // No data loss: the promoted log is exactly the replicated history plus
        // the one post-failover publish.
        let promoted_checkpoint = broker
            .export_owner_state_checkpoint(topic, Partition::new(0), None)
            .await
            .unwrap();
        assert_eq!(
            promoted_checkpoint.message_next_offset,
            replicated_message_next + 1,
            "promoted log continues from the replicated tails with no loss"
        );

        resolver.close_all().await;
        broker.shutdown().await;
        Ok(())
    });

    // Orchestrator: step the sim, partition the owner once the follower is
    // caught up, and run until the follower client finishes its assertions.
    loop {
        let finished = sim.step().expect("simulation step");
        if follower_caught_up.load(Ordering::SeqCst) && !owner_cut_off.load(Ordering::SeqCst) {
            sim.partition("a-owner", "b-follower");
            owner_cut_off.store(true, Ordering::SeqCst);
        }
        if finished {
            break;
        }
    }
}

const RAFT_PORT: u16 = 9200;

/// A [`RaftDialer`] that connects over turmoil's simulated TCP, so a ganglion
/// raft cluster can run inside a turmoil `Sim`. ganglion takes no turmoil
/// dependency itself - the transport is injected here, in test code, exactly the
/// way production injects `TokioDialer`.
#[derive(Clone, Default)]
struct TurmoilDialer;

impl ganglion::RaftDialer for TurmoilDialer {
    type Stream = turmoil::net::TcpStream;

    async fn dial(&self, addr: &str) -> std::io::Result<Self::Stream> {
        turmoil::net::TcpStream::connect(addr).await
    }
}

/// A 3-node ganglion raft cluster forms a leader and replicates a committed
/// write entirely over the simulated network, on the simulated clock. This is
/// the keystone proof for coordination-under-simulation: every vote, append, and
/// commit RPC flows through the injected `TurmoilDialer` and ganglion's now
/// transport-generic `serve_connection`, with no real sockets and no ganglion
/// dependency on the simulator. It unblocks the shared-coordination scenarios
/// (a partitioned old owner learning it was demoted) that static coordination
/// cannot express.
#[test]
fn ganglion_raft_cluster_forms_and_replicates_over_simulated_network() {
    use ganglion::openraft::BasicNode;
    use ganglion::{
        CoordinationSnapshot, DialerNetworkFactory, GanglionLogStore, GanglionStateMachine,
        RaftMetadataNode, WireFormat, default_raft_config, serve_connection,
    };

    let names: [&str; 3] = ["node1", "node2", "node3"];
    let ids: [u64; 3] = [1, 2, 3];

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(120))
        .build();

    // Counts nodes that have observed the replicated write. Each node waits for
    // all three before returning, so no node tears down its listener while a
    // peer still needs to catch up.
    let observed = Arc::new(AtomicUsize::new(0));

    for (name, id) in names.iter().zip(ids) {
        let observed = observed.clone();
        sim.client(*name, async move {
            let node = Arc::new(
                RaftMetadataNode::start_with_network(
                    id,
                    default_raft_config().unwrap(),
                    DialerNetworkFactory::with_dialer(TurmoilDialer),
                    GanglionLogStore::default(),
                    GanglionStateMachine::default(),
                )
                .await
                .expect("raft node starts"),
            );

            // Serve this node's raft RPCs over the simulated network.
            let serve_node = node.clone();
            tokio::spawn(async move {
                let listener =
                    turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, RAFT_PORT)).await?;
                loop {
                    let (stream, _) = listener.accept().await?;
                    let raft = serve_node.raft().clone();
                    tokio::spawn(serve_connection(stream, raft, WireFormat::default()));
                }
                #[allow(unreachable_code)]
                Ok::<_, std::io::Error>(())
            });

            // One node bootstraps membership with every peer's simulated address.
            // Peers whose listeners are not up yet are retried by openraft.
            if id == 1 {
                let mut members = BTreeMap::new();
                for (peer, peer_id) in names.iter().zip(ids) {
                    members.insert(peer_id, BasicNode::new(format!("{peer}:{RAFT_PORT}")));
                }
                node.initialize(members)
                    .await
                    .expect("membership initializes");
            }

            // A leader emerging at all proves vote and append RPCs crossed the
            // simulated transport.
            node.wait_for_any_leader(Duration::from_secs(60))
                .await
                .expect("a leader is elected");
            if node.is_leader().await {
                node.write_snapshot(CoordinationSnapshot {
                    generation: 1,
                    ..CoordinationSnapshot::default()
                })
                .await
                .expect("leader write commits");
            }

            // Every node observes the committed write replicated to its own state
            // machine.
            let mut committed = node.watch_committed();
            tokio::time::timeout(Duration::from_secs(60), async {
                while committed.borrow_and_update().generation < 1 {
                    committed.changed().await.expect("committed watch open");
                }
            })
            .await
            .expect("node observes the replicated write");
            assert!(node.committed_snapshot().generation >= 1);

            observed.fetch_add(1, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(60), async {
                while observed.load(Ordering::SeqCst) < names.len() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("all nodes observe the replicated write");

            Ok(())
        });
    }

    sim.run().expect("simulation runs to completion");
}

/// Broker-replication NodeInfo for a node, addressed by its simulated hostname so
/// the replication resolver can dial it over turmoil.
fn split_brain_node_info(name: &str) -> NodeInfo {
    NodeInfo {
        node_id: name.to_string(),
        broker_addr: format!("{name}:{OWNER_PORT}"),
        admin_addr: None,
    }
}

/// The live broker set the controller plans against. The owner drops out once it
/// is partitioned away, which is what moves ownership to the follower.
fn split_brain_live(owner_partitioned: bool) -> HashMap<String, NodeInfo> {
    let mut live = HashMap::new();
    live.insert("b-follower".to_string(), split_brain_node_info("b-follower"));
    if !owner_partitioned {
        live.insert("a-owner".to_string(), split_brain_node_info("a-owner"));
    }
    live
}

/// Start one raft node on the current simulated host: serve its RPCs over the
/// turmoil transport, and (on node 1) bootstrap the 3-node membership. Returns a
/// GanglionCoordination wrapping the node, ready for the broker watcher and the
/// controller loop.
async fn start_split_brain_node(name: &'static str, id: u64) -> Arc<GanglionCoordination> {
    use ganglion::openraft::{BasicNode, Config};
    use ganglion::{
        DialerNetworkFactory, GanglionLogStore, GanglionStateMachine, RaftMetadataNode, WireFormat,
        serve_connection,
    };

    // Tolerant timeouts: each turmoil host shares one current-thread runtime with
    // a full broker, so the default 150-300ms election window is too tight and
    // leadership flaps. Wider windows keep a stable leader under that load.
    let config = Arc::new(
        Config {
            heartbeat_interval: 200,
            election_timeout_min: 1_000,
            election_timeout_max: 2_000,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let node = RaftMetadataNode::start_with_network(
        id,
        config,
        DialerNetworkFactory::with_dialer(TurmoilDialer),
        GanglionLogStore::default(),
        GanglionStateMachine::default(),
    )
    .await
    .expect("raft node starts");

    let raft = node.raft().clone();
    tokio::spawn(async move {
        let listener =
            turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, RAFT_PORT)).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            tokio::spawn(serve_connection(stream, raft.clone(), WireFormat::default()));
        }
        #[allow(unreachable_code)]
        Ok::<_, std::io::Error>(())
    });

    if id == 1 {
        let mut members = BTreeMap::new();
        for (peer, peer_id) in [("a-owner", 1u64), ("b-follower", 2), ("coordinator", 3)] {
            members.insert(peer_id, BasicNode::new(format!("{peer}:{RAFT_PORT}")));
        }
        node.initialize(members)
            .await
            .expect("membership initializes");
    }

    Arc::new(GanglionCoordination::new(name, node))
}

/// The placement controller every host runs. `control_iteration` no-ops on a
/// non-leader, so only the current leader plans, which keeps writes on the leader
/// (the bridge applies leader writes locally over the raft network and never has
/// to forward over the non-simulated client_write path). Registrations are
/// idempotent merges, so re-running them each tick is harmless.
async fn run_split_brain_controller(
    coordination: Arc<GanglionCoordination>,
    queue: QueueIdentity,
    owner_partitioned: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
) {
    let queues = [queue];
    let no_streams: Vec<StreamIdentity> = Vec::new();
    while !done.load(Ordering::SeqCst) {
        if coordination.consensus_node().is_leader().await {
            let snapshot = coordination.snapshot();
            // Register each broker node once (idempotent merge), so the resolver
            // can dial owners. Re-checking the committed snapshot avoids
            // re-writing on every tick.
            if !snapshot.nodes.contains_key("a-owner") {
                coordination
                    .register_self(&split_brain_node_info("a-owner"))
                    .await
                    .ok();
            }
            if !snapshot.nodes.contains_key("b-follower") {
                coordination
                    .register_self(&split_brain_node_info("b-follower"))
                    .await
                    .ok();
            }
            // Only re-plan when the desired owner for the current phase differs
            // from the committed one. Planning every tick would bump the
            // generation on each pass and churn the watchers.
            let want_owner = if owner_partitioned.load(Ordering::SeqCst) {
                "b-follower"
            } else {
                "a-owner"
            };
            let current_owner = snapshot
                .assignments
                .values()
                .next()
                .map(|assignment| assignment.owner.to_string());
            if snapshot.nodes.contains_key("a-owner")
                && snapshot.nodes.contains_key("b-follower")
                && current_owner.as_deref() != Some(want_owner)
            {
                let live = split_brain_live(owner_partitioned.load(Ordering::SeqCst));
                coordination
                    .control_iteration(
                        &DeterministicPartitionPlacement,
                        &queues,
                        &DeterministicStreamPlacement,
                        &no_streams,
                        1,
                        1,
                        ReplicationDurabilityPolicy::LocalDurable,
                        &live,
                        8,
                    )
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The full split-brain scenario over a real distributed raft cluster: a
/// partitioned old owner learns it was demoted and refuses to serve.
///
/// Three ganglion raft nodes run inside turmoil over the injected transport
/// (`a-owner` and `b-follower` each carry a broker, `coordinator` is raft-only for
/// majority). The follower replicates the owner's data, then the orchestrator
/// partitions the owner away from the majority. The majority's controller drops
/// the unreachable owner from the live set and reassigns the queue to the
/// follower under a bumped epoch - a commit the partitioned owner cannot see. The
/// follower promotes and serves. When the partition heals, the old owner's node
/// catches up the raft log, its watcher observes the fenced reassignment, demotes
/// the local queue, and writes on its existing publisher are refused. This is the
/// property static coordination cannot express, because the demotion has to
/// propagate through a real consensus cluster across a partition and heal.
#[test]
fn ganglion_returning_old_owner_is_demoted_under_simulated_partition() {
    let topic = "sim.splitbrain";
    let queue = QueueIdentity::new(topic, Partition::new(0), None);

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(300))
        .build();

    let caught_up = Arc::new(AtomicBool::new(false));
    let owner_partitioned = Arc::new(AtomicBool::new(false));
    let reassigned = Arc::new(AtomicBool::new(false));
    let healed = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));

    // Old owner: publishes the initial data, becomes owner under the fence, then
    // (after the partition heals) must observe its demotion and refuse writes.
    {
        let queue = queue.clone();
        let owner_partitioned = owner_partitioned.clone();
        let healed = healed.clone();
        let done = done.clone();
        sim.client("a-owner", async move {
            let coordination = start_split_brain_node("a-owner", 1).await;
            let (engine, _dir) = open_engine("sb-owner").await;
            let broker = Broker::new(engine, test_broker_config(), None);

            let (publisher, _confirms) = broker
                .get_publisher(topic, Partition::new(0), &None)
                .await
                .unwrap();
            let reply = publisher
                .publish(
                    b"pre-fence".to_vec(),
                    unix_millis(),
                    unix_millis(),
                    None,
                    Default::default(),
                    None,
                )
                .await
                .unwrap();
            reply.await.unwrap().unwrap();

            let serve_broker = broker.clone();
            tokio::spawn(run_server(
                bind_addr(),
                serve_broker,
                TcpStats::new(10),
                ConnectionStats::new(),
                None::<StaticAuthHandler>,
                ConnectionSettings::new(Some(60)),
                None,
                None,
                None,
            ));

            let resolver = Arc::new(CoordinationProtocolOwnerPeerResolver::new(
                coordination.clone(),
            ));
            broker.spawn_assignment_watcher_with_follower_replication(
                coordination.clone(),
                resolver,
                FollowerReplicationWorkerConfig {
                    caught_up_poll_ms: 60_000,
                    ..Default::default()
                },
            );
            tokio::spawn(run_split_brain_controller(
                coordination.clone(),
                queue.clone(),
                owner_partitioned.clone(),
                done.clone(),
            ));

            // Become owner under the fenced epoch (the watcher applies BecomeOwner
            // and advances the replication epoch to the assignment's).
            tokio::time::timeout(Duration::from_secs(60), async {
                loop {
                    if let Ok(checkpoint) = broker
                        .export_owner_state_checkpoint(topic, Partition::new(0), None)
                        .await
                    {
                        if checkpoint.message_epoch >= 1 {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("old owner observes initial ownership under the fence");

            // Stay quiet through the catch-up and partition phases. A continuous
            // publish load here would starve this single-threaded host's raft
            // heartbeats and replication serving (the follower could never catch
            // up). Only once the partition has healed do we probe for the
            // demotion this node should now observe.
            tokio::time::timeout(Duration::from_secs(180), async {
                while !healed.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("partition heals");

            // After the partition moves ownership and the cluster heals, this
            // node catches up the raft log, its watcher demotes the local queue,
            // and writes on the existing publisher start failing.
            tokio::time::timeout(Duration::from_secs(180), async {
                loop {
                    let refused = match publisher
                        .publish(
                            b"stale-after-fence".to_vec(),
                            unix_millis(),
                            unix_millis(),
                            None,
                            Default::default(),
                            None,
                        )
                        .await
                    {
                        Ok(reply) => reply.await.map(|inner| inner.is_err()).unwrap_or(true),
                        Err(_) => true,
                    };
                    if refused {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("demoted old owner must refuse writes on its existing publisher");

            done.store(true, Ordering::SeqCst);
            Ok(())
        });
    }

    // New owner: replicates from the old owner, then takes over after the
    // partition and serves owner traffic.
    {
        let queue = queue.clone();
        let owner_partitioned = owner_partitioned.clone();
        let caught_up = caught_up.clone();
        let reassigned = reassigned.clone();
        let done = done.clone();
        sim.client("b-follower", async move {
            let coordination = start_split_brain_node("b-follower", 2).await;
            let (engine, _dir) = open_engine("sb-follower").await;
            let broker = Broker::new(engine, test_broker_config(), None);

            let serve_broker = broker.clone();
            tokio::spawn(run_server(
                bind_addr(),
                serve_broker,
                TcpStats::new(10),
                ConnectionStats::new(),
                None::<StaticAuthHandler>,
                ConnectionSettings::new(Some(60)),
                None,
                None,
                None,
            ));

            let resolver = Arc::new(CoordinationProtocolOwnerPeerResolver::new(
                coordination.clone(),
            ));
            broker.spawn_assignment_watcher_with_follower_replication(
                coordination.clone(),
                resolver,
                FollowerReplicationWorkerConfig {
                    caught_up_poll_ms: 60_000,
                    ..Default::default()
                },
            );
            tokio::spawn(run_split_brain_controller(
                coordination.clone(),
                queue.clone(),
                owner_partitioned.clone(),
                done.clone(),
            ));

            tokio::time::timeout(Duration::from_secs(60), async {
                loop {
                    let state = broker
                        .follower_replication_worker_snapshot(topic, Partition::new(0), None)
                        .await;
                    if state
                        .as_ref()
                        .is_some_and(|s| s.status == FollowerReplicationWorkerStatus::CaughtUp)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("follower catches up before the partition");
            caught_up.store(true, Ordering::SeqCst);

            // After the controller reassigns ownership, the watcher promotes this
            // node and it serves a fresh publish as the new owner.
            tokio::time::timeout(Duration::from_secs(120), async {
                loop {
                    if QueueOwnership::owns_queue(
                        coordination.as_ref(),
                        topic,
                        Partition::new(0),
                        None,
                    ) {
                        if let Ok((publisher, _confirms)) = broker
                            .get_publisher(topic, Partition::new(0), &None)
                            .await
                        {
                            if let Ok(reply) = publisher
                                .publish(
                                    b"post-failover".to_vec(),
                                    unix_millis(),
                                    unix_millis(),
                                    None,
                                    Default::default(),
                                    None,
                                )
                                .await
                            {
                                if reply.await.unwrap().is_ok() {
                                    break;
                                }
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("follower is promoted and serves owner traffic");
            reassigned.store(true, Ordering::SeqCst);

            // Keep serving the raft cluster until the old owner has healed and
            // observed its demotion.
            tokio::time::timeout(Duration::from_secs(180), async {
                while !done.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .ok();
            Ok(())
        });
    }

    // Coordinator: raft-only, provides the third vote so the majority survives the
    // owner's partition.
    {
        let queue = queue.clone();
        let owner_partitioned = owner_partitioned.clone();
        let done = done.clone();
        sim.client("coordinator", async move {
            let coordination = start_split_brain_node("coordinator", 3).await;
            tokio::spawn(run_split_brain_controller(
                coordination.clone(),
                queue.clone(),
                owner_partitioned.clone(),
                done.clone(),
            ));
            tokio::time::timeout(Duration::from_secs(240), async {
                while !done.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .ok();
            Ok(())
        });
    }

    // Orchestrator: partition the owner once the follower is caught up, heal once
    // ownership has moved, and run until the old owner has observed its demotion.
    let mut partitioned = false;
    let mut healed_partition = false;
    loop {
        let finished = sim.step().expect("simulation step");
        if caught_up.load(Ordering::SeqCst) && !partitioned {
            sim.partition("a-owner", "b-follower");
            sim.partition("a-owner", "coordinator");
            owner_partitioned.store(true, Ordering::SeqCst);
            partitioned = true;
        }
        if partitioned && reassigned.load(Ordering::SeqCst) && !healed_partition {
            sim.repair("a-owner", "b-follower");
            sim.repair("a-owner", "coordinator");
            healed.store(true, Ordering::SeqCst);
            healed_partition = true;
        }
        if finished {
            break;
        }
    }
    assert!(done.load(Ordering::SeqCst), "scenario runs to completion");
}

/// Raft coordination converges despite a lossy, latent, message-dropping network.
///
/// Three ganglion raft nodes run inside turmoil over the injected transport with
/// message loss, link flapping, and variable latency applied to every link. The
/// cluster must still elect a leader and commit a write that replicates to all
/// three state machines. Whichever node currently holds leadership retries the
/// write, so the commit survives the re-elections the loss induces. A fixed RNG
/// seed makes the fault schedule reproducible.
#[test]
fn ganglion_raft_cluster_converges_under_message_loss() {
    use ganglion::CoordinationSnapshot;

    // Host names match the membership addresses baked into start_split_brain_node.
    let names: [&str; 3] = ["a-owner", "b-follower", "coordinator"];
    let ids: [u64; 3] = [1, 2, 3];

    // Loss and latency stay well under the raft timers (heartbeat 200ms, election
    // 1000-2000ms) so a majority can stay connected long enough to make progress -
    // the point is resilience to a flapping network, not a total outage.
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(600))
        .min_message_latency(Duration::from_millis(5))
        .max_message_latency(Duration::from_millis(50))
        .fail_rate(0.01)
        .repair_rate(0.95)
        .rng_seed(7)
        .build();

    let observed = Arc::new(AtomicUsize::new(0));

    for (name, id) in names.iter().zip(ids) {
        let host: &'static str = name;
        let observed = observed.clone();
        sim.client(host, async move {
            let coordination = start_split_brain_node(host, id).await;

            // Converge a committed write under the loss. Whoever is leader at the
            // moment proposes it; the proposal is retried across the re-elections
            // the loss causes, and every node breaks once it observes the commit.
            let mut committed = coordination.consensus_node().watch_committed();
            tokio::time::timeout(Duration::from_secs(300), async {
                loop {
                    if committed.borrow_and_update().generation >= 1 {
                        break;
                    }
                    if coordination.consensus_node().is_leader().await {
                        let _ = coordination
                            .consensus_node()
                            .write_snapshot(CoordinationSnapshot {
                                generation: 1,
                                ..CoordinationSnapshot::default()
                            })
                            .await;
                    }
                    let _ =
                        tokio::time::timeout(Duration::from_millis(200), committed.changed()).await;
                }
            })
            .await
            .expect("cluster commits and replicates a write under message loss");
            assert!(coordination.consensus_node().committed_snapshot().generation >= 1);

            observed.fetch_add(1, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(120), async {
                while observed.load(Ordering::SeqCst) < names.len() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("all nodes observe the committed write");

            Ok(())
        });
    }

    sim.run().expect("simulation runs to completion");
}
