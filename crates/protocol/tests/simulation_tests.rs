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
};
use fibril_broker::coordination::{
    CoordinationSnapshot, NodeInfo, PartitionAssignment, QueueIdentity, StaticCoordination,
};
use fibril_broker::queue_engine::StromaEngine;
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

/// A follower broker, driven only by its supervised assignment watcher, catches
/// up to the owner over the simulated network, all on the simulated clock.
#[test]
fn follower_catches_up_over_simulated_network() {
    let topic = "sim.catchup";
    let payloads: &[&[u8]] = &[b"first", b"second"];

    let mut sim = turmoil::Builder::new().build();
    spawn_owner_host(&mut sim, topic, payloads);

    sim.client("b-follower", async move {
        let (engine, _dir) = open_engine("follower").await;
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
        .expect("follower catches up over the simulated network");

        resolver.close_all().await;
        broker.shutdown().await;
        Ok(())
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
