#![cfg(feature = "io-fault-injection")]
use fibril_broker::{
    Partition,
    broker::{Broker, BrokerConfig},
    coordination::Coordination,
    queue_engine::{KeratinConfig, QueueEngine, SnapshotConfig, StromaEngine, StromaKeratinConfig},
};
use fibril_coordination_ganglion::{
    GanglionCoordination,
    history_identity::resource_incarnation,
    promotion::{PendingRecovery, pending_recovery_key},
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_protocol::v1::{
    handler::{ConnectionSettings, handle_connection},
    replication::ProtocolOwnerPeerResolverConfig,
};
use fibril_util::{StaticAuthHandler, net::TcpListener, unix_millis};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

async fn serve_test_broker(
    broker: Arc<Broker<StromaEngine>>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (socket, peer) = listener.accept().await.unwrap();
            let stats = ConnectionStats::new();
            let id = stats.add_connection(peer, Instant::now(), false);
            let broker = broker.clone();
            connections.spawn(async move {
                let _ = handle_connection(
                    socket,
                    Some(peer),
                    broker,
                    TcpStats::new(10),
                    stats,
                    id,
                    Some(StaticAuthHandler::new("@node".into(), "secret".into())),
                    None,
                    ConnectionSettings::new(Some(60)),
                    None,
                    None,
                    None,
                )
                .await;
            });
        }
    });
    (address, task)
}

fn early_config() -> BrokerConfig {
    BrokerConfig {
        replication_early: true,
        ..BrokerConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn admitted_cluster_owner_io_failure_recovery_and_rejoin() {
    fibril_util::init_tracing();
    for all in [false, true] {
        for after_sync in [false, true] {
            tokio::time::timeout(Duration::from_secs(180), run_case(all, after_sync, false))
                .await
                .expect("cluster case deadline");
        }
    }
}

/// A buffered owner confirms once followers hold written copies, so a later
/// failed sync on the owner loses nothing confirmed. The owner latches, the
/// queue fails over, and the old owner rejoins after cutting its log back.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn buffered_owner_sync_failure_keeps_confirmed_messages() {
    for after_sync in [false, true] {
        tokio::time::timeout(Duration::from_secs(180), run_case(false, after_sync, true))
            .await
            .expect("cluster case deadline");
    }
}

async fn run_case(all: bool, after_sync: bool, buffered: bool) {
    eprintln!("CASE all={all} after_sync={after_sync} buffered={buffered}");
    use fibril_broker::coordination::{NodeInfo, QueueIdentity};
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test_data/cluster-io")
        .join(format!(
            "fibril-three-node-enrollment-{}",
            uuid::Uuid::new_v4()
        ));
    let mut nodes = Vec::new();
    let mut servers = Vec::new();
    for id in 1..=3 {
        let (node, server) = ganglion::RaftMetadataNode::start_durable_tcp(
            id,
            ganglion::default_raft_config().unwrap(),
            "127.0.0.1:0",
            root.join(format!("metadata-{id}")),
        )
        .await
        .unwrap();
        nodes.push(node);
        servers.push(server);
    }
    nodes[0]
        .initialize(
            servers
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    (
                        i as u64 + 1,
                        ganglion::openraft::BasicNode::new(s.local_addr().to_string()),
                    )
                })
                .collect(),
        )
        .await
        .unwrap();
    let leader = nodes[0]
        .wait_for_any_leader(Duration::from_secs(10))
        .await
        .unwrap();
    let mut providers: Vec<_> = nodes
        .into_iter()
        .zip(["a", "b", "c"])
        .map(|(node, id)| Arc::new(GanglionCoordination::new(id, node)))
        .collect();
    let mut brokers = Vec::new();
    let mut listeners = Vec::new();
    let mut addresses = HashMap::new();
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        let engine = StromaEngine::open(
            root.join(format!("data-{id}")).join("storage"),
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let broker =
            Broker::new_with_ownership(engine, early_config(), None, providers[i].clone());
        let (address, listener) = serve_test_broker(broker.clone()).await;
        providers[i]
            .register_self(&NodeInfo {
                node_id: id.to_string(),
                broker_addr: address.clone(),
                admin_addr: None,
            })
            .await
            .unwrap();
        addresses.insert(id.to_string(), address);
        brokers.push(broker);
        listeners.push(listener);
    }
    let coordinator = &providers[leader as usize - 1];
    if buffered {
        coordinator
            .declare_queue_declared(
                "q",
                None,
                1,
                fibril_broker::queue_engine::QueueDeclaration::new(
                    Default::default(),
                    Some(fibril_broker::queue_engine::QueueDurability::Buffered),
                ),
            )
            .await
            .unwrap();
    }
    coordinator
        .register_queue(&QueueIdentity::new("q", Partition::new(0), None))
        .await
        .unwrap();
    let resource = ganglion_core::ResourceIdentity::new("fibril/queue", "q", 0, None::<String>);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for coordinator in &providers {
                if !coordinator.consensus_node().is_leader().await {
                    continue;
                }
                let mut state = coordinator.consensus_node().committed_snapshot();
                if !state.resources.contains(&resource) {
                    continue;
                }
                let generation = state.generation;
                let mut assignment = ganglion_core::PartitionAssignment::new(
                    resource.clone(),
                    "a",
                    vec!["b".into(), "c".into()],
                    1,
                );
                assignment.durability = if all {
                    ganglion_core::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 3 }
                } else {
                    ganglion_core::ReplicationDurabilityPolicy::MajorityDurable
                };
                state.assignments.insert(resource.clone(), assignment);
                state.generation += 1;
                match coordinator
                    .consensus_node()
                    .write_snapshot_guarded(generation, state)
                    .await
                {
                    Ok(_) => return,
                    Err(
                        ganglion::OpenraftAdapterError::NotLeader
                        | ganglion::OpenraftAdapterError::GenerationMismatch { .. },
                    ) => {}
                    Err(e) => panic!("assignment setup failed: {e}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut config = ProtocolOwnerPeerResolverConfig::new(addresses).with_auth("@node", "secret");
    let owner_worker =
        fibril::recovery_driver::spawn(providers[0].clone(), brokers[0].clone(), config.clone());
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            if providers.iter().all(|p| {
                p.local_initial_history_admissions()
                    .is_ok_and(|a| a.len() == 1)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for i in 0..3 {
        let decision = providers[i].local_initial_history_admissions().unwrap()[0]
            .0
            .clone();
        providers[i]
            .admit_local_initial_history(&decision, &brokers[i].engine())
            .await
            .unwrap();
        for result in brokers[i]
            .apply_assignment_snapshot_transitions(
                ["a", "b", "c"][i],
                &fibril_broker::coordination::CoordinationSnapshot::default(),
                &providers[i].snapshot(),
            )
            .await
        {
            result.unwrap();
        }
    }
    for i in 0..3 {
        brokers[i].spawn_assignment_watcher(providers[i].clone());
    }
    if buffered {
        for broker in &brokers {
            assert!(broker.engine().queue_is_buffered("q", 0, None));
        }
        // Syncs happen only when the test asks for one.
        brokers[0]
            .engine()
            .set_queue_background_sync_interval("q", 0, None, Duration::ZERO)
            .await
            .unwrap();
    }
    owner_worker.abort();
    let _ = owner_worker.await;
    let assignment = providers[0]
        .snapshot()
        .assignment_for("q", Partition::new(0), None)
        .unwrap()
        .clone();
    assert_eq!(assignment.history.as_ref().unwrap().replicas.len(), 3);
    let mut peers = Vec::new();
    for id in ["b", "c"] {
        peers.push(
            fibril_protocol::v1::replication::connect_protocol_owner_peer(
                config.nodes["a"].clone(),
                config.auth.as_ref(),
                None,
                "fault-test",
                "1",
            )
            .await
            .unwrap()
            .with_reporter(id)
            .with_history_session(
                assignment
                    .history
                    .as_ref()
                    .unwrap()
                    .session("q", Partition::new(0), None, false, id, "a")
                    .unwrap(),
            ),
        );
    }
    let publisher = brokers[0]
        .get_publisher("q", Partition::new(0), &None)
        .await
        .unwrap();
    const PREFIX: u64 = 8;
    // A declared queue starts with its Declare record.
    let base = brokers[0]
        .engine()
        .queue_log_sync_status("q", 0, None)
        .await
        .unwrap()
        .event_log
        .written_next;
    for n in 0..PREFIX {
        let receipt = publisher
            .publish(
                n.to_le_bytes().to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        for i in 1..3 {
            catch_up(&brokers[i], &peers[i - 1], base + n + 1).await;
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), receipt)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            base + n
        );
    }
    let path = segment_path(&root.join("data-a").join("storage"), "log");
    let fault_kind = if after_sync {
        keratin_log::io_fault_injection::Fault::FsyncAfterSuccess
    } else {
        keratin_log::io_fault_injection::Fault::Fsync
    };
    if buffered {
        buffered_sync_failure(&brokers, &peers, &publisher, &path, fault_kind, base, PREFIX).await;
    } else {
    let fault = keratin_log::io_fault_injection::arm_gated(&path, fault_kind).unwrap();
    let mut failed = publisher
        .publish(
            PREFIX.to_le_bytes().to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while fault.hits() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    for i in 1..3 {
        catch_up(&brokers[i], &peers[i - 1], base + PREFIX + 1).await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut failed)
            .await
            .is_err(),
        "followers cannot replace owner durability"
    );
    // Ordinary reads retain the owner durable frontier. The quiescent frontier
    // API intentionally waits for active publishes, so do not call it while
    // holding this publish's fsync.
    let engine = brokers[0].engine();
    let visible = engine
        .read_owner_queue_records("q", 0, None, base + PREFIX, 1, usize::MAX)
        .await
        .map(|r| match r {
            fibril_broker::queue_engine::OwnerReplicationRead::Batch(b) => b.records.len(),
            _ => panic!("expected the queue log"),
        })
        .unwrap();
    assert_eq!(visible, 0);
    drop(engine);
    fault.release();
    let result = tokio::time::timeout(Duration::from_secs(10), &mut failed)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err(), "false broker confirmation: {result:?}");
    eprintln!("FAULT all={all} after_sync={after_sync}: {result:?}");
    drop(fault);
    }
    drop(publisher);
    drop(peers);
    eprintln!("STAGE stopping old owner");
    listeners[0].abort();
    let _ = (&mut listeners[0]).await;
    tokio::time::timeout(Duration::from_secs(10), brokers[0].shutdown())
        .await
        .expect("old owner shutdown deadline");
    eprintln!("STAGE old owner stopped");
    // Keep metadata quorum alive. This stage isolates data-owner loss, rather
    // than also changing the metadata leader while diagnosing storage failures.
    let stopped = brokers.remove(0);
    drop(stopped);
    let previous = providers[0].consensus_node().committed_snapshot();
    let prior = previous.assignments[&resource].clone();
    let mut proposed = prior.clone();
    proposed.owner = "b".into();
    proposed.followers = vec!["c".into(), "a".into()];
    proposed.epoch += 1;
    let pending = PendingRecovery {
        version: 1,
        resource_incarnation: resource_incarnation(&previous, &resource).unwrap(),
        previous_activation: Some(assignment.history.as_ref().unwrap().activation),
        requested_generation: previous.generation + 1,
        previous: prior,
        proposed,
        previous_write_nodes: if all { 3 } else { 2 },
        proposed_write_nodes: if all { 3 } else { 2 },
        required_old_witnesses: if all { 1 } else { 2 },
    };
    // This is the same immutable transition the controller requests for owner
    // loss. Preserve all-copy policy and the original witness threshold.
    let mut snapshot = previous;
    let generation = snapshot.generation;
    snapshot.generation += 1;
    snapshot.attributes.insert(
        pending_recovery_key(&resource),
        serde_json::to_string(&pending).unwrap(),
    );
    let leader = providers[0]
        .consensus_node()
        .wait_for_any_leader(Duration::from_secs(10))
        .await
        .unwrap();
    providers[leader as usize - 1]
        .consensus_node()
        .write_snapshot_guarded(generation, snapshot)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while providers[1]
            .consensus_node()
            .committed_snapshot()
            .generation
            <= generation
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("candidate must observe the committed recovery request");
    let mut workers = Vec::new();
    for i in 1..3 {
        workers.push(fibril::recovery_driver::spawn(
            providers[i].clone(),
            brokers[i - 1].clone(),
            config.clone(),
        ));
    }
    eprintln!("STAGE recovery requested");
    if all {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    !providers[1].pending_recoveries().unwrap().is_empty(),
                    "all-copy policy activated without its third replica"
                );
                let observations = providers[1].recovery_diagnostics().snapshot();
                if observations.attempts.iter().any(|a| {
                    a.epoch == "2"
                        && a.outcome
                            == fibril_coordination_ganglion::recovery_diagnostics::Outcome::Error
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("recovery must attempt and fail while its required third copy is unavailable");
        assert!(
            !providers[1].pending_recoveries().unwrap().is_empty(),
            "all-copy policy cannot activate only two replicas"
        );
    } else {
        wait_recovered(&providers[1..], &brokers, 2).await;
    }
    // New data-owner process identity, same disk and metadata node. Rejoin must
    // use current recovery/admission authority instead of its stale owner role.
    providers[0].consensus_node().shutdown().await.unwrap();
    let metadata_address = servers[0].local_addr();
    servers.remove(0).shutdown();
    let (node, server) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match ganglion::RaftMetadataNode::start_durable_tcp(
                1,
                ganglion::default_raft_config().unwrap(),
                metadata_address,
                root.join("metadata-1"),
            )
            .await
            {
                Ok(pair) => break pair,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap();
    servers.insert(0, server);
    node.wait_for_any_leader(Duration::from_secs(10))
        .await
        .unwrap();
    eprintln!("STAGE metadata reopened");
    let restarted = Arc::new(GanglionCoordination::new("a", node));
    let engine = StromaEngine::open(
        root.join("data-a").join("storage"),
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker =
        Broker::new_with_ownership(engine, early_config(), None, restarted.clone());
    let (address, listener) = serve_test_broker(broker.clone()).await;
    config.nodes.insert("a".into(), address.clone());
    restarted
        .register_self(&NodeInfo {
            node_id: "a".into(),
            broker_addr: address,
            admin_addr: None,
        })
        .await
        .unwrap();
    providers[0] = restarted;
    brokers.insert(0, broker);
    listeners[0] = listener;
    brokers[0].spawn_assignment_watcher(providers[0].clone());
    workers.push(fibril::recovery_driver::spawn(
        providers[0].clone(),
        brokers[0].clone(),
        config.clone(),
    ));
    eprintln!("STAGE waiting rejoin");
    wait_recovered(&providers, &brokers, 3).await;
    let current = providers[1]
        .snapshot()
        .assignment_for("q", Partition::new(0), None)
        .unwrap()
        .clone();
    assert_eq!(current.owner, "b");
    assert_eq!(
        current.durability,
        if all {
            fibril_broker::coordination::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 3 }
        } else {
            fibril_broker::coordination::ReplicationDurabilityPolicy::MajorityDurable
        }
    );
    assert_eq!(current.history.as_ref().unwrap().replicas.len(), 3);
    for i in 0..3 {
        for result in brokers[i]
            .apply_assignment_snapshot_transitions(
                ["a", "b", "c"][i],
                &fibril_broker::coordination::CoordinationSnapshot::default(),
                &providers[i].snapshot(),
            )
            .await
        {
            result.unwrap();
        }
    }
    let mut new_peers = Vec::new();
    for id in ["a", "c"] {
        new_peers.push(
            fibril_protocol::v1::replication::connect_protocol_owner_peer(
                config.nodes["b"].clone(),
                config.auth.as_ref(),
                None,
                "fault-test",
                "1",
            )
            .await
            .unwrap()
            .with_reporter(id)
            .with_history_session(
                current
                    .history
                    .as_ref()
                    .unwrap()
                    .session("q", Partition::new(0), None, false, id, "b")
                    .unwrap(),
            ),
        );
    }
    let publisher = brokers[1]
        .get_publisher("q", Partition::new(0), &None)
        .await
        .unwrap();
    let next = brokers[1]
        .engine()
        .queue_durable_commit("q", 0, None)
        .await
        .unwrap()
        .next;
    let confirmation = publisher
        .publish(
            99u64.to_le_bytes().to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    for (i, peer) in [0, 2].into_iter().zip(&new_peers) {
        catch_up(&brokers[i], peer, next + 1).await;
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), confirmation)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        next
    );
    let delivered = brokers[1]
        .engine()
        .poll_ready("q", 0, None, 100, unix_millis() + 60_000, u64::MAX)
        .await
        .unwrap();
    let values: Vec<_> = delivered
        .iter()
        .map(|m| u64::from_le_bytes(m.payload[..8].try_into().unwrap()))
        .collect();
    for n in 0..PREFIX {
        assert_eq!(
            values.iter().filter(|v| **v == n).count(),
            1,
            "confirmed prefix {n}: {values:?}"
        );
    }
    if buffered {
        assert_eq!(
            values.iter().filter(|v| **v == PREFIX).count(),
            1,
            "confirmed before the failed sync: {values:?}"
        );
    }
    assert_eq!(values.iter().filter(|v| **v == 99).count(), 1);
    eprintln!("RECOVERED all={all} after_sync={after_sync}: {values:?}");
    drop(publisher);
    drop(new_peers);
    for w in workers {
        w.abort();
        let _ = w.await;
    }
    for l in listeners {
        l.abort();
        let _ = l.await;
    }
    for b in &brokers {
        b.shutdown().await;
    }
    for p in &providers {
        p.consensus_node().shutdown().await.unwrap();
    }
    for s in servers {
        s.shutdown();
    }
    drop(brokers);
    drop(providers);
    std::fs::remove_dir_all(root).unwrap();
}

/// Confirm a buffered publish while the owner has not synced it, then fail
/// the owner's next sync. The owner latches and refuses further publishes.
async fn buffered_sync_failure(
    brokers: &[Arc<Broker<StromaEngine>>],
    peers: &[impl fibril_broker::broker::BrokerOwnerReplicationPeer],
    publisher: &fibril_broker::broker::PublisherHandle,
    path: &std::path::Path,
    fault_kind: keratin_log::io_fault_injection::Fault,
    base: u64,
    prefix: u64,
) {
    let fault = keratin_log::io_fault_injection::arm(path, fault_kind).unwrap();
    let receipt = publisher
        .publish(
            prefix.to_le_bytes().to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    for i in 1..3 {
        catch_up(&brokers[i], &peers[i - 1], base + prefix + 1).await;
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), receipt)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        base + prefix,
        "a buffered publish confirms before the owner syncs it"
    );
    assert_eq!(fault.hits(), 0);
    let engine = brokers[0].engine();
    let status = engine.queue_log_sync_status("q", 0, None).await.unwrap();
    assert!(status.event_log.durable_next <= base + prefix, "{status:?}");
    // Turning the background sync on syncs what the log has written first.
    let _ = engine
        .set_queue_background_sync_interval("q", 0, None, Duration::from_millis(10))
        .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while fault.hits() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(fault);
    let refused = async {
        let receipt = publisher
            .publish(
                (prefix + 1).to_le_bytes().to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await?;
        receipt.await.map_err(|_| fibril_broker::broker::BrokerError::ChannelClosed)?
    };
    let result = tokio::time::timeout(Duration::from_secs(10), refused).await.unwrap();
    assert!(result.is_err(), "a latched owner confirmed a publish: {result:?}");
    eprintln!("BUFFERED FAULT after_sync: {result:?}");
}

fn segment_path(root: &std::path::Path, log: &str) -> std::path::PathBuf {
    fn walk(path: &std::path::Path, log: &str, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, log, out);
            } else if path.file_name().unwrap() == "00000000000000000000.log"
                && path.components().any(|c| c.as_os_str() == log)
            {
                out.push(path);
            }
        }
    }
    let mut paths = Vec::new();
    walk(root, log, &mut paths);
    assert_eq!(paths.len(), 1, "{paths:?}");
    paths.pop().unwrap()
}

async fn catch_up(
    broker: &Arc<Broker<StromaEngine>>,
    peer: &impl fibril_broker::broker::BrokerOwnerReplicationPeer,
    target: u64,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let from = broker
                .engine()
                .queue_replication_next_offset("q", 0, None)
                .await
                .unwrap();
            if from >= target {
                break;
            }
            broker
                .catch_up_queue_follower(
                    peer,
                    "q",
                    Partition::new(0),
                    None,
                    fibril_broker::broker::QueueCatchUpOptions {
                        from,
                        max_records_per_read: 128,
                        max_bytes_per_read: 1024 * 1024,
                        max_iterations: 16,
                        max_wait_ms: 0,
                    },
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_recovered(
    providers: &[Arc<GanglionCoordination>],
    brokers: &[Arc<Broker<StromaEngine>>],
    admitted: usize,
) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if providers.iter().zip(brokers).all(|(p, b)| {
                if !p.pending_recoveries().is_ok_and(|v| v.is_empty()) {
                    return false;
                }
                let snapshot = p.snapshot();
                let Some(a) = snapshot.assignment_for("q", Partition::new(0), None) else {
                    return false;
                };
                let Some(h) = &a.history else {
                    return false;
                };
                if a.owner != "b" || h.replicas.len() < admitted || !h.permits_role(p.node_id()) {
                    return false;
                }
                let Some(replica) = h.replicas.get(p.node_id()) else {
                    return false;
                };
                b.engine()
                    .verify_admitted_storage_history(
                        &fibril_broker::queue_engine::PreparedStorageHistory {
                            topic: "q".into(),
                            partition: 0,
                            group: None,
                            stream: false,
                            binding: h.binding.clone(),
                            storage_instance: replica.storage,
                        },
                    )
                    .is_ok()
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        for p in providers {
            eprintln!(
                "DIAGNOSTICS {}",
                serde_json::to_string(&p.recovery_diagnostics().snapshot()).unwrap()
            );
        }
        panic!("recovery/admission deadline");
    });
}
