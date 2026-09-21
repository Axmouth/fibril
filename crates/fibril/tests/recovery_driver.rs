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
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

async fn serve_test_broker(
    broker: Arc<Broker<StromaEngine>>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        loop {
            let (socket, peer) = listener.accept().await.unwrap();
            let stats = ConnectionStats::new();
            let id = stats.add_connection(peer, Instant::now(), false);
            let broker = broker.clone();
            tokio::spawn(async move {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_queue_survives_repeated_broker_and_metadata_restarts() {
    use fibril_broker::coordination::{
        DeterministicPartitionPlacement, DeterministicStreamPlacement, NodeInfo, QueueIdentity,
        ReplicationDurabilityPolicy,
    };
    let root = std::env::temp_dir().join(format!(
        "fibril-enrollment-restarts-{}",
        uuid::Uuid::new_v4()
    ));
    let resource = ganglion_core::ResourceIdentity::new("fibril/queue", "q", 0, None::<String>);
    for cycle in 0..3u64 {
        let (node, server) = ganglion::RaftMetadataNode::start_durable_tcp(
            1,
            ganglion::default_raft_config().unwrap(),
            "127.0.0.1:0",
            root.join("metadata"),
        )
        .await
        .unwrap();
        if cycle == 0 {
            node.initialize(BTreeMap::from([(
                1,
                ganglion::openraft::BasicNode::new(server.local_addr().to_string()),
            )]))
            .await
            .unwrap();
        }
        node.wait_for_any_leader(Duration::from_secs(10))
            .await
            .unwrap();
        let provider = Arc::new(GanglionCoordination::new("a", node));
        let engine = StromaEngine::open(
            root.join("data"),
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let broker = Broker::new_with_ownership(
            engine.clone(),
            BrokerConfig::default(),
            None,
            provider.clone(),
        );
        let (address, listener) = serve_test_broker(broker.clone()).await;
        let local = NodeInfo {
            node_id: "a".into(),
            broker_addr: address.clone(),
            admin_addr: None,
        };
        provider.register_self(&local).await.unwrap();
        provider
            .register_queue(&QueueIdentity::new("q", Partition::new(0), None))
            .await
            .unwrap();
        provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                0,
                1,
                ReplicationDurabilityPolicy::LocalDurable,
                &HashMap::from([("a".into(), local)]),
                8,
            )
            .await
            .unwrap();
        if cycle == 0 {
            assert_eq!(
                resource_incarnation(&provider.consensus_node().committed_snapshot(), &resource)
                    .unwrap()
                    .unwrap()
                    .version,
                2
            );
            assert!(provider.initial_queue_work().unwrap().contains(&resource));
        } else {
            assert_eq!(provider.pending_recoveries().unwrap().len(), 1);
            assert!(provider.initial_queue_work().unwrap().is_empty());
        }
        let config = ProtocolOwnerPeerResolverConfig::new(HashMap::from([("a".into(), address)]))
            .with_auth("@node", "secret");
        let worker = fibril::recovery_driver::spawn(provider.clone(), broker.clone(), config);
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let admitted = if cycle == 0 {
                    provider
                        .local_initial_history_admissions()
                        .unwrap()
                        .iter()
                        .any(|(_, p)| engine.verify_admitted_storage_history(p).is_ok())
                } else {
                    provider
                        .local_queue_recovery_admissions()
                        .unwrap()
                        .iter()
                        .any(|(_, p)| engine.verify_admitted_storage_history(&p.storage).is_ok())
                };
                if admitted
                    && provider.pending_recoveries().unwrap().is_empty()
                    && provider
                        .snapshot()
                        .assignment_for("q", Partition::new(0), None)
                        .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let publisher = broker
            .get_publisher("q", Partition::new(0), &None)
            .await
            .unwrap();
        let confirm = publisher
            .publish(
                vec![cycle as u8],
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), confirm)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            cycle
        );
        drop(publisher);
        if cycle == 2 {
            let messages = engine
                .poll_ready("q", 0, None, 10, unix_millis() + 60_000, u64::MAX)
                .await
                .unwrap();
            assert_eq!(
                messages
                    .iter()
                    .map(|m| m.payload.clone())
                    .collect::<Vec<_>>(),
                vec![vec![0], vec![1], vec![2]]
            );
        }
        worker.abort();
        let _ = worker.await;
        broker.shutdown().await;
        listener.abort();
        let _ = listener.await;
        provider.consensus_node().shutdown().await.unwrap();
        server.shutdown();
        drop(broker);
        drop(engine);
        drop(provider);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_recovery_repeats_and_preserves_confirmed_messages() {
    let root =
        std::env::temp_dir().join(format!("fibril-recovery-driver-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let (node, server) = ganglion::RaftMetadataNode::start_durable_tcp(
        1,
        ganglion::default_raft_config().unwrap(),
        "127.0.0.1:0",
        root.join("metadata"),
    )
    .await
    .unwrap();
    node.initialize(BTreeMap::from([(
        1,
        ganglion::openraft::BasicNode::new(server.local_addr().to_string()),
    )]))
    .await
    .unwrap();
    node.wait_for_any_leader(Duration::from_secs(10))
        .await
        .unwrap();
    let provider = Arc::new(GanglionCoordination::new("a", node));
    let resource = ganglion_core::ResourceIdentity::new("fibril/queue", "q", 0, None::<String>);
    provider
        .register_queue(&fibril_broker::coordination::QueueIdentity::new(
            "q",
            Partition::new(0),
            None,
        ))
        .await
        .unwrap();
    let mut snapshot = provider.consensus_node().committed_snapshot();
    let generation = snapshot.generation;
    snapshot.assignments.insert(
        resource.clone(),
        ganglion_core::PartitionAssignment::new(resource.clone(), "a", vec![], 1),
    );
    snapshot.generation += 1;
    provider
        .consensus_node()
        .write_snapshot_guarded(generation, snapshot)
        .await
        .unwrap();
    let engine = StromaEngine::open(
        root.join("data"),
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new_with_ownership(
        engine.clone(),
        BrokerConfig::default(),
        None,
        provider.clone(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
        "a".into(),
        listener.local_addr().unwrap().to_string(),
    )]))
    .with_auth("@node", "secret");
    let mut registration = provider.consensus_node().committed_snapshot();
    let generation = registration.generation;
    registration.nodes.insert(
        "a".into(),
        ganglion_core::NodeInfo::new("a", config.nodes["a"].clone(), None::<String>),
    );
    registration.generation += 1;
    provider
        .consensus_node()
        .write_snapshot_guarded(generation, registration)
        .await
        .unwrap();
    let serving = broker.clone();
    let listener_task = tokio::spawn(async move {
        loop {
            let (socket, peer) = listener.accept().await.unwrap();
            let stats = ConnectionStats::new();
            let id = stats.add_connection(peer, Instant::now(), false);
            let broker = serving.clone();
            tokio::spawn(async move {
                handle_connection(
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
                .await
                .unwrap();
            });
        }
    });
    let worker = fibril::recovery_driver::spawn(provider.clone(), broker.clone(), config.clone());
    // Each cycle starts with a confirmed write, performs recovery through real
    // authenticated TCP, and writes again through the broker's existing cache.
    for cycle in 0..3u64 {
        tokio::time::timeout(Duration::from_secs(10), async {
            while provider
                .snapshot()
                .assignment_for("q", Partition::new(0), None)
                .is_none()
                || (cycle == 0
                    && !provider
                        .local_initial_history_admissions()
                        .unwrap()
                        .iter()
                        .any(|(_, prepared)| {
                            engine.verify_admitted_storage_history(prepared).is_ok()
                        }))
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let publisher = broker
            .get_publisher("q", Partition::new(0), &None)
            .await
            .unwrap();
        let confirmed = publisher
            .publish(
                vec![cycle as u8],
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), confirmed)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            cycle
        );
        if cycle == 2 {
            break;
        }
        let previous = provider.consensus_node().committed_snapshot();
        let assignment = previous.assignments[&resource].clone();
        let activation = provider
            .snapshot()
            .assignment_for("q", Partition::new(0), None)
            .unwrap()
            .history
            .as_ref()
            .unwrap()
            .activation;
        let mut proposed = assignment.clone();
        proposed.epoch += 1;
        let pending = PendingRecovery {
            version: 1,
            resource_incarnation: resource_incarnation(&previous, &resource).unwrap(),
            previous_activation: Some(activation),
            requested_generation: previous.generation + 1,
            previous: assignment,
            proposed,
            previous_write_nodes: 1,
            proposed_write_nodes: 1,
            required_old_witnesses: 1,
        };
        let generation = previous.generation;
        let mut snapshot = previous;
        snapshot.generation += 1;
        snapshot.attributes.insert(
            pending_recovery_key(&resource),
            serde_json::to_string(&pending).unwrap(),
        );
        provider
            .consensus_node()
            .write_snapshot_guarded(generation, snapshot)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let admissions = provider.local_queue_recovery_admissions().unwrap();
                if provider.pending_recoveries().unwrap().is_empty()
                    && !admissions.is_empty()
                    && admissions.iter().all(|(_, receipt)| {
                        engine
                            .verify_admitted_storage_history(&receipt.storage)
                            .is_ok()
                    })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(provider.pending_recoveries().unwrap().is_empty());
        assert_eq!(
            engine
                .queue_durable_frontiers("q", 0, None)
                .await
                .unwrap()
                .message_next,
            cycle + 1
        );
    }
    let messages = engine
        .poll_ready("q", 0, None, 10, unix_millis() + 60_000, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|m| m.payload.clone())
            .collect::<Vec<_>>(),
        vec![vec![0], vec![1], vec![2]]
    );
    worker.abort();
    broker.shutdown().await;
    listener_task.abort();
    provider.consensus_node().shutdown().await.unwrap();
    server.shutdown();
    drop(broker);
    drop(engine);
    drop(provider);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_three_node_enrollment_admits_followers_after_owner_stops() {
    use fibril_broker::coordination::{NodeInfo, QueueIdentity};
    let root = std::env::temp_dir().join(format!(
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
    let providers: Vec<_> = nodes
        .into_iter()
        .zip(["a", "b", "c"])
        .map(|(node, id)| Arc::new(GanglionCoordination::new(id, node)))
        .collect();
    let mut brokers = Vec::new();
    let mut listeners = Vec::new();
    let mut addresses = HashMap::new();
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        let engine = StromaEngine::open(
            root.join(format!("data-{id}")),
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let broker =
            Broker::new_with_ownership(engine, BrokerConfig::default(), None, providers[i].clone());
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
                assignment.durability = ganglion_core::ReplicationDurabilityPolicy::MajorityDurable;
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
    let config = ProtocolOwnerPeerResolverConfig::new(addresses).with_auth("@node", "secret");
    let owner_worker =
        fibril::recovery_driver::spawn(providers[0].clone(), brokers[0].clone(), config.clone());
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            if providers.iter().all(|p| {
                p.local_initial_history_admissions()
                    .is_ok_and(|a| a.len() == 1)
            }) && providers[0]
                .local_initial_history_admissions()
                .unwrap()
                .iter()
                .all(|(_, p)| {
                    brokers[0]
                        .engine()
                        .verify_admitted_storage_history(p)
                        .is_ok()
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for i in 1..3 {
        let admissions = providers[i].local_initial_history_admissions().unwrap();
        assert_eq!(admissions[0].0.required_write_nodes, 2);
        assert!(
            brokers[i]
                .engine()
                .verify_admitted_storage_history(&admissions[0].1)
                .is_err()
        );
    }
    owner_worker.abort();
    let _ = owner_worker.await;
    brokers[0].shutdown().await;
    let mut workers = Vec::new();
    for i in 1..3 {
        workers.push(fibril::recovery_driver::spawn(
            providers[i].clone(),
            brokers[i].clone(),
            config.clone(),
        ));
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while !(1..3).all(|i| {
            providers[i]
                .local_initial_history_admissions()
                .unwrap()
                .iter()
                .any(|(_, p)| {
                    brokers[i]
                        .engine()
                        .verify_admitted_storage_history(p)
                        .is_ok()
                })
        }) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for w in workers {
        w.abort();
        let _ = w.await;
    }
    for b in &brokers[1..] {
        b.shutdown().await;
    }
    for l in listeners {
        l.abort();
        let _ = l.await;
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
