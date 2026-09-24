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
    ordinary_queue_restarts(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agreed_checkpoints_replace_and_survive_repeated_broker_metadata_restarts() {
    ordinary_queue_restarts(true).await;
}

async fn finish_checkpoint(provider: &GanglionCoordination, engine: &StromaEngine,
    resource: &ganglion_core::ResourceIdentity) {
    provider.queue_checkpoint_step(resource, engine, true).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Err(error) = provider.queue_checkpoint_step(resource, engine, false).await {
                eprintln!("checkpoint retry: {error}");
            }
            let status = provider.queue_checkpoint_status(resource).unwrap();
            assert!(status.failed.is_none(), "{status:?}");
            if status.installed == status.admitted { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
}

async fn ordinary_queue_restarts(checkpoints: bool) {
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
        if checkpoints {
            let epoch = provider.snapshot().assignment_for("q", Partition::new(0), None).unwrap().epoch;
            engine.become_queue_owner_with_epoch("q", 0, None, epoch).await.unwrap();
            for outcome in broker.apply_assignment_snapshot_transitions("a", &fibril_broker::coordination::CoordinationSnapshot::default(), &provider.snapshot()).await {
                outcome.unwrap();
            }
            finish_checkpoint(&provider, &engine, &resource).await;
        }
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
        if checkpoints { finish_checkpoint(&provider, &engine, &resource).await; }
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
    three_node_owner_loss(false, LearnerCase::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_recovery_replaces_stopped_candidate_without_weakening_quorum() {
    three_node_owner_loss(true, LearnerCase::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_background_learner_joins_while_owner_keeps_confirming() {
    three_node_owner_loss(false, LearnerCase::Join).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_learner_resumes_incomplete_checkpoint_after_repeated_broker_restarts() {
    three_node_owner_loss(false, LearnerCase::Restart).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LearnerCase {
    None,
    Join,
    Restart,
}

async fn three_node_owner_loss(recover: bool, learner: LearnerCase) {
    three_node_owner_loss_with_checkpoints(recover, learner, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agreed_checkpoint_requires_three_receipts_then_recovers_confirmed_suffix() {
    three_node_owner_loss_with_checkpoints(true, LearnerCase::None, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agreed_checkpoint_partial_installation_recovers_using_compatible_old_history() {
    three_node_owner_loss_with_checkpoints(true, LearnerCase::None, true, true).await;
}

async fn three_node_owner_loss_with_checkpoints(recover: bool, learner: LearnerCase, checkpoints: bool, partial_checkpoint: bool) {
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
    if learner != LearnerCase::None {
        listeners[2].abort();
        let _ = (&mut listeners[2]).await;
    }
    let mut config = ProtocolOwnerPeerResolverConfig::new(addresses).with_auth("@node", "secret");
    let owner_worker =
        fibril::recovery_driver::spawn(providers[0].clone(), brokers[0].clone(), config.clone());
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            if providers.iter().take(if learner != LearnerCase::None { 2 } else { 3 }).all(|p| {
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
    if learner != LearnerCase::None {
        use fibril_broker::broker::{
            BrokerOwnerReplicationPeer, BrokerReplicationCatchUpOptions, ReplicationResourceKind,
        };
        use fibril_broker::coordination::CoordinationSnapshot;
        use fibril_protocol::v1::replication::connect_protocol_owner_peer;
        let before = providers[0].snapshot();
        let assignment = before
            .assignment_for("q", Partition::new(0), None)
            .unwrap()
            .clone();
        assert_eq!(assignment.history.as_ref().unwrap().replicas.len(), 2);
        assert!(!assignment.is_followed_by("c"));
        let decision = providers[1].local_initial_history_admissions().unwrap()[0]
            .0
            .clone();
        providers[1]
            .admit_local_initial_history(&decision, &brokers[1].engine())
            .await
            .unwrap();
        for i in 0..2 {
            for result in brokers[i]
                .apply_assignment_snapshot_transitions(
                    ["a", "b"][i],
                    &CoordinationSnapshot::default(),
                    &providers[i].snapshot(),
                )
                .await
            {
                result.unwrap();
            }
        }
        let peer = connect_protocol_owner_peer(
            config.nodes["a"].clone(),
            config.auth.as_ref(),
            None,
            "test",
            "1",
        )
        .await
        .unwrap()
        .with_reporter("b")
        .with_history_session(
            assignment
                .history
                .as_ref()
                .unwrap()
                .session("q", Partition::new(0), None, false, "b", "a")
                .unwrap(),
        );
        let publisher = brokers[0]
            .get_publisher("q", Partition::new(0), &None)
            .await
            .unwrap();
        let mut learner_worker = None;
        let mut frontiers = (0, 0);
        for n in 0..40u64 {
            if n == 4 {
                let (address, listener) = serve_test_broker(brokers[2].clone()).await;
                config.nodes.insert("c".into(), address.clone());
                listeners[2] = listener;
                providers[2]
                    .register_self(&NodeInfo {
                        node_id: "c".into(),
                        broker_addr: address,
                        admin_addr: None,
                    })
                    .await
                    .unwrap();
                // Resume from an installed checkpoint whose live payloads
                // have not arrived yet. The worker must backfill before voting.
                let intent = providers[2].begin_queue_learner(&resource).await.unwrap();
                let receipt = providers[2]
                    .prepare_queue_learner(&intent, &brokers[2].engine())
                    .await
                    .unwrap();
                let cut = peer
                    .export_owner_state_checkpoint("q", Partition::new(0), None)
                    .await
                    .unwrap();
                brokers[2]
                    .engine()
                    .become_queue_follower_with_epoch("q", 0, None, 1)
                    .await
                    .unwrap();
                brokers[2]
                    .engine()
                    .install_queue_learner_checkpoint(
                        receipt.clone(),
                        fibril_broker::queue_engine::FollowerStateCheckpointInstall {
                            message_epoch: cut.message_epoch,
                            event_epoch: cut.event_epoch,
                            message_next_offset: cut.message_checkpoint_offset,
                            event_next_offset: cut.event_next_offset,
                            applied_event_offset: cut.applied_event_offset,
                            state_snapshot: cut.state_snapshot,
                        },
                    )
                    .await
                    .unwrap();
                assert!(
                    brokers[2]
                        .engine()
                        .verify_queue_learner_caught_up("q", 0, None, 1, 0, 4)
                        .await
                        .is_err()
                );
                if learner == LearnerCase::Restart {
                    // Recreate the whole broker and its process identity twice,
                    // retaining only durable data. Restart its metadata node
                    // on the same address so the remaining quorum can reconnect.
                    // Native SIGKILL tests cover torn storage publication; this
                    // exercises broker-level reopening and stale authority.
                    let mut old_intent = intent;
                    let mut old_receipt = receipt.clone();
                    for _ in 0..2 {
                        listeners[2].abort();
                        let _ = (&mut listeners[2]).await;
                        let stopped = brokers.pop().unwrap();
                        stopped.shutdown().await;
                        drop(stopped);
                        providers[2].consensus_node().shutdown().await.unwrap();
                        let address = servers[2].local_addr();
                        servers.pop().unwrap().shutdown();
                        let (node, server) = tokio::time::timeout(Duration::from_secs(10), async {
                            loop {
                                match ganglion::RaftMetadataNode::start_durable_tcp(
                                    3,
                                    ganglion::default_raft_config().unwrap(),
                                    address,
                                    root.join("metadata-3"),
                                )
                                .await
                                {
                                    Ok(started) => break started,
                                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                                }
                            }
                        })
                        .await
                        .unwrap();
                        servers.push(server);
                        node.wait_for_any_leader(Duration::from_secs(10))
                            .await
                            .unwrap();
                        providers[2] = Arc::new(GanglionCoordination::new("c", node));
                        let engine = StromaEngine::open(
                            root.join("data-c"),
                            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                            SnapshotConfig::default(),
                        )
                        .await
                        .unwrap();
                        let broker = Broker::new_with_ownership(
                            engine,
                            BrokerConfig::default(),
                            None,
                            providers[2].clone(),
                        );
                        let (address, listener) = serve_test_broker(broker.clone()).await;
                        brokers.push(broker);
                        listeners[2] = listener;
                        config.nodes.insert("c".into(), address.clone());
                        providers[2]
                            .register_self(&NodeInfo {
                                node_id: "c".into(),
                                broker_addr: address,
                                admin_addr: None,
                            })
                            .await
                            .unwrap();
                        assert!(
                            providers[2]
                                .authorize_queue_learner(&old_intent)
                                .await
                                .is_err()
                        );
                        // Registration commits on the metadata leader; its
                        // forwarded reply need not mean this restarted follower
                        // has replayed that prefix yet. Wait for the new process
                        // registration before consulting its local history view.
                        let old_process = uuid::Uuid::from_bytes(old_intent.process).to_string();
                        tokio::time::timeout(Duration::from_secs(10), async {
                            loop {
                                let snapshot = providers[2].consensus_node().committed_snapshot();
                                if snapshot
                                    .nodes
                                    .get("c")
                                    .and_then(|node| {
                                        node.labels.get(
                                            fibril_coordination_ganglion::HISTORY_PROCESS_LABEL,
                                        )
                                    })
                                    .is_some_and(|process| process != &old_process)
                                {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        })
                        .await
                        .expect("restarted metadata follower must replay its registration");
                        let renewed = providers[2].begin_queue_learner(&resource).await.unwrap();
                        let prepared = providers[2]
                            .prepare_queue_learner(&renewed, &brokers[2].engine())
                            .await
                            .unwrap();
                        assert_ne!(renewed.process, old_intent.process);
                        assert_ne!(prepared.storage_instance, old_receipt.storage_instance);
                        assert_eq!(prepared.binding, old_receipt.binding);
                        assert!(
                            !providers[2]
                                .snapshot()
                                .assignment_for("q", Partition::new(0), None)
                                .unwrap()
                                .is_followed_by("c")
                        );
                        assert!(
                            providers[2]
                                .admit_queue_learner(&renewed, &brokers[2].engine(), 4, 4)
                                .await
                                .is_err()
                        );
                        assert!(providers[0].pending_recoveries().unwrap().is_empty());
                        old_intent = renewed;
                        old_receipt = prepared;
                    }
                }
                learner_worker = Some(fibril::recovery_driver::spawn(
                    providers[2].clone(),
                    brokers[2].clone(),
                    config.clone(),
                ));
            }
            let confirmation = publisher
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
            tokio::time::timeout(Duration::from_secs(10), async {
                while frontiers.0 <= n || frontiers.1 <= n {
                    brokers[1]
                        .catch_up_replication_follower_from_owner_with_checkpoint(
                            &peer,
                            "q",
                            Partition::new(0),
                            None,
                            ReplicationResourceKind::Queue,
                            BrokerReplicationCatchUpOptions {
                                message_from: frontiers.0,
                                event_from: frontiers.1,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    frontiers = brokers[1]
                        .engine()
                        .queue_replication_next_offsets("q", 0, None)
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), confirmation)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                n
            );
            assert!(providers[0].pending_recoveries().unwrap().is_empty());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            while providers.iter().any(|p| {
                !p.snapshot()
                    .assignment_for("q", Partition::new(0), None)
                    .is_some_and(|a| a.is_followed_by("c"))
            }) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let current = providers[0].snapshot();
        let joined = current
            .assignment_for("q", Partition::new(0), None)
            .unwrap();
        assert!(joined.preserves_replication_contract(&assignment));
        assert_eq!(joined.history.as_ref().unwrap().replicas.len(), 3);
        brokers[2]
            .engine()
            .verify_queue_learner_caught_up("q", 0, None, 1, 4, 4)
            .await
            .unwrap();
        learner_worker.unwrap().abort();
        // Handoff from checkpoint-backed learner to ordinary replication must
        // resume its admitted cursors. Starting from zero requests a second
        // checkpoint, which accepted-history storage correctly refuses.
        for result in brokers[0]
            .apply_assignment_snapshot_transitions("a", &before, &providers[0].snapshot())
            .await
        {
            result.unwrap();
        }
        let resolver = Arc::new(
            fibril_protocol::v1::replication::CoordinationProtocolOwnerPeerResolver::with_config(
                providers[2].clone(),
                config.clone().with_reporter("c"),
            ),
        );
        brokers[2].spawn_assignment_watcher_with_follower_replication(
            providers[2].clone(),
            resolver,
            fibril_broker::broker::FollowerReplicationWorkerConfig {
                allow_checkpoint_install: true,
                ..Default::default()
            },
        );
        let confirmation = publisher.publish(
            40u64.to_le_bytes().to_vec(), unix_millis(), unix_millis(),
            None, Default::default(), None,
        ).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), confirmation)
                .await.expect("ordinary follower must continue from the installed checkpoint")
                .unwrap().unwrap(),
            40,
        );
        owner_worker.abort();
        drop(peer);
        drop(publisher);
        for listener in listeners {
            listener.abort();
        }
        for broker in &brokers {
            broker.shutdown().await;
        }
        for provider in &providers {
            provider.consensus_node().shutdown().await.unwrap();
        }
        for server in servers {
            server.shutdown();
        }
        std::fs::remove_dir_all(root).unwrap();
        return;
    }
    if checkpoints {
        for i in 1..3 {
            let decision = providers[i].local_initial_history_admissions().unwrap()[0].0.clone();
            providers[i].admit_local_initial_history(&decision, &brokers[i].engine()).await.unwrap();
        }
        for i in 0..3 {
            for result in brokers[i].apply_assignment_snapshot_transitions(["a", "b", "c"][i],
                &fibril_broker::coordination::CoordinationSnapshot::default(), &providers[i].snapshot()).await {
                result.unwrap();
            }
            if i == 0 { brokers[i].engine().become_queue_owner_with_epoch("q", 0, None, 1).await.unwrap(); }
            else {
                brokers[i].engine().become_queue_follower_with_epoch("q", 0, None, 1).await.unwrap();

            }
        }
        let assignment = providers[0].snapshot().assignment_for("q", Partition::new(0), None).unwrap().clone();
        let mut peers = Vec::new();
        for id in ["b", "c"] {
            peers.push(fibril_protocol::v1::replication::connect_protocol_owner_peer(config.nodes["a"].clone(),
                config.auth.as_ref(), None, "checkpoint-test", "1").await.unwrap()
                .with_reporter(id).with_history_session(assignment.history.as_ref().unwrap().session("q", Partition::new(0), None, false, id, "a").unwrap()));
        }
        let publisher = brokers[0].get_publisher("q", Partition::new(0), &None).await.unwrap();
        for n in 0..16u64 {
            let confirm = publisher.publish(n.to_le_bytes().to_vec(), unix_millis(), unix_millis(), None, Default::default(), None).await.unwrap();
            for i in 1..3 {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let from = brokers[i].engine().queue_replication_next_offsets("q", 0, None).await.unwrap();
                        if from.0 >= n + 1 && from.1 >= n + 1 { break; }
                        brokers[i].catch_up_replication_follower_from_owner(&peers[i-1], "q", Partition::new(0), None,
                            fibril_broker::broker::ReplicationResourceKind::Queue,
                            fibril_broker::broker::BrokerReplicationCatchUpOptions { message_from: from.0, event_from: from.1,
                                max_messages_per_read: 4096, max_events_per_read: 4096, max_bytes_per_read: 16*1024*1024,
                                max_iterations: 16, max_wait_ms: 0 }).await.unwrap();
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }).await.unwrap();
            }
            assert_eq!(tokio::time::timeout(Duration::from_secs(10), confirm).await.unwrap().unwrap().unwrap(), n);
        }
        providers[0].queue_checkpoint_step(&resource, &brokers[0].engine(), true).await.unwrap();
        // Two durable replicas suffice for publishes, but cannot issue the
        // all-admitted checkpoint certificate while the third report is absent.
        for _ in 0..6 {
            for i in 0..2 { let _ = providers[i].queue_checkpoint_step(&resource, &brokers[i].engine(), false).await; }
        }
        assert!(providers[0].queue_checkpoint_status(&resource).unwrap().certificate.is_none());
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                for i in 0..3 {
                    if partial_checkpoint && i == 2 && providers[0].queue_checkpoint_status(&resource).unwrap().certificate.is_some() { continue; }
                    if let Err(error) = providers[i].queue_checkpoint_step(&resource, &brokers[i].engine(), false).await {
                        eprintln!("checkpoint node {i}: {error}");
                    }
                }
                let status = providers[0].queue_checkpoint_status(&resource).unwrap();
                assert!(status.failed.is_none(), "{status:?}");
                if status.installed == if partial_checkpoint { 2 } else { 3 } { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        let confirm = publisher.publish(16u64.to_le_bytes().to_vec(), unix_millis(), unix_millis(), None, Default::default(), None).await.unwrap();
            for i in 1..3 {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let from = brokers[i].engine().queue_replication_next_offsets("q", 0, None).await.unwrap();
                        if from.0 >= 17 && from.1 >= 17 { break; }
                        brokers[i].catch_up_replication_follower_from_owner(&peers[i-1], "q", Partition::new(0), None,
                            fibril_broker::broker::ReplicationResourceKind::Queue,
                            fibril_broker::broker::BrokerReplicationCatchUpOptions { message_from: from.0, event_from: from.1,
                                max_messages_per_read: 4096, max_events_per_read: 4096, max_bytes_per_read: 16*1024*1024,
                                max_iterations: 16, max_wait_ms: 0 }).await.unwrap();
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }).await.unwrap();
            }
        assert_eq!(tokio::time::timeout(Duration::from_secs(10), confirm).await.unwrap().unwrap().unwrap(), 16);
        drop(publisher);
    }
    for i in 1..3 {
        let admissions = providers[i].local_initial_history_admissions().unwrap();
        assert_eq!(admissions[0].0.required_write_nodes, 2);
        assert!(
            brokers[i]
                .engine()
                .verify_admitted_storage_history(&admissions[0].1)
                .is_err() != checkpoints
        );
    }
    owner_worker.abort();
    let _ = owner_worker.await;
    brokers[0].shutdown().await;
    listeners[0].abort();
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
    if recover {
        use fibril_broker::coordination::{
            DeterministicPartitionPlacement, DeterministicStreamPlacement,
            ReplicationDurabilityPolicy,
        };
        // Persist an unfinished transition whose candidate has actually stopped.
        // The controller must choose another member while leaving its proof fixed.
        let pending = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for provider in &providers {
                    if !provider.consensus_node().is_leader().await {
                        continue;
                    }
                    let mut snapshot = provider.consensus_node().committed_snapshot();
                    let previous = snapshot.assignments[&resource].clone();
                    let activation = provider
                        .snapshot()
                        .assignment_for("q", Partition::new(0), None)
                        .unwrap()
                        .history
                        .as_ref()
                        .unwrap()
                        .activation;
                    let mut proposed = previous.clone();
                    proposed.epoch += 1;
                    let pending = PendingRecovery {
                        version: 1,
                        resource_incarnation: resource_incarnation(&snapshot, &resource).unwrap(),
                        previous_activation: Some(activation),
                        requested_generation: snapshot.generation + 1,
                        previous,
                        proposed,
                        previous_write_nodes: 2,
                        proposed_write_nodes: 2,
                        required_old_witnesses: 2,
                    };
                    let generation = snapshot.generation;
                    snapshot.generation += 1;
                    snapshot.attributes.insert(
                        pending_recovery_key(&resource),
                        serde_json::to_string(&pending).unwrap(),
                    );
                    match provider
                        .consensus_node()
                        .write_snapshot_guarded(generation, snapshot)
                        .await
                    {
                        Ok(_) => return pending,
                        Err(
                            ganglion::OpenraftAdapterError::NotLeader
                            | ganglion::OpenraftAdapterError::GenerationMismatch { .. },
                        ) => {}
                        Err(e) => panic!("pending setup failed: {e}"),
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let live: HashMap<_, _> = providers[0]
            .live_nodes(Duration::from_secs(300))
            .into_iter()
            .filter(|(node, _)| node != "a")
            .collect();
        assert_eq!(live.len(), 2);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for provider in &providers {
                    let _ = provider
                        .control_iteration(
                            &DeterministicPartitionPlacement,
                            &[QueueIdentity::new("q", Partition::new(0), None)],
                            &DeterministicStreamPlacement,
                            &[],
                            2,
                            2,
                            ReplicationDurabilityPolicy::MajorityDurable,
                            &live,
                            8,
                        )
                        .await;
                }
                if providers.iter().all(|p| {
                    p.queue_recovery_candidate(&pending)
                        .is_ok_and(|a| a.owner == "b")
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            providers[1].pending_recoveries().unwrap(),
            vec![pending.clone()]
        );
        tokio::time::timeout(Duration::from_secs(70), async {
            loop {
                if (1..3).all(|i| {
                    providers[i]
                        .pending_recoveries()
                        .is_ok_and(|p| p.is_empty())
                        && providers[i]
                            .local_queue_recovery_admissions()
                            .is_ok_and(|a| {
                                a.len() == 1
                                    && a.iter().all(|(_, r)| {
                                        brokers[i]
                                            .engine()
                                            .verify_admitted_storage_history(&r.storage)
                                            .is_ok()
                                    })
                            })
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let assignment = providers[1]
            .snapshot()
            .assignment_for("q", Partition::new(0), None)
            .unwrap()
            .clone();
        assert_eq!(assignment.owner, "b");
        assert_eq!(assignment.epoch, pending.proposed.epoch);
        let history = assignment.history.as_ref().unwrap();
        assert_eq!(history.replicas.len(), 2);
        assert!(history.replicas.contains_key("b") && history.replicas.contains_key("c"));
        assert_eq!(
            assignment.durability,
            ReplicationDurabilityPolicy::MajorityDurable
        );
        if checkpoints {
            let deliveries = brokers[1].engine().poll_ready("q", 0, None, 32, unix_millis() + 60_000, u64::MAX).await.unwrap();
            assert_eq!(deliveries.iter().map(|m| u64::from_le_bytes(m.payload.as_slice().try_into().unwrap())).collect::<Vec<_>>(), (0..17).collect::<Vec<_>>());
        }

    }
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
