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
    if std::env::var_os("FIBRIL_TEST_RECOVERY_TRACE").is_some() {
        static TRACING: std::sync::Once = std::sync::Once::new();
        TRACING.call_once(fibril_util::init_tracing);
    }
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
        let durable = engine.queue_durable_frontiers("q", 0, None).await.unwrap();
        assert_eq!(durable.message_next, 0);
        assert_eq!(durable.event_next, cycle + 1);
    }
    // The same production stage guards feed the admin snapshot. Wait for the
    // attempt's final guard as activation can become visible just before it drops.
    let diagnostics = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = provider.recovery_diagnostics().snapshot();
            if snapshot.attempts.iter().filter(|a| a.outcome ==
                fibril_coordination_ganglion::recovery_diagnostics::Outcome::Ok).count() >= 2 {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    for attempt in diagnostics.attempts.iter().filter(|a| a.outcome ==
        fibril_coordination_ganglion::recovery_diagnostics::Outcome::Ok) {
        assert_eq!(attempt.topic, "q");
        assert_eq!(attempt.omitted_stages, 0);
        assert!(attempt.stages.iter().any(|s| s.name == "activate"));
        assert!(attempt.stages.iter().any(|s| s.name == "admit"));
        assert!(attempt.stages.iter().all(|s| s.start_us + s.elapsed_us <= attempt.elapsed_us));
    }
    let wire = serde_json::to_value(diagnostics).unwrap();
    assert_eq!(wire["scope"], "local_process");
    assert_eq!(wire["capacity"], 32);
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
    Reseed,
}

async fn three_node_owner_loss(recover: bool, learner: LearnerCase) {
    three_node_owner_loss_with_checkpoints(recover, learner, false, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agreed_checkpoint_requires_three_receipts_then_recovers_confirmed_suffix() {
    three_node_owner_loss_with_checkpoints(true, LearnerCase::None, true, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agreed_checkpoint_partial_installation_recovers_using_compatible_old_history() {
    three_node_owner_loss_with_checkpoints(true, LearnerCase::None, true, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_copies_multiple_pages_to_a_lagging_checkpoint_replica() {
    three_node_owner_loss_with_checkpoints(true, LearnerCase::None, true, false, true).await;
}

#[derive(Clone, Copy)]
struct RecoveryCopyHistory {
    records: u64,
    payload_bytes: usize,
    recovery_timeout: Duration,
    lose_source: bool,
    unique_ttl: bool,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "large replicated recovery acceptance, writes several GiB"]
async fn automatic_recovery_exceeds_old_inspection_and_staging_byte_limits() {
    three_node_owner_loss_with_history(true, LearnerCase::None, true, false,
        Some(RecoveryCopyHistory { records: 16_385, payload_bytes: 64 * 1024,
            recovery_timeout: Duration::from_secs(300), lose_source: false, unique_ttl: false })).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_keeps_downloaded_source_after_source_loss_and_candidate_restart() {
    three_node_owner_loss_with_history(true, LearnerCase::None, true, false,
        Some(RecoveryCopyHistory { records: 65, payload_bytes: 1024,
            recovery_timeout: Duration::from_secs(70), lose_source: true, unique_ttl: false })).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "large replicated recovery acceptance, over one million retained records"]
async fn automatic_recovery_exceeds_old_record_and_operation_limits() {
    three_node_owner_loss_with_history(true, LearnerCase::None, true, false,
        Some(RecoveryCopyHistory { records: 1_100_000, payload_bytes: 1024,
            recovery_timeout: Duration::from_secs(600), lose_source: false, unique_ttl: false })).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "large live-state recovery acceptance, 750000 distinct TTL deadlines"]
async fn automatic_recovery_transfers_snapshot_larger_than_one_control_frame() {
    three_node_owner_loss_with_history(true, LearnerCase::None, true, false,
        Some(RecoveryCopyHistory { records: 750_000, payload_bytes: 128,
            recovery_timeout: Duration::from_secs(600), lose_source: false, unique_ttl: true })).await;
}

async fn three_node_owner_loss_with_checkpoints(
    recover: bool,
    learner: LearnerCase,
    checkpoints: bool,
    partial_checkpoint: bool,
    copy_pages: bool,
) {
    three_node_owner_loss_with_history(recover, learner, checkpoints, partial_checkpoint,
        copy_pages.then_some(RecoveryCopyHistory { records: 1025, payload_bytes: 64 * 1024,
            recovery_timeout: Duration::from_secs(70), lose_source: false, unique_ttl: false })).await;
}

async fn three_node_owner_loss_with_history(
    recover: bool,
    learner: LearnerCase,
    checkpoints: bool,
    partial_checkpoint: bool,
    copy_history: Option<RecoveryCopyHistory>,
) {
    use fibril_broker::coordination::{NodeInfo, QueueIdentity};
    let root = std::path::Path::new(env!("CARGO_WORKSPACE_DIR")).join("test_data").join(format!(
        "fibril-three-node-enrollment-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let _fixture = keratin_log::util::TempDir { root: root.clone() };
    let lose_source = copy_history.is_some_and(|c| c.lose_source);
    let copied_follower = if lose_source { 2 } else { 1 };
    let lagging_follower = if lose_source { 1 } else { 2 };
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
    let message_tail = |_: u64| 0;
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
    if matches!(learner, LearnerCase::Join | LearnerCase::Restart) {
        listeners[2].abort();
        let _ = (&mut listeners[2]).await;
    }
    let mut config = ProtocolOwnerPeerResolverConfig::new(addresses).with_auth("@node", "secret");
    let owner_worker =
        fibril::recovery_driver::spawn(providers[0].clone(), brokers[0].clone(), config.clone());
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            if providers.iter().take(if matches!(learner, LearnerCase::Join | LearnerCase::Restart) { 2 } else { 3 }).all(|p| {
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
    if matches!(learner, LearnerCase::Join | LearnerCase::Restart) {
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
                            combined_log: cut.combined_log,
                            event_start_offset: cut.event_start_offset,
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
                            root.join("data-c").join("storage"),
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
                                .admit_queue_learner(&renewed, &brokers[2].engine(), message_tail(4), 4)
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
                while frontiers.0 < message_tail(n + 1) || frontiers.1 <= n {
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
            .verify_queue_learner_caught_up("q", 0, None, 1, message_tail(4), 4)
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
        return;
    }
    if learner == LearnerCase::Reseed {
        admitted_replica_reseed(&providers, &brokers, &config, &resource).await;
        owner_worker.abort();
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
                        if from.0 >= message_tail(n + 1) && from.1 >= n + 1 { break; }
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
        if let Some(copy) = copy_history {
            // Only b receives this confirmed suffix; c retains the agreed
            // checkpoint. More than 64 MiB forces multiple transfer pages after
            // a stops, so recovery must copy real payloads into c's new stage.
            let started = Instant::now();
            let final_offset = 16 + copy.records;
            let expiry_base = unix_millis() + 86_400_000;
            let batch_size = (8 * 1024 * 1024 / copy.payload_bytes).clamp(128, 4096);
            for batch_start in (16..final_offset).step_by(batch_size) {
                let batch_end = (batch_start + batch_size as u64).min(final_offset);
                let mut confirms = Vec::new();
                for n in batch_start..batch_end {
                    let mut payload = vec![0x5a; copy.payload_bytes];
                    payload[..8].copy_from_slice(&n.to_le_bytes());
                    confirms.push(publisher.publish(payload, unix_millis(), unix_millis(), None,
                        Default::default(), copy.unique_ttl.then_some(expiry_base + n)).await.unwrap());
                }
                tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        let from = brokers[copied_follower].engine().queue_replication_next_offsets("q", 0, None).await.unwrap();
                        if from.1 >= batch_end { break; }
                        brokers[copied_follower].catch_up_replication_follower_from_owner(&peers[copied_follower - 1], "q", Partition::new(0), None,
                            fibril_broker::broker::ReplicationResourceKind::Queue,
                            fibril_broker::broker::BrokerReplicationCatchUpOptions {
                                message_from: from.0, event_from: from.1,
                                max_messages_per_read: 4096, max_events_per_read: 4096,
                                max_bytes_per_read: 16 * 1024 * 1024, max_iterations: 16, max_wait_ms: 0,
                            }).await.unwrap();
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }).await.unwrap();
                for (index, confirm) in confirms.into_iter().enumerate() {
                    assert_eq!(tokio::time::timeout(Duration::from_secs(10), confirm).await.unwrap().unwrap().unwrap(),
                        batch_start + index as u64);
                }
            }
            eprintln!("confirmed suffix: records={} payload_bytes={} elapsed={:?}",
                copy.records, copy.records * copy.payload_bytes as u64, started.elapsed());
            assert_eq!(
                brokers[lagging_follower]
                    .engine()
                    .queue_replication_next_offsets("q", 0, None)
                    .await
                    .unwrap(),
                (message_tail(16), 16)
            );
        } else {
            let confirm = publisher
                .publish(
                    16u64.to_le_bytes().to_vec(),
                    unix_millis(),
                    unix_millis(),
                    None,
                    Default::default(),
                    None,
                )
                .await
                .unwrap();
            for i in 1..3 {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let from = brokers[i]
                            .engine()
                            .queue_replication_next_offsets("q", 0, None)
                            .await
                            .unwrap();
                        if from.0 >= message_tail(17) && from.1 >= 17 {
                            break;
                        }
                        brokers[i]
                            .catch_up_replication_follower_from_owner(
                                &peers[i - 1],
                                "q",
                                Partition::new(0),
                                None,
                                fibril_broker::broker::ReplicationResourceKind::Queue,
                                fibril_broker::broker::BrokerReplicationCatchUpOptions {
                                    message_from: from.0,
                                    event_from: from.1,
                                    max_messages_per_read: 4096,
                                    max_events_per_read: 4096,
                                    max_bytes_per_read: 16 * 1024 * 1024,
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
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(10), confirm)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                16
            );
        }
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
        if lose_source { continue; }
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
        if lose_source {
            use fibril_broker::recovery::inspection::RecoveryInspectionLimits;
            use fibril_coordination_ganglion::recovery_witnesses::RecoveryWitnessSet;
            use fibril_protocol::v1::recovery_inspection::{inspect_recovery_source_artifact_resumable, inspect_recovery_pair_resumable};
            let command = pending.seal_command().unwrap();
            let transition = pending.transition_digest().unwrap().iter().map(|b| format!("{b:02x}")).collect::<String>();
            let journal = brokers[1].engine().recovery_inspection_journal_root().join(transition);
            let limits = RecoveryInspectionLimits { page_records: 4096, page_bytes: 16 * 1024 * 1024, ..Default::default() };
            let mut witnesses = RecoveryWitnessSet::new(&providers[1].consensus_node().committed_snapshot(), &pending).unwrap();
            let mut reports = Vec::new();
            let mut summaries = BTreeMap::new();
            for id in ["b", "c"] {
                let report = fibril_protocol::v1::replication::request_recovery_seal(&config, id, &command, Duration::from_secs(10)).await.unwrap();
                let artifact = inspect_recovery_source_artifact_resumable(&config, &command, &report, limits,
                    Default::default(), 128 * 1024 * 1024, 128 * 1024 * 1024, Duration::from_secs(10), &journal).await.unwrap();
                witnesses.record(&providers[1].consensus_node().committed_snapshot(), id, report.clone()).unwrap();
                summaries.insert(id.to_owned(), artifact.summary());
                reports.push(report);
            }
            let pair = inspect_recovery_pair_resumable(&config, &command, &reports[0], &reports[1], limits,
                Duration::from_secs(10), &journal).await.unwrap();
            let selection = witnesses.select_queue_source_summaries(&providers[1].consensus_node().committed_snapshot(),
                &summaries, &[pair.evidence]).unwrap();
            let plan = providers[1].persist_queue_recovery_plan(&pending, &witnesses, &selection).await.unwrap();
            assert_eq!(plan.source_node(), "c");
            listeners[2].abort();
            let _ = (&mut listeners[2]).await;
            brokers[2].shutdown().await;
            assert!(fibril::recovery_driver::recover_queue_once(&providers[1], &brokers[1], &config, &pending).await.is_err());
            assert_eq!(providers[1].pending_recoveries().unwrap(), vec![pending.clone()],
                "source loss must not reduce the required installed quorum");
            // The candidate must finish its private copy without the source.
            use fibril_broker::recovery_transfer::{QueueRecoveryCommand, QueueRecoveryRequest, QueueRecoveryOperation, QueueRecoveryReply};
            let ready = brokers[1].recovery_transfer(QueueRecoveryRequest {
                command: QueueRecoveryCommand { replica_id: "b".into(), topic: "q".into(), partition: 0, group: None, plan: plan.digest().unwrap() },
                operation: QueueRecoveryOperation::Snapshot,
            }).await.unwrap();
            assert!(matches!(ready, QueueRecoveryReply::Snapshot(_)));
            // Restart both candidate and old source. The immutable plan and
            // completed target survive without redoing original source inspection.
            listeners[1].abort();
            let _ = (&mut listeners[1]).await;
            brokers[1].shutdown().await;
            for (i, id) in [(1, "b"), (2, "c")] {
                let engine = StromaEngine::open(root.join(format!("data-{id}")).join("storage"),
                    StromaKeratinConfig::from_message_log(KeratinConfig::test_default()), SnapshotConfig::default()).await.unwrap();
                let broker = Broker::new_with_ownership(engine, BrokerConfig::default(), None, providers[i].clone());
                let (address, listener) = serve_test_broker(broker.clone()).await;
                providers[i].register_self(&NodeInfo { node_id: id.into(), broker_addr: address.clone(), admin_addr: None }).await.unwrap();
                config.nodes.insert(id.into(), address);
                brokers[i] = broker;
                listeners[i] = listener;
            }
            for i in 1..3 {
                workers.push(fibril::recovery_driver::spawn(providers[i].clone(), brokers[i].clone(), config.clone()));
            }
        }
        let recovery_started = Instant::now();
        let mut observed_snapshot_bytes = 0;
        tokio::time::timeout(copy_history.map_or(Duration::from_secs(70), |c| c.recovery_timeout), async {
            loop {
                if copy_history.is_some_and(|c| c.unique_ttl) {
                    if let Ok(Some(plan)) = providers[1].queue_recovery_plan(&pending) {
                        observed_snapshot_bytes = plan.snapshot_bytes();
                    }
                }
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
        if copy_history.is_some_and(|c| c.unique_ttl) {
            assert!(observed_snapshot_bytes > 16 * 1024 * 1024,
                "large live-state case must cross a whole control-frame snapshot: {observed_snapshot_bytes}");
            eprintln!("selected snapshot bytes={observed_snapshot_bytes}");
        }
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
        if copy_history.is_some_and(|copy| copy.records >= 16_385) {
            // A returning old owner must use the non-voting learner path. Its
            // old bytes cannot grant admission into the newly recovered history.
            let rejoin_started = Instant::now();
            let engine = StromaEngine::open(root.join("data-a").join("storage"),
                StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                SnapshotConfig::default()).await.unwrap();
            let broker = Broker::new_with_ownership(engine, BrokerConfig::default(), None, providers[0].clone());
            let (address, listener) = serve_test_broker(broker.clone()).await;
            providers[0].register_self(&NodeInfo { node_id: "a".into(), broker_addr: address.clone(), admin_addr: None }).await.unwrap();
            config.nodes.insert("a".into(), address);
            brokers[0] = broker;
            listeners[0] = listener;
            for i in 1..3 {
                for result in brokers[i].apply_assignment_snapshot_transitions(["a", "b", "c"][i],
                    &fibril_broker::coordination::CoordinationSnapshot::default(), &providers[i].snapshot()).await {
                    result.unwrap();
                }
            }
            tokio::time::timeout(Duration::from_secs(180), async {
                loop {
                    match fibril::queue_learner_driver::learn_queue_once(&providers[0], &brokers[0], &config, &resource).await {
                        Ok(()) => break,
                        Err(error) => eprintln!("returning old owner learner retry: {error}"),
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }).await.expect("returning old owner must join the recovered history");
            let rejoined = providers[0].snapshot().assignment_for("q", Partition::new(0), None).unwrap().clone();
            assert_eq!(rejoined.owner, "b");
            assert_eq!(rejoined.epoch, assignment.epoch);
            assert_eq!(rejoined.durability, ReplicationDurabilityPolicy::MajorityDurable);
            assert!(rejoined.history.as_ref().unwrap().replicas.contains_key("a"));
            brokers[0].engine().verify_queue_learner_caught_up("q", 0, None,
                assignment.epoch, 0, 16 + copy_history.unwrap().records).await.unwrap();
            eprintln!("old owner rejoined after large recovery: elapsed={:?}", rejoin_started.elapsed());
        }
        if copy_history.is_some() {
                for i in 1..3 {
                    for result in brokers[i].apply_assignment_snapshot_transitions(["a", "b", "c"][i],
                        &fibril_broker::coordination::CoordinationSnapshot::default(), &providers[i].snapshot()).await {
                        result.unwrap();
                    }
                }
        }
        if checkpoints {
            let expected = copy_history.map_or(17, |c| 16 + c.records);
            let mut next = 0;
            while next < expected {
                let deliveries = brokers[1].engine().poll_ready("q", 0, None, 128, unix_millis() + 60_000, u64::MAX).await.unwrap();
                assert!(!deliveries.is_empty(), "confirmed payload missing at {next}");
                for message in deliveries {
                    assert_eq!(u64::from_le_bytes(message.payload[..8].try_into().unwrap()), next);
                    if let Some(copy) = copy_history {
                        if next >= 16 {
                            assert_eq!(message.payload.len(), copy.payload_bytes);
                            assert!(message.payload[8..].iter().all(|byte| *byte == 0x5a));
                        }
                    }
                    next += 1;
                }
            }
            eprintln!("recovery with delivery verification: confirmed_ids={expected} elapsed={:?}", recovery_started.elapsed());
            if copy_history.is_some() {
                // Exercise the installed history's ordinary publish path too.
                let peer = fibril_protocol::v1::replication::connect_protocol_owner_peer(
                    config.nodes["b"].clone(), config.auth.as_ref(), None, "recovered-test", "1")
                    .await.unwrap().with_reporter("c")
                    .with_history_session(history.session("q", Partition::new(0), None, false, "c", "b").unwrap());
                let publisher = brokers[1].get_publisher("q", Partition::new(0), &None).await.unwrap();
                let confirmation = publisher.publish(b"after-recovery".to_vec(), unix_millis(), unix_millis(),
                    None, Default::default(), None).await.unwrap();
                let replicate = async {
                    loop {
                        let from = brokers[2].engine().queue_replication_next_offsets("q", 0, None).await.unwrap();
                        brokers[2].catch_up_replication_follower_from_owner(&peer, "q", Partition::new(0), None,
                            fibril_broker::broker::ReplicationResourceKind::Queue,
                            fibril_broker::broker::BrokerReplicationCatchUpOptions {
                                message_from: from.0, event_from: from.1,
                                max_messages_per_read: 4096, max_events_per_read: 4096,
                                max_bytes_per_read: 16 * 1024 * 1024, max_iterations: 16, max_wait_ms: 0,
                            }).await.unwrap();
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                };
                tokio::select! {
                    result = tokio::time::timeout(Duration::from_secs(10), confirmation) => {
                        result.unwrap().unwrap().unwrap();
                    }
                    _ = replicate => unreachable!(),
                }
                let delivered = brokers[1].engine().poll_ready("q", 0, None, 1, unix_millis() + 60_000, u64::MAX).await.unwrap();
                assert_eq!(delivered.len(), 1);
                assert_eq!(&delivered[0].payload[..], b"after-recovery");
                eprintln!("fresh publish confirmed and delivered after recovery");
                if copy_history.is_some_and(|copy| copy.records >= 16_385) {
                    let transition: String = pending.transition_digest().unwrap().iter()
                        .map(|byte| format!("{byte:02x}")).collect();
                    let cache = brokers[1].engine().recovery_inspection_journal_root().join(transition);
                    tokio::time::timeout(Duration::from_secs(60), async {
                        while !cache.join("reclaimed").exists() {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }).await.expect("automatic sweep must reclaim completed recovery cache");
                    let mut files: Vec<_> = std::fs::read_dir(&cache).unwrap()
                        .map(|entry| entry.unwrap().file_name().into_string().unwrap()).collect();
                    files.sort();
                    assert_eq!(files, [".namespace.lock", "reclaimed", "retired"]);
                    eprintln!("automatic inspection cache reclamation verified");
                }
            }

        }

    }
    for w in workers {
        w.abort();
        let _ = w.await;
    }
    for b in &brokers {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_replica_reseeds_without_erasing_old_copy_or_weakening_confirms() {
    three_node_owner_loss(false, LearnerCase::Reseed).await;
}

async fn admitted_replica_reseed(
    providers: &[Arc<GanglionCoordination>],
    brokers: &[Arc<Broker<StromaEngine>>],
    config: &ProtocolOwnerPeerResolverConfig,
    resource: &ganglion_core::ResourceIdentity,
) {
    use fibril_broker::{
        broker::{
            BrokerOwnerReplicationPeer, BrokerReplicationCatchUp, BrokerReplicationCatchUpOptions,
            ReplicationResourceKind,
        },
        coordination::CoordinationSnapshot,
    };
    use fibril_protocol::v1::replication::connect_protocol_owner_peer;
    for i in 1..3 {
        let decision = providers[i].local_initial_history_admissions().unwrap()[0]
            .0
            .clone();
        providers[i]
            .admit_local_initial_history(&decision, &brokers[i].engine())
            .await
            .unwrap();
    }
    for i in 0..3 {
        for result in brokers[i]
            .apply_assignment_snapshot_transitions(
                ["a", "b", "c"][i],
                &CoordinationSnapshot::default(),
                &providers[i].snapshot(),
            )
            .await
        {
            result.unwrap();
        }
    }
    let message_tail = |_: u64| 0;
    let original = providers[0]
        .snapshot()
        .assignment_for("q", Partition::new(0), None)
        .unwrap()
        .clone();
    let old_session = original
        .history
        .as_ref()
        .unwrap()
        .session("q", Partition::new(0), None, false, "c", "a")
        .unwrap();
    let publisher = brokers[0]
        .get_publisher("q", Partition::new(0), &None)
        .await
        .unwrap();
    let catch_up = |i: usize, target: u64| async move {
        let assignment = providers[0]
            .snapshot()
            .assignment_for("q", Partition::new(0), None)
            .unwrap()
            .clone();
        let id = ["a", "b", "c"][i];
        let peer = connect_protocol_owner_peer(
            config.nodes["a"].clone(),
            config.auth.as_ref(),
            None,
            "reseed-test",
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
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
        let from = brokers[i]
            .engine()
            .queue_replication_next_offsets("q", 0, None)
            .await
            .unwrap();
        if from.1 >= target { break; }
        brokers[i]
            .catch_up_replication_follower_from_owner(
                &peer,
                "q",
                Partition::new(0),
                None,
                ReplicationResourceKind::Queue,
                BrokerReplicationCatchUpOptions {
                    message_from: from.0,
                    event_from: from.1,
                    max_messages_per_read: 4096,
                    max_events_per_read: 4096,
                    max_bytes_per_read: 16 * 1024 * 1024,
                    max_iterations: 16,
                    max_wait_ms: 0,
                },
            )
            .await
            .unwrap();
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }).await.unwrap();
        peer
    };
    let mut healthy_peer = None;
    for n in 0..4u64 {
        let confirm = publisher
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
        let b = catch_up(1, n + 1).await;
        let c = catch_up(2, n + 1).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), confirm)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            n
        );
        healthy_peer = Some(b);
        drop(c);
    }
    // Exercise the production exhausted-retention branch. It must request
    // coordinated repair and must never invoke legacy destructive installation.
    struct ExpiredHistory;
    impl BrokerOwnerReplicationPeer for ExpiredHistory {
        fn read_owner_replication_records<'a>(
            &'a self,
            _: &'a str,
            _: Partition,
            _: Option<&'a str>,
            message: u64,
            event: u64,
            _: usize,
            _: usize,
            _: usize,
            _: u64,
        ) -> futures::future::BoxFuture<
            'a,
            Result<
                fibril_broker::broker::BrokerOwnerReplicationRecords,
                fibril_broker::broker::BrokerError,
            >,
        > {
            Box::pin(async move {
                Ok(fibril_broker::broker::BrokerOwnerReplicationRecords {
                    messages:
                        fibril_broker::queue_engine::OwnerReplicationRead::CheckpointRequired {
                            epoch: 1,
                            requested_offset: message,
                            head_offset: 5,
                            next_offset: 5,
                        },
                    events: fibril_broker::queue_engine::OwnerReplicationRead::CheckpointRequired {
                        epoch: 1,
                        requested_offset: event,
                        head_offset: 5,
                        next_offset: 5,
                    },
                })
            })
        }
        fn export_owner_state_checkpoint<'a>(
            &'a self,
            _: &'a str,
            _: Partition,
            _: Option<&'a str>,
        ) -> futures::future::BoxFuture<
            'a,
            Result<
                fibril_broker::queue_engine::OwnerStateCheckpoint,
                fibril_broker::broker::BrokerError,
            >,
        > {
            Box::pin(async { panic!("admitted replica must not use legacy checkpoint reset") })
        }
    }
    let exhausted = brokers[2]
        .catch_up_replication_follower_from_owner_with_checkpoint(
            &ExpiredHistory,
            "q",
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            BrokerReplicationCatchUpOptions {
                message_from: message_tail(4),
                event_from: 4,
                max_messages_per_read: 16,
                max_events_per_read: 16,
                max_bytes_per_read: 65536,
                max_iterations: 1,
                max_wait_ms: 0,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(exhausted, BrokerReplicationCatchUp::CheckpointRequired { .. }),
        "{exhausted:?}"
    );
    let intent = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for i in 0..3 {
                for result in brokers[i]
                    .apply_assignment_snapshot_transitions(
                        ["a", "b", "c"][i],
                        &CoordinationSnapshot::default(),
                        &providers[i].snapshot(),
                    )
                    .await
                {
                    result.unwrap();
                }
            }
            if let Some(intent) = providers[0].queue_reseed_work().unwrap().first().cloned() {
                if intent.phase == fibril_coordination_ganglion::queue_reseed::ReseedPhase::Revoking
                {
                    let _ = providers[0]
                        .acknowledge_queue_reseed(&intent, &brokers[0])
                        .await;
                }
            }
            if let Some(intent) = providers[2]
                .queue_reseed_work()
                .unwrap()
                .into_iter()
                .find(|i| i.phase == fibril_coordination_ganglion::queue_reseed::ReseedPhase::Ready)
            {
                break intent;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        brokers[0]
            .authorize_history_replication(&old_session, true)
            .is_err()
    );
    let view = providers[2]
        .prepare_queue_reseed(&intent, &brokers[2].engine())
        .await
        .unwrap();
    let peer = connect_protocol_owner_peer(
        config.nodes["a"].clone(),
        config.auth.as_ref(),
        None,
        "reseed-test",
        "1",
    )
    .await
    .unwrap()
    .with_history_session(
        intent
            .session(&providers[2].consensus_node().committed_snapshot())
            .unwrap(),
    );
    let cut = peer
        .export_owner_state_checkpoint("q", Partition::new(0), None)
        .await
        .unwrap();
    view.become_queue_follower_with_epoch("q", 0, None, 1)
        .await
        .unwrap();
    view.install_queue_learner_checkpoint(
        intent.receipt(),
        fibril_broker::queue_engine::FollowerStateCheckpointInstall {
            combined_log: cut.combined_log,
            event_start_offset: cut.event_start_offset,
            message_epoch: 1,
            event_epoch: 1,
            message_next_offset: cut.message_checkpoint_offset,
            event_next_offset: cut.event_next_offset,
            applied_event_offset: cut.applied_event_offset,
            state_snapshot: cut.state_snapshot,
        },
    )
    .await
    .unwrap();
    assert!(
        view.verify_queue_learner_caught_up("q", 0, None, 1, message_tail(4), 4)
            .await
            .is_err(),
        "checkpoint is incomplete until payload backfill finishes"
    );
    assert!(
        brokers[2]
            .engine()
            .queue_replication_next_offsets("q", 0, None)
            .await
            .is_err(),
        "original replica stays frozen throughout repair"
    );
    let confirm = publisher
        .publish(
            4u64.to_le_bytes().to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let b = healthy_peer.take().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
    let from = brokers[1].engine().queue_replication_next_offsets("q",0,None).await.unwrap();
    if from.1 >= 5 { break; }
    brokers[1].catch_up_replication_follower_from_owner(&b,"q",Partition::new(0),None,ReplicationResourceKind::Queue,BrokerReplicationCatchUpOptions {
        message_from:from.0,event_from:from.1,max_messages_per_read:4096,max_events_per_read:4096,max_bytes_per_read:16*1024*1024,max_iterations:16,max_wait_ms:0,
    }).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), confirm)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        4,
        "the healthy majority still confirms while c is repairing"
    );
    drop(b);
    assert_eq!(intent.assignment.resource,*resource);
    let repair_worker=fibril::recovery_driver::spawn(providers[2].clone(),brokers[2].clone(),config.clone());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if providers[0]
                .snapshot()
                .assignment_for("q", Partition::new(0), None)
                .is_some_and(|a| a.is_followed_by("c"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    repair_worker.abort();
    for i in 0..3 {
        for result in brokers[i]
            .apply_assignment_snapshot_transitions(
                ["a", "b", "c"][i],
                &CoordinationSnapshot::default(),
                &providers[i].snapshot(),
            )
            .await
        {
            result.unwrap();
        }
    }
    assert!(
        brokers[0]
            .authorize_history_replication(&old_session, true)
            .is_err(),
        "old transport remains invalid after readmission"
    );
    brokers[2]
        .engine()
        .verify_queue_learner_caught_up("q", 0, None, 1, message_tail(5), 5)
        .await
        .unwrap();
    let confirm = publisher
        .publish(
            5u64.to_le_bytes().to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let c = catch_up(2, 6).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), confirm)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        5,
        "re-admitted c can confirm with owner without b"
    );
    drop(c);
    // Both followers can lose the owner's retained prefix. Repeating c's repair
    // also exercises replacement of a previously installed generation.
    for node in ["b","c"] {
        tokio::time::timeout(Duration::from_secs(10),async {
            loop {
                if providers[0].request_queue_reseed(resource,node).await.is_ok() {break;}
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
    }
    let workers:Vec<_>=(1..3).map(|i|fibril::recovery_driver::spawn(providers[i].clone(),brokers[i].clone(),config.clone())).collect();
    tokio::time::timeout(Duration::from_secs(15),async {
        loop {
            for i in 0..3 {
                for result in brokers[i].apply_assignment_snapshot_transitions(["a","b","c"][i],&CoordinationSnapshot::default(),&providers[i].snapshot()).await {result.unwrap();}
            }
            if providers[0].queue_reseed_work().unwrap().is_empty() && providers[0].snapshot().assignment_for("q",Partition::new(0),None).is_some_and(|a|a.is_followed_by("b") && a.is_followed_by("c")) {break;}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    for worker in workers {worker.abort();}
    for i in 1..3 {
        brokers[i].engine().verify_queue_learner_caught_up("q",0,None,1,message_tail(6),6).await.unwrap();
    }
}
