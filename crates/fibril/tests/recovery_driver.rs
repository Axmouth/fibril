use fibril_broker::{
    Partition,
    broker::{Broker, BrokerConfig},
    coordination::Coordination,
    queue_engine::{KeratinConfig, QueueEngine, SnapshotConfig, StromaEngine, StromaKeratinConfig},
};
use fibril_coordination_ganglion::{
    GanglionCoordination,
    history_identity::resource_incarnation,
    initial_history::InitialHistoryReceiptSet,
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
        .register_initial_history_resource(&resource)
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
    let decision = provider.prepare_initial_history(&resource).await.unwrap();
    let receipt = provider
        .prepare_local_initial_history(&decision, &engine)
        .await
        .unwrap();
    let mut receipts = InitialHistoryReceiptSet::new(
        &provider.consensus_node().committed_snapshot(),
        decision.clone(),
    )
    .unwrap();
    receipts
        .record(
            &provider.consensus_node().committed_snapshot(),
            "a",
            receipt,
        )
        .unwrap();
    provider
        .persist_initial_history_quorum(&receipts)
        .await
        .unwrap();
    provider
        .commit_initial_history_activation(&decision, &engine)
        .await
        .unwrap();
    provider
        .admit_local_initial_history(&decision, &engine)
        .await
        .unwrap();
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
