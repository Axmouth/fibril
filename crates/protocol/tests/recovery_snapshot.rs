//! Authenticated chunk transfer preserves the existing fresh authority gate.
use fibril_broker::{
    Partition,
    broker::{Broker, BrokerConfig, QueueOwnership},
    queue_engine::StromaEngine,
    recovery::{RecoverySealCommand, inspection::RecoveryInspectionLimits},
    recovery_transfer::{
        QueueRecoveryAuthorization, QueueRecoveryCommand, QueueRecoveryOperation as Op,
        QueueRecoveryReply as Reply, QueueRecoveryRequest,
    },
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_protocol::v1::{
    handler::{ConnectionSettings, handle_connection},
    recovery_inspection::inspect_recovery_source_artifact,
    recovery_transfer::request_transfer,
    replication::ProtocolOwnerPeerResolverConfig,
};
use fibril_util::{StaticAuthHandler, net::TcpListener};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Default)]
struct Authority {
    selected: Mutex<Option<QueueRecoveryAuthorization>>,
}
impl QueueOwnership for Authority {
    fn owns_queue(&self, _: &str, _: Partition, _: Option<&str>) -> bool {
        false
    }
    fn authorize_recovery_seal<'a>(
        &'a self,
        _: &'a RecoverySealCommand,
    ) -> futures::future::BoxFuture<'a, Result<String, String>> {
        Box::pin(async { Ok("a".into()) })
    }
    fn authorize_recovery_transfer<'a>(
        &'a self,
        command: &'a QueueRecoveryCommand,
    ) -> futures::future::BoxFuture<'a, Result<QueueRecoveryAuthorization, String>> {
        Box::pin(async move {
            let authority = self
                .selected
                .lock()
                .unwrap()
                .clone()
                .ok_or("no selected plan")?;
            if command.plan != authority.spec.plan
                || command.topic != authority.spec.topic
                || command.partition != authority.spec.partition
                || command.group != authority.spec.group
                || command.replica_id != "a"
            {
                return Err("wrong selected plan".into());
            }
            Ok(authority)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_snapshot_chunks_require_authority_and_complete_artifact() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test_data")
        .join(format!("fibril-snapshot-transfer-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = stroma_core::TempDir { root: root.clone() };
    let engine = StromaEngine::open(
        root.join("source"),
        stroma_core::StromaKeratinConfig::from_message_log(
            stroma_core::KeratinConfig::test_default(),
        ),
        stroma_core::SnapshotConfig::default(),
    )
    .await
    .unwrap();
    engine
        .initialize_empty_storage_history(
            "q",
            0,
            None,
            stroma_core::PartitionKind::Queue,
            stroma_core::StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
        )
        .await
        .unwrap();
    engine
        .become_queue_follower_with_epoch("q", 0, None, 1)
        .await
        .unwrap();
    engine
        .apply_replicated_queue_batch("q", 0, None, stroma_core::ReplicatedEventBatch {
                epoch: 1,
                first_offset: 0,
                durability: Some(stroma_core::KDurability::AfterFsync),
                encoded: None,
                events: (0..12)
                    .map(|n| stroma_core::StromaEvent::PayloadEnqueue {
                        not_before: None,
                        expire_at: None,
                        headers: vec![],
                        payload: vec![n; 128],
                    })
                    .collect(),
            })
        .await
        .unwrap();
    let authority = Arc::new(Authority::default());
    let broker =
        Broker::new_with_ownership(engine, BrokerConfig::default(), None, authority.clone());
    let command = RecoverySealCommand {
        topic: "q".into(),
        partition: Partition::new(0),
        group: None,
        stream: false,
        transition: [5; 32],
        fence_epoch: 8,
    };
    let sealed = broker
        .seal_replica_for_recovery(command.clone())
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
        "a".into(),
        listener.local_addr().unwrap().to_string(),
    )]))
    .with_auth("@node", "secret");
    let stop = tokio_util::sync::CancellationToken::new();
    let cancelled = stop.clone();
    let serving = broker.clone();
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (socket, peer) = tokio::select! { _ = cancelled.cancelled() => break, accepted = listener.accept() => accepted.unwrap() };
            let stats = ConnectionStats::new();
            let id = stats.add_connection(peer, Instant::now(), false);
            connections.spawn(handle_connection(
                socket,
                Some(peer),
                serving.clone(),
                TcpStats::new(10),
                stats,
                id,
                Some(StaticAuthHandler::new("@node".into(), "secret".into())),
                None,
                ConnectionSettings::new(Some(60)),
                None,
                None,
                None,
            ));
        }
        connections.shutdown().await;
    });

    let artifact = inspect_recovery_source_artifact(
        &config,
        &command,
        &sealed,
        RecoveryInspectionLimits::default(),
        Default::default(),
        4096,
        4096,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let snapshot = artifact.state_snapshot();
    let spec = stroma_core::QueueRecoveryStageSpec {
        plan: [9; 32],
        topic: "q".into(),
        partition: 0,
        group: None,
        binding: stroma_core::StorageHistoryBinding {
            resource_incarnation: [1; 16],
            accepted_history: [6; 16],
            writer_session: [7; 16],
        },
        fence_epoch: command.fence_epoch,
        source_history: sealed.seal.history.id,
        log: *sealed.seal.history.queue_log().unwrap(),
        snapshot_digest: artifact.snapshot_digest(),
        snapshot_bytes: snapshot.len() as u64,
        state_digest: artifact.evidence().lease_normalized_state_digest,
        live_payload_digest: artifact.evidence().live_payload_digest.unwrap(),
    };
    let auth = QueueRecoveryAuthorization {
        spec: spec.clone(),
        seal: sealed.seal.request.clone(),
        replica_process: [1; 16],
    };
    let request = |operation| QueueRecoveryRequest {
        command: QueueRecoveryCommand {
            replica_id: "a".into(),
            topic: "q".into(),
            partition: 0,
            group: None,
            plan: spec.plan,
        },
        operation,
    };
    let send = |operation| {
        let request = request(operation);
        let config = config.clone();
        async move { request_transfer(&config, &request, Duration::from_secs(10)).await }
    };
    assert!(send(Op::SnapshotProgress).await.is_err());
    *authority.selected.lock().unwrap() = Some(auth.clone());
    assert!(matches!(
        send(Op::SnapshotProgress).await.unwrap(),
        Reply::SnapshotProgress(0)
    ));
    send(Op::SnapshotAppend {
        from: 0,
        bytes: snapshot[..30].to_vec(),
    })
    .await
    .unwrap();
    assert!(send(Op::BeginTransferredSnapshot).await.is_err());
    *authority.selected.lock().unwrap() = None;
    assert!(
        send(Op::SnapshotAppend {
            from: 30,
            bytes: snapshot[30..].to_vec()
        })
        .await
        .is_err()
    );
    *authority.selected.lock().unwrap() = Some(auth);
    assert!(matches!(
        send(Op::SnapshotProgress).await.unwrap(),
        Reply::SnapshotProgress(30)
    ));
    send(Op::SnapshotAppend {
        from: 30,
        bytes: snapshot[30..].to_vec(),
    })
    .await
    .unwrap();
    // Matching local retained records can be reused after full artifact validation.
    assert!(matches!(
        send(Op::BeginTransferredSnapshot).await.unwrap(),
        Reply::Progress(12)
    ));
    assert!(
        send(Op::SnapshotRead {
            from: 0,
            max_bytes: 30
        })
        .await
        .is_err()
    );
    assert!(matches!(
        send(Op::Finish).await.unwrap(),
        Reply::Complete(_)
    ));
    let mut restored = vec![];
    while restored.len() < snapshot.len() {
        let Reply::SnapshotChunk { from, bytes } = send(Op::SnapshotRead {
            from: restored.len() as u64,
            max_bytes: 30,
        })
        .await
        .unwrap() else {
            panic!("expected snapshot chunk");
        };
        assert_eq!(from, restored.len() as u64);
        restored.extend_from_slice(&bytes);
    }
    assert_eq!(restored, snapshot);
    assert!(
        send(Op::SnapshotAppend {
            from: 0,
            bytes: vec![0; 30]
        })
        .await
        .is_err()
    );
    stop.cancel();
    server.await.unwrap();
    broker.shutdown().await;
}
