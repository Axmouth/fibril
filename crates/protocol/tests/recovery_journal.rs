//! Real authenticated TCP pages, with a deliberately interruptible test authority.
//! This verifies download continuation, never consensus selection or activation.
use fibril_broker::{
    Partition,
    broker::{Broker, BrokerConfig, QueueOwnership},
    queue_engine::StromaEngine,
    recovery::{RecoverySealCommand, inspection::RecoveryInspectionLimits},
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_protocol::v1::{
    handler::{ConnectionSettings, handle_connection},
    recovery_inspection::{
        inspect_recovery_source_artifact, inspect_recovery_source_artifact_resumable,
        inspect_recovery_source_artifact_with_journal, read_cached_recovery_page,
    },
    replication::ProtocolOwnerPeerResolverConfig,
};
use fibril_util::{StaticAuthHandler, net::TcpListener};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug)]
struct Authority {
    remaining: AtomicUsize,
    reads: AtomicUsize,
}
impl QueueOwnership for Authority {
    fn owns_queue(&self, _: &str, _: Partition, _: Option<&str>) -> bool {
        false
    }
    fn authorize_recovery_seal<'a>(
        &'a self,
        _: &'a RecoverySealCommand,
    ) -> futures::future::BoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .map_err(|_| "test source temporarily unavailable".to_string())?;
            Ok("a".into())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inspection_download_resumes_over_tcp_then_revalidates_without_source() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test_data")
        .join(format!(
            "fibril-inspection-journal-{}",
            uuid::Uuid::new_v4()
        ));
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
    let authority = Arc::new(Authority {
        remaining: AtomicUsize::new(usize::MAX),
        reads: AtomicUsize::new(0),
    });
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
    let limits = RecoveryInspectionLimits {
        page_records: 2,
        ..Default::default()
    };
    let continued_limits = RecoveryInspectionLimits {
        total_pages: 1,
        total_records: 1,
        total_bytes: 1,
        ..limits
    };
    let journal = root.join("candidate");
    authority.remaining.store(2, Ordering::Relaxed);
    let first = inspect_recovery_source_artifact_with_journal(
        &config,
        &command,
        &sealed,
        continued_limits,
        Default::default(),
        4096,
        4096,
        Duration::from_secs(10),
        &journal,
    )
    .await;
    assert!(first.is_err());
    authority.remaining.store(usize::MAX, Ordering::Relaxed);
    let before = authority.reads.load(Ordering::Relaxed);
    let resumed = inspect_recovery_source_artifact_with_journal(
        &config,
        &command,
        &sealed,
        continued_limits,
        Default::default(),
        4096,
        4096,
        Duration::from_secs(10),
        &journal,
    )
    .await
    .unwrap();
    assert_eq!(authority.reads.load(Ordering::Relaxed) - before, 4);
    let direct = inspect_recovery_source_artifact(
        &config,
        &command,
        &sealed,
        limits,
        Default::default(),
        4096,
        4096,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(resumed, direct);
    let completed = inspect_recovery_source_artifact_resumable(
        &config,
        &command,
        &sealed,
        continued_limits,
        Default::default(),
        4096,
        4096,
        Duration::from_secs(10),
        &journal,
    )
    .await
    .unwrap();
    assert_eq!(completed, direct);
    stop.cancel();
    server.await.unwrap();
    let offline = ProtocolOwnerPeerResolverConfig::new(HashMap::new());
    let restored = inspect_recovery_source_artifact_with_journal(
        &offline,
        &command,
        &sealed,
        continued_limits,
        Default::default(),
        4096,
        4096,
        Duration::from_secs(10),
        &journal,
    )
    .await
    .unwrap();
    assert_eq!(restored, direct);
    assert_eq!(restored.evidence().history_id, sealed.seal.history.id);
    let mut wrong = command.clone();
    wrong.transition[0] ^= 1;
    assert!(
        inspect_recovery_source_artifact_with_journal(
            &offline,
            &wrong,
            &sealed,
            limits,
            Default::default(),
            4096,
            4096,
            Duration::from_secs(10),
            &journal
        )
        .await
        .is_err()
    );
    // An offline source's exact cached pages can populate a fresh private
    // target. The cache supplies bytes only, and Finish must verify the full
    // selected digest even if the page's native framing was accepted.
    let evidence = direct.evidence();
    let spec = stroma_core::QueueRecoveryStageSpec {
        plan: [11; 32], topic: "q".into(), partition: 0, group: None,
        binding: stroma_core::StorageHistoryBinding {
            resource_incarnation: [1; 16], accepted_history: [9; 16], writer_session: [10; 16],
        },
        fence_epoch: 8, source_history: evidence.history_id,
        log: evidence.log,
        snapshot_digest: direct.snapshot_digest(), snapshot_bytes: direct.state_snapshot().len() as u64,
        state_digest: evidence.lease_normalized_state_digest,
        live_payload_digest: evidence.live_payload_digest.unwrap(),
    };
    let target = stroma_core::Stroma::open(root.join("target"),
        stroma_core::StromaKeratinConfig::from_message_log(stroma_core::KeratinConfig::test_default()),
        stroma_core::SnapshotConfig::default()).await.unwrap();
    let stage = target.open_queue_recovery_stage(spec.clone(), direct.state_snapshot().to_vec(),
        stroma_core::RecoveryStageLimits::incremental()).await.unwrap();
    let request_at = |from| fibril_broker::recovery::RecoveryReadRequest {
        seal: sealed.seal.request.clone(), history_id: sealed.seal.history.id,
        source: fibril_broker::recovery::RecoveryReadSource::Log, from, max_records: limits.page_records,
        max_bytes: limits.page_bytes,
    };
    let mut next = spec.log.head;
    while next < spec.log.next {
        let page = read_cached_recovery_page(&journal, &command, &sealed, &request_at(next), Duration::from_secs(10))
            .await.unwrap().expect("all inspected pages remain available offline");
        next = stage.append(page).await.unwrap();
    }
    assert_eq!(stage.finish().await.unwrap().source_history, evidence.history_id);
    drop(stage);
    target.shutdown().await.unwrap();

    let mut another = sealed.clone();
    another.node_id = "another-replica".into();
    assert!(read_cached_recovery_page(&journal, &command, &another, &request_at(0), Duration::from_secs(10))
        .await.unwrap().is_none());
    assert!(read_cached_recovery_page(&journal, &wrong, &sealed, &request_at(0), Duration::from_secs(10))
        .await.is_err());
    let mut wrong_request = request_at(0);
    wrong_request.history_id[0] ^= 1;
    assert!(read_cached_recovery_page(&journal, &command, &sealed, &wrong_request, Duration::from_secs(10))
        .await.is_err());
    let mut absent_request = request_at(0);
    absent_request.max_records += 1;
    assert!(read_cached_recovery_page(&journal, &command, &sealed, &absent_request, Duration::from_secs(10))
        .await.unwrap().is_none());

    let corrupt_path = std::fs::read_dir(&journal).unwrap().filter_map(Result::ok)
        .filter(|e| e.file_type().unwrap().is_dir())
        .flat_map(|e| std::fs::read_dir(e.path()).unwrap().filter_map(Result::ok))
        .map(|e| e.path()).find(|p| p.extension().is_some_and(|ext| ext == "page")).unwrap();
    let original = std::fs::read(&corrupt_path).unwrap();
    let mut corrupt = original.clone();
    corrupt[8] ^= 1;
    std::fs::write(&corrupt_path, corrupt).unwrap();
    let mut refused = 0;
    for from in (0..12).step_by(2) {
        if read_cached_recovery_page(&journal, &command, &sealed, &request_at(from), Duration::from_secs(10))
            .await.is_err() { refused += 1; }
    }
    assert_eq!(refused, 1, "corrupt local pages must not silently fall back");
    std::fs::write(corrupt_path, original).unwrap();

    // A completed phase survives removal of its tentative pages and an offline
    // source. Altered recipes cannot reuse it or invent verified evidence.
    let cache = fibril_broker::recovery::journal::RecoveryInspectionJournal::open(
        &journal,
        fibril_broker::recovery::journal::RecoveryJournalIdentity {
            topic: command.topic.clone(),
            partition: command.partition.id(),
            group: command.group.clone(),
            replica: sealed.node_id.clone(),
            seal: sealed.seal.clone(),
        },
    )
    .unwrap();
    assert!(cache.clear_pages().unwrap() > 0);
    drop(cache);
    let completed = inspect_recovery_source_artifact_resumable(
        &offline,
        &command,
        &sealed,
        continued_limits,
        Default::default(),
        4096,
        4096,
        Duration::from_secs(10),
        &journal,
    )
    .await
    .unwrap();
    assert_eq!(completed, direct);
    assert!(
        inspect_recovery_source_artifact_resumable(
            &offline,
            &command,
            &sealed,
            continued_limits,
            Default::default(),
            4096,
            8192,
            Duration::from_secs(10),
            &journal
        )
        .await
        .is_err()
    );
    broker.shutdown().await;
}
