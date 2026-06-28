use std::{
    collections::{HashMap, HashSet as StdHashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use fibril_broker::{
    CompletionPair,
    broker::{
        Broker, BrokerAssignmentTransitionApply, BrokerConfig, BrokerError,
        BrokerFollowerReplicationApply, BrokerOwnerReplicationPeer,
        BrokerOwnerReplicationPeerResolver, BrokerOwnerReplicationRecords,
        BrokerReplicationCatchUp, BrokerReplicationCatchUpOptions,
        BrokerReplicationCatchUpProgress, BrokerReplicationCheckpointRequired, ConsumerConfig,
        FollowerReplicationWorkerConfig, FollowerReplicationWorkerLoopExit,
        FollowerReplicationWorkerState, FollowerReplicationWorkerStatus, OwnedQueue,
        QueueEvictionAttempt, QueueEvictionSkip, ReplicationResourceKind, SettleRequest, SettleType,
        StaticQueueOwnership,
    },
    coordination::{
        CoordinationSnapshot, LocalAssignmentIntent, LocalAssignmentRole,
        LocalAssignmentTransition, LocalStreamAssignmentTransition, NodeInfo, PartitionAssignment,
        QueueIdentity, StaticCoordination, StreamAssignment, StreamIdentity,
    },
    queue_engine::{
        Deliverable, DestroyOutcome, EvictOutcome, FollowerStateCheckpointInstall, InspectMode,
        IoError, KeratinAppendCompletion, Message, MessageHeaders, OwnerReplicationBatch,
        OwnerReplicationRead, OwnerStateCheckpoint, QueueEngine, QueuePromotionOutcome,
        ReplayDeadLetterOutcome, ReplayDeadLettersReport, SettleRequest as EngineSettleRequest,
        StromaEngine,
    },
    test_util::TestState,
};
use fibril_metrics::{Metrics, QueuesStateSnapshot};
use fibril_storage::{DeliverableMessage, Offset, Partition};
use fibril_util::unix_millis;
use futures::future::BoxFuture;
use hashbrown::HashSet;
use stroma_core::{
    AckEventMeta, AppendCompletion, DLQDiscardPolicyWire, DeclareMeta, GlobalDLQ,
    GlobalDlqSnapshot, GlobalDlqUpdateOutcome, KeratinConfig, MessageInspectionPage, PublishItem,
    QueueRole, SnapshotConfig, StromaDebugSnapshot, StromaError, StromaKeratinConfig,
    StromaMetrics, TempDir, test_dir,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct FailingPublishEngine;

// This engine exercises the queue publish-failure path only; it hosts no streams,
// so every stream operation reports unsupported.
#[async_trait]
impl fibril_broker::queue_engine::StreamStore for FailingPublishEngine {
    async fn create_stream(
        &self,
        _tp: &str,
        _part: u32,
        _retention: Option<fibril_broker::queue_engine::RetentionConfig>,
    ) -> Result<(), StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }
    async fn append_stream_record(
        &self,
        _tp: &str,
        _part: u32,
        _headers: &MessageHeaders,
        _payload: Vec<u8>,
    ) -> Result<Offset, StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }
    async fn append_stream_records_enqueue(
        &self,
        _tp: &str,
        _part: u32,
        _records: Vec<(MessageHeaders, Vec<u8>)>,
        _durability: Option<fibril_broker::queue_engine::KDurability>,
    ) -> Result<fibril_broker::queue_engine::EnqueuedStreamAppend, StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }
    async fn sync_stream(&self, _tp: &str, _part: u32) -> Result<(), StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }
    async fn read_stream_records(
        &self,
        _tp: &str,
        _part: u32,
        _from: Offset,
        _max: usize,
    ) -> Result<Vec<(Offset, Vec<u8>, MessageHeaders)>, StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }
    async fn commit_stream_cursors(
        &self,
        _tp: &str,
        _part: u32,
        _commits: Vec<(String, Offset)>,
    ) -> Result<(), StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }
    async fn stream_cursor(
        &self,
        _tp: &str,
        _part: u32,
        _name: &str,
    ) -> Result<Option<Offset>, StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }
    async fn stream_head_tail(
        &self,
        _tp: &str,
        _part: u32,
    ) -> Result<(Offset, Offset), StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }
    async fn stream_offset_at_or_after_time(
        &self,
        _tp: &str,
        _part: u32,
        _ts_ms: u64,
    ) -> Result<Offset, StromaError> {
        Err(StromaError::Unsupported("no streams in test engine".into()))
    }

    fn durable_is_stream(&self, _tp: &str, _part: u32) -> bool {
        false
    }
}

#[async_trait]
impl QueueEngine for FailingPublishEngine {
    async fn poll_ready(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _max: usize,
        _lease_deadline: u64,
        _upper: Offset,
    ) -> Result<Vec<Deliverable>, StromaError> {
        unimplemented!()
    }

    async fn ack(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _offset: Offset,
        _completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }

    async fn ack_batch(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _items: Vec<stroma_core::AckEventMeta>,
        _completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }

    async fn nack(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _offset: Offset,
        _requeue: bool,
        _completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }

    async fn nack_batch(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _items: Vec<stroma_core::NackEventMeta>,
        _completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }

    async fn settle(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _req: EngineSettleRequest,
        _completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }

    async fn release_inflight_batch(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _items: Vec<AckEventMeta>,
        _completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }

    async fn publish(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _headers: &MessageHeaders,
        _payload: Vec<u8>,
        _completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }

    async fn publish_batch(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _items: Vec<PublishItem>,
    ) -> Result<(), StromaError> {
        Err(StromaError::Io(
            "Keratin already open for test queue".into(),
        ))
    }

    async fn next_expiry_hint(&self) -> Result<Option<u64>, StromaError> {
        Ok(None)
    }

    async fn requeue_expired(
        &self,
        _now: u64,
        _max: usize,
    ) -> Result<StdHashSet<(String, u32, Option<String>, u64)>, StromaError> {
        Ok(StdHashSet::new())
    }

    async fn drop_ttl_expired(
        &self,
        _now: u64,
        _max: usize,
    ) -> Result<StdHashSet<(String, u32, Option<String>, u64)>, StromaError> {
        Ok(StdHashSet::new())
    }

    async fn shutdown(&self) -> Result<(), StromaError> {
        Ok(())
    }

    async fn estimate_disk_used(&self) -> Result<u64, StromaError> {
        Ok(0)
    }

    async fn list_partitions(&self) -> Result<Vec<(String, u32, Option<String>)>, StromaError> {
        Ok(Vec::new())
    }

    async fn queue_stats_snapshot(&self) -> Result<QueuesStateSnapshot, StromaError> {
        Ok(QueuesStateSnapshot {
            queues: Default::default(),
        })
    }

    async fn debug_snapshot(&self) -> Result<StromaDebugSnapshot, StromaError> {
        unimplemented!()
    }

    async fn inspect_messages(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _from: Offset,
        _limit: usize,
        _mode: InspectMode,
        _include_payload: bool,
        _payload_limit_bytes: usize,
    ) -> Result<MessageInspectionPage, StromaError> {
        unimplemented!()
    }

    async fn replay_dead_letters(
        &self,
        _dlq_tp: &str,
        _dlq_group: Option<&str>,
        _offsets: &[Offset],
    ) -> Result<ReplayDeadLettersReport, StromaError> {
        unimplemented!()
    }

    async fn global_dlq(&self) -> Result<GlobalDlqSnapshot, StromaError> {
        unimplemented!()
    }

    async fn set_global_dlq(
        &self,
        _target: Option<GlobalDLQ>,
        _expected_version: u64,
    ) -> Result<GlobalDlqUpdateOutcome, StromaError> {
        unimplemented!()
    }

    async fn declare_queue(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _meta: DeclareMeta,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }

    async fn materialize(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
    ) -> Result<(), StromaError> {
        Ok(())
    }

    async fn become_queue_owner_with_epoch(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _epoch: u64,
    ) -> Result<(), StromaError> {
        Ok(())
    }

    async fn unmaterialize(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
    ) -> Result<EvictOutcome, StromaError> {
        unimplemented!()
    }

    async fn destroy_partition(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
    ) -> Result<DestroyOutcome, StromaError> {
        unimplemented!()
    }

    fn is_materialized(&self, _tp: &str, _part: u32, _group: Option<&str>) -> bool {
        false
    }

    async fn has_inflight(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
    ) -> Result<bool, StromaError> {
        Ok(false)
    }

    async fn lowest_unsettled_offset(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
    ) -> Result<Offset, StromaError> {
        Ok(0)
    }

    async fn current_next_offset(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
    ) -> Result<Offset, StromaError> {
        Ok(0)
    }

    async fn next_deliverable(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _from: Offset,
        _upper: Offset,
    ) -> Result<Option<Offset>, StromaError> {
        Ok(None)
    }

    fn metrics(&self) -> Arc<StromaMetrics> {
        Arc::new(StromaMetrics::default())
    }

    fn deadline_awaker(&self) -> Arc<Notify> {
        Arc::new(Notify::new())
    }

    fn quarantined_partitions(&self) -> Vec<stroma_core::QuarantineInfo> {
        Vec::new()
    }

    fn recovery_mismatch_policy(&self) -> stroma_core::RecoveryMismatchPolicy {
        stroma_core::RecoveryMismatchPolicy::Quarantine
    }

    async fn repair_partition(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
    ) -> Result<(), StromaError> {
        unimplemented!()
    }
}

async fn open_test_broker() -> (Arc<Broker<StromaEngine>>, TempDir) {
    let broker_cfg = BrokerConfig {
        inflight_ttl_ms: 2000,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000, // Make tests timeout if they rely on polling to pass, to indicate the issue
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    };
    open_test_broker_with_cfg(broker_cfg).await
}

async fn open_test_broker_with_cfg(cfg: BrokerConfig) -> (Arc<Broker<StromaEngine>>, TempDir) {
    let dir = test_dir!("broker_test");

    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new(engine, cfg, None);

    (broker, dir)
}

async fn open_test_broker_with_metrics(
    cfg: BrokerConfig,
    metrics: Metrics,
) -> (Arc<Broker<StromaEngine>>, TempDir) {
    let dir = test_dir!("broker_test");

    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new(engine, cfg, Some(metrics.broker()));

    (broker, dir)
}

async fn open_test_broker_with_ownership(
    ownership: Arc<dyn fibril_broker::broker::QueueOwnership>,
) -> (Arc<Broker<StromaEngine>>, TempDir) {
    let dir = test_dir!("broker_test");

    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new_with_ownership(engine, BrokerConfig::default(), None, ownership);

    (broker, dir)
}

#[tokio::test]
async fn stream_ownership_gate_rejects_non_owner_and_exposes_config() {
    use fibril_broker::broker::{
        OwnAllQueues, StreamOpenConfig, StreamOwnership as StreamOwnershipTrait,
    };
    use fibril_broker::stream::StreamDurability;

    // A node that owns no stream partitions but knows the declared config (the
    // cluster non-owner case): the gate must reject, yet the config is visible so
    // the actual owner could materialize.
    #[derive(Debug)]
    struct OwnsNoStreams;
    impl StreamOwnershipTrait for OwnsNoStreams {
        fn owns_stream(&self, _topic: &str, _partition: Partition) -> bool {
            false
        }
        fn stream_open_config(&self, _topic: &str) -> Option<StreamOpenConfig> {
            Some(StreamOpenConfig {
                durability: StreamDurability::Durable,
                retention: None,
            })
        }
    }

    let dir = test_dir!("broker_test");
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new_with_ownerships(
        engine,
        BrokerConfig::default(),
        None,
        Arc::new(OwnAllQueues),
        Arc::new(OwnsNoStreams),
    );

    assert!(broker.stream_declared_in_coordination("events"));
    assert!(!broker.owns_stream("events", 0));
    assert!(
        broker.ensure_stream_owner("events", 0).is_err(),
        "a non-owner must be rejected so the handler can redirect"
    );
}

async fn open_test_engine() -> (StromaEngine, TempDir) {
    let dir = test_dir!("broker_test");
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();

    (engine, dir)
}

#[tokio::test]
async fn static_ownership_rejects_publisher_for_unowned_queue() {
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(StdHashSet::new())))
            .await;

    let err = match broker
        .get_publisher("unowned", Partition::new(0), &None)
        .await
    {
        Ok(_) => panic!("unowned queue unexpectedly accepted a publisher"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        BrokerError::NotOwner {
            topic,
            partition: _,
            group: None,
        } if topic == "unowned"
    ));
    assert!(!broker.is_queue_materialized("unowned", None));
    broker.shutdown().await;
}

#[tokio::test]
async fn static_ownership_rejects_subscriber_for_unowned_queue() {
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(StdHashSet::new())))
            .await;

    let err = match broker
        .subscribe(
            "unowned",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await
    {
        Ok(_) => panic!("unowned queue unexpectedly accepted a subscriber"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        BrokerError::NotOwner {
            topic,
            partition: _,
            group: None,
        } if topic == "unowned"
    ));
    assert!(!broker.is_queue_materialized("unowned", None));
    broker.shutdown().await;
}

#[tokio::test]
async fn static_ownership_allows_owned_queue() {
    let mut owned = StdHashSet::new();
    owned.insert(OwnedQueue::new("owned", Partition::new(0), Some("workers")));
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(owned))).await;
    let group = Some("workers".to_string());

    let (publisher, _confirms) = broker
        .get_publisher("owned", Partition::new(0), &group)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"hello".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let mut sub = broker
        .subscribe(
            "owned",
            Partition::new(0),
            Some("workers"),
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let msg = recv_with_timeout(&mut sub, 1000)
        .await
        .expect("owned queue should deliver published message");
    assert_eq!(msg.message.offset, 0);
    broker.shutdown().await;
}

#[tokio::test]
async fn partition_lowest_unsettled_offset_reaches_boundary_when_drained() {
    let mut owned = StdHashSet::new();
    owned.insert(OwnedQueue::new("drain-probe", Partition::new(0), None));
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(owned))).await;

    // Publish three messages: the cutover boundary would be the next offset, 3.
    let (publisher, _confirms) = broker
        .get_publisher("drain-probe", Partition::new(0), &None)
        .await
        .unwrap();
    for _ in 0..3 {
        publisher
            .publish(
                b"m".to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
    }

    // The cutover boundary is the partition's next write offset (3 here).
    let boundary = broker
        .partition_next_offset("drain-probe", Partition::new(0), None)
        .await
        .unwrap();
    assert_eq!(boundary, 3);

    // Nothing settled yet: the lowest unacked offset is below the boundary.
    assert!(
        broker
            .partition_lowest_unsettled_offset("drain-probe", Partition::new(0), None)
            .await
            .unwrap()
            < boundary
    );

    // Consume and ack all three; the settled offset reaches the boundary.
    let mut sub = broker
        .subscribe(
            "drain-probe",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();
    for _ in 0..3 {
        let msg = recv_with_timeout(&mut sub, 1000).await.expect("delivery");
        sub.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: msg.delivery_tag,
        })
        .await
        .unwrap();
    }

    // Poll briefly for the ack to settle.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let settled = broker
            .partition_lowest_unsettled_offset("drain-probe", Partition::new(0), None)
            .await
            .unwrap();
        if settled >= boundary {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "settled offset never reached the boundary (last={settled})"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    broker.shutdown().await;
}

#[tokio::test]
async fn repartition_delivery_hold_blocks_until_released() {
    let mut owned = StdHashSet::new();
    owned.insert(OwnedQueue::new("repartition-hold", Partition::new(0), None));
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(owned))).await;

    // Subscribe first so the partition's delivery loop exists, then hold it.
    let mut sub = broker
        .subscribe(
            "repartition-hold",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 4 },
        )
        .await
        .unwrap();
    broker.set_partition_delivery_held("repartition-hold", Partition::new(0), None, true);

    // A message published while held is not delivered.
    let (publisher, _confirms) = broker
        .get_publisher("repartition-hold", Partition::new(0), &None)
        .await
        .unwrap();
    publisher
        .publish(
            b"held".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert!(
        recv_with_timeout(&mut sub, 300).await.is_none(),
        "a held partition delivers nothing"
    );

    // Releasing the hold delivers the queued message.
    broker.set_partition_delivery_held("repartition-hold", Partition::new(0), None, false);
    let msg = recv_with_timeout(&mut sub, 1000)
        .await
        .expect("released partition delivers the queued message");
    assert_eq!(msg.message.offset, 0);
    broker.shutdown().await;
}

#[tokio::test]
async fn repartition_transition_holds_new_partition_until_source_drains() {
    let mut owned = StdHashSet::new();
    owned.insert(OwnedQueue::new("grow", Partition::new(0), None));
    owned.insert(OwnedQueue::new("grow", Partition::new(1), None));
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(owned))).await;

    // Grow 1 -> 2: partition 1 is new and sources from partition 0 (1 % 1).
    broker.apply_repartition_transition("grow", None, 1, 1, 2);

    // Subscribing to the new partition starts its loop held.
    let mut new_sub = broker
        .subscribe(
            "grow",
            Partition::new(1),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 4 },
        )
        .await
        .unwrap();
    let (pub1, _c1) = broker
        .get_publisher("grow", Partition::new(1), &None)
        .await
        .unwrap();
    pub1.publish(
        b"new".to_vec(),
        unix_millis(),
        unix_millis(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    assert!(
        recv_with_timeout(&mut new_sub, 300).await.is_none(),
        "new partition is held while its source has not drained"
    );

    // The old partition delivers normally throughout the transition.
    let mut old_sub = broker
        .subscribe(
            "grow",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 4 },
        )
        .await
        .unwrap();
    let (pub0, _c0) = broker
        .get_publisher("grow", Partition::new(0), &None)
        .await
        .unwrap();
    pub0.publish(
        b"old".to_vec(),
        unix_millis(),
        unix_millis(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    assert!(
        recv_with_timeout(&mut old_sub, 1000).await.is_some(),
        "old partition delivers during the transition"
    );

    // Once the source partition drains cluster-wide, the new partition lifts.
    broker.apply_repartition_drained("grow", None, StdHashSet::from([0u32]));
    let msg = recv_with_timeout(&mut new_sub, 1000)
        .await
        .expect("new partition delivers after its source drains");
    assert_eq!(msg.message.payload, b"new");
    broker.shutdown().await;
}

#[tokio::test]
async fn repartition_drain_detection_lifts_new_partition_after_old_acks() {
    let mut owned = StdHashSet::new();
    owned.insert(OwnedQueue::new("grow2", Partition::new(0), None));
    owned.insert(OwnedQueue::new("grow2", Partition::new(1), None));
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(owned))).await;

    // Grow 1 -> 2. Partition 1 (new) sources from partition 0 (old).
    broker.apply_repartition_transition("grow2", None, 1, 1, 2);

    // Consumer on the old partition (so its loop exists for drain checks), and on
    // the new partition (which starts held).
    let mut old_sub = broker
        .subscribe(
            "grow2",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();
    let mut new_sub = broker
        .subscribe(
            "grow2",
            Partition::new(1),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();

    // Two pre-cutover messages in the old partition (boundary becomes 2).
    let (pub0, _c0) = broker
        .get_publisher("grow2", Partition::new(0), &None)
        .await
        .unwrap();
    for _ in 0..2 {
        pub0.publish(
            b"old".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    }
    // A post-cutover message routed to the new partition: held.
    let (pub1, _c1) = broker
        .get_publisher("grow2", Partition::new(1), &None)
        .await
        .unwrap();
    pub1.publish(
        b"new".to_vec(),
        unix_millis(),
        unix_millis(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();

    // Capture the boundary; nothing acked yet, so the old partition is not drained.
    broker.refresh_repartition_drain("grow2", None).await;
    assert!(
        broker
            .repartition_drained_reports()
            .iter()
            .all(|r| r.drained.is_empty()),
        "old partition is not drained before its backlog is acked"
    );
    assert!(
        recv_with_timeout(&mut new_sub, 200).await.is_none(),
        "new partition stays held while the source has backlog"
    );

    // Drain the old partition's backlog.
    for _ in 0..2 {
        let msg = recv_with_timeout(&mut old_sub, 1000)
            .await
            .expect("old delivery");
        old_sub
            .settle(SettleRequest {
                settle_type: SettleType::Ack,
                delivery_tag: msg.delivery_tag,
            })
            .await
            .unwrap();
    }

    // Now drain detection reports partition 0 drained.
    let drained = loop {
        broker.refresh_repartition_drain("grow2", None).await;
        let reports = broker.repartition_drained_reports();
        if reports.iter().any(|r| r.drained.contains(&0)) {
            break reports;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].version, 1);

    // Feeding that back (as the watcher would, via the cluster drained set) lifts
    // the new partition, which now delivers its held message.
    broker.apply_repartition_drained("grow2", None, StdHashSet::from([0u32]));
    let msg = recv_with_timeout(&mut new_sub, 1000)
        .await
        .expect("new partition delivers once drain is detected");
    assert_eq!(msg.message.payload, b"new");
    broker.shutdown().await;
}

#[tokio::test]
async fn shrink_survivor_holds_moved_key_until_removed_source_drains() {
    let mut owned = StdHashSet::new();
    owned.insert(OwnedQueue::new("merge", Partition::new(0), None));
    owned.insert(OwnedQueue::new("merge", Partition::new(1), None));
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(owned))).await;

    // Pre-cutover backlog in the removed partition (1), which merges into 0.
    let (pub1, _c1) = broker
        .get_publisher("merge", Partition::new(1), &None)
        .await
        .unwrap();
    pub1.publish(
        b"old1".to_vec(),
        unix_millis(),
        unix_millis(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();

    let mut survivor = broker
        .subscribe(
            "merge",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();
    let mut removed = broker
        .subscribe(
            "merge",
            Partition::new(1),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();

    // Shrink 2 -> 1: surviving partition 0 sources from {0, 1}.
    broker.apply_repartition_transition("merge", None, 1, 2, 1);
    broker.refresh_repartition_drain("merge", None).await;

    // A post-cutover message routed to the survivor (a moved key): held until the
    // removed source drains.
    let (pub0, _c0) = broker
        .get_publisher("merge", Partition::new(0), &None)
        .await
        .unwrap();
    pub0.publish(
        b"moved".to_vec(),
        unix_millis(),
        unix_millis(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    assert!(
        recv_with_timeout(&mut survivor, 300).await.is_none(),
        "survivor holds the moved key while the removed source has backlog"
    );

    // Drain the removed partition's backlog.
    let msg = recv_with_timeout(&mut removed, 1000)
        .await
        .expect("removed delivery");
    removed
        .settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: msg.delivery_tag,
        })
        .await
        .unwrap();

    // Refresh detects both sources drained; feed the cluster drained set back.
    let drained = loop {
        broker.refresh_repartition_drain("merge", None).await;
        let reports = broker.repartition_drained_reports();
        let set: StdHashSet<u32> = reports
            .iter()
            .flat_map(|r| r.drained.iter().copied())
            .collect();
        if set.contains(&0) && set.contains(&1) {
            break set;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    broker.apply_repartition_drained("merge", None, drained);
    broker.refresh_repartition_drain("merge", None).await;

    // The survivor now delivers the held moved key.
    let moved = recv_with_timeout(&mut survivor, 1000)
        .await
        .expect("survivor delivers the moved key once sources drain");
    assert_eq!(moved.message.payload, b"moved");
    broker.shutdown().await;
}

#[tokio::test]
async fn shrink_hold_delivers_below_boundary_and_holds_above() {
    let mut owned = StdHashSet::new();
    owned.insert(OwnedQueue::new("survivor", Partition::new(0), None));
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(owned))).await;

    let mut sub = broker
        .subscribe(
            "survivor",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();

    // Hold at boundary 2: offsets 0,1 (pre-cutover) deliver, 2,3 (post-cutover,
    // possibly moved keys) are held until the merge sources drain.
    broker.set_partition_hold_above_offset("survivor", Partition::new(0), None, Some(2));

    let (publisher, _c) = broker
        .get_publisher("survivor", Partition::new(0), &None)
        .await
        .unwrap();
    for _ in 0..4 {
        publisher
            .publish(
                b"m".to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
    }

    // Below-boundary messages deliver.
    let a = recv_with_timeout(&mut sub, 1000).await.expect("offset 0");
    let b = recv_with_timeout(&mut sub, 1000).await.expect("offset 1");
    assert_eq!(a.message.offset, 0);
    assert_eq!(b.message.offset, 1);
    // Above-boundary messages are held.
    assert!(
        recv_with_timeout(&mut sub, 300).await.is_none(),
        "offsets at/above the boundary are held during a shrink"
    );

    // Releasing the hold delivers the rest.
    broker.set_partition_hold_above_offset("survivor", Partition::new(0), None, None);
    let c = recv_with_timeout(&mut sub, 1000).await.expect("offset 2");
    assert_eq!(c.message.offset, 2);
    broker.shutdown().await;
}

#[tokio::test]
async fn static_coordination_can_drive_broker_ownership_gate() {
    let local_node = "node-a".to_string();
    let remote_node = "node-b".to_string();
    let mut nodes = HashMap::new();
    nodes.insert(
        local_node.clone(),
        NodeInfo {
            node_id: local_node.clone(),
            broker_addr: "127.0.0.1:1001".to_string(),
            admin_addr: None,
        },
    );
    nodes.insert(
        remote_node.clone(),
        NodeInfo {
            node_id: remote_node.clone(),
            broker_addr: "127.0.0.1:1002".to_string(),
            admin_addr: None,
        },
    );

    let owned_queue = QueueIdentity::new("coord-owned", Partition::new(0), Some("workers"));
    let followed_queue = QueueIdentity::new("coord-followed", Partition::new(0), Some("workers"));
    let mut assignments = HashMap::new();
    assignments.insert(
        owned_queue.clone(),
        PartitionAssignment::new(
            owned_queue,
            local_node.clone(),
            vec![remote_node.clone()],
            1,
        ),
    );
    assignments.insert(
        followed_queue.clone(),
        PartitionAssignment::new(followed_queue, remote_node, vec![local_node.clone()], 1),
    );

    let coordination = StaticCoordination::new(
        local_node,
        CoordinationSnapshot {
            nodes,
            assignments,
            stream_assignments: std::collections::HashMap::new(),
            generation: 1,
        },
    );
    let (broker, _dir) = open_test_broker_with_ownership(Arc::new(coordination)).await;
    let group = Some("workers".to_string());

    let (publisher, _confirms) = broker
        .get_publisher("coord-owned", Partition::new(0), &group)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"hello".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let err = match broker
        .get_publisher("coord-followed", Partition::new(0), &group)
        .await
    {
        Ok(_) => panic!("followed queue unexpectedly accepted an owner publisher"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        BrokerError::NotOwner {
            topic,
            partition: _,
            group: Some(group),
        } if topic == "coord-followed" && group == "workers"
    ));
    assert!(!broker.is_queue_materialized("coord-followed", Some("workers")));
    broker.shutdown().await;
}

fn assignment_transition(
    topic: &str,
    intent: LocalAssignmentIntent,
    previous_role: Option<LocalAssignmentRole>,
    next_role: Option<LocalAssignmentRole>,
) -> LocalAssignmentTransition {
    LocalAssignmentTransition {
        queue: QueueIdentity::new(topic, Partition::new(0), Some("workers")),
        previous_role,
        next_role,
        previous: None,
        next: None,
        intent,
    }
}

fn coordination_nodes() -> HashMap<String, NodeInfo> {
    let mut nodes = HashMap::new();
    nodes.insert(
        "node-a".to_string(),
        NodeInfo {
            node_id: "node-a".to_string(),
            broker_addr: "127.0.0.1:1001".to_string(),
            admin_addr: None,
        },
    );
    nodes.insert(
        "node-b".to_string(),
        NodeInfo {
            node_id: "node-b".to_string(),
            broker_addr: "127.0.0.1:1002".to_string(),
            admin_addr: None,
        },
    );
    nodes
}

fn coordination_snapshot(
    assignments: Vec<PartitionAssignment>,
    generation: u64,
) -> CoordinationSnapshot {
    CoordinationSnapshot {
        nodes: coordination_nodes(),
        assignments: assignments
            .into_iter()
            .map(|assignment| (assignment.queue.clone(), assignment))
            .collect(),
        stream_assignments: std::collections::HashMap::new(),
        generation,
    }
}

#[tokio::test]
async fn assignment_transition_apply_can_make_queue_follower() {
    let (broker, _dir) = open_test_broker().await;
    let transition = assignment_transition(
        "transition-followed",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );

    let outcome = broker
        .apply_assignment_transition(&transition)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::BecomeFollower)
    );
    assert!(broker.has_follower_replication_worker(
        "transition-followed",
        Partition::new(0),
        Some("workers")
    ));

    let err = broker
        .read_owner_replication_records(
            "transition-followed",
            Partition::new(0),
            Some("workers"),
            0,
            0,
            1,
            1,
            usize::MAX,
            0,
        )
        .await
        .expect_err("follower queue should reject owner replication reads");
    assert!(matches!(
        err,
        BrokerError::Engine(StromaError::WrongQueueRole { .. })
    ));
    broker.shutdown().await;
}

#[tokio::test]
async fn assignment_transition_apply_can_stop_follower() {
    let (broker, _dir) = open_test_broker().await;
    let become_follower = assignment_transition(
        "transition-stop-follower",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    broker
        .apply_assignment_transition(&become_follower)
        .await
        .unwrap();
    assert!(broker.has_follower_replication_worker(
        "transition-stop-follower",
        Partition::new(0),
        Some("workers")
    ));

    let stop = assignment_transition(
        "transition-stop-follower",
        LocalAssignmentIntent::StopFollower,
        Some(LocalAssignmentRole::Follower),
        None,
    );
    let outcome = broker.apply_assignment_transition(&stop).await.unwrap();

    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::StopFollower)
    );
    assert!(!broker.has_follower_replication_worker(
        "transition-stop-follower",
        Partition::new(0),
        Some("workers")
    ));

    let snapshot = broker.debug_snapshot().await.unwrap();
    let queue = snapshot
        .queues
        .iter()
        .find(|queue| {
            queue.topic == "transition-stop-follower"
                && queue.partition == 0
                && queue.group.as_deref() == Some("workers")
        })
        .expect("stopped follower should remain materialized but frozen");
    assert_eq!(queue.role, QueueRole::Frozen);

    let records = BrokerOwnerReplicationRecords {
        messages: OwnerReplicationRead::Batch(OwnerReplicationBatch::<Message> {
            epoch: 0,
            requested_offset: 0,
            next_offset: 0,
            records: Vec::new(),
        }),
        events: OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch: 0,
            requested_offset: 0,
            next_offset: 0,
            records: Vec::new(),
        }),
    };
    let err = broker
        .apply_follower_replication_records(
            "transition-stop-follower",
            Partition::new(0),
            Some("workers"),
            ReplicationResourceKind::Queue,
            records,
        )
        .await
        .expect_err("stopped follower should reject replicated ingest");
    assert!(matches!(
        err,
        BrokerError::Engine(StromaError::WrongQueueRole {
            expected: QueueRole::Follower,
            actual: QueueRole::Frozen
        })
    ));

    broker.shutdown().await;
}

#[tokio::test]
async fn refresh_follower_keeps_replication_worker_progress() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;
    let become_follower = assignment_transition(
        "transition-refresh-follower",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&become_follower)
        .await
        .unwrap();

    let group = Some("workers".to_string());
    let (publisher, _confirms) = owner
        .get_publisher("transition-refresh-follower", Partition::new(0), &group)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"before refresh".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let queue = QueueIdentity::new(
        "transition-refresh-follower",
        Partition::new(0),
        Some("workers"),
    );
    follower
        .run_follower_replication_worker_once(
            &owner,
            &queue,
            ReplicationResourceKind::Queue,
            FollowerReplicationWorkerConfig::default(),
        )
        .await
        .unwrap();
    let before = follower
        .follower_replication_worker_snapshot(
            "transition-refresh-follower",
            Partition::new(0),
            Some("workers"),
        )
        .await
        .unwrap();

    let refresh = assignment_transition(
        "transition-refresh-follower",
        LocalAssignmentIntent::RefreshFollower,
        Some(LocalAssignmentRole::Follower),
        Some(LocalAssignmentRole::Follower),
    );
    let outcome = follower
        .apply_assignment_transition(&refresh)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Noop(LocalAssignmentIntent::RefreshFollower)
    );
    let after = follower
        .follower_replication_worker_snapshot(
            "transition-refresh-follower",
            Partition::new(0),
            Some("workers"),
        )
        .await
        .unwrap();
    assert_eq!(after, before);

    owner.shutdown().await;
    follower.shutdown().await;
}

/// Publish-confirm enforcement: with a replica-durable assignment cached,
/// the producer's confirm resolves only after a follower's reported durable
/// progress passes the message offset — and times out with a descriptive
/// error when no follower reports.
#[tokio::test]
async fn publish_confirm_waits_for_follower_durable_progress() {
    let topic = "confirm-policy";
    let (broker, _dir) = open_test_broker_with_cfg(BrokerConfig {
        inflight_ttl_ms: 2000,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100_000,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        replication_confirm_timeout_ms: 400,
        ..Default::default()
    })
    .await;

    // The assignment demands TWO durable nodes: the owner plus one follower.
    broker.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(topic, Partition::new(0), None),
            "owner-a",
            vec!["follower-b".to_string()],
            1,
        )
        .with_durability(
            fibril_broker::coordination::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 },
        ),
    );

    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();

    // No follower progress: the confirm must time out with a clear reason.
    let reply = publisher
        .publish(
            b"needs-replica".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let err = reply
        .await
        .unwrap()
        .expect_err("confirm must fail without follower progress");
    let message = format!("{err:?}");
    assert!(
        message.contains("timed out") && message.contains("follower"),
        "descriptive timeout error expected, got: {message}"
    );

    // With follower progress reported past the offset, the confirm resolves.
    let progress_broker = broker.clone();
    let reporter = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Both messages (offsets 0 and 1) durably applied on the follower.
        progress_broker.record_follower_replication_progress(
            topic,
            Partition::new(0),
            None,
            "follower-b",
            2,
            2,
        );
    });
    let reply = publisher
        .publish(
            b"replicated".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let offset = reply
        .await
        .unwrap()
        .expect("confirm resolves once the follower reports durable progress");
    assert_eq!(offset, 1);
    reporter.await.unwrap();

    broker.shutdown().await;
}

/// In-sync-replica floor (Kafka min.insync.replicas): when the assigned
/// replica set is smaller than the configured floor, a replica-durable publish
/// is refused immediately — it can never satisfy the floor. A long confirm
/// timeout combined with the tight time bound proves the refusal is fast (the
/// floor is a precondition, not a wait).
#[tokio::test]
async fn publish_refused_when_min_in_sync_exceeds_replica_set() {
    let topic = "isr-static";
    let (broker, _dir) = open_test_broker_with_cfg(BrokerConfig {
        replication_confirm_timeout_ms: 60_000,
        replication_min_in_sync_replicas: 3,
        ..Default::default()
    })
    .await;

    // Owner + one follower = replica set of 2, but the floor demands 3.
    broker.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(topic, Partition::new(0), None),
            "owner-a",
            vec!["follower-b".to_string()],
            1,
        )
        .with_durability(
            fibril_broker::coordination::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 },
        ),
    );

    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"x".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(2), reply)
        .await
        .expect("ISR refusal must be fast, not wait out the confirm timeout")
        .unwrap()
        .expect_err("must refuse below ISR floor");
    let message = format!("{err:?}");
    assert!(
        message.contains("NotEnoughInSyncReplicas") && !message.contains("timed out"),
        "expected fast ISR refusal, got: {message}"
    );

    broker.shutdown().await;
}

/// When the replica set is large enough but too few followers are healthy
/// (none have reported, or their reports are stale), a replica-durable publish
/// is refused fast rather than waiting out the confirm timeout.
#[tokio::test]
async fn publish_refused_when_in_sync_replicas_below_floor() {
    let topic = "isr-dynamic";

    // No follower has reported: only the owner is in sync (1 < 2).
    let (broker, _dir) = open_test_broker_with_cfg(BrokerConfig {
        replication_confirm_timeout_ms: 60_000,
        replication_min_in_sync_replicas: 2,
        replication_isr_timeout_ms: 10_000,
        ..Default::default()
    })
    .await;
    broker.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(topic, Partition::new(0), None),
            "owner-a",
            vec!["follower-b".to_string()],
            1,
        )
        .with_durability(
            fibril_broker::coordination::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 },
        ),
    );
    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"x".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(2), reply)
        .await
        .expect("ISR refusal must be fast, not wait out the confirm timeout")
        .unwrap()
        .expect_err("must refuse with no healthy follower");
    let message = format!("{err:?}");
    assert!(
        message.contains("NotEnoughInSyncReplicas") && !message.contains("timed out"),
        "expected fast ISR refusal, got: {message}"
    );
    broker.shutdown().await;
}

/// A follower that reported but has gone silent past the ISR timeout falls out
/// of the in-sync set: with isr_timeout 0 even a just-recorded report is stale,
/// so the floor is unmet and the publish is refused.
#[tokio::test]
async fn stale_follower_excluded_from_in_sync_set() {
    let topic = "isr-stale";
    let (broker, _dir) = open_test_broker_with_cfg(BrokerConfig {
        replication_confirm_timeout_ms: 60_000,
        replication_min_in_sync_replicas: 2,
        replication_isr_timeout_ms: 0,
        ..Default::default()
    })
    .await;
    broker.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(topic, Partition::new(0), None),
            "owner-a",
            vec!["follower-b".to_string()],
            1,
        )
        .with_durability(
            fibril_broker::coordination::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 },
        ),
    );
    // The follower has reported, but with isr_timeout 0 it never counts as fresh.
    broker.record_follower_replication_progress(topic, Partition::new(0), None, "follower-b", 5, 5);

    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"x".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(2), reply)
        .await
        .expect("ISR refusal must be fast, not wait out the confirm timeout")
        .unwrap()
        .expect_err("stale follower must not satisfy ISR");
    let message = format!("{err:?}");
    assert!(
        message.contains("NotEnoughInSyncReplicas"),
        "expected ISR refusal for stale follower, got: {message}"
    );
    broker.shutdown().await;
}

/// With the floor met (a follower reported recently and is past the offset),
/// the publish is admitted and the confirm resolves normally.
#[tokio::test]
async fn publish_admitted_once_in_sync_floor_met() {
    let topic = "isr-met";
    let (broker, _dir) = open_test_broker_with_cfg(BrokerConfig {
        replication_confirm_timeout_ms: 2_000,
        replication_min_in_sync_replicas: 2,
        replication_isr_timeout_ms: 10_000,
        ..Default::default()
    })
    .await;
    broker.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(topic, Partition::new(0), None),
            "owner-a",
            vec!["follower-b".to_string()],
            1,
        )
        .with_durability(
            fibril_broker::coordination::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 },
        ),
    );
    // Fresh report past the first offset satisfies both the ISR floor and the
    // durability requirement.
    broker.record_follower_replication_progress(topic, Partition::new(0), None, "follower-b", 5, 5);

    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"x".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let offset = tokio::time::timeout(Duration::from_secs(2), reply)
        .await
        .expect("confirm should resolve well within the bound")
        .unwrap()
        .expect("confirm resolves once ISR floor and durability are met");
    assert_eq!(offset, 0);
    broker.shutdown().await;
}

/// Owner-side replication observability: for an owned queue, the report exposes
/// each follower's reported durable progress and in-sync status, counts the
/// in-sync replicas against the floor, and flags queues below it.
#[tokio::test]
async fn owner_replica_observability_reports_follower_lag_and_isr() {
    let topic = "isr-observe";
    let (broker, _dir) = open_test_broker_with_cfg(BrokerConfig {
        replication_min_in_sync_replicas: 2,
        replication_isr_timeout_ms: 10_000,
        ..Default::default()
    })
    .await;

    broker.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(topic, Partition::new(0), None),
            "owner-a",
            vec!["follower-b".to_string(), "follower-c".to_string()],
            1,
        )
        .with_durability(
            fibril_broker::coordination::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 },
        ),
    );
    // Only follower-b has reported; follower-c is silent.
    broker.record_follower_replication_progress(topic, Partition::new(0), None, "follower-b", 5, 5);

    let report = broker.sparse_queue_observability_report();
    assert_eq!(report.owned_replica_summary.owned_queue_count, 1);
    // Owner + one fresh follower = 2 in-sync, which meets the floor of 2.
    assert_eq!(report.owned_replica_summary.below_floor_count, 0);

    let owned = report
        .owned_replicas
        .iter()
        .find(|q| q.topic == topic)
        .expect("owned queue present in report");
    assert_eq!(owned.in_sync_replicas, 2);
    assert!(!owned.below_floor);

    let b = owned
        .followers
        .iter()
        .find(|f| f.node_id == "follower-b")
        .expect("follower-b present");
    assert!(b.in_sync);
    assert_eq!(b.durable_message_next, 5);
    assert!(b.last_report_age_ms.is_some());

    let c = owned
        .followers
        .iter()
        .find(|f| f.node_id == "follower-c")
        .expect("follower-c present");
    assert!(!c.in_sync);
    assert_eq!(c.last_report_age_ms, None);

    broker.shutdown().await;
}

/// Data-plane epoch fencing (the split-brain last line): a follower fenced at
/// a newer assignment epoch rejects replicated batches from an older-epoch
/// (stale) owner AT THE STORAGE LAYER; once the owner advances to the fenced
/// epoch, the same records apply cleanly.
#[tokio::test]
async fn epoch_fenced_follower_rejects_stale_owner_batches() {
    use fibril_broker::queue_engine::ReplicatedAppendOutcome;

    let topic = "epoch-fence";

    // Owner with one committed message; its logs are still at epoch 0.
    let (owner, _owner_dir) = open_test_broker().await;
    let (publisher, _confirms) = owner
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"fenced-payload".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();
    let stale_records = owner
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 10, 10, usize::MAX, 0)
        .await
        .unwrap();

    // Follower fenced at assignment epoch 2 (epoch persisted before use).
    let (follower, _follower_dir) = open_test_broker().await;
    follower
        .become_replication_follower_with_epoch(topic, Partition::new(0), None, 2)
        .await
        .unwrap();

    // Stale owner's batches (epoch 0) must be rejected, nothing applied.
    let outcome = follower
        .apply_follower_replication_records(
            topic,
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            stale_records,
        )
        .await
        .unwrap();
    let BrokerFollowerReplicationApply::Applied(apply) = outcome else {
        panic!("expected apply outcome, got {outcome:?}");
    };
    assert!(
        matches!(
            apply.message_log,
            Some(ReplicatedAppendOutcome::StaleEpoch {
                current_epoch: 2,
                attempted_epoch: 0,
            })
        ),
        "stale-epoch message batch must be rejected: {apply:?}"
    );
    assert_eq!(
        apply.event_log, None,
        "event batch must not append after stale message batch rejection: {apply:?}"
    );

    // The owner reaches the fenced epoch (e.g. it holds the new assignment):
    // identical records now apply.
    owner
        .advance_replication_epoch(topic, Partition::new(0), None, 2)
        .await
        .unwrap();
    let fresh_records = owner
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 10, 10, usize::MAX, 0)
        .await
        .unwrap();
    let outcome = follower
        .apply_follower_replication_records(
            topic,
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            fresh_records,
        )
        .await
        .unwrap();
    let BrokerFollowerReplicationApply::Applied(apply) = outcome else {
        panic!("expected apply outcome, got {outcome:?}");
    };
    assert!(
        matches!(apply.message_log, Some(ReplicatedAppendOutcome::Applied(_))),
        "fenced-epoch batch must apply: {apply:?}"
    );

    owner.shutdown().await;
    follower.shutdown().await;
}

#[tokio::test]
async fn catch_up_rejects_stale_append_without_recording_ready_state() {
    let topic = "stale-catchup";

    let (owner, _owner_dir) = open_test_broker().await;
    let (publisher, _confirms) = owner
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"stale-payload".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let (follower, _follower_dir) = open_test_broker().await;
    follower
        .become_replication_follower_with_epoch(topic, Partition::new(0), None, 2)
        .await
        .unwrap();

    let err = follower
        .catch_up_replication_follower_from_owner(
            &owner,
            topic,
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            BrokerReplicationCatchUpOptions {
                max_messages_per_read: 10,
                max_events_per_read: 10,
                max_iterations: 1,
                ..Default::default()
            },
        )
        .await
        .expect_err("stale owner batch must not advance follower progress");

    assert!(matches!(
        err,
        BrokerError::InvalidReplicationProgress {
            stream: "message",
            ..
        }
    ));

    let snapshot = follower.debug_snapshot().await.unwrap();
    let queue = snapshot
        .queues
        .iter()
        .find(|queue| queue.topic == topic && queue.partition == 0 && queue.group.is_none())
        .expect("stale catch-up should have materialized the follower queue");
    assert_eq!(queue.role, QueueRole::Follower);
    assert_eq!(queue.state.ready_count, 0);

    let not_promoted = follower
        .promote_replication_follower_if_caught_up(topic, Partition::new(0), None, 1, 1)
        .await
        .unwrap();
    assert!(matches!(
        not_promoted,
        QueuePromotionOutcome::MessageLogBehind {
            local_next_offset: 0,
            expected_next_offset: 1,
        }
    ));

    owner.shutdown().await;
    follower.shutdown().await;
}

#[tokio::test]
async fn assignment_watcher_applies_snapshot_update_to_follower_role() {
    let coordination = Arc::new(StaticCoordination::new(
        "node-a",
        coordination_snapshot(Vec::new(), 1),
    ));
    let (broker, _dir) = open_test_broker_with_ownership(coordination.clone()).await;
    broker.spawn_assignment_watcher(coordination.clone());

    let queue = QueueIdentity::new("watched-followed", Partition::new(0), Some("workers"));
    coordination.update_snapshot(coordination_snapshot(
        vec![PartitionAssignment::new(
            queue,
            "node-b",
            vec!["node-a".to_string()],
            2,
        )],
        2,
    ));

    // Epoch fencing materializes the queue early in the transition, so wait
    // for the worker (the end of the transition), not just materialization.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if broker.is_queue_materialized("watched-followed", Some("workers"))
                && broker.has_follower_replication_worker(
                    "watched-followed",
                    Partition::new(0),
                    Some("workers"),
                )
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("assignment watcher should materialize the follower queue and start its worker");

    let err = broker
        .engine()
        .read_owner_message_records("watched-followed", 0, Some("workers"), 0, 1)
        .await
        .expect_err("watched follower queue should reject owner reads");
    assert!(matches!(err, StromaError::WrongQueueRole { .. }));
    broker.shutdown().await;
}

#[tokio::test]
async fn assignment_transitions_use_cached_local_role_when_watch_skips_previous_owner() {
    let (broker, _dir) = open_test_broker().await;
    let group = Some("workers".to_string());
    let topic = "coalesced-owner-watch";
    let queue = QueueIdentity::new(topic, Partition::new(0), group.as_deref());
    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &group)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"owned-before-watch-skip".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let empty = coordination_snapshot(Vec::new(), 1);
    let owner_assignment = PartitionAssignment::new(queue.clone(), "node-a", Vec::new(), 2);
    let first = coordination_snapshot(vec![owner_assignment], 2);
    broker
        .apply_assignment_snapshot_transitions("node-a", &empty, &first)
        .await;

    let moved = coordination_snapshot(
        vec![PartitionAssignment::new(queue, "node-b", Vec::new(), 3)],
        3,
    );
    let outcomes = broker
        .apply_assignment_snapshot_transitions("node-a", &empty, &moved)
        .await;

    assert_eq!(outcomes.len(), 1);
    assert!(matches!(
        outcomes.into_iter().next().unwrap(),
        Ok(BrokerAssignmentTransitionApply::Applied(
            LocalAssignmentIntent::FreezeOwner
        ))
    ));
    let err = broker
        .engine()
        .read_owner_message_records(topic, 0, group.as_deref(), 0, 1)
        .await
        .expect_err("cached local owner role must be frozen even if watch skipped previous owner");
    assert!(matches!(err, StromaError::WrongQueueRole { .. }));

    broker.shutdown().await;
}

#[tokio::test]
async fn assignment_transition_apply_does_not_materialize_new_owner_queue() {
    let (broker, _dir) = open_test_broker().await;
    let transition = assignment_transition(
        "transition-owned",
        LocalAssignmentIntent::BecomeOwner,
        None,
        Some(LocalAssignmentRole::Owner),
    );

    let outcome = broker
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Noop(LocalAssignmentIntent::BecomeOwner)
    );
    assert!(!broker.is_queue_materialized("transition-owned", Some("workers")));
    assert!(!broker.has_follower_replication_worker(
        "transition-owned",
        Partition::new(0),
        Some("workers")
    ));
    broker.shutdown().await;
}

#[tokio::test]
async fn assignment_transition_apply_keeps_unmaterialized_promotion_cold() {
    // Failover semantics: materialized followers promote at local tails (covered by
    // the protocol-level failover test); a never-materialized queue has no
    // follower state to promote and must stay cold until real traffic.
    let (broker, _dir) = open_test_broker().await;
    let transition = assignment_transition(
        "transition-promote",
        LocalAssignmentIntent::PromoteFollowerToOwner,
        Some(LocalAssignmentRole::Follower),
        Some(LocalAssignmentRole::Owner),
    );

    let outcome = broker
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Noop(LocalAssignmentIntent::PromoteFollowerToOwner)
    );
    assert!(!broker.is_queue_materialized("transition-promote", Some("workers")));
    broker.shutdown().await;
}

#[tokio::test]
async fn demote_owner_to_follower_stops_broker_owner_runtime() {
    let (broker, _dir) = open_test_broker().await;
    let group = Some("workers".to_string());
    let (publisher, _confirms) = broker
        .get_publisher("transition-demote", Partition::new(0), &group)
        .await
        .unwrap();

    let reply = publisher
        .publish(
            b"before demote".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();
    assert_eq!(
        broker
            .queue_activity_snapshot("transition-demote", Some("workers"))
            .map(|snapshot| snapshot.active_publishers),
        Some(1)
    );

    let transition = assignment_transition(
        "transition-demote",
        LocalAssignmentIntent::DemoteOwnerToFollower,
        Some(LocalAssignmentRole::Owner),
        Some(LocalAssignmentRole::Follower),
    );
    let outcome = broker
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::DemoteOwnerToFollower)
    );
    assert!(broker.has_follower_replication_worker(
        "transition-demote",
        Partition::new(0),
        Some("workers")
    ));
    assert_eq!(
        broker.queue_activity_snapshot("transition-demote", Some("workers")),
        None
    );

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match publisher
                .publish(
                    b"after demote".to_vec(),
                    unix_millis(),
                    unix_millis(),
                    None,
                    Default::default(),
                    None,
                )
                .await
            {
                Err(BrokerError::ChannelClosed) => break,
                Ok(reply) => {
                    let _ = reply.await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(err) => panic!("unexpected stale publisher error: {err:?}"),
            }
        }
    })
    .await
    .expect("stale publisher should close after demotion");

    broker.shutdown().await;
}

#[tokio::test]
async fn demote_owner_to_follower_requeues_broker_tracked_deliveries() {
    let (broker, _dir) = open_test_broker().await;
    let group = Some("workers".to_string());
    let (publisher, _confirms) = broker
        .get_publisher("transition-demote-inflight", Partition::new(0), &group)
        .await
        .unwrap();

    let reply = publisher
        .publish(
            b"before demote".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let mut sub = broker
        .subscribe(
            "transition-demote-inflight",
            Partition::new(0),
            Some("workers"),
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let msg = recv_with_timeout(&mut sub, 1_000)
        .await
        .expect("published message should be delivered before demotion");
    assert_eq!(msg.message.offset, 0);

    let transition = assignment_transition(
        "transition-demote-inflight",
        LocalAssignmentIntent::DemoteOwnerToFollower,
        Some(LocalAssignmentRole::Owner),
        Some(LocalAssignmentRole::Follower),
    );
    let outcome = broker
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::DemoteOwnerToFollower)
    );

    let promoted = broker
        .promote_replication_follower_if_caught_up(
            "transition-demote-inflight",
            Partition::new(0),
            Some("workers"),
            1,
            2,
        )
        .await
        .expect("demoted queue should be promotable after local requeue");
    assert!(
        matches!(
            promoted,
            QueuePromotionOutcome::Promoted {
                message_next_offset: 1,
                event_next_offset: 2,
                ..
            }
        ),
        "unexpected promotion outcome: {promoted:?}"
    );

    let snapshot = broker.debug_snapshot().await.unwrap();
    let queue = snapshot
        .queues
        .iter()
        .find(|queue| {
            queue.topic == "transition-demote-inflight"
                && queue.partition == 0
                && queue.group.as_deref() == Some("workers")
        })
        .expect("queue should remain materialized after promotion");
    assert_eq!(format!("{:?}", queue.role), "Owner");
    assert_eq!(queue.state.ready_count, 1);
    assert_eq!(queue.state.inflight_count, 0);

    let mut redelivery = broker
        .subscribe(
            "transition-demote-inflight",
            Partition::new(0),
            Some("workers"),
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let msg = match recv_with_timeout(&mut redelivery, 1_000).await {
        Some(msg) => msg,
        None => {
            let snapshot = broker.debug_snapshot().await.unwrap();
            panic!(
                "demotion should requeue broker-tracked delivery, state after subscribe: {:?}",
                snapshot
                    .queues
                    .iter()
                    .find(|queue| {
                        queue.topic == "transition-demote-inflight"
                            && queue.partition == 0
                            && queue.group.as_deref() == Some("workers")
                    })
                    .map(|queue| &queue.state)
            );
        }
    };
    assert_eq!(msg.message.offset, 0);

    broker.shutdown().await;
}

#[tokio::test]
async fn freeze_owner_requeues_broker_tracked_deliveries() {
    let (broker, _dir) = open_test_broker().await;
    let group = Some("workers".to_string());
    let (publisher, _confirms) = broker
        .get_publisher("transition-freeze-inflight", Partition::new(0), &group)
        .await
        .unwrap();

    let reply = publisher
        .publish(
            b"before freeze".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let mut sub = broker
        .subscribe(
            "transition-freeze-inflight",
            Partition::new(0),
            Some("workers"),
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let msg = recv_with_timeout(&mut sub, 1_000)
        .await
        .expect("published message should be delivered before freeze");
    assert_eq!(msg.message.offset, 0);

    let freeze = assignment_transition(
        "transition-freeze-inflight",
        LocalAssignmentIntent::FreezeOwner,
        Some(LocalAssignmentRole::Owner),
        None,
    );
    let outcome = broker.apply_assignment_transition(&freeze).await.unwrap();

    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::FreezeOwner)
    );
    assert_eq!(
        broker.queue_activity_snapshot("transition-freeze-inflight", Some("workers")),
        None
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match publisher
                .publish(
                    b"after freeze".to_vec(),
                    unix_millis(),
                    unix_millis(),
                    None,
                    Default::default(),
                    None,
                )
                .await
            {
                Err(BrokerError::ChannelClosed) => break,
                Ok(reply) => {
                    let _ = reply.await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(err) => panic!("unexpected stale publisher error: {err:?}"),
            }
        }
    })
    .await
    .expect("stale publisher should close after freeze");

    let snapshot = broker.debug_snapshot().await.unwrap();
    let queue = snapshot
        .queues
        .iter()
        .find(|queue| {
            queue.topic == "transition-freeze-inflight"
                && queue.partition == 0
                && queue.group.as_deref() == Some("workers")
        })
        .expect("frozen queue should remain materialized");
    assert_eq!(queue.role, QueueRole::Frozen);
    assert_eq!(queue.state.ready_count, 1);
    assert_eq!(queue.state.inflight_count, 0);

    let become_owner = assignment_transition(
        "transition-freeze-inflight",
        LocalAssignmentIntent::BecomeOwner,
        None,
        Some(LocalAssignmentRole::Owner),
    );
    let outcome = broker
        .apply_assignment_transition(&become_owner)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::BecomeOwner)
    );

    let mut redelivery = broker
        .subscribe(
            "transition-freeze-inflight",
            Partition::new(0),
            Some("workers"),
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let msg = recv_with_timeout(&mut redelivery, 1_000)
        .await
        .expect("freeze should requeue broker-tracked delivery");
    assert_eq!(msg.message.offset, 0);

    broker.shutdown().await;
}

#[tokio::test]
async fn owner_replication_read_returns_published_records() {
    let (broker, _dir) = open_test_broker().await;
    let (publisher, _confirms) = broker
        .get_publisher("replicated", Partition::new(0), &None)
        .await
        .unwrap();

    let reply = publisher
        .publish(
            b"hello replication".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let records = broker
        .read_owner_replication_records(
            "replicated",
            Partition::new(0),
            None,
            0,
            0,
            10,
            10,
            usize::MAX,
            0,
        )
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message records, got checkpoint required");
    };
    assert_eq!(messages.requested_offset, 0);
    assert_eq!(messages.next_offset, 1);
    assert_eq!(messages.records.len(), 1);
    assert_eq!(messages.records[0].0, 0);
    assert_eq!(messages.records[0].1.payload, b"hello replication");

    let OwnerReplicationRead::Batch(events) = records.events else {
        panic!("expected event records, got checkpoint required");
    };
    assert_eq!(events.requested_offset, 0);
    assert_eq!(events.next_offset, 1);
    assert_eq!(events.records.len(), 1);

    broker.shutdown().await;
}

#[tokio::test]
async fn owner_replication_long_poll_wakes_after_publish() {
    let (broker, _dir) = open_test_broker().await;
    let topic = "replication-long-poll";
    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let initial_records = broker
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 10, 10, usize::MAX, 0)
        .await
        .unwrap();
    let message_from = match initial_records.messages {
        OwnerReplicationRead::Batch(batch) => batch.next_offset,
        OwnerReplicationRead::CheckpointRequired { .. } => {
            panic!("fresh long-poll test queue unexpectedly needed a message checkpoint")
        }
    };
    let event_from = match initial_records.events {
        OwnerReplicationRead::Batch(batch) => batch.next_offset,
        OwnerReplicationRead::CheckpointRequired { .. } => {
            panic!("fresh long-poll test queue unexpectedly needed an event checkpoint")
        }
    };

    let reader_broker = broker.clone();
    let mut read_task = tokio::spawn(async move {
        reader_broker
            .read_owner_replication_records(
                topic,
                Partition::new(0),
                None,
                message_from,
                event_from,
                10,
                10,
                usize::MAX,
                5_000,
            )
            .await
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut read_task)
            .await
            .is_err(),
        "empty long-poll read should wait instead of returning an empty batch immediately"
    );

    let reply = publisher
        .publish(
            b"wake follower".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let records = tokio::time::timeout(Duration::from_secs(2), read_task)
        .await
        .expect("long-poll read should wake after publish")
        .unwrap()
        .unwrap();
    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message records, got checkpoint required");
    };
    assert_eq!(messages.requested_offset, message_from);
    assert_eq!(messages.next_offset, message_from + 1);
    assert_eq!(messages.records.len(), 1);
    assert_eq!(messages.records[0].1.payload, b"wake follower");

    broker.shutdown().await;
}

#[tokio::test]
async fn owner_replication_long_poll_ignores_local_only_wake() {
    let (broker, _dir) = open_test_broker().await;
    let topic = "replication-local-wake";
    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let initial_records = broker
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 10, 10, usize::MAX, 0)
        .await
        .unwrap();
    let message_from = match initial_records.messages {
        OwnerReplicationRead::Batch(batch) => batch.next_offset,
        OwnerReplicationRead::CheckpointRequired { .. } => {
            panic!("fresh long-poll test queue unexpectedly needed a message checkpoint")
        }
    };
    let event_from = match initial_records.events {
        OwnerReplicationRead::Batch(batch) => batch.next_offset,
        OwnerReplicationRead::CheckpointRequired { .. } => {
            panic!("fresh long-poll test queue unexpectedly needed an event checkpoint")
        }
    };

    let reader_broker = broker.clone();
    let mut read_task = tokio::spawn(async move {
        reader_broker
            .read_owner_replication_records(
                topic,
                Partition::new(0),
                None,
                message_from,
                event_from,
                10,
                10,
                usize::MAX,
                5_000,
            )
            .await
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut read_task)
            .await
            .is_err(),
        "empty long-poll read should wait before a local-only wake"
    );

    broker.update_config(BrokerConfig {
        delivery_poll_max_ms: 123,
        ..Default::default()
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(75), &mut read_task)
            .await
            .is_err(),
        "local-only broker wake should not release owner replication long-poll"
    );

    let reply = publisher
        .publish(
            b"durable wake".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    tokio::time::timeout(Duration::from_secs(2), read_task)
        .await
        .expect("durable publish should still wake long-poll")
        .unwrap()
        .unwrap();

    broker.shutdown().await;
}

#[tokio::test]
async fn owner_replication_long_poll_wakes_after_ack_event() {
    let (broker, _dir) = open_test_broker().await;
    let topic = "replication-ack-long-poll";
    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"ack wakes follower".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let mut sub = broker
        .subscribe(
            topic,
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let msg = recv_with_timeout(&mut sub, 1000)
        .await
        .expect("message should be delivered before ack replication test");

    let initial_records = broker
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 10, 10, usize::MAX, 0)
        .await
        .unwrap();
    let message_from = match initial_records.messages {
        OwnerReplicationRead::Batch(batch) => batch.next_offset,
        OwnerReplicationRead::CheckpointRequired { .. } => {
            panic!("fresh long-poll test queue unexpectedly needed a message checkpoint")
        }
    };
    let event_from = match initial_records.events {
        OwnerReplicationRead::Batch(batch) => batch.next_offset,
        OwnerReplicationRead::CheckpointRequired { .. } => {
            panic!("fresh long-poll test queue unexpectedly needed an event checkpoint")
        }
    };

    let reader_broker = broker.clone();
    let mut read_task = tokio::spawn(async move {
        reader_broker
            .read_owner_replication_records(
                topic,
                Partition::new(0),
                None,
                message_from,
                event_from,
                10,
                10,
                usize::MAX,
                5_000,
            )
            .await
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut read_task)
            .await
            .is_err(),
        "empty event long-poll read should wait before ACK"
    );

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: msg.delivery_tag,
    })
    .await
    .unwrap();
    broker.wait_for_pending_settles().await;

    let records = tokio::time::timeout(Duration::from_secs(2), read_task)
        .await
        .expect("ACK event should wake long-poll")
        .unwrap()
        .unwrap();
    let OwnerReplicationRead::Batch(events) = records.events else {
        panic!("expected event records after ACK, got checkpoint required");
    };
    assert_eq!(events.requested_offset, event_from);
    assert!(events.next_offset > event_from);
    assert!(!events.records.is_empty());

    broker.shutdown().await;
}

#[tokio::test]
async fn cold_owner_materializes_at_cached_assignment_epoch() {
    let topic = "assigned-cold-owner";
    let (broker, _dir) = open_test_broker().await;
    let queue = QueueIdentity::new(topic, Partition::new(0), None);
    broker.cache_queue_assignment(&PartitionAssignment::new(queue, "node-a", Vec::new(), 7));

    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"epoch-seven".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let records = broker
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 10, 10, usize::MAX, 0)
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message records, got checkpoint required");
    };
    assert_eq!(messages.epoch, 7);
    assert_eq!(messages.records.len(), 1);

    let OwnerReplicationRead::Batch(events) = records.events else {
        panic!("expected event records, got checkpoint required");
    };
    assert_eq!(events.epoch, 7);
    assert_eq!(events.records.len(), 1);

    broker.shutdown().await;
}

/// Cold-restart orphan reconciliation: a partition this node was disowned of
/// while it was down stays on disk after restart. The indexer registers it as an
/// unmaterialized slot, but it is inert cold storage - never served, never
/// materialized - because serving is ownership-gated. orphaned_on_disk_partitions
/// surfaces it (coordination has provably reassigned it elsewhere) without
/// destroying it.
#[tokio::test]
async fn cold_restart_disowned_partition_is_inert_cold_storage() {
    let dir = test_dir!("broker_test");
    let topic = "disowned-on-restart";

    // First incarnation owns the partition and writes durable data.
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    publisher
        .publish(
            b"pre-downtime".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    broker.shutdown_graceful().await;
    drop(broker);

    // Cold restart: reopen the same data dir. The indexer registers the on-disk
    // partition as an unmaterialized slot.
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    // This node owns nothing now: coordination reassigned the partition away
    // while it was down.
    let broker = Broker::new_with_ownership(
        engine,
        BrokerConfig::default(),
        None,
        Arc::new(StaticQueueOwnership::new(StdHashSet::new())),
    );

    let queue = QueueIdentity::new(topic, Partition::new(0), None);
    let snapshot = coordination_snapshot(
        vec![PartitionAssignment::new(
            queue.clone(),
            "other-node",
            Vec::new(),
            1,
        )],
        1,
    );

    // Cold storage: indexed on disk but not materialized.
    assert!(
        !broker.engine().is_materialized(topic, 0, None),
        "a disowned partition must stay unmaterialized after restart"
    );

    // Reconciliation flags it as provably reassigned elsewhere, leaving it intact.
    let orphaned = broker
        .orphaned_on_disk_partitions("this-node", &snapshot)
        .await;
    assert_eq!(orphaned, vec![queue]);

    // Not served: an ownership-gated publish is rejected before any materialize.
    let publish = broker.get_publisher(topic, Partition::new(0), &None).await;
    assert!(
        matches!(publish, Err(BrokerError::NotOwner { .. })),
        "a disowned partition must not accept writes"
    );

    // The rejected publish did not mis-materialize the cold partition.
    assert!(!broker.engine().is_materialized(topic, 0, None));

    broker.shutdown_graceful().await;
}

/// A partition absent from the snapshot (coordination does not claim it - a
/// stream, an in-flight declare, or a queue this node was never told about) is
/// left untouched by orphan reconciliation: only a partition provably reassigned
/// elsewhere counts as orphaned.
#[tokio::test]
async fn orphan_reconciliation_ignores_partitions_unknown_to_coordination() {
    let dir = test_dir!("broker_test");
    let topic = "unknown-to-coordination";

    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    publisher
        .publish(
            b"data".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    // The snapshot has no assignment for this partition at all.
    let snapshot = coordination_snapshot(Vec::new(), 1);
    let orphaned = broker
        .orphaned_on_disk_partitions("this-node", &snapshot)
        .await;
    assert!(
        orphaned.is_empty(),
        "a partition coordination does not claim must not be treated as orphaned"
    );

    broker.shutdown_graceful().await;
}

/// Publishes to two partitions of the same logical queue land in independent
/// logs: each partition has its own offset sequence and only its own messages.
#[tokio::test]
async fn publishes_route_to_independent_partition_logs() {
    let (broker, _dir) = open_test_broker().await;
    let topic = "multi-part";
    let group = Some("workers".to_string());

    // One message to partition 0, two to partition 1.
    let (p0, _c0) = broker
        .get_publisher(topic, Partition::new(0), &group)
        .await
        .unwrap();
    p0.publish(
        b"p0-a".to_vec(),
        unix_millis(),
        unix_millis(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();

    let (p1, _c1) = broker
        .get_publisher(topic, Partition::new(1), &group)
        .await
        .unwrap();
    for payload in [b"p1-a".to_vec(), b"p1-b".to_vec()] {
        p1.publish(
            payload,
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    }

    let read_partition = |partition| {
        let broker = broker.clone();
        let group = group.clone();
        async move {
            let records = broker
                .read_owner_replication_records(
                    topic,
                    partition,
                    group.as_deref(),
                    0,
                    0,
                    10,
                    10,
                    usize::MAX,
                    0,
                )
                .await
                .unwrap();
            let OwnerReplicationRead::Batch(messages) = records.messages else {
                panic!("expected message records for partition {partition}");
            };
            messages
        }
    };

    let part0 = read_partition(Partition::new(0)).await;
    assert_eq!(part0.next_offset, 1, "partition 0 has one message");
    assert_eq!(part0.records.len(), 1);
    assert_eq!(part0.records[0].1.payload, b"p0-a");

    let part1 = read_partition(Partition::new(1)).await;
    assert_eq!(part1.next_offset, 2, "partition 1 has two messages");
    assert_eq!(part1.records.len(), 2);
    assert_eq!(part1.records[0].1.payload, b"p1-a");
    assert_eq!(part1.records[1].1.payload, b"p1-b");

    broker.shutdown().await;
}

/// The exclusive-assignee gate delivers a partition's messages only to the
/// assigned consumer; other consumers stay subscribed (standbys) and receive
/// nothing until reassigned. Reassigning routes new messages to the new
/// assignee while the prior one keeps its already-delivered in-flight.
#[tokio::test]
async fn exclusive_assignee_gate_delivers_only_to_assignee() {
    let (broker, _dir) = open_test_broker().await;
    let topic = "excl";

    let mut a = broker
        .subscribe(
            topic,
            Partition::ZERO,
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();
    let mut b = broker
        .subscribe(
            topic,
            Partition::ZERO,
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();

    // Gate delivery to `a` before any messages exist.
    broker.set_exclusive_assignee(topic, Partition::ZERO, None, Some(a.sub_id));

    let (publisher, _c) = broker
        .get_publisher(topic, Partition::ZERO, &None)
        .await
        .unwrap();
    for _ in 0..4 {
        publisher
            .publish(
                b"m".to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap()
            .await
            .unwrap()
            .unwrap();
    }

    // `a` receives all four; `b` (gated out, still subscribed) receives nothing.
    for _ in 0..4 {
        assert!(recv_with_timeout(&mut a, 1000).await.is_some());
    }
    assert!(
        recv_with_timeout(&mut b, 200).await.is_none(),
        "gated-out consumer must receive nothing"
    );

    // Reassign to `b`; a new message reaches `b` (a keeps its in-flight).
    broker.set_exclusive_assignee(topic, Partition::ZERO, None, Some(b.sub_id));
    publisher
        .publish(
            b"m2".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert!(
        recv_with_timeout(&mut b, 1000).await.is_some(),
        "new assignee should receive after reassignment"
    );

    broker.shutdown().await;
}

#[tokio::test]
async fn owner_replication_read_rejects_unowned_queue_before_materializing() {
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(StdHashSet::new())))
            .await;

    let err = broker
        .read_owner_replication_records(
            "unowned",
            Partition::new(0),
            None,
            0,
            0,
            10,
            10,
            usize::MAX,
            0,
        )
        .await
        .expect_err("unowned queue unexpectedly served replication records");

    assert!(matches!(
        err,
        BrokerError::NotOwner {
            topic,
            partition: _,
            group: None,
        } if topic == "unowned"
    ));
    assert!(!broker.is_queue_materialized("unowned", None));
    broker.shutdown().await;
}

#[tokio::test]
async fn broker_replication_read_applies_to_follower_and_promotes() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;

    follower
        .become_replication_follower("catchup", Partition::new(0), None)
        .await
        .unwrap();

    let (publisher, _confirms) = owner
        .get_publisher("catchup", Partition::new(0), &None)
        .await
        .unwrap();
    for payload in [b"first".to_vec(), b"second".to_vec()] {
        let reply = publisher
            .publish(
                payload,
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

    let records = owner
        .read_owner_replication_records(
            "catchup",
            Partition::new(0),
            None,
            0,
            0,
            10,
            10,
            usize::MAX,
            0,
        )
        .await
        .unwrap();
    let expected_message_next_offset = match &records.messages {
        OwnerReplicationRead::Batch(batch) => batch.next_offset,
        OwnerReplicationRead::CheckpointRequired { .. } => {
            panic!("fresh follower unexpectedly needed a message checkpoint")
        }
    };
    let expected_event_next_offset = match &records.events {
        OwnerReplicationRead::Batch(batch) => batch.next_offset,
        OwnerReplicationRead::CheckpointRequired { .. } => {
            panic!("fresh follower unexpectedly needed an event checkpoint")
        }
    };

    let apply = follower
        .apply_follower_replication_records(
            "catchup",
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            records,
        )
        .await
        .unwrap();
    assert!(matches!(apply, BrokerFollowerReplicationApply::Applied(_)));

    let promotion = follower
        .promote_replication_follower_if_caught_up(
            "catchup",
            Partition::new(0),
            None,
            expected_message_next_offset,
            expected_event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promotion,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 2,
            event_next_offset: 2,
            applied_event_offset: Some(1),
        }
    );

    let mut sub = follower
        .subscribe(
            "catchup",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 2 },
        )
        .await
        .unwrap();
    let first = recv_with_timeout(&mut sub, 1000)
        .await
        .expect("first replicated message should deliver after promotion");
    let second = recv_with_timeout(&mut sub, 1000)
        .await
        .expect("second replicated message should deliver after promotion");
    assert_eq!(first.message.payload, b"first");
    assert_eq!(second.message.payload, b"second");

    owner.shutdown().await;
    follower.shutdown().await;
}

/// Durable stream replication end to end through the stream-mode pull path: an
/// owner publishes stream records and commits a cursor; a follower becomes a
/// stream follower and catches up via `ReplicationResourceKind::Stream`, landing
/// the records at the owner's offsets plus the replicated cursor-commit event.
/// Promotion is a later brick, so this asserts up to follower-has-the-data.
#[tokio::test]
async fn stream_follower_catches_up_records_and_cursor_from_owner() {
    use fibril_broker::stream::StreamDurability;

    let headers = || MessageHeaders {
        published: unix_millis(),
        publish_received: unix_millis(),
        content_type: None,
        extra: Default::default(),
    };

    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;

    let stream = StreamIdentity::new("events", Partition::new(0));

    // Owner: open a durable stream and take ownership at the assignment epoch so
    // its appends are stamped with epoch 1 (matching what the follower fences on).
    let owner_channel = owner
        .get_or_open_stream("events", 0, StreamDurability::Durable, None)
        .await
        .unwrap();
    let become_owner = LocalStreamAssignmentTransition {
        stream: stream.clone(),
        previous_role: None,
        next_role: Some(LocalAssignmentRole::Owner),
        previous: None,
        next: Some(StreamAssignment::new(
            stream.clone(),
            "owner",
            vec!["follower".to_string()],
            1,
        )),
        intent: LocalAssignmentIntent::BecomeOwner,
    };
    assert!(matches!(
        owner
            .apply_stream_assignment_transition(&become_owner)
            .await
            .unwrap(),
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::BecomeOwner)
    ));
    owner_channel
        .publish(headers(), b"first".to_vec())
        .await
        .unwrap();
    let last_offset = owner_channel
        .publish(headers(), b"second".to_vec())
        .await
        .unwrap();
    owner_channel.settle("group", last_offset).await.unwrap();
    // settle is microbatched; flush so the cursor-commit event is durable before
    // the follower pulls it.
    owner_channel.flush_cursor_commits().await.unwrap();

    // Follower: take the stream follower role (storage role + epoch fence) so it
    // accepts replicated stream batches.
    let become_follower = LocalStreamAssignmentTransition {
        stream: stream.clone(),
        previous_role: None,
        next_role: Some(LocalAssignmentRole::Follower),
        previous: None,
        next: Some(StreamAssignment::new(
            stream.clone(),
            "owner",
            vec!["follower".to_string()],
            1,
        )),
        intent: LocalAssignmentIntent::BecomeFollower,
    };
    assert!(matches!(
        follower
            .apply_stream_assignment_transition(&become_follower)
            .await
            .unwrap(),
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::BecomeFollower)
    ));

    // Catch up the follower from the owner over the stream-mode pull path.
    let outcome = follower
        .catch_up_replication_follower_from_owner(
            owner.as_ref(),
            "events",
            Partition::new(0),
            None,
            ReplicationResourceKind::Stream,
            BrokerReplicationCatchUpOptions {
                message_from: 0,
                event_from: 0,
                max_messages_per_read: 10,
                max_events_per_read: 10,
                max_bytes_per_read: usize::MAX,
                max_iterations: 4,
                max_wait_ms: 0,
            },
        )
        .await
        .unwrap();
    let progress = match outcome {
        BrokerReplicationCatchUp::CaughtUp(progress) => progress,
        other => panic!("stream follower did not catch up: {other:?}"),
    };
    assert_eq!(progress.applied_message_records, 2);
    assert_eq!(progress.applied_event_records, 1);

    // The follower's stream log tail matches the owner's: records landed at the
    // owner's offsets.
    let (owner_head, owner_tail) = owner_channel.head_tail().await.unwrap();
    let follower_channel = follower
        .get_or_open_stream("events", 0, StreamDurability::Durable, None)
        .await
        .unwrap();
    let (follower_head, follower_tail) = follower_channel.head_tail().await.unwrap();
    assert_eq!((follower_head, follower_tail), (owner_head, owner_tail));

    // Failover: the owner is gone. Promote the caught-up follower to owner at its
    // own tails under a bumped epoch; it must accept and then serve writes.
    let promote = LocalStreamAssignmentTransition {
        stream: stream.clone(),
        previous_role: Some(LocalAssignmentRole::Follower),
        next_role: Some(LocalAssignmentRole::Owner),
        previous: Some(StreamAssignment::new(
            stream.clone(),
            "owner",
            vec!["follower".to_string()],
            1,
        )),
        next: Some(StreamAssignment::new(stream.clone(), "follower", vec![], 2)),
        intent: LocalAssignmentIntent::PromoteFollowerToOwner,
    };
    assert!(matches!(
        follower
            .apply_stream_assignment_transition(&promote)
            .await
            .unwrap(),
        BrokerAssignmentTransitionApply::Applied(LocalAssignmentIntent::PromoteFollowerToOwner)
    ));
    let promoted_offset = follower_channel
        .publish(headers(), b"after-promotion".to_vec())
        .await
        .expect("promoted follower should serve writes as the new owner");
    assert_eq!(promoted_offset, follower_tail);

    owner.shutdown().await;
    follower.shutdown().await;
}

/// A durable stream with replicas confirms a publish only after the replication
/// durability policy is met: the tier-derived policy (durable + 1 follower =>
/// ReplicaDurable nodes 2) is cached when the owner role is applied, so the
/// confirm gate waits for the follower to report durable progress past the
/// offset, then resolves.
#[tokio::test]
async fn durable_stream_publish_confirm_waits_for_follower_progress() {
    use fibril_broker::broker::{OwnAllQueues, StreamOpenConfig, StreamOwnership as StreamOwnershipTrait};
    use fibril_broker::stream::StreamDurability;

    #[derive(Debug)]
    struct DurableStreamOwner;
    impl StreamOwnershipTrait for DurableStreamOwner {
        fn owns_stream(&self, _topic: &str, _partition: Partition) -> bool {
            true
        }
        fn stream_open_config(&self, _topic: &str) -> Option<StreamOpenConfig> {
            Some(StreamOpenConfig {
                durability: StreamDurability::Durable,
                retention: None,
            })
        }
    }

    let dir = test_dir!("broker_test");
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let broker = Broker::new_with_ownerships(
        engine,
        BrokerConfig {
            replication_confirm_timeout_ms: 300,
            ..Default::default()
        },
        None,
        Arc::new(OwnAllQueues),
        Arc::new(DurableStreamOwner),
    );

    let stream = StreamIdentity::new("events", Partition::new(0));
    // Owner of a durable stream with one assigned follower: the cached policy is
    // ReplicaDurable nodes 2 (owner + one replica).
    let become_owner = LocalStreamAssignmentTransition {
        stream: stream.clone(),
        previous_role: None,
        next_role: Some(LocalAssignmentRole::Owner),
        previous: None,
        next: Some(StreamAssignment::new(
            stream.clone(),
            "owner",
            vec!["follower-b".to_string()],
            1,
        )),
        intent: LocalAssignmentIntent::BecomeOwner,
    };
    broker
        .apply_stream_assignment_transition(&become_owner)
        .await
        .unwrap();

    // No follower progress yet: the confirm for offset 0 must time out.
    let err = broker
        .await_replication_confirm("events", Partition::new(0), None, 0)
        .await
        .expect_err("confirm must wait without follower progress");
    assert!(
        format!("{err:?}").contains("timed out"),
        "expected a timeout error, got: {err:?}"
    );

    // The follower reports durable progress past offset 0; the confirm resolves.
    broker.record_follower_replication_progress("events", Partition::new(0), None, "follower-b", 1, 0);
    broker
        .await_replication_confirm("events", Partition::new(0), None, 0)
        .await
        .expect("confirm resolves once the follower reports durable progress");

    broker.shutdown().await;
}

#[tokio::test]
async fn broker_state_checkpoint_export_installs_then_messages_catch_up() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;

    let (publisher, _confirms) = owner
        .get_publisher("checkpoint", Partition::new(0), &None)
        .await
        .unwrap();
    for payload in [b"first".to_vec(), b"second".to_vec()] {
        let reply = publisher
            .publish(
                payload,
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

    let checkpoint = owner
        .export_owner_state_checkpoint("checkpoint", Partition::new(0), None)
        .await
        .unwrap();
    assert_eq!(checkpoint.message_checkpoint_offset, 0);
    assert_eq!(checkpoint.message_next_offset, 2);
    assert_eq!(checkpoint.event_next_offset, 2);
    assert_eq!(checkpoint.applied_event_offset, 1);

    follower
        .become_replication_follower("checkpoint", Partition::new(0), None)
        .await
        .unwrap();
    follower
        .install_follower_state_checkpoint(
            "checkpoint",
            Partition::new(0),
            None,
            FollowerStateCheckpointInstall {
                message_next_offset: checkpoint.message_checkpoint_offset,
                event_next_offset: checkpoint.event_next_offset,
                applied_event_offset: checkpoint.applied_event_offset,
                state_snapshot: checkpoint.state_snapshot,
            },
        )
        .await
        .unwrap();

    let records = owner
        .read_owner_replication_records(
            "checkpoint",
            Partition::new(0),
            None,
            checkpoint.message_checkpoint_offset,
            checkpoint.event_next_offset,
            10,
            10,
            usize::MAX,
            0,
        )
        .await
        .unwrap();
    let apply = follower
        .apply_follower_replication_records(
            "checkpoint",
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            records,
        )
        .await
        .unwrap();
    assert!(matches!(apply, BrokerFollowerReplicationApply::Applied(_)));

    let promotion = follower
        .promote_replication_follower_if_caught_up(
            "checkpoint",
            Partition::new(0),
            None,
            checkpoint.message_next_offset,
            checkpoint.event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promotion,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 2,
            event_next_offset: 2,
            applied_event_offset: Some(1),
        }
    );

    let mut sub = follower
        .subscribe(
            "checkpoint",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 2 },
        )
        .await
        .unwrap();
    let first = recv_with_timeout(&mut sub, 1000)
        .await
        .expect("first checkpointed message should deliver after promotion");
    let second = recv_with_timeout(&mut sub, 1000)
        .await
        .expect("second checkpointed message should deliver after promotion");
    assert_eq!(first.message.payload, b"first");
    assert_eq!(second.message.payload, b"second");

    owner.shutdown().await;
    follower.shutdown().await;
}

#[tokio::test]
async fn follower_apply_returns_checkpoint_required_without_materializing_queue() {
    let (follower, _dir) = open_test_broker().await;
    let message_checkpoint = BrokerReplicationCheckpointRequired {
        epoch: 3,
        requested_offset: 5,
        head_offset: 8,
        next_offset: 20,
    };
    let event_checkpoint = BrokerReplicationCheckpointRequired {
        epoch: 3,
        requested_offset: 4,
        head_offset: 9,
        next_offset: 21,
    };

    let apply = follower
        .apply_follower_replication_records(
            "checkpointed",
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            BrokerOwnerReplicationRecords {
                messages: OwnerReplicationRead::CheckpointRequired {
                    epoch: message_checkpoint.epoch,
                    requested_offset: message_checkpoint.requested_offset,
                    head_offset: message_checkpoint.head_offset,
                    next_offset: message_checkpoint.next_offset,
                },
                events: OwnerReplicationRead::CheckpointRequired {
                    epoch: event_checkpoint.epoch,
                    requested_offset: event_checkpoint.requested_offset,
                    head_offset: event_checkpoint.head_offset,
                    next_offset: event_checkpoint.next_offset,
                },
            },
        )
        .await
        .unwrap();

    assert_eq!(
        apply,
        BrokerFollowerReplicationApply::CheckpointRequired {
            messages: Some(message_checkpoint),
            events: Some(event_checkpoint),
        }
    );
    assert!(!follower.is_queue_materialized("checkpointed", None));

    follower.shutdown().await;
}

#[tokio::test]
async fn follower_apply_returns_event_checkpoint_required_after_empty_message_batch() {
    let (follower, _dir) = open_test_broker().await;
    let event_checkpoint = BrokerReplicationCheckpointRequired {
        epoch: 4,
        requested_offset: 11,
        head_offset: 13,
        next_offset: 30,
    };

    let apply = follower
        .apply_follower_replication_records(
            "event-checkpointed",
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            BrokerOwnerReplicationRecords {
                messages: OwnerReplicationRead::Batch(OwnerReplicationBatch::<Message> {
                    epoch: 4,
                    requested_offset: 0,
                    next_offset: 0,
                    records: Vec::new(),
                }),
                events: OwnerReplicationRead::CheckpointRequired {
                    epoch: event_checkpoint.epoch,
                    requested_offset: event_checkpoint.requested_offset,
                    head_offset: event_checkpoint.head_offset,
                    next_offset: event_checkpoint.next_offset,
                },
            },
        )
        .await
        .unwrap();

    assert_eq!(
        apply,
        BrokerFollowerReplicationApply::CheckpointRequired {
            messages: None,
            events: Some(event_checkpoint),
        }
    );
    assert!(!follower.is_queue_materialized("event-checkpointed", None));

    follower.shutdown().await;
}

#[tokio::test]
async fn broker_replication_catch_up_loop_handles_multiple_passes() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;

    follower
        .become_replication_follower("multi-pass", Partition::new(0), None)
        .await
        .unwrap();

    let (publisher, _confirms) = owner
        .get_publisher("multi-pass", Partition::new(0), &None)
        .await
        .unwrap();
    for payload in [
        b"one".to_vec(),
        b"two".to_vec(),
        b"three".to_vec(),
        b"four".to_vec(),
        b"five".to_vec(),
    ] {
        let reply = publisher
            .publish(
                payload,
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

    let outcome = follower
        .catch_up_replication_follower_from_owner(
            &owner,
            "multi-pass",
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            BrokerReplicationCatchUpOptions {
                max_messages_per_read: 2,
                max_events_per_read: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BrokerReplicationCatchUp::CaughtUp(BrokerReplicationCatchUpProgress {
            iterations: 4,
            applied_message_records: 5,
            applied_event_records: 5,
            message_next_offset: 5,
            event_next_offset: 5,
        })
    );

    let promotion = follower
        .promote_replication_follower_if_caught_up("multi-pass", Partition::new(0), None, 5, 5)
        .await
        .unwrap();
    assert!(matches!(
        promotion,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 5,
            event_next_offset: 5,
            applied_event_offset: Some(4),
        }
    ));

    let mut sub = follower
        .subscribe(
            "multi-pass",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 5 },
        )
        .await
        .unwrap();
    let mut payloads = Vec::new();
    for _ in 0..5 {
        let msg = recv_with_timeout(&mut sub, 1000)
            .await
            .expect("replicated message should deliver after promotion");
        payloads.push(msg.message.payload);
    }
    assert_eq!(
        payloads,
        vec![
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"four".to_vec(),
            b"five".to_vec(),
        ]
    );

    owner.shutdown().await;
    follower.shutdown().await;
}

#[tokio::test]
async fn checkpoint_aware_catch_up_preserves_normal_catch_up_path() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;

    follower
        .become_replication_follower("checkpoint-aware-normal", Partition::new(0), None)
        .await
        .unwrap();

    let (publisher, _confirms) = owner
        .get_publisher("checkpoint-aware-normal", Partition::new(0), &None)
        .await
        .unwrap();
    for payload in [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
        let reply = publisher
            .publish(
                payload,
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

    let outcome = follower
        .catch_up_replication_follower_from_owner_with_checkpoint(
            &owner,
            "checkpoint-aware-normal",
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            BrokerReplicationCatchUpOptions {
                max_messages_per_read: 2,
                max_events_per_read: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BrokerReplicationCatchUp::CaughtUp(BrokerReplicationCatchUpProgress {
            iterations: 3,
            applied_message_records: 3,
            applied_event_records: 3,
            message_next_offset: 3,
            event_next_offset: 3,
        })
    );

    owner.shutdown().await;
    follower.shutdown().await;
}

#[tokio::test]
async fn checkpoint_aware_catch_up_repairs_overlapping_local_prefix() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;
    let topic = "checkpoint-overlap";
    let partition = Partition::new(0);

    let (stale_publisher, _stale_confirms) = follower
        .get_publisher(topic, partition, &None)
        .await
        .unwrap();
    let stale_reply = stale_publisher
        .publish(
            b"stale local data".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    stale_reply.await.unwrap().unwrap();

    follower
        .become_replication_follower_with_epoch(topic, partition, None, 1)
        .await
        .unwrap();
    owner
        .advance_replication_epoch(topic, partition, None, 1)
        .await
        .unwrap();

    let (publisher, _confirms) = owner.get_publisher(topic, partition, &None).await.unwrap();
    for payload in [
        b"owner one".to_vec(),
        b"owner two".to_vec(),
        b"owner three".to_vec(),
    ] {
        let reply = publisher
            .publish(
                payload,
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

    let outcome = follower
        .catch_up_replication_follower_from_owner_with_checkpoint(
            &owner,
            topic,
            partition,
            None,
            ReplicationResourceKind::Queue,
            BrokerReplicationCatchUpOptions {
                max_messages_per_read: 10,
                max_events_per_read: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let BrokerReplicationCatchUp::CaughtUp(progress) = outcome else {
        panic!("overlapping prefix should be repaired by checkpoint install");
    };
    assert_eq!(progress.message_next_offset, 3);
    assert_eq!(progress.event_next_offset, 3);

    let promotion = follower
        .promote_replication_follower_if_caught_up(topic, partition, None, 3, 3)
        .await
        .unwrap();
    assert!(matches!(
        promotion,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 3,
            event_next_offset: 3,
            applied_event_offset: Some(2),
        }
    ));

    let mut sub = follower
        .subscribe(
            topic,
            partition,
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();
    let mut payloads = Vec::new();
    for _ in 0..3 {
        payloads.push(
            recv_with_timeout(&mut sub, 1000)
                .await
                .expect("checkpoint-repaired message should deliver")
                .message
                .payload,
        );
    }
    assert_eq!(
        payloads,
        vec![
            b"owner one".to_vec(),
            b"owner two".to_vec(),
            b"owner three".to_vec(),
        ]
    );

    owner.shutdown().await;
    follower.shutdown().await;
}

#[test]
fn follower_worker_state_builds_catch_up_options_from_current_offsets() {
    let cfg = FollowerReplicationWorkerConfig {
        max_messages_per_read: 11,
        max_events_per_read: 13,
        max_bytes_per_read: 19,
        max_iterations_per_tick: 17,
        allow_checkpoint_install: false,
        caught_up_poll_ms: 1000,
        retry_poll_ms: 100,
        checkpoint_retry_poll_ms: 5000,
        follow_runtime_settings: false,
        stream_enabled: false,
        stream_apply_linger_us: 2_000,
        stream_apply_max_merge_bytes: 16 * 1024 * 1024,
        stream_buffer_batches: 8,
    };
    let state = FollowerReplicationWorkerState::new(23, 29);

    assert_eq!(
        state.catch_up_options(cfg),
        BrokerReplicationCatchUpOptions {
            message_from: 23,
            event_from: 29,
            max_messages_per_read: 11,
            max_events_per_read: 13,
            max_bytes_per_read: 19,
            max_iterations: 17,
            max_wait_ms: 0,
        }
    );
}

#[test]
fn follower_worker_state_records_caught_up_progress_and_enables_wait_budget() {
    let cfg = FollowerReplicationWorkerConfig::default();
    let mut state = FollowerReplicationWorkerState::new(0, 0);
    let progress = BrokerReplicationCatchUpProgress {
        message_next_offset: 5,
        event_next_offset: 7,
        ..Default::default()
    };

    state.record_catch_up(cfg, &BrokerReplicationCatchUp::CaughtUp(progress));

    assert_eq!(state.message_next_offset, 5);
    assert_eq!(state.event_next_offset, 7);
    assert_eq!(state.status, FollowerReplicationWorkerStatus::CaughtUp);
    assert_eq!(state.last_progress, Some(progress));
    assert_eq!(state.next_delay_ms, 0);
    assert_eq!(
        state.catch_up_options(cfg).max_wait_ms,
        cfg.caught_up_poll_ms
    );
}

#[test]
fn follower_worker_state_uses_wait_budget_only_after_caught_up() {
    let cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 250,
        ..Default::default()
    };
    let mut state = FollowerReplicationWorkerState::new(0, 0);

    assert_eq!(state.catch_up_options(cfg).max_wait_ms, 0);

    state.record_catch_up(
        cfg,
        &BrokerReplicationCatchUp::IterationLimit {
            progress: BrokerReplicationCatchUpProgress::default(),
        },
    );
    assert_eq!(state.catch_up_options(cfg).max_wait_ms, 0);

    state.record_catch_up(
        cfg,
        &BrokerReplicationCatchUp::CaughtUp(BrokerReplicationCatchUpProgress::default()),
    );
    assert_eq!(
        state.catch_up_options(cfg).max_wait_ms,
        cfg.caught_up_poll_ms
    );
}

#[test]
fn follower_worker_state_records_iteration_limit_as_retry() {
    let cfg = FollowerReplicationWorkerConfig::default();
    let mut state = FollowerReplicationWorkerState::new(1, 2);
    let progress = BrokerReplicationCatchUpProgress {
        message_next_offset: 3,
        event_next_offset: 4,
        ..Default::default()
    };

    state.record_catch_up(cfg, &BrokerReplicationCatchUp::IterationLimit { progress });

    assert_eq!(state.message_next_offset, 3);
    assert_eq!(state.event_next_offset, 4);
    assert_eq!(state.status, FollowerReplicationWorkerStatus::PendingRetry);
    assert_eq!(state.last_progress, Some(progress));
    assert_eq!(state.next_delay_ms, cfg.retry_poll_ms);
}

#[test]
fn follower_worker_state_records_checkpoint_requirement() {
    let cfg = FollowerReplicationWorkerConfig::default();
    let mut state = FollowerReplicationWorkerState::new(10, 20);
    let checkpoint = BrokerReplicationCheckpointRequired {
        epoch: 2,
        requested_offset: 10,
        head_offset: 15,
        next_offset: 40,
    };
    let progress = BrokerReplicationCatchUpProgress {
        message_next_offset: 10,
        event_next_offset: 20,
        ..Default::default()
    };

    state.record_catch_up(
        cfg,
        &BrokerReplicationCatchUp::CheckpointRequired {
            progress,
            messages: Some(checkpoint.clone()),
            events: None,
        },
    );

    assert_eq!(
        state.status,
        FollowerReplicationWorkerStatus::CheckpointRequired {
            messages: Some(checkpoint),
            events: None,
        }
    );
    assert_eq!(state.last_progress, Some(progress));
    assert_eq!(state.next_delay_ms, cfg.checkpoint_retry_poll_ms);
}

#[test]
fn follower_worker_checkpoint_install_is_policy_gated() {
    let mut state = FollowerReplicationWorkerState::new(10, 20);
    let checkpoint = BrokerReplicationCheckpointRequired {
        epoch: 2,
        requested_offset: 10,
        head_offset: 15,
        next_offset: 40,
    };

    state.record_catch_up(
        FollowerReplicationWorkerConfig::default(),
        &BrokerReplicationCatchUp::CheckpointRequired {
            progress: BrokerReplicationCatchUpProgress {
                message_next_offset: 10,
                event_next_offset: 20,
                ..Default::default()
            },
            messages: Some(checkpoint),
            events: None,
        },
    );

    assert!(!state.should_install_checkpoint(FollowerReplicationWorkerConfig::default()));
    assert!(
        state.should_install_checkpoint(FollowerReplicationWorkerConfig {
            allow_checkpoint_install: true,
            ..Default::default()
        })
    );
}

#[test]
fn follower_worker_checkpoint_install_requires_checkpoint_status() {
    let mut state = FollowerReplicationWorkerState::new(0, 0);
    let cfg = FollowerReplicationWorkerConfig {
        allow_checkpoint_install: true,
        ..Default::default()
    };

    assert!(!state.should_install_checkpoint(cfg));

    state.record_catch_up(
        cfg,
        &BrokerReplicationCatchUp::CaughtUp(BrokerReplicationCatchUpProgress {
            message_next_offset: 1,
            event_next_offset: 1,
            ..Default::default()
        }),
    );

    assert!(!state.should_install_checkpoint(cfg));
}

#[tokio::test]
async fn follower_worker_tick_records_catch_up_progress() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("worker-tick", Partition::new(0), Some("workers"));

    let transition = assignment_transition(
        "worker-tick",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    let group = Some("workers".to_string());
    let (publisher, _confirms) = owner
        .get_publisher("worker-tick", Partition::new(0), &group)
        .await
        .unwrap();
    for payload in [b"one".to_vec(), b"two".to_vec()] {
        let reply = publisher
            .publish(
                payload,
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

    let cfg = FollowerReplicationWorkerConfig {
        max_messages_per_read: 1,
        max_events_per_read: 1,
        max_iterations_per_tick: 1,
        ..Default::default()
    };
    let outcome = follower
        .run_follower_replication_worker_once(&owner, &queue, ReplicationResourceKind::Queue, cfg)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BrokerReplicationCatchUp::IterationLimit {
            progress: BrokerReplicationCatchUpProgress {
                iterations: 1,
                applied_message_records: 1,
                applied_event_records: 1,
                message_next_offset: 1,
                event_next_offset: 1,
            },
        }
    );
    assert_eq!(
        follower
            .follower_replication_worker_snapshot("worker-tick", Partition::new(0), Some("workers"))
            .await,
        Some(FollowerReplicationWorkerState {
            message_next_offset: 1,
            event_next_offset: 1,
            status: FollowerReplicationWorkerStatus::PendingRetry,
            last_progress: Some(BrokerReplicationCatchUpProgress {
                iterations: 1,
                applied_message_records: 1,
                applied_event_records: 1,
                message_next_offset: 1,
                event_next_offset: 1,
            }),
            next_delay_ms: cfg.retry_poll_ms,
        })
    );
    let observability = follower.sparse_queue_observability_report();
    assert_eq!(observability.replication_summary.follower_worker_count, 1);
    assert_eq!(observability.replication_summary.pending_retry_count, 1);
    assert_eq!(observability.replication_followers.len(), 1);
    let worker = &observability.replication_followers[0];
    assert_eq!(worker.topic, "worker-tick");
    assert_eq!(worker.partition, Partition::new(0));
    assert_eq!(worker.group.as_deref(), Some("workers"));
    assert!(!worker.busy);
    assert_eq!(
        worker.state.as_ref().map(|state| state.message_next_offset),
        Some(1)
    );

    owner.shutdown().await;
    follower.shutdown().await;
}

#[derive(Debug)]
struct EmptyOwnerPeer;

impl BrokerOwnerReplicationPeer for EmptyOwnerPeer {
    fn read_owner_replication_records<'a>(
        &'a self,
        _topic: &'a str,
        _partition: fibril_storage::Partition,
        _group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        _max_messages: usize,
        _max_events: usize,
        _max_bytes: usize,
        _max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        Box::pin(async move {
            Ok(BrokerOwnerReplicationRecords {
                messages: OwnerReplicationRead::Batch(OwnerReplicationBatch::<Message> {
                    epoch: 0,
                    requested_offset: message_from,
                    next_offset: message_from,
                    records: Vec::new(),
                }),
                events: OwnerReplicationRead::Batch(OwnerReplicationBatch {
                    epoch: 0,
                    requested_offset: event_from,
                    next_offset: event_from,
                    records: Vec::new(),
                }),
            })
        })
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        _topic: &'a str,
        _partition: fibril_storage::Partition,
        _group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        Box::pin(async {
            Err(BrokerError::Unknown(
                "empty owner peer does not export checkpoints".into(),
            ))
        })
    }
}

struct StaticOwnerPeerResolver {
    peer: Arc<dyn BrokerOwnerReplicationPeer>,
}

impl StaticOwnerPeerResolver {
    fn new(peer: Arc<dyn BrokerOwnerReplicationPeer>) -> Self {
        Self { peer }
    }
}

impl BrokerOwnerReplicationPeerResolver for StaticOwnerPeerResolver {
    fn resolve_owner_peer<'a>(
        &'a self,
        _assignment: &'a PartitionAssignment,
        _kind: ReplicationResourceKind,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn BrokerOwnerReplicationPeer>>, BrokerError>> {
        Box::pin(async move { Ok(Some(self.peer.clone())) })
    }
}

#[derive(Debug)]
struct NotOwnerPeer;

impl BrokerOwnerReplicationPeer for NotOwnerPeer {
    fn read_owner_replication_records<'a>(
        &'a self,
        topic: &'a str,
        partition: fibril_storage::Partition,
        group: Option<&'a str>,
        _message_from: Offset,
        _event_from: Offset,
        _max_messages: usize,
        _max_events: usize,
        _max_bytes: usize,
        _max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        Box::pin(async move {
            Err(BrokerError::NotOwner {
                topic: topic.into(),
                partition,
                group: group.map(str::to_string),
            })
        })
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        _topic: &'a str,
        _partition: fibril_storage::Partition,
        _group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        Box::pin(async {
            Err(BrokerError::Unknown(
                "not-owner peer does not export checkpoints".into(),
            ))
        })
    }
}

#[derive(Debug)]
struct CountingEmptyOwnerPeer {
    reads: AtomicUsize,
    last_max_wait_ms: AtomicU64,
    read_notify: Notify,
}

impl CountingEmptyOwnerPeer {
    fn new() -> Self {
        Self {
            reads: AtomicUsize::new(0),
            last_max_wait_ms: AtomicU64::new(0),
            read_notify: Notify::new(),
        }
    }

    async fn wait_for_read(&self) {
        while self.reads.load(Ordering::Acquire) == 0 {
            self.read_notify.notified().await;
        }
    }
}

impl BrokerOwnerReplicationPeer for CountingEmptyOwnerPeer {
    fn read_owner_replication_records<'a>(
        &'a self,
        _topic: &'a str,
        _partition: fibril_storage::Partition,
        _group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        _max_messages: usize,
        _max_events: usize,
        _max_bytes: usize,
        max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        self.last_max_wait_ms.store(max_wait_ms, Ordering::Release);
        self.read_notify.notify_waiters();
        Box::pin(async move {
            Ok(BrokerOwnerReplicationRecords {
                messages: OwnerReplicationRead::Batch(OwnerReplicationBatch::<Message> {
                    epoch: 0,
                    requested_offset: message_from,
                    next_offset: message_from,
                    records: Vec::new(),
                }),
                events: OwnerReplicationRead::Batch(OwnerReplicationBatch {
                    epoch: 0,
                    requested_offset: event_from,
                    next_offset: event_from,
                    records: Vec::new(),
                }),
            })
        })
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        _topic: &'a str,
        _partition: fibril_storage::Partition,
        _group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        Box::pin(async {
            Err(BrokerError::Unknown(
                "counting owner peer does not export checkpoints".into(),
            ))
        })
    }
}

#[derive(Debug)]
struct BlockingOwnerPeer {
    reads: AtomicUsize,
    read_notify: Notify,
}

impl BlockingOwnerPeer {
    fn new() -> Self {
        Self {
            reads: AtomicUsize::new(0),
            read_notify: Notify::new(),
        }
    }

    async fn wait_for_read(&self) {
        while self.reads.load(Ordering::Acquire) == 0 {
            self.read_notify.notified().await;
        }
    }
}

impl BrokerOwnerReplicationPeer for BlockingOwnerPeer {
    fn read_owner_replication_records<'a>(
        &'a self,
        _topic: &'a str,
        _partition: fibril_storage::Partition,
        _group: Option<&'a str>,
        _message_from: Offset,
        _event_from: Offset,
        _max_messages: usize,
        _max_events: usize,
        _max_bytes: usize,
        _max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        self.read_notify.notify_waiters();
        Box::pin(std::future::pending())
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        _topic: &'a str,
        _partition: fibril_storage::Partition,
        _group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn follower_worker_tick_uses_owner_peer_abstraction() {
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("peer-abstraction", Partition::new(0), Some("workers"));
    let transition = assignment_transition(
        "peer-abstraction",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    let cfg = FollowerReplicationWorkerConfig::default();
    let outcome = follower
        .run_follower_replication_worker_once(
            &EmptyOwnerPeer,
            &queue,
            ReplicationResourceKind::Queue,
            cfg,
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BrokerReplicationCatchUp::CaughtUp(BrokerReplicationCatchUpProgress {
            iterations: 1,
            applied_message_records: 0,
            applied_event_records: 0,
            message_next_offset: 0,
            event_next_offset: 0,
        })
    );
    assert_eq!(
        follower
            .follower_replication_worker_snapshot(
                "peer-abstraction",
                Partition::new(0),
                Some("workers")
            )
            .await
            .map(|state| state.status),
        Some(FollowerReplicationWorkerStatus::CaughtUp)
    );

    follower.shutdown().await;
}

#[tokio::test]
async fn follower_worker_tick_long_polls_only_after_caught_up() {
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("peer-long-poll", Partition::new(0), Some("workers"));
    let transition = assignment_transition(
        "peer-long-poll",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    let peer = CountingEmptyOwnerPeer::new();
    let cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 375,
        ..Default::default()
    };

    follower
        .run_follower_replication_worker_once(&peer, &queue, ReplicationResourceKind::Queue, cfg)
        .await
        .unwrap();
    assert_eq!(peer.last_max_wait_ms.load(Ordering::Acquire), 0);

    follower
        .run_follower_replication_worker_once(&peer, &queue, ReplicationResourceKind::Queue, cfg)
        .await
        .unwrap();
    assert_eq!(
        peer.last_max_wait_ms.load(Ordering::Acquire),
        cfg.caught_up_poll_ms
    );

    follower.shutdown().await;
}

#[tokio::test]
async fn owner_replication_long_poll_wakes_on_broker_shutdown() {
    let (owner, _owner_dir) = open_test_broker().await;
    let reader = {
        let owner = owner.clone();
        tokio::spawn(async move {
            owner
                .read_owner_replication_records(
                    "shutdown-long-poll",
                    Partition::new(0),
                    None,
                    0,
                    0,
                    8,
                    8,
                    usize::MAX,
                    60_000,
                )
                .await
        })
    };

    tokio::time::sleep(Duration::from_millis(20)).await;
    owner.shutdown().await;
    let result = tokio::time::timeout(Duration::from_secs(1), reader)
        .await
        .expect("broker shutdown should wake owner replication long-poll")
        .unwrap();
    drop(result);
}

#[tokio::test]
async fn follower_worker_loop_cancels_in_flight_owner_read() {
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("peer-loop-cancel-read", Partition::new(0), Some("workers"));
    let assignment =
        PartitionAssignment::new(queue.clone(), "node-a", vec!["node-b".to_string()], 1);
    let transition = assignment_transition(
        "peer-loop-cancel-read",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    let peer = Arc::new(BlockingOwnerPeer::new());
    let resolver = Arc::new(StaticOwnerPeerResolver::new(peer.clone()));
    let shutdown = CancellationToken::new();
    let cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 60_000,
        ..Default::default()
    };

    let canceller = async {
        peer.wait_for_read().await;
        shutdown.cancel();
    };
    let loop_result =
        follower.run_follower_replication_worker_loop(
            assignment,
            resolver,
            ReplicationResourceKind::Queue,
            cfg,
            shutdown.clone(),
        );
    let (_, outcome) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(canceller, loop_result)
    })
    .await
    .expect("worker loop cancellation should not wait for owner read timeout");

    assert_eq!(
        outcome.unwrap(),
        FollowerReplicationWorkerLoopExit::Cancelled { ticks: 0 }
    );
    assert_eq!(peer.reads.load(Ordering::Acquire), 1);

    follower.shutdown().await;
}

#[tokio::test]
async fn broker_shutdown_stops_spawned_follower_worker() {
    let (follower, _follower_dir) = open_test_broker().await;
    let topic = "broker-shutdown-stops-follower";
    let queue = QueueIdentity::new(topic, Partition::new(0), Some("workers"));
    let assignment =
        PartitionAssignment::new(queue.clone(), "node-a", vec!["node-b".to_string()], 1);
    let transition = assignment_transition(
        topic,
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    let peer = Arc::new(BlockingOwnerPeer::new());
    let resolver = Arc::new(StaticOwnerPeerResolver::new(peer.clone()));
    follower
        .spawn_follower_replication_worker_loop(
            assignment,
            resolver,
            ReplicationResourceKind::Queue,
            FollowerReplicationWorkerConfig {
                caught_up_poll_ms: 60_000,
                ..Default::default()
            },
        )
        .unwrap();

    peer.wait_for_read().await;
    tokio::time::timeout(Duration::from_secs(1), follower.shutdown())
        .await
        .expect("broker shutdown should stop spawned follower workers promptly");
    assert!(!follower.has_follower_replication_worker(topic, Partition::new(0), Some("workers")));
}

#[tokio::test]
async fn follower_worker_loop_ticks_until_cancelled() {
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("peer-loop", Partition::new(0), Some("workers"));
    let assignment =
        PartitionAssignment::new(queue.clone(), "node-a", vec!["node-b".to_string()], 1);
    let transition = assignment_transition(
        "peer-loop",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    let peer = Arc::new(CountingEmptyOwnerPeer::new());
    let resolver = Arc::new(StaticOwnerPeerResolver::new(peer.clone()));
    let shutdown = CancellationToken::new();
    let cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 60_000,
        ..Default::default()
    };

    let canceller = async {
        peer.wait_for_read().await;
        shutdown.cancel();
    };
    let loop_result =
        follower.run_follower_replication_worker_loop(
            assignment,
            resolver,
            ReplicationResourceKind::Queue,
            cfg,
            shutdown.clone(),
        );
    let (_, outcome) = tokio::join!(canceller, loop_result);

    assert_eq!(
        outcome.unwrap(),
        FollowerReplicationWorkerLoopExit::Cancelled { ticks: 1 }
    );
    assert_eq!(peer.reads.load(Ordering::Acquire), 1);

    follower.shutdown().await;
}

#[tokio::test]
async fn follower_worker_loop_exits_when_worker_is_stopped() {
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("peer-loop-stopped", Partition::new(0), Some("workers"));
    let assignment = PartitionAssignment::new(queue, "node-a", vec!["node-b".to_string()], 1);
    let resolver = Arc::new(StaticOwnerPeerResolver::new(Arc::new(EmptyOwnerPeer)));
    let shutdown = CancellationToken::new();

    let outcome = follower
        .run_follower_replication_worker_loop(
            assignment,
            resolver,
            ReplicationResourceKind::Queue,
            FollowerReplicationWorkerConfig::default(),
            shutdown,
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        FollowerReplicationWorkerLoopExit::WorkerStopped { ticks: 0 }
    );

    follower.shutdown().await;
}

#[tokio::test]
async fn follower_worker_loop_exits_when_owner_peer_is_not_owner() {
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("peer-loop-not-owner", Partition::new(0), Some("workers"));
    let assignment =
        PartitionAssignment::new(queue.clone(), "node-a", vec!["node-b".to_string()], 1);
    let transition = assignment_transition(
        "peer-loop-not-owner",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    let resolver = Arc::new(StaticOwnerPeerResolver::new(Arc::new(NotOwnerPeer)));
    let shutdown = CancellationToken::new();
    let outcome = follower
        .run_follower_replication_worker_loop(
            assignment,
            resolver,
            ReplicationResourceKind::Queue,
            FollowerReplicationWorkerConfig::default(),
            shutdown,
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        FollowerReplicationWorkerLoopExit::OwnerChanged { ticks: 0 }
    );

    follower.shutdown().await;
}

#[tokio::test]
async fn follower_worker_loop_supervisor_starts_only_once() {
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("peer-loop-once", Partition::new(0), Some("workers"));
    let assignment =
        PartitionAssignment::new(queue.clone(), "node-a", vec!["node-b".to_string()], 1);
    let transition = assignment_transition(
        "peer-loop-once",
        LocalAssignmentIntent::BecomeFollower,
        None,
        Some(LocalAssignmentRole::Follower),
    );
    follower
        .apply_assignment_transition(&transition)
        .await
        .unwrap();

    let peer = Arc::new(CountingEmptyOwnerPeer::new());
    let resolver_peer: Arc<dyn BrokerOwnerReplicationPeer> = peer.clone();
    let resolver = Arc::new(StaticOwnerPeerResolver::new(resolver_peer));
    let cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 60_000,
        ..Default::default()
    };

    assert!(
        follower
            .spawn_follower_replication_worker_loop(
                assignment.clone(),
                resolver.clone(),
                ReplicationResourceKind::Queue,
                cfg,
            )
            .unwrap()
    );
    assert!(
        !follower
            .spawn_follower_replication_worker_loop(
                assignment,
                resolver,
                ReplicationResourceKind::Queue,
                cfg,
            )
            .unwrap()
    );

    peer.wait_for_read().await;
    assert_eq!(peer.reads.load(Ordering::Acquire), 1);

    follower.shutdown().await;
}

#[tokio::test]
async fn assignment_watcher_can_start_and_stop_follower_replication_loop() {
    let coordination = Arc::new(StaticCoordination::new(
        "node-a",
        coordination_snapshot(Vec::new(), 1),
    ));
    let (broker, _dir) = open_test_broker_with_ownership(coordination.clone()).await;
    let peer = Arc::new(CountingEmptyOwnerPeer::new());
    let resolver_peer: Arc<dyn BrokerOwnerReplicationPeer> = peer.clone();
    let resolver = Arc::new(StaticOwnerPeerResolver::new(resolver_peer));
    let cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 60_000,
        ..Default::default()
    };
    broker.spawn_assignment_watcher_with_follower_replication(coordination.clone(), resolver, cfg);

    let queue = QueueIdentity::new("watched-replicated", Partition::new(0), Some("workers"));
    coordination.update_snapshot(coordination_snapshot(
        vec![PartitionAssignment::new(
            queue,
            "node-b",
            vec!["node-a".to_string()],
            2,
        )],
        2,
    ));

    tokio::time::timeout(Duration::from_secs(2), peer.wait_for_read())
        .await
        .expect("assignment watcher should start follower replication loop");
    assert!(broker.has_follower_replication_worker(
        "watched-replicated",
        Partition::new(0),
        Some("workers")
    ));

    coordination.update_snapshot(coordination_snapshot(Vec::new(), 3));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !broker.has_follower_replication_worker(
                "watched-replicated",
                Partition::new(0),
                Some("workers"),
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("assignment watcher should stop follower replication loop");

    broker.shutdown().await;
}

#[tokio::test]
async fn follower_worker_tick_requires_registered_worker() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;
    let queue = QueueIdentity::new("missing-worker", Partition::new(0), None);

    let err = follower
        .run_follower_replication_worker_once(
            &owner,
            &queue,
            ReplicationResourceKind::Queue,
            FollowerReplicationWorkerConfig::default(),
        )
        .await
        .expect_err("worker tick should reject unregistered queues");

    assert!(matches!(err, BrokerError::InvalidArgument(_)));
    assert!(!follower.has_follower_replication_worker("missing-worker", Partition::new(0), None));

    owner.shutdown().await;
    follower.shutdown().await;
}

async fn recv_with_timeout(
    sub: &mut fibril_broker::broker::ConsumerHandle,
    timeout_ms: u64,
) -> Option<DeliverableMessage> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), sub.recv())
        .await
        .ok()
        .flatten()
}

fn assert_dlq_metadata(
    msg: &DeliverableMessage,
    source_topic: &str,
    source_offset: &str,
    retry_count: &str,
    reason: &str,
) {
    assert_eq!(
        msg.message
            .headers
            .get("stroma.dlq.source_topic")
            .map(String::as_str),
        Some(source_topic)
    );
    assert_eq!(
        msg.message
            .headers
            .get("stroma.dlq.source_offset")
            .map(String::as_str),
        Some(source_offset)
    );
    assert_eq!(
        msg.message
            .headers
            .get("stroma.dlq.retry_count")
            .map(String::as_str),
        Some(retry_count)
    );
    assert_eq!(
        msg.message
            .headers
            .get("stroma.dlq.reason")
            .map(String::as_str),
        Some(reason)
    );
    assert!(
        msg.message
            .headers
            .contains_key("stroma.dlq.dead_lettered_at_ms")
    );
}

async fn wait_for_queue_activity(
    broker: &Broker<StromaEngine>,
    topic: &str,
    group: Option<&str>,
    active_publishers: usize,
    active_subscribers: usize,
    idle: bool,
) {
    for _ in 0..2_000 {
        if broker
            .queue_activity_snapshot(topic, group)
            .is_some_and(|snapshot| {
                snapshot.active_publishers == active_publishers
                    && snapshot.active_subscribers == active_subscribers
                    && (snapshot.idle_since_ms.is_some() == idle)
            })
        {
            return;
        }
        tokio::task::yield_now().await;
    }

    panic!(
        "queue activity for {topic}/{group:?} did not become publishers={active_publishers} subscribers={active_subscribers} idle={idle}"
    );
}

async fn wait_for_queue_idle(broker: &Broker<StromaEngine>, topic: &str, group: Option<&str>) {
    wait_for_queue_activity(broker, topic, group, 0, 0, true).await;
}

#[tokio::test]
async fn queue_activity_tracks_independent_publisher_sinks() {
    let (broker, _dir) = open_test_broker().await;

    assert!(broker.queue_activity_snapshot("t", None).is_none());

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 1);
    assert_eq!(snapshot.active_subscribers, 0);
    assert_eq!(snapshot.idle_since_ms, None);
    assert!(broker.is_queue_materialized("t", None));

    let (pubh2, _confirms2) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 2);
    assert_eq!(snapshot.idle_since_ms, None);

    drop(pubh2);
    wait_for_queue_activity(&broker, "t", None, 1, 0, false).await;
    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 1);
    assert_eq!(snapshot.idle_since_ms, None);

    let before_idle = unix_millis();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;
    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 0);
    assert_eq!(snapshot.active_subscribers, 0);
    assert!(snapshot.idle_since_ms.is_some_and(|ts| ts >= before_idle));
}

#[tokio::test]
async fn queue_activity_starts_idle_after_last_publisher_and_subscriber_drop() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    let sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();

    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 1);
    assert_eq!(snapshot.active_subscribers, 1);
    assert_eq!(snapshot.idle_since_ms, None);
    assert!(broker.is_queue_materialized("t", None));

    drop(pubh);
    wait_for_queue_activity(&broker, "t", None, 0, 1, false).await;
    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 0);
    assert_eq!(snapshot.active_subscribers, 1);
    assert_eq!(snapshot.idle_since_ms, None);

    let before_idle = unix_millis();
    drop(sub);
    wait_for_queue_idle(&broker, "t", None).await;
    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 0);
    assert_eq!(snapshot.active_subscribers, 0);
    assert!(snapshot.idle_since_ms.is_some_and(|ts| ts >= before_idle));

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 1);
    assert_eq!(snapshot.active_subscribers, 0);
    assert_eq!(snapshot.idle_since_ms, None);

    drop(pubh);
}

#[tokio::test]
async fn subscriber_lease_materializes_queue_before_returning() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();

    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 0);
    assert_eq!(snapshot.active_subscribers, 1);
    assert!(broker.is_queue_materialized("t", None));

    drop(sub);
}

#[tokio::test]
async fn queue_eviction_skips_active_publishers() {
    let (broker, _dir) = open_test_broker().await;

    let (_pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();

    let attempt = broker.try_evict_inactive_queue("t", None, 0).await.unwrap();

    assert_eq!(
        attempt,
        QueueEvictionAttempt::Skipped(QueueEvictionSkip::Active)
    );
}

#[tokio::test]
async fn queue_eviction_skips_untracked_queue() {
    let (broker, _dir) = open_test_broker().await;

    let attempt = broker
        .try_evict_inactive_queue("missing", None, 0)
        .await
        .unwrap();

    assert_eq!(
        attempt,
        QueueEvictionAttempt::Skipped(QueueEvictionSkip::NotTracked)
    );
}

#[tokio::test]
async fn queue_eviction_skips_active_subscribers() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let _sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();

    let attempt = broker.try_evict_inactive_queue("t", None, 0).await.unwrap();

    assert_eq!(
        attempt,
        QueueEvictionAttempt::Skipped(QueueEvictionSkip::Active)
    );
}

#[tokio::test]
async fn queue_eviction_skips_when_idle_threshold_not_met() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    let attempt = broker
        .try_evict_inactive_queue("t", None, 60_000)
        .await
        .unwrap();

    assert_eq!(
        attempt,
        QueueEvictionAttempt::Skipped(QueueEvictionSkip::NotIdleEnough)
    );
}

#[tokio::test]
async fn queue_eviction_skips_broker_delivery_tags() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_activity(&broker, "t", None, 0, 0, true).await;

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let _msg = sub.recv().await.unwrap();
    drop(sub);
    wait_for_queue_idle(&broker, "t", None).await;

    let attempt = broker.try_evict_inactive_queue("t", None, 0).await.unwrap();

    assert_eq!(
        attempt,
        QueueEvictionAttempt::Skipped(QueueEvictionSkip::HasBrokerDeliveries)
    );
}

#[tokio::test]
async fn queue_eviction_unmaterializes_idle_publisher_without_messages() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    let attempt = broker.try_evict_inactive_queue("t", None, 0).await.unwrap();

    assert_eq!(
        attempt,
        QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
    );
}

#[tokio::test]
async fn queue_eviction_unmaterializes_idle_materialized_queue() {
    let metrics = Metrics::new(60);
    let (broker, _dir) =
        open_test_broker_with_metrics(BrokerConfig::default(), metrics.clone()).await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    let attempt = broker.try_evict_inactive_queue("t", None, 0).await.unwrap();

    assert_eq!(
        attempt,
        QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
    );
    let sparse = broker.sparse_queue_observability_snapshot();
    assert_eq!(sparse.len(), 1);
    assert_eq!(sparse[0].topic, "t");
    assert!(sparse[0].idle_for_ms.is_some());
    let cleanup = sparse[0].last_eviction_attempt.as_ref().unwrap();
    assert_eq!(cleanup.kind, "storage");
    assert_eq!(cleanup.outcome, "evicted");
    let cleanup_metrics = metrics.broker().snapshot().queue_cleanup;
    assert_eq!(cleanup_metrics.attempts_total, 1);
    assert_eq!(cleanup_metrics.storage_evicted_total, 1);
}

#[tokio::test]
async fn queue_eviction_reports_not_materialized_after_previous_unmaterialize() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    let first = broker.try_evict_inactive_queue("t", None, 0).await.unwrap();
    let second = broker.try_evict_inactive_queue("t", None, 0).await.unwrap();

    assert_eq!(first, QueueEvictionAttempt::Storage(EvictOutcome::Evicted));
    assert_eq!(
        second,
        QueueEvictionAttempt::Storage(EvictOutcome::NotMaterialized)
    );
}

#[tokio::test]
async fn queue_eviction_sweep_reports_skips_and_storage_outcomes() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (_active_pubh, _confirms) = broker
        .get_publisher("active", Partition::new(0), &None)
        .await
        .unwrap();
    let idle_group = Some("g".to_string());
    let (idle_pubh, _confirms) = broker
        .get_publisher("idle", Partition::new(0), &idle_group)
        .await
        .unwrap();
    idle_pubh
        .publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    drop(idle_pubh);
    wait_for_queue_idle(&broker, "idle", Some("g")).await;

    let sub = broker
        .subscribe(
            "sub",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();

    let mut attempts = broker.evict_inactive_queues(0).await.unwrap();
    attempts.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    assert_eq!(
        attempts,
        vec![
            (
                "active".to_string(),
                None,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::Active)
            ),
            (
                "idle".to_string(),
                idle_group,
                QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
            ),
            (
                "sub".to_string(),
                None,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::Active)
            ),
        ]
    );

    drop(sub);
}

#[tokio::test]
async fn queue_eviction_sweep_unmaterializes_idle_materialized_queue() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    let attempts = broker.evict_inactive_queues(0).await.unwrap();

    assert_eq!(
        attempts,
        vec![(
            "t".to_string(),
            None,
            QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
        )]
    );
}

#[tokio::test]
async fn queue_eviction_sweep_does_not_repeat_already_unloaded_queue() {
    let metrics = Metrics::new(60);
    let (broker, _dir) =
        open_test_broker_with_metrics(BrokerConfig::default(), metrics.clone()).await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    let first = broker.evict_inactive_queues(0).await.unwrap();
    let second = broker.evict_inactive_queues(0).await.unwrap();

    assert_eq!(
        first,
        vec![(
            "t".to_string(),
            None,
            QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
        )]
    );
    assert!(second.is_empty());
    let cleanup_metrics = metrics.broker().snapshot().queue_cleanup;
    assert_eq!(cleanup_metrics.attempts_total, 1);
    assert_eq!(cleanup_metrics.storage_evicted_total, 1);
    assert_eq!(cleanup_metrics.storage_not_materialized_total, 0);
}

#[tokio::test]
async fn queue_eviction_sweep_rechecks_queue_materialized_after_previous_unload() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    let first = broker.evict_inactive_queues(0).await.unwrap();
    assert_eq!(
        first,
        vec![(
            "t".to_string(),
            None,
            QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
        )]
    );
    assert!(!broker.is_queue_materialized("t", None));

    broker.engine().materialize("t", 0, None).await.unwrap();
    assert!(broker.is_queue_materialized("t", None));

    let second = broker.evict_inactive_queues(0).await.unwrap();
    assert_eq!(
        second,
        vec![(
            "t".to_string(),
            None,
            QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
        )]
    );
    assert!(!broker.is_queue_materialized("t", None));
}

#[tokio::test]
async fn queue_eviction_sweep_unmaterializes_storage_only_materialized_queue() {
    let (engine, _dir) = open_test_engine().await;
    engine.materialize("inspected", 0, None).await.unwrap();
    let broker = Broker::new(engine, BrokerConfig::default(), None);

    assert!(broker.is_queue_materialized("inspected", None));

    let attempts = broker.evict_inactive_queues(0).await.unwrap();

    assert_eq!(
        attempts,
        vec![(
            "inspected".to_string(),
            None,
            QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
        )]
    );
    assert!(!broker.is_queue_materialized("inspected", None));
}

#[tokio::test(start_paused = true)]
async fn queue_eviction_worker_is_disabled_by_default() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    tokio::time::advance(Duration::from_secs(60)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }

    assert!(broker.is_queue_materialized("t", None));
    broker.shutdown().await;
}

#[tokio::test]
async fn queue_eviction_worker_can_be_enabled_after_startup() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_idle(&broker, "t", None).await;

    assert!(broker.is_queue_materialized("t", None));

    broker.update_config(BrokerConfig {
        queue_idle_evict_after_ms: Some(0),
        queue_idle_sweep_interval_ms: 10,
        ..(*broker.config_snapshot()).clone()
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while broker.is_queue_materialized("t", None) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(!broker.is_queue_materialized("t", None));
    broker.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn queue_eviction_worker_leaves_active_publisher_materialized() {
    let (broker, _dir) = open_test_broker_with_cfg(BrokerConfig {
        inflight_ttl_ms: 2000,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
        queue_idle_evict_after_ms: Some(0),
        queue_idle_sweep_interval_ms: 10,
        ..Default::default()
    })
    .await;

    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();

    tokio::time::advance(Duration::from_millis(11)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    assert!(broker.is_queue_materialized("t", None));
    drop(pubh);
    broker.shutdown().await;
}

#[tokio::test]
async fn active_publisher_does_not_race_idle_cleanup_into_double_open() {
    let (broker, _dir) = open_test_broker_with_cfg(BrokerConfig {
        inflight_ttl_ms: 2000,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
        queue_idle_evict_after_ms: Some(0),
        queue_idle_sweep_interval_ms: 1,
        ..Default::default()
    })
    .await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();

    for i in 0..50 {
        let reply = pubh
            .publish(
                format!("msg-{i}").into_bytes(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        reply
            .await
            .unwrap()
            .unwrap_or_else(|err| panic!("publish {i} failed while cleanup was sweeping: {err}"));
        tokio::task::yield_now().await;
    }

    assert!(broker.is_queue_materialized("t", None));
    drop(pubh);
    broker.shutdown().await;
}

#[tokio::test]
async fn broker_delivers_messages_in_order() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    let client_id = Uuid::now_v7();

    for _ in 0..5 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 10 },
        )
        .await
        .unwrap();

    let mut offs = Vec::new();
    for _ in 0..5 {
        let msg = sub.recv().await.unwrap();
        offs.push(msg.message.offset);
        sub.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: msg.delivery_tag,
        })
        .await
        .unwrap();
    }

    assert_eq!(offs, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn delayed_publish_waits_until_deadline() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    let client_id = Uuid::now_v7();

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();

    let not_before = unix_millis() + 500;
    let confirm = pubh
        .publish_delayed(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            not_before,
        )
        .await
        .unwrap();

    // Leave a generous gap before the deadline. This verifies delayed messages
    // are not immediately visible without making the test depend on tight
    // scheduler timing around the exact not-before instant. Awaiting the
    // publish confirm after this check prevents confirm latency from consuming
    // the whole delay window before the assertion runs.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), sub.recv())
            .await
            .is_err()
    );

    confirm.await.unwrap().unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.message.offset, 0);
    assert!(unix_millis() >= not_before);
}

#[tokio::test]
async fn delayed_retry_waits_until_deadline() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _confirms) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let msg = sub.recv().await.expect("initial delivery");
    let not_before = unix_millis() + 500;
    sub.settle(SettleRequest {
        settle_type: SettleType::Nack {
            requeue: Some(true),
            not_before: Some(not_before),
        },
        delivery_tag: msg.delivery_tag,
    })
    .await
    .unwrap();

    assert!(recv_with_timeout(&mut sub, 100).await.is_none());

    let redelivered = recv_with_timeout(&mut sub, 2_000)
        .await
        .expect("message should be redelivered after delayed retry deadline");
    assert_eq!(redelivered.message.offset, msg.message.offset);
    assert!(unix_millis() >= not_before);
}

#[tokio::test]
async fn broker_respects_prefetch() {
    let (broker, _dir) = open_test_broker().await;

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    let client_id = Uuid::now_v7();

    for i in 0..10 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
        println!("Published message {}", i);
    }

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();

    let mut msgs = Vec::new();
    for _ in 0..3 {
        msgs.push(sub.recv().await.unwrap());
        println!(
            "Received message with offset {}",
            msgs.last().unwrap().message.offset
        );
    }

    // Should block / timeout / return None depending on API
    assert!(
        tokio::time::timeout(Duration::from_millis(50), sub.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn ack_releases_prefetch_slot() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    for i in 0..5 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 2 },
        )
        .await
        .unwrap();

    let m1 = sub.recv().await.unwrap();
    let m2 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m1.delivery_tag,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await; // allow for async processing

    assert!(!sub.is_empty());

    let m3 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m2.delivery_tag,
    })
    .await
    .unwrap();

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m3.delivery_tag,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await; // allow for async processing

    assert!(!sub.is_empty());

    let m4 = sub.recv().await.unwrap();
    let m5 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    assert_eq!(m1.message.offset, 0);
    assert_eq!(m1.message.payload, b"x0");
    assert_eq!(m2.message.offset, 1);
    assert_eq!(m2.message.payload, b"x1");
    assert_eq!(m3.message.offset, 2);
    assert_eq!(m3.message.payload, b"x2");
    assert_eq!(m4.message.offset, 3);
    assert_eq!(m4.message.payload, b"x3");
    assert_eq!(m5.message.offset, 4);
    assert_eq!(m5.message.payload, b"x4");
}

#[tokio::test]
async fn ack_releases_prefetch_slot2() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    for i in 0..5 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 2 },
        )
        .await
        .unwrap();

    let m1 = sub.recv().await.unwrap();
    let m2 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m1.delivery_tag,
    })
    .await
    .unwrap();

    let m3 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m2.delivery_tag,
    })
    .await
    .unwrap();

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: m3.delivery_tag,
    })
    .await
    .unwrap();

    let m4 = sub.recv().await.unwrap();
    let m5 = sub.recv().await.unwrap();

    assert!(sub.is_empty());

    dbg!(&m1, &m2, &m3, &m4, &m5);

    assert_eq!(m1.message.offset, 0);
    assert_eq!(m1.message.payload, b"x0");
    assert_eq!(m2.message.offset, 1);
    assert_eq!(m2.message.payload, b"x1");
    assert_eq!(m3.message.offset, 2);
    assert_eq!(m3.message.payload, b"x2");
    assert_eq!(m4.message.offset, 3);
    assert_eq!(m4.message.payload, b"x3");
    assert_eq!(m5.message.offset, 4);
    assert_eq!(m5.message.payload, b"x4");
}

#[tokio::test]
async fn broker_redelivers_after_expiry() {
    // fibril_util::init_tracing_dbg();
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
        None,
    )
    .await
    .unwrap();
    println!("Published message with offset 0");

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    println!("Receiving first message");

    let msg1 = sub.recv().await.unwrap();
    assert_eq!(msg1.message.offset, 0);

    // do NOT ack - let it expire
    tokio::time::sleep(Duration::from_millis(2200)).await;

    println!("Attempting to receive after expiry");

    let msg2 = sub.recv().await.unwrap();
    println!(
        "Received message with offset {} after expiry",
        msg2.message.offset
    );
    assert_eq!(msg2.message.offset, 0);
}

#[tokio::test]
async fn broker_distributes_across_consumers() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    for _ in 0..10 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut c1 = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let mut c2 = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();

    let m1 = c1.recv().await.unwrap();
    let m2 = c2.recv().await.unwrap();

    assert_ne!(m1.message.offset, m2.message.offset);
}

#[tokio::test]
async fn slow_consumer_does_not_starve_fast_one() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    for _ in 0..5 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut slow = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();
    let mut fast = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 5 },
        )
        .await
        .unwrap();

    let _ = slow.recv().await.unwrap(); // never ack

    let mut got = Vec::new();
    for _ in 0..4 {
        let m = fast.recv().await.unwrap();
        got.push(m.message.offset);
        fast.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: m.delivery_tag,
        })
        .await
        .unwrap();
    }

    assert_eq!(got.len(), 4);
}

#[tokio::test]
async fn unsubscribe_requeues_prefetched_unacked_messages() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    for i in 0..3 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();

    let mut leased = Vec::new();
    for _ in 0..3 {
        leased.push(sub.recv().await.unwrap().message.offset);
    }
    assert_eq!(leased, vec![0, 1, 2]);

    broker
        .unsubscribe(&sub.topic, sub.group.as_deref(), sub.partition, sub.sub_id)
        .await
        .unwrap();

    let mut replacement = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();

    let mut redelivered = Vec::new();
    for _ in 0..3 {
        let msg = replacement.recv().await.unwrap();
        redelivered.push(msg.message.offset);
        replacement
            .settle(SettleRequest {
                settle_type: SettleType::Ack,
                delivery_tag: msg.delivery_tag,
            })
            .await
            .unwrap();
    }

    redelivered.sort_unstable();
    assert_eq!(redelivered, vec![0, 1, 2]);
}

#[tokio::test]
async fn unsubscribe_redistributes_prefetched_messages_to_active_subscriber() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    for i in 0..3 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut first = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();

    let mut leased = Vec::new();
    for _ in 0..3 {
        leased.push(first.recv().await.unwrap().message.offset);
    }
    leased.sort_unstable();
    assert_eq!(leased, vec![0, 1, 2]);

    let mut replacement = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), replacement.recv())
            .await
            .is_err()
    );

    broker
        .unsubscribe(
            &first.topic,
            first.group.as_deref(),
            first.partition,
            first.sub_id,
        )
        .await
        .unwrap();

    let mut redelivered = Vec::new();
    for _ in 0..3 {
        let msg = replacement.recv().await.unwrap();
        redelivered.push(msg.message.offset);
        replacement
            .settle(SettleRequest {
                settle_type: SettleType::Ack,
                delivery_tag: msg.delivery_tag,
            })
            .await
            .unwrap();
    }

    assert_eq!(redelivered, vec![0, 1, 2]);
}

#[tokio::test]
async fn unsubscribe_redelivery_survives_if_active_replacement_has_no_capacity_then_unsubs() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    for i in 0..3 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut first = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 2 },
        )
        .await
        .unwrap();
    let mut replacement = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1 },
        )
        .await
        .unwrap();

    let first_a = first.recv().await.unwrap();
    let first_b = first.recv().await.unwrap();
    let held_by_replacement = replacement.recv().await.unwrap();
    let mut initially_leased = vec![
        first_a.message.offset,
        first_b.message.offset,
        held_by_replacement.message.offset,
    ];
    initially_leased.sort_unstable();
    assert_eq!(initially_leased, vec![0, 1, 2]);

    broker
        .unsubscribe(
            &first.topic,
            first.group.as_deref(),
            first.partition,
            first.sub_id,
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), replacement.recv())
            .await
            .is_err()
    );

    broker
        .unsubscribe(
            &replacement.topic,
            replacement.group.as_deref(),
            replacement.partition,
            replacement.sub_id,
        )
        .await
        .unwrap();

    let mut final_sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();

    let mut redelivered = Vec::new();
    for _ in 0..3 {
        let msg = final_sub.recv().await.unwrap();
        redelivered.push(msg.message.offset);
        final_sub
            .settle(SettleRequest {
                settle_type: SettleType::Ack,
                delivery_tag: msg.delivery_tag,
            })
            .await
            .unwrap();
    }

    redelivered.sort_unstable();
    assert_eq!(redelivered, vec![0, 1, 2]);
}

#[tokio::test]
async fn unsubscribe_does_not_requeue_acked_messages_after_settles_drain() {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, _) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();
    for i in 0..3 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();

    let acked = sub.recv().await.unwrap();
    let first_unacked = sub.recv().await.unwrap();
    let second_unacked = sub.recv().await.unwrap();

    sub.settle(SettleRequest {
        settle_type: SettleType::Ack,
        delivery_tag: acked.delivery_tag,
    })
    .await
    .unwrap();
    broker.wait_for_pending_settles().await;

    broker
        .unsubscribe(&sub.topic, sub.group.as_deref(), sub.partition, sub.sub_id)
        .await
        .unwrap();

    let mut replacement = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 3 },
        )
        .await
        .unwrap();

    let first = replacement.recv().await.unwrap();
    let second = replacement.recv().await.unwrap();

    assert_eq!(acked.message.offset, 0);
    assert_eq!(first_unacked.message.offset, 1);
    assert_eq!(second_unacked.message.offset, 2);
    assert_eq!(first.message.offset, 1);
    assert_eq!(second.message.offset, 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), replacement.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn nack_requeue_redelivers() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m1 = t.recv(&c).await?;
    assert_eq!(m1.offset, 0);

    t.nack(&c, m1, true).await?;

    let m2 = t.recv(&c).await?;
    assert_eq!(m2.offset, 0);

    Ok(())
}

#[tokio::test]
async fn nack_without_requeue_drops_message() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m = t.recv(&c).await?;
    t.nack(&c, m, false).await?;

    t.expect_no_message(&c, 50).await;

    Ok(())
}

#[tokio::test]
async fn publish_engine_open_conflict_is_returned_to_publisher() -> anyhow::Result<()> {
    let broker = Broker::new(FailingPublishEngine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker.get_publisher("t", Partition::new(0), &None).await?;
    let reply = publisher
        .publish(
            b"conflict".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await?;
    let err = tokio::time::timeout(Duration::from_secs(2), reply)
        .await
        .expect("publish reply should not hang")?
        .expect_err("storage open conflict should be returned to publisher");

    assert!(
        err.to_string().contains("already open"),
        "unexpected publish error: {err}"
    );

    broker.shutdown_graceful().await;
    Ok(())
}

#[tokio::test]
async fn global_dlq_policy_routes_exhausted_message_to_global_target()
-> Result<(), Box<dyn std::error::Error>> {
    let (engine, _dir) = open_test_engine().await;
    let target = GlobalDLQ::new("_dlq.source", 0, None).await?;

    assert_eq!(
        engine.set_global_dlq(Some(target), 0).await?,
        GlobalDlqUpdateOutcome::Stored(GlobalDlqSnapshot {
            version: 1,
            target: Some(GlobalDLQ::new("_dlq.source", 0, None).await?),
        })
    );
    engine
        .declare_queue(
            "source",
            0,
            None,
            DeclareMeta {
                dlq_policy: Some(DLQDiscardPolicyWire::GlobalDQL),
                dlq_max_retries: Some(0),
                default_message_ttl_ms: None,
            },
        )
        .await?;

    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker
        .get_publisher("source", Partition::new(0), &None)
        .await?;
    publisher
        .publish(
            b"poison".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await?
        .await??;

    let source_client = Uuid::now_v7();
    let mut source = broker
        .subscribe(
            "source",
            Partition::new(0),
            None,
            source_client,
            ConsumerConfig { prefetch: 1 },
        )
        .await?;
    let msg = source.recv().await.expect("source message delivered");
    source
        .settle(SettleRequest {
            settle_type: SettleType::Nack {
                requeue: Some(true),
                not_before: None,
            },
            delivery_tag: msg.delivery_tag,
        })
        .await?;

    let dlq_client = Uuid::now_v7();
    let mut dlq = broker
        .subscribe(
            "_dlq.source",
            Partition::new(0),
            None,
            dlq_client,
            ConsumerConfig { prefetch: 1 },
        )
        .await?;
    let dlq_msg = recv_with_timeout(&mut dlq, 2_000)
        .await
        .expect("message should be copied to global DLQ target");
    assert_eq!(dlq_msg.message.topic, "_dlq.source");
    assert_eq!(dlq_msg.message.offset, 0);
    assert_eq!(dlq_msg.message.payload, b"poison");
    assert_dlq_metadata(&dlq_msg, "source", "0", "0", "retries_exhausted");

    assert!(recv_with_timeout(&mut source, 100).await.is_none());

    broker.shutdown_graceful().await;
    Ok(())
}

#[tokio::test]
async fn dlq_replay_copies_message_back_to_source_without_system_headers()
-> Result<(), Box<dyn std::error::Error>> {
    let (engine, _dir) = open_test_engine().await;
    let replay_engine = engine.clone();
    engine
        .set_global_dlq(Some(GlobalDLQ::new("_dlq.source", 0, None).await?), 0)
        .await?;
    engine
        .declare_queue(
            "source",
            0,
            None,
            DeclareMeta {
                dlq_policy: Some(DLQDiscardPolicyWire::GlobalDQL),
                dlq_max_retries: Some(0),
                default_message_ttl_ms: None,
            },
        )
        .await?;

    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker
        .get_publisher("source", Partition::new(0), &None)
        .await?;
    publisher
        .publish(
            b"poison".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await?
        .await??;

    let source_client = Uuid::now_v7();
    let mut source = broker
        .subscribe(
            "source",
            Partition::new(0),
            None,
            source_client,
            ConsumerConfig { prefetch: 1 },
        )
        .await?;
    let msg = source.recv().await.expect("source message delivered");
    source
        .settle(SettleRequest {
            settle_type: SettleType::Nack {
                requeue: Some(true),
                not_before: None,
            },
            delivery_tag: msg.delivery_tag,
        })
        .await?;

    let dlq_client = Uuid::now_v7();
    let mut dlq = broker
        .subscribe(
            "_dlq.source",
            Partition::new(0),
            None,
            dlq_client,
            ConsumerConfig { prefetch: 1 },
        )
        .await?;
    let dlq_msg = recv_with_timeout(&mut dlq, 2_000)
        .await
        .expect("message should be copied to global DLQ target");
    assert_dlq_metadata(&dlq_msg, "source", "0", "0", "retries_exhausted");

    let report = replay_engine
        .replay_dead_letters("_dlq.source", None, &[dlq_msg.message.offset])
        .await?;
    assert_eq!(report.requested, 1);
    assert_eq!(report.replayed, 1);
    assert_eq!(report.items[0].target_topic.as_deref(), Some("source"));

    let replayed = recv_with_timeout(&mut source, 2_000)
        .await
        .expect("replayed message should be delivered to source queue");
    assert_eq!(replayed.message.topic, "source");
    assert_eq!(replayed.message.payload, b"poison");
    assert!(
        !replayed
            .message
            .headers
            .keys()
            .any(|key| key.starts_with("stroma.") || key.starts_with("fibril."))
    );

    broker.shutdown_graceful().await;
    Ok(())
}

#[tokio::test]
async fn dlq_replay_skips_messages_without_source_metadata() -> anyhow::Result<()> {
    let (engine, _dir) = open_test_engine().await;
    let headers = MessageHeaders {
        published: unix_millis(),
        publish_received: unix_millis(),
        content_type: None,
        extra: Default::default(),
    };
    let (completion, rx) = KeratinAppendCompletion::pair();
    engine
        .publish(
            "_dlq.source",
            0,
            None,
            &headers,
            b"missing metadata".to_vec(),
            completion,
        )
        .await?;
    rx.await??;

    let report = engine
        .replay_dead_letters("_dlq.source", None, &[0, 42])
        .await?;

    assert_eq!(report.requested, 2);
    assert_eq!(report.replayed, 0);
    assert_eq!(report.items.len(), 2);
    assert_eq!(report.items[0].offset, 0);
    assert_eq!(report.items[0].outcome, ReplayDeadLetterOutcome::Skipped);
    assert!(report.items[0].target_topic.is_none());
    assert!(
        report.items[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("stroma.dlq.source_topic")
    );
    assert_eq!(report.items[1].offset, 42);
    assert_eq!(report.items[1].outcome, ReplayDeadLetterOutcome::Skipped);
    assert!(
        report.items[1]
            .reason
            .as_deref()
            .unwrap()
            .contains("not active")
    );

    Ok(())
}

#[tokio::test]
async fn global_dlq_metadata_reports_retry_count_after_requeues()
-> Result<(), Box<dyn std::error::Error>> {
    let (engine, _dir) = open_test_engine().await;
    engine
        .set_global_dlq(Some(GlobalDLQ::new("_dlq.source", 0, None).await?), 0)
        .await?;
    engine
        .declare_queue(
            "source",
            0,
            None,
            DeclareMeta {
                dlq_policy: Some(DLQDiscardPolicyWire::GlobalDQL),
                dlq_max_retries: Some(2),
                default_message_ttl_ms: None,
            },
        )
        .await?;

    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker
        .get_publisher("source", Partition::new(0), &None)
        .await?;
    publisher
        .publish(
            b"poison".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await?
        .await??;

    let mut source = broker
        .subscribe(
            "source",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await?;

    for _ in 0..3 {
        let msg = recv_with_timeout(&mut source, 2_000)
            .await
            .expect("source message should be delivered until retry exhaustion");
        source
            .settle(SettleRequest {
                settle_type: SettleType::Nack {
                    requeue: Some(true),
                    not_before: None,
                },
                delivery_tag: msg.delivery_tag,
            })
            .await?;
    }

    let mut dlq = broker
        .subscribe(
            "_dlq.source",
            Partition::new(0),
            None,
            Uuid::now_v7(),
            ConsumerConfig { prefetch: 1 },
        )
        .await?;
    let dlq_msg = recv_with_timeout(&mut dlq, 2_000)
        .await
        .expect("message should be copied to global DLQ target");

    assert_eq!(dlq_msg.message.payload, b"poison");
    assert_dlq_metadata(&dlq_msg, "source", "0", "2", "retries_exhausted");
    assert!(recv_with_timeout(&mut source, 100).await.is_none());

    broker.shutdown_graceful().await;
    Ok(())
}

#[tokio::test]
async fn clearing_global_dlq_makes_global_policy_discard_exhausted_messages()
-> Result<(), Box<dyn std::error::Error>> {
    let (engine, _dir) = open_test_engine().await;
    let target = GlobalDLQ::new("_dlq.source", 0, None).await?;
    engine.set_global_dlq(Some(target), 0).await?;
    engine.set_global_dlq(None, 1).await?;
    engine
        .declare_queue(
            "source",
            0,
            None,
            DeclareMeta {
                dlq_policy: Some(DLQDiscardPolicyWire::GlobalDQL),
                dlq_max_retries: Some(0),
                default_message_ttl_ms: None,
            },
        )
        .await?;

    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker
        .get_publisher("source", Partition::new(0), &None)
        .await?;
    publisher
        .publish(
            b"discard".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await?
        .await??;

    let source_client = Uuid::now_v7();
    let mut source = broker
        .subscribe(
            "source",
            Partition::new(0),
            None,
            source_client,
            ConsumerConfig { prefetch: 1 },
        )
        .await?;
    let msg = source.recv().await.expect("source message delivered");
    source
        .settle(SettleRequest {
            settle_type: SettleType::Nack {
                requeue: Some(true),
                not_before: None,
            },
            delivery_tag: msg.delivery_tag,
        })
        .await?;

    let dlq_client = Uuid::now_v7();
    let mut dlq = broker
        .subscribe(
            "_dlq.source",
            Partition::new(0),
            None,
            dlq_client,
            ConsumerConfig { prefetch: 1 },
        )
        .await?;
    assert!(recv_with_timeout(&mut dlq, 200).await.is_none());
    assert!(recv_with_timeout(&mut source, 100).await.is_none());

    broker.shutdown_graceful().await;
    Ok(())
}

#[tokio::test]
async fn restart_redelivers_unacked() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m1 = t.recv(&c).await?;
    assert_eq!(m1.offset, 0);

    // no ack
    t.restart_broker("b1").await?;

    // must resubscribe
    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m2 = t.recv(&c2).await?;
    assert_eq!(m2.offset, 0);

    Ok(())
}

#[tokio::test]
async fn restart_does_not_redeliver_acked() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m = t.recv(&c).await?;
    t.ack(&c, m).await?;

    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    t.expect_no_message(&c2, 50).await;

    Ok(())
}

#[tokio::test]
async fn restart_preserves_prefetch_semantics() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish_many("b1", "t", None, b"x", 3).await?;

    let c = t.sub("b1", "t", None).prefetch(2).create().await?;

    let _m1 = t.recv(&c).await?;
    let _m2 = t.recv(&c).await?;

    // No ack → prefetch exhausted
    t.expect_no_message(&c, 50).await;

    Ok(())
}

#[tokio::test]
async fn double_ack_is_ignored() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m = t.recv(&c).await?;

    t.ack(&c, m.clone()).await?;
    // second ack — should not panic
    let _ = t.ack(&c, m).await;

    Ok(())
}

#[tokio::test]
async fn expired_after_consumer_drop() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 1,
        expiry_poll_min_ms: 10,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    });

    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c1 = t.sub("b1", "t", None).prefetch(1).create().await?;

    let _m = t.recv(&c1).await?;

    // drop consumer
    t.remove_consumer(&c1.id);

    t.sleep_ms(1200).await;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    let m2 = t.recv(&c2).await?;
    assert_eq!(m2.offset, 0);

    Ok(())
}

#[tokio::test]
async fn restart_during_ack_completion() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c).await?;

    // Fire ack but do not await anything else
    let _ = t.ack(&c, m).await;

    // Immediately restart
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    // Should NOT redeliver
    t.expect_no_message(&c2, 50).await;

    Ok(())
}

#[tokio::test]
async fn nack_requeue_then_restart() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c).await?;

    t.nack(&c, m, true).await?;
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m2 = t.recv(&c2).await?;
    assert_eq!(m2.offset, 0);

    Ok(())
}

#[tokio::test]
async fn expiry_across_restart() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 1000,
        expiry_poll_min_ms: 10,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    });

    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let _ = t.recv(&c).await?;

    t.sleep_ms(1200).await;
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c2).await?;
    assert_eq!(m.offset, 0);

    Ok(())
}

#[tokio::test]
async fn uneven_prefetch_fairness() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish_many("b1", "t", None, b"x", 20).await?;

    let slow = t.sub("b1", "t", None).prefetch(1).create().await?;
    let fast = t.sub("b1", "t", None).prefetch(10).create().await?;

    let _ = t.recv(&slow).await?; // never ack

    let msgs = t.recv_n(&fast, 10).await?;
    t.ack_all(&fast, msgs).await?;

    // Fast should continue progressing
    let more = t.recv_n(&fast, 5).await?;
    assert!(!more.is_empty());

    Ok(())
}

#[tokio::test]
async fn chaos_small_run() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new();
    t.start_broker("b1").await?;

    t.publish_many("b1", "t", None, b"x", 50).await?;

    let mut c = t.sub("b1", "t", None).prefetch(5).create().await?;

    for i in 0..100 {
        dbg!("Iteration ", i);
        if let Ok(Ok(m)) = tokio::time::timeout(Duration::from_millis(1000), t.recv(&c)).await {
            if i % 3 == 0 {
                t.nack(&c, m, true).await?;
            } else {
                t.ack(&c, m).await?;
            }
        }

        if i % 10 == 0 {
            dbg!("Restarting broker");
            t.restart_broker("b1").await?;
            dbg!("Broker restarted");
            c = t.sub("b1", "t", None).prefetch(5).create().await?;
            dbg!("Resubscribed");
        }
    }

    Ok(())
}

#[tokio::test]
async fn chaos_deterministic_restart_ack_nack() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 4_000,
        expiry_poll_min_ms: 50,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    });
    t.start_broker("b1").await?;

    let total = 50;
    t.publish_many("b1", "t", None, b"x", total).await?;

    let mut c = t.sub("b1", "t", None).prefetch(5).create().await?;

    let mut seen = std::collections::HashSet::new();
    let mut acked = std::collections::HashSet::new();

    // Phase 1: mixed processing + periodic restart
    for i in 0..250 {
        // bounded wait so we never hang
        if let Ok(Ok(m)) =
            tokio::time::timeout(std::time::Duration::from_millis(100), t.recv(&c)).await
        {
            // detect duplicate delivery within this test run
            assert!(
                !acked.contains(&m.offset),
                "Offset {} redelivered after ack",
                m.offset
            );
            seen.insert(m.offset);

            // deterministic pattern
            let offset = m.offset;
            if i % 3 == 0 {
                t.nack(&c, m, true).await?;
                println!("Nacked offset {}", offset);
            } else {
                t.ack(&c, m).await?;
                acked.insert(offset);
                println!("Acked offset {}", offset);
            }
        }

        // restart every 10 iterations (deterministic)
        if i > 0 && i % 10 == 0 {
            println!("Restarting broker at iteration {}", i);
            t.restart_broker("b1").await?;
            c = t.sub("b1", "t", None).prefetch(5).create().await?;
        }
    }

    // Phase 2: drain remaining messages
    let mut drained = 0;
    while let Ok(Ok(m)) =
        tokio::time::timeout(std::time::Duration::from_millis(100), t.recv(&c)).await
    {
        if !seen.insert(m.offset) {
            panic!(
                "Duplicate delivery detected in drain for offset {}",
                m.offset
            );
        }
        acked.insert(m.offset);
        t.ack(&c, m).await?;
        drained += 1;
    }

    println!("Drained {} messages after chaos phase", drained);

    // Final verification

    // All offsets must have been eventually acked
    assert_eq!(acked.len(), total);

    // No phantom offsets
    for off in acked {
        assert!(off < total as u64, "Unexpected offset {}", off);
    }

    Ok(())
}

#[tokio::test]
async fn restart_race_with_ack() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 50,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    });
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c).await?;

    t.ack(&c, m).await?;

    // Immediately restart
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    // What do we expect here?
    match tokio::time::timeout(Duration::from_millis(100), t.recv(&c2)).await {
        Ok(Ok(m2)) => {
            println!("Redelivered offset {}", m2.offset);
        }
        _ => {
            println!("Not redelivered");
        }
    }

    Ok(())
}

#[tokio::test]
async fn restart_race_with_ack2() -> Result<(), Box<dyn std::error::Error>> {
    let mut t = TestState::new_with_cfg(BrokerConfig {
        inflight_ttl_ms: 500,
        expiry_poll_min_ms: 100,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 100000,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    });
    t.start_broker("b1").await?;

    t.publish("b1", "t", None, b"x").await?;

    let c = t.sub("b1", "t", None).prefetch(1).create().await?;
    let m = t.recv(&c).await?;

    t.ack(&c, m).await?;

    // Immediately restart
    t.restart_broker("b1").await?;

    let c2 = t.sub("b1", "t", None).prefetch(1).create().await?;

    // What do we expect here?
    match tokio::time::timeout(Duration::from_millis(100), t.recv(&c2)).await {
        Ok(Ok(m2)) => {
            panic!("Redelivered offset {}", m2.offset);
        }
        _ => {
            println!("Not redelivered");
        }
    }

    Ok(())
}

#[tokio::test]
async fn stress_single_consumer_100k() {
    stress_single_consumer(100_000).await;
}

// Only on `--release` builds
#[cfg(not(debug_assertions))]
#[tokio::test]
async fn stress_single_consumer_500k() {
    stress_single_consumer(500_000).await;
}

#[cfg(not(debug_assertions))]
#[tokio::test]
async fn stress_single_consumer_1m() {
    stress_single_consumer(1_000_000).await;
}

async fn stress_single_consumer(total: usize) {
    let (broker, _dir) = open_test_broker().await;
    let client_id = Uuid::now_v7();

    let (pubh, mut confirmer) = broker
        .get_publisher("t", Partition::new(0), &None)
        .await
        .unwrap();

    let mut to_publish_list = (0..total)
        .map(|i| format!("b{i}").as_bytes().to_vec())
        .collect::<Vec<_>>();
    to_publish_list.sort_unstable();

    let confirme_task = tokio::spawn(async move {
        let mut i = 0;
        while i < total {
            confirmer.recv().await.unwrap();
            i += 1;
        }
    });

    for (i, to_publish) in to_publish_list.iter().enumerate() {
        pubh.publish(
            to_publish.to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
        if i % 5_000 == 0 {
            println!("Published {}", i);
        }
    }

    println!("Published {} messages", total);

    confirme_task.await.unwrap();

    println!("Confirmed {} messages", total);

    let mut sub = broker
        .subscribe(
            "t",
            Partition::new(0),
            None,
            client_id,
            ConsumerConfig { prefetch: 1000 },
        )
        .await
        .unwrap();

    let mut seen = 0;

    let mut received = Vec::new();
    while seen < total {
        let m = sub.recv().await.unwrap();
        received.push(m.message.payload);

        sub.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: m.delivery_tag,
        })
        .await
        .unwrap();

        seen += 1;

        if seen % 10_000 == 0 {
            println!("Processed {}", seen);
        }
    }

    // TODO: verify no duplicates or missing messages via offsets as well?
    received.sort_unstable();

    // Same payloads sent and received
    assert_eq!(to_publish_list, received);

    let received_len = received.len();
    let received_set: HashSet<Vec<u8>> = HashSet::from_iter(received.into_iter());

    // No duplicates
    assert_eq!(received_len, received_set.len());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
    // No overdelivery
    assert!(sub.is_empty());

    assert_eq!(seen, total);
}

#[tokio::test]
async fn stream_registry_opens_caches_routes_and_closes() {
    use fibril_broker::stream::SubscribeStart;

    let (broker, _dir) = open_test_broker().await;

    // open is idempotent: the second call returns the same hosted channel
    let ch1 = broker.get_or_open_stream("sensors", 0, fibril_broker::stream::StreamDurability::Durable, None).await.unwrap();
    let ch2 = broker.get_or_open_stream("sensors", 0, fibril_broker::stream::StreamDurability::Durable, None).await.unwrap();
    assert!(Arc::ptr_eq(&ch1, &ch2));

    // routing predicate distinguishes hosted streams
    assert!(broker.is_stream("sensors", 0));
    assert!(!broker.is_stream("sensors", 1));
    assert!(!broker.is_stream("other", 0));

    // publish + subscribe flow through the hosted channel
    let headers = MessageHeaders {
        published: 0,
        publish_received: 0,
        content_type: None,
        extra: Default::default(),
    };
    ch1.publish(headers.clone(), b"a".to_vec()).await.unwrap();
    ch1.publish(headers, b"b".to_vec()).await.unwrap();

    let sub = ch1.subscribe(SubscribeStart::Earliest, None).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let driver = tokio::spawn(sub.run(tx));
    assert_eq!(rx.recv().await.unwrap().offset, 0);
    assert_eq!(rx.recv().await.unwrap().offset, 1);
    driver.abort();

    // closing drops the hosted channel (durable data stays in stroma)
    broker.close_stream("sensors", 0);
    assert!(!broker.is_stream("sensors", 0));
}
