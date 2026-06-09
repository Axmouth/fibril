use std::{
    collections::{HashMap, HashSet as StdHashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use fibril_broker::{
    CompletionPair,
    broker::{
        Broker, BrokerAssignmentTransitionApply, BrokerConfig, BrokerError,
        BrokerFollowerReplicationApply, BrokerReplicationCatchUp, BrokerReplicationCatchUpOptions,
        BrokerReplicationCatchUpProgress, BrokerReplicationCheckpointRequired, ConsumerConfig,
        FollowerReplicationWorkerConfig, FollowerReplicationWorkerState,
        FollowerReplicationWorkerStatus, OwnedQueue, QueueEvictionAttempt, QueueEvictionSkip,
        SettleRequest, SettleType, StaticQueueOwnership,
    },
    coordination::{
        CoordinationSnapshot, LocalAssignmentIntent, LocalAssignmentRole,
        LocalAssignmentTransition, NodeInfo, PartitionAssignment, QueueIdentity,
        StaticCoordination,
    },
    queue_engine::{
        Deliverable, EvictOutcome, InspectMode, IoError, KeratinAppendCompletion, MessageHeaders,
        OwnerReplicationRead, QueueEngine, QueuePromotionOutcome, ReplayDeadLetterOutcome,
        ReplayDeadLettersReport, SettleRequest as EngineSettleRequest, StromaEngine,
    },
    test_util::TestState,
};
use fibril_metrics::{Metrics, QueuesStateSnapshot};
use fibril_storage::{DeliverableMessage, Offset};
use fibril_util::unix_millis;
use hashbrown::HashSet;
use stroma_core::{
    AppendCompletion, DLQDiscardPolicyWire, DeclareMeta, GlobalDLQ, GlobalDlqSnapshot,
    GlobalDlqUpdateOutcome, KeratinConfig, MessageInspectionPage, PublishItem, SnapshotConfig,
    StromaDebugSnapshot, StromaError, StromaKeratinConfig, StromaMetrics, TempDir, test_dir,
};
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct FailingPublishEngine;

#[async_trait]
impl QueueEngine for FailingPublishEngine {
    async fn poll_ready(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
        _max: usize,
        _lease_deadline: u64,
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

    async fn shutdown(&self) -> Result<(), StromaError> {
        Ok(())
    }

    async fn estimate_disk_used(&self) -> Result<u64, StromaError> {
        Ok(0)
    }

    async fn list_queues(&self) -> Result<Vec<(String, Option<String>)>, StromaError> {
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

    async fn unmaterialize(
        &self,
        _tp: &str,
        _part: u32,
        _group: Option<&str>,
    ) -> Result<EvictOutcome, StromaError> {
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

    fn metrics(&self) -> Arc<StromaMetrics> {
        Arc::new(StromaMetrics::default())
    }

    fn deadline_awaker(&self) -> Arc<Notify> {
        Arc::new(Notify::new())
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

    let err = match broker.get_publisher("unowned", &None).await {
        Ok(_) => panic!("unowned queue unexpectedly accepted a publisher"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        BrokerError::NotOwner {
            topic,
            partition: 0,
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
            partition: 0,
            group: None,
        } if topic == "unowned"
    ));
    assert!(!broker.is_queue_materialized("unowned", None));
    broker.shutdown().await;
}

#[tokio::test]
async fn static_ownership_allows_owned_queue() {
    let mut owned = StdHashSet::new();
    owned.insert(OwnedQueue::new("owned", 0, Some("workers")));
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(owned))).await;
    let group = Some("workers".to_string());

    let (publisher, _confirms) = broker.get_publisher("owned", &group).await.unwrap();
    let reply = publisher
        .publish(
            b"hello".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let mut sub = broker
        .subscribe(
            "owned",
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
async fn static_coordination_can_drive_broker_ownership_gate() {
    let local_node = "node-a".to_string();
    let remote_node = "node-b".to_string();
    let mut nodes = HashMap::new();
    nodes.insert(
        local_node.clone(),
        NodeInfo {
            node_id: local_node.clone(),
            broker_addr: "127.0.0.1:1001".parse().unwrap(),
            admin_addr: None,
        },
    );
    nodes.insert(
        remote_node.clone(),
        NodeInfo {
            node_id: remote_node.clone(),
            broker_addr: "127.0.0.1:1002".parse().unwrap(),
            admin_addr: None,
        },
    );

    let owned_queue = QueueIdentity::new("coord-owned", 0, Some("workers"));
    let followed_queue = QueueIdentity::new("coord-followed", 0, Some("workers"));
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
            generation: 1,
        },
    );
    let (broker, _dir) = open_test_broker_with_ownership(Arc::new(coordination)).await;
    let group = Some("workers".to_string());

    let (publisher, _confirms) = broker.get_publisher("coord-owned", &group).await.unwrap();
    let reply = publisher
        .publish(
            b"hello".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let err = match broker.get_publisher("coord-followed", &group).await {
        Ok(_) => panic!("followed queue unexpectedly accepted an owner publisher"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        BrokerError::NotOwner {
            topic,
            partition: 0,
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
        queue: QueueIdentity::new(topic, 0, Some("workers")),
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
            broker_addr: "127.0.0.1:1001".parse().unwrap(),
            admin_addr: None,
        },
    );
    nodes.insert(
        "node-b".to_string(),
        NodeInfo {
            node_id: "node-b".to_string(),
            broker_addr: "127.0.0.1:1002".parse().unwrap(),
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
    assert!(broker.has_follower_replication_worker("transition-followed", 0, Some("workers")));

    let err = broker
        .read_owner_replication_records("transition-followed", 0, Some("workers"), 0, 0, 1, 1)
        .await
        .expect_err("follower queue should reject owner replication reads");
    assert!(matches!(
        err,
        BrokerError::Engine(StromaError::WrongQueueRole { .. })
    ));
    broker.shutdown().await;
}

#[tokio::test]
async fn assignment_watcher_applies_snapshot_update_to_follower_role() {
    let coordination = Arc::new(StaticCoordination::new(
        "node-a",
        coordination_snapshot(Vec::new(), 1),
    ));
    let (broker, _dir) = open_test_broker_with_ownership(coordination.clone()).await;
    broker.spawn_assignment_watcher(coordination.clone());

    let queue = QueueIdentity::new("watched-followed", 0, Some("workers"));
    coordination.update_snapshot(coordination_snapshot(
        vec![PartitionAssignment::new(
            queue,
            "node-b",
            vec!["node-a".to_string()],
            2,
        )],
        2,
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if broker.is_queue_materialized("watched-followed", Some("workers")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("assignment watcher should materialize local follower queue");
    assert!(broker.has_follower_replication_worker("watched-followed", 0, Some("workers")));

    let err = broker
        .engine()
        .read_owner_message_records("watched-followed", 0, Some("workers"), 0, 1)
        .await
        .expect_err("watched follower queue should reject owner reads");
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
    assert!(!broker.has_follower_replication_worker("transition-owned", 0, Some("workers")));
    broker.shutdown().await;
}

#[tokio::test]
async fn assignment_transition_apply_defers_follower_promotion_without_offsets() {
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
        BrokerAssignmentTransitionApply::Deferred {
            intent: LocalAssignmentIntent::PromoteFollowerToOwner,
            reason: "promotion requires verified follower catch-up offsets",
        }
    );
    assert!(!broker.is_queue_materialized("transition-promote", Some("workers")));
    broker.shutdown().await;
}

#[tokio::test]
async fn owner_replication_read_returns_published_records() {
    let (broker, _dir) = open_test_broker().await;
    let (publisher, _confirms) = broker.get_publisher("replicated", &None).await.unwrap();

    let reply = publisher
        .publish(
            b"hello replication".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let records = broker
        .read_owner_replication_records("replicated", 0, None, 0, 0, 10, 10)
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
async fn owner_replication_read_rejects_unowned_queue_before_materializing() {
    let (broker, _dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(StdHashSet::new())))
            .await;

    let err = broker
        .read_owner_replication_records("unowned", 0, None, 0, 0, 10, 10)
        .await
        .expect_err("unowned queue unexpectedly served replication records");

    assert!(matches!(
        err,
        BrokerError::NotOwner {
            topic,
            partition: 0,
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
        .become_replication_follower("catchup", 0, None)
        .await
        .unwrap();

    let (publisher, _confirms) = owner.get_publisher("catchup", &None).await.unwrap();
    for payload in [b"first".to_vec(), b"second".to_vec()] {
        let reply = publisher
            .publish(
                payload,
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
            )
            .await
            .unwrap();
        reply.await.unwrap().unwrap();
    }

    let records = owner
        .read_owner_replication_records("catchup", 0, None, 0, 0, 10, 10)
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
        .apply_follower_replication_records("catchup", 0, None, records)
        .await
        .unwrap();
    assert!(matches!(apply, BrokerFollowerReplicationApply::Applied(_)));

    let promotion = follower
        .promote_replication_follower_if_caught_up(
            "catchup",
            0,
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

#[tokio::test]
async fn broker_replication_catch_up_loop_handles_multiple_passes() {
    let (owner, _owner_dir) = open_test_broker().await;
    let (follower, _follower_dir) = open_test_broker().await;

    follower
        .become_replication_follower("multi-pass", 0, None)
        .await
        .unwrap();

    let (publisher, _confirms) = owner.get_publisher("multi-pass", &None).await.unwrap();
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
            )
            .await
            .unwrap();
        reply.await.unwrap().unwrap();
    }

    let outcome = follower
        .catch_up_replication_follower_from_owner(
            &owner,
            "multi-pass",
            0,
            None,
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
        .promote_replication_follower_if_caught_up("multi-pass", 0, None, 5, 5)
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

#[test]
fn follower_worker_state_builds_catch_up_options_from_current_offsets() {
    let cfg = FollowerReplicationWorkerConfig {
        max_messages_per_read: 11,
        max_events_per_read: 13,
        max_iterations_per_tick: 17,
        caught_up_poll_ms: 1000,
        retry_poll_ms: 100,
        checkpoint_retry_poll_ms: 5000,
    };
    let state = FollowerReplicationWorkerState::new(23, 29);

    assert_eq!(
        state.catch_up_options(cfg),
        BrokerReplicationCatchUpOptions {
            message_from: 23,
            event_from: 29,
            max_messages_per_read: 11,
            max_events_per_read: 13,
            max_iterations: 17,
        }
    );
}

#[test]
fn follower_worker_state_records_caught_up_progress_and_cooldown() {
    let cfg = FollowerReplicationWorkerConfig::default();
    let mut state = FollowerReplicationWorkerState::new(0, 0);

    state.record_catch_up(
        cfg,
        &BrokerReplicationCatchUp::CaughtUp(BrokerReplicationCatchUpProgress {
            message_next_offset: 5,
            event_next_offset: 7,
            ..Default::default()
        }),
    );

    assert_eq!(state.message_next_offset, 5);
    assert_eq!(state.event_next_offset, 7);
    assert_eq!(state.status, FollowerReplicationWorkerStatus::CaughtUp);
    assert_eq!(state.next_delay_ms, cfg.caught_up_poll_ms);
}

#[test]
fn follower_worker_state_records_iteration_limit_as_retry() {
    let cfg = FollowerReplicationWorkerConfig::default();
    let mut state = FollowerReplicationWorkerState::new(1, 2);

    state.record_catch_up(
        cfg,
        &BrokerReplicationCatchUp::IterationLimit {
            progress: BrokerReplicationCatchUpProgress {
                message_next_offset: 3,
                event_next_offset: 4,
                ..Default::default()
            },
        },
    );

    assert_eq!(state.message_next_offset, 3);
    assert_eq!(state.event_next_offset, 4);
    assert_eq!(state.status, FollowerReplicationWorkerStatus::PendingRetry);
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

    state.record_catch_up(
        cfg,
        &BrokerReplicationCatchUp::CheckpointRequired {
            progress: BrokerReplicationCatchUpProgress {
                message_next_offset: 10,
                event_next_offset: 20,
                ..Default::default()
            },
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
    assert_eq!(state.next_delay_ms, cfg.checkpoint_retry_poll_ms);
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    let snapshot = broker.queue_activity_snapshot("t", None).unwrap();
    assert_eq!(snapshot.active_publishers, 1);
    assert_eq!(snapshot.active_subscribers, 0);
    assert_eq!(snapshot.idle_since_ms, None);
    assert!(broker.is_queue_materialized("t", None));

    let (pubh2, _confirms2) = broker.get_publisher("t", &None).await.unwrap();
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    let sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
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
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
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

    let (_pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();

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
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();
    drop(pubh);
    wait_for_queue_activity(&broker, "t", None, 0, 0, true).await;

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
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

    let (_active_pubh, _confirms) = broker.get_publisher("active", &None).await.unwrap();
    let idle_group = Some("g".to_string());
    let (idle_pubh, _confirms) = broker.get_publisher("idle", &idle_group).await.unwrap();
    idle_pubh
        .publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    drop(idle_pubh);
    wait_for_queue_idle(&broker, "idle", Some("g")).await;

    let sub = broker
        .subscribe("sub", None, client_id, ConsumerConfig { prefetch: 1 })
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
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
    })
    .await;

    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
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
    })
    .await;

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();

    for i in 0..50 {
        let reply = pubh
            .publish(
                format!("msg-{i}").into_bytes(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    let client_id = Uuid::now_v7();

    for _ in 0..5 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 10 })
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    let client_id = Uuid::now_v7();

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
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

    let (pubh, _confirms) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap();

    let mut sub = broker
        .subscribe("t", None, Uuid::now_v7(), ConsumerConfig { prefetch: 1 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    let client_id = Uuid::now_v7();

    for i in 0..10 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
        println!("Published message {}", i);
    }

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 3 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..5 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 2 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..5 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 2 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    pubh.publish(
        b"x".to_vec(),
        Default::default(),
        Default::default(),
        None,
        Default::default(),
    )
    .await
    .unwrap();
    println!("Published message with offset 0");

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for _ in 0..10 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut c1 = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
        .await
        .unwrap();
    let mut c2 = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for _ in 0..5 {
        pubh.publish(
            b"x".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut slow = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
        .await
        .unwrap();
    let mut fast = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 5 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..3 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 3 })
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
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 3 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..3 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut first = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 3 })
        .await
        .unwrap();

    let mut leased = Vec::new();
    for _ in 0..3 {
        leased.push(first.recv().await.unwrap().message.offset);
    }
    leased.sort_unstable();
    assert_eq!(leased, vec![0, 1, 2]);

    let mut replacement = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 3 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..3 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut first = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 2 })
        .await
        .unwrap();
    let mut replacement = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1 })
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
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 3 })
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

    let (pubh, _) = broker.get_publisher("t", &None).await.unwrap();
    for i in 0..3 {
        pubh.publish(
            format!("x{i}").as_bytes().to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let mut sub = broker
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 3 })
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
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 3 })
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
    let (publisher, _confirms) = broker.get_publisher("t", &None).await?;
    let reply = publisher
        .publish(
            b"conflict".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
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
            },
        )
        .await?;

    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker.get_publisher("source", &None).await?;
    publisher
        .publish(
            b"poison".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await?
        .await??;

    let source_client = Uuid::now_v7();
    let mut source = broker
        .subscribe(
            "source",
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
            },
        )
        .await?;

    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker.get_publisher("source", &None).await?;
    publisher
        .publish(
            b"poison".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await?
        .await??;

    let source_client = Uuid::now_v7();
    let mut source = broker
        .subscribe(
            "source",
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
            },
        )
        .await?;

    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker.get_publisher("source", &None).await?;
    publisher
        .publish(
            b"poison".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await?
        .await??;

    let mut source = broker
        .subscribe(
            "source",
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
            },
        )
        .await?;

    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (publisher, _confirms) = broker.get_publisher("source", &None).await?;
    publisher
        .publish(
            b"discard".to_vec(),
            Default::default(),
            Default::default(),
            None,
            Default::default(),
        )
        .await?
        .await??;

    let source_client = Uuid::now_v7();
    let mut source = broker
        .subscribe(
            "source",
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

    let (pubh, mut confirmer) = broker.get_publisher("t", &None).await.unwrap();

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
        .subscribe("t", None, client_id, ConsumerConfig { prefetch: 1000 })
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
